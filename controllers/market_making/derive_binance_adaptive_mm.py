"""Native Hummingbot V2 Binance-reference market maker for Derive perpetuals.

One controller instance owns one asset.  The same class is configured once for
XRP and once for LINK.  Binance is market data only; every executor action is
hard-wired to ``derive_perpetual``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from itertools import islice
from pathlib import Path
from threading import RLock
from typing import Any, ClassVar

from hummingbot.core.data_type.common import MarketDict, PositionAction, PositionMode, TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from pydantic import Field, field_validator, model_validator

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


def _is_hummingbot_2160(module_file: str) -> bool:
    """Return true only when the loaded connector belongs to Hummingbot 2.16.0."""
    try:
        path = Path(module_file).resolve()
        version_file = next(parent / "VERSION" for parent in path.parents if (parent / "VERSION").is_file())
        return version_file.read_text(encoding="utf-8").strip() == "2.16.0"
    except (OSError, StopIteration):
        return False


def _snapshot_shape_is_affected(data_source_class: type[Any]) -> bool:
    """Recognize the exact private-member shape audited in Hummingbot 2.16.0."""
    try:
        current = data_source_class._request_order_book_snapshot
        source = inspect.getsource(current)
        init_source = inspect.getsource(data_source_class.__init__)
        module = inspect.getmodule(data_source_class)
        return bool(
            module
            and _is_hummingbot_2160(module.__file__ or "")
            and "message_queue.get()" in source
            and "self._snapshot_messages" in source
            and "self._snapshot_messages = {}" in init_source
            and "self._snapshot_messages_queue_key" in init_source
        )
    except (AttributeError, OSError, TypeError):
        return False


def _install_derive_snapshot_race_compatibility() -> bool:
    """Patch only the known 2.16.0 snapshot-consumer race.

    The affected connector's initializer and snapshot parser both consume the
    same raw-message queue. Waiting for the parser-owned cache avoids stealing
    its messages and keeps all data sourcing inside the native connector.
    """
    try:
        from hummingbot.connector.derivative.derive_perpetual.derive_perpetual_api_order_book_data_source import (
            DerivePerpetualAPIOrderBookDataSource,
        )
    except (ImportError, ModuleNotFoundError):
        return False

    current = DerivePerpetualAPIOrderBookDataSource._request_order_book_snapshot
    if getattr(current, "_adaptive_mm_snapshot_race_compatibility", False):
        return True
    if not _snapshot_shape_is_affected(DerivePerpetualAPIOrderBookDataSource):
        return False

    async def wait_for_parsed_snapshot(self, trading_pair: str) -> dict[str, Any]:
        for _ in range(300):
            cached = self._snapshot_messages.get(trading_pair)
            if cached is not None:
                return {
                    "params": {
                        "data": {
                            "instrument_name": await self._connector.exchange_symbol_associated_to_pair(trading_pair),
                            "publish_id": cached.update_id,
                            "bids": cached.bids,
                            "asks": cached.asks,
                            "timestamp": cached.timestamp * 1000,
                        }
                    }
                }
            await asyncio.sleep(0.1)
        raise RuntimeError(f"Timed out waiting for parsed Derive order book snapshot for {trading_pair}")

    wait_for_parsed_snapshot._adaptive_mm_snapshot_race_compatibility = True
    DerivePerpetualAPIOrderBookDataSource._request_order_book_snapshot = wait_for_parsed_snapshot
    logging.getLogger(__name__).warning(
        "Activated guarded Hummingbot 2.16.0 Derive snapshot compatibility shim"
    )
    return True


DERIVE_SNAPSHOT_RACE_COMPATIBILITY_ACTIVE = _install_derive_snapshot_race_compatibility()


class MarketState(StrEnum):
    NORMAL = "NORMAL"
    UP_TREND = "UP_TREND"
    DOWN_TREND = "DOWN_TREND"
    HIGH_VOL = "HIGH_VOL"
    EXTREME = "EXTREME"


class MMMode(StrEnum):
    NEUTRAL = "NEUTRAL"
    LONG_BIAS = "LONG_BIAS"
    SHORT_BIAS = "SHORT_BIAS"
    DEFENSIVE = "DEFENSIVE"
    PAUSED = "PAUSED"


class InventoryMode(StrEnum):
    FLAT = "FLAT"
    LONG_SKEW = "LONG_SKEW"
    SHORT_SKEW = "SHORT_SKEW"
    ASK_ONLY = "ASK_ONLY"
    BID_ONLY = "BID_ONLY"


class OperationalState(StrEnum):
    SHADOW = "SHADOW"
    LIVE_DISARMED = "LIVE_DISARMED"
    LIVE_ARMED = "LIVE_ARMED"
    REFERENCE_PAUSED = "REFERENCE_PAUSED"
    DERIVE_PAUSED = "DERIVE_PAUSED"
    RISK_PAUSED = "RISK_PAUSED"
    ERROR = "ERROR"


class FillEffect(StrEnum):
    INCREASE_SAME_DIRECTION = "INCREASE_SAME_DIRECTION"
    REDUCE = "REDUCE"
    FLATTEN = "FLATTEN"
    FLIP_DIRECTION = "FLIP_DIRECTION"


@dataclass(frozen=True)
class PortfolioSnapshot:
    position_amount: Decimal
    mark_price: Decimal
    open_bid_amount: Decimal = ZERO
    open_bid_price: Decimal = ZERO
    open_ask_amount: Decimal = ZERO
    open_ask_price: Decimal = ZERO

    @property
    def position_notional(self) -> Decimal:
        return self.position_amount * self.mark_price

    @property
    def open_order_notional(self) -> Decimal:
        return self.open_bid_amount * self.open_bid_price + self.open_ask_amount * self.open_ask_price


@dataclass(frozen=True)
class PendingReservation:
    controller_id: str
    asset: str
    level: str
    side: TradeType
    amount: Decimal
    price: Decimal
    created_at: float

    @property
    def notional(self) -> Decimal:
        return self.amount * self.price


STATE_TO_MODE = {
    MarketState.NORMAL: MMMode.NEUTRAL,
    MarketState.UP_TREND: MMMode.LONG_BIAS,
    MarketState.DOWN_TREND: MMMode.SHORT_BIAS,
    MarketState.HIGH_VOL: MMMode.DEFENSIVE,
    MarketState.EXTREME: MMMode.PAUSED,
}


@dataclass(frozen=True)
class QuotePlan:
    bid_price: Decimal | None
    ask_price: Decimal | None
    amount: Decimal
    fair_value: Decimal
    market_state: MarketState
    mm_mode: MMMode
    inventory_mode: InventoryMode
    reason: str = "READY"


def classify_market_state(
    direction_bps: Decimal,
    volatility_bps: Decimal,
    derive_spread_bps: Decimal,
    *,
    trend_threshold_bps: Decimal,
    high_volatility_bps: Decimal,
    extreme_volatility_bps: Decimal,
    extreme_spread_bps: Decimal,
) -> MarketState:
    if volatility_bps >= extreme_volatility_bps or derive_spread_bps >= extreme_spread_bps:
        return MarketState.EXTREME
    if volatility_bps >= high_volatility_bps:
        return MarketState.HIGH_VOL
    if direction_bps >= trend_threshold_bps:
        return MarketState.UP_TREND
    if direction_bps <= -trend_threshold_bps:
        return MarketState.DOWN_TREND
    return MarketState.NORMAL


def classify_inventory(position_notional: Decimal, cap: Decimal, one_sided_ratio: Decimal) -> InventoryMode:
    if cap <= ZERO:
        return InventoryMode.FLAT
    ratio = position_notional / cap
    if ratio >= one_sided_ratio:
        return InventoryMode.ASK_ONLY
    if ratio <= -one_sided_ratio:
        return InventoryMode.BID_ONLY
    if ratio > ZERO:
        return InventoryMode.LONG_SKEW
    if ratio < ZERO:
        return InventoryMode.SHORT_SKEW
    return InventoryMode.FLAT


def signed_fill_amount(side: TradeType, amount: Decimal) -> Decimal:
    return amount if side == TradeType.BUY else -amount


def classify_fill_effect(current: Decimal, projected: Decimal) -> FillEffect:
    if projected == ZERO:
        return FillEffect.FLATTEN
    if current == ZERO or (current > ZERO) == (projected > ZERO):
        return FillEffect.REDUCE if abs(projected) < abs(current) else FillEffect.INCREASE_SAME_DIRECTION
    return FillEffect.FLIP_DIRECTION


def projected_position(current: Decimal, side: TradeType, amount: Decimal) -> tuple[Decimal, FillEffect]:
    projected = current + signed_fill_amount(side, amount)
    return projected, classify_fill_effect(current, projected)


def apply_state_hysteresis(
    current: MarketState,
    candidate: MarketState,
    candidate_since: float | None,
    now: float,
    minimum_seconds: float,
) -> tuple[MarketState, float | None]:
    if candidate == current:
        return current, None
    if candidate == MarketState.EXTREME:
        return candidate, None
    if candidate_since is None:
        return current, now
    if now - candidate_since >= minimum_seconds:
        return candidate, None
    return current, candidate_since


def calculate_fair_value(binance_mid: Decimal, basis_bps: Decimal, microprice_adjustment_bps: Decimal) -> Decimal:
    return binance_mid * (ONE + (basis_bps + microprice_adjustment_bps) / BPS)


def should_refresh(
    *,
    side: TradeType,
    current_price: Decimal,
    desired_price: Decimal,
    tick_size: Decimal,
    age_seconds: Decimal,
    minimum_residency_seconds: Decimal,
    deadband_bps: Decimal,
    fast_adverse: bool,
) -> tuple[bool, str]:
    if fast_adverse:
        return True, "FAST_ADVERSE_MOVE"
    if current_price == desired_price or abs(current_price - desired_price) < tick_size:
        return False, "TICK_AWARE_HOLD"
    if age_seconds < minimum_residency_seconds:
        return False, "MINIMUM_RESIDENCY"
    distance_bps = abs(desired_price / current_price - ONE) * BPS
    if distance_bps < deadband_bps:
        return False, "DEADBAND"
    favorable = (side == TradeType.BUY and desired_price > current_price) or (
        side == TradeType.SELL and desired_price < current_price
    )
    return True, "NORMAL_REFRESH_FAVORABLE" if favorable else "NORMAL_REFRESH"


class DeriveBinanceAdaptiveMMConfig(ControllerConfigBase):
    controller_name: str = "derive_binance_adaptive_mm"
    controller_type: str = "market_making"
    connector_name: str = Field(default="derive_perpetual")
    reference_connector_name: str = Field(default="binance_perpetual")
    trading_pair: str = Field(default="XRP-USDC")
    reference_trading_pair: str = Field(default="XRP-USDT")
    asset: str = Field(default="XRP")
    portfolio_id: str = Field(default="derive_xrp_link_800")
    position_mode: PositionMode = Field(default=PositionMode.ONEWAY)
    leverage: int = Field(default=1, ge=1, le=5)

    shadow_mode: bool = Field(default=True)
    mainnet_armed: bool = Field(default=False)
    manual_kill_switch: bool = Field(default=False)
    allow_position_flips: bool = Field(default=False)
    max_account_drawdown_quote: Decimal | None = Field(default=None, gt=0)

    total_amount_quote: Decimal = Field(default=Decimal("800"), gt=0)
    portfolio_capital_quote: Decimal = Field(default=Decimal("800"), gt=0)
    reserve_quote: Decimal = Field(default=Decimal("200"), ge=0)
    asset_cap_quote: Decimal = Field(default=Decimal("300"), gt=0)
    max_total_inventory_quote: Decimal = Field(default=Decimal("400"), gt=0)
    max_total_open_order_quote: Decimal = Field(default=Decimal("400"), gt=0)
    max_asset_inventory_quote: Decimal = Field(default=Decimal("180"), gt=0)
    max_asset_open_order_quote: Decimal = Field(default=Decimal("100"), gt=0)
    order_amount_quote: Decimal = Field(default=Decimal("25"), gt=0)

    binance_stale_seconds: Decimal = Field(default=Decimal("3"), gt=0)
    derive_stale_seconds: Decimal = Field(default=Decimal("3"), gt=0)
    binance_recovery_seconds: Decimal = Field(default=Decimal("3"), ge=0)
    book_depth_levels: int = Field(default=5, ge=1, le=20)
    basis_window: int = Field(default=120, ge=5, le=3600)
    direction_window_seconds: Decimal = Field(default=Decimal("10"), gt=0)
    volatility_window: int = Field(default=60, ge=5, le=600)
    trend_threshold_bps: Decimal = Field(default=Decimal("2"), gt=0)
    high_volatility_bps: Decimal = Field(default=Decimal("8"), gt=0)
    extreme_volatility_bps: Decimal = Field(default=Decimal("20"), gt=0)
    extreme_spread_bps: Decimal = Field(default=Decimal("80"), gt=0)
    state_hysteresis_seconds: Decimal = Field(default=Decimal("10"), ge=0)

    maker_fee_buffer_bps: Decimal = Field(default=Decimal("1"), ge=0)
    minimum_profit_buffer_bps: Decimal = Field(default=Decimal("2"), ge=0)
    volatility_buffer_multiplier: Decimal = Field(default=Decimal("0.5"), ge=0)
    latency_toxicity_buffer_bps: Decimal = Field(default=Decimal("1"), ge=0)
    direction_skew_bps: Decimal = Field(default=Decimal("1.5"), ge=0)
    inventory_skew_bps: Decimal = Field(default=Decimal("6"), ge=0)
    microprice_adjustment_max_bps: Decimal = Field(default=Decimal("0.5"), ge=0)
    one_sided_inventory_ratio: Decimal = Field(default=Decimal("0.70"), gt=0, le=1)

    normal_refresh_deadband_bps: Decimal = Field(default=Decimal("3"), gt=0)
    minimum_normal_quote_residency_seconds: Decimal = Field(default=Decimal("10"), ge=0)
    fast_adverse_move_bps: Decimal = Field(default=Decimal("5"), gt=0)
    fast_adverse_window_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    max_quote_mutations_per_minute: int = Field(default=30, ge=1, le=120)

    @field_validator("asset", mode="before")
    @classmethod
    def normalize_asset(cls, value: Any) -> str:
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_final_architecture(self):
        if self.asset not in {"XRP", "LINK"}:
            raise ValueError("asset must be XRP or LINK")
        if self.connector_name != "derive_perpetual":
            raise ValueError("execution connector must be derive_perpetual")
        if self.reference_connector_name != "binance_perpetual":
            raise ValueError("reference connector must be binance_perpetual")
        if self.trading_pair != f"{self.asset}-USDC" or self.reference_trading_pair != f"{self.asset}-USDT":
            raise ValueError("trading pairs must match the configured asset")
        if self.position_mode != PositionMode.ONEWAY:
            raise ValueError("Derive supports ONEWAY position mode only")
        if self.mainnet_armed and self.shadow_mode:
            raise ValueError("mainnet_armed requires shadow_mode=false")
        if self.total_amount_quote != self.portfolio_capital_quote:
            raise ValueError("native total_amount_quote must equal portfolio_capital_quote")
        usable = self.portfolio_capital_quote - self.reserve_quote
        if usable <= ZERO or self.asset_cap_quote > usable:
            raise ValueError("asset cap must fit inside capital after reserve")
        if self.max_asset_inventory_quote > self.asset_cap_quote:
            raise ValueError("inventory cap must fit inside asset cap")
        if self.max_asset_open_order_quote > self.asset_cap_quote:
            raise ValueError("open-order cap must fit inside asset cap")
        if self.order_amount_quote * 2 > self.max_asset_open_order_quote:
            raise ValueError("one bid plus one ask must fit the asset open-order cap")
        if self.high_volatility_bps >= self.extreme_volatility_bps:
            raise ValueError("high volatility threshold must be below extreme threshold")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        result = markets.add_or_update(self.execution_market_connector_name, self.trading_pair)
        if result is not None:
            markets = result
        # This public-data wrapper uses the native Binance perpetual order-book
        # tracker without requiring private Binance credentials.
        result = markets.add_or_update(self.reference_market_connector_name, self.reference_trading_pair)
        return result if result is not None else markets

    @property
    def reference_market_connector_name(self) -> str:
        return f"{self.reference_connector_name}_paper_trade"

    @property
    def execution_market_connector_name(self) -> str:
        return f"{self.connector_name}_paper_trade" if self.shadow_mode else self.connector_name

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Keep V2's live-account initializer away from public shadow wrappers."""
        data = super().model_dump(*args, **kwargs)
        if self.shadow_mode:
            # v2_with_controllers treats any connector name containing
            # "perpetual" as a live derivative, then applies these fields to
            # config.connector_name. In shadow mode that logical name is not a
            # registered market: its public paper-trade wrapper is. Suppressing
            # only these serialized fields avoids private account mutations and
            # lets the controllers start; the typed values remain available on
            # self for validation and executor construction. Live mode retains
            # Hummingbot's standard position-mode and leverage initialization.
            data.pop("position_mode", None)
            data.pop("leverage", None)
        return data


class DeriveBinanceAdaptiveMM(ControllerBase):
    """Single-asset controller; deploy XRP and LINK configs in the same bot."""

    _logger = None
    _portfolio: ClassVar[dict[str, dict[str, PortfolioSnapshot]]] = {}
    _reservations: ClassVar[dict[str, dict[tuple[str, str], PendingReservation]]] = {}
    _portfolio_caps: ClassVar[dict[str, dict[str, Decimal]]] = {}
    _portfolio_terms: ClassVar[dict[str, tuple[Decimal, Decimal]]] = {}
    _portfolio_lock: ClassVar[RLock] = RLock()
    _reservation_ttl_seconds: ClassVar[float] = 30.0

    @classmethod
    def logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: DeriveBinanceAdaptiveMMConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.processed_data: dict[str, Any] = {}
        self._basis = deque(maxlen=config.basis_window)
        self._reference_history = deque(maxlen=max(config.volatility_window + 1, 10))
        self._returns = deque(maxlen=config.volatility_window)
        self._book_uids: dict[str, Any] = {}
        self._book_updated_at: dict[str, float] = {}
        self._reference_recovered_at: float | None = None
        self._market_state = MarketState.NORMAL
        self._candidate_state: MarketState | None = None
        self._candidate_since: float | None = None
        self._plan: QuotePlan | None = None
        self._last_reference_mid: Decimal | None = None
        self._fast_move_bps = ZERO
        self._action_timestamps: deque[float] = deque()
        self._action_events: deque[tuple[float, str]] = deque()
        self._pending_stops: set[str] = set()
        self._unexpected_active: list[Any] = []
        self._last_action: dict[str, str] = {"bid": "NONE", "ask": "NONE"}
        self._fill_observations: dict[str, dict[str, Any]] = {}
        self._markout_30s: list[Decimal] = []
        self._markout_60s: list[Decimal] = []
        self._peak_pnl = ZERO
        self._peak_account_equity: Decimal | None = None
        self._peak_account_collateral: Decimal | None = None
        self._price_tick: Decimal | None = None
        self._amount_quantizer: Any | None = None
        self._trading_rule: Any | None = None
        self._started_at = self.market_data_provider.time()
        terms = (config.portfolio_capital_quote, config.reserve_quote)
        existing_terms = self._portfolio_terms.setdefault(config.portfolio_id, terms)
        if existing_terms != terms:
            raise ValueError("controllers sharing a portfolio_id must use identical capital and reserve")
        caps = self._portfolio_caps.setdefault(config.portfolio_id, {})
        caps[config.asset] = config.asset_cap_quote
        if sum(caps.values(), ZERO) > config.portfolio_capital_quote - config.reserve_quote:
            raise ValueError("shared asset caps exceed portfolio capital after reserve")

    def _read_book(self, connector: str, pair: str, now: float) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal, bool]:
        book = self.market_data_provider.get_order_book(connector, pair)
        bids = list(islice(book.bid_entries(), self.config.book_depth_levels))
        asks = list(islice(book.ask_entries(), self.config.book_depth_levels))
        if not bids or not asks:
            raise ValueError("empty_order_book")
        bid, ask = Decimal(str(bids[0].price)), Decimal(str(asks[0].price))
        bid_amount, ask_amount = Decimal(str(bids[0].amount)), Decimal(str(asks[0].amount))
        if bid <= ZERO or ask <= bid:
            raise ValueError("invalid_order_book")
        # PaperTradeExchange exposes a composite book whose top-level
        # last_diff_uid can remain fixed while its native source rows update.
        # Include row update IDs and observable BBO/size in the freshness
        # signature so real source changes reset the gate without inventing a
        # heartbeat or forward-filling a price.
        signature = (
            getattr(book, "last_diff_uid", None),
            getattr(bids[0], "update_id", None),
            getattr(asks[0], "update_id", None),
            bid,
            ask,
            bid_amount,
            ask_amount,
        )
        key = f"{connector}:{pair}"
        changed = signature != self._book_uids.get(key)
        if changed:
            self._book_uids[key] = signature
            self._book_updated_at[key] = now
        updated_at = self._book_updated_at.setdefault(key, now)
        return bid, ask, bid_amount, ask_amount, Decimal(str(updated_at)), changed

    @staticmethod
    def _mid(bid: Decimal, ask: Decimal) -> Decimal:
        return (bid + ask) / Decimal("2")

    def _position_amount(self) -> Decimal:
        connector = self.market_data_provider.get_connector(self.config.execution_market_connector_name)
        total = ZERO
        for position in (getattr(connector, "account_positions", {}) or {}).values():
            if getattr(position, "trading_pair", "") == self.config.trading_pair:
                total += Decimal(str(getattr(position, "amount", ZERO)))
        return total

    async def _execution_rules_and_quantizer(self) -> tuple[Any, Any]:
        """Return Derive's native rule and quantizer, including in shadow."""
        market_name = self.config.execution_market_connector_name
        if self.config.shadow_mode:
            paper = self.market_data_provider.get_connector(market_name)
            data_source = getattr(getattr(paper, "order_book_tracker", None), "data_source", None)
            native_connector = getattr(data_source, "_connector", None)
            if native_connector is not None:
                rules = getattr(native_connector, "trading_rules", {})
                if self.config.trading_pair not in rules:
                    await native_connector._update_trading_rules()
                    rules = native_connector.trading_rules
                return rules[self.config.trading_pair], native_connector
        # Test providers and live mode expose this through MarketDataProvider.
        return (
            self.market_data_provider.get_trading_rules(market_name, self.config.trading_pair),
            self.market_data_provider,
        )

    def _active(self) -> dict[str, Any]:
        result = {}
        self._unexpected_active = []
        for executor in self.executors_info:
            if not bool(getattr(executor, "is_active", False)):
                continue
            level = str(getattr(getattr(executor, "config", None), "level_id", ""))
            if level in {"bid", "ask"} and level not in result:
                result[level] = executor
            else:
                self._unexpected_active.append(executor)
        return result

    def _mutations_last_minute(self, now: float) -> int:
        while self._action_timestamps and now - self._action_timestamps[0] >= 60:
            self._action_timestamps.popleft()
        while self._action_events and now - self._action_events[0][0] >= 60:
            self._action_events.popleft()
        return len(self._action_timestamps)

    def _record_mutation(self, now: float, action: str) -> None:
        self._action_timestamps.append(now)
        self._action_events.append((now, action))

    @classmethod
    def _reset_shared_state_for_tests(cls) -> None:
        with cls._portfolio_lock:
            cls._portfolio.clear()
            cls._reservations.clear()
            cls._portfolio_caps.clear()
            cls._portfolio_terms.clear()

    def _prune_reservations(self, now: float, active: dict[str, Any]) -> None:
        reservations = self._reservations.setdefault(self.config.portfolio_id, {})
        for key, reservation in list(reservations.items()):
            # Only the owning controller can reconcile its reservation. If an
            # asset controller stalls, peers must retain its pending exposure
            # and fail closed instead of expiring risk they cannot observe.
            if reservation.controller_id != self.config.id:
                continue
            if reservation.level in active or now - reservation.created_at >= self._reservation_ttl_seconds:
                reservations.pop(key, None)

    def _portfolio_totals(self) -> tuple[Decimal, Decimal]:
        values = self._portfolio.setdefault(self.config.portfolio_id, {})
        reservations = self._reservations.setdefault(self.config.portfolio_id, {})
        return (
            sum((abs(snapshot.position_notional) for snapshot in values.values()), ZERO),
            sum((snapshot.open_order_notional for snapshot in values.values()), ZERO)
            + sum((reservation.notional for reservation in reservations.values()), ZERO),
        )

    def _publish_portfolio(
        self,
        position_amount: Decimal,
        mark_price: Decimal,
        active: dict[str, Any],
    ) -> PortfolioSnapshot:
        def amount_and_price(level: str) -> tuple[Decimal, Decimal]:
            config = getattr(active.get(level), "config", None)
            return (
                Decimal(str(getattr(config, "amount", ZERO) or ZERO)),
                Decimal(str(getattr(config, "price", ZERO) or ZERO)),
            )

        bid_amount, bid_price = amount_and_price("bid")
        ask_amount, ask_price = amount_and_price("ask")
        snapshot = PortfolioSnapshot(
            position_amount=position_amount,
            mark_price=mark_price,
            open_bid_amount=bid_amount,
            open_bid_price=bid_price,
            open_ask_amount=ask_amount,
            open_ask_price=ask_price,
        )
        self._portfolio.setdefault(self.config.portfolio_id, {})[self.config.asset] = snapshot
        return snapshot

    def _native_feed_age(self, connector_name: str, pair: str) -> tuple[Decimal | None, str]:
        """Read the 2.16.0 tracker message clock; never infer transport health from price."""
        try:
            connector = self.market_data_provider.get_connector(connector_name)
            tracker = connector.order_book_tracker
            pair_metrics = tracker.metrics.per_pair_metrics.get(pair)
            if pair_metrics is None:
                return None, "BBO_CHANGE_FALLBACK"
            received_at = max(
                float(getattr(pair_metrics, "last_diff_timestamp", 0) or 0),
                float(getattr(pair_metrics, "last_snapshot_timestamp", 0) or 0),
            )
            if received_at <= 0:
                return None, "BBO_CHANGE_FALLBACK"
            return Decimal(str(max(0.0, time.perf_counter() - received_at))), "NATIVE_MESSAGE_TIMESTAMP"
        except (AttributeError, KeyError, TypeError, ValueError):
            return None, "BBO_CHANGE_FALLBACK"

    def _account_risk(self, current_mid: Decimal) -> dict[str, Decimal | None]:
        """Return native account values when authenticated state exists, otherwise N/A.

        Hummingbot 2.16.0 exposes collateral balances and unrealized PnL but no
        reliable account realized-PnL field. We therefore leave realized PnL
        unset instead of deriving it from strategy executors.
        """
        empty = {
            "account_realized_pnl": None,
            "account_unrealized_pnl": None,
            "account_equity": None,
            "account_collateral_balance": None,
            "available_collateral": None,
            "account_gross_position_exposure": None,
            "account_net_position_exposure": None,
            "account_drawdown": None,
            "collateral_balance_drawdown": None,
        }
        if self.config.shadow_mode:
            return empty
        try:
            connector = self.market_data_provider.get_connector(self.config.execution_market_connector_name)
            quote_asset = self.config.trading_pair.rsplit("-", 1)[1]
            collateral = Decimal(str(connector.get_balance(quote_asset)))
            available = Decimal(str(connector.get_available_balance(quote_asset)))
            raw_equity = getattr(connector, "account_equity", None)
            equity = Decimal(str(raw_equity)) if raw_equity is not None else None
            positions = list((getattr(connector, "account_positions", {}) or {}).values())
            unrealized = sum(
                (Decimal(str(getattr(position, "unrealized_pnl", ZERO) or ZERO)) for position in positions), ZERO
            )
            signed_exposures: list[Decimal] = []
            for position in positions:
                amount = Decimal(str(getattr(position, "amount", ZERO) or ZERO))
                pair = str(getattr(position, "trading_pair", ""))
                if not pair or amount == ZERO:
                    continue
                mark = current_mid if pair == self.config.trading_pair else Decimal(str(connector.get_mid_price(pair)))
                signed_exposures.append(amount * mark)
            gross = sum((abs(exposure) for exposure in signed_exposures), ZERO)
            net = sum(signed_exposures, ZERO)
            if equity is not None:
                self._peak_account_equity = (
                    equity if self._peak_account_equity is None else max(self._peak_account_equity, equity)
                )
            self._peak_account_collateral = (
                collateral
                if self._peak_account_collateral is None
                else max(self._peak_account_collateral, collateral)
            )
            return {
                "account_realized_pnl": None,
                "account_unrealized_pnl": unrealized,
                "account_equity": equity,
                "account_collateral_balance": collateral,
                "available_collateral": available,
                "account_gross_position_exposure": gross,
                "account_net_position_exposure": net,
                "account_drawdown": self._peak_account_equity - equity if equity is not None else None,
                "collateral_balance_drawdown": self._peak_account_collateral - collateral,
            }
        except (AttributeError, KeyError, TypeError, ValueError):
            return empty

    def _update_markouts(self, now: float, derive_mid: Decimal) -> tuple[Decimal | None, Decimal | None]:
        for executor in self.executors_info:
            executor_id = str(getattr(executor, "id", ""))
            custom = getattr(executor, "custom_info", {}) or {}
            filled_base = Decimal(str(custom.get("executed_amount_base", ZERO) or ZERO))
            fill_price = Decimal(str(custom.get("average_executed_price", ZERO) or ZERO))
            if not executor_id or filled_base <= ZERO or fill_price <= ZERO:
                continue
            previous = self._fill_observations.get(executor_id)
            if previous is None or filled_base > previous["filled_base"]:
                update_time = custom.get("order_last_update")
                try:
                    fill_time = float(update_time)
                except (TypeError, ValueError):
                    fill_time = now
                if fill_time <= 0 or fill_time > now + 60:
                    fill_time = now
                self._fill_observations[executor_id] = {
                    "filled_base": filled_base,
                    "fill_price": fill_price,
                    "fill_time": fill_time,
                    "side": getattr(getattr(executor, "config", None), "side", None),
                    "done_30": False,
                    "done_60": False,
                }
        for observation in self._fill_observations.values():
            age = now - observation["fill_time"]
            side = observation["side"]
            fill_price = observation["fill_price"]
            if side == TradeType.BUY:
                markout = (derive_mid / fill_price - ONE) * BPS
            elif side == TradeType.SELL:
                markout = (fill_price / derive_mid - ONE) * BPS
            else:
                continue
            if age >= 30 and not observation["done_30"]:
                self._markout_30s.append(markout)
                observation["done_30"] = True
            if age >= 60 and not observation["done_60"]:
                self._markout_60s.append(markout)
                observation["done_60"] = True
        markout_30 = sum(self._markout_30s, ZERO) / len(self._markout_30s) if self._markout_30s else None
        markout_60 = sum(self._markout_60s, ZERO) / len(self._markout_60s) if self._markout_60s else None
        return markout_30, markout_60

    async def update_processed_data(self):
        now = self.market_data_provider.time()
        operational = OperationalState.SHADOW if self.config.shadow_mode else OperationalState.LIVE_DISARMED
        try:
            dbid, dask, _, _, derive_updated, _ = self._read_book(
                self.config.execution_market_connector_name, self.config.trading_pair, now
            )
            bbid, bask, bsize, asize, binance_updated, reference_changed = self._read_book(
                self.config.reference_market_connector_name, self.config.reference_trading_pair, now
            )
            derive_bbo_change_age = Decimal(str(now)) - derive_updated
            binance_bbo_change_age = Decimal(str(now)) - binance_updated
            derive_native_age, derive_freshness_source = self._native_feed_age(
                self.config.execution_market_connector_name, self.config.trading_pair
            )
            binance_native_age, binance_freshness_source = self._native_feed_age(
                self.config.reference_market_connector_name, self.config.reference_trading_pair
            )
            derive_age = derive_native_age if derive_native_age is not None else derive_bbo_change_age
            binance_age = binance_native_age if binance_native_age is not None else binance_bbo_change_age
            # Preserve the last observed public BBO and measured ages even when
            # a freshness gate pauses quoting. Operators still need the source
            # evidence that explains a fail-closed state.
            self.processed_data.update(
                {
                    "asset": self.config.asset,
                    "derive_bbo": [dbid, dask],
                    "derive_feed_age_seconds": derive_age,
                    "derive_age_seconds": derive_age,
                    "derive_bbo_change_age_seconds": derive_bbo_change_age,
                    "derive_freshness_source": derive_freshness_source,
                    "binance_bbo": [bbid, bask],
                    "binance_feed_age_seconds": binance_age,
                    "binance_age_seconds": binance_age,
                    "binance_bbo_change_age_seconds": binance_bbo_change_age,
                    "binance_freshness_source": binance_freshness_source,
                    "updated_at": now,
                }
            )
            if derive_age > self.config.derive_stale_seconds:
                operational = OperationalState.DERIVE_PAUSED
                raise RuntimeError("DERIVE_STALE")
            if binance_age > self.config.binance_stale_seconds:
                self._reference_recovered_at = None
                operational = OperationalState.REFERENCE_PAUSED
                raise RuntimeError("BINANCE_STALE")
            if reference_changed and self._reference_recovered_at is None:
                self._reference_recovered_at = now
            if self._reference_recovered_at is None or now - self._reference_recovered_at < float(self.config.binance_recovery_seconds):
                operational = OperationalState.REFERENCE_PAUSED
                raise RuntimeError("BINANCE_RECOVERY")

            derive_mid, binance_mid = self._mid(dbid, dask), self._mid(bbid, bask)
            basis = (derive_mid / binance_mid - ONE) * BPS
            self._basis.append(basis)
            causal_basis = Decimal(str(statistics.median(self._basis)))
            microprice = (bask * bsize + bbid * asize) / (bsize + asize) if bsize + asize > ZERO else binance_mid
            micro_bps = (microprice / binance_mid - ONE) * BPS
            micro_bps = max(-self.config.microprice_adjustment_max_bps, min(self.config.microprice_adjustment_max_bps, micro_bps))

            if self._last_reference_mid and reference_changed:
                self._returns.append(float(binance_mid / self._last_reference_mid - ONE))
            self._last_reference_mid = binance_mid
            self._reference_history.append((now, binance_mid))
            cutoff = now - float(self.config.direction_window_seconds)
            anchor = next((price for stamp, price in self._reference_history if stamp >= cutoff), self._reference_history[0][1])
            direction_bps = (binance_mid / anchor - ONE) * BPS if anchor > ZERO else ZERO
            volatility_bps = Decimal(str(statistics.pstdev(self._returns) * 10000)) if len(self._returns) >= 2 else ZERO
            spread_bps = (dask - dbid) / derive_mid * BPS
            candidate = classify_market_state(
                direction_bps,
                volatility_bps,
                spread_bps,
                trend_threshold_bps=self.config.trend_threshold_bps,
                high_volatility_bps=self.config.high_volatility_bps,
                extreme_volatility_bps=self.config.extreme_volatility_bps,
                extreme_spread_bps=self.config.extreme_spread_bps,
            )
            if candidate != self._candidate_state:
                self._candidate_state, self._candidate_since = candidate, None
            self._market_state, self._candidate_since = apply_state_hysteresis(
                self._market_state,
                candidate,
                self._candidate_since,
                now,
                float(self.config.state_hysteresis_seconds),
            )
            if self._reference_history:
                fast_cutoff = now - float(self.config.fast_adverse_window_seconds)
                fast_anchor = next((price for stamp, price in self._reference_history if stamp >= fast_cutoff), self._reference_history[0][1])
                self._fast_move_bps = (binance_mid / fast_anchor - ONE) * BPS

            position = self._position_amount()
            position_notional = position * derive_mid
            inventory_mode = classify_inventory(position_notional, self.config.max_asset_inventory_quote, self.config.one_sided_inventory_ratio)
            mode = STATE_TO_MODE[self._market_state]
            if self.config.manual_kill_switch:
                mode = MMMode.PAUSED
                operational = OperationalState.RISK_PAUSED
            fair = calculate_fair_value(binance_mid, causal_basis, micro_bps)
            vol_buffer = volatility_bps * self.config.volatility_buffer_multiplier
            edge = self.config.maker_fee_buffer_bps + self.config.minimum_profit_buffer_bps + vol_buffer + self.config.latency_toxicity_buffer_bps
            direction_skew = self.config.direction_skew_bps if mode == MMMode.LONG_BIAS else -self.config.direction_skew_bps if mode == MMMode.SHORT_BIAS else ZERO
            inventory_ratio = max(Decimal("-1"), min(Decimal("1"), position_notional / self.config.max_asset_inventory_quote))
            reservation = fair * (ONE + (direction_skew - inventory_ratio * self.config.inventory_skew_bps) / BPS)
            raw_bid = min(dbid, reservation * (ONE - edge / BPS))
            raw_ask = max(dask, reservation * (ONE + edge / BPS))
            execution_market = self.config.execution_market_connector_name
            trading_rules, quantizer = await self._execution_rules_and_quantizer()
            self._trading_rule = trading_rules
            self._amount_quantizer = quantizer
            self._price_tick = Decimal(str(trading_rules.min_price_increment))
            if quantizer is self.market_data_provider:
                bid_price = quantizer.quantize_order_price(execution_market, self.config.trading_pair, raw_bid)
                ask_price = quantizer.quantize_order_price(execution_market, self.config.trading_pair, raw_ask)
                amount = quantizer.quantize_order_amount(
                    execution_market, self.config.trading_pair, self.config.order_amount_quote / derive_mid
                )
            else:
                bid_price = quantizer.quantize_order_price(self.config.trading_pair, raw_bid)
                ask_price = quantizer.quantize_order_price(self.config.trading_pair, raw_ask)
                amount = quantizer.quantize_order_amount(self.config.trading_pair, self.config.order_amount_quote / derive_mid)
            minimum_amount = Decimal(str(getattr(trading_rules, "min_order_size", ZERO) or ZERO))
            minimum_notional = max(
                Decimal(str(getattr(trading_rules, "min_notional_size", ZERO) or ZERO)),
                Decimal(str(getattr(trading_rules, "min_order_value", ZERO) or ZERO)),
            )
            size_reason = "READY"
            if amount < minimum_amount or amount * derive_mid < minimum_notional:
                amount = ZERO
                size_reason = "ORDER_BELOW_NATIVE_MINIMUM"
            if inventory_mode == InventoryMode.ASK_ONLY:
                bid_price = None
            elif inventory_mode == InventoryMode.BID_ONLY:
                ask_price = None
            if mode == MMMode.PAUSED or amount <= ZERO:
                bid_price = ask_price = None
            self._plan = QuotePlan(bid_price, ask_price, amount, fair, self._market_state, mode, inventory_mode, size_reason)

            active = self._active()
            with self._portfolio_lock:
                self._prune_reservations(now, active)
                own_snapshot = self._publish_portfolio(position, derive_mid, active)
                total_inventory, total_orders = self._portfolio_totals()
                previews: dict[str, tuple[Decimal, str, Decimal, FillEffect]] = {}
                for level, side, price in (
                    ("bid", TradeType.BUY, self._plan.bid_price),
                    ("ask", TradeType.SELL, self._plan.ask_price),
                ):
                    if price is None:
                        previews[level] = (ZERO, self._plan.reason, position, classify_fill_effect(position, position))
                    else:
                        previews[level] = self._risk_adjusted_amount(
                            level=level,
                            side=side,
                            price=price,
                            desired_amount=amount,
                            now=now,
                            reserve=False,
                        )
            markout_30, markout_60 = self._update_markouts(now, derive_mid)
            account_risk = self._account_risk(derive_mid)
            account_drawdown = account_risk["account_drawdown"]
            if (
                own_snapshot.open_order_notional + abs(position_notional) > self.config.asset_cap_quote
                or total_inventory > self.config.max_total_inventory_quote
                or total_orders > self.config.max_total_open_order_quote
            ):
                operational = OperationalState.RISK_PAUSED
                self._plan = QuotePlan(None, None, amount, fair, self._market_state, MMMode.PAUSED, inventory_mode, "PORTFOLIO_LIMIT")
            elif (
                self.config.max_account_drawdown_quote is not None
                and account_drawdown is not None
                and account_drawdown >= self.config.max_account_drawdown_quote
            ):
                operational = OperationalState.RISK_PAUSED
                self._plan = QuotePlan(None, None, amount, fair, self._market_state, MMMode.PAUSED, inventory_mode, "ACCOUNT_DRAWDOWN_LIMIT")
            elif not self.config.shadow_mode and self.config.mainnet_armed:
                operational = OperationalState.LIVE_ARMED

            self.processed_data = {
                "asset": self.config.asset,
                "operational_state": operational.value,
                "derive_bbo": [dbid, dask],
                "derive_spread_bps": spread_bps,
                "derive_feed_age_seconds": derive_age,
                "derive_age_seconds": derive_age,
                "derive_bbo_change_age_seconds": derive_bbo_change_age,
                "derive_freshness_source": derive_freshness_source,
                "derive_freshness_state": "HEALTHY" if derive_age <= self.config.derive_stale_seconds else "STALE",
                "binance_bbo": [bbid, bask],
                "binance_feed_age_seconds": binance_age,
                "binance_age_seconds": binance_age,
                "binance_bbo_change_age_seconds": binance_bbo_change_age,
                "binance_freshness_source": binance_freshness_source,
                "binance_freshness_state": "HEALTHY" if binance_age <= self.config.binance_stale_seconds else "STALE",
                "binance_fair_value": fair,
                "basis_bps": causal_basis,
                "direction_bps": direction_bps,
                "volatility_bps": volatility_bps,
                "market_state": self._market_state.value,
                "mm_mode": self._plan.mm_mode.value,
                "inventory_mode": inventory_mode.value,
                "position_amount": position,
                "position_notional": position_notional,
                "current_position_amount": position,
                "current_position_notional": position_notional,
                "desired_order_amount": amount,
                "risk_adjusted_bid_order_amount": previews["bid"][0],
                "risk_adjusted_ask_order_amount": previews["ask"][0],
                "projected_position_amount_if_bid_fills": previews["bid"][2],
                "projected_position_notional_if_bid_fills": previews["bid"][2]
                * max(self._plan.bid_price or ZERO, derive_mid),
                "projected_position_amount_if_ask_fills": previews["ask"][2],
                "projected_position_notional_if_ask_fills": previews["ask"][2]
                * max(self._plan.ask_price or ZERO, derive_mid),
                "bid_fill_effect": previews["bid"][3].value,
                "ask_fill_effect": previews["ask"][3].value,
                "asset_inventory_limit": self.config.max_asset_inventory_quote,
                "portfolio_inventory_limit": self.config.max_total_inventory_quote,
                "asset_cap_quote": self.config.asset_cap_quote,
                "bid_size_block_reason": None if previews["bid"][0] > ZERO else previews["bid"][1],
                "ask_size_block_reason": None if previews["ask"][0] > ZERO else previews["ask"][1],
                "desired_bid": self._plan.bid_price,
                "desired_ask": self._plan.ask_price,
                "order_amount": amount,
                "risk_adjusted_order_amount": {"bid": previews["bid"][0], "ask": previews["ask"][0]},
                "fast_move_bps": self._fast_move_bps,
                "portfolio_inventory": total_inventory,
                "portfolio_open_orders": total_orders,
                "markout_30s_bps": markout_30,
                "markout_60s_bps": markout_60,
                "block_reason": self._plan.reason,
                "derive_snapshot_compatibility_active": DERIVE_SNAPSHOT_RACE_COMPATIBILITY_ACTIVE,
                **account_risk,
                "updated_at": now,
            }
        except Exception as exc:
            if operational not in {
                OperationalState.REFERENCE_PAUSED,
                OperationalState.DERIVE_PAUSED,
                OperationalState.RISK_PAUSED,
            }:
                operational = OperationalState.ERROR
            self._plan = None
            self.processed_data.update(
                {
                    "asset": self.config.asset,
                    "operational_state": operational.value,
                    "mm_mode": MMMode.PAUSED.value,
                    "desired_bid": None,
                    "desired_ask": None,
                    "block_reason": str(exc),
                    "updated_at": now,
                }
            )

    def _stop(self, executor: Any, now: float, reason: str) -> StopExecutorAction | None:
        executor_id = str(getattr(executor, "id", ""))
        if not executor_id or executor_id in self._pending_stops:
            return None
        self._pending_stops.add(executor_id)
        mutation = "replace" if reason in {"FAST_ADVERSE_MOVE", "NORMAL_REFRESH", "NORMAL_REFRESH_FAVORABLE"} else "cancel"
        self._record_mutation(now, mutation)
        level = str(getattr(getattr(executor, "config", None), "level_id", ""))
        self._last_action[level] = reason
        return StopExecutorAction(controller_id=self.config.id, executor_id=executor_id, keep_position=True)

    def _quantize_amount(self, amount: Decimal) -> Decimal:
        if amount <= ZERO or self._amount_quantizer is None:
            return ZERO
        market_name = self.config.execution_market_connector_name
        if self._amount_quantizer is self.market_data_provider:
            result = self._amount_quantizer.quantize_order_amount(market_name, self.config.trading_pair, amount)
        else:
            result = self._amount_quantizer.quantize_order_amount(self.config.trading_pair, amount)
        return min(amount, Decimal(str(result)))

    @staticmethod
    def _reservation_amounts(
        reservations: dict[tuple[str, str], PendingReservation], asset_controller_ids: set[str]
    ) -> tuple[Decimal, Decimal, Decimal]:
        bids = asks = notional = ZERO
        for reservation in reservations.values():
            if reservation.controller_id not in asset_controller_ids:
                continue
            notional += reservation.notional
            if reservation.side == TradeType.BUY:
                bids += reservation.amount
            else:
                asks += reservation.amount
        return bids, asks, notional

    def _portfolio_projected_gross(
        self,
        candidate_side: TradeType,
        candidate_amount: Decimal,
        candidate_price: Decimal,
    ) -> Decimal:
        snapshots = self._portfolio.setdefault(self.config.portfolio_id, {})
        reservations = self._reservations.setdefault(self.config.portfolio_id, {})
        gross = ZERO
        for asset, snapshot in snapshots.items():
            asset_reservations = [reservation for reservation in reservations.values() if reservation.asset == asset]
            ids = {reservation.controller_id for reservation in asset_reservations}
            if asset == self.config.asset:
                ids.add(self.config.id)
            pending_bids, pending_asks, _ = self._reservation_amounts(reservations, ids)
            active_bid = snapshot.open_bid_amount
            active_ask = snapshot.open_ask_amount
            if asset == self.config.asset:
                if candidate_side == TradeType.BUY:
                    pending_bids += candidate_amount
                else:
                    pending_asks += candidate_amount
            conservative_price = max(
                snapshot.mark_price,
                snapshot.open_bid_price,
                snapshot.open_ask_price,
                *(reservation.price for reservation in asset_reservations),
                candidate_price if asset == self.config.asset else ZERO,
            )
            worst = max(
                abs(snapshot.position_amount * snapshot.mark_price),
                abs((snapshot.position_amount + active_bid + pending_bids) * conservative_price),
                abs((snapshot.position_amount - active_ask - pending_asks) * conservative_price),
            )
            gross += worst
        return gross

    def _risk_adjusted_amount(
        self,
        *,
        level: str,
        side: TradeType,
        price: Decimal,
        desired_amount: Decimal,
        now: float,
        reserve: bool,
    ) -> tuple[Decimal, str, Decimal, FillEffect]:
        """Size one quote against signed post-fill and shared portfolio risk.

        Quote price is the conservative valuation price: it is the exact price
        at which the proposed maker fill would enter inventory. Existing
        position exposure remains marked at the current Derive midpoint.
        """
        snapshots = self._portfolio.setdefault(self.config.portfolio_id, {})
        own = snapshots.get(self.config.asset)
        if own is None or self._trading_rule is None:
            return ZERO, "RISK_STATE_UNAVAILABLE", ZERO, FillEffect.INCREASE_SAME_DIRECTION
        reservations = self._reservations.setdefault(self.config.portfolio_id, {})
        risk_price = max(price, own.mark_price)
        own_ids = {self.config.id}
        pending_bid, pending_ask, pending_notional = self._reservation_amounts(reservations, own_ids)
        same_side_pending = pending_bid if side == TradeType.BUY else pending_ask
        same_side_active = own.open_bid_amount if side == TradeType.BUY else own.open_ask_amount
        starting_amount = own.position_amount + signed_fill_amount(side, same_side_active + same_side_pending)
        allowed = desired_amount
        limiting_reason = "READY"

        def apply_limit(value: Decimal, reason: str) -> None:
            nonlocal allowed, limiting_reason
            if value < allowed:
                allowed = value
                limiting_reason = reason

        effect_at_desired = classify_fill_effect(own.position_amount, own.position_amount + signed_fill_amount(side, desired_amount))
        controlled_flip_allowed = self.config.allow_position_flips and self._market_state == MarketState.NORMAL
        if not controlled_flip_allowed and effect_at_desired == FillEffect.FLIP_DIRECTION:
            apply_limit(abs(own.position_amount), "FLIP_BLOCKED_AT_FLATTEN")

        # Asset inventory and projected portfolio inventory are monotonic for
        # inventory-increasing orders. A reducing/flattening order is not cut by
        # these two caps, though it remains subject to capital/open-order caps.
        increasing = starting_amount == ZERO or (starting_amount > ZERO and side == TradeType.BUY) or (
            starting_amount < ZERO and side == TradeType.SELL
        )
        if increasing:
            inventory_residual = max(ZERO, self.config.max_asset_inventory_quote / risk_price - abs(starting_amount))
            apply_limit(inventory_residual, "PROJECTED_ASSET_INVENTORY_LIMIT")
            portfolio_without = self._portfolio_projected_gross(side, ZERO, price)
            portfolio_residual = max(ZERO, self.config.max_total_inventory_quote - portfolio_without) / risk_price
            apply_limit(portfolio_residual, "PROJECTED_PORTFOLIO_INVENTORY_LIMIT")

        current_orders = sum((snapshot.open_order_notional for snapshot in snapshots.values()), ZERO) + sum(
            (reservation.notional for reservation in reservations.values()), ZERO
        )
        own_risk = abs(own.position_notional) + own.open_order_notional + pending_notional
        apply_limit(max(ZERO, self.config.asset_cap_quote - own_risk) / risk_price, "ASSET_CAP_LIMIT")
        apply_limit(
            max(ZERO, self.config.max_asset_open_order_quote - own.open_order_notional - pending_notional) / risk_price,
            "ASSET_OPEN_ORDER_LIMIT",
        )
        apply_limit(
            max(ZERO, self.config.max_total_open_order_quote - current_orders) / risk_price,
            "PORTFOLIO_OPEN_ORDER_LIMIT",
        )
        total_inventory, _ = self._portfolio_totals()
        usable_capital = self.config.portfolio_capital_quote - self.config.reserve_quote
        apply_limit(
            max(ZERO, usable_capital - total_inventory - current_orders) / risk_price,
            "PORTFOLIO_USABLE_CAPITAL_LIMIT",
        )
        amount = self._quantize_amount(allowed)
        minimum_amount = Decimal(str(getattr(self._trading_rule, "min_order_size", ZERO) or ZERO))
        minimum_notional = max(
            Decimal(str(getattr(self._trading_rule, "min_notional_size", ZERO) or ZERO)),
            Decimal(str(getattr(self._trading_rule, "min_order_value", ZERO) or ZERO)),
        )
        if amount < minimum_amount or amount * price < minimum_notional:
            projected, effect = projected_position(own.position_amount, side, ZERO)
            return ZERO, limiting_reason if limiting_reason != "READY" else "ORDER_BELOW_NATIVE_MINIMUM", projected, effect
        projected, effect = projected_position(own.position_amount, side, amount)
        if abs(projected * risk_price) > self.config.max_asset_inventory_quote:
            return ZERO, "PROJECTED_ASSET_INVENTORY_LIMIT", own.position_amount, effect
        projected_gross = self._portfolio_projected_gross(side, amount, price)
        if projected_gross > self.config.max_total_inventory_quote:
            return ZERO, "PROJECTED_PORTFOLIO_INVENTORY_LIMIT", own.position_amount, effect
        reason = limiting_reason if amount < desired_amount else "READY"
        if reserve:
            reservations[(self.config.id, level)] = PendingReservation(
                controller_id=self.config.id,
                asset=self.config.asset,
                level=level,
                side=side,
                amount=amount,
                price=price,
                created_at=now,
            )
        return amount, reason, projected, effect

    def determine_executor_actions(self) -> list[ExecutorAction]:
        now = self.market_data_provider.time()
        active = self._active()
        position_amount = Decimal(str(self.processed_data.get("current_position_amount", ZERO) or ZERO))
        derive_bbo = self.processed_data.get("derive_bbo") or [ZERO, ZERO]
        mark_price = (Decimal(str(derive_bbo[0])) + Decimal(str(derive_bbo[1]))) / Decimal("2")
        with self._portfolio_lock:
            self._prune_reservations(now, active)
            self._publish_portfolio(position_amount, mark_price, active)
        actions: list[ExecutorAction] = []
        for executor in self._unexpected_active:
            action = self._stop(executor, now, "UNEXPECTED_OR_DUPLICATE_EXECUTOR")
            if action:
                actions.append(action)
        if actions:
            return actions
        armed = not self.config.shadow_mode and self.config.mainnet_armed and not self.config.manual_kill_switch
        if not armed or self._plan is None or self._plan.mm_mode == MMMode.PAUSED:
            for executor in active.values():
                action = self._stop(executor, now, "SAFETY_CANCEL")
                if action:
                    actions.append(action)
            return actions

        desired = {
            "bid": (TradeType.BUY, self._plan.bid_price),
            "ask": (TradeType.SELL, self._plan.ask_price),
        }
        if self._price_tick is None:
            return []
        tick = self._price_tick
        for level, (side, desired_price) in desired.items():
            current = active.get(level)
            if current is not None:
                if desired_price is None:
                    action = self._stop(current, now, "INVENTORY_OR_RISK_CANCEL")
                    if action:
                        actions.append(action)
                    continue
                config = getattr(current, "config", None)
                current_price = Decimal(str(getattr(config, "price", ZERO)))
                age = Decimal(str(max(0.0, now - float(getattr(current, "timestamp", now)))))
                adverse = (side == TradeType.BUY and self._fast_move_bps <= -self.config.fast_adverse_move_bps) or (
                    side == TradeType.SELL and self._fast_move_bps >= self.config.fast_adverse_move_bps
                )
                refresh, reason = should_refresh(
                    side=side,
                    current_price=current_price,
                    desired_price=desired_price,
                    tick_size=tick,
                    age_seconds=age,
                    minimum_residency_seconds=self.config.minimum_normal_quote_residency_seconds,
                    deadband_bps=self.config.normal_refresh_deadband_bps,
                    fast_adverse=adverse,
                )
                self._last_action[level] = reason
                budget_available = self._mutations_last_minute(now) < self.config.max_quote_mutations_per_minute
                if refresh and (adverse or budget_available):
                    action = self._stop(current, now, reason)
                    if action:
                        actions.append(action)
        if actions:
            return actions

        for level, (side, price) in desired.items():
            if level in active or price is None:
                continue
            with self._portfolio_lock:
                if (self.config.id, level) in self._reservations.setdefault(self.config.portfolio_id, {}):
                    self._last_action[level] = "PENDING_CREATE_RESERVATION"
                    continue
            if self._mutations_last_minute(now) >= self.config.max_quote_mutations_per_minute:
                self._last_action[level] = "ACTION_GOVERNOR"
                continue
            with self._portfolio_lock:
                amount, size_reason, projected, fill_effect = self._risk_adjusted_amount(
                    level=level,
                    side=side,
                    price=price,
                    desired_amount=self._plan.amount,
                    now=now,
                    reserve=True,
                )
            self.processed_data[f"risk_adjusted_{level}_order_amount"] = amount
            self.processed_data[f"projected_position_amount_if_{level}_fills"] = projected
            self.processed_data[f"projected_position_notional_if_{level}_fills"] = projected * max(price, mark_price)
            self.processed_data[f"{level}_fill_effect"] = fill_effect.value
            self.processed_data[f"{level}_size_block_reason"] = None if amount > ZERO else size_reason
            if amount <= ZERO:
                self._last_action[level] = size_reason
                continue
            executor_config = OrderExecutorConfig(
                timestamp=now,
                controller_id=self.config.id,
                level_id=level,
                connector_name="derive_perpetual",
                trading_pair=self.config.trading_pair,
                side=side,
                amount=amount,
                price=price,
                position_action=PositionAction.OPEN,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                leverage=self.config.leverage,
            )
            actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=executor_config))
            self._record_mutation(now, "create")
            self._last_action[level] = "CREATE" if size_reason == "READY" else f"CREATE_{size_reason}"
        return actions

    def get_custom_info(self) -> dict[str, Any]:
        now = self.market_data_provider.time()
        active = self._active()
        details = dict(self.processed_data)
        pnl = sum((Decimal(str(getattr(item, "net_pnl_quote", ZERO) or ZERO)) for item in self.executors_info), ZERO)
        self._peak_pnl = max(self._peak_pnl, pnl)
        for level in ("bid", "ask"):
            executor = active.get(level)
            details[f"active_{level}"] = getattr(getattr(executor, "config", None), "price", None)
            details[f"{level}_age"] = max(0.0, now - float(getattr(executor, "timestamp", now))) if executor else None
        details.update(
            {
                "creates_per_minute": sum(1 for _, action in self._action_events if action == "create"),
                "replaces_per_minute": sum(1 for _, action in self._action_events if action == "replace"),
                "cancels_per_minute": sum(1 for _, action in self._action_events if action == "cancel"),
                "mutations_per_minute": self._mutations_last_minute(now),
                "max_quote_mutations_per_minute": self.config.max_quote_mutations_per_minute,
                "last_actions": self._last_action,
                "fills": sum(
                    1
                    for item in self.executors_info
                    if Decimal(str(getattr(item, "filled_amount_quote", ZERO) or ZERO)) > ZERO
                ),
                "volume": sum((Decimal(str(getattr(item, "filled_amount_quote", ZERO) or ZERO)) for item in self.executors_info), ZERO),
                "pnl": pnl,
                "drawdown": self._peak_pnl - pnl,
                "strategy_executor_pnl": pnl,
                "strategy_executor_drawdown": self._peak_pnl - pnl,
                "markout_30s_bps": self.processed_data.get("markout_30s_bps"),
                "markout_60s_bps": self.processed_data.get("markout_60s_bps"),
                "uptime_seconds": max(0.0, now - self._started_at),
                "shadow_mode": self.config.shadow_mode,
                "mainnet_armed": self.config.mainnet_armed,
            }
        )
        return details

    def to_format_status(self) -> list[str]:
        row = self.get_custom_info()
        return [
            f"DERIVE BINANCE ADAPTIVE MM {self.config.asset}",
            f"state={row.get('market_state', 'WAITING')} mode={row.get('mm_mode', 'PAUSED')} operational={row.get('operational_state', 'ERROR')}",
            f"fair={row.get('binance_fair_value', '—')} basis_bps={row.get('basis_bps', '—')} inventory={row.get('inventory_mode', '—')}",
            f"desired={row.get('desired_bid', '—')}/{row.get('desired_ask', '—')} active={row.get('active_bid', '—')}/{row.get('active_ask', '—')}",
        ]
