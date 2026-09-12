"""Native Hummingbot V2 Binance-reference market maker for Derive perpetuals.

One controller instance owns one asset.  The same class is configured once for
XRP and once for LINK.  Binance is market data only; every executor action is
hard-wired to ``derive_perpetual``.
"""

from __future__ import annotations

import logging
import statistics
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from itertools import islice
from typing import Any, ClassVar

from hummingbot.core.data_type.common import MarketDict, PositionAction, PositionMode, TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from pydantic import Field, field_validator, model_validator

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


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
        result = markets.add_or_update(self.connector_name, self.trading_pair)
        if result is not None:
            markets = result
        result = markets.add_or_update(self.reference_connector_name, self.reference_trading_pair)
        return result if result is not None else markets


class DeriveBinanceAdaptiveMM(ControllerBase):
    """Single-asset controller; deploy XRP and LINK configs in the same bot."""

    _logger = None
    _portfolio: ClassVar[dict[str, dict[str, tuple[Decimal, Decimal]]]] = {}
    _portfolio_caps: ClassVar[dict[str, dict[str, Decimal]]] = {}
    _portfolio_terms: ClassVar[dict[str, tuple[Decimal, Decimal]]] = {}

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
        uid = getattr(book, "last_diff_uid", None)
        key = f"{connector}:{pair}"
        changed = uid is None or uid != self._book_uids.get(key)
        if changed:
            self._book_uids[key] = uid
            self._book_updated_at[key] = now
        updated_at = self._book_updated_at.setdefault(key, now)
        return bid, ask, bid_amount, ask_amount, Decimal(str(updated_at)), changed

    @staticmethod
    def _mid(bid: Decimal, ask: Decimal) -> Decimal:
        return (bid + ask) / Decimal("2")

    def _position_amount(self) -> Decimal:
        connector = self.market_data_provider.get_connector(self.config.connector_name)
        total = ZERO
        for position in (getattr(connector, "account_positions", {}) or {}).values():
            if getattr(position, "trading_pair", "") == self.config.trading_pair:
                total += Decimal(str(getattr(position, "amount", ZERO)))
        return total

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

    def _portfolio_totals(self) -> tuple[Decimal, Decimal]:
        values = self._portfolio.setdefault(self.config.portfolio_id, {})
        return (
            sum((abs(position) for position, _ in values.values()), ZERO),
            sum((orders for _, orders in values.values()), ZERO),
        )

    def _publish_portfolio(self, position_notional: Decimal, open_order_notional: Decimal) -> None:
        self._portfolio.setdefault(self.config.portfolio_id, {})[self.config.asset] = (
            position_notional,
            open_order_notional,
        )

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
            dbid, dask, _, _, derive_updated, _ = self._read_book(self.config.connector_name, self.config.trading_pair, now)
            bbid, bask, bsize, asize, binance_updated, reference_changed = self._read_book(
                self.config.reference_connector_name, self.config.reference_trading_pair, now
            )
            derive_age = Decimal(str(now)) - derive_updated
            binance_age = Decimal(str(now)) - binance_updated
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
            trading_rules = self.market_data_provider.get_trading_rules(self.config.connector_name, self.config.trading_pair)
            bid_price = self.market_data_provider.quantize_order_price(self.config.connector_name, self.config.trading_pair, raw_bid)
            ask_price = self.market_data_provider.quantize_order_price(self.config.connector_name, self.config.trading_pair, raw_ask)
            amount = self.market_data_provider.quantize_order_amount(
                self.config.connector_name, self.config.trading_pair, self.config.order_amount_quote / derive_mid
            )
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

            active_notional = sum(
                Decimal(str(getattr(getattr(item, "config", None), "amount", ZERO)))
                * Decimal(str(getattr(getattr(item, "config", None), "price", ZERO)))
                for item in self._active().values()
            )
            self._publish_portfolio(position_notional, active_notional)
            total_inventory, total_orders = self._portfolio_totals()
            markout_30, markout_60 = self._update_markouts(now, derive_mid)
            if total_inventory > self.config.max_total_inventory_quote or total_orders > self.config.max_total_open_order_quote:
                operational = OperationalState.RISK_PAUSED
                self._plan = QuotePlan(None, None, amount, fair, self._market_state, MMMode.PAUSED, inventory_mode, "PORTFOLIO_LIMIT")
            elif not self.config.shadow_mode and self.config.mainnet_armed:
                operational = OperationalState.LIVE_ARMED

            self.processed_data = {
                "asset": self.config.asset,
                "operational_state": operational.value,
                "derive_bbo": [dbid, dask],
                "derive_spread_bps": spread_bps,
                "derive_age_seconds": derive_age,
                "binance_bbo": [bbid, bask],
                "binance_age_seconds": binance_age,
                "binance_fair_value": fair,
                "basis_bps": causal_basis,
                "direction_bps": direction_bps,
                "volatility_bps": volatility_bps,
                "market_state": self._market_state.value,
                "mm_mode": self._plan.mm_mode.value,
                "inventory_mode": inventory_mode.value,
                "position_amount": position,
                "position_notional": position_notional,
                "desired_bid": self._plan.bid_price,
                "desired_ask": self._plan.ask_price,
                "order_amount": amount,
                "fast_move_bps": self._fast_move_bps,
                "portfolio_inventory": total_inventory,
                "portfolio_open_orders": total_orders,
                "markout_30s_bps": markout_30,
                "markout_60s_bps": markout_60,
                "block_reason": self._plan.reason,
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

    def determine_executor_actions(self) -> list[ExecutorAction]:
        now = self.market_data_provider.time()
        active = self._active()
        active_notional = sum(
            Decimal(str(getattr(getattr(item, "config", None), "amount", ZERO)))
            * Decimal(str(getattr(getattr(item, "config", None), "price", ZERO)))
            for item in active.values()
        )
        position_notional = Decimal(str(self.processed_data.get("position_notional", ZERO) or ZERO))
        self._publish_portfolio(position_notional, active_notional)
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
        tick = Decimal(str(self.market_data_provider.get_trading_rules(self.config.connector_name, self.config.trading_pair).min_price_increment))
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
            notional = price * self._plan.amount
            total_inventory, total_orders = self._portfolio_totals()
            if active_notional + notional > self.config.max_asset_open_order_quote:
                self._last_action[level] = "ASSET_OPEN_ORDER_LIMIT"
                continue
            if total_orders + notional > self.config.max_total_open_order_quote:
                self._last_action[level] = "PORTFOLIO_OPEN_ORDER_LIMIT"
                continue
            if self._mutations_last_minute(now) >= self.config.max_quote_mutations_per_minute:
                self._last_action[level] = "ACTION_GOVERNOR"
                continue
            executor_config = OrderExecutorConfig(
                timestamp=now,
                controller_id=self.config.id,
                level_id=level,
                connector_name="derive_perpetual",
                trading_pair=self.config.trading_pair,
                side=side,
                amount=self._plan.amount,
                price=price,
                position_action=PositionAction.OPEN,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                leverage=self.config.leverage,
            )
            actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=executor_config))
            self._record_mutation(now, "create")
            self._last_action[level] = "CREATE"
            active_notional += notional
            self._publish_portfolio(position_notional, active_notional)
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
