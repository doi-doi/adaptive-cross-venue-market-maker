"""Native Hummingbot V2 Binance-reference market maker for Derive XRP perpetuals.

One controller instance owns XRP. Binance is market data only; every executor
action is hard-wired to ``derive_perpetual``.
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
from functools import wraps
from itertools import islice
from pathlib import Path
from threading import Lock, RLock
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


_derive_nonce_lock = Lock()
_last_derive_action_nonce = 0


def _next_unique_derive_nonce(candidate: int) -> int:
    """Make a Derive action nonce strictly increasing within this process."""
    global _last_derive_action_nonce
    with _derive_nonce_lock:
        unique = max(int(candidate), _last_derive_action_nonce + 1)
        _last_derive_action_nonce = unique
        return unique


def _nonce_shape_is_affected(auth_class: type[Any], web_utils_module: Any) -> bool:
    """Recognize only the audited 2.16.0 nonce implementation."""
    try:
        module = inspect.getmodule(auth_class)
        sign_source = inspect.getsource(auth_class.sign)
        nonce_source = inspect.getsource(web_utils_module.get_action_nonce)
        return bool(
            module
            and _is_hummingbot_2160(module.__file__ or "")
            and "nonce=get_action_nonce()" in sign_source
            and "nonce_iter: int = 0" in nonce_source
            and "return int(str(utc_now_ms()) + str(nonce_iter))" in nonce_source
        )
    except (AttributeError, OSError, TypeError):
        return False


def _install_derive_nonce_compatibility() -> bool:
    """Use a process-unique nonce for the audited Hummingbot 2.16.0 connector.

    Hummingbot 2.16.0 calls ``get_action_nonce()`` with its default zero suffix.
    Two concurrent authenticated actions in one millisecond therefore sign the
    same action nonce.  This shim changes only that audited call path and leaves
    all request signing and order execution in the native connector.
    """
    try:
        from hummingbot.connector.derivative.derive_perpetual import derive_perpetual_auth as auth_module
        from hummingbot.connector.derivative.derive_perpetual import derive_perpetual_web_utils as web_utils_module
        from hummingbot.connector.derivative.derive_perpetual.derive_perpetual_auth import DerivePerpetualAuth
    except (ImportError, ModuleNotFoundError):
        return False

    current = getattr(auth_module, "get_action_nonce", None)
    if getattr(current, "_adaptive_mm_nonce_compatibility", False):
        return True
    if not _nonce_shape_is_affected(DerivePerpetualAuth, web_utils_module):
        return False
    original = current

    @wraps(original)
    def unique_action_nonce(nonce_iter: int | None = None) -> int:
        # Passing None selects the native random suffix instead of the affected
        # default zero suffix.  The monotonic floor also covers same-ms races.
        candidate = original(None if nonce_iter in (None, 0) else nonce_iter)
        return _next_unique_derive_nonce(candidate)

    unique_action_nonce._adaptive_mm_nonce_compatibility = True
    auth_module.get_action_nonce = unique_action_nonce
    web_utils_module.get_action_nonce = unique_action_nonce
    logging.getLogger(__name__).warning(
        "Activated guarded Hummingbot 2.16.0 Derive nonce compatibility shim"
    )
    return True


DERIVE_NONCE_COMPATIBILITY_ACTIVE = _install_derive_nonce_compatibility()


def _order_shape_is_affected(derive_class: type[Any]) -> bool:
    """Recognize the 2.16.0 Derive order-price/rejection path under audit."""
    try:
        module = inspect.getmodule(derive_class)
        source = inspect.getsource(derive_class._place_order)
        return bool(
            module
            and _is_hummingbot_2160(module.__file__ or "")
            and 'new_price = float(f"{price:.4g}")' in source
            and '"Self-crossing disallowed"' in source
            and '"reduce_only": False' in source
        )
    except (AttributeError, OSError, TypeError):
        return False


def _remember_derive_order_error(connector: Any, message: Any) -> None:
    connector._adaptive_mm_last_order_error = str(message)
    connector._adaptive_mm_last_order_error_at = time.time()


def _install_derive_order_rejection_compatibility() -> bool:
    """Normalize the audited 2.16.0 Derive rejection path without replacing it."""
    try:
        from hummingbot.connector.derivative.derive_perpetual import derive_perpetual_constants as constants
        from hummingbot.connector.derivative.derive_perpetual.derive_perpetual_derivative import (
            DerivePerpetualDerivative,
        )
    except (ImportError, ModuleNotFoundError):
        return False

    current_place = DerivePerpetualDerivative._place_order
    if getattr(current_place, "_adaptive_mm_order_compatibility", False):
        return True
    if not _order_shape_is_affected(DerivePerpetualDerivative):
        return False

    current_api_post = DerivePerpetualDerivative._api_post

    @wraps(current_api_post)
    async def capture_order_error(self: Any, *args: Any, **kwargs: Any):
        result = await current_api_post(self, *args, **kwargs)
        path = kwargs.get("path_url") or (args[0] if args else None)
        if path == constants.CREATE_ORDER_URL and isinstance(result, dict) and "error" in result:
            error = result.get("error") or {}
            _remember_derive_order_error(self, error.get("message") or error.get("data") or error)
        return result

    @wraps(current_place)
    async def guarded_place_order(self: Any, *args: Any, **kwargs: Any):
        try:
            result = await current_place(self, *args, **kwargs)
        except Exception as exc:
            _remember_derive_order_error(self, exc)
            raise
        if result is None:
            reason = getattr(self, "_adaptive_mm_last_order_error", None) or "Derive order placement returned no result"
            _remember_derive_order_error(self, reason)
            raise OSError(str(reason))
        return result

    capture_order_error._adaptive_mm_order_api_compatibility = True
    guarded_place_order._adaptive_mm_order_compatibility = True
    DerivePerpetualDerivative._api_post = capture_order_error
    DerivePerpetualDerivative._place_order = guarded_place_order
    logging.getLogger(__name__).warning(
        "Activated guarded Hummingbot 2.16.0 Derive order rejection compatibility shim"
    )
    return True


DERIVE_ORDER_REJECTION_COMPATIBILITY_ACTIVE = _install_derive_order_rejection_compatibility()


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
    updated_at: float = 0.0

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
    """Calculate the legacy Binance-relative fair value for diagnostics.

    The normal Derive quote path no longer uses this absolute reference price
    as its centre.  Keeping the helper preserves the causal basis diagnostic
    and the existing public API used by older reports/tests.
    """
    return binance_mid * (ONE + (basis_bps + microprice_adjustment_bps) / BPS)


def calculate_derive_microprice(
    derive_bid: Decimal,
    derive_ask: Decimal,
    derive_bid_size: Decimal,
    derive_ask_size: Decimal,
) -> Decimal:
    """Return a Decimal-safe Derive microprice, falling back to its midpoint.

    ``microprice = ask * bid_size + bid * ask_size`` is only meaningful when
    both top-of-book sizes are positive and the book itself is valid.  A
    malformed/empty size must never make the quote centre jump to Binance.
    """
    midpoint = (derive_bid + derive_ask) / Decimal("2")
    if (
        derive_bid <= ZERO
        or derive_ask <= derive_bid
        or derive_bid_size <= ZERO
        or derive_ask_size <= ZERO
    ):
        return midpoint
    return (derive_ask * derive_bid_size + derive_bid * derive_ask_size) / (
        derive_bid_size + derive_ask_size
    )


def calculate_basis_bps(derive_mid: Decimal, reference_mid: Decimal) -> Decimal:
    """Return a normalized cross-venue basis without assuming equal quotes."""
    if derive_mid <= ZERO or reference_mid <= ZERO:
        return ZERO
    return (derive_mid / reference_mid - ONE) * BPS


def quote_staleness_bps(current_fair_value: Decimal, fair_value_at_creation: Decimal) -> Decimal:
    """Measure movement of the Derive fair value since quote creation."""
    if current_fair_value <= ZERO or fair_value_at_creation <= ZERO:
        return ZERO
    return abs(current_fair_value / fair_value_at_creation - ONE) * BPS


def classify_binance_toxicity_side(fast_move_bps: Decimal, threshold_bps: Decimal) -> str | None:
    """Map a sharp Binance move to the vulnerable Derive quote side."""
    if fast_move_bps <= -abs(threshold_bps):
        return "bid"
    if fast_move_bps >= abs(threshold_bps):
        return "ask"
    return None


def classify_binance_shock(
    fast_move_bps: Decimal,
    basis_residual_bps: Decimal,
    volatility_bps: Decimal,
    *,
    emergency_move_bps: Decimal = Decimal("20"),
    emergency_dislocation_bps: Decimal = Decimal("12"),
    emergency_volatility_bps: Decimal = Decimal("20"),
    elevated_move_bps: Decimal = Decimal("5"),
    elevated_dislocation_bps: Decimal = Decimal("5"),
) -> tuple[str, str | None, tuple[str, ...]]:
    """Classify Binance movement without turning normal noise into cancels.

    Emergency requires at least two independent danger conditions.  The
    cross-venue residual is signed as Derive minus Binance, so a positive
    Binance move (or negative residual) makes the ask vulnerable and vice
    versa.  A single elevated condition widens the quote but does not cancel.
    """
    conditions: list[str] = []
    if abs(fast_move_bps) >= emergency_move_bps:
        conditions.append("LARGE_BINANCE_MOVE")
    if abs(basis_residual_bps) >= emergency_dislocation_bps:
        conditions.append("CROSS_VENUE_DISLOCATION")
    if volatility_bps >= emergency_volatility_bps:
        conditions.append("VOLATILITY_SPIKE")
    direction = fast_move_bps if abs(fast_move_bps) >= elevated_move_bps else -basis_residual_bps
    side = "ask" if direction > ZERO else "bid" if direction < ZERO else None
    elevated = abs(fast_move_bps) >= elevated_move_bps or abs(basis_residual_bps) >= elevated_dislocation_bps
    if len(conditions) >= 2:
        state = "EMERGENCY"
    elif elevated:
        state = "ELEVATED"
    else:
        state = "NORMAL"
    return state, side, tuple(conditions)


def update_binance_shock_hysteresis(
    *,
    active: bool,
    latched_side: str | None,
    latched_conditions: tuple[str, ...],
    recovery_since: float | None,
    raw_state: str,
    raw_side: str | None,
    raw_conditions: tuple[str, ...],
    now: float,
    recovery_seconds: float,
) -> tuple[bool, str | None, tuple[str, ...], float | None, str]:
    """Keep a true shock latched until conditions are calm for long enough."""
    if raw_state == "EMERGENCY":
        return True, raw_side or latched_side, raw_conditions, None, "EMERGENCY"
    if not active:
        return False, None, (), None, raw_state
    if raw_state == "NORMAL":
        if recovery_since is None:
            return True, latched_side, latched_conditions, now, "EMERGENCY_RECOVERY"
        if now - recovery_since >= recovery_seconds:
            return False, None, (), None, "NORMAL"
    return True, latched_side or raw_side, latched_conditions, recovery_since, "EMERGENCY_RECOVERY"


def calculate_total_spread_bps(bid_price: Decimal | None, ask_price: Decimal | None) -> Decimal | None:
    """Return the quoted bid/ask distance using the TOTAL-spread convention."""
    if bid_price is None or ask_price is None or bid_price <= ZERO or ask_price <= bid_price:
        return None
    midpoint = (bid_price + ask_price) / Decimal("2")
    return (ask_price - bid_price) / midpoint * BPS


def normal_quote_edge_bps(total_spread_bps: Decimal) -> Decimal:
    """Convert a configured total bid/ask spread to the per-side edge."""
    if total_spread_bps <= ZERO:
        raise ValueError("total spread must be positive")
    return total_spread_bps / Decimal("2")


def calculate_normal_quote_prices(
    derive_bid: Decimal,
    derive_ask: Decimal,
    reservation: Decimal,
    total_spread_bps: Decimal,
    tick_size: Decimal,
) -> tuple[Decimal, Decimal]:
    """Place calm quotes inside a wide Derive BBO without taking liquidity.

    The returned prices remain one native tick away from the opposite BBO. If
    the book is too narrow to improve safely, the current BBO is retained.
    ``total_spread_bps`` is a complete bid-to-ask target, never a per-side
    value.
    """
    if derive_bid <= ZERO or derive_ask <= derive_bid or reservation <= ZERO or tick_size <= ZERO:
        return derive_bid, derive_ask
    edge = normal_quote_edge_bps(total_spread_bps) / BPS
    target_bid = reservation * (ONE - edge)
    target_ask = reservation * (ONE + edge)
    bid = min(max(derive_bid, target_bid), derive_ask - tick_size)
    ask = max(min(derive_ask, target_ask), derive_bid + tick_size)
    if bid >= ask:
        return derive_bid, derive_ask
    return bid, ask


def effective_quote_edge_bps(
    market_state: MarketState,
    normal_total_spread_bps: Decimal,
    protected_edge_bps: Decimal,
    toxicity_guard_active: bool = False,
    toxicity_widening_total_spread_bps: Decimal = ZERO,
) -> Decimal:
    """Select the per-side edge while preserving wider protective regimes."""
    edge = (
        normal_quote_edge_bps(normal_total_spread_bps)
        if market_state == MarketState.NORMAL
        else protected_edge_bps
    )
    if toxicity_guard_active and toxicity_widening_total_spread_bps > ZERO:
        edge += normal_quote_edge_bps(toxicity_widening_total_spread_bps)
    return edge


def markout_is_toxic(
    markout_5s_bps: Decimal | None,
    markout_30s_bps: Decimal | None,
    threshold_bps: Decimal,
) -> bool:
    """Return true when either recent markout horizon is materially negative."""
    return any(
        markout is not None and markout <= -abs(threshold_bps)
        for markout in (markout_5s_bps, markout_30s_bps)
    )


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
    derive_staleness_bps: Decimal | None = None,
    refresh_reason: str | None = None,
) -> tuple[bool, str]:
    """Decide whether one resting quote should be replaced.

    ``derive_staleness_bps`` is an optional source diagnostic.  When present,
    the normal replacement reason is explicitly Derive-driven; Binance-only
    movement is intentionally not an input to this decision.  ``fast_adverse``
    remains for backwards-compatible callers, while the live controller routes
    its Binance shock through a separate side-specific toxicity cancellation.
    """
    if fast_adverse:
        return True, "FAST_ADVERSE_MOVE"
    if current_price == desired_price or abs(current_price - desired_price) < tick_size:
        return False, "TICK_AWARE_HOLD"
    if age_seconds < minimum_residency_seconds:
        return False, "MINIMUM_RESIDENCY"
    distance_bps = abs(desired_price / current_price - ONE) * BPS
    if distance_bps < deadband_bps:
        return False, "DEADBAND"
    if (
        derive_staleness_bps is not None
        and derive_staleness_bps < deadband_bps
        and refresh_reason in {None, "DERIVE_STALENESS_REFRESH"}
    ):
        return False, "DEADBAND"
    if refresh_reason is not None:
        return True, refresh_reason
    if derive_staleness_bps is not None:
        return True, "DERIVE_STALENESS_REFRESH"
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
    portfolio_id: str = Field(default="derive_xrp_800")
    position_mode: PositionMode = Field(default=PositionMode.ONEWAY)
    leverage: int = Field(default=1, ge=1, le=5)

    shadow_mode: bool = Field(default=True)
    mainnet_armed: bool = Field(default=False)
    # Hummingbot's V2 controller reload only applies fields explicitly marked
    # updatable.  Keep the normal Condor stop path live after this subclass
    # redeclares the base controller field.
    manual_kill_switch: bool = Field(default=False, json_schema_extra={"is_updatable": True})
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

    # ``normal_total_spread_bps`` is explicitly the complete bid-to-ask
    # distance.  The controller converts it to a half-spread per side only in
    # the calm NORMAL regime; trend/volatility/event protection keeps the
    # existing wider edge.
    normal_total_spread_bps: Decimal = Field(default=Decimal("8"), gt=0)

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
    # Optional research/diagnostic ceiling.  It is unset in the committed
    # production surface, so normal quote age remains governed by Derive
    # staleness and the existing residency/deadband rules.
    max_normal_quote_age_seconds: Decimal | None = Field(default=None, gt=0)
    fast_adverse_move_bps: Decimal = Field(default=Decimal("5"), gt=0)
    fast_adverse_window_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    max_quote_mutations_per_minute: int = Field(default=30, ge=1, le=120)

    # Binance is a reference-only danger sensor.  One condition is elevated
    # (widen, do not cancel); two independent conditions are required for an
    # immediate vulnerable-side emergency cancel.
    binance_emergency_move_bps: Decimal = Field(default=Decimal("20"), gt=0)
    binance_emergency_dislocation_bps: Decimal = Field(default=Decimal("12"), gt=0)
    binance_emergency_volatility_bps: Decimal = Field(default=Decimal("20"), gt=0)
    binance_elevated_move_bps: Decimal = Field(default=Decimal("5"), gt=0)
    binance_elevated_dislocation_bps: Decimal = Field(default=Decimal("5"), gt=0)
    binance_elevated_widening_total_spread_bps: Decimal = Field(default=Decimal("2"), gt=0)
    binance_emergency_recovery_seconds: Decimal = Field(default=Decimal("10"), gt=0)

    # A recent strongly negative maker markout temporarily widens the quote.
    # These are guardrails, not optimisation knobs; the spread study reports
    # their raw activation count separately.
    toxicity_markout_threshold_bps: Decimal = Field(default=Decimal("5"), gt=0)
    toxicity_widening_total_spread_bps: Decimal = Field(default=Decimal("4"), gt=0)
    toxicity_guard_seconds: Decimal = Field(default=Decimal("60"), gt=0)

    @field_validator("asset", mode="before")
    @classmethod
    def normalize_asset(cls, value: Any) -> str:
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_final_architecture(self):
        if self.asset != "XRP":
            raise ValueError("asset must be XRP")
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
        if self.binance_emergency_move_bps <= self.binance_elevated_move_bps:
            raise ValueError("Binance emergency move threshold must exceed elevated threshold")
        if self.binance_emergency_dislocation_bps <= self.binance_elevated_dislocation_bps:
            raise ValueError("Binance emergency dislocation threshold must exceed elevated threshold")
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
    """Single-asset XRP controller for Derive execution and Binance reference data."""

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
        self._volatility_sample_at: float | None = None
        self._volatility_sample_mid: Decimal | None = None
        self._fast_move_bps = ZERO
        self._action_timestamps: deque[float] = deque()
        self._action_events: deque[tuple[float, str]] = deque()
        self._pending_stops: set[str] = set()
        self._pending_stop_levels: dict[str, str] = {}
        self._unexpected_active: list[Any] = []
        self._execution_fail_closed_reason: str | None = None
        self._last_action: dict[str, str] = {"bid": "NONE", "ask": "NONE"}
        self._fill_observations: dict[str, dict[str, Any]] = {}
        self._markout_5s: list[Decimal] = []
        self._markout_30s: list[Decimal] = []
        self._markout_60s: list[Decimal] = []
        self._markout_300s: list[Decimal] = []
        self._toxicity_guard_until = 0.0
        self._toxicity_guard_active = False
        self._toxicity_guard_activations = 0
        self._toxicity_markout_counts: tuple[int, int] = (0, 0)
        self._binance_shock_state = "NORMAL"
        self._binance_emergency_active = False
        self._binance_emergency_side: str | None = None
        self._binance_emergency_conditions: tuple[str, ...] = ()
        self._binance_recovery_since: float | None = None
        # Per-level quote anchors let normal refreshes compare a current
        # Derive fair value with the fair value captured when that quote was
        # created.  The map is deliberately local to this controller; missing
        # executor metadata is handled fail-closed by initializing on first
        # observation rather than borrowing Binance state.
        self._quote_fair_at_creation: dict[str, tuple[float, Decimal]] = {}
        self._quote_fair_anchor_trusted: dict[str, bool] = {}
        self._quote_lifetimes_seconds: list[Decimal] = []
        self._replacement_count = 0
        self._cancel_count = 0
        self._derive_refresh_count = 0
        self._binance_emergency_cancels: dict[str, int] = {"bid": 0, "ask": 0}
        self._replacement_reason_counts: dict[str, int] = {}
        self._started_at = self.market_data_provider.time()
        self._quote_uptime_seconds = 0.0
        self._quote_uptime_last_at = self._started_at
        self._quote_was_available = False
        self._peak_pnl = ZERO
        self._peak_account_equity: Decimal | None = None
        self._peak_account_collateral: Decimal | None = None
        self._price_tick: Decimal | None = None
        self._amount_quantizer: Any | None = None
        self._trading_rule: Any | None = None
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

    @staticmethod
    def _executor_status(executor: Any) -> str:
        status = getattr(executor, "status", None)
        value = getattr(status, "value", status)
        return str(value or "").upper()

    @classmethod
    def _is_terminal_executor(cls, executor: Any) -> bool:
        return bool(getattr(executor, "is_done", False)) or cls._executor_status(executor) in {
            "TERMINATED",
            "CLOSED",
        }

    def _reconcile_pending_stops(self) -> set[str]:
        """Release replacement gates only after a native executor is terminal."""
        by_id = {str(getattr(executor, "id", "")): executor for executor in self.executors_info}
        for executor_id in list(self._pending_stops):
            executor = by_id.get(executor_id)
            if executor is not None and self._is_terminal_executor(executor):
                self._pending_stops.discard(executor_id)
                self._pending_stop_levels.pop(executor_id, None)
        return {self._pending_stop_levels[executor_id] for executor_id in self._pending_stops if executor_id in self._pending_stop_levels}

    def _pending_create_levels(self) -> set[str]:
        reservations = self._reservations.setdefault(self.config.portfolio_id, {})
        return {
            level
            for (controller_id, level), reservation in reservations.items()
            if controller_id == self.config.id and reservation.asset == self.config.asset
        }

    @staticmethod
    def _native_submit_price(price: Decimal) -> Decimal:
        """Mirror Derive 2.16.0's four-significant-digit price conversion."""
        return Decimal(str(float(f"{price:.4g}")))

    def _self_crossing_level(
        self,
        level: str,
        desired_price: Decimal,
        active: dict[str, Any],
        desired_prices: dict[str, Decimal | None],
    ) -> bool:
        """Return true when native submitted prices would cross own liquidity."""
        native_price = self._native_submit_price(desired_price)
        opposite_level = "ask" if level == "bid" else "bid"
        opposite = active.get(opposite_level)
        if opposite is not None:
            opposite_config = getattr(opposite, "config", None)
            opposite_price = Decimal(str(getattr(opposite_config, "price", ZERO) or ZERO))
            if opposite_price > ZERO:
                native_opposite = self._native_submit_price(opposite_price)
                if level == "bid" and native_price >= native_opposite:
                    return True
                if level == "ask" and native_price <= native_opposite:
                    return True
        # Also guard two new quotes in the same controller cycle after the
        # connector's native four-significant-digit conversion.
        other_desired = desired_prices.get(opposite_level)
        if other_desired is not None:
            native_other = self._native_submit_price(other_desired)
            if level == "bid" and native_price >= native_other:
                return True
            if level == "ask" and native_price <= native_other:
                return True
        return False

    def _observe_execution_failures(self) -> str | None:
        """Read native connector/error metadata and executor retry state."""
        if self.config.shadow_mode:
            return None
        try:
            connector = self.market_data_provider.get_connector(self.config.execution_market_connector_name)
            connector_error = getattr(connector, "_adaptive_mm_last_order_error", None)
            if connector_error:
                return str(connector_error)
        except (AttributeError, KeyError, TypeError, ValueError):
            pass
        processed_error = self.processed_data.get("last_error")
        if processed_error and any(
            marker in str(processed_error).lower() for marker in ("cross", "nonce", "reject", "already been used")
        ):
            return str(processed_error)
        for executor in self.executors_info:
            custom = getattr(executor, "custom_info", {}) or {}
            try:
                retries = int(custom.get("current_retries", 0) or 0)
            except (TypeError, ValueError):
                retries = 0
            if retries > 0:
                return str(custom.get("last_error") or custom.get("error_message") or "DERIVE_ORDER_RETRY")
            custom_error = custom.get("last_error") or custom.get("error_message")
            if custom_error and any(
                marker in str(custom_error).lower() for marker in ("cross", "nonce", "reject", "already been used")
            ):
                return str(custom_error)
        return None

    def _mutations_last_minute(self, now: float) -> int:
        while self._action_timestamps and now - self._action_timestamps[0] >= 60:
            self._action_timestamps.popleft()
        while self._action_events and now - self._action_events[0][0] >= 60:
            self._action_events.popleft()
        return len(self._action_timestamps)

    def _record_mutation(self, now: float, action: str) -> None:
        self._action_timestamps.append(now)
        self._action_events.append((now, action))

    def _quote_lifetime_metrics(self) -> dict[str, Decimal | None]:
        if not self._quote_lifetimes_seconds:
            return {
                "quote_lifetime_average_seconds": None,
                "quote_lifetime_median_seconds": None,
                "quote_lifetime_p90_seconds": None,
            }
        ordered = sorted(self._quote_lifetimes_seconds)
        index = (len(ordered) - 1) * 0.90
        lower, upper = int(index), min(len(ordered) - 1, int(index) + 1)
        fraction = Decimal(str(index - int(index)))
        p90 = ordered[lower] + (ordered[upper] - ordered[lower]) * fraction
        return {
            "quote_lifetime_average_seconds": sum(ordered, ZERO) / Decimal(len(ordered)),
            "quote_lifetime_median_seconds": Decimal(str(statistics.median(ordered))),
            "quote_lifetime_p90_seconds": p90,
        }

    def _quote_fair_anchor(self, level: str, executor: Any, current_fair: Decimal) -> Decimal:
        """Resolve the Derive fair value captured for a resting quote.

        Native executor implementations differ in whether arbitrary creation
        metadata is exposed.  Prefer that metadata when present, then use the
        controller's per-level anchor, and finally initialize the anchor from
        the current Derive fair value.  The fallback is deterministic and never
        uses the absolute Binance price.
        """
        custom = getattr(executor, "custom_info", {}) or {}
        for key in ("derive_fair_value_at_creation", "quote_fair_value_at_creation"):
            try:
                value = Decimal(str(custom.get(key)))
            except (TypeError, ValueError, ArithmeticError):
                continue
            if value > ZERO:
                self._quote_fair_anchor_trusted[level] = True
                return value
        timestamp = float(getattr(executor, "timestamp", self.market_data_provider.time()))
        cached = self._quote_fair_at_creation.get(level)
        if cached is None or abs(cached[0] - timestamp) > 1e-6:
            self._quote_fair_at_creation[level] = (timestamp, current_fair)
            self._quote_fair_anchor_trusted[level] = False
            return current_fair
        self._quote_fair_anchor_trusted.setdefault(level, True)
        return cached[1]

    def _binance_toxicity_level(self) -> str | None:
        """Return the vulnerable side only for an asserted emergency."""
        return self._binance_emergency_side if self._binance_emergency_active else None

    def _update_binance_shock_state(self, now: float, basis_residual_bps: Decimal, volatility_bps: Decimal) -> None:
        raw_state, raw_side, raw_conditions = classify_binance_shock(
            self._fast_move_bps,
            basis_residual_bps,
            volatility_bps,
            emergency_move_bps=self.config.binance_emergency_move_bps,
            emergency_dislocation_bps=self.config.binance_emergency_dislocation_bps,
            emergency_volatility_bps=self.config.binance_emergency_volatility_bps,
            elevated_move_bps=self.config.binance_elevated_move_bps,
            elevated_dislocation_bps=self.config.binance_elevated_dislocation_bps,
        )
        (
            self._binance_emergency_active,
            self._binance_emergency_side,
            self._binance_emergency_conditions,
            self._binance_recovery_since,
            self._binance_shock_state,
        ) = update_binance_shock_hysteresis(
            active=self._binance_emergency_active,
            latched_side=self._binance_emergency_side,
            latched_conditions=self._binance_emergency_conditions,
            recovery_since=self._binance_recovery_since,
            raw_state=raw_state,
            raw_side=raw_side,
            raw_conditions=raw_conditions,
            now=now,
            recovery_seconds=float(self.config.binance_emergency_recovery_seconds),
        )

    def _binance_emergency_reason(self, level: str) -> str:
        conditions = "_".join(self._binance_emergency_conditions) or "RECOVERY"
        return f"BINANCE_TRUE_SHOCK_{conditions}_CANCEL_{level.upper()}"

    def _normal_refresh_reason(self, level: str, *, max_age_due: bool = False) -> str:
        """Label the source of a non-emergency quote replacement."""
        if max_age_due:
            # The age check is applied by ``determine_executor_actions`` and
            # this helper is used only for its explicit reason label.
            return "MAX_QUOTE_AGE_REFRESH"
        inventory_mode = str(self.processed_data.get("inventory_mode", "FLAT"))
        if inventory_mode not in {InventoryMode.FLAT.value, "FLAT", ""}:
            return "INVENTORY_REFRESH"
        if self._market_state != MarketState.NORMAL:
            return "REGIME_WIDEN"
        return "DERIVE_STALENESS_REFRESH"

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
            # Only the owning controller can reconcile its reservation. A
            # stalled controller retains its pending exposure until it resumes
            # or the bounded safety TTL expires.
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
        updated_at: float,
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
            updated_at=updated_at,
        )
        self._portfolio.setdefault(self.config.portfolio_id, {})[self.config.asset] = snapshot
        return snapshot

    def _record_fixed_volatility_sample(self, now: float, mid: Decimal) -> None:
        """Record returns between adjacent one-second buckets; never fill missed buckets."""
        sample_second = float(int(now))
        if self._volatility_sample_at is None or self._volatility_sample_mid is None:
            self._volatility_sample_at = sample_second
            self._volatility_sample_mid = mid
            return
        elapsed_seconds = sample_second - self._volatility_sample_at
        if elapsed_seconds <= 0:
            return
        if elapsed_seconds == 1:
            self._returns.append(float(mid / self._volatility_sample_mid - ONE))
        self._volatility_sample_at = sample_second
        self._volatility_sample_mid = mid

    def _pause_volatility_sampling(self) -> None:
        self._volatility_sample_at = None
        self._volatility_sample_mid = None

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

    @staticmethod
    def _average_markout(values: list[Decimal]) -> Decimal | None:
        return sum(values, ZERO) / len(values) if values else None

    def _update_markouts(self, now: float, derive_mid: Decimal) -> tuple[Decimal | None, Decimal | None]:
        """Record signed markouts at deterministic horizons after each fill."""
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
                    "done_5": False,
                    "done_30": False,
                    "done_60": False,
                    "done_300": False,
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
            if age >= 5 and not observation["done_5"]:
                self._markout_5s.append(markout)
                observation["done_5"] = True
            if age >= 30 and not observation["done_30"]:
                self._markout_30s.append(markout)
                observation["done_30"] = True
            if age >= 60 and not observation["done_60"]:
                self._markout_60s.append(markout)
                observation["done_60"] = True
            if age >= 300 and not observation["done_300"]:
                self._markout_300s.append(markout)
                observation["done_300"] = True
        markout_30 = self._average_markout(self._markout_30s)
        markout_60 = self._average_markout(self._markout_60s)
        return markout_30, markout_60

    def _refresh_toxicity_guard(self, now: float) -> bool:
        """Temporarily widen after a newly observed toxic 5s/30s markout."""
        counts = (len(self._markout_5s), len(self._markout_30s))
        if counts != self._toxicity_markout_counts:
            self._toxicity_markout_counts = counts
            markout_5 = self._markout_5s[-1] if self._markout_5s else None
            markout_30 = self._markout_30s[-1] if self._markout_30s else None
            if markout_is_toxic(markout_5, markout_30, self.config.toxicity_markout_threshold_bps):
                self._toxicity_guard_until = now + float(self.config.toxicity_guard_seconds)
                self._toxicity_guard_activations += 1
        self._toxicity_guard_active = now < self._toxicity_guard_until
        return self._toxicity_guard_active

    def _record_quote_uptime(self, now: float, quote_available: bool) -> None:
        elapsed = max(0.0, now - self._quote_uptime_last_at)
        if self._quote_was_available:
            self._quote_uptime_seconds += elapsed
        self._quote_uptime_last_at = now
        self._quote_was_available = quote_available

    def _volume_metrics(self, now: float, pnl: Decimal) -> dict[str, Any]:
        """Return raw maker-turnover diagnostics without fabricating fills."""
        volume = sum(
            (Decimal(str(getattr(item, "filled_amount_quote", ZERO) or ZERO)) for item in self.executors_info),
            ZERO,
        )
        fees = sum(
            (Decimal(str(getattr(item, "cum_fees_quote", ZERO) or ZERO)) for item in self.executors_info),
            ZERO,
        )
        fills = sum(
            1
            for item in self.executors_info
            if Decimal(str(getattr(item, "filled_amount_quote", ZERO) or ZERO)) > ZERO
        )
        uptime_seconds = max(0.0, now - self._started_at)
        uptime = Decimal(str(uptime_seconds))
        hours = uptime / Decimal("3600") if uptime > ZERO else ZERO
        days = uptime / Decimal("86400") if uptime > ZERO else ZERO
        volume_per_hour = volume / hours if hours > ZERO else ZERO
        volume_per_day = volume / days if days > ZERO else ZERO
        pnl_per_1000 = pnl / volume * Decimal("1000") if volume > ZERO else None
        pnl_per_fill = pnl / Decimal(fills) if fills else None
        markout_5 = self._average_markout(self._markout_5s)
        markout_30 = self._average_markout(self._markout_30s)
        markout_toxic = markout_is_toxic(markout_5, markout_30, self.config.toxicity_markout_threshold_bps)
        valid = pnl > ZERO and pnl_per_1000 is not None and pnl_per_1000 > ZERO and not markout_toxic
        if self.config.max_account_drawdown_quote is not None:
            valid = valid and (self._peak_pnl - pnl) <= self.config.max_account_drawdown_quote
        score = volume_per_day if valid else ZERO
        capital_turnover = (
            volume_per_day / self.config.portfolio_capital_quote
            if self.config.portfolio_capital_quote > ZERO
            else None
        )
        uptime_pct = (
            Decimal(str(min(1.0, self._quote_uptime_seconds / uptime_seconds))) * Decimal("100")
            if uptime_seconds > 0
            else ZERO
        )
        return {
            "maker_volume_quote": volume,
            "maker_volume_per_hour": volume_per_hour,
            "maker_volume_per_day": volume_per_day,
            "capital_turnover_per_day": capital_turnover,
            "fills": fills,
            "fills_per_hour": Decimal(fills) / hours if hours > ZERO else ZERO,
            "maker_fees_quote": fees,
            "gross_spread_capture_quote": None,
            "inventory_pnl_quote": None,
            "pnl_per_1000_volume": pnl_per_1000,
            "pnl_per_fill": pnl_per_fill,
            "quote_uptime_pct": uptime_pct,
            "profitable_volume_efficiency": score,
            "profitable_volume_efficiency_valid": valid,
        }

    async def update_processed_data(self):
        now = self.market_data_provider.time()
        operational = OperationalState.SHADOW if self.config.shadow_mode else OperationalState.LIVE_DISARMED
        try:
            dbid, dask, dbid_size, dask_size, derive_updated, _ = self._read_book(
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
                self._pause_volatility_sampling()
                self.processed_data["volatility_sampling_state"] = "PAUSED_STALE"
                operational = OperationalState.REFERENCE_PAUSED
                raise RuntimeError("BINANCE_STALE")
            if reference_changed and self._reference_recovered_at is None:
                self._reference_recovered_at = now
            if self._reference_recovered_at is None or now - self._reference_recovered_at < float(self.config.binance_recovery_seconds):
                operational = OperationalState.REFERENCE_PAUSED
                raise RuntimeError("BINANCE_RECOVERY")

            derive_mid, binance_mid = self._mid(dbid, dask), self._mid(bbid, bask)
            markout_30, markout_60 = self._update_markouts(now, derive_mid)
            markout_5 = self._average_markout(self._markout_5s)
            markout_300 = self._average_markout(self._markout_300s)
            toxicity_guard_active = self._refresh_toxicity_guard(now)
            basis = calculate_basis_bps(derive_mid, binance_mid)
            self._basis.append(basis)
            expected_basis = Decimal(str(statistics.median(self._basis)))
            basis_residual = basis - expected_basis
            derive_microprice = calculate_derive_microprice(dbid, dask, dbid_size, dask_size)
            derive_micro_bps = (derive_microprice / derive_mid - ONE) * BPS if derive_mid > ZERO else ZERO
            derive_micro_bps = max(
                -self.config.microprice_adjustment_max_bps,
                min(self.config.microprice_adjustment_max_bps, derive_micro_bps),
            )
            # Keep a Binance-relative fair value for predictive diagnostics
            # only.  It is never fed to the normal Derive quote centre.
            binance_microprice = (
                (bask * bsize + bbid * asize) / (bsize + asize)
                if bsize + asize > ZERO
                else binance_mid
            )
            binance_micro_bps = (
                (binance_microprice / binance_mid - ONE) * BPS if binance_mid > ZERO else ZERO
            )
            binance_micro_bps = max(
                -self.config.microprice_adjustment_max_bps,
                min(self.config.microprice_adjustment_max_bps, binance_micro_bps),
            )
            binance_predictive_fair = calculate_fair_value(binance_mid, expected_basis, binance_micro_bps)

            self._record_fixed_volatility_sample(now, binance_mid)
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
            self._update_binance_shock_state(now, basis_residual, volatility_bps)

            position = self._position_amount()
            position_notional = position * derive_mid
            inventory_mode = classify_inventory(position_notional, self.config.max_asset_inventory_quote, self.config.one_sided_inventory_ratio)
            mode = STATE_TO_MODE[self._market_state]
            if self.config.manual_kill_switch:
                mode = MMMode.PAUSED
                operational = OperationalState.RISK_PAUSED
            # Normal quoting is anchored to Derive's own microprice (or its
            # midpoint fallback).  Binance remains a predictive overlay for
            # trend/volatility/toxicity and a normalized basis diagnostic.
            fair = derive_microprice
            vol_buffer = volatility_bps * self.config.volatility_buffer_multiplier
            base_edge = (
                self.config.maker_fee_buffer_bps
                + self.config.minimum_profit_buffer_bps
                + vol_buffer
                + self.config.latency_toxicity_buffer_bps
            )
            # Only the calm NORMAL regime uses the configurable total spread.
            # All protective states retain the existing fee/volatility edge;
            # toxicity widening is additive and applies to every non-paused
            # state.
            edge = effective_quote_edge_bps(
                self._market_state,
                self.config.normal_total_spread_bps,
                base_edge,
                toxicity_guard_active,
                self.config.toxicity_widening_total_spread_bps,
            )
            direction_skew = self.config.direction_skew_bps if mode == MMMode.LONG_BIAS else -self.config.direction_skew_bps if mode == MMMode.SHORT_BIAS else ZERO
            inventory_ratio = max(Decimal("-1"), min(Decimal("1"), position_notional / self.config.max_asset_inventory_quote))
            reservation = fair * (ONE + (direction_skew - inventory_ratio * self.config.inventory_skew_bps) / BPS)
            execution_market = self.config.execution_market_connector_name
            trading_rules, quantizer = await self._execution_rules_and_quantizer()
            self._trading_rule = trading_rules
            self._amount_quantizer = quantizer
            self._price_tick = Decimal(str(trading_rules.min_price_increment))
            if self._market_state == MarketState.NORMAL:
                elevated_widening = (
                    self.config.binance_elevated_widening_total_spread_bps
                    if self._binance_shock_state in {"ELEVATED", "EMERGENCY_RECOVERY"}
                    else ZERO
                )
                raw_bid, raw_ask = calculate_normal_quote_prices(
                    dbid,
                    dask,
                    reservation,
                    self.config.normal_total_spread_bps
                    + elevated_widening
                    + (self.config.toxicity_widening_total_spread_bps if toxicity_guard_active else ZERO),
                    self._price_tick,
                )
            else:
                raw_bid = min(dbid, reservation * (ONE - edge / BPS))
                raw_ask = max(dask, reservation * (ONE + edge / BPS))
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
            binance_toxicity_side = self._binance_toxicity_level()
            if mode != MMMode.PAUSED and amount > ZERO:
                # Suppress only the vulnerable side on a sharp Binance move;
                # the opposite safe side remains eligible to rest.
                if binance_toxicity_side == "bid":
                    bid_price = None
                elif binance_toxicity_side == "ask":
                    ask_price = None
            self._plan = QuotePlan(bid_price, ask_price, amount, fair, self._market_state, mode, inventory_mode, size_reason)
            if self._execution_fail_closed_reason is not None:
                operational = OperationalState.ERROR
                self._plan = QuotePlan(
                    None,
                    None,
                    amount,
                    fair,
                    self._market_state,
                    MMMode.PAUSED,
                    inventory_mode,
                    "EXECUTION_FAIL_CLOSED",
                )

            active = self._active()
            with self._portfolio_lock:
                self._prune_reservations(now, active)
                own_snapshot = self._publish_portfolio(position, derive_mid, active, now)
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

            self._record_quote_uptime(
                now,
                self._plan.bid_price is not None or self._plan.ask_price is not None,
            )

            quote_total_spread = calculate_total_spread_bps(self._plan.bid_price, self._plan.ask_price)

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
                "binance_fair_value": binance_predictive_fair,
                "binance_predictive_fair_value": binance_predictive_fair,
                "derive_microprice": derive_microprice,
                "derive_fair_value": fair,
                "quote_center_source": "DERIVE_MICROPRICE" if derive_microprice != derive_mid else "DERIVE_MIDPOINT",
                "basis_bps": basis,
                "expected_basis_bps": expected_basis,
                "basis_residual_bps": basis_residual,
                "derive_microprice_displacement_bps": derive_micro_bps,
                "binance_microprice_displacement_bps": binance_micro_bps,
                "direction_bps": direction_bps,
                "volatility_bps": volatility_bps,
                "volatility_sample_interval_seconds": 1,
                "volatility_sample_count": len(self._returns),
                "volatility_sampling_state": "FIXED_1S",
                "normal_total_spread_bps": self.config.normal_total_spread_bps,
                "quote_spread_convention": "TOTAL_BID_ASK",
                "quote_total_spread_bps": quote_total_spread,
                "toxicity_guard_active": toxicity_guard_active,
                "toxicity_guard_activations": self._toxicity_guard_activations,
                "toxicity_guard_until": self._toxicity_guard_until if toxicity_guard_active else None,
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
                "binance_toxicity_side": binance_toxicity_side,
                "binance_shock_state": self._binance_shock_state,
                "binance_emergency_conditions": self._binance_emergency_conditions,
                "binance_emergency_active": self._binance_emergency_active,
                "binance_emergency_recovery_since": self._binance_recovery_since,
                "binance_emergency_action": (
                    self._binance_emergency_reason(binance_toxicity_side)
                    if binance_toxicity_side
                    else None
                ),
                "derive_normal_refresh_count": self._derive_refresh_count,
                "binance_emergency_bid_cancels": self._binance_emergency_cancels["bid"],
                "binance_emergency_ask_cancels": self._binance_emergency_cancels["ask"],
                "replacement_reason_counts": dict(self._replacement_reason_counts),
                "last_actions": dict(self._last_action),
                "portfolio_inventory": total_inventory,
                "portfolio_open_orders": total_orders,
                "markout_5s_bps": markout_5,
                "markout_30s_bps": markout_30,
                "markout_60s_bps": markout_60,
                "markout_300s_bps": markout_300,
                "block_reason": self._plan.reason,
                "derive_snapshot_compatibility_active": DERIVE_SNAPSHOT_RACE_COMPATIBILITY_ACTIVE,
                "risk_snapshot_updated_at": now,
                "pending_cancels": sorted(self._reconcile_pending_stops()),
                "pending_creates": sorted(self._pending_create_levels()),
                "execution_fail_closed": self._execution_fail_closed_reason is not None,
                "last_error": self._execution_fail_closed_reason,
                "diagnostics_state": "HEALTHY",
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
            self._record_quote_uptime(now, False)
            self.processed_data.update(
                {
                    "asset": self.config.asset,
                    "operational_state": operational.value,
                    "mm_mode": MMMode.PAUSED.value,
                    "desired_bid": None,
                    "desired_ask": None,
                    "block_reason": str(exc),
                    "pending_cancels": sorted(self._reconcile_pending_stops()),
                    "pending_creates": sorted(self._pending_create_levels()),
                    "execution_fail_closed": self._execution_fail_closed_reason is not None,
                    "last_error": self._execution_fail_closed_reason or str(exc),
                    "diagnostics_state": "ERROR_PATH_REPORTED",
                    "updated_at": now,
                }
            )

    def _stop(self, executor: Any, now: float, reason: str) -> StopExecutorAction | None:
        executor_id = str(getattr(executor, "id", ""))
        if not executor_id or executor_id in self._pending_stops:
            return None
        self._pending_stops.add(executor_id)
        try:
            created_at = float(getattr(executor, "timestamp", now))
            self._quote_lifetimes_seconds.append(Decimal(str(max(0.0, now - created_at))))
        except (TypeError, ValueError, ArithmeticError):
            pass
        replace_reasons = {
            "FAST_ADVERSE_MOVE",
            "NORMAL_REFRESH",
            "NORMAL_REFRESH_FAVORABLE",
            "DERIVE_STALENESS_REFRESH",
            "MAX_QUOTE_AGE_REFRESH",
            "INVENTORY_REFRESH",
            "REGIME_WIDEN",
        }
        mutation = "replace" if reason in replace_reasons else "cancel"
        self._record_mutation(now, mutation)
        if mutation == "replace":
            self._replacement_count += 1
        else:
            self._cancel_count += 1
        if mutation == "replace":
            self._replacement_reason_counts[reason] = self._replacement_reason_counts.get(reason, 0) + 1
            if reason == "DERIVE_STALENESS_REFRESH":
                self._derive_refresh_count += 1
        if reason.startswith("BINANCE_TRUE_SHOCK_") and reason.endswith("_BID"):
            self._binance_emergency_cancels["bid"] += 1
        elif reason.startswith("BINANCE_TRUE_SHOCK_") and reason.endswith("_ASK"):
            self._binance_emergency_cancels["ask"] += 1
        level = str(getattr(getattr(executor, "config", None), "level_id", ""))
        self._pending_stop_levels[executor_id] = level
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
        pending_cancel_levels = self._reconcile_pending_stops()
        position_amount = Decimal(str(self.processed_data.get("current_position_amount", ZERO) or ZERO))
        derive_bbo = self.processed_data.get("derive_bbo") or [ZERO, ZERO]
        mark_price = (Decimal(str(derive_bbo[0])) + Decimal(str(derive_bbo[1]))) / Decimal("2")
        with self._portfolio_lock:
            self._prune_reservations(now, active)
            risk_updated_at = float(self.processed_data.get("risk_snapshot_updated_at", 0.0) or 0.0)
            self._publish_portfolio(position_amount, mark_price, active, risk_updated_at)
        self.processed_data["pending_cancels"] = sorted(pending_cancel_levels)
        self.processed_data["pending_creates"] = sorted(self._pending_create_levels())
        actions: list[ExecutorAction] = []
        for executor in self._unexpected_active:
            action = self._stop(executor, now, "UNEXPECTED_OR_DUPLICATE_EXECUTOR")
            if action:
                actions.append(action)
        if actions:
            self.processed_data["last_actions"] = dict(self._last_action)
            return actions
        armed = not self.config.shadow_mode and self.config.mainnet_armed and not self.config.manual_kill_switch
        if not armed or self._plan is None or self._plan.mm_mode == MMMode.PAUSED:
            safety_reason = "STOP_CANCEL" if self.config.manual_kill_switch else "SAFETY_CANCEL"
            for executor in active.values():
                action = self._stop(executor, now, safety_reason)
                if action:
                    actions.append(action)
            self.processed_data["last_actions"] = dict(self._last_action)
            return actions

        observed_failure = self._observe_execution_failures()
        if observed_failure and self._execution_fail_closed_reason is None:
            self._execution_fail_closed_reason = observed_failure
        if self._execution_fail_closed_reason is not None:
            self.processed_data["operational_state"] = OperationalState.ERROR.value
            self.processed_data["execution_fail_closed"] = True
            self.processed_data["last_error"] = self._execution_fail_closed_reason
            self.processed_data["block_reason"] = "EXECUTION_FAIL_CLOSED"
            self.processed_data["diagnostics_state"] = "FAIL_CLOSED"
            for executor in active.values():
                action = self._stop(executor, now, "EXECUTION_FAIL_CLOSED")
                if action:
                    actions.append(action)
            self.processed_data["last_actions"] = dict(self._last_action)
            return actions

        desired = {
            "bid": (TradeType.BUY, self._plan.bid_price),
            "ask": (TradeType.SELL, self._plan.ask_price),
        }
        if self._price_tick is None:
            return []
        tick = self._price_tick
        toxicity_side = self.processed_data.get("binance_toxicity_side")
        for level, (side, desired_price) in desired.items():
            current = active.get(level)
            if current is not None:
                if desired_price is None:
                    reason = (
                        self._binance_emergency_reason(level)
                        if toxicity_side == level
                        else "INVENTORY_REFRESH"
                        if self.processed_data.get("inventory_mode") in {
                            InventoryMode.ASK_ONLY.value,
                            InventoryMode.BID_ONLY.value,
                        }
                        else "INVENTORY_OR_RISK_CANCEL"
                    )
                    action = self._stop(current, now, reason)
                    if action:
                        actions.append(action)
                    continue
                if toxicity_side == level:
                    # A Binance shock is an emergency vulnerable-side action,
                    # not a normal replacement.  It bypasses residency and
                    # leaves the opposite safe side eligible to remain.
                    action = self._stop(current, now, self._binance_emergency_reason(level))
                    if action:
                        actions.append(action)
                    continue
                config = getattr(current, "config", None)
                current_price = Decimal(str(getattr(config, "price", ZERO)))
                age = Decimal(str(max(0.0, now - float(getattr(current, "timestamp", now)))))
                quote_fair_at_creation = self._quote_fair_anchor(level, current, self._plan.fair_value)
                derive_staleness = quote_staleness_bps(self._plan.fair_value, quote_fair_at_creation)
                self.processed_data[f"{level}_derive_fair_value_at_creation"] = quote_fair_at_creation
                self.processed_data[f"{level}_derive_staleness_bps"] = derive_staleness
                self.processed_data[f"{level}_quote_age_seconds"] = age
                max_age_due = (
                    self.config.max_normal_quote_age_seconds is not None
                    and age >= self.config.max_normal_quote_age_seconds
                )
                refresh_reason = (
                    "MAX_QUOTE_AGE_REFRESH"
                    if max_age_due
                    else self._normal_refresh_reason(level, max_age_due=max_age_due)
                )
                refresh, reason = should_refresh(
                    side=side,
                    current_price=current_price,
                    desired_price=desired_price,
                    tick_size=tick,
                    age_seconds=age,
                    minimum_residency_seconds=self.config.minimum_normal_quote_residency_seconds,
                    deadband_bps=self.config.normal_refresh_deadband_bps,
                    fast_adverse=False,
                    derive_staleness_bps=(
                        derive_staleness if self._quote_fair_anchor_trusted.get(level, False) else None
                    ),
                    refresh_reason=refresh_reason,
                )
                if max_age_due and age >= self.config.minimum_normal_quote_residency_seconds:
                    # A quote-age ceiling is a normal lifecycle reason and
                    # remains subject to the same cancel-confirm-create gate.
                    refresh, reason = True, "MAX_QUOTE_AGE_REFRESH"
                self._last_action[level] = reason
                budget_available = self._mutations_last_minute(now) < self.config.max_quote_mutations_per_minute
                if refresh and budget_available:
                    action = self._stop(current, now, reason)
                    if action:
                        actions.append(action)
        self.processed_data.update(
            {
                "last_actions": dict(self._last_action),
                **self._quote_lifetime_metrics(),
                "replacements_total": self._replacement_count,
                "cancels_total": self._cancel_count,
                "derive_normal_refresh_count": self._derive_refresh_count,
                "binance_emergency_bid_cancels": self._binance_emergency_cancels["bid"],
                "binance_emergency_ask_cancels": self._binance_emergency_cancels["ask"],
                "replacement_reason_counts": dict(self._replacement_reason_counts),
            }
        )
        if actions:
            return actions

        if pending_cancel_levels:
            self.processed_data["block_reason"] = "PENDING_CANCEL_CONFIRMATION"
            for level, (_, price) in desired.items():
                if level not in active and price is not None:
                    self._last_action[level] = "PENDING_CANCEL_CONFIRMATION"
                    self.processed_data[f"{level}_size_block_reason"] = "PENDING_CANCEL_CONFIRMATION"
            return []

        guard_blocked = {
            level
            for level, (_, price) in desired.items()
            if price is not None and level not in active and self._self_crossing_level(level, price, active, {
                other_level: other_price
                for other_level, (_, other_price) in desired.items()
            })
        }
        if guard_blocked:
            self.processed_data["block_reason"] = "SELF_CROSS_GUARD"

        for level, (side, price) in desired.items():
            if level in active or price is None:
                if price is None and toxicity_side == level:
                    self._last_action[level] = self._binance_emergency_reason(level)
                continue
            if level in guard_blocked:
                self._last_action[level] = "SELF_CROSS_GUARD"
                self.processed_data[f"{level}_size_block_reason"] = "SELF_CROSS_GUARD"
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
                position_action=(
                    PositionAction.CLOSE
                    if fill_effect in {FillEffect.REDUCE, FillEffect.FLATTEN}
                    else PositionAction.OPEN
                ),
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                leverage=self.config.leverage,
            )
            actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=executor_config))
            self._record_mutation(now, "create")
            # Capture the fair value used for this quote so future normal
            # refreshes are explicitly Derive-staleness comparisons.
            self._quote_fair_at_creation[level] = (now, self._plan.fair_value)
            self._last_action[level] = "CREATE" if size_reason == "READY" else f"CREATE_{size_reason}"
        self.processed_data["last_actions"] = dict(self._last_action)
        return actions

    def get_custom_info(self) -> dict[str, Any]:
        now = self.market_data_provider.time()
        active = self._active()
        pending_cancel_levels = self._reconcile_pending_stops()
        details = dict(self.processed_data)
        pnl = sum((Decimal(str(getattr(item, "net_pnl_quote", ZERO) or ZERO)) for item in self.executors_info), ZERO)
        self._peak_pnl = max(self._peak_pnl, pnl)
        volume_metrics = self._volume_metrics(now, pnl)
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
                **self._quote_lifetime_metrics(),
                "replacements_total": self._replacement_count,
                "cancels_total": self._cancel_count,
                "fills": volume_metrics["fills"],
                "volume": volume_metrics["maker_volume_quote"],
                "maker_volume_quote": volume_metrics["maker_volume_quote"],
                "maker_volume_per_hour": volume_metrics["maker_volume_per_hour"],
                "maker_volume_per_day": volume_metrics["maker_volume_per_day"],
                "capital_turnover_per_day": volume_metrics["capital_turnover_per_day"],
                "fills_per_hour": volume_metrics["fills_per_hour"],
                "maker_fees_quote": volume_metrics["maker_fees_quote"],
                "gross_spread_capture_quote": volume_metrics["gross_spread_capture_quote"],
                "inventory_pnl_quote": volume_metrics["inventory_pnl_quote"],
                "pnl_per_1000_volume": volume_metrics["pnl_per_1000_volume"],
                "pnl_per_fill": volume_metrics["pnl_per_fill"],
                "quote_uptime_pct": volume_metrics["quote_uptime_pct"],
                "profitable_volume_efficiency": volume_metrics["profitable_volume_efficiency"],
                "profitable_volume_efficiency_valid": volume_metrics["profitable_volume_efficiency_valid"],
                "pnl": pnl,
                "drawdown": self._peak_pnl - pnl,
                "strategy_executor_pnl": pnl,
                "strategy_executor_drawdown": self._peak_pnl - pnl,
                "markout_5s_bps": self.processed_data.get("markout_5s_bps"),
                "markout_30s_bps": self.processed_data.get("markout_30s_bps"),
                "markout_60s_bps": self.processed_data.get("markout_60s_bps"),
                "markout_300s_bps": self.processed_data.get("markout_300s_bps"),
                "toxicity_guard_active": self._toxicity_guard_active,
                "toxicity_guard_activations": self._toxicity_guard_activations,
                "binance_shock_state": self._binance_shock_state,
                "binance_emergency_conditions": self._binance_emergency_conditions,
                "binance_emergency_active": self._binance_emergency_active,
                "binance_emergency_recovery_since": self._binance_recovery_since,
                "derive_normal_refresh_count": self._derive_refresh_count,
                "binance_emergency_bid_cancels": self._binance_emergency_cancels["bid"],
                "binance_emergency_ask_cancels": self._binance_emergency_cancels["ask"],
                "replacement_reason_counts": dict(self._replacement_reason_counts),
                "uptime_seconds": max(0.0, now - self._started_at),
                "shadow_mode": self.config.shadow_mode,
                "mainnet_armed": self.config.mainnet_armed,
                "pending_cancels": sorted(pending_cancel_levels),
                "pending_creates": sorted(self._pending_create_levels()),
                "execution_fail_closed": self._execution_fail_closed_reason is not None,
                "last_error": self._execution_fail_closed_reason or details.get("last_error"),
                "diagnostics_state": details.get("diagnostics_state", "HEALTHY"),
                "feed_health": {
                    "derive": details.get("derive_freshness_state", "UNKNOWN"),
                    "binance": details.get("binance_freshness_state", "UNKNOWN"),
                },
            }
        )
        return details

    def to_format_status(self) -> list[str]:
        row = self.get_custom_info()
        return [
            f"DERIVE BINANCE ADAPTIVE MM {self.config.asset}",
            f"state={row.get('market_state', 'WAITING')} mode={row.get('mm_mode', 'PAUSED')} operational={row.get('operational_state', 'ERROR')}",
            f"derive_fair={row.get('derive_fair_value', row.get('binance_fair_value', '—'))} basis_residual_bps={row.get('basis_residual_bps', '—')} inventory={row.get('inventory_mode', '—')}",
            f"desired={row.get('desired_bid', '—')}/{row.get('desired_ask', '—')} active={row.get('active_bid', '—')}/{row.get('active_ask', '—')}",
        ]
