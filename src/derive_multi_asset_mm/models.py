"""Shared immutable contracts for the strategy and its shadow research path."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any

ZERO = Decimal("0")
BPS = Decimal("10000")


class Mode(StrEnum):
    MAINNET_SHADOW = "MAINNET_SHADOW"
    MAINNET_LIVE = "MAINNET_LIVE"


class MarketMode(StrEnum):
    AGGRESSIVE = "AGGRESSIVE"
    NORMAL = "NORMAL"
    DEFENSIVE = "DEFENSIVE"
    PAUSED = "PAUSED"


class DirectionState(StrEnum):
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"


class VolatilityState(StrEnum):
    LOW_VOL = "LOW_VOL"
    NORMAL_VOL = "NORMAL_VOL"
    HIGH_VOL = "HIGH_VOL"
    EXTREME_VOL = "EXTREME_VOL"


class InventoryMode(StrEnum):
    FLAT = "FLAT"
    LONG_SKEW = "LONG_SKEW"
    SHORT_SKEW = "SHORT_SKEW"
    ASK_ONLY = "ASK_ONLY"
    BID_ONLY = "BID_ONLY"


class QuotePlacement(StrEnum):
    AT_TOUCH = "AT_TOUCH"
    IMPROVE_BY_ONE_TICK = "IMPROVE_BY_ONE_TICK"


class ReferenceControl(StrEnum):
    """Fair-value controls compared against the same Derive observations."""

    DERIVE_ONLY = "DERIVE_ONLY"
    BINANCE_ONLY_REFERENCE = "BINANCE_ONLY_REFERENCE"
    BINANCE_ONLY_NO_FAILOVER = "BINANCE_ONLY_NO_FAILOVER"
    PRIORITY_FAILOVER = "PRIORITY_FAILOVER"
    MULTI_SOURCE_CONSENSUS = "MULTI_SOURCE_CONSENSUS"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class LifecycleState(StrEnum):
    NO_ORDER = "NO_ORDER"
    ACTIVE_MATCHING = "ACTIVE_MATCHING"
    REFRESH_NEEDED = "REFRESH_NEEDED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    WAITING_CANCEL = "WAITING_CANCEL"
    READY_TO_CREATE = "READY_TO_CREATE"
    CREATE_REQUESTED = "CREATE_REQUESTED"


@dataclass(frozen=True)
class BookSnapshot:
    """A causal BBO plus optional depth rows."""

    timestamp: float
    best_bid: Decimal
    best_ask: Decimal
    bid_size: Decimal
    ask_size: Decimal
    bids: tuple[tuple[Decimal, Decimal], ...] = ()
    asks: tuple[tuple[Decimal, Decimal], ...] = ()
    exchange_timestamp: float | None = None
    source: str = "unknown"

    @property
    def mid(self) -> Decimal:
        return (self.best_bid + self.best_ask) / Decimal("2")

    @property
    def spread(self) -> Decimal:
        return self.best_ask - self.best_bid

    def valid(self) -> bool:
        return (
            self.best_bid > ZERO
            and self.best_ask >= self.best_bid
            and self.bid_size >= ZERO
            and self.ask_size >= ZERO
        )


@dataclass(frozen=True)
class TradePrint:
    timestamp: float
    price: Decimal
    amount: Decimal
    side: Side
    trade_id: str | None = None
    exchange_timestamp: float | None = None
    source: str = "unknown"


@dataclass(frozen=True)
class DeriveRules:
    instrument_name: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    amount_step: Decimal
    minimum_amount: Decimal
    maximum_amount: Decimal | None = None
    minimum_notional: Decimal = ZERO
    maker_fee_bps: Decimal | None = None
    taker_fee_bps: Decimal | None = None

    def valid(self) -> bool:
        return (
            bool(self.instrument_name)
            and self.tick_size > ZERO
            and self.amount_step > ZERO
            and self.minimum_amount > ZERO
        )


@dataclass(frozen=True)
class AssetSpec:
    symbol: str
    enabled: bool = True
    order_size_multiplier: Decimal = Decimal("1")
    max_inventory_per_asset: Decimal | None = None


@dataclass(frozen=True)
class ReferenceMarket:
    """An exact, currently validated public market for one reference venue."""

    asset: str
    venue: str
    connector: str
    symbol: str | None
    status: str
    reason: str = ""
    contract_type: str = "perpetual"
    underlying: str | None = None
    quote: str = "USDT"
    amount_multiplier: Decimal = Decimal("1")
    tick_size: Decimal | None = None
    amount_step: Decimal | None = None
    minimum_amount: Decimal | None = None
    minimum_notional: Decimal | None = None

    @property
    def ready(self) -> bool:
        return self.status == "READY" and bool(self.symbol)


@dataclass(frozen=True)
class AssetMapping:
    asset: str
    derive_instrument: str | None
    derive_pair: str | None
    binance_symbol: str | None
    reference_type: str = "BINANCE_USDM_PERPETUAL"
    reference_available: bool = False
    valid: bool = False
    reason: str = "UNVALIDATED"
    rules: DeriveRules | None = None
    reference_markets: tuple[ReferenceMarket, ...] = ()


@dataclass(frozen=True)
class FairValue:
    binance_mid: Decimal
    binance_microprice: Decimal
    top_n_imbalance: Decimal
    fair_value_raw: Decimal
    baseline_basis_bps: Decimal
    basis_bps: Decimal
    derive_fair_value: Decimal
    ewma_basis_bps: Decimal = ZERO
    source_fair_values: dict[str, Decimal] = field(default_factory=dict)
    source_mids: dict[str, Decimal] = field(default_factory=dict)
    valid_sources: tuple[str, ...] = ()
    outliers: tuple[str, ...] = ()
    dispersion_bps: Decimal | None = None
    confidence: str = "PAUSED"
    pause_reason: str = ""
    reference_control: str = "BINANCE_ONLY_REFERENCE"


@dataclass(frozen=True)
class SourceFairValue:
    venue: str
    fair_value: Decimal
    mid: Decimal
    microprice: Decimal
    imbalance: Decimal
    timestamp: float
    age_seconds: Decimal
    health: str
    deviation_bps: Decimal | None = None


@dataclass(frozen=True)
class ConsensusReference:
    """Robust multi-source reference decision at one causal timestamp."""

    fair_value: Decimal | None
    robust_median: Decimal | None
    source_values: tuple[SourceFairValue, ...]
    valid_sources: tuple[str, ...]
    outliers: tuple[str, ...]
    dispersion_bps: Decimal | None
    confidence: str
    pause_reason: str = ""

    @property
    def source_count(self) -> int:
        return len(self.valid_sources)


@dataclass(frozen=True)
class MarketState:
    market_mode: MarketMode
    direction: DirectionState
    volatility: VolatilityState
    return_1s: Decimal = ZERO
    return_5s: Decimal = ZERO
    return_15s: Decimal = ZERO
    realized_vol_30s: Decimal = ZERO
    realized_vol_60s: Decimal = ZERO
    velocity_bps: Decimal = ZERO
    bbo_changes_per_minute: Decimal = ZERO
    reason: str = ""


@dataclass(frozen=True)
class EdgeBreakdown:
    maker_fee_bps: Decimal
    volatility_buffer_bps: Decimal
    latency_buffer_bps: Decimal
    toxicity_buffer_bps: Decimal
    minimum_profit_bps: Decimal
    mode_adjustment_bps: Decimal
    total_required_bps: Decimal


@dataclass(frozen=True)
class QuotePlan:
    asset: str
    derive_pair: str | None
    market_mode: MarketMode
    direction: DirectionState
    inventory_mode: InventoryMode
    bid_price: Decimal | None
    bid_amount: Decimal
    ask_price: Decimal | None
    ask_amount: Decimal
    buy_edge_bps: Decimal
    sell_edge_bps: Decimal
    edge: EdgeBreakdown
    reservation_price: Decimal
    local_inventory_skew_bps: Decimal
    portfolio_skew_bps: Decimal
    block_reason: str = ""
    bid_reason: str = ""
    ask_reason: str = ""

    @property
    def bid_notional(self) -> Decimal:
        return (self.bid_price or ZERO) * self.bid_amount

    @property
    def ask_notional(self) -> Decimal:
        return (self.ask_price or ZERO) * self.ask_amount


@dataclass(frozen=True)
class InventorySnapshot:
    amount: Decimal
    mid_price: Decimal
    position_notional: Decimal
    ratio: Decimal
    mode: InventoryMode


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str
    notional: Decimal = ZERO
    rounded_price: Decimal = ZERO
    rounded_amount: Decimal = ZERO


@dataclass(frozen=True)
class FillRecord:
    timestamp: float
    asset: str
    side: Side
    amount: Decimal
    fill_price: Decimal
    binance_fair_value: Decimal
    derive_mid: Decimal
    inventory_before: Decimal
    inventory_after: Decimal
    maker_fee_bps: Decimal
    market_mode: MarketMode
    direction: DirectionState
    basis_bps: Decimal
    quoted_edge_bps: Decimal
    model: str
    reference_control: str = "BINANCE_ONLY_REFERENCE"


@dataclass(frozen=True)
class MarkoutRecord:
    fill_timestamp: float
    horizon_seconds: int
    asset: str
    side: Side
    reference_price: Decimal
    derive_mid: Decimal
    binance_markout_bps: Decimal
    derive_markout_bps: Decimal
    model: str
    reference_control: str = "BINANCE_ONLY_REFERENCE"


@dataclass
class RuntimeCounters:
    quote_creates: int = 0
    holds: int = 0
    replaces: int = 0
    cancels: int = 0
    touch_fills: int = 0
    conservative_fills: int = 0
    no_op_holds: int = 0
    actions: int = 0


def enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def decimal_string(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def json_safe(value: Any) -> Any:
    """Convert strategy objects to stable JSON-compatible values."""

    if isinstance(value, Decimal):
        return decimal_string(value)
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: json_safe(getattr(value, key)) for key in value.__dataclass_fields__}
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value
