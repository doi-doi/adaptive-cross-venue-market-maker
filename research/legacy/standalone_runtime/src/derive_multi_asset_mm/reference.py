"""Reference fair-value, basis, and robust multi-venue aggregation helpers."""

from __future__ import annotations

from collections import deque
from decimal import Decimal
from statistics import median

from .models import BPS, ZERO, BookSnapshot, FairValue, SourceFairValue


def mid_price(book: BookSnapshot) -> Decimal:
    if not book.valid():
        raise ValueError("invalid order book")
    return (book.best_bid + book.best_ask) / Decimal("2")


def microprice(book: BookSnapshot) -> Decimal:
    if not book.valid():
        raise ValueError("invalid order book")
    total = book.bid_size + book.ask_size
    if total <= ZERO:
        return mid_price(book)
    return (book.best_ask * book.bid_size + book.best_bid * book.ask_size) / total


def top_n_imbalance(book: BookSnapshot, levels: int = 5) -> Decimal:
    if levels < 1:
        raise ValueError("levels must be positive")
    bids = book.bids[:levels] or ((book.best_bid, book.bid_size),)
    asks = book.asks[:levels] or ((book.best_ask, book.ask_size),)
    bid_depth = sum((amount for _, amount in bids), ZERO)
    ask_depth = sum((amount for _, amount in asks), ZERO)
    total = bid_depth + ask_depth
    return (bid_depth - ask_depth) / total if total > ZERO else ZERO


def weighted_fair_value(book: BookSnapshot, mid_weight: Decimal, microprice_weight: Decimal) -> Decimal:
    weight_sum = mid_weight + microprice_weight
    if weight_sum <= ZERO:
        raise ValueError("fair-value weights must sum to a positive value")
    return (mid_price(book) * mid_weight + microprice(book) * microprice_weight) / weight_sum


def basis_bps(derive_mid: Decimal, binance_fair_value: Decimal) -> Decimal:
    if derive_mid <= ZERO or binance_fair_value <= ZERO:
        raise ValueError("basis prices must be positive")
    return (derive_mid / binance_fair_value - Decimal("1")) * BPS


def source_fair_value(
    venue: str,
    book: BookSnapshot,
    *,
    mid_weight: Decimal,
    microprice_weight: Decimal,
    max_levels: int,
    now: float,
    health: str,
    deviation_bps: Decimal | None = None,
) -> SourceFairValue:
    """Build a venue-level value without retaining an observation forward."""

    return SourceFairValue(
        venue=venue,
        fair_value=weighted_fair_value(book, mid_weight, microprice_weight),
        mid=mid_price(book),
        microprice=microprice(book),
        imbalance=top_n_imbalance(book, max_levels),
        timestamp=book.timestamp,
        age_seconds=Decimal(str(max(0.0, now - book.timestamp))),
        health=health,
        deviation_bps=deviation_bps,
    )


def aggregate_reference_book(books: dict[str, BookSnapshot]) -> BookSnapshot | None:
    """Create a synthetic BBO from currently eligible books.

    Prices and displayed sizes are component-wise medians. The synthetic book
    is diagnostic only; it never represents an executable order book.
    """

    if not books:
        return None
    ordered = list(books.values())
    bids = sorted(book.best_bid for book in ordered)
    asks = sorted(book.best_ask for book in ordered)
    bid_sizes = sorted(book.bid_size for book in ordered)
    ask_sizes = sorted(book.ask_size for book in ordered)
    bid = Decimal(str(median(bids)))
    ask = Decimal(str(median(asks)))
    if ask < bid:
        return None
    bid_size = Decimal(str(median(bid_sizes)))
    ask_size = Decimal(str(median(ask_sizes)))
    timestamp = max(book.timestamp for book in ordered)
    exchange_times = [book.exchange_timestamp for book in ordered if book.exchange_timestamp is not None]
    return BookSnapshot(
        timestamp=timestamp,
        best_bid=bid,
        best_ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        bids=((bid, bid_size),),
        asks=((ask, ask_size),),
        exchange_timestamp=max(exchange_times) if exchange_times else None,
        source="multi_source_consensus",
    )


def fair_value_from_reference(
    derive_book: BookSnapshot,
    reference_book: BookSnapshot,
    basis_tracker: RobustBasis,
    *,
    raw_fair_value: Decimal,
    source_fair_values: dict[str, Decimal] | None = None,
    source_mids: dict[str, Decimal] | None = None,
    valid_sources: tuple[str, ...] = (),
    outliers: tuple[str, ...] = (),
    dispersion_bps: Decimal | None = None,
    confidence: str = "NORMAL_REFERENCE_CONFIDENCE",
    pause_reason: str = "",
    reference_control: str = "BINANCE_ONLY_REFERENCE",
    max_levels: int = 5,
) -> FairValue:
    """Apply the bounded Derive/reference basis to any reference model."""

    observed_basis = basis_bps(mid_price(derive_book), raw_fair_value)
    current, baseline, _, _ = basis_tracker.update(observed_basis)
    derive_fair = raw_fair_value * (Decimal("1") + baseline / BPS)
    return FairValue(
        binance_mid=mid_price(reference_book),
        binance_microprice=microprice(reference_book),
        top_n_imbalance=top_n_imbalance(reference_book, max_levels),
        fair_value_raw=raw_fair_value,
        baseline_basis_bps=baseline,
        basis_bps=current,
        derive_fair_value=derive_fair,
        ewma_basis_bps=basis_tracker.ewma_bps,
        source_fair_values=source_fair_values or {},
        source_mids=source_mids or {},
        valid_sources=valid_sources,
        outliers=outliers,
        dispersion_bps=dispersion_bps,
        confidence=confidence,
        pause_reason=pause_reason,
        reference_control=reference_control,
    )


class RobustBasis:
    """Rolling median basis with a bounded admission rule.

    The current basis is always reported. Extreme observations are not used to
    move the baseline, which prevents a temporary cross-venue dislocation from
    becoming a new fair-value anchor.
    """

    def __init__(self, window: int, max_deviation_bps: Decimal, ewma_alpha: Decimal = Decimal("0.2")) -> None:
        if window < 1 or max_deviation_bps <= ZERO or ewma_alpha <= ZERO or ewma_alpha > Decimal("1"):
            raise ValueError("basis window, deviation, and EWMA alpha must be positive")
        self.window = window
        self.max_deviation_bps = max_deviation_bps
        self.ewma_alpha = ewma_alpha
        self._values: deque[Decimal] = deque(maxlen=window)
        self._ewma: Decimal | None = None

    @property
    def baseline_bps(self) -> Decimal:
        return Decimal(str(median(self._values))) if self._values else ZERO

    @property
    def ewma_bps(self) -> Decimal:
        return self._ewma if self._ewma is not None else ZERO

    def update(self, observed_basis_bps: Decimal) -> tuple[Decimal, Decimal, Decimal, bool]:
        self._ewma = (
            observed_basis_bps
            if self._ewma is None
            else self.ewma_alpha * observed_basis_bps + (Decimal("1") - self.ewma_alpha) * self._ewma
        )
        baseline = self.baseline_bps
        deviation = observed_basis_bps - baseline
        protected = bool(self._values and abs(deviation) > self.max_deviation_bps)
        if not protected:
            self._values.append(observed_basis_bps)
            baseline = self.baseline_bps
            deviation = observed_basis_bps - baseline
        return observed_basis_bps, baseline, deviation, protected


def build_fair_value(
    derive_book: BookSnapshot,
    binance_book: BookSnapshot,
    basis_tracker: RobustBasis,
    *,
    mid_weight: Decimal,
    microprice_weight: Decimal,
    max_levels: int,
) -> FairValue:
    reference_mid = mid_price(binance_book)
    reference_micro = microprice(binance_book)
    raw = weighted_fair_value(binance_book, mid_weight, microprice_weight)
    observed_basis = basis_bps(mid_price(derive_book), raw)
    current, baseline, _, _ = basis_tracker.update(observed_basis)
    normalized_basis = baseline / BPS
    derive_fair = raw * (Decimal("1") + normalized_basis)
    return FairValue(
        binance_mid=reference_mid,
        binance_microprice=reference_micro,
        top_n_imbalance=top_n_imbalance(binance_book, max_levels),
        fair_value_raw=raw,
        baseline_basis_bps=baseline,
        basis_bps=current,
        derive_fair_value=derive_fair,
        ewma_basis_bps=basis_tracker.ewma_bps,
        source_fair_values={"binance": raw},
        source_mids={"binance": reference_mid},
        valid_sources=("binance",),
        confidence="SINGLE_SOURCE_REFERENCE",
        reference_control="BINANCE_ONLY_REFERENCE",
    )
