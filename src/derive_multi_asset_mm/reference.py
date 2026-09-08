"""Binance fair-value and Derive/Binance basis calculations."""

from __future__ import annotations

from collections import deque
from decimal import Decimal
from statistics import median

from .models import BPS, ZERO, BookSnapshot, FairValue


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


class RobustBasis:
    """Rolling median basis with a bounded admission rule.

    The current basis is always reported. Extreme observations are not used to
    move the baseline, which prevents a temporary cross-venue dislocation from
    becoming a new fair-value anchor.
    """

    def __init__(self, window: int, max_deviation_bps: Decimal) -> None:
        if window < 1 or max_deviation_bps <= ZERO:
            raise ValueError("basis window and deviation must be positive")
        self.window = window
        self.max_deviation_bps = max_deviation_bps
        self._values: deque[Decimal] = deque(maxlen=window)

    @property
    def baseline_bps(self) -> Decimal:
        return Decimal(str(median(self._values))) if self._values else ZERO

    def update(self, observed_basis_bps: Decimal) -> tuple[Decimal, Decimal, Decimal, bool]:
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
    )
