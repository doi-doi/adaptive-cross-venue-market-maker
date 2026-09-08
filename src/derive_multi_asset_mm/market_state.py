"""Causal short-horizon direction, volatility, and market-mode state."""

from __future__ import annotations

from collections import deque
from decimal import Decimal
from math import sqrt

from .models import BPS, ZERO, BookSnapshot, DirectionState, MarketMode, MarketState, VolatilityState
from .reference import top_n_imbalance


class TimedPriceSeries:
    def __init__(self, max_seconds: float = 300.0) -> None:
        self.max_seconds = max_seconds
        self._points: deque[tuple[float, Decimal]] = deque()

    def add(self, timestamp: float, price: Decimal) -> None:
        self._points.append((timestamp, price))
        cutoff = timestamp - self.max_seconds
        while self._points and self._points[0][0] < cutoff:
            self._points.popleft()

    def price_at_or_before(self, timestamp: float) -> Decimal | None:
        selected = None
        for point_timestamp, price in reversed(self._points):
            if point_timestamp <= timestamp:
                selected = price
                break
        return selected

    def return_bps(self, now: float, horizon_seconds: float) -> Decimal:
        if not self._points:
            return ZERO
        current = self._points[-1][1]
        old = self.price_at_or_before(now - horizon_seconds)
        if old is None or old <= ZERO:
            return ZERO
        return (current / old - Decimal("1")) * BPS

    def realized_vol_bps(self, now: float, horizon_seconds: float) -> Decimal:
        cutoff = now - horizon_seconds
        points = [(timestamp, price) for timestamp, price in self._points if timestamp >= cutoff]
        if len(points) < 2:
            return ZERO
        squared = []
        previous = points[0][1]
        for _, current in points[1:]:
            if previous > ZERO and current > ZERO:
                change = (current / previous - Decimal("1")) * BPS
                squared.append(float(change * change))
            previous = current
        if not squared:
            return ZERO
        return Decimal(str(sqrt(sum(squared))))

    @property
    def count(self) -> int:
        return len(self._points)


def classify_direction(score_bps: Decimal, threshold_bps: Decimal) -> DirectionState:
    if score_bps >= threshold_bps:
        return DirectionState.BULLISH
    if score_bps <= -threshold_bps:
        return DirectionState.BEARISH
    return DirectionState.NEUTRAL


def classify_volatility(
    realized_vol_60s: Decimal,
    *,
    high_threshold_bps: Decimal,
    extreme_threshold_bps: Decimal,
    low_threshold_bps: Decimal = Decimal("1"),
) -> VolatilityState:
    if realized_vol_60s >= extreme_threshold_bps:
        return VolatilityState.EXTREME_VOL
    if realized_vol_60s >= high_threshold_bps:
        return VolatilityState.HIGH_VOL
    if realized_vol_60s <= low_threshold_bps:
        return VolatilityState.LOW_VOL
    return VolatilityState.NORMAL_VOL


def classify_market_mode(
    *,
    derive_book: BookSnapshot,
    binance_book: BookSnapshot,
    now: float,
    bbo_stale_seconds: Decimal,
    reference_stale_seconds: Decimal,
    volatility: VolatilityState,
    aggressive_spread_max_bps: Decimal,
    defensive_spread_min_bps: Decimal,
    divergence_protected: bool,
    fast_move_bps: Decimal = ZERO,
) -> tuple[MarketMode, str]:
    derive_age = Decimal(str(max(0.0, now - derive_book.timestamp)))
    reference_age = Decimal(str(max(0.0, now - binance_book.timestamp)))
    if derive_age > bbo_stale_seconds:
        return MarketMode.PAUSED, "DERIVE_STALE"
    if reference_age > reference_stale_seconds:
        return MarketMode.PAUSED, "REFERENCE_STALE"
    if divergence_protected:
        return MarketMode.PAUSED, "REFERENCE_DIVERGENCE_PROTECTION"
    if fast_move_bps > ZERO:
        return MarketMode.PAUSED, "FAST_REFERENCE_MOVE"
    if volatility == VolatilityState.EXTREME_VOL:
        return MarketMode.PAUSED, "EXTREME_VOL"
    spread_bps = (derive_book.spread / derive_book.mid * BPS) if derive_book.mid > ZERO else ZERO
    if volatility == VolatilityState.HIGH_VOL:
        return MarketMode.DEFENSIVE, "HIGH_VOL"
    if spread_bps < defensive_spread_min_bps:
        return MarketMode.DEFENSIVE, "DERIVE_SPREAD_TOO_TIGHT"
    if volatility in {VolatilityState.LOW_VOL, VolatilityState.NORMAL_VOL} and spread_bps <= aggressive_spread_max_bps:
        return MarketMode.AGGRESSIVE, "HEALTHY_SPREAD_AND_VOL"
    return MarketMode.NORMAL, "NORMAL_MARKET_STATE"


class MarketStateEngine:
    """Per-asset state engine. It never fills missing observations forward."""

    def __init__(self, *, max_history_seconds: float = 300.0) -> None:
        self.prices = TimedPriceSeries(max_history_seconds)
        self.last_state: MarketState | None = None

    def update(
        self,
        *,
        derive_book: BookSnapshot,
        binance_book: BookSnapshot,
        now: float,
        bbo_stale_seconds: Decimal,
        reference_stale_seconds: Decimal,
        direction_threshold_bps: Decimal,
        high_vol_threshold_bps: Decimal,
        extreme_vol_threshold_bps: Decimal,
        aggressive_spread_max_bps: Decimal,
        defensive_spread_min_bps: Decimal,
        divergence_protected: bool,
        max_levels: int,
        fast_move_threshold_bps: Decimal = ZERO,
    ) -> MarketState:
        self.prices.add(now, binance_book.mid)
        return_1s = self.prices.return_bps(now, 1)
        return_5s = self.prices.return_bps(now, 5)
        return_15s = self.prices.return_bps(now, 15)
        rv_30s = self.prices.realized_vol_bps(now, 30)
        rv_60s = self.prices.realized_vol_bps(now, 60)
        imbalance = top_n_imbalance(binance_book, max_levels)
        direction_score = return_5s + imbalance * max(direction_threshold_bps, Decimal("1"))
        direction = classify_direction(direction_score, direction_threshold_bps)
        volatility = classify_volatility(
            rv_60s,
            high_threshold_bps=high_vol_threshold_bps,
            extreme_threshold_bps=extreme_vol_threshold_bps,
        )
        mode, reason = classify_market_mode(
            derive_book=derive_book,
            binance_book=binance_book,
            now=now,
            bbo_stale_seconds=bbo_stale_seconds,
            reference_stale_seconds=reference_stale_seconds,
            volatility=volatility,
            aggressive_spread_max_bps=aggressive_spread_max_bps,
            defensive_spread_min_bps=defensive_spread_min_bps,
            divergence_protected=divergence_protected,
            fast_move_bps=abs(return_1s) if abs(return_1s) >= fast_move_threshold_bps > ZERO else ZERO,
        )
        state = MarketState(
            market_mode=mode,
            direction=direction,
            volatility=volatility,
            return_1s=return_1s,
            return_5s=return_5s,
            return_15s=return_15s,
            realized_vol_30s=rv_30s,
            realized_vol_60s=rv_60s,
            velocity_bps=abs(return_1s),
            bbo_changes_per_minute=Decimal(str(min(self.prices.count, 60))),
            reason=reason,
        )
        self.last_state = state
        return state
