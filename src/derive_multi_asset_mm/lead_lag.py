"""Causal reference lead/lag diagnostics without interpolation or forward fill."""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Iterable
from decimal import Decimal
from math import sqrt
from typing import TypeAlias

PricePoint: TypeAlias = tuple[float, Decimal]


def _nearest(points: list[PricePoint], target: float, tolerance_seconds: float) -> PricePoint | None:
    """Return the nearest observed point without interpolating or forward filling."""

    if not points:
        return None
    insertion = bisect_left(points, (target, Decimal("-Infinity")))
    candidates = []
    if insertion < len(points):
        candidates.append(points[insertion])
    if insertion > 0:
        candidates.append(points[insertion - 1])
    nearest = min(candidates, key=lambda point: abs(point[0] - target))
    return nearest if abs(nearest[0] - target) <= tolerance_seconds else None


def _return(start: Decimal, end: Decimal) -> float | None:
    if start <= 0 or end <= 0:
        return None
    return float((end - start) / start)


def _correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) < 3 or len(left) != len(right):
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    covariance = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right, strict=True))
    left_scale = sqrt(sum((a - left_mean) ** 2 for a in left))
    right_scale = sqrt(sum((b - right_mean) ** 2 for b in right))
    if left_scale == 0 or right_scale == 0:
        return None
    return covariance / (left_scale * right_scale)


def estimate_lead_lag(
    reference_points: Iterable[PricePoint],
    derive_points: Iterable[PricePoint],
    *,
    lags_seconds: tuple[float, ...] = (0.1, 0.25, 0.5, 1, 2, 5),
    return_horizon_seconds: float = 1,
    tolerance_seconds: float = 0.35,
) -> list[dict[str, object]]:
    """Compare same-horizon returns at explicit time offsets.

    A row at lag ``k`` compares a reference return starting at ``t`` with a Derive
    return starting at ``t + k``. Only observed points within the declared
    tolerance are paired; missing observations are omitted rather than filled.
    """

    reference = sorted((float(timestamp), Decimal(str(price))) for timestamp, price in reference_points)
    derive = sorted((float(timestamp), Decimal(str(price))) for timestamp, price in derive_points)
    results: list[dict[str, object]] = []
    for lag in lags_seconds:
        reference_returns: list[float] = []
        derive_returns: list[float] = []
        for timestamp, start_price in reference:
            reference_end = _nearest(reference, timestamp + return_horizon_seconds, tolerance_seconds)
            derive_start = _nearest(derive, timestamp + lag, tolerance_seconds)
            derive_end = _nearest(derive, timestamp + lag + return_horizon_seconds, tolerance_seconds)
            if reference_end is None or derive_start is None or derive_end is None:
                continue
            reference_return = _return(start_price, reference_end[1])
            derive_return = _return(derive_start[1], derive_end[1])
            if reference_return is not None and derive_return is not None:
                reference_returns.append(reference_return)
                derive_returns.append(derive_return)
        correlation = _correlation(reference_returns, derive_returns)
        results.append(
            {
                "lag_seconds": lag,
                "return_horizon_seconds": return_horizon_seconds,
                "observations": len(reference_returns),
                "correlation": Decimal(str(correlation)) if correlation is not None else None,
                "status": "READY" if correlation is not None else "INSUFFICIENT_OBSERVATIONS",
            }
        )
    return results
