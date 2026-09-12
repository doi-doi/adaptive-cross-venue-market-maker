"""Read-only adapters for reports backed by retained telemetry.

Configured shadow runs keep a short raw window and retain minute aggregates
plus decision rollups for the rest of the run.  This module keeps report
code explicit about that boundary: raw rows are preferred for permanent
event evidence, while aggregate-backed metrics are marked as such instead
of being reconstructed as if the deleted detail still existed.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any


def json_object(value: Any) -> dict[str, Any]:
    """Decode a JSON object column without allowing malformed telemetry to abort reporting."""

    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def has_retained_aggregates(rows: list[dict[str, Any]]) -> bool:
    return bool(rows)


def rows_by_asset(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        result[str(row.get("asset"))].append(row)
    return result


def _count(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


def sum_aggregate_field(rows: list[dict[str, Any]], asset: str, field: str) -> int | float:
    values = [_number(row.get(field)) for row in rows if str(row.get("asset")) == asset]
    numbers = [value for value in values if value is not None]
    total = sum(numbers)
    return int(total) if all(float(value).is_integer() for value in numbers) else total


def latest_aggregate(rows: list[dict[str, Any]], asset: str) -> dict[str, Any]:
    candidates = [row for row in rows if str(row.get("asset")) == asset]
    return max(candidates, key=lambda row: float(row.get("last_timestamp") or row.get("timestamp_minute") or 0)) if candidates else {}


def aggregate_json_counts(rows: list[dict[str, Any]], asset: str, field: str) -> Counter[str]:
    result: Counter[str] = Counter()
    for row in rows:
        if str(row.get("asset")) != asset:
            continue
        for key, value in json_object(row.get(field)).items():
            count = _count(value)
            if count:
                result[str(key)] += count
    return result


def aggregate_model_metrics(rows: list[dict[str, Any]], asset: str, model: str) -> dict[str, int | float]:
    totals: dict[str, int | float] = defaultdict(int)
    for row in rows:
        if str(row.get("asset")) != asset:
            continue
        metrics = json_object(row.get("model_metrics_json")).get(model)
        if not isinstance(metrics, dict):
            continue
        for key, value in metrics.items():
            number = _number(value)
            if number is None:
                continue
            totals[str(key)] += number
    return dict(totals)


def aggregate_model_metrics_by_model(rows: list[dict[str, Any]], asset: str) -> dict[str, dict[str, int | float]]:
    result: dict[str, dict[str, int | float]] = {}
    for row in rows:
        if str(row.get("asset")) != asset:
            continue
        metrics = json_object(row.get("model_metrics_json"))
        for model, values in metrics.items():
            if not isinstance(values, dict):
                continue
            target = result.setdefault(str(model), defaultdict(int))
            for key, value in values.items():
                number = _number(value)
                if number is not None:
                    target[str(key)] += number
    return {model: dict(values) for model, values in result.items()}


def aggregate_metric_values(
    rows: list[dict[str, Any]],
    asset: str,
    field: str,
) -> list[float]:
    values = []
    for row in rows:
        if str(row.get("asset")) != asset:
            continue
        value = _number(row.get(field))
        if value is not None:
            values.append(value)
    return values


def aggregate_observation_count(rows: list[dict[str, Any]], asset: str) -> int:
    return int(sum(_count(row.get("observation_count")) for row in rows if str(row.get("asset")) == asset))


def aggregate_rollup_count(rows: list[dict[str, Any]], asset: str) -> int:
    return int(sum(_count(row.get("count")) for row in rows if str(row.get("asset")) == asset))


def _normalise_rollup_controls(payload: dict[str, Any]) -> dict[str, Any]:
    controls = payload.get("controls")
    if not isinstance(controls, dict):
        return payload
    normalised: dict[str, Any] = {}
    for control, value in controls.items():
        if not isinstance(value, dict):
            continue
        pause_reason = value.get("pause_reason")
        normalised[str(control)] = {
            "plan": {
                "bid_price": value.get("desired_bid"),
                "ask_price": value.get("desired_ask"),
                "bid_amount": value.get("desired_bid_amount"),
                "ask_amount": value.get("desired_ask_amount"),
                "market_mode": value.get("market_mode"),
                "block_reason": value.get("block_reason"),
            },
            "state": {
                "market_mode": value.get("market_mode"),
                "direction": value.get("direction"),
                "volatility": value.get("volatility"),
            },
            "consensus": {"pause_reason": pause_reason},
            "fair_value": {"pause_reason": pause_reason},
        }
    result = dict(payload)
    result["controls"] = normalised
    return result


def rollup_decision_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Materialize compact semantic decisions with their repetition count.

    The result is intentionally not presented as raw event detail.  Callers
    can use ``count`` for count-weighted occupancy and the first/last fields
    for event audit, while retaining the aggregate-derived qualifier.
    """

    result = []
    for row in rows:
        payload = _normalise_rollup_controls(json_object(row.get("summary_json")))
        result.append(
            {
                "timestamp": float(row.get("last_timestamp") or row.get("first_timestamp") or 0),
                "first_timestamp": float(row.get("first_timestamp") or 0),
                "last_timestamp": float(row.get("last_timestamp") or 0),
                "asset": row.get("asset"),
                "payload": payload,
                "count": _count(row.get("count")),
                "data_basis": "DECISION_ROLLUP",
            }
        )
    return result


def retention_metadata(
    *,
    aggregates: list[dict[str, Any]],
    rollups: list[dict[str, Any]],
    raw_decision_rows: int,
) -> dict[str, Any]:
    raw_windows = [
        _number(row.get("raw_window_seconds"))
        for row in aggregates
        if _number(row.get("raw_window_seconds")) is not None
    ]
    aggregate_backed = has_retained_aggregates(aggregates)
    return {
        "high_frequency_detail_source": "RAW_DECISIONS" if not aggregate_backed else "RAW_DECISIONS_WITH_MINUTE_AGGREGATES",
        "aggregate_backed": aggregate_backed,
        "raw_decision_rows_retained": raw_decision_rows,
        "decision_rollup_rows": len(rollups),
        "minute_aggregate_rows": len(aggregates),
        "raw_window_seconds": max(raw_windows, default=None),
        "aggregate_metric_qualification": (
            "Minute medians/P90 and count-weighted occupancy are retained; exact event-time distributions older than the raw window are not reconstructed."
            if aggregate_backed
            else "Raw decision detail is available for this store."
        ),
    }


def aggregate_artifact_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return stable, human-readable aggregate rows for report artifacts."""

    result = []
    for row in rows:
        value = dict(row)
        for field in (
            "selected_reference_occupancy_json",
            "reference_health_counts_json",
            "reference_value_stats_json",
            "market_mode_occupancy_json",
            "direction_occupancy_json",
            "volatility_occupancy_json",
            "inventory_mode_occupancy_json",
            "action_counts_json",
            "model_metrics_json",
        ):
            value[field[:-5]] = json_object(value.pop(field))
        result.append(value)
    return result
