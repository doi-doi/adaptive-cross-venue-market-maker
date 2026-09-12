"""Measurement-only exporter for the three-asset six-hour shadow run.

The exporter reads the completed run's SQLite telemetry and state files. It
does not change strategy configuration, orders, or telemetry. When invoked
with ``--wait-for-pid`` it waits in its own detached process and exits after
the shadow runner reaches a terminal state.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
import time
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.quote_fill_diagnostic import export_diagnostics
from derive_multi_asset_mm.retained_reporting import (
    aggregate_artifact_rows,
    aggregate_json_counts,
    aggregate_metric_values,
    aggregate_model_metrics,
    aggregate_observation_count,
    has_retained_aggregates,
    latest_aggregate,
    retention_metadata,
    rollup_decision_rows,
    rows_by_asset,
)

THRESHOLDS = (1, 5, 15, 30)
SPREAD_THRESHOLDS = (2, 4, 6, 8, 10, 15)
PERSISTENCE_THRESHOLDS = (4, 6, 8, 10)
MARKOUT_HORIZONS = (1, 5, 15, 30, 60)
FILL_MODELS = ("CONSERVATIVE", "TOUCH_SENSITIVITY")
REQUIRED_REPORTS = (
    "run_metadata.json",
    "trade_feed_audit.csv",
    "trade_activity.csv",
    "trade_gap_statistics.csv",
    "spread_statistics.csv",
    "spread_persistence.csv",
    "quote_uptime.csv",
    "action_breakdown.csv",
    "quote_churn.csv",
    "reference_usage.csv",
    "reference_health.csv",
    "reference_failovers.csv",
    "reference_disagreement.csv",
    "shadow_fills_conservative.csv",
    "shadow_fills_touch.csv",
    "reference_markouts.csv",
    "derive_markouts.csv",
    "minute_aggregates.csv",
    "decision_rollups.csv",
    "toxicity.csv",
    "net_capture.csv",
    "maker_volume.csv",
    "capital_compatibility.csv",
    "inventory_statistics.csv",
    "portfolio_statistics.csv",
    "model_comparison.csv",
    "asset_ranking.csv",
    "final_6h_validation_report.md",
    "final_6h_validation_report.json",
)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("status\nDATA_INSUFFICIENT\n", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _float(value: Any) -> float | None:
    result = _decimal(value)
    return float(result) if result is not None else None


def _round(value: Any, places: int = 6) -> float | None:
    number = _float(value)
    return round(number, places) if number is not None else None


def _quantile(values: Iterable[Any], probability: float) -> float | None:
    numbers = sorted(number for value in values if (number := _float(value)) is not None and math.isfinite(number))
    if not numbers:
        return None
    if len(numbers) == 1:
        return numbers[0]
    position = (len(numbers) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return numbers[lower]
    return numbers[lower] + (numbers[upper] - numbers[lower]) * (position - lower)


def _mean(values: Iterable[Any]) -> float | None:
    numbers = [number for value in values if (number := _float(value)) is not None]
    return sum(numbers) / len(numbers) if numbers else None


def _median(values: Iterable[Any]) -> float | None:
    return _quantile(values, 0.5)


def _pct(numerator: float, denominator: float) -> float:
    return round(100.0 * numerator / denominator, 6) if denominator > 0 else 0.0


def _iso(timestamp: Any) -> str | None:
    value = _float(timestamp)
    return datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z") if value is not None else None


def _read_rows(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        tables = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        decisions = []
        for row in connection.execute("SELECT timestamp, asset, payload_json FROM decisions ORDER BY timestamp, rowid"):
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError:
                payload = {}
            decisions.append({"timestamp": float(row["timestamp"]), "asset": row["asset"], "payload": payload})
        actions = [dict(row) for row in connection.execute("SELECT * FROM actions ORDER BY timestamp, id")]
        fills = [dict(row) for row in connection.execute("SELECT * FROM fills ORDER BY timestamp, id")]
        markouts = [dict(row) for row in connection.execute("SELECT * FROM markouts ORDER BY fill_timestamp, id")]
        trades = [dict(row) for row in connection.execute("SELECT * FROM trades ORDER BY timestamp, id")]
        health = []
        for row in connection.execute("SELECT timestamp, asset, venue, payload_json FROM reference_health ORDER BY timestamp, id"):
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError:
                payload = {}
            health.append({"timestamp": float(row["timestamp"]), "asset": row["asset"], "venue": row["venue"], **payload})
        aggregates = (
            [dict(row) for row in connection.execute("SELECT * FROM minute_aggregates ORDER BY timestamp_minute, asset")]
            if "minute_aggregates" in tables
            else []
        )
        rollups = (
            [dict(row) for row in connection.execute("SELECT * FROM decision_rollups ORDER BY timestamp_minute, asset, decision_signature")]
            if "decision_rollups" in tables
            else []
        )
        state_row = connection.execute("SELECT value_json FROM state WHERE key='runtime'").fetchone()
        runtime = json.loads(state_row["value_json"]) if state_row else {}
    finally:
        connection.close()
    return {
        "decisions": decisions,
        "actions": actions,
        "fills": fills,
        "markouts": markouts,
        "trades": trades,
        "health": health,
        "runtime": runtime,
        "aggregates": aggregates,
        "rollups": rollups,
    }


def _retained_decisions(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Use semantic rollups for old history once minute aggregates exist."""

    if has_retained_aggregates(data.get("aggregates", [])) and data.get("rollups"):
        return rollup_decision_rows(data["rollups"])
    return data["decisions"]


def _aggregate_rows(data: dict[str, Any], asset: str | None = None) -> list[dict[str, Any]]:
    rows = data.get("aggregates", [])
    return [row for row in rows if asset is None or str(row.get("asset")) == asset]


def _aggregate_health_counts(data: dict[str, Any], asset: str, venue: str) -> Counter[str]:
    counts = aggregate_json_counts(data.get("aggregates", []), asset, "reference_health_counts_json")
    prefix = f"{venue}:"
    return Counter({key.removeprefix(prefix): value for key, value in counts.items() if key.startswith(prefix)})


def _aggregate_source_counts(data: dict[str, Any], asset: str) -> Counter[str]:
    return aggregate_json_counts(data.get("aggregates", []), asset, "selected_reference_occupancy_json")


def _aggregate_rollup_count(data: dict[str, Any], asset: str) -> int:
    return sum(int(row.get("count") or 0) for row in data.get("rollups", []) if str(row.get("asset")) == asset)


def _intervals(rows: list[dict[str, Any]], start: float, end: float) -> list[tuple[dict[str, Any], float]]:
    ordered = sorted(rows, key=lambda row: float(row["timestamp"]))
    result = []
    for index, row in enumerate(ordered):
        timestamp = max(start, float(row["timestamp"]))
        next_timestamp = float(ordered[index + 1]["timestamp"]) if index + 1 < len(ordered) else end
        interval_end = min(end, max(timestamp, next_timestamp))
        if interval_end > timestamp:
            result.append((row, interval_end - timestamp))
    return result


def _rolling_max(timestamps: list[float], window_seconds: float = 60.0) -> int:
    ordered = sorted(timestamps)
    maximum = 0
    left = 0
    for right, timestamp in enumerate(ordered):
        while left <= right and ordered[left] < timestamp - window_seconds:
            left += 1
        maximum = max(maximum, right - left + 1)
    return maximum


def _window_counts(timestamps: list[float], window_seconds: float = 60.0) -> list[int]:
    ordered = sorted(timestamps)
    if not ordered:
        return []
    start = math.floor(ordered[0] / window_seconds) * window_seconds
    end = math.ceil(ordered[-1] / window_seconds) * window_seconds
    counts = []
    cursor = start
    while cursor <= end:
        counts.append(sum(cursor <= value < cursor + window_seconds for value in ordered))
        cursor += window_seconds
    return counts


def _control_payload(row: dict[str, Any], control: str) -> dict[str, Any]:
    controls = row.get("payload", {}).get("controls") or {}
    value = controls.get(control) or {}
    return value if isinstance(value, dict) else {}


def _plan(control_payload: dict[str, Any]) -> dict[str, Any]:
    value = control_payload.get("plan") or {}
    return value if isinstance(value, dict) else {}


def _fair(control_payload: dict[str, Any]) -> dict[str, Any]:
    value = control_payload.get("fair_value") or {}
    return value if isinstance(value, dict) else {}


def _consensus(control_payload: dict[str, Any]) -> dict[str, Any]:
    value = control_payload.get("consensus") or {}
    return value if isinstance(value, dict) else {}


def _block_reason(control_payload: dict[str, Any]) -> str:
    plan = _plan(control_payload)
    fair = _fair(control_payload)
    return str(plan.get("block_reason") or fair.get("pause_reason") or "")


def _block_category(reason: str) -> str:
    text = reason.lower()
    if "stale" in text:
        return "reference_stale" if "reference" in text else "derive_stale"
    if "disagreement" in text:
        return "reference_disagreement"
    if "edge" in text or "profit" in text or "spread" in text:
        return "insufficient_edge"
    if "inventory" in text:
        return "inventory"
    if "risk" in text or "limit" in text or "action" in text:
        return "risk"
    return "other"


def _model_parts(model: str) -> tuple[str, str]:
    control, _, fill_model = str(model).partition(":")
    return control, fill_model or "UNKNOWN"


def _trade_time(row: dict[str, Any]) -> float:
    return _float(row.get("exchange_timestamp")) or _float(row.get("timestamp")) or 0.0


def _run_window(state: dict[str, Any], metadata: dict[str, Any], decisions: list[dict[str, Any]]) -> tuple[float, float, float]:
    start = _float(state.get("started_at")) or _float(metadata.get("start_time_epoch"))
    end = _float(state.get("ended_at")) or _float(metadata.get("planned_end_epoch"))
    timestamps = [row["timestamp"] for row in decisions]
    if start is None:
        start = min(timestamps) if timestamps else time.time()
    if end is None or end <= start:
        end = max(timestamps) if timestamps else start
    return start, end, max(0.0, end - start)


def _trade_reports(data: dict[str, Any], assets: list[str], start: float, end: float, duration: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data["trades"]:
        if row.get("source") == "derive" and row.get("asset") in assets:
            by_asset[str(row["asset"])].append(row)
    activity = []
    gaps = []
    details: dict[str, dict[str, Any]] = {}
    hours = duration / 3600.0 if duration > 0 else 0.0
    for asset in assets:
        rows = sorted(by_asset[asset], key=_trade_time)
        timestamps = [_trade_time(row) for row in rows]
        notionals = [(_decimal(row.get("amount")) or Decimal("0")) * (_decimal(row.get("price")) or Decimal("0")) for row in rows]
        intertrade = [right - left for left, right in zip(timestamps, timestamps[1:], strict=True)]
        segments = ([timestamps[0] - start] if timestamps else []) + intertrade + ([end - timestamps[-1]] if timestamps else [])
        segments = [max(0.0, value) for value in segments]
        largest_gap = max(segments) if segments else duration
        count = len(rows)
        notional = sum(notionals, Decimal("0"))
        trade_row = {
            "asset": asset,
            "source": "derive",
            "observation_seconds": _round(duration, 3),
            "observation_hours": _round(hours, 6),
            "trade_count": count,
            "trades_per_hour": _round(count / hours if hours else None, 6),
            "trade_notional": str(notional),
            "trade_notional_per_hour": _round(float(notional) / hours if hours else None, 6),
            "median_trade_notional": _round(_median(notionals), 6),
            "p90_trade_notional": _round(_quantile(notionals, 0.90), 6),
            "largest_trade_notional": _round(max(notionals, default=None), 6),
            "first_trade_timestamp_utc": _iso(timestamps[0] if timestamps else None),
            "last_trade_timestamp_utc": _iso(timestamps[-1] if timestamps else None),
            "largest_gap_seconds": _round(largest_gap, 6),
            "status": "OBSERVED" if rows else "NO_TRADES_OBSERVED",
        }
        activity.append(trade_row)
        gap_row: dict[str, Any] = {
            "asset": asset,
            "observation_seconds": _round(duration, 3),
            "trade_count": count,
            "median_intertrade_seconds": _round(_median(intertrade), 6),
            "p90_intertrade_seconds": _round(_quantile(intertrade, 0.90), 6),
            "p99_intertrade_seconds": _round(_quantile(intertrade, 0.99), 6),
            "largest_gap_seconds": _round(largest_gap, 6),
            "status": "OBSERVED" if rows else "NO_TRADES_OBSERVED",
        }
        for threshold in THRESHOLDS:
            long_segments = [segment for segment in segments if segment > threshold * 60]
            gap_row[f"gaps_over_{threshold}m_count"] = len(long_segments)
            gap_row[f"no_trade_time_over_{threshold}m_seconds"] = _round(sum(long_segments), 6)
            gap_row[f"no_trade_time_over_{threshold}m_pct"] = _round(_pct(sum(long_segments), duration), 6)
        gaps.append(gap_row)
        details[asset] = {"rows": rows, "timestamps": timestamps, "notionals": notionals, "intertrade": intertrade, "segments": segments}
    return activity, gaps, details


def _feed_audit(data: dict[str, Any], assets: list[str], activity: list[dict[str, Any]], duration: float, state: dict[str, Any]) -> list[dict[str, Any]]:
    activity_by_asset = {row["asset"]: row for row in activity}
    health_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data["health"]:
        if row.get("venue") == "derive":
            health_by_asset[str(row["asset"])].append(row)
    errors = [str(value) for value in state.get("errors", [])]
    rows = []
    decisions = _retained_decisions(data)
    for asset in assets:
        health = health_by_asset[asset]
        health_counts = _aggregate_health_counts(data, asset, "derive")
        updates = max((_float(row.get("updates")) or 0 for row in health), default=0)
        aggregate_observations = aggregate_observation_count(data.get("aggregates", []), asset)
        bbo_observed = updates > 0 or any(
            row.get("payload", {}).get("derive_bid") is not None
            for row in decisions
            if row.get("asset") == asset
        ) or bool(aggregate_observations)
        trade_count = int(activity_by_asset[asset]["trade_count"])
        trades_per_hour = _float(activity_by_asset[asset]["trades_per_hour"]) or 0.0
        asset_errors = [error for error in errors if "derive" in error.lower()]
        if not bbo_observed:
            classification = "TRADE_FEED_FAILED" if asset_errors else "TRADE_FEED_DEGRADED"
        elif trade_count == 0:
            classification = "TRADE_FEED_VERIFIED_NO_TRADES_OCCURRED"
        elif trades_per_hour < 1.0:
            classification = "TRADE_FEED_VERIFIED_LOW_ACTIVITY"
        else:
            classification = "TRADE_FEED_VERIFIED_ACTIVE"
        rows.append(
            {
                "asset": asset,
                "instrument": f"{asset}-PERP",
                "observation_seconds": _round(duration, 3),
                "derive_bbo_observed": bbo_observed,
                "derive_health_observations": len(health) or sum(health_counts.values()),
                "derive_updates_latest": updates or sum(health_counts.values()),
                "trade_count": trade_count,
                "trades_per_hour": trades_per_hour,
                "transport_error_count": len(asset_errors),
                "classification": classification,
                "classification_basis": "Derive BBO/health telemetry observed; zero trades is not classified as transport failure."
                if classification == "TRADE_FEED_VERIFIED_NO_TRADES_OCCURRED"
                else "Derived from direct Derive telemetry and observed transport health.",
                "data_basis": "RAW_HEALTH_AND_DECISIONS" if health or data["decisions"] else "MINUTE_AGGREGATES",
            }
        )
    return rows


def _spread_reports(data: dict[str, Any], assets: list[str], start: float, end: float, duration: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if has_retained_aggregates(data.get("aggregates", [])):
        spread_rows = []
        persistence_rows = []
        for asset in assets:
            aggregate_rows = _aggregate_rows(data, asset)
            medians = aggregate_metric_values(data.get("aggregates", []), asset, "derive_spread_bps_median")
            p90s = aggregate_metric_values(data.get("aggregates", []), asset, "derive_spread_bps_p90")
            observations = aggregate_observation_count(data.get("aggregates", []), asset)
            spread_rows.append(
                {
                    "asset": asset,
                    "observations": observations,
                    "observation_seconds": _round(duration, 3),
                    "median_spread_bps": _round(_quantile(medians, 0.50), 6),
                    "p25_spread_bps": _round(_quantile(medians, 0.25), 6),
                    "p75_spread_bps": _round(_quantile(medians, 0.75), 6),
                    "p90_spread_bps": None,
                    "p95_spread_bps": None,
                    "p99_spread_bps": None,
                    "minute_p90_median_bps": _round(_quantile(p90s, 0.50), 6),
                    "minute_p90_max_bps": _round(max(p90s, default=None), 6),
                    "weighted_decision_seconds": None,
                    "aggregate_minutes": len(aggregate_rows),
                    "statistics_basis": "MINUTE_MEDIANS;_RUN_P90_NOT_IDENTIFIABLE_FROM_RETAINED_SUMMARIES",
                    "status": "AGGREGATE_DERIVED" if medians else "DATA_INSUFFICIENT",
                }
            )
            persistence = {
                "asset": asset,
                "observations": observations,
                "aggregate_minutes": len(aggregate_rows),
                "statistics_basis": "THRESHOLD_EPISODES_NOT_RETAINED_AFTER_RAW_WINDOW",
                "status": "AGGREGATE_DERIVED" if medians else "DATA_INSUFFICIENT",
            }
            for threshold in PERSISTENCE_THRESHOLDS:
                persistence[f"above_{threshold}bps_episode_count"] = None
                persistence[f"above_{threshold}bps_median_seconds"] = None
                persistence[f"above_{threshold}bps_p90_seconds"] = None
                persistence[f"above_{threshold}bps_longest_seconds"] = None
            persistence_rows.append(persistence)
        return spread_rows, persistence_rows

    rows_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data["decisions"]:
        if row.get("asset") in assets:
            rows_by_asset[str(row["asset"])].append(row)
    spread_rows = []
    persistence_rows = []
    for asset in assets:
        decisions = rows_by_asset[asset]
        values = [_float(row.get("payload", {}).get("derive_spread_bps")) for row in decisions]
        values = [value for value in values if value is not None]
        intervals = _intervals(decisions, start, end)
        weighted_seconds = sum(seconds for _, seconds in intervals)
        row = {
            "asset": asset,
            "observations": len(values),
            "observation_seconds": _round(duration, 3),
            "median_spread_bps": _round(_quantile(values, 0.50), 6),
            "p25_spread_bps": _round(_quantile(values, 0.25), 6),
            "p75_spread_bps": _round(_quantile(values, 0.75), 6),
            "p90_spread_bps": _round(_quantile(values, 0.90), 6),
            "p95_spread_bps": _round(_quantile(values, 0.95), 6),
            "p99_spread_bps": _round(_quantile(values, 0.99), 6),
            "weighted_decision_seconds": _round(weighted_seconds, 6),
            "status": "OBSERVED" if values else "DATA_INSUFFICIENT",
        }
        for threshold in SPREAD_THRESHOLDS:
            above = sum(seconds for decision, seconds in intervals if (_float(decision.get("payload", {}).get("derive_spread_bps")) or 0) > threshold)
            row[f"time_above_{threshold}bps_pct"] = _round(_pct(above, duration), 6)
        spread_rows.append(row)
        persistence = {"asset": asset, "observations": len(values), "status": "OBSERVED" if values else "DATA_INSUFFICIENT"}
        for threshold in PERSISTENCE_THRESHOLDS:
            episodes: list[float] = []
            started: float | None = None
            current_end: float | None = None
            for decision, seconds in intervals:
                timestamp = float(decision["timestamp"])
                is_above = (_float(decision.get("payload", {}).get("derive_spread_bps")) or 0) > threshold
                if is_above:
                    started = timestamp if started is None else started
                    current_end = timestamp + seconds
                elif started is not None and current_end is not None:
                    episodes.append(max(0.0, current_end - started))
                    started = None
                    current_end = None
            if started is not None and current_end is not None:
                episodes.append(max(0.0, current_end - started))
            persistence[f"above_{threshold}bps_episode_count"] = len(episodes)
            persistence[f"above_{threshold}bps_median_seconds"] = _round(_median(episodes), 6)
            persistence[f"above_{threshold}bps_p90_seconds"] = _round(_quantile(episodes, 0.90), 6)
            persistence[f"above_{threshold}bps_longest_seconds"] = _round(max(episodes, default=None), 6)
        persistence_rows.append(persistence)
    return spread_rows, persistence_rows


def _quote_uptime(data: dict[str, Any], assets: list[str], controls: tuple[str, ...], start: float, end: float, duration: float) -> list[dict[str, Any]]:
    if has_retained_aggregates(data.get("aggregates", [])) and data.get("rollups"):
        decisions_by_asset = rows_by_asset(_retained_decisions(data))
        result = []
        for asset in assets:
            asset_decisions = decisions_by_asset.get(asset, [])
            total_count = sum(int(row.get("count") or 0) for row in asset_decisions)
            for control in controls:
                times = Counter()
                block_times = Counter()
                for decision in asset_decisions:
                    count = int(decision.get("count") or 0)
                    seconds = duration * count / total_count if total_count else 0.0
                    payload = _control_payload(decision, control)
                    plan = _plan(payload)
                    bid_active = plan.get("bid_price") not in (None, "") and (_decimal(plan.get("bid_amount")) or Decimal("0")) > 0
                    ask_active = plan.get("ask_price") not in (None, "") and (_decimal(plan.get("ask_amount")) or Decimal("0")) > 0
                    reason = _block_reason(payload)
                    paused = str(plan.get("market_mode", "")) == "PAUSED" or bool(_fair(payload).get("pause_reason"))
                    blocked = bool(reason)
                    if bid_active:
                        times["bid"] += seconds
                    if ask_active:
                        times["ask"] += seconds
                    if bid_active and ask_active and not paused and not blocked:
                        times["quoteable"] += seconds
                    if bid_active and ask_active:
                        times["both"] += seconds
                    if paused:
                        times["paused"] += seconds
                    if blocked:
                        times["blocked"] += seconds
                        block_times[_block_category(reason)] += seconds
                result.append(
                    {
                        "asset": asset,
                        "control": control,
                        "observation_seconds": _round(duration, 3),
                        "quoteable_time_pct": _round(_pct(times["quoteable"], duration), 6),
                        "bid_active_time_pct": _round(_pct(times["bid"], duration), 6),
                        "ask_active_time_pct": _round(_pct(times["ask"], duration), 6),
                        "both_sides_active_time_pct": _round(_pct(times["both"], duration), 6),
                        "paused_time_pct": _round(_pct(times["paused"], duration), 6),
                        "blocked_time_pct": _round(_pct(times["blocked"], duration), 6),
                        **{f"{key}_time_pct": _round(_pct(value, duration), 6) for key, value in block_times.items()},
                        "time_basis": "COUNT_WEIGHTED_DECISION_ROLLUPS",
                        "status": "AGGREGATE_DERIVED" if total_count else "DATA_INSUFFICIENT",
                    }
                )
        return result

    decisions_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data["decisions"]:
        if row.get("asset") in assets:
            decisions_by_asset[str(row["asset"])].append(row)
    result = []
    for asset in assets:
        for control in controls:
            intervals = _intervals(decisions_by_asset[asset], start, end)
            times = Counter()
            block_times = Counter()
            for decision, seconds in intervals:
                payload = _control_payload(decision, control)
                plan = _plan(payload)
                bid_active = plan.get("bid_price") not in (None, "") and (_decimal(plan.get("bid_amount")) or Decimal("0")) > 0
                ask_active = plan.get("ask_price") not in (None, "") and (_decimal(plan.get("ask_amount")) or Decimal("0")) > 0
                reason = _block_reason(payload)
                paused = str(plan.get("market_mode", "")) == "PAUSED" or bool(_fair(payload).get("pause_reason"))
                blocked = bool(reason)
                if bid_active:
                    times["bid"] += seconds
                if ask_active:
                    times["ask"] += seconds
                if bid_active and ask_active and not paused and not blocked:
                    times["quoteable"] += seconds
                if bid_active and ask_active:
                    times["both"] += seconds
                if paused:
                    times["paused"] += seconds
                if blocked:
                    times["blocked"] += seconds
                    block_times[_block_category(reason)] += seconds
            result.append(
                {
                    "asset": asset,
                    "control": control,
                    "observation_seconds": _round(duration, 3),
                    "quoteable_time_pct": _round(_pct(times["quoteable"], duration), 6),
                    "bid_active_time_pct": _round(_pct(times["bid"], duration), 6),
                    "ask_active_time_pct": _round(_pct(times["ask"], duration), 6),
                    "both_sides_active_time_pct": _round(_pct(times["both"], duration), 6),
                    "paused_time_pct": _round(_pct(times["paused"], duration), 6),
                    "blocked_time_pct": _round(_pct(times["blocked"], duration), 6),
                    **{f"{key}_time_pct": _round(_pct(value, duration), 6) for key, value in block_times.items()},
                    "status": "OBSERVED" if intervals else "DATA_INSUFFICIENT",
                }
            )
    return result


def _action_reports(data: dict[str, Any], assets: list[str], controls: tuple[str, ...], duration: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if has_retained_aggregates(data.get("aggregates", [])):
        decisions_by_asset = rows_by_asset(_retained_decisions(data))
        rows = []
        churn = []
        duration_minutes = duration / 60.0 if duration > 0 else 0.0
        for asset in assets:
            asset_decisions = decisions_by_asset.get(asset, [])
            decision_count = aggregate_observation_count(data.get("aggregates", []), asset)
            if not decision_count:
                decision_count = sum(int(row.get("count") or 0) for row in asset_decisions)
            for control in controls:
                for fill_model in FILL_MODELS:
                    model = f"{control}:{fill_model}"
                    metrics = aggregate_model_metrics(data.get("aggregates", []), asset, model)
                    model_actions = [
                        row for row in data["actions"]
                        if row.get("asset") == asset and row.get("model") == model
                    ]
                    if "action_rows" in metrics:
                        diagnostic_rows = int(metrics["action_rows"])
                    else:
                        diagnostic_rows = len(model_actions) + int(metrics.get("holds", 0))
                    creates = int(metrics.get("creates", 0))
                    holds = int(metrics.get("holds", 0))
                    cancels = int(metrics.get("cancels", 0))
                    replaces = int(metrics.get("replaces", 0))
                    non_replace = max(0, cancels - replaces)
                    block_count = 0
                    for decision in asset_decisions:
                        payload = _control_payload(decision, control)
                        if _block_reason(payload):
                            block_count += int(decision.get("count") or 0)
                    mutation_timestamps = [
                        float(row["timestamp"])
                        for row in model_actions
                        if row.get("action") in {"CREATE", "CANCEL", "REPLACE"}
                    ]
                    rows.append(
                        {
                            "asset": asset,
                            "control": control,
                            "fill_model": fill_model,
                            "model": model,
                            "decisions": decision_count,
                            "diagnostic_action_rows": diagnostic_rows,
                            "creates": creates,
                            "holds": holds,
                            "replaces": replaces,
                            "cancels": cancels,
                            "non_replace_cancels": non_replace,
                            "blocks": block_count,
                            "real_quote_mutation_events": len(mutation_timestamps),
                            "data_basis": "MINUTE_AGGREGATES_AND_PERMANENT_MUTATIONS",
                            "status": "AGGREGATE_DERIVED" if decision_count else "DATA_INSUFFICIENT",
                        }
                    )
                    windows = _window_counts(mutation_timestamps)
                    rolling_max = _rolling_max(mutation_timestamps)
                    churn.append(
                        {
                            "asset": asset,
                            "control": control,
                            "fill_model": fill_model,
                            "model": model,
                            "creates_per_min": _round(creates / duration_minutes if duration_minutes else None),
                            "replaces_per_min": _round(replaces / duration_minutes if duration_minutes else None),
                            "cancels_per_min": _round(cancels / duration_minutes if duration_minutes else None),
                            "actual_quote_mutations_per_min": _round(len(mutation_timestamps) / duration_minutes if duration_minutes else None),
                            "max_mutations_rolling_60s": rolling_max,
                            "p90_mutations_per_min": _round(_quantile(windows, 0.90)),
                            "diagnostic_action_rows": diagnostic_rows,
                            "churn_classification": "QUOTE_CHURN_HIGH" if rolling_max > 30 else "QUOTE_CHURN_WITHIN_LIMIT",
                            "data_basis": "PERMANENT_MUTATION_ROWS;HOLDS_FROM_MINUTE_AGGREGATES",
                            "status": "AGGREGATE_DERIVED" if decision_count else "DATA_INSUFFICIENT",
                        }
                    )
        return rows, churn

    decisions_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data["decisions"]:
        if row.get("asset") in assets:
            decisions_by_asset[str(row["asset"])].append(row)
    rows = []
    churn = []
    duration_minutes = duration / 60.0 if duration > 0 else 0.0
    for asset in assets:
        for control in controls:
            asset_decisions = decisions_by_asset[asset]
            for fill_model in FILL_MODELS:
                model = f"{control}:{fill_model}"
                model_actions = [row for row in data["actions"] if row.get("asset") == asset and row.get("model") == model]
                creates = sum(row.get("action") == "CREATE" for row in model_actions)
                holds = sum(row.get("action") == "HOLD" for row in model_actions)
                cancels = sum(row.get("action") == "CANCEL" for row in model_actions)
                replaces = sum(row.get("action") == "REPLACE" or (row.get("action") == "CANCEL" and row.get("reason") == "REFRESH_NEEDED") for row in model_actions)
                non_replace = cancels - sum(row.get("action") == "CANCEL" and row.get("reason") == "REFRESH_NEEDED" for row in model_actions)
                block_count = sum(bool(_block_reason(_control_payload(row, control))) for row in asset_decisions)
                mutation_timestamps = [float(row["timestamp"]) for row in model_actions if row.get("action") in {"CREATE", "CANCEL", "REPLACE"}]
                row = {
                    "asset": asset,
                    "control": control,
                    "fill_model": fill_model,
                    "model": model,
                    "decisions": len(asset_decisions),
                    "diagnostic_action_rows": len(model_actions),
                    "creates": creates,
                    "holds": holds,
                    "replaces": replaces,
                    "cancels": cancels,
                    "non_replace_cancels": non_replace,
                    "blocks": block_count,
                    "real_quote_mutation_events": len(mutation_timestamps),
                    "status": "OBSERVED" if asset_decisions else "DATA_INSUFFICIENT",
                }
                rows.append(row)
                windows = _window_counts(mutation_timestamps)
                churn.append(
                    {
                        "asset": asset,
                        "control": control,
                        "fill_model": fill_model,
                        "model": model,
                        "creates_per_min": _round(creates / duration_minutes if duration_minutes else None),
                        "replaces_per_min": _round(replaces / duration_minutes if duration_minutes else None),
                        "cancels_per_min": _round(cancels / duration_minutes if duration_minutes else None),
                        "actual_quote_mutations_per_min": _round(len(mutation_timestamps) / duration_minutes if duration_minutes else None),
                        "max_mutations_rolling_60s": _rolling_max(mutation_timestamps),
                        "p90_mutations_per_min": _round(_quantile(windows, 0.90)),
                        "diagnostic_action_rows": len(model_actions),
                        "churn_classification": "QUOTE_CHURN_HIGH" if _rolling_max(mutation_timestamps) > 30 else "QUOTE_CHURN_WITHIN_LIMIT",
                        "status": "OBSERVED" if asset_decisions else "DATA_INSUFFICIENT",
                    }
                )
    return rows, churn


def _reference_reports(data: dict[str, Any], assets: list[str], start: float, end: float, duration: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if has_retained_aggregates(data.get("aggregates", [])):
        decisions_by_asset = rows_by_asset(_retained_decisions(data))
        usage = []
        failovers = []
        disagreements = []
        for asset in assets:
            decisions = decisions_by_asset.get(asset, [])
            source_counts = _aggregate_source_counts(data, asset)
            total = sum(source_counts.values()) or aggregate_observation_count(data.get("aggregates", []), asset)
            source_time = Counter(
                {source: duration * count / total for source, count in source_counts.items()}
                if total
                else {}
            )
            failures = Counter()
            for key, count in aggregate_json_counts(data.get("aggregates", []), asset, "reference_health_counts_json").items():
                venue, _, health = key.partition(":")
                if venue in {"binance", "bybit", "okx"} and health not in {"HEALTHY", "DEGRADED"}:
                    failures[venue] += count
            failover_events = []
            disagreement_events = []
            previous = None
            source_switches = 0
            for decision in decisions:
                payload = decision.get("payload", {})
                source = payload.get("selected_reference")
                if previous is not None and source != previous:
                    source_switches += 1
                previous = source
                event = str(payload.get("failover_event") or "")
                if event:
                    failover_events.append(
                        {
                            "timestamp": decision["timestamp"],
                            "asset": asset,
                            "event": event,
                            "status": "FAILOVER_OBSERVED",
                            "data_basis": "DECISION_ROLLUP",
                        }
                    )
                if payload.get("reference_pause_reason") == "REFERENCE_DISAGREEMENT_PAUSE":
                    disagreement_events.append(
                        {
                            "timestamp": decision["timestamp"],
                            "asset": asset,
                            "dispersion_bps": payload.get("reference_dispersion_bps"),
                            "pause_reason": payload.get("reference_pause_reason"),
                            "status": "PAUSED",
                            "data_basis": "DECISION_ROLLUP",
                        }
                    )
            paused_count = sum(
                count
                for source, count in source_counts.items()
                if source not in {"binance", "bybit", "okx"}
            )
            usage.append(
                {
                    "asset": asset,
                    "binance_selected_time_pct": _round(_pct(source_time["binance"], duration), 6),
                    "bybit_selected_time_pct": _round(_pct(source_time["bybit"], duration), 6),
                    "okx_selected_time_pct": _round(_pct(source_time["okx"], duration), 6),
                    "paused_time_pct": _round(_pct(duration * paused_count / total if total else 0.0, duration), 6),
                    "binance_failures": failures["binance"],
                    "bybit_failovers": sum("bybit_TO_" in row["event"] for row in failover_events),
                    "okx_failovers": sum("okx_TO_" in row["event"] for row in failover_events),
                    "recovery_to_binance_events": sum(
                        decision.get("payload", {}).get("recovery_event") == "RECOVERY_TO_BINANCE"
                        for decision in decisions
                    ),
                    "reference_switches": source_switches,
                    "reference_switches_per_hour": _round(source_switches / (duration / 3600.0) if duration else None),
                    "reference_disagreement_pauses": len(disagreement_events),
                    "data_basis": "MINUTE_OCCUPANCY_AND_DECISION_ROLLUPS",
                    "status": "AGGREGATE_DERIVED" if total else "DATA_INSUFFICIENT",
                }
            )
            failovers.extend(failover_events or [{"asset": asset, "event": "", "status": "NO_FAILOVER_OBSERVED"}])
            disagreements.extend(
                disagreement_events or [{"asset": asset, "pause_reason": "", "status": "NO_DISAGREEMENT_OBSERVED"}]
            )

        health_summary = []
        for asset in assets:
            for venue in ("binance", "bybit", "okx", "derive"):
                counts = _aggregate_health_counts(data, asset, venue)
                observations = sum(counts.values())
                healthy = counts["HEALTHY"]
                degraded = counts["DEGRADED"]
                stale = counts["STALE"]
                health_summary.append(
                    {
                        "asset": asset,
                        "venue": venue,
                        "observations": observations,
                        "uptime_pct": _round(_pct(healthy + degraded, observations), 6),
                        "healthy_time_pct": _round(_pct(healthy, observations), 6),
                        "degraded_time_pct": _round(_pct(degraded, observations), 6),
                        "stale_time_pct": _round(_pct(stale, observations), 6),
                        "median_bbo_age_seconds": None,
                        "p99_bbo_age_seconds": None,
                        "maximum_gap_seconds": None,
                        "reconnect_count": None,
                        "stale_observations": stale,
                        "degraded_observations": degraded,
                        "statistics_basis": "HEALTH_OBSERVATION_RATIO;WALL_CLOCK_GAPS_NOT_RETAINED",
                        "status": "AGGREGATE_DERIVED" if observations else "DATA_INSUFFICIENT",
                    }
                )
        return usage, health_summary, failovers, disagreements

    decisions_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data["decisions"]:
        if row.get("asset") in assets:
            decisions_by_asset[str(row["asset"])].append(row)
    usage = []
    failovers = []
    disagreements = []
    for asset in assets:
        decisions = decisions_by_asset[asset]
        intervals = _intervals(decisions, start, end)
        source_time = Counter()
        source_switches = 0
        previous = None
        failures = Counter()
        pause_time = 0.0
        failover_events = []
        disagreement_events = []
        for decision, seconds in intervals:
            payload = decision.get("payload", {})
            source = payload.get("selected_reference")
            source_time[str(source or "pause")] += seconds
            if previous is not None and source != previous:
                source_switches += 1
            previous = source
            if source is None or payload.get("reference_pause_reason"):
                pause_time += seconds
            for venue, health in (payload.get("source_health") or {}).items():
                if venue in {"binance", "bybit", "okx"} and str((health or {}).get("health")) not in {"HEALTHY", "DEGRADED"}:
                    failures[venue] += 1
            event = str(payload.get("failover_event") or "")
            if event:
                failover_events.append({"timestamp": decision["timestamp"], "asset": asset, "event": event, "status": "FAILOVER_OBSERVED"})
            if payload.get("reference_pause_reason") == "REFERENCE_DISAGREEMENT_PAUSE":
                disagreement_events.append(
                    {
                        "timestamp": decision["timestamp"],
                        "asset": asset,
                        "dispersion_bps": payload.get("reference_dispersion_bps"),
                        "pause_reason": payload.get("reference_pause_reason"),
                        "status": "PAUSED",
                    }
                )
        usage.append(
            {
                "asset": asset,
                "binance_selected_time_pct": _round(_pct(source_time["binance"], duration), 6),
                "bybit_selected_time_pct": _round(_pct(source_time["bybit"], duration), 6),
                "okx_selected_time_pct": _round(_pct(source_time["okx"], duration), 6),
                "paused_time_pct": _round(_pct(source_time["pause"], duration), 6),
                "binance_failures": failures["binance"],
                "bybit_failovers": sum("bybit_TO_" in row["event"] for row in failover_events),
                "okx_failovers": sum("okx_TO_" in row["event"] for row in failover_events),
                "recovery_to_binance_events": sum(row.get("event") == "RECOVERY_TO_BINANCE" for decision, _ in intervals for row in [{"event": decision.get("payload", {}).get("recovery_event")}]),
                "reference_switches": source_switches,
                "reference_switches_per_hour": _round(source_switches / (duration / 3600.0) if duration else None),
                "reference_disagreement_pauses": len(disagreement_events),
                "status": "OBSERVED" if decisions else "DATA_INSUFFICIENT",
            }
        )
        failovers.extend(failover_events or [{"asset": asset, "event": "", "status": "NO_FAILOVER_OBSERVED"}])
        disagreements.extend(disagreement_events or [{"asset": asset, "pause_reason": "", "status": "NO_DISAGREEMENT_OBSERVED"}])
    health_rows_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in data["health"]:
        if row.get("asset") in assets and row.get("venue") in {"binance", "bybit", "okx", "derive"}:
            health_rows_by_key[(str(row["asset"]), str(row["venue"]))].append(row)
    health_summary = []
    for asset in assets:
        for venue in ("binance", "bybit", "okx", "derive"):
            rows = sorted(health_rows_by_key[(asset, venue)], key=lambda row: float(row["timestamp"]))
            statuses = [str(row.get("health", "UNOBSERVED")) for row in rows]
            ages = [_float(row.get("bbo_age")) for row in rows]
            gaps = [_float(row.get("maximum_recent_gap")) for row in rows]
            reconnects = [_float(row.get("reconnect_count")) or 0 for row in rows]
            healthy_seconds = 0.0
            available_seconds = 0.0
            stale_seconds = 0.0
            for index, row in enumerate(rows):
                row_end = float(rows[index + 1]["timestamp"]) if index + 1 < len(rows) else end
                seconds = max(0.0, min(end, row_end) - max(start, float(row["timestamp"])))
                status = str(row.get("health", ""))
                healthy_seconds += seconds if status == "HEALTHY" else 0.0
                available_seconds += seconds if status in {"HEALTHY", "DEGRADED"} else 0.0
                stale_seconds += seconds if status == "STALE" else 0.0
            health_summary.append(
                {
                    "asset": asset,
                    "venue": venue,
                    "observations": len(rows),
                    "uptime_pct": _round(_pct(available_seconds, duration), 6),
                    "healthy_time_pct": _round(_pct(healthy_seconds, duration), 6),
                    "degraded_time_pct": _round(_pct(max(0.0, available_seconds - healthy_seconds), duration), 6),
                    "stale_time_pct": _round(_pct(stale_seconds, duration), 6),
                    "median_bbo_age_seconds": _round(_median(ages), 6),
                    "p99_bbo_age_seconds": _round(_quantile(ages, 0.99), 6),
                    "maximum_gap_seconds": _round(max((value for value in gaps if value is not None), default=None), 6),
                    "reconnect_count": max(reconnects, default=0),
                    "stale_observations": statuses.count("STALE"),
                    "degraded_observations": statuses.count("DEGRADED"),
                    "status": "OBSERVED" if rows else "DATA_INSUFFICIENT",
                }
            )
    return usage, health_summary, failovers, disagreements


def _fill_reports(data: dict[str, Any], out_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fills = data["fills"]
    markouts = data["markouts"]
    conservative = [row for row in fills if _model_parts(str(row.get("model")))[1] == "CONSERVATIVE"]
    touch = [row for row in fills if _model_parts(str(row.get("model")))[1] == "TOUCH_SENSITIVITY"]
    _write_csv(out_dir / "shadow_fills_conservative.csv", conservative or [{"status": "NO_CONSERVATIVE_FILL_OBSERVED"}])
    _write_csv(out_dir / "shadow_fills_touch.csv", touch or [{"status": "NO_TOUCH_FILL_OBSERVED"}])
    _write_csv(out_dir / "reference_markouts.csv", markouts or [{"status": "NO_MARKOUT_OBSERVED"}])
    _write_csv(out_dir / "derive_markouts.csv", markouts or [{"status": "NO_MARKOUT_OBSERVED"}])
    return conservative, touch, markouts, fills


def _fill_summary(data: dict[str, Any], assets: list[str], duration: float, markouts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fills = data["fills"]
    volume_rows = []
    toxicity_rows = []
    net_rows = []
    hours = duration / 3600.0 if duration else 0.0
    for asset in assets:
        for control in ("DERIVE_ONLY", "BINANCE_ONLY_NO_FAILOVER", "PRIORITY_FAILOVER"):
            for fill_model in FILL_MODELS:
                model = f"{control}:{fill_model}"
                model_fills = [row for row in fills if row.get("asset") == asset and row.get("model") == model]
                notionals = [(_decimal(row.get("amount")) or Decimal("0")) * (_decimal(row.get("fill_price")) or Decimal("0")) for row in model_fills]
                volume = sum(notionals, Decimal("0"))
                volume_rows.append(
                    {
                        "asset": asset,
                        "control": control,
                        "fill_model": fill_model,
                        "model": model,
                        "fill_count": len(model_fills),
                        "fills_per_hour": _round(len(model_fills) / hours if hours else None),
                        "maker_volume": str(volume),
                        "maker_volume_per_hour": _round(float(volume) / hours if hours else None),
                        "average_notional_per_fill": _round(_mean(notionals)),
                        "status": "OBSERVED" if model_fills else "DATA_INSUFFICIENT",
                    }
                )
                model_markouts = [row for row in markouts if row.get("asset") == asset and row.get("model") == model]
                for horizon in (5, 30, 60):
                    values = [row.get("derive_markout_bps") for row in model_markouts if int(row.get("horizon_seconds", 0)) == horizon]
                    negatives = [value for value in values if (_float(value) or 0) < 0]
                    toxicity = "INSUFFICIENT_SAMPLE"
                    if fill_model == "CONSERVATIVE" and len(values) >= 20:
                        median = _median(values) or 0.0
                        toxicity = "HIGH_TOXICITY" if median < -2 else "MODERATE_TOXICITY" if median < 0 else "LOW_TOXICITY"
                    toxicity_rows.append(
                        {
                            "asset": asset,
                            "control": control,
                            "fill_model": fill_model,
                            "model": model,
                            "horizon_seconds": horizon,
                            "sample_count": len(values),
                            "median_markout_bps": _round(_median(values)),
                            "mean_markout_bps": _round(_mean(values)),
                            "negative_markout_pct": _round(_pct(len(negatives), len(values)), 6),
                            "classification": toxicity,
                            "sample_gate": "CONSERVATIVE_FILL_SAMPLE_GE_20" if fill_model == "CONSERVATIVE" and len(values) >= 20 else "CONSERVATIVE_SAMPLE_INSUFFICIENT",
                            "status": "OBSERVED" if values else "DATA_INSUFFICIENT",
                        }
                    )
                    net_values = []
                    for row in model_markouts:
                        if int(row.get("horizon_seconds", 0)) != horizon:
                            continue
                        fill = next((item for item in model_fills if abs(float(item["timestamp"]) - float(row["fill_timestamp"])) < 1e-9), None)
                        if fill is None or row.get("derive_markout_bps") is None:
                            continue
                        quoted_edge = _decimal(fill.get("quoted_edge_bps")) or Decimal("0")
                        fee = _decimal(fill.get("maker_fee_bps")) or Decimal("0")
                        markout = _decimal(row.get("derive_markout_bps")) or Decimal("0")
                        net_values.append(quoted_edge - fee + markout)
                    net_rows.append(
                        {
                            "asset": asset,
                            "control": control,
                            "fill_model": fill_model,
                            "model": model,
                            "horizon_seconds": horizon,
                            "sample_count": len(net_values),
                            "net_capture_proxy_bps_mean": _round(_mean(net_values)),
                            "net_capture_proxy_bps_median": _round(_median(net_values)),
                            "net_capture_proxy_bps_p25": _round(_quantile(net_values, 0.25)),
                            "net_capture_proxy_bps_p75": _round(_quantile(net_values, 0.75)),
                            "formula": "quoted_edge_bps - maker_fee_bps + derive_markout_bps",
                            "status": "PROXY_NOT_REALIZED_PNL" if net_values else "DATA_INSUFFICIENT",
                        }
                    )
    return volume_rows, toxicity_rows, net_rows


def _capital_reports(config: RuntimeConfig, assets: list[str], out_dir: Path, data: dict[str, Any], duration: float) -> list[dict[str, Any]]:
    mapping = _read_json(out_dir / "asset_reference_mapping.json")
    mappings = mapping.get("mappings") or {}
    latest_mid: dict[str, Decimal] = {}
    for row in _retained_decisions(data):
        if row.get("asset") in assets:
            bid = _decimal(row.get("payload", {}).get("derive_bid"))
            ask = _decimal(row.get("payload", {}).get("derive_ask"))
            if bid is not None and ask is not None:
                latest_mid[str(row["asset"])] = (bid + ask) / 2
    if has_retained_aggregates(data.get("aggregates", [])):
        for asset in assets:
            aggregate = latest_aggregate(data.get("aggregates", []), asset)
            median_mid = _decimal(aggregate.get("derive_mid_median"))
            if median_mid is not None:
                latest_mid.setdefault(asset, median_mid)
    result = []
    for asset in assets:
        rules = (mappings.get(asset) or {}).get("rules") or {}
        minimum_amount = _decimal(rules.get("minimum_amount")) or Decimal("0")
        configured_notional = _decimal(rules.get("minimum_notional")) or Decimal("0")
        minimum_notional = configured_notional if configured_notional > 0 else minimum_amount * latest_mid.get(asset, Decimal("0"))
        result.append(
            {
                "asset": asset,
                "minimum_amount": str(minimum_amount),
                "minimum_notional": str(minimum_notional),
                "capital_usdc": str(config.capital_usdc),
                "minimum_notional_pct_capital": _round(minimum_notional / config.capital_usdc * 100 if config.capital_usdc else None, 6),
                "one_minimum_fill_pct_capital": _round(minimum_notional / config.capital_usdc * 100 if config.capital_usdc else None, 6),
                "two_same_side_fills_pct_capital": _round(minimum_notional * 2 / config.capital_usdc * 100 if config.capital_usdc else None, 6),
                "three_same_side_fills_pct_capital": _round(minimum_notional * 3 / config.capital_usdc * 100 if config.capital_usdc else None, 6),
                "status": "OBSERVED" if minimum_notional > 0 else "DATA_INSUFFICIENT",
            }
        )
    return result


def _inventory_reports(data: dict[str, Any], assets: list[str], start: float, end: float, duration: float) -> list[dict[str, Any]]:
    if has_retained_aggregates(data.get("aggregates", [])):
        result = []
        for asset in assets:
            aggregate_rows = _aggregate_rows(data, asset)
            occupancy = aggregate_json_counts(data.get("aggregates", []), asset, "inventory_mode_occupancy_json")
            observations = sum(occupancy.values()) or aggregate_observation_count(data.get("aggregates", []), asset)
            latest = latest_aggregate(data.get("aggregates", []), asset)
            max_notional = _float(latest.get("inventory_notional_max"))
            result.append(
                {
                    "asset": asset,
                    "model": "PRIORITY_FAILOVER:CONSERVATIVE",
                    "max_long_position": None,
                    "max_short_position": None,
                    "max_inventory_notional": _round(max_notional, 6),
                    "flat_time_pct": _round(_pct(occupancy["FLAT"], observations), 6),
                    "long_skew_time_pct": _round(_pct(occupancy["LONG_SKEW"], observations), 6),
                    "short_skew_time_pct": _round(_pct(occupancy["SHORT_SKEW"], observations), 6),
                    "ask_only_time_pct": _round(_pct(occupancy["ASK_ONLY"], observations), 6),
                    "bid_only_time_pct": _round(_pct(occupancy["BID_ONLY"], observations), 6),
                    "aggregate_minutes": len(aggregate_rows),
                    "data_basis": "MINUTE_INVENTORY_OCCUPANCY;POSITION_AMOUNTS_NOT_RETAINED",
                    "status": "AGGREGATE_DERIVED" if observations else "DATA_INSUFFICIENT",
                }
            )
        return result

    result = []
    for asset in assets:
        decisions = [row for row in data["decisions"] if row.get("asset") == asset]
        intervals = _intervals(decisions, start, end)
        amounts = [_decimal(row.get("payload", {}).get("position_amount")) or Decimal("0") for row, _ in intervals]
        notionals = [_decimal(row.get("payload", {}).get("position_notional")) or Decimal("0") for row, _ in intervals]
        mode_time = Counter()
        for row, seconds in intervals:
            mode_time[str(row.get("payload", {}).get("inventory_mode") or "UNKNOWN")] += seconds
        result.append(
            {
                "asset": asset,
                "model": "PRIORITY_FAILOVER:CONSERVATIVE",
                "max_long_position": str(max(amounts, default=Decimal("0"))),
                "max_short_position": str(min(amounts, default=Decimal("0"))),
                "max_inventory_notional": _round(max((abs(value) for value in notionals), default=None), 6),
                "flat_time_pct": _round(_pct(mode_time["FLAT"], duration), 6),
                "long_skew_time_pct": _round(_pct(mode_time["LONG_SKEW"], duration), 6),
                "short_skew_time_pct": _round(_pct(mode_time["SHORT_SKEW"], duration), 6),
                "ask_only_time_pct": _round(_pct(mode_time["ASK_ONLY"], duration), 6),
                "bid_only_time_pct": _round(_pct(mode_time["BID_ONLY"], duration), 6),
                "status": "OBSERVED" if decisions else "DATA_INSUFFICIENT",
            }
        )
    return result


def _portfolio_reports(config: RuntimeConfig, data: dict[str, Any], controls: tuple[str, ...], duration: float) -> list[dict[str, Any]]:
    runtime_models = data.get("runtime", {}).get("models") or {}
    result = []
    for control in controls:
        for fill_model in FILL_MODELS:
            model = f"{control}:{fill_model}"
            state = runtime_models.get(model) or {}
            equity = _decimal(state.get("equity")) or config.capital_usdc
            fees = _decimal(state.get("fees")) or Decimal("0")
            fills = int(state.get("fills") or 0)
            result.append(
                {
                    "control": control,
                    "fill_model": fill_model,
                    "model": model,
                    "gross_shadow_pnl": str(equity - config.capital_usdc + fees),
                    "fees": str(fees),
                    "net_shadow_pnl": str(equity - config.capital_usdc),
                    "max_drawdown": str(_decimal(state.get("max_drawdown")) or Decimal("0")),
                    "maker_volume": str(sum(((_decimal(row.get("amount")) or Decimal("0")) * (_decimal(row.get("fill_price")) or Decimal("0"))) for row in data["fills"] if row.get("model") == model)),
                    "fill_count": fills,
                    "final_gross_inventory": str(_decimal(state.get("gross_inventory")) or Decimal("0")),
                    "final_net_inventory": str(_decimal(state.get("net_inventory")) or Decimal("0")),
                    "status": "SHADOW_ESTIMATE",
                }
            )
    return result


def _model_reports(config: RuntimeConfig, assets: list[str], action_rows: list[dict[str, Any]], churn_rows: list[dict[str, Any]], uptime_rows: list[dict[str, Any]], volume_rows: list[dict[str, Any]], toxicity_rows: list[dict[str, Any]], net_rows: list[dict[str, Any]], inventory_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for control in config.control_models:
        for fill_model in FILL_MODELS:
            model = f"{control}:{fill_model}"
            action = [row for row in action_rows if row["model"] == model]
            churn = [row for row in churn_rows if row["model"] == model]
            volumes = [row for row in volume_rows if row["model"] == model]
            toxicity = [row for row in toxicity_rows if row["model"] == model and row["horizon_seconds"] in {5, 30, 60}]
            net = [row for row in net_rows if row["model"] == model and row["horizon_seconds"] == 30]
            result.append(
                {
                    "control": control,
                    "fill_model": fill_model,
                    "model": model,
                    "decision_rows": sum(int(row["decisions"]) for row in action),
                    "diagnostic_action_rows": sum(int(row["diagnostic_action_rows"]) for row in action),
                    "creates": sum(int(row["creates"]) for row in action),
                    "replaces": sum(int(row["replaces"]) for row in action),
                    "cancels": sum(int(row["cancels"]) for row in action),
                    "blocks": sum(int(row["blocks"]) for row in action),
                    "fill_count": sum(int(row["fill_count"]) for row in volumes),
                    "maker_volume": str(sum((Decimal(str(row["maker_volume"])) for row in volumes), Decimal("0"))),
                    "median_5s_markout_bps": next((row["median_markout_bps"] for row in toxicity if row["horizon_seconds"] == 5), None),
                    "median_30s_markout_bps": next((row["median_markout_bps"] for row in toxicity if row["horizon_seconds"] == 30), None),
                    "median_60s_markout_bps": next((row["median_markout_bps"] for row in toxicity if row["horizon_seconds"] == 60), None),
                    "net_capture_30s_bps": next((row["net_capture_proxy_bps_mean"] for row in net), None),
                    "quote_mutations_per_min": _round(_mean(row["actual_quote_mutations_per_min"] for row in churn)),
                    "model_sample_sufficient": fill_model != "CONSERVATIVE" or sum(int(row["fill_count"]) for row in volumes) >= 20,
                    "status": "HYPOTHETICAL_SHADOW_ONLY" if volumes else "MODEL_COMPARISON_INSUFFICIENT",
                }
            )
    return result


def _classify_asset(metrics: dict[str, Any], churn: dict[str, Any] | None, toxicity: list[dict[str, Any]], net_capture: list[dict[str, Any]]) -> tuple[str, str]:
    trades = int(metrics.get("trade_count") or 0)
    fills = int(metrics.get("conservative_fill_count") or 0)
    if trades == 0:
        return "DROP", "DROP_LOW_ACTIVITY"
    if churn and churn.get("churn_classification") == "QUOTE_CHURN_HIGH":
        return "MORE_DATA_REQUIRED", "DROP_HIGH_CHURN_OR_MORE_DATA_NEEDED"
    if fills < 20:
        return ("DROP" if fills == 0 and int(metrics.get("touch_fill_count") or 0) == 0 else "MORE_DATA_REQUIRED"), "CONSERVATIVE_SAMPLE_INSUFFICIENT"
    median_60 = next((row.get("median_markout_bps") for row in toxicity if row.get("horizon_seconds") == 60), None)
    net_30 = next((row.get("net_capture_proxy_bps_mean") for row in net_capture if row.get("horizon_seconds") == 30), None)
    if median_60 is not None and median_60 < -2:
        return "DROP", "DROP_HIGH_TOXICITY"
    if net_30 is not None and net_30 < 0:
        return "DROP", "DROP_LOW_NET_CAPTURE"
    return "KEEP", "KEEP_SAMPLE_SUPPORTED"


def _ranking_reports(config: RuntimeConfig, assets: list[str], activity: list[dict[str, Any]], volume_rows: list[dict[str, Any]], toxicity_rows: list[dict[str, Any]], net_rows: list[dict[str, Any]], churn_rows: list[dict[str, Any]], health_rows: list[dict[str, Any]], capital_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    activity_by_asset = {row["asset"]: row for row in activity}
    volume_by_asset = {asset: next((row for row in volume_rows if row["asset"] == asset and row["model"] == "PRIORITY_FAILOVER:CONSERVATIVE"), {}) for asset in assets}
    health_by_asset = {asset: next((row for row in health_rows if row["asset"] == asset and row["venue"] == "binance"), {}) for asset in assets}
    capital_by_asset = {row["asset"]: row for row in capital_rows}
    ranking = []
    decisions = []
    for asset in assets:
        activity_row = activity_by_asset[asset]
        volume = volume_by_asset[asset]
        toxicity = [row for row in toxicity_rows if row["asset"] == asset and row["model"] == "PRIORITY_FAILOVER:CONSERVATIVE"]
        net = [row for row in net_rows if row["asset"] == asset and row["model"] == "PRIORITY_FAILOVER:CONSERVATIVE"]
        churn = next((row for row in churn_rows if row["asset"] == asset and row["model"] == "PRIORITY_FAILOVER:CONSERVATIVE"), None)
        metrics = {
            **activity_row,
            "conservative_fill_count": volume.get("fill_count", 0),
            "touch_fill_count": next((row.get("fill_count", 0) for row in volume_rows if row["asset"] == asset and row["model"] == "PRIORITY_FAILOVER:TOUCH_SENSITIVITY"), 0),
        }
        classification, detail = _classify_asset(metrics, churn, toxicity, net)
        net_30 = next((row.get("net_capture_proxy_bps_mean") for row in net if row["horizon_seconds"] == 30), None)
        markout_30 = next((row.get("median_markout_bps") for row in toxicity if row["horizon_seconds"] == 30), None)
        markout_60 = next((row.get("median_markout_bps") for row in toxicity if row["horizon_seconds"] == 60), None)
        ranking.append(
            {
                "asset": asset,
                "trades_per_hour": activity_row.get("trades_per_hour"),
                "conservative_fills": volume.get("fill_count", 0),
                "maker_volume_per_hour": volume.get("maker_volume_per_hour"),
                "net_capture_30s_bps": net_30,
                "median_30s_markout_bps": markout_30,
                "median_60s_markout_bps": markout_60,
                "quote_mutations_per_min": churn.get("actual_quote_mutations_per_min") if churn else None,
                "reference_uptime_pct": health_by_asset[asset].get("uptime_pct"),
                "minimum_notional_pct_capital": capital_by_asset[asset].get("minimum_notional_pct_capital"),
                "classification": classification,
                "classification_detail": detail,
                "status": "OBSERVED" if activity_row.get("trade_count", 0) or volume.get("fill_count", 0) else "DATA_INSUFFICIENT",
            }
        )
        decisions.append({"asset": asset, "classification": classification, "detail": detail})
    def score(row: dict[str, Any]) -> tuple[float, ...]:
        def value(key: str, default: float = -1e12) -> float:
            number = _float(row.get(key))
            return number if number is not None else default

        return (
            value("trades_per_hour"),
            float(row.get("conservative_fills") or 0),
            value("maker_volume_per_hour"),
            value("net_capture_30s_bps"),
            value("median_30s_markout_bps"),
            value("median_60s_markout_bps"),
            -value("quote_mutations_per_min", 1e12),
            -value("minimum_notional_pct_capital", 1e12),
            value("reference_uptime_pct"),
        )

    ranking.sort(key=score, reverse=True)
    for index, row in enumerate(ranking, start=1):
        row["rank"] = index
    return ranking, decisions


def export(
    config: RuntimeConfig,
    telemetry_path: Path,
    state_path: Path,
    out_dir: Path,
    metadata_path: Path,
    diagnostic_out_dir: Path | None = None,
) -> dict[str, Any] | None:
    state = _read_json(state_path)
    status = str(state.get("status", "UNKNOWN"))
    if status not in {"COMPLETE", "DATA_INSUFFICIENT"}:
        elapsed = max(0.0, time.time() - (_float(state.get("started_at")) or time.time()))
        planned = _float(state.get("started_at"))
        remaining = max(0.0, 21600.0 - elapsed) if planned is not None else None
        print("CANARY_NOT_COMPLETE")
        print(f"ELAPSED: {elapsed:.3f}")
        print(f"REMAINING: {remaining:.3f}" if remaining is not None else "REMAINING: UNKNOWN")
        return None
    metadata = _read_json(metadata_path)
    data = _read_rows(telemetry_path)
    assets = [asset.symbol for asset in config.enabled_assets]
    retained_decisions = _retained_decisions(data)
    start, end, duration = _run_window(state, metadata, retained_decisions)
    retention = retention_metadata(
        aggregates=data.get("aggregates", []),
        rollups=data.get("rollups", []),
        raw_decision_rows=len(data["decisions"]),
    )
    controls = tuple(config.control_models)
    activity, gap_stats, trade_details = _trade_reports(data, assets, start, end, duration)
    feed = _feed_audit(data, assets, activity, duration, state)
    spread, persistence = _spread_reports(data, assets, start, end, duration)
    uptime = _quote_uptime(data, assets, controls, start, end, duration)
    action_rows, churn = _action_reports(data, assets, controls, duration)
    usage, health, failovers, disagreements = _reference_reports(data, assets, start, end, duration)
    conservative, touch, markouts, _ = _fill_reports(data, out_dir)
    volume_rows, toxicity_rows, net_rows = _fill_summary(data, assets, duration, markouts)
    capital_rows = _capital_reports(config, assets, out_dir, data, duration)
    inventory_rows = _inventory_reports(data, assets, start, end, duration)
    portfolio_rows = _portfolio_reports(config, data, controls, duration)
    model_rows = _model_reports(config, assets, action_rows, churn, uptime, volume_rows, toxicity_rows, net_rows, inventory_rows)
    ranking, decisions = _ranking_reports(config, assets, activity, volume_rows, toxicity_rows, net_rows, churn, health, capital_rows)
    _write_csv(out_dir / "trade_feed_audit.csv", feed)
    _write_csv(out_dir / "trade_activity.csv", activity)
    _write_csv(out_dir / "trade_gap_statistics.csv", gap_stats)
    _write_csv(out_dir / "spread_statistics.csv", spread)
    _write_csv(out_dir / "spread_persistence.csv", persistence)
    _write_csv(out_dir / "quote_uptime.csv", uptime)
    _write_csv(out_dir / "action_breakdown.csv", action_rows)
    _write_csv(out_dir / "quote_churn.csv", churn)
    _write_csv(out_dir / "reference_usage.csv", usage)
    _write_csv(out_dir / "reference_health.csv", health)
    _write_csv(out_dir / "reference_failovers.csv", failovers)
    _write_csv(out_dir / "reference_disagreement.csv", disagreements)
    _write_csv(out_dir / "minute_aggregates.csv", aggregate_artifact_rows(data.get("aggregates", [])))
    _write_csv(out_dir / "decision_rollups.csv", data.get("rollups", []))
    _write_csv(out_dir / "toxicity.csv", toxicity_rows)
    _write_csv(out_dir / "net_capture.csv", net_rows)
    _write_csv(out_dir / "maker_volume.csv", volume_rows)
    _write_csv(out_dir / "capital_compatibility.csv", capital_rows)
    _write_csv(out_dir / "inventory_statistics.csv", inventory_rows)
    _write_csv(out_dir / "portfolio_statistics.csv", portfolio_rows)
    _write_csv(out_dir / "model_comparison.csv", model_rows)
    _write_csv(out_dir / "asset_ranking.csv", ranking)
    metadata.update(
        {
            "status": status,
            "end_time_utc": _iso(state.get("ended_at") or end),
            "elapsed_seconds": _round(duration, 3),
            "planned_duration_seconds": 21600.0,
            "real_orders": state.get("real_orders", 0),
            "real_positions": state.get("real_positions", 0),
            "mainnet_armed": state.get("mainnet_armed", False),
            "final_safety_audit_passed": state.get("mode") == "MAINNET_SHADOW" and state.get("mainnet_armed") is False and state.get("real_orders") == 0 and state.get("real_positions") == 0 and state.get("reference_execution") is False,
            "retention": retention,
            "denominators": {
                "decision_rows": len(data["decisions"]),
                "decision_observations": sum(int(row.get("observation_count") or 0) for row in data.get("aggregates", [])),
                "decision_rollup_rows": len(data.get("rollups", [])),
                "minute_aggregate_rows": len(data.get("aggregates", [])),
                "diagnostic_action_rows": len(data["actions"]),
                "derive_trade_rows": len([row for row in data["trades"] if row.get("source") == "derive"]),
                "fill_rows": len(data["fills"]),
                "markout_rows": len(data["markouts"]),
                "reference_health_rows": len(data["health"]),
            },
        }
    )
    _write_json(metadata_path, metadata)
    run_id = str(metadata.get("run_id") or out_dir.name)
    if diagnostic_out_dir is None:
        reports_root = out_dir.parent if out_dir.parent.name == "reports" else out_dir.parent.parent
        diagnostic_out_dir = reports_root / "quote_fill_diagnostic" / run_id
    diagnostic_summary = export_diagnostics(
        config,
        telemetry_path,
        state_path,
        diagnostic_out_dir,
        metadata_path,
    )
    if diagnostic_summary.get("restart_required_to_remove_bitget"):
        print("RESTART_REQUIRED_TO_REMOVE_BITGET")
    metadata.update(
        {
            "quote_fill_diagnostic_dir": str(diagnostic_out_dir),
            "quote_fill_diagnostic_summary": str(Path(diagnostic_out_dir) / "diagnostic_summary.json"),
        }
    )
    _write_json(metadata_path, metadata)
    per_asset = {asset: {**next(row for row in activity if row["asset"] == asset), "feed_classification": next(row["classification"] for row in feed if row["asset"] == asset), "conservative_fills": next(row["fill_count"] for row in volume_rows if row["asset"] == asset and row["model"] == "PRIORITY_FAILOVER:CONSERVATIVE"), "touch_fills": next(row["fill_count"] for row in volume_rows if row["asset"] == asset and row["model"] == "PRIORITY_FAILOVER:TOUCH_SENSITIVITY") } for asset in assets}
    final_report = {
        "report_version": "three-asset-six-hour-viability-v1",
        "measurement_only": True,
        "status": status,
        "run_metadata": metadata,
        "safety": {
            "mode": state.get("mode"),
            "mainnet_armed": state.get("mainnet_armed"),
            "real_orders": state.get("real_orders", 0),
            "real_positions": state.get("real_positions", 0),
            "binance_orders": 0,
            "bybit_orders": 0,
            "okx_orders": 0,
            "live_execution": False,
        },
        "active_assets": assets,
        "reference_priority": ["binance", "bybit", "okx", "pause"],
        "trade_feed_audit": feed,
        "per_asset": per_asset,
        "model_comparison": model_rows,
        "asset_ranking": ranking,
        "asset_decisions": decisions,
        "quote_fill_diagnostic": diagnostic_summary,
        "portfolio": portfolio_rows,
        "retention": retention,
        "ready_for_small_mainnet_canary": False,
        "ready_for_live": False,
        "final_classification": "NOT_READY_FOR_SMALL_MAINNET_CANARY",
        "next_action": "Keep live execution disabled; review the completed evidence and only change scope under a separate authorization.",
        "required_artifacts": list(REQUIRED_REPORTS),
        "limitations": [
            "Shadow fills are hypothetical and do not establish realized Derive execution or PnL.",
            "Conservative fill-quality conclusions require at least 20 conservative fills; otherwise the sample gate remains insufficient.",
            "A wide spread without direct Derive trade activity is not treated as useful market-making opportunity.",
            "When minute aggregates back the report, exact event-time threshold episodes and distributions older than the raw window are not reconstructed.",
        ],
    }
    _write_json(out_dir / "final_6h_validation_report.json", final_report)
    lines = [
        "# DERIVE THREE-ASSET SIX-HOUR VIABILITY AUDIT COMPLETE",
        "",
        f"- Status: `{status}`",
        f"- Observation: `{duration:.3f}` seconds",
        "- Mode: `MAINNET_SHADOW`",
        "- Mainnet armed: `FALSE`",
        "- Real Derive orders: `0`",
        "- Real Derive positions: `0`",
        "- Reference priority: `BINANCE -> BYBIT -> OKX -> PAUSE`",
        "- Ready for live: `NO`",
        "",
        "## Per-asset viability",
        "",
        "| Asset | Trades/hour | Trade notional/hour | Conservative fills | Touch fills | Maker volume/hour | 5s markout | 30s markout | 60s markout | Net capture | Classification |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in ranking:
        toxicity = [item for item in toxicity_rows if item["asset"] == row["asset"] and item["model"] == "PRIORITY_FAILOVER:CONSERVATIVE"]
        net = [item for item in net_rows if item["asset"] == row["asset"] and item["model"] == "PRIORITY_FAILOVER:CONSERVATIVE" and item["horizon_seconds"] == 30]
        m5 = next((item["median_markout_bps"] for item in toxicity if item["horizon_seconds"] == 5), None)
        lines.append(f"| {row['asset']} | {row.get('trades_per_hour')} | {next(item['trade_notional_per_hour'] for item in activity if item['asset'] == row['asset'])} | {row.get('conservative_fills')} | {row.get('touch_fill_count', next((item['fill_count'] for item in volume_rows if item['asset'] == row['asset'] and item['model'] == 'PRIORITY_FAILOVER:TOUCH_SENSITIVITY'), 0))} | {row.get('maker_volume_per_hour')} | {m5} | {row.get('median_30s_markout_bps')} | {row.get('median_60s_markout_bps')} | {next((item['net_capture_proxy_bps_mean'] for item in net), None)} | `{row.get('classification')}` |")
    lines.extend(
        [
            "",
            "## Model comparison",
            "",
            "| Control | Fill model | Fills | Maker volume | 30s markout | Net capture | Quote mutations/min | Status |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in model_rows:
        lines.append(f"| {row['control']} | {row['fill_model']} | {row['fill_count']} | {row['maker_volume']} | {row['median_30s_markout_bps']} | {row['net_capture_30s_bps']} | {row['quote_mutations_per_min']} | `{row['status']}` |")
    lines.extend(
        [
            "",
            "## Asset ranking",
            "",
            *[f"{row['rank']}. {row['asset']} — `{row['classification']}` ({row['classification_detail']})" for row in ranking],
            "",
            "## Final decision",
            "",
            "Ready for small mainnet canary: `NO`",
            "Ready for live: `NO`",
            "Public shadow evidence remains hypothetical; no live execution is authorized by this report.",
            "",
        ]
    )
    (out_dir / "final_6h_validation_report.md").write_text("\n".join(lines), encoding="utf-8")
    return final_report


def _wait_for_pid(pid: int, state_path: Path, poll_seconds: float) -> None:
    terminal_seen_at: float | None = None
    process_dead_at: float | None = None
    while True:
        state = _read_json(state_path)
        if state.get("status") in {"COMPLETE", "DATA_INSUFFICIENT"} and terminal_seen_at is None:
            terminal_seen_at = time.monotonic()
        process_alive = True
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            process_alive = False
        except PermissionError:
            process_alive = False
        if not process_alive and process_dead_at is None:
            process_dead_at = time.monotonic()
        if terminal_seen_at is not None and not process_alive and time.monotonic() - terminal_seen_at >= 1.0:
            return
        if process_dead_at is not None and terminal_seen_at is None and time.monotonic() - process_dead_at >= 5.0:
            return
        time.sleep(max(1.0, poll_seconds))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--telemetry", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--run-metadata", required=True)
    parser.add_argument("--diagnostic-out-dir", default=None)
    parser.add_argument("--wait-for-pid", type=int, default=None)
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args()
    config = RuntimeConfig.from_yaml(args.config)
    state_path = Path(args.state)
    if args.wait_for_pid is not None:
        _wait_for_pid(args.wait_for_pid, state_path, args.poll_seconds)
    report = export(
        config,
        Path(args.telemetry),
        state_path,
        Path(args.out_dir),
        Path(args.run_metadata),
        Path(args.diagnostic_out_dir) if args.diagnostic_out_dir else None,
    )
    if report is not None:
        print("DERIVE THREE-ASSET SIX-HOUR VIABILITY AUDIT COMPLETE")
        print(json.dumps({"status": report["status"], "classification": report["final_classification"], "report_dir": args.out_dir}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
