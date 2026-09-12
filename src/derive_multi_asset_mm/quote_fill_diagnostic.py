"""Causal quote-lifetime and Derive-trade crossing diagnostics.

This module is deliberately measurement-only.  It reads a shadow run's
SQLite database in read-only mode, reconstructs quote intervals from the
permanent CREATE/CANCEL/FILL events, and writes bounded diagnostic artifacts.
It never changes a strategy parameter, submits an order, or updates the run's
telemetry database.

The reconstruction is intentionally conservative:

* quote activity uses receipt timestamps and requires CREATE < trade < end;
* a Derive public trade is not a fill unless the recorded shadow fill can be
  associated with the same model/side/quote event;
* strict conservative crossings require the correct aggressor and a strict
  trade-through; a touch is reported separately;
* no observation is forward-filled across a gap larger than the configured
  observation window, and compressed rollups are marked as such;
* ``queue_residency_proxy`` means continuous same-price observed time.  It is
  not a claim about exchange queue position.
"""

from __future__ import annotations

import csv
import json
import math
import os
import shutil
import sqlite3
import time
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .config import RuntimeConfig

PRIMARY_MODEL = "PRIORITY_FAILOVER:CONSERVATIVE"
PRIMARY_TOUCH_MODEL = "PRIORITY_FAILOVER:TOUCH_SENSITIVITY"
FILL_MODELS = ("CONSERVATIVE", "TOUCH_SENSITIVITY")
LIFETIME_BUCKETS = (
    ("<250ms", None, 0.250),
    ("250-500ms", 0.250, 0.500),
    ("500ms-1s", 0.500, 1.000),
    ("1-2s", 1.000, 2.000),
    ("2-5s", 2.000, 5.000),
    ("5-10s", 5.000, 10.000),
    (">10s", 10.000, None),
)
SURVIVAL_THRESHOLDS = (
    ("100ms", 0.100),
    ("250ms", 0.250),
    ("500ms", 0.500),
    ("1s", 1.000),
    ("2s", 2.000),
    ("5s", 5.000),
    ("10s", 10.000),
    ("30s", 30.000),
)
DISTANCE_BUCKETS = (
    ("<1bps", None, 1.0),
    ("1-2bps", 1.0, 2.0),
    ("2-5bps", 2.0, 5.0),
    ("5-10bps", 5.0, 10.0),
    (">=10bps", 10.0, None),
)
MICRO_CHURN_THRESHOLDS = (0.1, 0.25, 0.5, 1.0)
CANCEL_BEFORE_WINDOWS = (0.100, 0.250, 0.500, 1.000, 2.000, 5.000)
DIAGNOSTIC_FILES = (
    "run_metadata.json",
    "quote_lifetime.csv",
    "quote_lifetime_distribution.csv",
    "quote_residency.csv",
    "quote_distance_to_touch.csv",
    "quote_distance_to_fair_value.csv",
    "replacement_reasons.csv",
    "replacement_fv_move.csv",
    "micro_churn.csv",
    "quote_mutation_rate.csv",
    "derive_trade_activity.csv",
    "asset_root_cause.csv",
    "trade_quote_crossings.csv",
    "missed_fill_analysis.csv",
    "cancel_before_trade.csv",
    "lifetime_fill_relationship.csv",
    "fill_logic_audit.csv",
    "reference_health.csv",
    "storage_health.csv",
    "recommended_next_tuning.md",
    "diagnostic_summary.md",
    "diagnostic_summary.json",
)


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return number if number.is_finite() else None


def _float(value: Any) -> float | None:
    number = _decimal(value)
    return float(number) if number is not None else None


def _round(value: Any, places: int = 6) -> float | None:
    number = _float(value)
    return round(number, places) if number is not None else None


def _pct(numerator: float, denominator: float) -> float:
    return round(100.0 * numerator / denominator, 6) if denominator else 0.0


def _quantile(values: Iterable[Any], probability: float) -> float | None:
    numbers = sorted(
        number
        for value in values
        if (number := _float(value)) is not None and math.isfinite(number)
    )
    if not numbers:
        return None
    if len(numbers) == 1:
        return numbers[0]
    position = (len(numbers) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return numbers[lower]
    return numbers[lower] + (numbers[upper] - numbers[lower]) * (position - lower)


def _mean(values: Iterable[Any]) -> float | None:
    numbers = [number for value in values if (number := _float(value)) is not None]
    return sum(numbers) / len(numbers) if numbers else None


def _iso(value: Any) -> str | None:
    number = _float(value)
    return datetime.fromtimestamp(number, UTC).isoformat().replace("+00:00", "Z") if number is not None else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: Iterable[str] = ()) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(fieldnames)
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["status"]
    if not rows:
        rows = [{"status": "DATA_INSUFFICIENT", "reason": "no qualifying rows"}]
        if "status" not in fields:
            fields.append("status")
        if "reason" not in fields:
            fields.append("reason")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(_json_safe(rows))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _read_telemetry(path: Path) -> dict[str, Any]:
    """Read the run without taking a writer lock or changing SQLite state."""

    empty = {
        "decisions": [],
        "rollups": [],
        "actions": [],
        "fills": [],
        "markouts": [],
        "trades": [],
        "health": [],
        "reference_values": [],
        "aggregates": [],
        "runtime": {},
        "tables": set(),
        "read_error": None,
    }
    if not path.exists():
        empty["read_error"] = "TELEMETRY_UNAVAILABLE"
        return empty
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.75)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        tables = {row["name"] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        result = dict(empty)
        result["tables"] = tables

        if "decisions" in tables:
            for row in connection.execute("SELECT timestamp, asset, payload_json FROM decisions ORDER BY timestamp, rowid"):
                try:
                    payload = json.loads(row["payload_json"])
                except (TypeError, json.JSONDecodeError):
                    payload = {}
                result["decisions"].append({"timestamp": float(row["timestamp"]), "asset": row["asset"], "payload": payload})
        for table in ("actions", "fills", "markouts", "trades", "aggregates", "reference_values"):
            if table == "aggregates":
                table_name = "minute_aggregates"
            else:
                table_name = table
            if table_name in tables:
                result[table] = [dict(row) for row in connection.execute(f"SELECT * FROM {table_name} ORDER BY 1, rowid")]
        if "decision_rollups" in tables:
            result["rollups"] = [dict(row) for row in connection.execute("SELECT * FROM decision_rollups ORDER BY timestamp_minute, asset, decision_signature")]
        if "reference_health" in tables:
            for row in connection.execute("SELECT timestamp, asset, venue, payload_json FROM reference_health ORDER BY timestamp, id"):
                payload = _json_mapping(row["payload_json"])
                result["health"].append({"timestamp": float(row["timestamp"]), "asset": row["asset"], "venue": row["venue"], **payload})
        if "state" in tables:
            state_row = connection.execute("SELECT value_json FROM state WHERE key='runtime'").fetchone()
            if state_row:
                result["runtime"] = _json_mapping(state_row["value_json"])
            governor_row = connection.execute("SELECT value_json FROM state WHERE key='storage_governor'").fetchone()
            if governor_row:
                result["storage_governor"] = _json_mapping(governor_row["value_json"])
        return result
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        empty["read_error"] = f"TELEMETRY_READ_UNAVAILABLE:{type(exc).__name__}"
        return empty
    finally:
        if connection is not None:
            connection.close()


def _model_parts(model: Any) -> tuple[str, str]:
    text = str(model or "UNKNOWN")
    if ":" not in text:
        return text, "UNKNOWN"
    return text.split(":", 1)[0], text.split(":", 1)[1]


def _all_models(config: RuntimeConfig, actions: list[dict[str, Any]], fills: list[dict[str, Any]]) -> list[str]:
    models = [f"{control}:{fill_model}" for control in config.control_models for fill_model in FILL_MODELS]
    models.extend(str(row.get("model")) for row in actions + fills if row.get("model"))
    return list(dict.fromkeys(models))


def _observations(data: dict[str, Any], assets: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
    """Combine exact retained decisions with non-forward-filled rollup endpoints."""

    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    raw_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    allowed = set(assets)
    for row in data.get("decisions", []):
        asset = str(row.get("asset", "")).upper()
        if asset not in allowed:
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        item = {"timestamp": float(row["timestamp"]), "payload": payload, "compressed": False, "sample_count": 1}
        raw_by_asset[asset].append(item)
        by_asset[asset].append(item)
    for row in data.get("rollups", []):
        asset = str(row.get("asset", "")).upper()
        if asset not in allowed:
            continue
        first = _float(row.get("first_timestamp"))
        last = _float(row.get("last_timestamp"))
        timestamp = last if last is not None else first
        if timestamp is None:
            continue
        # A retained exact decision inside the rollup is preferred.  When the
        # raw row was pruned, one marked endpoint remains usable for coverage
        # and is never treated as an unobserved interval.
        if any(first is not None and first - 1e-9 <= item["timestamp"] <= (last or first) + 1e-9 for item in raw_by_asset[asset]):
            continue
        payload = _json_mapping(row.get("summary_json"))
        by_asset[asset].append(
            {
                "timestamp": timestamp,
                "payload": payload,
                "compressed": True,
                "sample_count": int(row.get("count") or 1),
            }
        )
    for asset in by_asset:
        dedup: dict[float, dict[str, Any]] = {}
        for item in sorted(by_asset[asset], key=lambda value: value["timestamp"]):
            previous = dedup.get(item["timestamp"])
            if previous is None or (previous.get("compressed") and not item.get("compressed")):
                dedup[item["timestamp"]] = item
        by_asset[asset] = list(dedup.values())
    return by_asset


def _payload_values(observation: dict[str, Any]) -> dict[str, Any]:
    payload = observation.get("payload") or {}
    bid = _decimal(payload.get("derive_bid"))
    ask = _decimal(payload.get("derive_ask"))
    mid = _decimal(payload.get("derive_mid"))
    if mid is None and bid is not None and ask is not None:
        mid = (bid + ask) / Decimal("2")
    fair = _decimal(payload.get("reference_fair_value", payload.get("fair_value")))
    return {
        "derive_bid": bid,
        "derive_ask": ask,
        "derive_mid": mid,
        "fair_value": fair,
        "spread_bps": _decimal(payload.get("derive_spread_bps")),
        "basis_bps": _decimal(payload.get("basis_bps")),
        "selected_reference": payload.get("selected_reference"),
        "data_health": payload.get("data_health"),
        "desired_bid": _decimal(payload.get("desired_bid")),
        "desired_ask": _decimal(payload.get("desired_ask")),
    }


def _causal_observation(
    observations: list[dict[str, Any]],
    timestamp: float,
    *,
    strict_before: bool = False,
    max_age: float = 5.0,
) -> dict[str, Any] | None:
    for observation in reversed(observations):
        observed_at = float(observation["timestamp"])
        if (observed_at < timestamp - 1e-9 if strict_before else observed_at <= timestamp + 1e-9):
            if timestamp - observed_at > max_age:
                return None
            return {**observation, "values": _payload_values(observation)}
    return None


def _quote_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("model") or "UNKNOWN"),
        str(row.get("asset") or "").upper(),
        str(row.get("side") or "").upper(),
        str(row.get("order_id") or ""),
    )


def reconstruct_quote_lifecycles(
    actions: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    analysis_end: float,
) -> tuple[list[dict[str, Any]], int]:
    """Reconstruct intervals for each model/asset/side/order id.

    The returned ``unmatched_fill_count`` is a data-quality counter.  A fill
    without a matching open quote is not silently assigned to another quote.
    """

    events: list[tuple[float, int, int, dict[str, Any]]] = []
    for row in actions:
        timestamp = _float(row.get("timestamp"))
        if timestamp is not None:
            events.append((timestamp, 0, int(row.get("id") or 0), {**row, "_event": "action"}))
    for row in fills:
        timestamp = _float(row.get("timestamp"))
        if timestamp is not None:
            events.append((timestamp, 1, int(row.get("id") or 0), {**row, "_event": "fill"}))
    events.sort(key=lambda item: (item[0], item[1], item[2]))
    open_quotes: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    last_refresh: dict[tuple[str, str, str], dict[str, Any]] = {}
    quotes: list[dict[str, Any]] = []
    quote_ids: Counter[str] = Counter()
    unmatched_fills = 0

    def close_quote(quote: dict[str, Any], timestamp: float, reason: str) -> None:
        quote["end_time"] = max(float(quote["create_time"]), timestamp)
        quote["end_reason"] = reason
        quote["active_at_end"] = False

    for timestamp, _, _, row in events:
        if row["_event"] == "action":
            action = str(row.get("action") or "").upper()
            key = _quote_key(row)
            if action == "CREATE":
                order_id = str(row.get("order_id") or f"missing-{len(quotes) + 1}")
                base_id = f"{key[0]}:{key[1]}:{key[2]}:{order_id}"
                quote_ids[base_id] += 1
                quote_id = base_id if quote_ids[base_id] == 1 else f"{base_id}#{quote_ids[base_id]}"
                previous = open_quotes.pop(key, None)
                if previous is not None:
                    close_quote(previous, timestamp, "IMPLICIT_REPLACEMENT")
                    quotes.append(previous)
                quote = {
                    "quote_id": quote_id,
                    "order_id": row.get("order_id"),
                    "asset": key[1],
                    "side": key[2],
                    "model": key[0],
                    "control": _model_parts(key[0])[0],
                    "fill_model": _model_parts(key[0])[1],
                    "price": _float(row.get("price")),
                    "amount": _float(row.get("amount")),
                    "create_time": timestamp,
                    "active_time": timestamp,
                    "cancel_time": None,
                    "replace_time": None,
                    "fill_time": None,
                    "fill_price": None,
                    "end_time": None,
                    "end_reason": None,
                    "replacement_reason": None,
                    "replacement_new_price": None,
                    "replacement_quote_id": None,
                    "active_at_end": True,
                    "observed_samples": 0,
                    "observed_seconds": 0.0,
                    "same_price_observed_seconds": 0.0,
                    "time_at_touch_seconds": 0.0,
                    "time_one_tick_seconds": 0.0,
                    "time_two_plus_ticks_seconds": 0.0,
                    "inside_spread_seconds": 0.0,
                    "away_from_touch_seconds": 0.0,
                    "distance_touch_bps": [],
                    "distance_fair_bps": [],
                    "distance_fair_buckets": Counter(),
                    "distance_touch_buckets": Counter(),
                }
                lifecycle_key = key[:3]
                prior = last_refresh.pop(lifecycle_key, None)
                if prior is not None and prior.get("end_time") is not None and prior["end_time"] <= timestamp + 1e-9:
                    prior["replace_time"] = timestamp
                    prior["replacement_new_price"] = quote["price"]
                    prior["replacement_quote_id"] = quote_id
                open_quotes[key] = quote
            elif action in {"CANCEL", "REPLACE"}:
                quote = open_quotes.pop(key, None)
                if quote is None:
                    continue
                reason = str(row.get("reason") or "CANCELLED").upper()
                quote["cancel_time"] = timestamp
                quote["replacement_reason"] = reason
                if action == "REPLACE" or reason == "REFRESH_NEEDED":
                    end_reason = "REPLACED"
                    last_refresh[key[:3]] = quote
                elif reason == "PAUSED":
                    end_reason = "PAUSED"
                else:
                    end_reason = f"CANCELLED_{reason}"
                close_quote(quote, timestamp, end_reason)
                quotes.append(quote)
        else:
            # fills intentionally have no order_id in the existing schema;
            # match only the unique currently-open quote for this
            # model/asset/side.  Never guess across multiple candidates.
            model = str(row.get("model") or "UNKNOWN")
            asset = str(row.get("asset") or "").upper()
            side = str(row.get("side") or "").upper()
            candidates = [
                (key, quote)
                for key, quote in open_quotes.items()
                if key[:3] == (model, asset, side)
            ]
            if len(candidates) != 1:
                unmatched_fills += 1
                continue
            key, quote = candidates[0]
            quote["fill_time"] = timestamp
            quote["fill_price"] = _float(row.get("fill_price"))
            close_quote(quote, timestamp, "FILL")
            open_quotes.pop(key, None)
            quotes.append(quote)

    end = max(float(analysis_end), max((float(item["create_time"]) for item in open_quotes.values()), default=float(analysis_end)))
    for quote in open_quotes.values():
        quote["end_time"] = end
        quote["end_reason"] = "SNAPSHOT_OPEN" if analysis_end == end else "RUN_END_OPEN"
        quote["active_at_end"] = True
        quotes.append(quote)
    for quote in quotes:
        quote["lifetime_seconds"] = max(0.0, float(quote["end_time"]) - float(quote["create_time"]))
        quote["lifetime_ms"] = quote["lifetime_seconds"] * 1000.0
        quote["filled"] = quote["end_reason"] == "FILL"
    quotes.sort(key=lambda item: (float(item["create_time"]), str(item["model"]), str(item["asset"]), str(item["side"])))
    return quotes, unmatched_fills


def _lifetime_bucket(seconds: float) -> str:
    for label, lower, upper in LIFETIME_BUCKETS:
        if (lower is None or seconds >= lower) and (upper is None or seconds < upper):
            return label
    return ">10s"


def _distance_bucket(value: float | None) -> str:
    if value is None:
        return "DATA_UNAVAILABLE"
    for label, lower, upper in DISTANCE_BUCKETS:
        if (lower is None or value >= lower) and (upper is None or value < upper):
            return label
    return ">=10bps"


def _quote_distance_class(quote: dict[str, Any], values: dict[str, Any], tick_size: Decimal | None) -> tuple[str, float | None, float | None]:
    price = _decimal(quote.get("price"))
    if price is None:
        return "DATA_UNAVAILABLE", None, None
    bid, ask = values.get("derive_bid"), values.get("derive_ask")
    if quote["side"] == "BUY":
        touch = bid
    else:
        touch = ask
    if touch is None or touch <= 0:
        return "DATA_UNAVAILABLE", None, None
    distance_bps = abs(price - touch) / touch * Decimal("10000")
    ticks = abs(price - touch) / tick_size if tick_size and tick_size > 0 else None
    if ticks is not None:
        if ticks <= Decimal("0.1"):
            location = "AT_TOUCH"
        elif ticks < Decimal("1.5"):
            location = "ONE_TICK"
        elif quote["side"] == "BUY" and bid is not None and ask is not None and bid < price < ask:
            location = "INSIDE_SPREAD"
        elif quote["side"] == "SELL" and bid is not None and ask is not None and bid < price < ask:
            location = "INSIDE_SPREAD"
        elif ticks >= Decimal("1.5"):
            location = "TWO_PLUS_TICKS"
        else:
            location = "AWAY_FROM_TOUCH"
    elif quote["side"] == "BUY" and bid is not None and ask is not None and bid < price < ask:
        location = "INSIDE_SPREAD"
    elif quote["side"] == "SELL" and bid is not None and ask is not None and bid < price < ask:
        location = "INSIDE_SPREAD"
    else:
        location = "AWAY_FROM_TOUCH"
    fair = values.get("fair_value")
    fair_bps = abs(price - fair) / fair * Decimal("10000") if fair is not None and fair > 0 else None
    return location, float(distance_bps), float(fair_bps) if fair_bps is not None else None


def _apply_quote_observations(
    quotes: list[dict[str, Any]],
    observations: dict[str, list[dict[str, Any]]],
    tick_sizes: dict[str, Decimal | None],
    max_gap_seconds: float,
) -> None:
    for quote in quotes:
        asset_observations = observations.get(quote["asset"], [])
        relevant = [
            observation
            for observation in asset_observations
            if float(quote["create_time"]) <= float(observation["timestamp"]) < float(quote["end_time"])
        ]
        for index in range(len(relevant) - 1):
            current, following = relevant[index], relevant[index + 1]
            gap = float(following["timestamp"]) - float(current["timestamp"])
            if gap <= 0 or gap > max_gap_seconds or float(following["timestamp"]) > float(quote["end_time"]) + 1e-9:
                continue
            values = _payload_values(current)
            location, touch_bps, fair_bps = _quote_distance_class(quote, values, tick_sizes.get(quote["asset"]))
            quote["observed_samples"] += 1
            quote["observed_seconds"] += gap
            quote["same_price_observed_seconds"] += gap
            if touch_bps is not None:
                quote["distance_touch_bps"].append(touch_bps)
                quote["distance_touch_buckets"][_distance_bucket(touch_bps)] += 1
            if fair_bps is not None:
                quote["distance_fair_bps"].append(fair_bps)
                quote["distance_fair_buckets"][_distance_bucket(fair_bps)] += 1
            if location == "AT_TOUCH":
                quote["time_at_touch_seconds"] += gap
            elif location == "ONE_TICK":
                quote["time_one_tick_seconds"] += gap
            elif location == "TWO_PLUS_TICKS":
                quote["time_two_plus_ticks_seconds"] += gap
            elif location == "INSIDE_SPREAD":
                quote["inside_spread_seconds"] += gap
            elif location == "AWAY_FROM_TOUCH":
                quote["away_from_touch_seconds"] += gap


def _tick_sizes(config: RuntimeConfig, state: dict[str, Any], metadata: dict[str, Any], out_dir: Path) -> dict[str, Decimal | None]:
    candidates: list[Path] = []
    run_id = str(metadata.get("run_id") or out_dir.name)
    candidates.extend(
        [
            Path(config.report_dir) / run_id / "asset_reference_mapping.json",
            Path(config.report_dir) / "asset_reference_mapping.json",
            out_dir.parent / "asset_reference_mapping.json",
            Path(state.get("mapping_report_path", "")) if state.get("mapping_report_path") else Path(""),
        ]
    )
    raw: dict[str, Any] = {}
    for candidate in candidates:
        if candidate and candidate.exists():
            raw = _read_json(candidate)
            if raw:
                break
    result: dict[str, Decimal | None] = {}
    for asset, item in (raw.get("mappings") or {}).items():
        rules = item.get("rules") if isinstance(item, dict) else {}
        result[str(asset).upper()] = _decimal((rules or {}).get("tick_size"))
    for asset, item in (state.get("mappings") or {}).items():
        if str(asset).upper() in result:
            continue
        rules = item.get("rules") if isinstance(item, dict) else {}
        result[str(asset).upper()] = _decimal((rules or {}).get("tick_size"))
    return result


def _replacement_rows(
    quotes: list[dict[str, Any]],
    observations: dict[str, list[dict[str, Any]]],
    tick_sizes: dict[str, Decimal | None],
    max_observation_age: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for quote in quotes:
        if quote.get("end_reason") != "REPLACED":
            continue
        old_price = _decimal(quote.get("price"))
        new_price = _decimal(quote.get("replacement_new_price"))
        before = _causal_observation(observations.get(quote["asset"], []), float(quote["cancel_time"]), strict_before=True, max_age=max_observation_age)
        after_time = quote.get("replace_time") or quote.get("cancel_time")
        after = _causal_observation(observations.get(quote["asset"], []), float(after_time), max_age=max_observation_age)
        before_values = (before or {}).get("values", {})
        after_values = (after or {}).get("values", {})
        fair_before, fair_after = before_values.get("fair_value"), after_values.get("fair_value")
        fair_move = abs(fair_after - fair_before) / fair_before * Decimal("10000") if fair_before and fair_after and fair_before > 0 else None
        bbo_moves: list[Decimal] = []
        for key in ("derive_bid", "derive_ask"):
            old_value, new_value = before_values.get(key), after_values.get(key)
            if old_value and new_value and old_value > 0:
                bbo_moves.append(abs(new_value - old_value) / old_value * Decimal("10000"))
        derive_bbo_move = max(bbo_moves) if bbo_moves else None
        quote_move = abs(new_price - old_price) / old_price * Decimal("10000") if old_price and new_price and old_price > 0 else None
        tick = tick_sizes.get(quote["asset"])
        quote_move_ticks = abs(new_price - old_price) / tick if new_price is not None and old_price is not None and tick and tick > 0 else None
        if fair_move is not None and (derive_bbo_move is None or fair_move >= derive_bbo_move):
            driver = "FAIR_VALUE_MOVE"
        elif derive_bbo_move is not None:
            driver = "DERIVE_BBO_MOVE"
        elif quote_move is not None and quote_move < Decimal("0.5"):
            driver = "MICRO_CHURN"
        elif before is None or after is None:
            driver = "DATA_UNAVAILABLE"
        else:
            driver = "OTHER"
        rows.append(
            {
                "quote_id": quote["quote_id"],
                "asset": quote["asset"],
                "side": quote["side"],
                "model": quote["model"],
                "cancel_time": quote.get("cancel_time"),
                "replace_time": quote.get("replace_time"),
                "raw_reason": quote.get("replacement_reason"),
                "old_price": _round(old_price),
                "new_price": _round(new_price),
                "quote_move_bps": _round(quote_move),
                "quote_move_ticks": _round(quote_move_ticks),
                "fair_value_before": _round(fair_before),
                "fair_value_after": _round(fair_after),
                "fair_value_move_bps": _round(fair_move),
                "derive_bbo_move_bps": _round(derive_bbo_move),
                "derive_bid_before": _round(before_values.get("derive_bid")),
                "derive_bid_after": _round(after_values.get("derive_bid")),
                "derive_ask_before": _round(before_values.get("derive_ask")),
                "derive_ask_after": _round(after_values.get("derive_ask")),
                "driver": driver,
                "observation_quality": "CAUSAL_OBSERVED" if before is not None and after is not None else "INSUFFICIENT_OBSERVED_DATA",
            }
        )
    return rows


def _active_quote(quotes: list[dict[str, Any]], asset: str, model: str, side: str, timestamp: float) -> dict[str, Any] | None:
    candidates = [
        quote
        for quote in quotes
        if quote["asset"] == asset
        and quote["model"] == model
        and quote["side"] == side
        and float(quote["create_time"]) < timestamp - 1e-9
        and float(quote["end_time"]) > timestamp + 1e-9
    ]
    return max(candidates, key=lambda quote: float(quote["create_time"])) if candidates else None


def _trade_hit(trade_side: str, quote_side: str, trade_price: Decimal | None, quote_price: Decimal | None, *, strict: bool) -> bool:
    if trade_price is None or quote_price is None:
        return False
    if quote_side == "BUY" and trade_side == "SELL":
        return trade_price < quote_price if strict else trade_price <= quote_price
    if quote_side == "SELL" and trade_side == "BUY":
        return trade_price > quote_price if strict else trade_price >= quote_price
    return False


def _fill_matches_trade(quote: dict[str, Any], trade: dict[str, Any]) -> bool:
    fill_time = _float(quote.get("fill_time"))
    trade_time = _float(trade.get("timestamp"))
    if fill_time is None or trade_time is None or quote.get("end_reason") != "FILL":
        return False
    if abs(fill_time - trade_time) > 0.750:
        return False
    fill_price, quote_price = _decimal(quote.get("fill_price")), _decimal(quote.get("price"))
    trade_price = _decimal(trade.get("price"))
    return fill_price == quote_price and fill_price is not None and trade_price is not None


def _crossing_rows(
    trades: list[dict[str, Any]],
    quotes: list[dict[str, Any]],
    models: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    derive_trades = [row for row in trades if str(row.get("source")) == "derive"]
    for trade in derive_trades:
        asset = str(trade.get("asset") or "").upper()
        timestamp = _float(trade.get("timestamp"))
        if timestamp is None:
            continue
        trade_side = str(trade.get("side") or "").upper()
        trade_price = _decimal(trade.get("price"))
        for model in models:
            for side in ("BUY", "SELL"):
                quote = _active_quote(quotes, asset, model, side, timestamp)
                strict = _trade_hit(trade_side, side, trade_price, _decimal((quote or {}).get("price")), strict=True) if quote else False
                touch = _trade_hit(trade_side, side, trade_price, _decimal((quote or {}).get("price")), strict=False) if quote else False
                timing_ambiguous = bool(quote and _float(trade.get("exchange_timestamp")) is not None and _float(trade.get("exchange_timestamp")) <= float(quote["create_time"]))
                fill_observed = bool(quote and _fill_matches_trade(quote, trade))
                _, fill_model = _model_parts(model)
                mismatch = bool(strict and fill_model == "CONSERVATIVE" and not fill_observed and not timing_ambiguous)
                rows.append(
                    {
                        "trade_id": trade.get("trade_id"),
                        "trade_timestamp": timestamp,
                        "exchange_timestamp": trade.get("exchange_timestamp"),
                        "asset": asset,
                        "trade_side": trade_side,
                        "trade_price": _round(trade_price),
                        "trade_amount": _round(trade.get("amount")),
                        "model": model,
                        "quote_side": side,
                        "quote_id": quote.get("quote_id") if quote else None,
                        "quote_price": _round(quote.get("price")) if quote else None,
                        "quote_create_time": quote.get("create_time") if quote else None,
                        "quote_end_time": quote.get("end_time") if quote else None,
                        "quote_active_immediately_before": bool(quote),
                        "strict_trade_through": strict,
                        "touch_or_better": touch,
                        "conservative_fill_observed": fill_observed if fill_model == "CONSERVATIVE" else False,
                        "touch_fill_observed": fill_observed if fill_model == "TOUCH_SENSITIVITY" else False,
                        "timing_ambiguous": timing_ambiguous,
                        "fill_logic_mismatch": mismatch,
                        "classification": (
                            "FILL_LOGIC_MISMATCH" if mismatch else
                            "STRICT_TRADE_THROUGH" if strict else
                            "TOUCH_ONLY" if touch else
                            "NO_ACTIVE_QUOTE" if quote is None else
                            "NO_CROSSING"
                        ),
                    }
                )
    return rows


def _latest_cancelled_quote(quotes: list[dict[str, Any]], asset: str, model: str, side: str, timestamp: float) -> dict[str, Any] | None:
    candidates = [
        quote
        for quote in quotes
        if quote["asset"] == asset
        and quote["model"] == model
        and quote["side"] == side
        and quote.get("end_reason") not in {"FILL", "SNAPSHOT_OPEN", "RUN_END_OPEN"}
        and float(quote.get("end_time") or 0) <= timestamp + 1e-9
    ]
    return max(candidates, key=lambda quote: float(quote["end_time"])) if candidates else None


def _missed_fill_rows(trades: list[dict[str, Any]], quotes: list[dict[str, Any]], models: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in (row for row in trades if str(row.get("source")) == "derive"):
        asset = str(trade.get("asset") or "").upper()
        timestamp = _float(trade.get("timestamp"))
        if timestamp is None:
            continue
        trade_side = str(trade.get("side") or "").upper()
        relevant_side = "BUY" if trade_side == "SELL" else "SELL" if trade_side == "BUY" else "UNKNOWN"
        for model in models:
            quote = _active_quote(quotes, asset, model, relevant_side, timestamp) if relevant_side != "UNKNOWN" else None
            fill_model = _model_parts(model)[1]
            category = "FILLED"
            detail = ""
            if relevant_side == "UNKNOWN":
                category = "AGGRESSOR_UNKNOWN"
            elif quote is None:
                prior = _latest_cancelled_quote(quotes, asset, model, relevant_side, timestamp)
                if prior is not None and _trade_hit(trade_side, relevant_side, _decimal(trade.get("price")), _decimal(prior.get("price")), strict=True):
                    category = "QUOTE_CANCELED_BEFORE_TRADE"
                    detail = str(prior.get("end_reason"))
                elif not any(item["asset"] == asset and item["model"] == model and item["side"] == relevant_side for item in quotes):
                    category = "MISSING_QUOTE_DATA"
                else:
                    category = "NO_QUOTE_ACTIVE"
            elif _float(trade.get("exchange_timestamp")) is not None and _float(trade.get("exchange_timestamp")) <= float(quote["create_time"]):
                category = "TIMING_AMBIGUOUS"
            elif _trade_hit(trade_side, relevant_side, _decimal(trade.get("price")), _decimal(quote.get("price")), strict=True):
                if _fill_matches_trade(quote, trade):
                    category = "FILLED"
                else:
                    category = "FILL_LOGIC_MISMATCH" if fill_model == "CONSERVATIVE" else "FILL_SIMULATOR_REJECTED"
            elif _trade_hit(trade_side, relevant_side, _decimal(trade.get("price")), _decimal(quote.get("price")), strict=False):
                category = "FILL_SIMULATOR_TOO_STRICT"
            else:
                category = "QUOTE_TOO_PASSIVE"
            if category != "FILLED":
                rows.append(
                    {
                        "trade_id": trade.get("trade_id"),
                        "trade_timestamp": timestamp,
                        "asset": asset,
                        "model": model,
                        "relevant_quote_side": relevant_side,
                        "trade_side": trade_side,
                        "trade_price": _round(trade.get("price")),
                        "quote_id": quote.get("quote_id") if quote else None,
                        "quote_price": _round(quote.get("price")) if quote else None,
                        "category": category,
                        "detail": detail,
                    }
                )
    return rows


def _cancel_before_rows(trades: list[dict[str, Any]], quotes: list[dict[str, Any]], models: list[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    derive_trades = [row for row in trades if str(row.get("source")) == "derive"]
    for asset in sorted({str(row.get("asset") or "").upper() for row in derive_trades} | {str(row.get("asset") or "").upper() for row in quotes}):
        asset_trades = [row for row in derive_trades if str(row.get("asset") or "").upper() == asset]
        for model in models:
            for window in CANCEL_BEFORE_WINDOWS:
                count = 0
                churn_count = 0
                for trade in asset_trades:
                    timestamp = _float(trade.get("timestamp"))
                    if timestamp is None:
                        continue
                    side = "BUY" if str(trade.get("side") or "").upper() == "SELL" else "SELL" if str(trade.get("side") or "").upper() == "BUY" else "UNKNOWN"
                    if side == "UNKNOWN":
                        continue
                    prior = _latest_cancelled_quote(quotes, asset, model, side, timestamp)
                    if prior is None:
                        continue
                    gap = timestamp - float(prior["end_time"])
                    if 0 <= gap <= window and _trade_hit(str(trade.get("side") or "").upper(), side, _decimal(trade.get("price")), _decimal(prior.get("price")), strict=True):
                        count += 1
                        churn_count += int(prior.get("end_reason") == "REPLACED")
                result.append(
                    {
                        "asset": asset,
                        "model": model,
                        "window_ms": round(window * 1000.0, 3),
                        "cancel_before_trade_count": count,
                        "potential_churn_missed_fill_count": churn_count,
                        "denominator_derive_trades": len(asset_trades),
                        "pct_of_derive_trades": _pct(count, len(asset_trades)),
                    }
                )
    return result


def _mutation_rows(actions: list[dict[str, Any]], quotes: list[dict[str, Any]], assets: list[str], start: float, end: float) -> list[dict[str, Any]]:
    duration = max(0.001, end - start)
    rows: list[dict[str, Any]] = []
    for asset in assets:
        for model in sorted({str(row.get("model") or "UNKNOWN") for row in actions if str(row.get("asset") or "").upper() == asset} | {str(row.get("model")) for row in quotes if str(row.get("asset") or "").upper() == asset}):
            model_actions = [row for row in actions if str(row.get("asset") or "").upper() == asset and str(row.get("model") or "UNKNOWN") == model]
            mutations = [row for row in model_actions if str(row.get("action") or "").upper() != "HOLD"]
            mutation_times = [_float(row.get("timestamp")) for row in mutations]
            mutation_times = [value for value in mutation_times if value is not None]
            minute_counts: list[int] = []
            if mutation_times:
                cursor = math.floor(min(mutation_times) / 60.0) * 60.0
                finish = max(mutation_times)
                while cursor <= finish:
                    minute_counts.append(sum(cursor <= value < cursor + 60.0 for value in mutation_times))
                    cursor += 60.0
            hold_count = sum(1 for row in model_actions if str(row.get("action") or "").upper() == "HOLD")
            rows.append(
                {
                    "asset": asset,
                    "model": model,
                    "create_count": sum(str(row.get("action") or "").upper() == "CREATE" for row in model_actions),
                    "replace_count": sum(str(row.get("action") or "").upper() == "REPLACE" or (str(row.get("action") or "").upper() == "CANCEL" and str(row.get("reason") or "").upper() == "REFRESH_NEEDED") for row in model_actions),
                    "cancel_count": sum(str(row.get("action") or "").upper() == "CANCEL" and str(row.get("reason") or "").upper() != "REFRESH_NEEDED" for row in model_actions),
                    "hold_count_aggregated_or_raw": hold_count,
                    "mutation_count_excluding_hold": len(mutations),
                    "run_minutes": duration / 60.0,
                    "mutations_per_min": len(mutations) / (duration / 60.0),
                    "rolling_60s_max": max(minute_counts, default=0),
                    "rolling_60s_p90": _quantile(minute_counts, 0.90),
                    "hold_excluded_from_mutation_rate": True,
                }
            )
    return rows


def _lifetime_rows(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for quote in quotes:
        row = {
            key: value
            for key, value in quote.items()
            if key not in {"distance_touch_bps", "distance_fair_bps", "distance_fair_buckets", "distance_touch_buckets"}
        }
        row.update(
            {
                "lifetime_bucket": _lifetime_bucket(float(quote["lifetime_seconds"])),
                "queue_residency_proxy_seconds": _round(quote.get("same_price_observed_seconds")),
                "queue_residency_definition": "continuous same-price observed time; not real exchange queue position",
                "distance_touch_bps_median": _quantile(quote.get("distance_touch_bps", []), 0.5),
                "distance_touch_bps_p95": _quantile(quote.get("distance_touch_bps", []), 0.95),
                "distance_fair_bps_median": _quantile(quote.get("distance_fair_bps", []), 0.5),
                "distance_fair_bps_p95": _quantile(quote.get("distance_fair_bps", []), 0.95),
            }
        )
        rows.append(row)
    return rows


def _lifetime_distribution(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for quote in quotes:
        groups[(quote["asset"], quote["model"], quote["side"])].append(quote)
    rows: list[dict[str, Any]] = []
    for (asset, model, side), items in sorted(groups.items()):
        lifetimes = [float(item["lifetime_seconds"]) for item in items]
        row: dict[str, Any] = {
            "asset": asset,
            "model": model,
            "side": side,
            "count": len(items),
            "min_lifetime_ms": _round(min(lifetimes) * 1000.0),
            "p25_lifetime_ms": _round(_quantile(lifetimes, 0.25) * 1000.0 if _quantile(lifetimes, 0.25) is not None else None),
            "median_lifetime_ms": _round(_quantile(lifetimes, 0.50) * 1000.0 if _quantile(lifetimes, 0.50) is not None else None),
            "p75_lifetime_ms": _round(_quantile(lifetimes, 0.75) * 1000.0 if _quantile(lifetimes, 0.75) is not None else None),
            "p90_lifetime_ms": _round(_quantile(lifetimes, 0.90) * 1000.0 if _quantile(lifetimes, 0.90) is not None else None),
            "p95_lifetime_ms": _round(_quantile(lifetimes, 0.95) * 1000.0 if _quantile(lifetimes, 0.95) is not None else None),
            "p99_lifetime_ms": _round(_quantile(lifetimes, 0.99) * 1000.0 if _quantile(lifetimes, 0.99) is not None else None),
            "max_lifetime_ms": _round(max(lifetimes) * 1000.0),
            "filled_count": sum(item.get("filled") is True for item in items),
            "active_at_end_count": sum(item.get("active_at_end") is True for item in items),
        }
        for label, threshold in SURVIVAL_THRESHOLDS:
            count = sum(lifetime >= threshold for lifetime in lifetimes)
            row[f"survived_{label}_count"] = count
            row[f"survived_{label}_pct"] = _pct(count, len(items))
        rows.append(row)
    return rows


def _residency_rows(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for quote in quotes:
        groups[(quote["asset"], quote["model"], quote["side"])].append(quote)
    rows: list[dict[str, Any]] = []
    for (asset, model, side), items in sorted(groups.items()):
        observed = sum(float(item.get("observed_seconds") or 0.0) for item in items)
        rows.append(
            {
                "asset": asset,
                "model": model,
                "side": side,
                "quote_count": len(items),
                "observed_seconds": _round(observed),
                "queue_residency_proxy_median_ms": _round(_quantile([item.get("same_price_observed_seconds") for item in items], 0.5) * 1000 if _quantile([item.get("same_price_observed_seconds") for item in items], 0.5) is not None else None),
                "queue_residency_proxy_p90_ms": _round(_quantile([item.get("same_price_observed_seconds") for item in items], 0.90) * 1000 if _quantile([item.get("same_price_observed_seconds") for item in items], 0.90) is not None else None),
                "quote_time_at_touch_seconds": _round(sum(float(item.get("time_at_touch_seconds") or 0.0) for item in items)),
                "quote_time_one_tick_seconds": _round(sum(float(item.get("time_one_tick_seconds") or 0.0) for item in items)),
                "quote_time_two_plus_ticks_seconds": _round(sum(float(item.get("time_two_plus_ticks_seconds") or 0.0) for item in items)),
                "inside_spread_seconds": _round(sum(float(item.get("inside_spread_seconds") or 0.0) for item in items)),
                "away_from_touch_seconds": _round(sum(float(item.get("away_from_touch_seconds") or 0.0) for item in items)),
                "definition": "proxy only; continuous same-price observed time, no exchange queue data",
            }
        )
    return rows


def _distance_rows(quotes: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for quote in quotes:
        values = quote.get("distance_touch_bps" if kind == "touch" else "distance_fair_bps", [])
        buckets = quote.get("distance_touch_buckets" if kind == "touch" else "distance_fair_buckets", {})
        rows.append(
            {
                "quote_id": quote["quote_id"],
                "asset": quote["asset"],
                "model": quote["model"],
                "side": quote["side"],
                "observed_sample_count": len(values),
                "observed_seconds": _round(quote.get("observed_seconds")),
                "distance_bps_median": _quantile(values, 0.5),
                "distance_bps_p95": _quantile(values, 0.95),
                "distance_bps_min": min(values) if values else None,
                "distance_bps_max": max(values) if values else None,
                "bucket_counts": dict(buckets),
                "data_status": "OBSERVED" if values else "DATA_INSUFFICIENT",
            }
        )
    return rows


def _fair_bucket_rows(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], dict[str, float]] = defaultdict(lambda: {"samples": 0, "seconds": 0.0})
    for quote in quotes:
        for bucket, count in quote.get("distance_fair_buckets", {}).items():
            key = (quote["asset"], quote["model"], quote["side"], bucket)
            groups[key]["samples"] += int(count)
            groups[key]["seconds"] += float(quote.get("observed_seconds") or 0.0) * int(count) / max(1, len(quote.get("distance_fair_bps", [])))
    totals: dict[tuple[str, str, str], float] = defaultdict(float)
    for (asset, model, side, _), value in groups.items():
        totals[(asset, model, side)] += value["samples"]
    return [
        {
            "asset": asset,
            "model": model,
            "side": side,
            "distance_to_fair_bucket": bucket,
            "observed_sample_count": int(value["samples"]),
            "observed_seconds_estimate": _round(value["seconds"]),
            "pct_of_distance_samples": _pct(value["samples"], totals[(asset, model, side)]),
        }
        for (asset, model, side, bucket), value in sorted(groups.items())
    ]


def _replacement_reason_rows(replacements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str, str]] = Counter()
    for row in replacements:
        counts[(row["asset"], row["model"], row["side"], str(row.get("driver") or "DATA_UNAVAILABLE"))] += 1
    totals: Counter[tuple[str, str, str]] = Counter()
    for (asset, model, side, _), count in counts.items():
        totals[(asset, model, side)] += count
    return [
        {
            "asset": asset,
            "model": model,
            "side": side,
            "replacement_reason": reason,
            "count": count,
            "pct_of_replacements": _pct(count, totals[(asset, model, side)]),
            "denominator_replacements": totals[(asset, model, side)],
        }
        for (asset, model, side, reason), count in sorted(counts.items())
    ]


def _micro_churn_rows(replacements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for replacement in replacements:
        groups[(replacement["asset"], replacement["model"])].append(replacement)
    for (asset, model), items in sorted(groups.items()):
        for measure, field in (("fair_value_move_bps", "fair_value_move_bps"), ("quote_move_bps", "quote_move_bps")):
            available = [float(row[field]) for row in items if row.get(field) is not None]
            for threshold in MICRO_CHURN_THRESHOLDS:
                count = sum(value < threshold for value in available)
                rows.append(
                    {
                        "asset": asset,
                        "model": model,
                        "measure": measure,
                        "threshold_bps": threshold,
                        "below_threshold_count": count,
                        "below_threshold_pct": _pct(count, len(available)),
                        "available_replacement_events": len(available),
                        "high_micro_churn_rule": "flag when >=50% of >=5 observed replacements are below 0.5bps",
                    }
                )
            count = sum(value > 1.0 for value in available)
            rows.append(
                {
                    "asset": asset,
                    "model": model,
                    "measure": measure,
                    "threshold_bps": ">1",
                    "below_threshold_count": count,
                    "below_threshold_pct": _pct(count, len(available)),
                    "available_replacement_events": len(available),
                    "high_micro_churn_rule": "flag when >=50% of >=5 observed replacements are below 0.5bps",
                }
            )
    return rows


def _fill_logic_rows(crossings: list[dict[str, Any]], fills: list[dict[str, Any]], unmatched_fills: int) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in crossings:
        groups[(row["asset"], row["model"])].append(row)
    rows: list[dict[str, Any]] = []
    for (asset, model), items in sorted(groups.items()):
        strict = sum(bool(row["strict_trade_through"]) for row in items)
        touches = sum(bool(row["touch_or_better"]) for row in items)
        mismatches = sum(bool(row["fill_logic_mismatch"]) for row in items)
        observed_fills = sum(bool(row["conservative_fill_observed"] or row["touch_fill_observed"]) for row in items)
        rows.append(
            {
                "asset": asset,
                "model": model,
                "derive_trade_side_rows": len(items),
                "strict_trade_throughs": strict,
                "touch_or_better": touches,
                "observed_fills_associated": observed_fills,
                "strict_crossings_without_associated_fill": max(0, strict - observed_fills),
                "fill_logic_mismatch_count": mismatches,
                "unmatched_fill_rows_run_wide": unmatched_fills,
                "status": "FILL_LOGIC_MISMATCH" if mismatches else "NO_MISMATCH_OBSERVED",
                "association_policy": "same model/side/quote, receipt-time difference <=750ms and fill price equals quote price",
            }
        )
    return rows


def _markout_stats(markouts: list[dict[str, Any]], asset: str, model: str) -> dict[str, Any]:
    selected = [row for row in markouts if str(row.get("asset")) == asset and str(row.get("model")) == model]
    result: dict[str, Any] = {}
    for horizon in (1, 5, 15, 30, 60):
        values = [_float(row.get("derive_markout_bps")) for row in selected if int(row.get("horizon_seconds") or 0) == horizon]
        values = [value for value in values if value is not None]
        result[f"markout_{horizon}s_count"] = len(values)
        result[f"markout_{horizon}s_median_bps"] = _round(_quantile(values, 0.5))
    return result


def _aggregate_uptime(data: dict[str, Any], asset: str) -> tuple[float | None, float | None, float | None]:
    rows = [row for row in data.get("aggregates", []) if str(row.get("asset")) == asset]
    if not rows:
        return None, None, None
    observations = sum(int(row.get("quote_observations") or 0) for row in rows)
    uptime = sum(int(row.get("quote_uptime_observations") or 0) for row in rows)
    return (_pct(uptime, observations) if observations else None, float(observations), float(uptime))


def _activity_rows(
    config: RuntimeConfig,
    assets: list[str],
    quotes: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    markouts: list[dict[str, Any]],
    crossings: list[dict[str, Any]],
    replacements: list[dict[str, Any]],
    data: dict[str, Any],
    duration: float,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    models = sorted({quote["model"] for quote in quotes} | {str(row.get("model")) for row in actions if row.get("model")} | {f"{control}:{fill}" for control in config.control_models for fill in FILL_MODELS})
    result: list[dict[str, Any]] = []
    primary: dict[str, dict[str, Any]] = {}
    observations_by_asset = _observations(data, assets)
    for asset in assets:
        derive_trades = [row for row in trades if str(row.get("source")) == "derive" and str(row.get("asset") or "").upper() == asset]
        asset_observations = observations_by_asset.get(asset, [])
        spread_values = []
        for observation in asset_observations:
            spread = _payload_values(observation).get("spread_bps")
            if spread is not None:
                spread_values.append(float(spread))
        latest_values = _payload_values(asset_observations[-1]) if asset_observations else {}
        asset_fills = [
            row
            for row in data.get("fills", [])
            if str(row.get("asset") or "").upper() == asset
        ]
        uptime_pct, uptime_observations, uptime_active = _aggregate_uptime(data, asset)
        for model in models:
            model_quotes = [quote for quote in quotes if quote["asset"] == asset and quote["model"] == model]
            model_crossings = [row for row in crossings if row["asset"] == asset and row["model"] == model]
            model_replacements = [row for row in replacements if row["asset"] == asset and row["model"] == model]
            strict = sum(bool(row["strict_trade_through"]) for row in model_crossings)
            touches = sum(bool(row["touch_or_better"]) for row in model_crossings)
            fill_model = _model_parts(model)[1]
            fills = sum(quote.get("filled") is True for quote in model_quotes)
            markout = _markout_stats(markouts, asset, model)
            micro_values = [row.get("quote_move_bps") for row in model_replacements if row.get("quote_move_bps") is not None]
            micro_pct = _pct(sum(float(value) < 0.5 for value in micro_values), len(micro_values))
            driver_counts = Counter(str(row.get("driver")) for row in model_replacements)
            mutation_rows = [row for row in actions if str(row.get("asset") or "").upper() == asset and str(row.get("model") or "") == model and str(row.get("action") or "").upper() != "HOLD"]
            model_actions = [row for row in actions if str(row.get("asset") or "").upper() == asset and str(row.get("model") or "") == model]
            create_count = sum(str(row.get("action") or "").upper() == "CREATE" for row in model_actions)
            replace_count = sum(
                str(row.get("action") or "").upper() == "REPLACE"
                or (
                    str(row.get("action") or "").upper() == "CANCEL"
                    and str(row.get("reason") or "").upper() == "REFRESH_NEEDED"
                )
                for row in model_actions
            )
            cancel_count = sum(
                str(row.get("action") or "").upper() == "CANCEL"
                and str(row.get("reason") or "").upper() != "REFRESH_NEEDED"
                for row in model_actions
            )
            replacement_actions = [
                row
                for row in model_actions
                if str(row.get("action") or "").upper() == "REPLACE"
                or (
                    str(row.get("action") or "").upper() == "CANCEL"
                    and str(row.get("reason") or "").upper() == "REFRESH_NEEDED"
                )
            ]
            latest_replacement = max(
                replacement_actions,
                key=lambda row: _float(row.get("timestamp")) or float("-inf"),
                default=None,
            )
            median_lifetime = _quantile([quote["lifetime_seconds"] for quote in model_quotes], 0.5)
            queue_median = _quantile([quote.get("same_price_observed_seconds") for quote in model_quotes], 0.5)
            observed_quote_seconds = sum(float(quote.get("observed_seconds") or 0.0) for quote in model_quotes)
            touch_seconds = sum(float(quote.get("time_at_touch_seconds") or 0.0) for quote in model_quotes)
            one_tick_seconds = sum(float(quote.get("time_one_tick_seconds") or 0.0) for quote in model_quotes)
            two_plus_seconds = sum(float(quote.get("time_two_plus_ticks_seconds") or 0.0) for quote in model_quotes)
            inside_seconds = sum(float(quote.get("inside_spread_seconds") or 0.0) for quote in model_quotes)
            away_seconds = sum(float(quote.get("away_from_touch_seconds") or 0.0) for quote in model_quotes)
            lifetimes = [float(quote["lifetime_seconds"]) for quote in model_quotes]
            touch_distance_values = [
                float(value)
                for quote in model_quotes
                for value in quote.get("distance_touch_bps", [])
                if _float(value) is not None
            ]
            fair_distance_values = [
                float(value)
                for quote in model_quotes
                for value in quote.get("distance_fair_bps", [])
                if _float(value) is not None
            ]
            open_quotes = [quote for quote in model_quotes if quote.get("active_at_end") is True]
            current_by_side = {
                side: max(
                    (quote for quote in open_quotes if quote.get("side") == side),
                    key=lambda quote: float(quote.get("create_time") or 0.0),
                    default=None,
                )
                for side in ("BUY", "SELL")
            }
            current_bid = current_by_side["BUY"]
            current_ask = current_by_side["SELL"]
            last_trade = max(
                derive_trades,
                key=lambda row: _float(row.get("timestamp")) or float("-inf"),
                default=None,
            )
            model_fills = [
                row
                for row in asset_fills
                if str(row.get("model") or "") == model
            ]
            last_fill = max(
                model_fills,
                key=lambda row: _float(row.get("timestamp")) or float("-inf"),
                default=None,
            )
            if not derive_trades:
                classification, root = "NO_DERIVE_TRADES", "LOW_MARKET_ACTIVITY"
            elif strict > fills and any(row.get("fill_logic_mismatch") for row in model_crossings):
                classification, root = "CROSSINGS_NO_FILL", "FILL_LOGIC_MISMATCH"
            elif strict == 0 and touches > 0 and fill_model == "CONSERVATIVE":
                classification, root = "CROSSINGS_TOUCH_ONLY", "FILL_SIMULATOR_TOO_STRICT"
            elif median_lifetime is not None and median_lifetime < 0.500 and fills == 0:
                classification, root = "SHORT_QUOTES", "QUOTE_LIFETIME_TOO_SHORT"
            elif len(model_replacements) >= 5 and micro_pct >= 50.0:
                classification, root = "HIGH_CHURN", "MICRO_CHURN_HIGH"
            elif strict == 0 and touches == 0:
                classification, root = "TRADES_BUT_NO_CROSSINGS", "QUOTE_TOO_PASSIVE"
            elif driver_counts.get("FAIR_VALUE_MOVE", 0) > max(driver_counts.get("DERIVE_BBO_MOVE", 0), len(model_replacements) / 2):
                classification, root = "REFERENCE_OVER_REPLACEMENT", "REFERENCE_DRIVEN_OVER_REPLACEMENT"
            elif driver_counts.get("DERIVE_BBO_MOVE", 0) > len(model_replacements) / 2:
                classification, root = "DERIVE_BBO_OVER_REPLACEMENT", "DERIVE_BBO_DRIVEN_OVER_REPLACEMENT"
            elif queue_median is not None and queue_median < 0.250 and fills == 0:
                classification, root = "LOW_QUEUE_RESIDENCY_PROXY", "QUEUE_RESIDENCY_LOW"
            else:
                classification, root = "HEALTHY", "HEALTHY"
            markout30 = int(markout.get("markout_30s_count") or 0)
            markout60 = int(markout.get("markout_60s_count") or 0)
            sample_sufficient = (len(derive_trades) >= 30 or fills >= 20) and markout30 >= 20 and markout60 >= 20
            sample_status = "SUFFICIENT" if sample_sufficient else "MORE_DATA_REQUIRED"
            row = {
                "asset": asset,
                "model": model,
                "fill_model": fill_model,
                "derive_trades": len(derive_trades),
                "trades_per_hour": _round(len(derive_trades) / max(duration / 3600.0, 1 / 3600.0)),
                "trade_notional": _round(sum((_float(item.get("amount")) or 0.0) * (_float(item.get("price")) or 0.0) for item in derive_trades)),
                "median_spread_bps": _round(_quantile(spread_values, 0.5)),
                "quote_count": len(model_quotes),
                "open_quote_count": sum(quote.get("active_at_end") is True for quote in model_quotes),
                "current_shadow_bid": _round(current_bid.get("price")) if current_bid else None,
                "current_shadow_ask": _round(current_ask.get("price")) if current_ask else None,
                "current_bid_age_ms": _round(float(current_bid.get("lifetime_seconds")) * 1000.0) if current_bid else None,
                "current_ask_age_ms": _round(float(current_ask.get("lifetime_seconds")) * 1000.0) if current_ask else None,
                "current_quote_age_ms": _round(max(float(quote.get("lifetime_seconds") or 0.0) for quote in open_quotes) * 1000.0) if open_quotes else None,
                "current_derive_bid": _round(latest_values.get("derive_bid")),
                "current_derive_ask": _round(latest_values.get("derive_ask")),
                "median_lifetime_ms": _round(median_lifetime * 1000.0 if median_lifetime is not None else None),
                "p90_lifetime_ms": _round(_quantile([quote["lifetime_seconds"] for quote in model_quotes], 0.90) * 1000.0 if _quantile([quote["lifetime_seconds"] for quote in model_quotes], 0.90) is not None else None),
                **{
                    f"survived_{label}_pct": _pct(sum(value >= threshold for value in lifetimes), len(lifetimes))
                    for label, threshold in SURVIVAL_THRESHOLDS
                },
                "queue_residency_proxy_median_ms": _round(queue_median * 1000.0 if queue_median is not None else None),
                "queue_residency_proxy_definition": "continuous same-price observed time; not real queue position",
                "quote_mutations": len(mutation_rows),
                "quote_mutations_per_min": _round(len(mutation_rows) / max(duration / 60.0, 1 / 60.0)),
                "creates_per_min": _round(create_count / max(duration / 60.0, 1 / 60.0)),
                "replaces_per_min": _round(replace_count / max(duration / 60.0, 1 / 60.0)),
                "cancels_per_min": _round(cancel_count / max(duration / 60.0, 1 / 60.0)),
                "current_replace_reason": latest_replacement.get("reason") if latest_replacement else None,
                "distance_to_touch_median_bps": _round(_quantile(touch_distance_values, 0.5)),
                "distance_to_fair_value_median_bps": _round(_quantile(fair_distance_values, 0.5)),
                "trades_while_quoting": len(model_crossings),
                "last_trade_time_utc": _iso(last_trade.get("timestamp")) if last_trade else None,
                "last_trade_price": _round(last_trade.get("price")) if last_trade else None,
                "last_fill_time_utc": _iso(last_fill.get("timestamp")) if last_fill else None,
                "last_fill_price": _round(last_fill.get("price")) if last_fill else None,
                "strict_crossings": strict,
                "touches_or_better": touches,
                "conservative_fills": fills if fill_model == "CONSERVATIVE" else 0,
                "touch_fills": fills if fill_model == "TOUCH_SENSITIVITY" else 0,
                "markout_30s_count": markout30,
                "markout_60s_count": markout60,
                "markout_30s_median_bps": markout.get("markout_30s_median_bps"),
                "markout_60s_median_bps": markout.get("markout_60s_median_bps"),
                "quote_uptime_pct": uptime_pct,
                "quote_uptime_observations": uptime_observations,
                "quote_active_observations": uptime_active,
                "observed_quote_seconds": _round(observed_quote_seconds),
                "time_at_touch_seconds": _round(touch_seconds),
                "time_one_tick_seconds": _round(one_tick_seconds),
                "time_two_plus_ticks_seconds": _round(two_plus_seconds),
                "inside_spread_seconds": _round(inside_seconds),
                "away_from_touch_seconds": _round(away_seconds),
                "micro_churn_below_0_5bps_pct": micro_pct,
                "replacement_count": len(model_replacements),
                "replacement_fair_value_move_count": driver_counts.get("FAIR_VALUE_MOVE", 0),
                "replacement_derive_bbo_move_count": driver_counts.get("DERIVE_BBO_MOVE", 0),
                "replacement_micro_churn_count": driver_counts.get("MICRO_CHURN", 0),
                "activity_classification": classification,
                "root_cause": root,
                "sample_status": sample_status,
                "evidence_rule": "sufficient only when derive trades >=30 OR conservative fills >=20, plus >=20 30s and >=20 60s markouts",
            }
            result.append(row)
            if model == PRIMARY_MODEL:
                primary[asset] = row
    return result, primary


def _lifetime_fill_rows(quotes: list[dict[str, Any]], crossings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    crossing_by_quote: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in crossings:
        if row.get("quote_id"):
            crossing_by_quote[str(row["quote_id"])].append(row)
    groups: dict[tuple[str, str, str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for quote in quotes:
        key = (quote["asset"], quote["model"], quote["side"], quote["lifetime_bucket"] if "lifetime_bucket" in quote else _lifetime_bucket(quote["lifetime_seconds"]))
        group = groups[key]
        group["quote_count"] += 1
        group["filled_quote_count"] += int(quote.get("filled") is True)
        group["trade_encounter_count"] += len(crossing_by_quote.get(quote["quote_id"], []))
        group["strict_crossing_count"] += sum(bool(row.get("strict_trade_through")) for row in crossing_by_quote.get(quote["quote_id"], []))
        group["touch_count"] += sum(bool(row.get("touch_or_better")) for row in crossing_by_quote.get(quote["quote_id"], []))
    rows: list[dict[str, Any]] = []
    for (asset, model, side, bucket), group in sorted(groups.items()):
        rows.append({"asset": asset, "model": model, "side": side, "lifetime_bucket": bucket, **group, "fill_rate_pct": _pct(group["filled_quote_count"], group["quote_count"])})
    return rows


def _effective_reference_path(
    config: RuntimeConfig,
    metadata: dict[str, Any],
    state: dict[str, Any],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    configured = state.get("reference_venues") or metadata.get("reference_venues") or config.reference_venues
    priority = state.get("reference_priority") or metadata.get("reference_priority") or config.reference_priority
    venues = tuple(str(venue).lower() for venue in configured)
    ordered = tuple(str(venue).lower() for venue in priority if str(venue).lower() in venues)
    return venues, ordered


def _reference_health_rows(
    config: RuntimeConfig,
    assets: list[str],
    health: list[dict[str, Any]],
    runtime: dict[str, Any],
    reference_venues: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    venues = reference_venues or config.reference_venues
    allowed_venues = set(venues)
    for row in health:
        asset, venue = str(row.get("asset") or "").upper(), str(row.get("venue") or "").lower()
        if asset in assets and venue in allowed_venues:
            groups[(asset, venue)].append(row)
    rows: list[dict[str, Any]] = []
    latest_health = runtime.get("source_health") or {}
    for asset in assets:
        for venue in venues:
            items = groups.get((asset, venue), [])
            latest = latest_health.get(asset, {}).get(venue, {}) if isinstance(latest_health.get(asset, {}), dict) else {}
            counts = Counter(str(item.get("health", item.get("status", "UNOBSERVED"))) for item in items)
            rows.append(
                {
                    "asset": asset,
                    "venue": venue,
                    "persisted_observations": len(items),
                    "healthy_observations": counts.get("HEALTHY", 0),
                    "degraded_observations": counts.get("DEGRADED", 0),
                    "stale_observations": counts.get("STALE", 0),
                    "unobserved_observations": counts.get("UNOBSERVED", 0),
                    "latest_health": latest.get("health") or (items[-1].get("health") if items else "UNOBSERVED"),
                    "latest_bbo_age_seconds": latest.get("bbo_age"),
                    "reconnect_count_latest": latest.get("reconnect_count"),
                    "parse_failures_latest": latest.get("parse_failures"),
                    "reference_path_status": "CONFIGURED_ACTIVE_PATH" if venue in config.reference_venues else "NOT_CONFIGURED",
                }
            )
    return rows


def _storage_rows(config: RuntimeConfig, telemetry_path: Path, data: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
    paths = [telemetry_path, Path(str(telemetry_path) + "-wal"), Path(str(telemetry_path) + "-shm")]
    sizes = {path.name: path.stat().st_size for path in paths if path.exists()}
    current_size = sum(sizes.values())
    try:
        usage = shutil.disk_usage(telemetry_path.parent)
        free_gb, total_gb, used_gb = usage.free / 1024**3, usage.total / 1024**3, usage.used / 1024**3
    except OSError:
        free_gb = total_gb = used_gb = None
    compressed = 0
    roots = {Path(config.log_dir).parent, Path(config.report_dir).parent}
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        try:
            for candidate in root.glob("**/*.zst"):
                if candidate in seen:
                    continue
                seen.add(candidate)
                try:
                    compressed += candidate.stat().st_size
                except OSError:
                    continue
        except OSError:
            continue
    governor = data.get("storage_governor") or {}
    policy = governor.get("policy") or {}
    aggregate_bytes: int | None = None
    if telemetry_path.exists():
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(f"file:{telemetry_path}?mode=ro", uri=True, timeout=0.25)
            aggregate_bytes = int(connection.execute("SELECT COALESCE(SUM(pgsize), 0) FROM dbstat WHERE name='minute_aggregates'").fetchone()[0])
        except (OSError, sqlite3.Error, TypeError, ValueError):
            aggregate_bytes = None
        finally:
            if connection is not None:
                connection.close()
    return [
        {
            "run_id": run_id,
            "free_disk_gb": _round(free_gb),
            "total_disk_gb": _round(total_gb),
            "used_disk_gb": _round(used_gb),
            "current_run_size_bytes": current_size,
            "telemetry_db_bytes": sizes.get(telemetry_path.name, 0),
            "raw_buffer_bytes_in_db_wal_shm": current_size,
            "aggregate_table_bytes": aggregate_bytes,
            "compressed_archive_bytes_visible": compressed,
            "pruned_rows_total": governor.get("total_pruned_rows", 0),
            "storage_state": governor.get("level", "UNKNOWN"),
            "raw_retention_seconds": policy.get("raw_retention_seconds", config.raw_retention_seconds),
            "feature_persist_interval_seconds": policy.get("feature_persist_interval_seconds", config.feature_persist_interval_seconds),
            "aggregate_interval_seconds": policy.get("aggregate_interval_seconds", config.aggregate_interval_seconds),
            "storage_governor_enabled": governor.get("governor_enabled", True),
            "db_file_components": sizes,
        }
    ]


def _run_metadata(config: RuntimeConfig, metadata: dict[str, Any], state: dict[str, Any], data: dict[str, Any], telemetry_path: Path, out_dir: Path) -> dict[str, Any]:
    run_id = str(metadata.get("run_id") or out_dir.name)
    status = str(state.get("status") or data.get("runtime", {}).get("status") or "UNKNOWN")
    reference_venues, reference_priority = _effective_reference_path(config, metadata, state)
    bitget_configured = "bitget" in reference_venues
    runtime = data.get("runtime") or {}
    result = dict(metadata)
    result.update(
        {
            "run_id": run_id,
            "diagnostic_generated_at_utc": _iso(time.time()),
            "diagnostic_status": "RUNNING_SNAPSHOT" if status == "RUNNING" else "FINAL_OR_TERMINAL_SNAPSHOT",
            "run_status": status,
            "telemetry_path": str(telemetry_path),
            "diagnostic_dir": str(out_dir),
            "assets": metadata.get("assets") or state.get("active_assets") or [asset.symbol for asset in config.enabled_assets],
            "reference_venues": list(reference_venues),
            "reference_priority": list(reference_priority) + ["pause"],
            "bitget_enabled": config.bitget_enabled,
            "runtime_bitget_enabled": bitget_configured,
            "bitget_runtime_status": "ACTIVE_UNTIL_RESTART" if bitget_configured else "INACTIVE",
            "restart_required_to_remove_bitget": bitget_configured,
            "restart_reason": (
                "The current PID loaded Bitget from reference_venues at startup; changing the profile cannot cancel its websocket/reconnect task. A restart is required, but was not performed so the fixed current run remains valid."
                if bitget_configured else "Bitget is absent from reference_venues and is not scheduled."
            ),
            "pid": state.get("pid") or metadata.get("pid") or runtime.get("pid"),
            "runner_alive": _process_exists(state.get("pid") or metadata.get("pid") or runtime.get("pid")),
            "mainnet_armed": state.get("mainnet_armed", metadata.get("mainnet_armed", False)),
            "dry_run": state.get("dry_run", metadata.get("dry_run", True)),
            "real_orders": state.get("real_orders", metadata.get("real_orders", 0)),
            "real_positions": state.get("real_positions", metadata.get("real_positions", 0)),
            "continuous_telemetry_writes": True,
            "measurement_only": True,
        }
    )
    return result


def _process_exists(pid: Any) -> bool:
    try:
        os.kill(int(pid), 0)
    except (OSError, TypeError, ValueError):
        return False
    return True


def _recommendations(primary: dict[str, dict[str, Any]], replacements: list[dict[str, Any]], unmatched_fills: int) -> str:
    lines = [
        "# Recommended next tuning (hypothetical only)",
        "",
        "No strategy or core market-making parameter was changed by this diagnostic.",
        "The following are hypotheses to test only after the evidence gate is met; they are not live recommendations or automatic edits.",
        "",
    ]
    for asset, row in sorted(primary.items()):
        root = row.get("root_cause")
        lines.append(f"## {asset}: `{root}` / `{row.get('sample_status')}`")
        if row.get("sample_status") != "SUFFICIENT":
            lines.append("- Collect more causal Derive trade and 30/60-second markout observations. Do not tune or stop the collector from this snapshot.")
        elif root == "QUOTE_LIFETIME_TOO_SHORT":
            lines.append("- Hypothesis: test a longer quote maximum age or a lower refresh frequency while holding all economic buffers fixed; compare lifetime survival and conservative crossings.")
        elif root == "MICRO_CHURN_HIGH":
            lines.append("- Hypothesis: test a larger refresh tolerance against the observed micro-move distribution; compare mutations/min, quote uptime, and strict crossings.")
        elif root == "QUOTE_TOO_PASSIVE":
            lines.append("- Hypothesis: test a less-passive placement only in a separate shadow control; compare strict trade-throughs and markouts, with costs unchanged.")
        elif root == "FILL_LOGIC_MISMATCH":
            lines.append("- Do not tune price placement yet. First reconcile trade receipt/exchange timestamps, active quote state, and fill association.")
        elif root in {"REFERENCE_DRIVEN_OVER_REPLACEMENT", "DERIVE_BBO_DRIVEN_OVER_REPLACEMENT"}:
            lines.append("- Hypothesis: test replacement hysteresis in an isolated control only after confirming the driver with sufficient observed FV/BBO pairs.")
        else:
            lines.append("- No tuning hypothesis is justified by this snapshot.")
    if not primary:
        lines.append("- MORE_DATA_REQUIRED: no primary-control diagnostic row is available.")
    if unmatched_fills:
        lines.append(f"- Fill association remains incomplete: {unmatched_fills} fill row(s) had no matching open quote and must not be treated as realized evidence.")
    lines.extend(
        [
            "",
            "Safety: `dry_run=true`, `mainnet_armed=false`, `real_orders=0`, `real_positions=0` remain required. Bitget removal is a successor-run restart boundary when the current profile includes Bitget.",
        ]
    )
    return "\n".join(lines) + "\n"


def _asset_root_cause_rows(
    summary_assets: list[dict[str, Any]],
    missed: list[dict[str, Any]],
    cancel_before: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Write a compact, per-asset decision artifact with explicit denominators."""

    rows: list[dict[str, Any]] = []
    for asset in summary_assets:
        asset_name = str(asset.get("asset") or "").upper()
        asset_missed = [row for row in missed if str(row.get("asset") or "").upper() == asset_name]
        asset_cancel = [row for row in cancel_before if str(row.get("asset") or "").upper() == asset_name]
        rows.append(
            {
                "asset": asset_name,
                "model": asset.get("model"),
                "activity_classification": asset.get("activity_classification"),
                "root_cause": asset.get("root_cause"),
                "sample_status": asset.get("sample_status"),
                "derive_trades": asset.get("derive_trades", 0),
                "conservative_fills": asset.get("conservative_fills", 0),
                "strict_crossings": asset.get("strict_crossings", 0),
                "touches_or_better": asset.get("touches_or_better", 0),
                "median_lifetime_ms": asset.get("median_lifetime_ms"),
                "p90_lifetime_ms": asset.get("p90_lifetime_ms"),
                "quote_mutations_per_min": asset.get("quote_mutations_per_min"),
                "missed_fill_rows": len(asset_missed),
                "potential_churn_missed_fill_rows": sum(
                    1
                    for row in asset_cancel
                    if int(row.get("potential_churn_missed_fill_count") or 0) > 0
                ),
                "potential_churn_missed_fill_max_count": max(
                    (int(row.get("potential_churn_missed_fill_count") or 0) for row in asset_cancel),
                    default=0,
                ),
                "evidence_rule": asset.get("evidence_rule"),
            }
        )
    return rows


def _summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Derive three-asset quote/fill diagnostic",
        "",
        f"- Diagnostic status: `{summary.get('diagnostic_status')}`",
        f"- Run: `{summary.get('run_id')}` / PID `{summary.get('pid')}`",
        f"- Reference path: `{' -> '.join(str(item).upper() for item in summary.get('reference_priority', []))}`",
        f"- Bitget profile enabled: `{summary.get('bitget_enabled')}`; current runtime: `{summary.get('bitget_runtime_status')}`; restart required: `{summary.get('restart_required_to_remove_bitget')}`",
        f"- Evidence policy: `{summary.get('observation_policy')}`",
        "",
        "## Per-asset headline",
        "",
        "| Asset | Trades/hour | Median spread bps | Median lifetime ms | P90 lifetime ms | Queue proxy ms | Mutations/min | Strict crossings | Touches | Potential churn-missed (5s) | Conservative fills | 30s/60s markout bps | Root cause | Evidence |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for row in summary.get("assets", []):
        lines.append(
            f"| {row.get('asset')} | {row.get('trades_per_hour')} | {row.get('median_spread_bps')} | {row.get('median_lifetime_ms')} | {row.get('p90_lifetime_ms')} | {row.get('queue_residency_proxy_median_ms')} | {row.get('quote_mutations_per_min')} | {row.get('strict_crossings')} | {row.get('touches_or_better')} | {row.get('potential_churn_missed_fill_count_5s')} | {row.get('conservative_fills')} | {row.get('markout_30s_median_bps')}/{row.get('markout_60s_median_bps')} | `{row.get('root_cause')}` | `{row.get('sample_status')}` |"
        )
    lines.extend(
        [
            "",
            "## Decision tree",
            "",
            "1. No Derive trades -> `LOW_MARKET_ACTIVITY` / `NO_DERIVE_TRADES`.",
            "2. Derive trades but no quote crossings -> inspect `QUOTE_TOO_PASSIVE`, quote lifetime, and quote uptime.",
            "3. Touches without strict trade-throughs -> `FILL_SIMULATOR_TOO_STRICT` is a model comparison, not a realized fill claim.",
            "4. Strict trade-throughs without an associated conservative fill -> `FILL_LOGIC_MISMATCH` and audit timestamps before tuning.",
            "5. Only after the evidence gate is sufficient may a root cause be used to design an isolated hypothetical threshold test.",
            "",
            "## Limitations",
            "",
            "- Shadow fills are hypothetical and do not establish realized Derive execution or PnL.",
            "- Queue residency is a same-price observation proxy, not exchange queue position.",
            "- Gaps beyond the causal observation window are not forward-filled.",
            "- A completed report is not a deployment approval.",
        ]
    )
    return "\n".join(lines) + "\n"


def export_diagnostics(
    config: RuntimeConfig,
    telemetry_path: Path,
    state_path: Path,
    out_dir: Path,
    metadata_path: Path | None = None,
) -> dict[str, Any]:
    """Export all quote/fill diagnostic artifacts for a run or live snapshot."""

    data = _read_telemetry(Path(telemetry_path))
    state = _read_json(Path(state_path))
    metadata = _read_json(Path(metadata_path)) if metadata_path else {}
    runtime = data.get("runtime") or {}
    status = str(state.get("status") or runtime.get("status") or "UNKNOWN")
    assets = [asset.symbol for asset in config.enabled_assets]
    if not assets:
        assets = [str(asset).upper() for asset in state.get("active_assets", [])]
    event_times = [
        _float(row.get("timestamp"))
        for table in (data.get("actions", []), data.get("fills", []), data.get("trades", []), data.get("decisions", []))
        for row in table
        if _float(row.get("timestamp")) is not None
    ]
    start = _float(state.get("started_at") or metadata.get("start_time_epoch") or runtime.get("started_at")) or (min(event_times) if event_times else time.time())
    ended = _float(state.get("ended_at") or runtime.get("ended_at"))
    analysis_end = ended if ended is not None else max(time.time(), max(event_times, default=time.time()))
    duration = max(0.001, analysis_end - start)
    diagnostic_metadata = _run_metadata(config, metadata, state or runtime, data, Path(telemetry_path), Path(out_dir))
    reference_venues, reference_priority = _effective_reference_path(config, metadata, state or runtime)
    tick_sizes = _tick_sizes(config, state or runtime, diagnostic_metadata, Path(out_dir))
    observations = _observations(data, assets)
    quotes, unmatched_fills = reconstruct_quote_lifecycles(data.get("actions", []), data.get("fills", []), analysis_end)
    max_gap = max(0.5, min(5.0, 2.0 * float(config.feature_persist_interval_seconds)))
    _apply_quote_observations(quotes, observations, tick_sizes, max_gap)
    quote_rows = _lifetime_rows(quotes)
    distributions = _lifetime_distribution(quotes)
    residency = _residency_rows(quotes)
    touch_distances = _distance_rows(quotes, "touch")
    fair_distances = _distance_rows(quotes, "fair")
    fair_distances.extend(_fair_bucket_rows(quotes))
    replacements = _replacement_rows(quotes, observations, tick_sizes, max_gap)
    replacement_reasons = _replacement_reason_rows(replacements)
    micro_churn = _micro_churn_rows(replacements)
    models = _all_models(config, data.get("actions", []), data.get("fills", []))
    crossings = _crossing_rows(data.get("trades", []), quotes, models)
    missed = _missed_fill_rows(data.get("trades", []), quotes, models)
    cancel_before = _cancel_before_rows(data.get("trades", []), quotes, models)
    mutation_rows = _mutation_rows(data.get("actions", []), quotes, assets, start, analysis_end)
    fill_logic = _fill_logic_rows(crossings, data.get("fills", []), unmatched_fills)
    activity_rows, primary = _activity_rows(config, assets, quotes, data.get("actions", []), data.get("trades", []), data.get("markouts", []), crossings, replacements, data, duration)
    lifetime_fill = _lifetime_fill_rows(quotes, crossings)
    reference_health = _reference_health_rows(config, assets, data.get("health", []), state or runtime, reference_venues)
    storage_health = _storage_rows(config, Path(telemetry_path), data, str(diagnostic_metadata.get("run_id")))

    for quote in quotes:
        quote["lifetime_bucket"] = _lifetime_bucket(float(quote["lifetime_seconds"]))
    summary_assets = [primary.get(asset) or next((row for row in activity_rows if row["asset"] == asset and row["model"] == PRIMARY_MODEL), {"asset": asset, "root_cause": "INSUFFICIENT_SAMPLE", "sample_status": "MORE_DATA_REQUIRED"}) for asset in assets]
    potential_churn_by_asset = {
        asset: max(
            (
                int(row.get("potential_churn_missed_fill_count") or 0)
                for row in cancel_before
                if str(row.get("asset") or "").upper() == asset
                and float(row.get("window_ms") or 0.0) == max(
                    (
                        float(candidate.get("window_ms") or 0.0)
                        for candidate in cancel_before
                        if str(candidate.get("asset") or "").upper() == asset
                    ),
                    default=0.0,
                )
            ),
            default=0,
        )
        for asset in assets
    }
    for row in summary_assets:
        row["potential_churn_missed_fill_count_5s"] = potential_churn_by_asset.get(str(row.get("asset") or "").upper(), 0)
    headline = {
        "run_id": diagnostic_metadata.get("run_id"),
        "pid": diagnostic_metadata.get("pid"),
        "diagnostic_status": diagnostic_metadata.get("diagnostic_status"),
        "run_status": status,
        "start_time_utc": _iso(start),
        "analysis_end_time_utc": _iso(analysis_end),
        "elapsed_seconds": _round(duration, 3),
        "assets": summary_assets,
        "reference_venues": list(reference_venues),
        "reference_priority": list(reference_priority) + ["pause"],
        "bitget_enabled": config.bitget_enabled,
        "runtime_bitget_enabled": diagnostic_metadata.get("runtime_bitget_enabled", "bitget" in reference_venues),
        "bitget_runtime_status": diagnostic_metadata.get("bitget_runtime_status"),
        "restart_required_to_remove_bitget": diagnostic_metadata.get("restart_required_to_remove_bitget"),
        "restart_reason": diagnostic_metadata.get("restart_reason"),
        "observation_policy": "causal receipt-time comparisons; no forward fill across gaps > max(0.5s, 2x feature interval); compressed rollups marked",
        "evidence_gate": "derive trades >=30 OR conservative fills >=20, plus >=20 30s and >=20 60s markouts",
        "unmatched_fill_rows": unmatched_fills,
        "derive_trade_rows": sum(str(row.get("source")) == "derive" for row in data.get("trades", [])),
        "action_rows": len(data.get("actions", [])),
        "quote_rows": len(quotes),
        "strict_crossing_rows": sum(bool(row.get("strict_trade_through")) for row in crossings),
        "fill_logic_mismatch_rows": sum(bool(row.get("fill_logic_mismatch")) for row in crossings),
        "report_files": {name: str(Path(out_dir) / name) for name in DIAGNOSTIC_FILES},
        "safety": {
            "mode": diagnostic_metadata.get("mode") or state.get("mode") or runtime.get("mode"),
            "dry_run": diagnostic_metadata.get("dry_run", True),
            "mainnet_armed": diagnostic_metadata.get("mainnet_armed", False),
            "real_orders": diagnostic_metadata.get("real_orders", 0),
            "real_positions": diagnostic_metadata.get("real_positions", 0),
            "private_api_used": False,
            "reference_execution": False,
        },
    }
    diagnostic_metadata["denominators"] = {
        "assets": len(assets),
        "quote_lifecycle_rows": len(quotes),
        "replacement_rows": len(replacements),
        "derive_trade_rows": headline["derive_trade_rows"],
        "crossing_rows": len(crossings),
        "missed_fill_rows": len(missed),
        "markout_rows": len(data.get("markouts", [])),
        "decision_rows": len(data.get("decisions", [])),
        "decision_rollup_rows": len(data.get("rollups", [])),
    }
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    _write_json(out / "run_metadata.json", diagnostic_metadata)
    _write_csv(out / "quote_lifetime.csv", quote_rows)
    _write_csv(out / "quote_lifetime_distribution.csv", distributions)
    _write_csv(out / "quote_residency.csv", residency)
    _write_csv(out / "quote_distance_to_touch.csv", touch_distances)
    _write_csv(out / "quote_distance_to_fair_value.csv", fair_distances)
    _write_csv(out / "replacement_reasons.csv", replacement_reasons)
    _write_csv(out / "replacement_fv_move.csv", replacements)
    _write_csv(out / "micro_churn.csv", micro_churn)
    _write_csv(out / "quote_mutation_rate.csv", mutation_rows)
    _write_csv(out / "derive_trade_activity.csv", activity_rows)
    _write_csv(out / "asset_root_cause.csv", _asset_root_cause_rows(summary_assets, missed, cancel_before))
    _write_csv(out / "trade_quote_crossings.csv", crossings)
    _write_csv(out / "missed_fill_analysis.csv", missed)
    _write_csv(out / "cancel_before_trade.csv", cancel_before)
    _write_csv(out / "lifetime_fill_relationship.csv", lifetime_fill)
    _write_csv(out / "fill_logic_audit.csv", fill_logic)
    _write_csv(out / "reference_health.csv", reference_health)
    _write_csv(out / "storage_health.csv", storage_health)
    (out / "recommended_next_tuning.md").write_text(_recommendations(primary, replacements, unmatched_fills), encoding="utf-8")
    headline["storage_health"] = storage_health
    headline["classification_by_asset"] = {row.get("asset"): row.get("activity_classification") for row in summary_assets}
    headline["root_cause_by_asset"] = {row.get("asset"): row.get("root_cause") for row in summary_assets}
    headline["potential_churn_missed_fill_count_by_asset"] = potential_churn_by_asset
    _write_json(out / "diagnostic_summary.json", headline)
    (out / "diagnostic_summary.md").write_text(_summary_markdown(headline), encoding="utf-8")
    return headline


__all__ = [
    "DIAGNOSTIC_FILES",
    "PRIMARY_MODEL",
    "export_diagnostics",
    "reconstruct_quote_lifecycles",
]
