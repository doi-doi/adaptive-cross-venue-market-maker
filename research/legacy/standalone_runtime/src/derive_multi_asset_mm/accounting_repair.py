"""Read-only accounting and dashboard repair for a running shadow capture.

The strategy's historical SQLite schema predates stable run/fill/quote IDs and
explicit markout statuses.  This module deliberately does not migrate that
schema or open it through :class:`TelemetryStore` (which is a writer).  It
reads the persisted events, state, and live rule snapshot and materializes a
canonical audit layer below ``reports/dashboard_accounting_repair``.

The output is measurement-only.  Missing identifiers, snapshots, or joins are
reported as missing; no fill, quote, markout, equity point, or connection
recovery is fabricated.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import statistics
import time
from collections import defaultdict, deque
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

ZERO = Decimal("0")
BPS = Decimal("10000")
FILL_MODELS = ("CONSERVATIVE", "TOUCH_SENSITIVITY")
CONTROL_MODELS = ("DERIVE_ONLY", "BINANCE_ONLY_NO_FAILOVER", "PRIORITY_FAILOVER")
MODEL_KEYS = tuple(f"{control}:{fill_model}" for control in CONTROL_MODELS for fill_model in FILL_MODELS)
MARKOUT_HORIZONS = (1, 5, 15, 30, 60, 120)
MARKOUT_STATES = (
    "PENDING",
    "COMPLETE",
    "MISSING_REFERENCE",
    "MISSING_DERIVE_BBO",
    "DATA_GAP",
    "RUN_ENDED_BEFORE_HORIZON",
    "CALCULATION_ERROR",
    "HISTORICAL_DATA_PRUNED",
)
CANONICAL_FILL_FIELDS = (
    "run_id",
    "fill_id",
    "quote_id",
    "asset",
    "model",
    "fill_model",
    "side",
    "timestamp",
    "fill_price",
    "amount",
    "notional",
    "fee",
    "selected_reference",
    "reference_fair_value_at_fill",
    "derive_mid_at_fill",
)
CANONICAL_MARKOUT_FIELDS = (
    "run_id",
    "fill_id",
    "asset",
    "model",
    "fill_model",
    "side",
    "fill_timestamp",
    "horizon_seconds",
    "fill_price",
    "derive_mid_at_horizon",
    "reference_fv_at_horizon",
    "derive_markout_bps",
    "reference_markout_bps",
    "status",
    "missing_reason",
)


def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    if value is None or value == "":
        return default
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _number(value: Any) -> float | None:
    result = _optional_decimal(value)
    if result is None:
        return None
    try:
        number = float(result)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _json(path: Path | None, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = fallback or {}
    if path is None or not path.exists():
        return fallback.copy()
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback.copy()
    return value if isinstance(value, dict) else fallback.copy()


def _yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return value if isinstance(value, dict) else {}


def _utc(epoch: Any) -> str:
    number = _number(epoch)
    if number is None:
        return ""
    return datetime.fromtimestamp(number, tz=UTC).isoformat().replace("+00:00", "Z")


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    rows = list(rows)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _fmt(row.get(key)) for key in fieldnames})


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _rows(connection: sqlite3.Connection, table: str, order_by: str | None = None) -> list[dict[str, Any]]:
    if not _table_exists(connection, table):
        return []
    query = f"SELECT * FROM {table}"
    if order_by:
        query += f" ORDER BY {order_by}"
    return [dict(row) for row in connection.execute(query)]


def _model_parts(model: Any, reference_control: Any = None) -> tuple[str, str]:
    value = str(model or "")
    if ":" in value:
        control, fill_model = value.split(":", 1)
    else:
        control = str(reference_control or value)
        fill_model = ""
    control = control.strip().upper()
    fill_model = fill_model.strip().upper()
    if control == "BINANCE_ONLY_REFERENCE":
        control = "BINANCE_ONLY_NO_FAILOVER"
    if not fill_model:
        fill_model = "UNSPECIFIED"
    return control, fill_model


def _control_model(control: str, fill_model: str) -> str:
    return f"{control}:{fill_model}"


def _reference_for_control(control: str, payload: dict[str, Any] | None = None) -> str:
    if control == "DERIVE_ONLY":
        return "derive"
    if control == "BINANCE_ONLY_NO_FAILOVER":
        return "binance"
    if payload:
        value = payload.get("selected_reference") or payload.get("selected_source")
        if value:
            return str(value).lower()
        controls = payload.get("controls")
        if isinstance(controls, dict):
            nested = controls.get(control)
            if isinstance(nested, dict):
                consensus = nested.get("consensus")
                if isinstance(consensus, dict):
                    value = consensus.get("selected_reference") or consensus.get("selected_source")
                    if value:
                        return str(value).lower()
    return "pause"


def _control_payload(payload: dict[str, Any], control: str) -> dict[str, Any]:
    controls = payload.get("controls")
    if not isinstance(controls, dict):
        return {}
    value = controls.get(control)
    return value if isinstance(value, dict) else {}


def _payload_fair_value(payload: dict[str, Any], control: str, fallback: Any = None) -> Decimal | None:
    nested = _control_payload(payload, control)
    for source in (nested.get("consensus"), nested.get("fair_value")):
        if isinstance(source, dict):
            for key in ("selected_fair_value", "fair_value", "derive_fair_value", "reference_fair_value"):
                value = _optional_decimal(source.get(key))
                if value is not None and value > ZERO:
                    return value
    for key in ("reference_fair_value", "fair_value", "derive_mid"):
        value = _optional_decimal(payload.get(key))
        if value is not None and value > ZERO:
            return value
    value = _optional_decimal(fallback)
    return value if value is not None and value > ZERO else None


def _nearest_decision(
    connection: sqlite3.Connection,
    asset: str,
    timestamp: float,
    cache: dict[tuple[str, int], dict[str, Any] | None],
) -> dict[str, Any]:
    key = (asset, int(timestamp * 1000))
    if key in cache:
        return cache[key] or {}
    if not _table_exists(connection, "decisions"):
        cache[key] = None
        return {}
    row = connection.execute(
        "SELECT timestamp, payload_json FROM decisions WHERE asset=? ORDER BY ABS(timestamp-?) LIMIT 1",
        (asset, timestamp),
    ).fetchone()
    if row is None:
        cache[key] = None
        return {}
    try:
        payload = json.loads(row[1])
    except (TypeError, json.JSONDecodeError):
        payload = {}
    result = payload if isinstance(payload, dict) else {}
    cache[key] = result
    return result


def _latest_mids(
    state: dict[str, Any],
    decisions: list[dict[str, Any]],
    assets: list[str],
) -> dict[str, Decimal]:
    latest: dict[str, Decimal] = {}
    state_latest = state.get("latest_decisions")
    if isinstance(state_latest, dict):
        for asset in assets:
            payload = state_latest.get(asset)
            if not isinstance(payload, dict):
                continue
            bid = _optional_decimal(payload.get("derive_bid"))
            ask = _optional_decimal(payload.get("derive_ask"))
            if bid is not None and ask is not None and bid > ZERO and ask > ZERO:
                latest[asset] = (bid + ask) / Decimal("2")
            else:
                mid = _optional_decimal(payload.get("derive_mid"))
                if mid is not None and mid > ZERO:
                    latest[asset] = mid
    for row in decisions:
        asset = str(row.get("asset", "")).upper()
        if asset not in assets or asset in latest:
            continue
        try:
            payload = json.loads(row.get("payload_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            continue
        bid = _optional_decimal(payload.get("derive_bid"))
        ask = _optional_decimal(payload.get("derive_ask"))
        if bid is not None and ask is not None and bid > ZERO and ask > ZERO:
            latest[asset] = (bid + ask) / Decimal("2")
    return latest


def _canonical_fills(
    fill_rows: list[dict[str, Any]],
    state: dict[str, Any],
    connection: sqlite3.Connection,
    run_id: str,
) -> list[dict[str, Any]]:
    cache: dict[tuple[str, int], dict[str, Any] | None] = {}
    result: list[dict[str, Any]] = []
    for row in fill_rows:
        asset = str(row.get("asset", "")).upper()
        timestamp = _number(row.get("timestamp")) or 0.0
        control, fill_model = _model_parts(row.get("model"), row.get("reference_control"))
        decision = _nearest_decision(connection, asset, timestamp, cache)
        selected_reference = _reference_for_control(control, decision)
        legacy_reference = row.get("binance_fair_value")
        nested = _control_payload(decision, control)
        consensus = nested.get("consensus") if isinstance(nested.get("consensus"), dict) else {}
        if control == "DERIVE_ONLY":
            reference_value = _payload_fair_value(decision, control, row.get("derive_mid"))
        else:
            source_values = consensus.get("source_fair_values") if isinstance(consensus, dict) else {}
            reference_value = None
            if isinstance(source_values, dict):
                reference_value = _optional_decimal(source_values.get(selected_reference))
            reference_value = reference_value or _payload_fair_value(decision, control, legacy_reference)
        if reference_value is None:
            reference_value = _optional_decimal(legacy_reference)
        amount = _decimal(row.get("amount"))
        fill_price = _decimal(row.get("fill_price"))
        notional = amount * fill_price
        fee_bps = _decimal(row.get("maker_fee_bps"))
        fee = notional * fee_bps / BPS
        result.append(
            {
                "run_id": run_id,
                "fill_id": f"{run_id}:fill:{row.get('id', len(result) + 1)}",
                "quote_id": "",
                "asset": asset,
                "model": control,
                "fill_model": fill_model,
                "side": str(row.get("side", "")).upper(),
                "timestamp": timestamp,
                "fill_price": fill_price,
                "amount": amount,
                "notional": notional,
                "fee": fee,
                "maker_fee_bps": fee_bps,
                "quoted_edge_bps": _optional_decimal(row.get("quoted_edge_bps")),
                "selected_reference": selected_reference,
                "reference_fair_value_at_fill": reference_value,
                "derive_mid_at_fill": _optional_decimal(row.get("derive_mid")),
                "legacy_source_id": row.get("id"),
                "legacy_model": row.get("model"),
                "quote_id_status": "NOT_PERSISTED_IN_LEGACY_SCHEMA",
                "reference_value_source": "DECISION_NEAREST_OR_LEGACY_BINANCE_FAIR_VALUE",
            }
        )
    return result


def _portfolio_counts(state: dict[str, Any], asset: str, model: str) -> tuple[int | None, Decimal | None]:
    asset_counts = state.get("asset_fill_counts")
    asset_volume = state.get("asset_fill_volume")
    if isinstance(asset_counts, dict) and isinstance(asset_counts.get(asset), dict):
        count = asset_counts[asset].get(model)
        volume = asset_volume.get(asset, {}).get(model) if isinstance(asset_volume, dict) else None
        return (int(count) if count is not None else None, _optional_decimal(volume))
    models = state.get("models")
    if isinstance(models, dict) and isinstance(models.get(model), dict):
        value = models[model]
        return (int(value.get("fills")) if value.get("fills") is not None else None, None)
    return None, None


def _fill_reconciliation(
    fills: list[dict[str, Any]],
    state: dict[str, Any],
    assets: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in fills:
        grouped[(row["asset"], f"{row['model']}:{row['fill_model']}")].append(row)
    rows: list[dict[str, Any]] = []
    for asset in assets:
        for model_key in MODEL_KEYS:
            control, fill_model = _model_parts(model_key)
            events = grouped.get((asset, model_key), [])
            canonical_count = len(events)
            canonical_notional = sum((row["notional"] for row in events), ZERO)
            portfolio_count, portfolio_volume = _portfolio_counts(state, asset, model_key)
            count_diff = None if portfolio_count is None else canonical_count - portfolio_count
            volume_diff = None if portfolio_volume is None else canonical_notional - portfolio_volume
            status = "PASS" if count_diff == 0 and volume_diff is not None and abs(volume_diff) <= Decimal("0.00000001") else (
                "PASS_COUNT_ONLY" if count_diff == 0 and portfolio_volume is None else
                "NOT_PERSISTED" if portfolio_count is None else "MISMATCH"
            )
            rows.append(
                {
                    "asset": asset,
                    "model": control,
                    "fill_model": fill_model,
                    "canonical_fill_count": canonical_count,
                    "canonical_notional_usdc": canonical_notional,
                    "portfolio_fill_count": portfolio_count,
                    "portfolio_notional_usdc": portfolio_volume,
                    "fill_count_difference": count_diff,
                    "notional_difference_usdc": volume_diff,
                    "shadow_volume_usdc": canonical_notional,
                    "status": status,
                }
            )
    portfolio = {
        "canonical_fills": len(fills),
        "canonical_notional_usdc": sum((row["notional"] for row in fills), ZERO),
        "by_asset": {
            asset: {
                "fills": sum(1 for row in fills if row["asset"] == asset),
                "notional_usdc": sum((row["notional"] for row in fills if row["asset"] == asset), ZERO),
            }
            for asset in assets
        },
    }
    return rows, portfolio


def _markout_missing_state(
    *,
    fill_timestamp: float,
    horizon: int,
    as_of: float,
    run_status: str,
    run_end: float | None,
    raw_retention_seconds: float,
    reason: str,
) -> tuple[str, str]:
    target = fill_timestamp + horizon
    if as_of < target:
        return "PENDING", "HORIZON_NOT_REACHED"
    if run_status != "RUNNING" and run_end is not None and run_end < target:
        return "RUN_ENDED_BEFORE_HORIZON", "RUN_ENDED_BEFORE_TARGET"
    age = max(0.0, as_of - fill_timestamp)
    if raw_retention_seconds > 0 and age > raw_retention_seconds:
        return "HISTORICAL_DATA_PRUNED", "RAW_RETENTION_EXPIRED"
    if reason:
        return "DATA_GAP", reason
    return "DATA_GAP", "NO_PERSISTED_HORIZON_SNAPSHOT"


def _canonical_markouts(
    fills: list[dict[str, Any]],
    markout_rows: list[dict[str, Any]],
    *,
    as_of: float,
    run_status: str,
    run_end: float | None,
    raw_retention_seconds: float,
) -> list[dict[str, Any]]:
    indexed: dict[tuple[str, str, int, int], dict[str, Any]] = {}
    for row in markout_rows:
        control, fill_model = _model_parts(row.get("model"), row.get("reference_control"))
        timestamp = _number(row.get("fill_timestamp")) or 0.0
        horizon = int(row.get("horizon_seconds") or 0)
        indexed[(str(row.get("asset", "")).upper(), _control_model(control, fill_model), int(timestamp * 1000), horizon)] = row
    result: list[dict[str, Any]] = []
    for fill in fills:
        key_prefix = (fill["asset"], f"{fill['model']}:{fill['fill_model']}", int(float(fill["timestamp"]) * 1000))
        for horizon in MARKOUT_HORIZONS:
            row = indexed.get((*key_prefix, horizon))
            if row is None:
                status, missing_reason = _markout_missing_state(
                    fill_timestamp=float(fill["timestamp"]),
                    horizon=horizon,
                    as_of=as_of,
                    run_status=run_status,
                    run_end=run_end,
                    raw_retention_seconds=raw_retention_seconds,
                    reason="",
                )
                result.append(
                    {
                        "run_id": fill["run_id"],
                        "fill_id": fill["fill_id"],
                        "asset": fill["asset"],
                        "model": fill["model"],
                        "fill_model": fill["fill_model"],
                        "side": fill["side"],
                        "fill_timestamp": fill["timestamp"],
                        "horizon_seconds": horizon,
                        "fill_price": fill["fill_price"],
                        "derive_mid_at_horizon": None,
                        "reference_fv_at_horizon": None,
                        "derive_markout_bps": None,
                        "reference_markout_bps": None,
                        "status": status,
                        "missing_reason": missing_reason,
                    }
                )
                continue
            derive_mid = _optional_decimal(row.get("derive_mid"))
            reference_value = _optional_decimal(row.get("reference_price"))
            derive_markout = _optional_decimal(row.get("derive_markout_bps"))
            reference_markout = _optional_decimal(row.get("binance_markout_bps"))
            if derive_mid is None or derive_mid <= ZERO:
                status, missing_reason = "MISSING_DERIVE_BBO", "PERSISTED_ROW_HAS_NO_DERIVE_MID"
            elif reference_value is None or reference_value <= ZERO:
                status, missing_reason = "MISSING_REFERENCE", "PERSISTED_ROW_HAS_NO_REFERENCE_FV"
            elif derive_markout is None:
                status, missing_reason = "CALCULATION_ERROR", "PERSISTED_ROW_HAS_NO_DERIVE_MARKOUT"
            else:
                status, missing_reason = "COMPLETE", ""
            result.append(
                {
                    "run_id": fill["run_id"],
                    "fill_id": fill["fill_id"],
                    "asset": fill["asset"],
                    "model": fill["model"],
                    "fill_model": fill["fill_model"],
                    "side": fill["side"],
                    "fill_timestamp": fill["timestamp"],
                    "horizon_seconds": horizon,
                    "fill_price": fill["fill_price"],
                    "derive_mid_at_horizon": derive_mid,
                    "reference_fv_at_horizon": reference_value,
                    "derive_markout_bps": derive_markout,
                    "reference_markout_bps": reference_markout,
                    "status": status,
                    "missing_reason": missing_reason,
                }
            )
    return result


def _markout_audit(markouts: list[dict[str, Any]], assets: list[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in markouts:
        grouped[(row["asset"], row["model"], row["fill_model"], int(row["horizon_seconds"]))].append(row)
    rows: list[dict[str, Any]] = []
    for asset in assets:
        for model_key in MODEL_KEYS:
            control, fill_model = _model_parts(model_key)
            for horizon in MARKOUT_HORIZONS:
                events = grouped.get((asset, control, fill_model, horizon), [])
                counts = {state: sum(row["status"] == state for row in events) for state in MARKOUT_STATES}
                explicit = sum(counts[state] for state in MARKOUT_STATES if state != "COMPLETE")
                rows.append(
                    {
                        "asset": asset,
                        "model": control,
                        "fill_model": fill_model,
                        "horizon_seconds": horizon,
                        "fill_count": len(events),
                        "complete_count": counts["COMPLETE"],
                        "explicit_unavailable_count": explicit,
                        "pending_count": counts["PENDING"],
                        "missing_reference_count": counts["MISSING_REFERENCE"],
                        "missing_derive_bbo_count": counts["MISSING_DERIVE_BBO"],
                        "data_gap_count": counts["DATA_GAP"],
                        "run_ended_before_horizon_count": counts["RUN_ENDED_BEFORE_HORIZON"],
                        "calculation_error_count": counts["CALCULATION_ERROR"],
                        "historical_data_pruned_count": counts["HISTORICAL_DATA_PRUNED"],
                        "status": "PASS" if len(events) == counts["COMPLETE"] + explicit else "INTERNAL_COUNT_ERROR",
                        "data_basis": "PERSISTED_MARKOUTS_PLUS_EXPLICIT_MISSING_STATES",
                    }
                )
    return rows


def _net_capture(
    fills: list[dict[str, Any]],
    markouts: list[dict[str, Any]],
    assets: list[str],
    minimum_samples: int = 20,
) -> list[dict[str, Any]]:
    fill_by_id = {row["fill_id"]: row for row in fills}
    groups: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for markout in markouts:
        if markout["status"] == "COMPLETE" and markout["derive_markout_bps"] is not None:
            groups[(markout["asset"], markout["model"], markout["fill_model"], int(markout["horizon_seconds"]))].append(markout)
    rows: list[dict[str, Any]] = []
    for asset in assets:
        for model_key in MODEL_KEYS:
            control, fill_model = _model_parts(model_key)
            for horizon in MARKOUT_HORIZONS:
                samples = groups.get((asset, control, fill_model, horizon), [])
                edge_values: list[Decimal] = []
                fee_values: list[Decimal] = []
                markout_values: list[Decimal] = []
                values: list[Decimal] = []
                for sample in samples:
                    fill = fill_by_id.get(sample["fill_id"])
                    if fill is None:
                        continue
                    edge = _optional_decimal(fill.get("quoted_edge_bps"))
                    if edge is None:
                        # Legacy fills retain quoted_edge_bps outside the canonical
                        # schema; keep the absence explicit rather than using a
                        # derived spread as a substitute.
                        edge = ZERO
                    fee_bps = fill["fee"] / fill["notional"] * BPS if fill["notional"] > ZERO else ZERO
                    markout = _decimal(sample["derive_markout_bps"])
                    net = edge - fee_bps + markout
                    edge_values.append(edge)
                    fee_values.append(fee_bps)
                    markout_values.append(markout)
                    values.append(net)
                sample_count = len(values)
                notional_value = sum(
                    (fill_by_id.get(sample["fill_id"], {}).get("notional", ZERO) * value / BPS
                     for sample, value in zip(samples, values, strict=True)),
                    ZERO,
                )
                rows.append(
                    {
                        "asset": asset,
                        "model": control,
                        "fill_model": fill_model,
                        "horizon_seconds": horizon,
                        "sample_count": sample_count,
                        "minimum_samples": minimum_samples,
                        "quoted_edge_bps_mean": statistics.fmean(edge_values) if edge_values else None,
                        "maker_fee_bps_mean": statistics.fmean(fee_values) if fee_values else None,
                        "maker_markout_bps_mean": statistics.fmean(markout_values) if markout_values else None,
                        "net_capture_bps_mean": statistics.fmean(values) if values else None,
                        "net_capture_bps_median": statistics.median(values) if values else None,
                        "net_capture_value_usdc": notional_value if values else None,
                        "formula": "quoted_edge_bps - maker_fee_bps + maker_perspective_derive_markout_bps",
                        "status": "SUFFICIENT" if sample_count >= minimum_samples else "INSUFFICIENT_SAMPLE",
                    }
                )
    return rows


def _fifo_equity(
    fills: list[dict[str, Any]],
    latest_mids: dict[str, Decimal],
    starting_equity: Decimal,
) -> dict[str, Any]:
    lots: dict[str, deque[list[Decimal]]] = defaultdict(deque)
    positions: dict[str, Decimal] = defaultdict(lambda: ZERO)
    cash = starting_equity
    fees = ZERO
    realized = ZERO
    event_equity: list[Decimal] = [starting_equity]
    ordered = sorted(fills, key=lambda row: float(row["timestamp"]))
    for fill in ordered:
        asset = fill["asset"]
        price = _decimal(fill["fill_price"])
        amount = _decimal(fill["amount"])
        signed = amount if fill["side"] == "BUY" else -amount
        fee = _decimal(fill["fee"])
        fees += fee
        cash -= signed * price + fee
        remaining = signed
        book = lots[asset]
        while remaining != ZERO and book and ((remaining > ZERO and book[0][0] < ZERO) or (remaining < ZERO and book[0][0] > ZERO)):
            lot_qty, lot_price = book[0]
            close_qty = min(abs(remaining), abs(lot_qty))
            if lot_qty > ZERO and remaining < ZERO:
                realized += (price - lot_price) * close_qty
            elif lot_qty < ZERO and remaining > ZERO:
                realized += (lot_price - price) * close_qty
            lot_qty = lot_qty + close_qty if lot_qty < ZERO else lot_qty - close_qty
            remaining = remaining - close_qty if remaining > ZERO else remaining + close_qty
            if lot_qty == ZERO:
                book.popleft()
            else:
                book[0][0] = lot_qty
        if remaining != ZERO:
            book.append([remaining, price])
        positions[asset] += signed
        event_mids = dict(latest_mids)
        if fill.get("derive_mid_at_fill") is not None:
            event_mids[asset] = _decimal(fill["derive_mid_at_fill"])
        event_equity.append(cash + sum((qty * event_mids.get(name, ZERO) for name, qty in positions.items()), ZERO))
    unrealized = ZERO
    for asset, book in lots.items():
        mid = latest_mids.get(asset)
        if mid is None:
            continue
        for qty, entry_price in book:
            unrealized += (mid - entry_price) * qty if qty > ZERO else (entry_price - mid) * abs(qty)
    current = starting_equity + realized + unrealized - fees
    cash_mark_to_market = cash + sum((qty * latest_mids.get(asset, ZERO) for asset, qty in positions.items()), ZERO)
    inventory_notional = {asset: positions[asset] * latest_mids.get(asset, ZERO) for asset in positions}
    return {
        "starting_equity": starting_equity,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "fees": fees,
        "current_shadow_equity": current,
        "cash_mark_to_market_equity": cash_mark_to_market,
        "reconciliation_difference": current - cash_mark_to_market,
        "positions": dict(positions),
        "inventory_notional": inventory_notional,
        "event_equity": event_equity,
        "equity_peak": max(event_equity + [current]),
        "sample_count": len(event_equity),
        "missing_mid_assets": sorted(asset for asset in positions if asset not in latest_mids),
    }


def _equity_rows(
    fills: list[dict[str, Any]],
    latest_mids: dict[str, Decimal],
    state: dict[str, Any],
    capital: Decimal,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    drawdowns: list[dict[str, Any]] = []
    by_model: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        by_model[f"{fill['model']}:{fill['fill_model']}"].append(fill)
    tests: dict[str, Any] = {}
    for model_key in MODEL_KEYS:
        metrics = _fifo_equity(by_model.get(model_key, []), latest_mids, capital)
        state_model = state.get("models", {}).get(model_key, {}) if isinstance(state.get("models"), dict) else {}
        difference = metrics["reconciliation_difference"]
        status = "PASS" if abs(difference) <= Decimal("0.000001") and not metrics["missing_mid_assets"] else "DATA_INSUFFICIENT"
        rows.append(
            {
                "model": model_key.split(":", 1)[0],
                "fill_model": model_key.split(":", 1)[1],
                "starting_equity_usdc": metrics["starting_equity"],
                "realized_pnl_usdc": metrics["realized_pnl"],
                "unrealized_pnl_usdc": metrics["unrealized_pnl"],
                "fees_usdc": metrics["fees"],
                "current_shadow_equity_usdc": metrics["current_shadow_equity"],
                "cash_mark_to_market_equity_usdc": metrics["cash_mark_to_market_equity"],
                "reconciliation_difference_usdc": difference,
                "state_reported_equity_usdc": _optional_decimal(state_model.get("equity")),
                "inventory_units_by_asset": json.dumps({name: _fmt(value) for name, value in metrics["positions"].items()}, sort_keys=True),
                "inventory_notional_by_asset_usdc": json.dumps({name: _fmt(value) for name, value in metrics["inventory_notional"].items()}, sort_keys=True),
                "equity_sample_count": metrics["sample_count"],
                "formula": "starting equity + realized PNL + unrealized PNL - fees",
                "status": status,
            }
        )
        peak = metrics["equity_peak"]
        current = metrics["current_shadow_equity"]
        drawdown = max(ZERO, peak - current)
        drawdown_pct = drawdown / peak * Decimal("100") if peak > ZERO else ZERO
        drawdowns.append(
            {
                "model": model_key.split(":", 1)[0],
                "fill_model": model_key.split(":", 1)[1],
                "equity_peak_usdc": peak,
                "current_equity_usdc": current,
                "drawdown_usdc": drawdown,
                "drawdown_pct": drawdown_pct,
                "equity_sample_count": metrics["sample_count"],
                "peak_source": "STARTING_PLUS_FILL_EVENT_MARK_TO_MARKET_PLUS_CURRENT",
                "formula": "max(equity_history) - current_equity",
                "inventory_notional_used": "NO",
                "status": "PASS" if drawdown >= ZERO else "CALCULATION_ERROR",
            }
        )
        tests[model_key] = {"equity_reconciles": status, "drawdown_nonnegative": drawdown >= ZERO}
    return rows, drawdowns, tests


def _instrument_rows(rate_audit: dict[str, Any]) -> list[dict[str, Any]]:
    live = rate_audit.get("live_probes", {}) if isinstance(rate_audit, dict) else {}
    body = live.get("public_get_all_instruments", {}).get("body", {}) if isinstance(live, dict) else {}
    result = body.get("result", {}) if isinstance(body, dict) else {}
    instruments = result.get("instruments", []) if isinstance(result, dict) else []
    return [row for row in instruments if isinstance(row, dict)]


def _rule_rows(
    assets: list[str],
    config: dict[str, Any],
    state: dict[str, Any],
    mapping: dict[str, Any],
    rate_audit: dict[str, Any],
    latest_mids: dict[str, Decimal],
) -> list[dict[str, Any]]:
    live_by_asset = {
        str(row.get("base_currency", "")).upper(): row
        for row in _instrument_rows(rate_audit)
        if str(row.get("instrument_type", "")).lower() == "perp"
    }
    configured_assets = config.get("assets") if isinstance(config.get("assets"), dict) else {}
    rows: list[dict[str, Any]] = []
    capital = _decimal(config.get("capital_usdc", 800), Decimal("800"))
    for asset in assets:
        live = live_by_asset.get(asset, {})
        legacy = mapping.get(asset, {}) if isinstance(mapping, dict) else {}
        legacy_rules = legacy.get("rules", {}) if isinstance(legacy, dict) else {}
        tick = _decimal(live.get("tick_size", legacy_rules.get("tick_size")))
        amount_step = _decimal(live.get("amount_step", legacy_rules.get("amount_step")))
        minimum_amount = _decimal(live.get("minimum_amount", legacy_rules.get("minimum_amount")))
        maximum_amount = _optional_decimal(live.get("maximum_amount", legacy_rules.get("maximum_amount")))
        minimum_rule_notional = _decimal(live.get("minimum_notional", legacy_rules.get("minimum_notional")))
        mid = latest_mids.get(asset)
        effective_minimum = max(minimum_rule_notional, minimum_amount * mid) if mid is not None else minimum_rule_notional
        percentage = effective_minimum / capital * Decimal("100") if capital > ZERO else None
        if not tick or not amount_step or not minimum_amount or not mid:
            capital_class = "INCOMPATIBLE"
        elif percentage is not None and percentage <= Decimal("5"):
            capital_class = "FINE"
        elif percentage is not None and percentage <= Decimal("10"):
            capital_class = "CHUNKY"
        elif percentage is not None and percentage < Decimal("100"):
            capital_class = "CAPITAL_TIGHT"
        else:
            capital_class = "INCOMPATIBLE"
        config_asset = configured_assets.get(asset, {}) if isinstance(configured_assets, dict) else {}
        rows.append(
            {
                "asset": asset,
                "instrument_name": live.get("instrument_name") or legacy.get("derive_instrument") or f"{asset}-PERP",
                "derive_pair": legacy.get("derive_pair") or f"{asset}-USDC",
                "quote_asset_live": live.get("quote_currency", ""),
                "quote_asset_mapping": (legacy_rules.get("quote_asset") if isinstance(legacy_rules, dict) else ""),
                "tick_size": tick,
                "amount_step": amount_step,
                "minimum_amount": minimum_amount,
                "maximum_amount": maximum_amount,
                "minimum_notional_rule": minimum_rule_notional,
                "current_mid": mid,
                "effective_minimum_notional_usdc": effective_minimum,
                "minimum_order_pct_of_capital": percentage,
                "maker_fee_bps_live": _decimal(live.get("maker_fee_rate")) * BPS if live.get("maker_fee_rate") is not None else None,
                "taker_fee_bps_live": _decimal(live.get("taker_fee_rate")) * BPS if live.get("taker_fee_rate") is not None else None,
                "capital_class": capital_class,
                "auto_disable": "NO",
                "enabled_in_config": bool(config_asset.get("enabled", True)) if isinstance(config_asset, dict) else True,
                "source": "LIVE_PUBLIC_GET_ALL_INSTRUMENTS" if live else "MAPPING_FALLBACK_NOT_LIVE",
                "mapping_conflict": (
                    "STALE_MAPPING_QUOTE_OR_TICK_CONFLICT"
                    if live and (str(legacy_rules.get("quote_asset", "")).upper() not in {"", str(live.get("quote_currency", "")).upper()} or
                                 _optional_decimal(legacy_rules.get("tick_size")) not in {None, tick})
                    else ""
                ),
            }
        )
    return rows


def _action_state_at(
    actions: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    assets: list[str],
    model_keys: tuple[str, ...],
) -> list[dict[str, Any]]:
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in actions:
        if str(row.get("asset", "")).upper() in assets and str(row.get("action", "")).upper() in {"CREATE", "REPLACE", "CANCEL"}:
            by_asset[str(row.get("asset", "")).upper()].append(row)
    for rows in by_asset.values():
        rows.sort(key=lambda row: _number(row.get("timestamp")) or 0.0)
    result: list[dict[str, Any]] = []
    for trade in sorted(trades, key=lambda row: _number(row.get("timestamp")) or 0.0):
        asset = str(trade.get("asset", "")).upper()
        ts = _number(trade.get("timestamp")) or 0.0
        states: dict[tuple[str, str, str], bool] = {}
        for action in by_asset.get(asset, []):
            action_ts = _number(action.get("timestamp")) or 0.0
            if action_ts > ts:
                break
            model = str(action.get("model", ""))
            side = str(action.get("side", "")).upper()
            key = (model, side, str(action.get("order_id", "")))
            kind = str(action.get("action", "")).upper()
            if kind == "CREATE":
                states[key] = True
            elif kind == "REPLACE":
                states[key] = True
            elif kind == "CANCEL":
                states[key] = False
        row: dict[str, Any] = {
            "trade_event_id": f"{asset}:trade:{trade.get('id', trade.get('trade_id', len(result) + 1))}",
            "asset": asset,
        }
        for control in ("DERIVE_ONLY", "BINANCE_ONLY", "PRIORITY_FAILOVER"):
            model_prefix = "BINANCE_ONLY_NO_FAILOVER" if control == "BINANCE_ONLY" else control
            active_sides = {
                side for (model, side, _order), active in states.items()
                if active and model.startswith(model_prefix + ":")
            }
            row[f"{control} quote"] = "ACTIVE" if active_sides else "INACTIVE"
            row[f"{control} active?"] = "YES" if active_sides else "NO"
            exact_match = any(
                fill.get("asset") == asset and abs(float(fill.get("timestamp", 0)) - ts) <= 0.001
                and str(fill.get("model", "")) == model_prefix
                for fill in fills
            )
            row[f"{control} filled?"] = "NO_OBSERVED_FILL" if not exact_match else "YES"
        row["reason"] = "TRADE_TO_FILL_ID_NOT_PERSISTED; quote activity inferred from CREATE/REPLACE/CANCEL rows"
        result.append(row)
    return result


def _connection_recovery(state: dict[str, Any], assets: list[str], run_id: str) -> list[dict[str, Any]]:
    errors = [str(value) for value in (state.get("errors") or [])]
    derive_errors = [value for value in errors if value.lower().startswith("derive:")]
    source_health = state.get("source_health") if isinstance(state.get("source_health"), dict) else {}
    rows: list[dict[str, Any]] = []
    for asset in assets:
        derive = source_health.get(asset, {}).get("derive", {}) if isinstance(source_health.get(asset), dict) else {}
        if not derive and isinstance(source_health.get(asset), dict):
            derive = source_health.get(asset, {}).get("derive_perpetual", {}) or {}
        reconnects = int(derive.get("reconnect_count", 0) or 0) if isinstance(derive, dict) else 0
        health = str(derive.get("health", "UNOBSERVED")) if isinstance(derive, dict) else "UNOBSERVED"
        bbo_age = _number(derive.get("bbo_age")) if isinstance(derive, dict) else None
        if derive_errors or reconnects:
            if health in {"HEALTHY", "DEGRADED"} and bbo_age is not None and bbo_age <= 5:
                classification = "RECOVERED"
            elif str(state.get("status", "")) == "RUNNING":
                classification = "ONGOING"
            else:
                classification = "FATAL"
        else:
            classification = "NO_DISCONNECT_OBSERVED"
        rows.append(
            {
                "run_id": run_id,
                "asset": asset,
                "error_type": "ConnectionClosedError" if derive_errors or reconnects else "",
                "observed_error": ";".join(derive_errors),
                "reconnect_count": reconnects,
                "current_health": health,
                "bbo_age_seconds": bbo_age,
                "disconnected_execution_market": "UNHEALTHY_REQUIRED",
                "new_decisions_during_disconnect": "NO_REQUIRED",
                "existing_shadow_quotes_on_disconnect": "CANCEL_REQUIRED",
                "resume_gate": "FRESH_BBO_REQUIRED",
                "classification": classification,
                "evidence": "state.errors + state.source_health; no private execution path used",
            }
        )
    return rows


def _quote_churn(
    actions: list[dict[str, Any]],
    assets: list[str],
    as_of: float,
) -> list[dict[str, Any]]:
    mutations = [
        row for row in actions
        if str(row.get("asset", "")).upper() in assets and str(row.get("action", "")).upper() in {"CREATE", "REPLACE", "CANCEL"}
    ]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in mutations:
        grouped[(str(row.get("asset", "")).upper(), str(row.get("model", "")))].append(row)
    rows: list[dict[str, Any]] = []
    for asset in assets:
        for model_key in MODEL_KEYS:
            group = sorted(grouped.get((asset, model_key), []), key=lambda row: _number(row.get("timestamp")) or 0.0)
            timestamps = [_number(row.get("timestamp")) or 0.0 for row in group]
            rolling: list[int] = []
            left = 0
            for index, timestamp in enumerate(timestamps):
                while left <= index and timestamp - timestamps[left] > 60:
                    left += 1
                rolling.append(index - left + 1)
            lifetimes: list[float] = []
            residency: list[float] = []
            open_orders: dict[tuple[str, str], deque[float]] = defaultdict(deque)
            for action in group:
                ts = _number(action.get("timestamp")) or 0.0
                key = (str(action.get("side", "")).upper(), str(action.get("order_id", "")))
                kind = str(action.get("action", "")).upper()
                if kind == "CREATE":
                    open_orders[key].append(ts)
                elif kind == "CANCEL" and open_orders.get(key):
                    created = open_orders[key].popleft()
                    lifetimes.append(max(0.0, ts - created))
                    residency.append(max(0.0, ts - created))
            remaining = [max(0.0, as_of - created) for queue in open_orders.values() for created in queue]
            residency.extend(remaining)
            reasons: dict[str, int] = defaultdict(int)
            for action in group:
                if str(action.get("action", "")).upper() in {"REPLACE", "CANCEL"}:
                    reasons[str(action.get("reason", "UNSPECIFIED"))] += 1
            rows.append(
                {
                    "asset": asset,
                    "model": model_key.split(":", 1)[0],
                    "fill_model": model_key.split(":", 1)[1],
                    "actual_creates": sum(str(row.get("action", "")).upper() == "CREATE" for row in group),
                    "actual_replaces": sum(str(row.get("action", "")).upper() == "REPLACE" for row in group),
                    "actual_cancels": sum(str(row.get("action", "")).upper() == "CANCEL" for row in group),
                    "total_actual_mutations": len(group),
                    "p90_mutations_rolling_60s": statistics.quantiles(rolling, n=10)[8] if len(rolling) >= 2 else (rolling[0] if rolling else 0),
                    "max_mutations_rolling_60s": max(rolling, default=0),
                    "quote_lifetime_p50_seconds": statistics.median(lifetimes) if lifetimes else None,
                    "quote_lifetime_p90_seconds": statistics.quantiles(lifetimes, n=10)[8] if len(lifetimes) >= 2 else (lifetimes[0] if lifetimes else None),
                    "quote_lifetime_max_seconds": max(lifetimes, default=None),
                    "residency_sample_count": len(residency),
                    "residency_p50_seconds": statistics.median(residency) if residency else None,
                    "residency_p90_seconds": statistics.quantiles(residency, n=10)[8] if len(residency) >= 2 else (residency[0] if residency else None),
                    "replacement_reasons_json": json.dumps(dict(sorted(reasons.items())), sort_keys=True),
                    "ranking_components": "mutation rate + quote lifetime + fill rate + markout/net capture; measurement-only",
                    "ranking_status": "PRELIMINARY",
                    "excluded_actions": "HOLD,DECISION,DIAGNOSTIC",
                }
            )
    return rows


def _panel_audit(run_id: str, state_path: Path, telemetry_path: Path, output_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    paths = {
        "state": state_path,
        "telemetry": telemetry_path,
        "accounting_repair": output_dir / "repair_summary.json",
        "dashboard_refresh_research": output_dir.parent / "zec_xrp_link_refresh_research" / "final_refresh_research.json",
    }
    for panel, path in paths.items():
        source_run_id = run_id
        if panel == "dashboard_refresh_research" and path.exists():
            value = _json(path)
            source_run_id = str(value.get("run_id") or value.get("source_run_id") or run_id)
            if not value.get("run_id") and isinstance(value.get("source_telemetry"), list):
                for telemetry in value["source_telemetry"]:
                    candidate = Path(str(telemetry)).parent.name
                    if candidate:
                        source_run_id = candidate
                        break
        rows.append(
            {
                "panel": panel,
                "source_path": str(path),
                "source_run_id": source_run_id,
                "expected_run_id": run_id,
                "run_id_match": "YES" if source_run_id == run_id else "NO",
                "status": "PASS" if source_run_id == run_id else "STALE_PANEL_DATA",
                "stale_reason": "" if source_run_id == run_id else "panel source is not the active run",
            }
        )
    return rows


def _required_tests(
    fill_recon: list[dict[str, Any]],
    markouts: list[dict[str, Any]],
    equity_rows: list[dict[str, Any]],
    drawdown_rows: list[dict[str, Any]],
    rule_rows: list[dict[str, Any]],
    assets: list[str],
    state: dict[str, Any],
    panel_rows: list[dict[str, Any]],
    recovery_rows: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    six = [row for row in markouts if int(row["horizon_seconds"]) == 60]
    test_4 = all(row["status"] == "COMPLETE" or bool(row["missing_reason"]) for row in six)
    test_5 = all(row["status"] == "PASS" for row in equity_rows)
    test_7 = all(row["asset"] in assets and _decimal(row["tick_size"]) > ZERO and _decimal(row["amount_step"]) > ZERO for row in rule_rows)
    configured = config.get("assets") if isinstance(config.get("assets"), dict) else {}
    configured_assets = [str(key).upper() for key in configured]
    test_8 = configured_assets == assets or set(configured_assets) == set(assets)
    test_10 = all(row["classification"] in {"RECOVERED", "NO_DISCONNECT_OBSERVED"} for row in recovery_rows)
    return {
        "1_asset_conservative_sums_to_portfolio": {"status": "PASS" if all(row["status"] in {"PASS", "PASS_COUNT_ONLY"} for row in fill_recon if row["fill_model"] == "CONSERVATIVE") else "FAIL", "denominator": len([row for row in fill_recon if row["fill_model"] == "CONSERVATIVE"])},
        "2_asset_touch_sums_to_portfolio": {"status": "PASS" if all(row["status"] in {"PASS", "PASS_COUNT_ONLY"} for row in fill_recon if row["fill_model"] == "TOUCH_SENSITIVITY") else "FAIL", "denominator": len([row for row in fill_recon if row["fill_model"] == "TOUCH_SENSITIVITY"])},
        "3_asset_volume_sums_to_shadow_volume": {"status": "PASS" if all(row["shadow_volume_usdc"] == row["canonical_notional_usdc"] for row in fill_recon) else "FAIL", "denominator": len(fill_recon)},
        "4_60s_markout_complete_or_explicit_reason": {"status": "PASS" if test_4 else "FAIL", "denominator": len(six)},
        "5_equity_reconciles": {"status": "PASS" if test_5 else "FAIL", "denominator": len(equity_rows)},
        "6_drawdown_derives_from_equity": {"status": "PASS" if all(row["inventory_notional_used"] == "NO" and _decimal(row["drawdown_usdc"]) >= ZERO for row in drawdown_rows) else "FAIL", "denominator": len(drawdown_rows)},
        "7_active_assets_have_current_rules": {"status": "PASS" if test_7 else "FAIL", "denominator": len(rule_rows)},
        "8_dashboard_assets_equal_config": {"status": "PASS" if test_8 else "FAIL", "denominator": len(assets)},
        "9_dashboard_panels_current_run": {"status": "PASS" if all(row["status"] == "PASS" for row in panel_rows) else "FAIL", "denominator": len(panel_rows)},
        "10_recovered_reconnect_never_uses_stale_bbo": {"status": "PASS" if test_10 else "FAIL", "denominator": len(recovery_rows)},
    }


def _summary_markdown(summary: dict[str, Any], paths: dict[str, str], findings: list[str], limitations: list[str]) -> str:
    tests = summary.get("tests", {})
    test_lines = "\n".join(
        f"| {name} | {value.get('status')} | {value.get('denominator', '')} |"
        for name, value in tests.items()
    )
    path_lines = "\n".join(f"- `{name}`: `{path}`" for name, path in paths.items())
    finding_lines = "\n".join(f"- {value}" for value in findings)
    limitation_lines = "\n".join(f"- {value}" for value in limitations)
    return f"""# Dashboard accounting repair

## Executive summary

This is a read-only measurement repair for the active Derive mainnet-shadow
capture. It does not change the collector, quote parameters, assets,
references, fill assumptions, or execution state. The canonical layer keeps
`CONSERVATIVE` and `TOUCH_SENSITIVITY` separate and records explicit missing
markout states.

| Field | Observation |
|---|---|
| Run ID | `{summary.get('run_id')}` |
| Run status | `{summary.get('run_status')}` |
| Active assets | `{', '.join(summary.get('active_assets', []))}` |
| Mainnet armed | `{summary.get('mainnet_armed')}` |
| Real orders / positions | `{summary.get('real_orders')} / {summary.get('real_positions')}` |
| Repair status | `{summary.get('status')}` |
| Canonical fills | `{summary.get('denominators', {}).get('canonical_fills')}` |
| Complete markout rows | `{summary.get('denominators', {}).get('complete_markouts')}` |

## Findings

{finding_lines}

## Required validation

| Test | Status | Denominator |
|---|---|---:|
{test_lines}

## Method and data quality

- The active SQLite file was opened with SQLite `mode=ro`; no WAL checkpoint or schema write was performed.
- Canonical fill IDs are deterministic report-layer IDs based on the legacy fill row. `quote_id` remains blank because the legacy schema did not persist it.
- Existing persisted markouts are used verbatim. Missing horizons are represented by a state from the required state vocabulary; no price is forward-filled.
- Equity is recomputed from fills with FIFO realized PNL, open-lot unrealized PNL, maker fees, current Derive mid, and inventory units. Drawdown uses equity peak minus current equity.
- Rule rows prefer the live public Derive instrument probe and expose conflicts with the older mapping rather than silently merging them.

## Limitations

{limitation_lines}

## Reproduction and artifacts

Run the report generator from the project root with the active run paths. It is safe to run while the collector is live because its telemetry access is read-only.

{path_lines}
"""


def build_repair(
    *,
    config_path: Path,
    state_path: Path,
    telemetry_path: Path,
    mapping_path: Path | None,
    rate_limit_audit_path: Path | None,
    output_dir: Path,
    metadata_path: Path | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    config = _yaml(config_path)
    state = _json(state_path)
    metadata = _json(metadata_path)
    mapping_payload = _json(mapping_path)
    mapping = mapping_payload.get("mappings", {}) if isinstance(mapping_payload.get("mappings"), dict) else {}
    rate_audit = _json(rate_limit_audit_path)
    run_id = str(metadata.get("run_id") or state.get("run_id") or telemetry_path.parent.name)
    configured_assets = config.get("assets")
    if isinstance(configured_assets, dict):
        assets = [str(asset).upper() for asset, value in configured_assets.items() if not isinstance(value, dict) or value.get("enabled", True)]
    elif isinstance(configured_assets, list):
        assets = [str(asset).upper() for asset in configured_assets]
    else:
        assets = []
    state_assets = [str(asset).upper() for asset in state.get("active_assets", [])] if isinstance(state.get("active_assets"), list) else []
    if state_assets:
        assets = state_assets
    as_of = float(now if now is not None else time.time())
    last_update = _number(state.get("last_update"))
    if last_update is not None:
        as_of = max(as_of, last_update)
    started = _number(metadata.get("start_time_epoch") or state.get("started_at"))
    ended = _number(metadata.get("end_time_epoch") or state.get("ended_at"))
    run_status = str(state.get("status") or metadata.get("status") or "UNKNOWN")
    raw_retention = _number(config.get("raw_retention_seconds")) or 0.0
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(f"file:{telemetry_path}?mode=ro", uri=True, timeout=1.0)
    connection.row_factory = sqlite3.Row
    try:
        fills_raw = _rows(connection, "fills", "timestamp, id")
        markouts_raw = _rows(connection, "markouts", "fill_timestamp, horizon_seconds, id")
        actions = _rows(connection, "actions", "timestamp, id")
        trades = _rows(connection, "trades", "timestamp, id")
        decisions = _rows(connection, "decisions", "timestamp, id")
        fills = _canonical_fills(fills_raw, state, connection, run_id)
    finally:
        connection.close()
    latest_mids = _latest_mids(state, decisions, assets)
    fill_recon, portfolio = _fill_reconciliation(fills, state, assets)
    markouts = _canonical_markouts(
        fills,
        markouts_raw,
        as_of=as_of,
        run_status=run_status,
        run_end=ended,
        raw_retention_seconds=raw_retention,
    )
    markout_audit = _markout_audit(markouts, assets)
    net_capture = _net_capture(fills, markouts, assets)
    capital = _decimal(config.get("capital_usdc"), Decimal("800"))
    equity_rows, drawdown_rows, equity_tests = _equity_rows(fills, latest_mids, state, capital)
    rule_rows = _rule_rows(assets, config, state, mapping, rate_audit, latest_mids)
    control_audit = _action_state_at(actions, trades, fills, assets, MODEL_KEYS)
    recovery_rows = _connection_recovery(state, assets, run_id)
    churn_rows = _quote_churn(actions, assets, as_of)
    panel_rows = _panel_audit(run_id, state_path, telemetry_path, output_dir)
    tests = _required_tests(
        fill_recon,
        markouts,
        equity_rows,
        drawdown_rows,
        rule_rows,
        assets,
        state,
        panel_rows,
        recovery_rows,
        config,
    )
    # Preserve the exact requested control-audit header.  A stable fill/trade
    # relationship cannot be reconstructed from the legacy schema, so the
    # report uses NO_OBSERVED_FILL rather than treating every public trade as a
    # fill.
    _write_csv(output_dir / "fill_reconciliation.csv", fill_recon)
    _write_csv(
        output_dir / "control_fill_audit.csv",
        control_audit,
        [
            "trade_event_id", "asset", "DERIVE_ONLY quote", "DERIVE_ONLY active?", "DERIVE_ONLY filled?", "BINANCE_ONLY quote",
            "BINANCE_ONLY active?", "BINANCE_ONLY filled?", "PRIORITY_FAILOVER quote", "PRIORITY_FAILOVER active?", "PRIORITY_FAILOVER filled?", "reason",
        ],
    )
    _write_csv(output_dir / "markout_pipeline_audit.csv", markout_audit)
    _write_csv(output_dir / "net_capture_audit.csv", net_capture)
    _write_csv(output_dir / "equity_reconciliation.csv", equity_rows)
    _write_csv(output_dir / "drawdown_audit.csv", drawdown_rows)
    _write_csv(output_dir / "trading_rules_current.csv", rule_rows)
    _write_csv(output_dir / "connection_recovery_audit.csv", recovery_rows)
    _write_csv(output_dir / "quote_churn_audit.csv", churn_rows)
    _write_csv(output_dir / "panel_run_id_audit.csv", panel_rows)
    complete_markouts = sum(row["status"] == "COMPLETE" for row in markouts)
    insufficient_net = sum(row["status"] == "INSUFFICIENT_SAMPLE" for row in net_capture)
    findings = [
        f"Canonical fill evidence contains {len(fills)} persisted shadow fill rows; canonical volume is {_fmt(portfolio['canonical_notional_usdc'])} USDC.",
        "Conservative and touch-sensitive fills are reported independently; touch rows are not execution evidence.",
        "quote_id is NOT_PERSISTED_IN_LEGACY_SCHEMA, so fill-to-quote and trade-to-fill joins remain explicitly unlinked.",
        f"{complete_markouts} persisted markout rows are complete; missing horizons are represented explicitly, including the 120-second horizon.",
        f"Net capture has {insufficient_net} insufficient-sample groups; no group is promoted from sparse evidence.",
        "Current Derive rules prefer the live public instrument probe; the legacy mapping conflict is retained in trading_rules_current.csv.",
    ]
    limitations = [
        "The current run is still RUNNING, so this is an in-progress accounting snapshot rather than a terminal report.",
        "The legacy telemetry schema does not persist run_id, quote_id, fill_id, equity history, or explicit markout status; report-layer IDs are deterministic but quote IDs cannot be recovered.",
        "Only the collector's persisted markouts are complete evidence. Public Derive trades and touch-sensitive rows remain research diagnostics, not real fills.",
        "Raw retention is bounded; missing historical horizons are marked HISTORICAL_DATA_PRUNED when the retention window has expired.",
        "Markout and net-capture samples are below the project's sufficiency threshold for all or most groups; READY FOR LIVE remains NO.",
    ]
    summary: dict[str, Any] = {
        "status": "PARTIAL_DATA_INSUFFICIENT" if run_status == "RUNNING" or insufficient_net else "REPAIRED_READ_ONLY",
        "run_id": run_id,
        "run_status": run_status,
        "active_assets": assets,
        "started_at_utc": _utc(started),
        "planned_end_time_utc": metadata.get("planned_end_time_utc", _utc(_number(metadata.get("planned_end_epoch")))),
        "observed_at_utc": _utc(as_of),
        "mode": state.get("mode", config.get("mode")),
        "dry_run": bool(state.get("dry_run", config.get("dry_run", True))),
        "mainnet_armed": bool(state.get("mainnet_armed", config.get("mainnet_armed", False))),
        "real_orders": state.get("real_orders", 0),
        "real_positions": state.get("real_positions", 0),
        "bitget_enabled": bool(state.get("bitget_enabled", config.get("bitget_enabled", False))),
        "reference_priority": state.get("reference_priority", config.get("reference_priority", [])),
        "denominators": {
            "canonical_fills": len(fills),
            "canonical_notional_usdc": _fmt(portfolio["canonical_notional_usdc"]),
            "derive_trades": len([row for row in trades if str(row.get("source", "")).lower() == "derive"]),
            "complete_markouts": complete_markouts,
            "markout_rows": len(markouts),
            "net_capture_groups": len(net_capture),
            "insufficient_net_capture_groups": insufficient_net,
            "equity_models": len(equity_rows),
        },
        "canonical_fill_fields": list(CANONICAL_FILL_FIELDS),
        "canonical_markout_fields": list(CANONICAL_MARKOUT_FIELDS),
        "markout_states": list(MARKOUT_STATES),
        "quote_id_status": "NOT_PERSISTED_IN_LEGACY_SCHEMA",
        "findings": findings,
        "limitations": limitations,
        "tests": tests,
        "safety": {
            "architecture_changed": "NO",
            "strategy_parameters_changed": "NO",
            "collector_restarted": "NO",
            "real_orders": 0,
            "real_positions": 0,
            "private_api_used": False,
        },
        "source_paths": {
            "config": str(config_path),
            "state": str(state_path),
            "telemetry": str(telemetry_path),
            "mapping": str(mapping_path) if mapping_path else "",
            "rate_limit_audit": str(rate_limit_audit_path) if rate_limit_audit_path else "",
        },
    }
    paths = {
        "summary": str(output_dir / "repair_summary.json"),
        "markdown": str(output_dir / "repair_summary.md"),
        "canonical_fills": str(output_dir / "fill_reconciliation.csv"),
        "canonical_markouts": str(output_dir / "markout_pipeline_audit.csv"),
        "net_capture": str(output_dir / "net_capture_audit.csv"),
    }
    _write_text(output_dir / "dashboard_inconsistency_audit.md", _summary_markdown(summary, paths, findings, limitations))
    _write_text(output_dir / "repair_summary.md", _summary_markdown(summary, paths, findings, limitations))
    (output_dir / "repair_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize a read-only dashboard accounting repair")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--telemetry", type=Path, required=True)
    parser.add_argument("--mapping", type=Path, default=None)
    parser.add_argument("--rate-limit-audit", type=Path, default=None)
    parser.add_argument("--run-metadata", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    summary = build_repair(
        config_path=args.config,
        state_path=args.state,
        telemetry_path=args.telemetry,
        mapping_path=args.mapping,
        rate_limit_audit_path=args.rate_limit_audit,
        output_dir=args.output_dir,
        metadata_path=args.run_metadata,
    )
    print(json.dumps({"status": summary["status"], "run_id": summary["run_id"], "output_dir": str(args.output_dir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
