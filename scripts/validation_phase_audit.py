"""Measurement-only audit for the next three-asset validation phase.

This script does not alter strategy configuration or trading behavior. It
performs a short public Derive websocket probe and reads a prior telemetry
database in SQLite read-only mode to separate decision cycles from quote
mutations, fills, and observed Derive trades.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

try:
    from websockets.asyncio.client import connect
except ImportError:  # pragma: no cover
    from websockets import connect

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.public_data import parse_book_message, parse_trade_message

FILL_MODELS = ("CONSERVATIVE", "TOUCH_SENSITIVITY")
QUOTE_ACTIONS = {"CREATE", "CANCEL", "REPLACE"}


def _json_default(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("\n", encoding="utf-8")
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
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _instruments(config: RuntimeConfig, mapping_path: Path | None) -> dict[str, str]:
    report = _read_json(mapping_path) if mapping_path else {}
    mappings = report.get("mappings", {}) if isinstance(report, dict) else {}
    return {
        asset.symbol: str((mappings.get(asset.symbol) or {}).get("derive_instrument") or f"{asset.symbol}-PERP").upper()
        for asset in config.enabled_assets
    }


def _trade_row_payload(channel: str, row: dict[str, Any]) -> dict[str, Any]:
    return {"params": {"channel": channel, "data": [row]}}


async def probe_trade_feed(config: RuntimeConfig, instruments: dict[str, str], probe_seconds: float) -> dict[str, Any]:
    assets = {
        asset: {
            "asset": asset,
            "instrument": instrument,
            "trade_feed_channel": f"trades.{instrument}",
            "book_feed_channel": f"orderbook.{instrument}.1.20",
            "subscription_status": None,
            "subscription_success": False,
            "trade_channel_messages": 0,
            "book_messages": 0,
            "book_parse_failures": 0,
            "trade_parse_failures": 0,
            "raw_trade_rows": 0,
            "trade_count": 0,
            "first_trade_timestamp": None,
            "last_trade_timestamp": None,
            "first_trade_receipt_timestamp": None,
            "last_trade_receipt_timestamp": None,
            "trade_ids": [],
            "aggressor_sides": Counter(),
            "trade_timestamps": [],
        }
        for asset, instrument in instruments.items()
    }
    channels = [
        channel
        for row in assets.values()
        for channel in (row["trade_feed_channel"], row["book_feed_channel"])
    ]
    started = time.time()
    ack_success = False
    ack_status: dict[str, Any] = {}
    connection_error: str | None = None
    try:
        async with connect(
            config.derive_websocket_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=2,
            max_size=4 * 1024 * 1024,
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "subscribe",
                        "params": {"channels": channels},
                    }
                )
            )
            deadline = time.monotonic() + probe_seconds
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=max(0.1, min(2.0, deadline - time.monotonic())))
                except TimeoutError:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if payload.get("id") == 1 and isinstance(payload.get("result"), dict):
                    ack_status = payload["result"].get("status", {})
                    ack_success = bool(ack_status) and all(value == "ok" for value in ack_status.values())
                    for _asset, state in assets.items():
                        state["subscription_status"] = ack_status.get(state["trade_feed_channel"])
                        state["subscription_success"] = state["subscription_status"] == "ok"
                params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
                channel = str(params.get("channel", ""))
                for state in assets.values():
                    if channel == state["book_feed_channel"]:
                        state["book_messages"] += 1
                        try:
                            if parse_book_message(payload, source="derive", receipt_timestamp=time.time()) is None:
                                state["book_parse_failures"] += 1
                        except (TypeError, ValueError, KeyError):
                            state["book_parse_failures"] += 1
                    if channel != state["trade_feed_channel"]:
                        continue
                    state["trade_channel_messages"] += 1
                    rows = params.get("data") if isinstance(params.get("data"), list) else []
                    for row in rows:
                        if not isinstance(row, dict):
                            state["trade_parse_failures"] += 1
                            continue
                        state["raw_trade_rows"] += 1
                        receipt = time.time()
                        try:
                            parsed = parse_trade_message(
                                _trade_row_payload(channel, row), source="derive", receipt_timestamp=receipt
                            )
                        except (TypeError, ValueError, KeyError):
                            parsed = None
                            state["trade_parse_failures"] += 1
                        if parsed is None or parsed[0] != state["instrument"]:
                            continue
                        trade = parsed[1]
                        state["trade_count"] += 1
                        state["trade_timestamps"].append(trade.exchange_timestamp or trade.timestamp)
                        state["first_trade_timestamp"] = (
                            trade.exchange_timestamp if state["first_trade_timestamp"] is None else state["first_trade_timestamp"]
                        )
                        state["last_trade_timestamp"] = trade.exchange_timestamp
                        state["first_trade_receipt_timestamp"] = (
                            trade.timestamp if state["first_trade_receipt_timestamp"] is None else state["first_trade_receipt_timestamp"]
                        )
                        state["last_trade_receipt_timestamp"] = trade.timestamp
                        if len(state["trade_ids"]) < 20:
                            state["trade_ids"].append(str(trade.trade_id))
                        state["aggressor_sides"][trade.side.value] += 1
    except Exception as exc:  # pragma: no cover - endpoint-dependent
        connection_error = f"{type(exc).__name__}: {exc}"

    for state in assets.values():
        timestamps = sorted(value for value in state.pop("trade_timestamps") if value is not None)
        gaps = [right - left for left, right in zip(timestamps, timestamps[1:], strict=True)]
        state["largest_trade_gap_seconds"] = max(gaps) if gaps else None
        state["aggressor_sides"] = dict(state["aggressor_sides"])
        if state["subscription_success"] and state["trade_count"] > 0:
            state["classification"] = "TRADE_FEED_VERIFIED_WITH_TRADES"
        elif state["subscription_success"] and not connection_error and not state["trade_parse_failures"]:
            state["classification"] = "TRADE_FEED_VERIFIED_NO_TRADES_OCCURRED"
            state["classification_caveat"] = (
                "No trade event was delivered during this probe window; this does not prove that no market trades "
                "occurred outside the window."
            )
        elif connection_error:
            state["classification"] = "TRADE_FEED_BROKEN"
        else:
            state["classification"] = "TRADE_FEED_NOT_VERIFIED"
    return {
        "endpoint": config.derive_websocket_url,
        "channels": channels,
        "probe_seconds": round(time.time() - started, 3),
        "subscription_ack_success": ack_success,
        "subscription_status": ack_status,
        "connection_error": connection_error,
        "assets": assets,
    }


def _rolling_max(timestamps: list[float], window_seconds: float = 60.0) -> int:
    timestamps = sorted(timestamps)
    maximum = 0
    left = 0
    for right, timestamp in enumerate(timestamps):
        while timestamps[left] < timestamp - window_seconds:
            left += 1
        maximum = max(maximum, right - left + 1)
    return maximum


def _plan_fingerprint(control_payload: dict[str, Any]) -> str:
    plan = control_payload.get("plan") or {}
    return json.dumps(
        [plan.get(key) for key in ("bid_price", "bid_amount", "ask_price", "ask_amount", "block_reason", "market_mode")],
        sort_keys=True,
    )


def _decision_blocked(control_payload: dict[str, Any]) -> bool:
    plan = control_payload.get("plan") or {}
    fair_value = control_payload.get("fair_value") or {}
    return bool(plan.get("block_reason") or fair_value.get("pause_reason") or (plan.get("market_mode") == "PAUSED"))


def analyze_telemetry(config: RuntimeConfig, telemetry_path: Path) -> dict[str, Any]:
    connection = _read_only(telemetry_path)
    try:
        decisions = [
            {"timestamp": float(row["timestamp"]), "asset": row["asset"], "payload": json.loads(row["payload_json"])}
            for row in connection.execute("SELECT timestamp, asset, payload_json FROM decisions ORDER BY timestamp")
        ]
        actions = [dict(row) for row in connection.execute("SELECT * FROM actions ORDER BY timestamp, id")]
        fills = [dict(row) for row in connection.execute("SELECT * FROM fills ORDER BY timestamp, id")]
        trades = [dict(row) for row in connection.execute("SELECT * FROM trades ORDER BY timestamp, id")]
    finally:
        connection.close()

    assets = [asset.symbol for asset in config.enabled_assets]
    decisions_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for decision in decisions:
        decisions_by_asset[decision["asset"]].append(decision)
    fills_by_model_asset = Counter((row["model"], row["asset"]) for row in fills)
    trade_counts = Counter(row["asset"] for row in trades if row.get("source") == "derive")
    rows: list[dict[str, Any]] = []
    for asset in assets:
        asset_decisions = decisions_by_asset[asset]
        asset_timestamps = [row["timestamp"] for row in asset_decisions]
        duration_seconds = max(asset_timestamps) - min(asset_timestamps) if len(asset_timestamps) > 1 else 0.0
        duration_minutes = max(duration_seconds / 60.0, 1.0 / 60.0)
        asset_actions = [row for row in actions if row["asset"] == asset]
        for control in config.control_models:
            control_decisions = []
            desired_changes = 0
            previous_fingerprint: str | None = None
            blocks = 0
            for decision in asset_decisions:
                control_payload = (decision["payload"].get("controls") or {}).get(control) or {}
                fingerprint = _plan_fingerprint(control_payload)
                if fingerprint != previous_fingerprint:
                    desired_changes += 1
                    previous_fingerprint = fingerprint
                blocks += int(_decision_blocked(control_payload))
                control_decisions.append(control_payload)
            for fill_model in FILL_MODELS:
                model = f"{control}:{fill_model}"
                model_actions = [row for row in asset_actions if row["model"] == model]
                creates = sum(row["action"] == "CREATE" for row in model_actions)
                holds = sum(row["action"] == "HOLD" for row in model_actions)
                cancels = sum(row["action"] == "CANCEL" for row in model_actions)
                replacement_cancels = sum(
                    row["action"] == "CANCEL" and row["reason"] == "REFRESH_NEEDED" for row in model_actions
                )
                quote_timestamps = [row["timestamp"] for row in model_actions if row["action"] in QUOTE_ACTIONS]
                row = {
                    "asset": asset,
                    "control": control,
                    "fill_model": fill_model,
                    "model": model,
                    "decision_cycles": len(asset_decisions),
                    "action_rows": len(model_actions),
                    "desired_quote_changes": desired_changes,
                    "creates": creates,
                    "holds": holds,
                    "replaces": replacement_cancels,
                    "cancels": cancels,
                    "non_replace_cancels": cancels - replacement_cancels,
                    "blocks": blocks,
                    "shadow_bid_created": sum(row["action"] == "CREATE" and row["side"] == "BUY" for row in model_actions),
                    "shadow_ask_created": sum(row["action"] == "CREATE" and row["side"] == "SELL" for row in model_actions),
                    "derive_public_trades": trade_counts.get(asset, 0),
                    "touch_events": fills_by_model_asset.get((f"{control}:TOUCH_SENSITIVITY", asset), 0),
                    "conservative_fill_events": fills_by_model_asset.get((f"{control}:CONSERVATIVE", asset), 0),
                    "creates_per_min": round(creates / duration_minutes, 6),
                    "replaces_per_min": round(replacement_cancels / duration_minutes, 6),
                    "cancels_per_min": round(cancels / duration_minutes, 6),
                    "holds_per_min": round(holds / duration_minutes, 6),
                    "max_quote_actions_rolling_60s": _rolling_max(quote_timestamps),
                    "churn_classification": (
                        "QUOTE_CHURN_HIGH"
                        if _rolling_max(quote_timestamps) > config.max_actions_per_minute
                        else "QUOTE_CHURN_WITHIN_LIMIT"
                    ),
                    "trade_event_definition": "Derive telemetry rows with source=derive",
                    "touch_event_definition": "fills recorded by the TOUCH_SENSITIVITY shadow model",
                    "replace_definition": "CANCEL rows with reason=REFRESH_NEEDED",
                    "status": "OBSERVED" if asset_decisions else "DATA_INSUFFICIENT",
                }
                rows.append(row)

    comparison = []
    for asset in assets:
        for control in config.control_models:
            conservative = next(row for row in rows if row["asset"] == asset and row["control"] == control and row["fill_model"] == "CONSERVATIVE")
            comparison.append({**conservative, "comparison_scope": "CONSERVATIVE"})
    return {
        "telemetry_path": str(telemetry_path),
        "decision_rows": len(decisions),
        "action_rows": len(actions),
        "trade_rows": len([row for row in trades if row.get("source") == "derive"]),
        "fill_rows": len(fills),
        "action_breakdown": rows,
        "asset_control_comparison": comparison,
    }


def _pre_run_state(state_path: Path, config: RuntimeConfig, trade_probe: dict[str, Any]) -> dict[str, Any]:
    state = _read_json(state_path)
    latest_consensus = state.get("latest_consensus") or {}
    latest_decisions = state.get("latest_decisions") or {}
    health = state.get("source_health") or {}
    assets = {}
    for asset in [item.symbol for item in config.enabled_assets]:
        derive_health = (health.get(asset) or {}).get("derive") or {}
        selected = (latest_consensus.get(asset) or {}).get("selected_reference")
        assets[asset] = {
            "derive_bbo_feed": "WORKING" if derive_health.get("health") in {"HEALTHY", "DEGRADED"} and derive_health.get("updates", 0) > 0 else "NOT_WORKING",
            "derive_bbo_updates": derive_health.get("updates", 0),
            "derive_trade_feed": (trade_probe.get("assets") or {}).get(asset, {}).get("classification", "TRADE_FEED_NOT_VERIFIED"),
            "binance": ((health.get(asset) or {}).get("binance") or {}).get("health", "UNKNOWN"),
            "bybit": ((health.get(asset) or {}).get("bybit") or {}).get("health", "UNKNOWN"),
            "okx": ((health.get(asset) or {}).get("okx") or {}).get("health", "UNKNOWN"),
            "selected_source": selected,
            "selected_source_valid": selected in config.reference_priority,
            "shadow_quote_engine": bool(latest_decisions.get(asset)),
        }
    return {
        "state_path": str(state_path),
        "state_status": state.get("status"),
        "mode": state.get("mode"),
        "dry_run": state.get("dry_run"),
        "mainnet_armed": state.get("mainnet_armed"),
        "real_orders": state.get("real_orders", 0),
        "real_positions": state.get("real_positions", 0),
        "reference_execution": state.get("reference_execution", False),
        "assets": assets,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="conf/mainnet_shadow.yml")
    parser.add_argument("--telemetry", required=True)
    parser.add_argument("--state", default="logs/priority_reference_3asset/state.json")
    parser.add_argument("--mapping", default=None)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--probe-seconds", type=float, default=45.0)
    args = parser.parse_args()

    config = RuntimeConfig.from_yaml(args.config)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    instruments = _instruments(config, Path(args.mapping) if args.mapping else None)
    trade_probe = asyncio.run(probe_trade_feed(config, instruments, args.probe_seconds))
    telemetry = analyze_telemetry(config, Path(args.telemetry))
    pre_run = _pre_run_state(Path(args.state), config, trade_probe)
    audit = {
        "active_assets": [asset.symbol for asset in config.enabled_assets],
        "reference_priority": list(config.reference_priority),
        "mode": config.mode.value,
        "dry_run": config.dry_run,
        "mainnet_armed": config.mainnet_armed,
        "real_orders": 0,
        "real_positions": 0,
        "reference_execution": False,
        "instruments": instruments,
        "pre_run_state": pre_run,
        "trade_feed_probe": trade_probe,
        "telemetry": {
            key: telemetry[key] for key in ("telemetry_path", "decision_rows", "action_rows", "trade_rows", "fill_rows")
        },
    }
    _write_json(out_dir / "pre_run_audit.json", audit)
    _write_csv(out_dir / "action_breakdown_10m.csv", telemetry["action_breakdown"])
    _write_csv(out_dir / "quote_churn_10m.csv", telemetry["action_breakdown"])
    _write_csv(out_dir / "asset_control_comparison_10m.csv", telemetry["asset_control_comparison"])
    summary = {
        "report_version": "validation-phase-audit-v1",
        "measurement_only": True,
        "feed_classifications": {
            asset: row["classification"] for asset, row in trade_probe["assets"].items()
        },
        "trade_probe_seconds": trade_probe["probe_seconds"],
        "prior_10m": {
            key: telemetry[key] for key in ("decision_rows", "action_rows", "trade_rows", "fill_rows")
        },
        "ready_for_new_shadow": all(
            row["subscription_success"] and row["classification"] in {"TRADE_FEED_VERIFIED_WITH_TRADES", "TRADE_FEED_VERIFIED_NO_TRADES_OCCURRED"}
            for row in trade_probe["assets"].values()
        ),
        "primary_limitations": [
            "A zero-count public trade probe is not proof that no market trades occurred outside the probe window.",
            "The prior ten-minute run has no conservative or touch fills, so markouts and net capture remain DATA_INSUFFICIENT.",
        ],
    }
    _write_json(out_dir / "validation_phase_summary.json", summary)
    markdown = [
        "# NEXT VALIDATION PHASE AUDIT",
        "",
        "Measurement-only; strategy, active assets, reference priority, and execution safety were unchanged.",
        "",
        "## Trade-feed classification",
        "",
        "| Asset | Channel | Subscription | Trades observed | Classification |",
        "|---|---|---:|---:|---|",
    ]
    for asset, row in trade_probe["assets"].items():
        markdown.append(
            f"| {asset} | `{row['trade_feed_channel']}` | {row['subscription_success']} | {row['trade_count']} | `{row['classification']}` |"
        )
    markdown.extend(
        [
            "",
            "Zero-count classification means the channel subscription was acknowledged and no trade event was delivered during the probe; it is not a claim that the market had no trades outside the probe window.",
            "",
            "## Prior ten-minute telemetry",
            "",
            f"Decision rows: `{telemetry['decision_rows']}`; action rows: `{telemetry['action_rows']}`; Derive public trades: `{telemetry['trade_rows']}`; fills: `{telemetry['fill_rows']}`.",
            "",
            "See `action_breakdown_10m.csv`, `quote_churn_10m.csv`, and `asset_control_comparison_10m.csv` for separate decision, quote mutation, hold, block, trade, touch, and conservative-fill counts.",
            "",
        ]
    )
    (out_dir / "validation_phase_summary.md").write_text("\n".join(markdown), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
