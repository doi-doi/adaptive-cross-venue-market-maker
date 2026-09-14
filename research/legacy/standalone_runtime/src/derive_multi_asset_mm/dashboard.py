"""Read-only local dashboard server."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def _safe_json(path: Path, fallback: dict) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def _process_exists(pid: object) -> bool:
    try:
        os.kill(int(pid), 0)
    except (OSError, TypeError, ValueError):
        return False
    return True


def _json_mapping(value: object) -> dict:
    """Return a JSON object from telemetry without allowing one bad row to break the dashboard."""

    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dominant(value: object) -> str | None:
    mapping = _json_mapping(value)
    if not mapping:
        return None
    try:
        return max(mapping.items(), key=lambda item: float(item[1]))[0]
    except (TypeError, ValueError):
        return None


def _average(total: object, count: object) -> float | None:
    try:
        denominator = int(count or 0)
        return float(total) / denominator if denominator else None
    except (TypeError, ValueError):
        return None


def _finite_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def make_handler(root: Path):
    priority_state_base = root / "logs/xrp_link_mainnet_shadow"
    priority_report_base = root / "reports/xrp_link_mainnet_shadow"
    priority_telemetry_base = root / "logs/xrp_link_mainnet_shadow"
    validation6h_log_base = root / "logs/xrp_link_mainnet_shadow"
    validation6h_report_base = root / "reports/xrp_link_mainnet_shadow"
    refresh_log_base = root / "logs/xrp_link_refresh_research"
    refresh_report_base = root / "reports/xrp_link_refresh_research"
    accounting_repair_base = root / "reports/dashboard_accounting_repair"
    quote_fill_diagnostic_base = root / "reports/quote_fill_diagnostic"
    index_path = root / "dashboard/index.html"
    validation6h_cache: tuple[float, dict] | None = None

    def latest_child_artifact(base: Path, name: str) -> Path | None:
        candidates = [
            child / name
            for child in base.iterdir()
            if child.is_dir() and (child / name).is_file()
        ] if base.is_dir() else []
        return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None

    def refresh_context() -> tuple[Path, Path, Path, Path] | None:
        """Resolve the newest XRP/LINK refresh run, if one exists."""

        candidates: list[tuple[float, Path]] = []
        if refresh_log_base.is_dir():
            for state_path in refresh_log_base.rglob("state.json"):
                if not state_path.is_file():
                    continue
                state = _safe_json(state_path, {})
                try:
                    started = _finite_float(state.get("started_at")) or state_path.stat().st_mtime
                except OSError:
                    started = 0.0
                candidates.append((started, state_path))
        if not candidates:
            return None
        _, state_path = max(candidates, key=lambda item: item[0])
        run_id = state_path.parent.name
        metadata_path = refresh_report_base / "runtime" / run_id / "run_metadata.json"
        if not metadata_path.exists():
            metadata_path = state_path.parent / "run_metadata.json"
        if not metadata_path.exists():
            metadata_path = refresh_report_base / "run_metadata.json"
        return (
            state_path,
            refresh_report_base / "final_refresh_research.json",
            metadata_path,
            state_path.parent / "telemetry.sqlite",
        )

    def validation6h_context() -> tuple[Path, Path, Path, Path]:
        """Resolve the newest active refresh/shadow run without overwriting evidence."""

        refresh = refresh_context()
        if refresh is not None:
            return refresh

        candidates: list[tuple[float, Path, Path]] = []
        log_dirs = [validation6h_log_base]
        if validation6h_log_base.is_dir():
            log_dirs.extend(child for child in validation6h_log_base.iterdir() if child.is_dir())
        for log_dir in log_dirs:
            state_path = log_dir / "state.json"
            telemetry_path = log_dir / "telemetry.sqlite"
            if not state_path.exists() and not telemetry_path.exists():
                continue
            state = _safe_json(state_path, {})
            try:
                started = float(state.get("started_at") or state_path.stat().st_mtime)
            except (OSError, TypeError, ValueError):
                started = 0.0
            report_dir = validation6h_report_base / log_dir.name if log_dir != validation6h_log_base else validation6h_report_base
            candidates.append((started, log_dir, report_dir))
        if not candidates:
            return (
                validation6h_log_base / "state.json",
                validation6h_report_base / "final_6h_validation_report.json",
                validation6h_report_base / "run_metadata.json",
                validation6h_log_base / "telemetry.sqlite",
            )
        _, log_dir, report_dir = max(candidates, key=lambda item: item[0])
        return (
            log_dir / "state.json",
            report_dir / "final_6h_validation_report.json",
            report_dir / "run_metadata.json",
            log_dir / "telemetry.sqlite",
        )

    def selected_artifact(
        priority_base: Path,
        priority_name: str,
        preferred: Path,
        legacy: Path,
    ) -> Path:
        """Resolve priority artifacts first, including isolated child runs."""

        priority = priority_base / priority_name
        if priority.exists():
            return priority
        child = latest_child_artifact(priority_base, priority_name)
        if child is not None:
            return child
        if preferred.exists():
            return preferred
        return legacy

    def current_dashboard_context() -> tuple[Path, Path, Path, Path]:
        """Use the newest active refresh/shadow run for every panel while it exists."""

        six_hour = validation6h_context()
        if any(path.exists() for path in six_hour):
            return six_hour
        return (
            priority_state_base / "state.json",
            priority_report_base / "final_report.json",
            priority_report_base / "run_metadata.json",
            priority_telemetry_base / "telemetry.sqlite",
        )

    def context_run_id(context: tuple[Path, Path, Path, Path]) -> str | None:
        state_path, _, metadata_path, telemetry_path = context
        metadata = _safe_json(metadata_path, {})
        state = _safe_json(state_path, {})
        run_id = metadata.get("run_id") or state.get("run_id")
        if run_id:
            return str(run_id)
        parent_name = telemetry_path.parent.name
        if parent_name not in {
            refresh_log_base.name,
            validation6h_log_base.name,
            priority_telemetry_base.name,
        }:
            return parent_name
        return None

    def telemetry_path_for(run: str) -> Path:
        """Resolve the read-only telemetry source selected by the dashboard."""

        if run.lower() in {"6h", "six_hour", "validation6h"}:
            return validation6h_context()[3]
        if run.lower() in {"current", "priority"}:
            return current_dashboard_context()[3]
        return selected_artifact(
            priority_telemetry_base,
            "telemetry.sqlite",
            priority_telemetry_base / "telemetry.sqlite",
            priority_telemetry_base / "telemetry.sqlite",
        )

    def csv_rows(path: Path) -> list[dict[str, str]]:
        if not path.exists():
            return []
        try:
            with path.open(newline="", encoding="utf-8") as handle:
                return list(csv.DictReader(handle))
        except (OSError, csv.Error):
            return []

    def refresh_research_payload() -> dict:
        """Read the refresh study and its newest runtime snapshot without writes."""

        final_path = refresh_report_base / "final_refresh_research.json"
        final = _safe_json(final_path, {})
        recommendations = csv_rows(refresh_report_base / "asset_recommendations.csv")
        matrix = csv_rows(refresh_report_base / "deadband_residency_matrix.csv")
        utilization = csv_rows(refresh_report_base / "rate_limit_utilization.csv")
        current_state: dict = {}
        context = refresh_context()
        if context is not None:
            current_state = _safe_json(context[0], {})
        repair_path = accounting_repair_base / "repair_summary.json"
        repair = _safe_json(repair_path, {})
        run_id = context_run_id(context) if context is not None else None
        if not run_id:
            run_id = str(repair.get("run_id")) if repair.get("run_id") else None
        expected_run_id = context_run_id(current_dashboard_context())
        repair_run_id = str(repair.get("run_id")) if repair.get("run_id") else None
        return {
            "available": bool(final or recommendations or matrix),
            "report_path": str(final_path),
            "final": final,
            "recommendations": recommendations,
            "matrix": matrix,
            "rate_limit_utilization": utilization,
            "runtime_state": current_state,
            "run_id": run_id,
            "repair": repair,
            "repair_path": str(repair_path),
            "panel_status": "PASS" if not repair_run_id or not expected_run_id or repair_run_id == expected_run_id else "STALE_PANEL_DATA",
        }

    def accounting_repair_payload() -> dict:
        """Read the materialized accounting repair and expose compact dashboard rows."""

        summary_path = accounting_repair_base / "repair_summary.json"
        summary = _safe_json(summary_path, {})
        if not summary:
            return {
                "available": False,
                "summary_path": str(summary_path),
                "panel_status": "NOT_AVAILABLE",
                "reason": "ACCOUNTING_REPAIR_SNAPSHOT_NOT_AVAILABLE",
                "asset_rows": [],
            }

        fill_rows = csv_rows(accounting_repair_base / "fill_reconciliation.csv")
        markout_rows = csv_rows(accounting_repair_base / "markout_pipeline_audit.csv")
        net_rows = csv_rows(accounting_repair_base / "net_capture_audit.csv")
        equity_rows = csv_rows(accounting_repair_base / "equity_reconciliation.csv")
        drawdown_rows = csv_rows(accounting_repair_base / "drawdown_audit.csv")
        rule_rows = csv_rows(accounting_repair_base / "trading_rules_current.csv")
        recovery_rows = csv_rows(accounting_repair_base / "connection_recovery_audit.csv")
        panel_rows = csv_rows(accounting_repair_base / "panel_run_id_audit.csv")
        model_keys = (
            "DERIVE_ONLY:CONSERVATIVE",
            "DERIVE_ONLY:TOUCH_SENSITIVITY",
            "BINANCE_ONLY_NO_FAILOVER:CONSERVATIVE",
            "BINANCE_ONLY_NO_FAILOVER:TOUCH_SENSITIVITY",
            "PRIORITY_FAILOVER:CONSERVATIVE",
            "PRIORITY_FAILOVER:TOUCH_SENSITIVITY",
        )

        def as_horizon(row: dict[str, str]) -> int | None:
            try:
                return int(float(row.get("horizon_seconds", "")))
            except (TypeError, ValueError):
                return None

        fill_by_key = {
            (row.get("asset"), row.get("model"), row.get("fill_model")): row
            for row in fill_rows
        }
        markout_by_key = {
            (row.get("asset"), row.get("model"), row.get("fill_model"), as_horizon(row)): row
            for row in markout_rows
        }
        net_by_key = {
            (row.get("asset"), row.get("model"), row.get("fill_model"), as_horizon(row)): row
            for row in net_rows
        }
        equity_by_key = {(row.get("model"), row.get("fill_model")): row for row in equity_rows}
        drawdown_by_key = {(row.get("model"), row.get("fill_model")): row for row in drawdown_rows}
        asset_rows: list[dict[str, str]] = []
        for asset in summary.get("active_assets", []):
            for model_key in model_keys:
                model, fill_model = model_key.split(":", 1)
                fill = fill_by_key.get((asset, model, fill_model), {})
                markout = markout_by_key.get((asset, model, fill_model, 60), {})
                net = net_by_key.get((asset, model, fill_model, 60), {})
                equity = equity_by_key.get((model, fill_model), {})
                drawdown = drawdown_by_key.get((model, fill_model), {})
                asset_rows.append(
                    {
                        "asset": str(asset),
                        "model": model,
                        "fill_model": fill_model,
                        "fills": fill.get("canonical_fill_count", "0"),
                        "canonical_volume_usdc": fill.get("canonical_notional_usdc", "0"),
                        "portfolio_volume_usdc": fill.get("portfolio_notional_usdc", "0"),
                        "markout_60s_complete": markout.get("complete_count", "0"),
                        "markout_60s_missing": markout.get("explicit_unavailable_count", "0"),
                        "net_capture_60s_samples": net.get("sample_count", "0"),
                        "net_capture_60s_bps": net.get("net_capture_bps_mean", ""),
                        "net_capture_60s_status": net.get("status", "INSUFFICIENT_SAMPLE"),
                        "equity_usdc": equity.get("current_shadow_equity_usdc", ""),
                        "equity_status": equity.get("status", ""),
                        "drawdown_usdc": drawdown.get("drawdown_usdc", ""),
                        "drawdown_pct": drawdown.get("drawdown_pct", ""),
                    }
                )
        run_id = str(summary.get("run_id")) if summary.get("run_id") else None
        expected_run_id = context_run_id(current_dashboard_context())
        panel_status = "PASS" if not expected_run_id or not run_id or expected_run_id == run_id else "STALE_PANEL_DATA"
        return {
            "available": True,
            "summary_path": str(summary_path),
            "run_id": run_id,
            "current_context_run_id": expected_run_id,
            "panel_status": panel_status,
            "summary": summary,
            "asset_rows": asset_rows,
            "rules": rule_rows,
            "connection_recovery": recovery_rows,
            "panel_audit": panel_rows,
        }

    def validation6h_payload() -> dict:
        nonlocal validation6h_cache
        now = time.time()
        if validation6h_cache is not None and now - validation6h_cache[0] < 5.0:
            return validation6h_cache[1]
        state_path, report_path, metadata_path, telemetry_path = validation6h_context()
        report_base = report_path.parent
        metadata = _safe_json(metadata_path, {})
        state = _safe_json(state_path, {})
        report = _safe_json(report_path, {})
        assets = metadata.get("assets") or state.get("active_assets") or []
        started = _finite_float(state.get("started_at")) or now
        ended = _finite_float(state.get("ended_at")) or now
        elapsed = max(0.0, ended - started)
        planned_end = _finite_float(metadata.get("planned_end_epoch")) or (
            started + (_finite_float(metadata.get("duration_seconds")) or 21600.0)
        )
        runner_pid = state.get("pid") or metadata.get("pid")
        last_update_age = (
            max(0.0, time.time() - float(state["last_update"]))
            if state.get("last_update") is not None
            else None
        )
        raw_status = str(state.get("status", "NOT_STARTED"))
        runner_alive = _process_exists(runner_pid) if runner_pid is not None else False
        display_status = (
            "RUNNER_MISSING"
            if raw_status == "RUNNING" and (not runner_alive or (last_update_age is not None and last_update_age > 30.0))
            else raw_status
        )
        trade_activity = {row.get("asset"): row for row in csv_rows(report_base / "trade_activity.csv")}
        feed_audit = {row.get("asset"): row for row in csv_rows(report_base / "trade_feed_audit.csv")}
        churn = {row.get("asset"): row for row in csv_rows(report_base / "quote_churn.csv") if row.get("model") == "PRIORITY_FAILOVER:CONSERVATIVE"}
        usage = {row.get("asset"): row for row in csv_rows(report_base / "reference_usage.csv")}
        uptime = {row.get("asset"): row for row in csv_rows(report_base / "quote_uptime.csv") if row.get("control") == "PRIORITY_FAILOVER"}
        volumes = {row.get("asset"): row for row in csv_rows(report_base / "maker_volume.csv") if row.get("model") == "PRIORITY_FAILOVER:CONSERVATIVE"}
        toxicity = {
            (row.get("asset"), row.get("horizon_seconds")): row
            for row in csv_rows(report_base / "toxicity.csv")
            if row.get("model") == "PRIORITY_FAILOVER:CONSERVATIVE"
        }
        net_capture = {
            row.get("asset"): row
            for row in csv_rows(report_base / "net_capture.csv")
            if row.get("model") == "PRIORITY_FAILOVER:CONSERVATIVE" and row.get("horizon_seconds") == "30"
        }
        per_asset_report = report.get("per_asset") or {}

        rows = []
        connection = None
        has_minute_aggregates = False
        aggregate_totals: dict[str, dict[str, float]] = {}
        aggregate_latest: dict[str, dict] = {}
        aggregate_row_count = 0
        aggregate_observation_count = 0
        aggregate_detail_count = 0
        aggregate_compressed_count = 0
        if telemetry_path.exists():
            try:
                connection = sqlite3.connect(f"file:{telemetry_path}?mode=ro", uri=True)
                connection.row_factory = sqlite3.Row
                has_minute_aggregates = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='minute_aggregates'"
                ).fetchone() is not None
                if has_minute_aggregates:
                    for asset in assets:
                        aggregate_rows = connection.execute(
                            "SELECT * FROM minute_aggregates WHERE asset=? ORDER BY timestamp_minute",
                            (asset,),
                        ).fetchall()
                        if not aggregate_rows:
                            continue
                        totals = {
                            "trade_count": 0.0,
                            "notional": 0.0,
                            "conservative_fills": 0.0,
                            "touch_fills": 0.0,
                            "maker_volume": 0.0,
                            "creates": 0.0,
                            "replaces": 0.0,
                            "cancels": 0.0,
                            "quote_observations": 0.0,
                            "quote_uptime_observations": 0.0,
                        }
                        for aggregate in aggregate_rows:
                            metrics = _json_mapping(aggregate["model_metrics_json"])
                            priority = _json_mapping(metrics.get("PRIORITY_FAILOVER:CONSERVATIVE"))
                            touch = _json_mapping(metrics.get("PRIORITY_FAILOVER:TOUCH_SENSITIVITY"))
                            totals["trade_count"] += _finite_float(aggregate["derive_trade_count"]) or 0.0
                            totals["notional"] += _finite_float(aggregate["derive_trade_notional"]) or 0.0
                            totals["conservative_fills"] += _finite_float(priority.get("fills")) or _finite_float(aggregate["conservative_fill_count"]) or 0.0
                            totals["touch_fills"] += _finite_float(touch.get("fills")) or _finite_float(aggregate["touch_fill_count"]) or 0.0
                            totals["maker_volume"] += _finite_float(priority.get("maker_volume")) or _finite_float(aggregate["maker_volume"]) or 0.0
                            totals["creates"] += _finite_float(priority.get("creates")) or 0.0
                            totals["replaces"] += _finite_float(priority.get("replaces")) or 0.0
                            totals["cancels"] += _finite_float(priority.get("cancels")) or 0.0
                            totals["quote_observations"] += _finite_float(aggregate["quote_observations"]) or 0.0
                            totals["quote_uptime_observations"] += _finite_float(aggregate["quote_uptime_observations"]) or 0.0
                            aggregate_row_count += 1
                            aggregate_observation_count += int(aggregate["observation_count"] or 0)
                            aggregate_detail_count += int(aggregate["decision_detail_count"] or 0)
                            aggregate_compressed_count += int(aggregate["decision_compressed_count"] or 0)
                            aggregate_latest[asset] = dict(aggregate)
                        aggregate_totals[asset] = totals
            except (OSError, sqlite3.Error, TypeError, ValueError):
                connection = None
                has_minute_aggregates = False
                aggregate_totals.clear()
                aggregate_latest.clear()
                aggregate_row_count = 0
                aggregate_observation_count = 0
                aggregate_detail_count = 0
                aggregate_compressed_count = 0
        aggregate_backed = has_minute_aggregates and aggregate_row_count > 0
        try:
            for asset in assets:
                latest_payload = state.get("latest_decisions", {}).get(asset) or {}
                trade_count = 0
                notional = 0.0
                last_trade = None
                creates = replaces = cancels = conservative_fills = touch_fills = None
                maker_volume = 0.0
                aggregate_quote_observations = 0.0
                aggregate_quote_uptime_observations = 0.0
                if connection is not None:
                    last_trade = connection.execute("SELECT MAX(COALESCE(exchange_timestamp, timestamp)) FROM trades WHERE asset=? AND source='derive'", (asset,)).fetchone()[0]
                    if aggregate_backed and asset in aggregate_totals:
                        totals = aggregate_totals[asset]
                        trade_count = int(totals["trade_count"])
                        notional = totals["notional"]
                        conservative_fills = int(totals["conservative_fills"])
                        touch_fills = int(totals["touch_fills"])
                        maker_volume = totals["maker_volume"]
                        creates = int(totals["creates"])
                        replaces = int(totals["replaces"])
                        cancels = int(totals["cancels"])
                        aggregate_quote_observations = totals["quote_observations"]
                        aggregate_quote_uptime_observations = totals["quote_uptime_observations"]
                    else:
                        # Historical runs without minute aggregates are kept usable without
                        # scanning their potentially million-row action table on every refresh.
                        trade_count = int(connection.execute("SELECT COUNT(*) FROM trades WHERE asset=? AND source='derive'", (asset,)).fetchone()[0])
                        notional = float(connection.execute("SELECT COALESCE(SUM(CAST(amount AS REAL) * CAST(price AS REAL)), 0) FROM trades WHERE asset=? AND source='derive'", (asset,)).fetchone()[0] or 0)
                        conservative_fills = int(connection.execute("SELECT COUNT(*) FROM fills WHERE asset=? AND model='PRIORITY_FAILOVER:CONSERVATIVE'", (asset,)).fetchone()[0])
                        touch_fills = int(connection.execute("SELECT COUNT(*) FROM fills WHERE asset=? AND model='PRIORITY_FAILOVER:TOUCH_SENSITIVITY'", (asset,)).fetchone()[0])
                        maker_volume = float(connection.execute("SELECT COALESCE(SUM(CAST(amount AS REAL) * CAST(fill_price AS REAL)), 0) FROM fills WHERE asset=? AND model='PRIORITY_FAILOVER:CONSERVATIVE'", (asset,)).fetchone()[0] or 0)
                latest_aggregate = aggregate_latest.get(asset, {})
                bid = latest_payload.get("derive_bid")
                ask = latest_payload.get("derive_ask")
                derive_mid = (float(bid) + float(ask)) / 2 if bid is not None and ask is not None else latest_aggregate.get("derive_mid_median")
                spread = latest_payload.get("derive_spread_bps") or latest_aggregate.get("derive_spread_bps_median")
                selected_reference = (state.get("latest_consensus", {}).get(asset) or {}).get("selected_reference") or latest_payload.get("selected_reference") or _dominant(latest_aggregate.get("selected_reference_occupancy_json"))
                if selected_reference in {"UNSELECTED", "UNSPECIFIED"}:
                    selected_reference = None
                reference_fair_value = latest_payload.get("reference_fair_value") or latest_payload.get("fair_value") or latest_aggregate.get("reference_fair_value_median")
                basis = latest_payload.get("basis_bps") or latest_aggregate.get("basis_bps_median")
                reference_age = latest_payload.get("reference_data_age_seconds")
                if reference_age is None and latest_aggregate.get("last_timestamp") is not None:
                    reference_age = max(0.0, now - float(latest_aggregate["last_timestamp"]))
                hours = max(elapsed / 3600.0, 1 / 3600.0)
                activity = trade_activity.get(asset, {})
                feed = feed_audit.get(asset, {})
                volume = volumes.get(asset, {})
                report_asset = per_asset_report.get(asset, {})
                quote_uptime = (uptime.get(asset) or {}).get("quoteable_time_pct")
                if quote_uptime is None and aggregate_quote_observations:
                    quote_uptime = aggregate_quote_uptime_observations / aggregate_quote_observations * 100.0
                rows.append(
                    {
                        "asset": asset,
                        "derive_mid": derive_mid,
                        "spread_bps": spread,
                        "derive_trades_per_hour": activity.get("trades_per_hour") or trade_count / hours,
                        "trade_notional_per_hour": activity.get("trade_notional_per_hour") or notional / hours,
                        "trade_age_seconds": max(0.0, time.time() - float(last_trade)) if last_trade else None,
                        "trade_feed_classification": feed.get("classification") or report_asset.get("feed_classification") or ("RUNNING" if display_status == "RUNNING" else display_status),
                        "current_reference": selected_reference,
                        "reference_age_seconds": reference_age,
                        "shadow_bid": latest_payload.get("desired_bid"),
                        "shadow_ask": latest_payload.get("desired_ask"),
                        "create_count": creates,
                        "replace_count": replaces,
                        "cancel_count": cancels,
                        "conservative_fills": conservative_fills,
                        "touch_fills": touch_fills,
                        "maker_volume": volume.get("maker_volume") or maker_volume,
                        "maker_volume_per_hour": volume.get("maker_volume_per_hour") or maker_volume / hours,
                        "markout_5s": (toxicity.get((asset, "5")) or {}).get("median_markout_bps"),
                        "markout_30s": (toxicity.get((asset, "30")) or {}).get("median_markout_bps"),
                        "markout_60s": (toxicity.get((asset, "60")) or {}).get("median_markout_bps"),
                        "net_capture": (net_capture.get(asset) or {}).get("net_capture_proxy_bps_mean"),
                        "quote_uptime": quote_uptime,
                        "reference_fair_value": reference_fair_value,
                        "basis_bps": basis,
                        "aggregate_backed": aggregate_backed,
                    }
                )
        finally:
            if connection is not None:
                connection.close()
        health = state.get("source_health") or {}
        reference_rows = []
        for asset in assets:
            latest_aggregate = aggregate_latest.get(asset, {})
            source = (state.get("latest_consensus", {}).get(asset) or {}).get("selected_reference") or _dominant(latest_aggregate.get("selected_reference_occupancy_json"))
            if source in {"UNSELECTED", "UNSPECIFIED"}:
                source = None
            source_health = (health.get(asset, {}).get(source or "") or {}).get("health")
            if source_health is None and source:
                health_counts = _json_mapping(latest_aggregate.get("reference_health_counts_json"))
                source_health = _dominant({key.split(":", 1)[1]: value for key, value in health_counts.items() if key.startswith(f"{source}:")})
            reference_rows.append(
                {
                    "asset": asset,
                    "binance_uptime": (usage.get(asset) or {}).get("binance_selected_time_pct") or (100 if source == "binance" else None),
                    "bybit_failover_time": (usage.get(asset) or {}).get("bybit_selected_time_pct"),
                    "okx_failover_time": (usage.get(asset) or {}).get("okx_selected_time_pct"),
                    "source_switches": (usage.get(asset) or {}).get("reference_switches"),
                    "reference_pauses": (usage.get(asset) or {}).get("paused_time_pct"),
                    "disagreement_pauses": (usage.get(asset) or {}).get("reference_disagreement_pauses"),
                    "current_reference": source,
                    "current_health": source_health,
                }
            )
        resolved_run_id = context_run_id((state_path, report_path, metadata_path, telemetry_path))
        payload = {
            "run_id": resolved_run_id,
            "status": display_status,
            "state_status": raw_status,
            "runner_pid": runner_pid,
            "runner_alive": runner_alive,
            "last_update_age_seconds": last_update_age,
            "state_path": str(state_path),
            "report_base": str(report_base),
            "telemetry_path": str(telemetry_path),
            "data_basis": "MINUTE_AGGREGATES_PLUS_PERMANENT_EVENTS" if aggregate_backed else "RAW_TELEMETRY",
            "aggregate_backed": aggregate_backed,
            "aggregate_minutes": aggregate_row_count,
            "aggregate_observations": aggregate_observation_count,
            "aggregate_detail_rows": aggregate_detail_count,
            "aggregate_compressed_rows": aggregate_compressed_count,
            "elapsed_seconds": elapsed,
            "remaining_seconds": max(0.0, planned_end - now) if raw_status == "RUNNING" else 0.0,
            "start_time_utc": metadata.get("start_time_utc"),
            "planned_end_time_utc": metadata.get("planned_end_time_utc"),
            "mainnet_armed": state.get("mainnet_armed", False),
            "real_orders": state.get("real_orders", 0),
            "real_positions": state.get("real_positions", 0),
            "assets": rows,
            "churn": list(churn.values()),
            "reference": reference_rows,
        }
        validation6h_cache = (now, payload)
        return payload

    def quote_fill_diagnostic_payload() -> dict:
        """Read the latest measurement snapshot without generating files on refresh."""

        state_path, report_path, metadata_path, telemetry_path = validation6h_context()
        metadata = _safe_json(metadata_path, {})
        state = _safe_json(state_path, {})
        run_id = metadata.get("run_id") or (telemetry_path.parent.name if telemetry_path.parent != validation6h_log_base else None)
        candidates = []
        if run_id:
            candidates.append(quote_fill_diagnostic_base / str(run_id) / "diagnostic_summary.json")
        configured = metadata.get("quote_fill_diagnostic_summary")
        if configured:
            candidates.append(Path(str(configured)))
        candidates.append(report_path.parent / "quote_fill_diagnostic" / "diagnostic_summary.json")
        for candidate in candidates:
            summary = _safe_json(candidate, {})
            if summary:
                summary["available"] = True
                summary["summary_path"] = str(candidate)
                return summary
        storage_files = [telemetry_path, Path(str(telemetry_path) + "-wal"), Path(str(telemetry_path) + "-shm")]
        current_size = sum(path.stat().st_size for path in storage_files if path.exists())
        try:
            usage = shutil.disk_usage(telemetry_path.parent)
            storage = {
                "free_disk_gb": round(usage.free / (1024 ** 3), 6),
                "current_run_size_bytes": current_size,
                "storage_state": "UNKNOWN",
                "raw_buffer_bytes_in_db_wal_shm": current_size,
            }
        except OSError:
            storage = {"current_run_size_bytes": current_size, "storage_state": "UNKNOWN"}
        return {
            "available": False,
            "run_id": run_id,
            "run_status": state.get("status", "NOT_STARTED"),
            "diagnostic_status": "WAITING_FOR_SNAPSHOT",
            "assets": [],
            "storage_health": [storage],
            "reason": "DIAGNOSTIC_SNAPSHOT_NOT_AVAILABLE",
        }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/api/state":
                context = current_dashboard_context()
                state_path = context[0]
                payload = dict(_safe_json(
                    state_path,
                    {"mode": "MAINNET_SHADOW", "mainnet_armed": False, "real_orders": 0, "real_positions": 0, "mappings": {}},
                ))
                payload.setdefault("run_id", context_run_id(context))
                payload["dashboard_context_run_id"] = context_run_id(context)
                self._send_json(payload)
            elif path == "/api/validation6h":
                self._send_json(validation6h_payload())
            elif path == "/api/quote-fill-diagnostic":
                self._send_json(quote_fill_diagnostic_payload())
            elif path == "/api/refresh-research":
                self._send_json(refresh_research_payload())
            elif path == "/api/accounting-repair":
                self._send_json(accounting_repair_payload())
            elif path == "/api/report":
                context = current_dashboard_context()
                state_path, report_path, _, _ = context
                if report_path == validation6h_context()[1] and not report_path.exists():
                    state = _safe_json(state_path, {})
                    self._send_json(
                        {
                            "run_id": context_run_id(context),
                            "status": state.get("status", "RUNNING"),
                            "classification": "RUNNING" if state.get("status") == "RUNNING" else "DATA_INSUFFICIENT",
                            "active_assets": state.get("active_assets", []),
                            "reference_priority": state.get("reference_priority", []),
                        }
                    )
                    return
                payload = dict(_safe_json(report_path, {"classification": "NOT_READY_FOR_SMALL_MAINNET_CANARY"}))
                payload.setdefault("run_id", context_run_id(context))
                payload["dashboard_context_run_id"] = context_run_id(context)
                self._send_json(payload)
            elif path == "/api/timeseries":
                query = parse_qs(urlparse(self.path).query)
                asset = (query.get("asset") or [""])[0].upper()
                try:
                    window_seconds = int((query.get("window") or ["300"])[0])
                except ValueError:
                    window_seconds = 300
                self._send_json(self._timeseries(asset, window_seconds))
            elif path == "/api/minute-aggregates":
                query = parse_qs(urlparse(self.path).query)
                asset = (query.get("asset") or [""])[0].upper()
                run = (query.get("run") or ["priority"])[0]
                try:
                    window_seconds = int((query.get("window") or ["3600"])[0])
                except ValueError:
                    window_seconds = 3600
                self._send_json(self._minute_aggregates(asset, window_seconds, run))
            elif path in {"/", "/index.html"}:
                body = index_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

        def _send_json(self, payload: dict) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _minute_aggregates(self, asset: str, window_seconds: int, run: str) -> dict:
            telemetry_path = telemetry_path_for(run)
            base = {
                "asset": asset,
                "run": run,
                "source": "minute_aggregates",
                "available": False,
                "rows": [],
            }
            if not asset or not telemetry_path.exists():
                base["reason"] = "TELEMETRY_UNAVAILABLE"
                return base
            window_seconds = max(60, min(window_seconds, 86400))
            cutoff = time.time() - window_seconds
            try:
                connection = sqlite3.connect(f"file:{telemetry_path}?mode=ro", uri=True, timeout=0.25)
                connection.row_factory = sqlite3.Row
                table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='minute_aggregates'"
                ).fetchone()
                if table is None:
                    base["reason"] = "RUN_PREDATES_MINUTE_AGGREGATES"
                    return base
                rows = connection.execute(
                    "SELECT * FROM minute_aggregates "
                    "WHERE asset = ? AND timestamp_minute >= ? "
                    "ORDER BY timestamp_minute DESC LIMIT ?",
                    (asset, int(cutoff), min(2000, int(window_seconds / 60) + 2)),
                ).fetchall()
            except (OSError, sqlite3.Error):
                base["reason"] = "TELEMETRY_READ_UNAVAILABLE"
                return base
            finally:
                if "connection" in locals():
                    connection.close()

            result = []
            for row in reversed(rows):
                observation_count = row["observation_count"]
                quote_observations = row["quote_observations"]
                action_counts = _json_mapping(row["action_counts_json"])
                result.append(
                    {
                        "timestamp_minute": row["timestamp_minute"],
                        "first_timestamp": row["first_timestamp"],
                        "last_timestamp": row["last_timestamp"],
                        "observation_count": observation_count,
                        "feature_snapshot_count": row["feature_snapshot_count"],
                        "derive_mid": row["derive_mid_median"] if "derive_mid_median" in row.keys() else _average(row["derive_mid_sum"], observation_count),
                        "spread_bps": row["derive_spread_bps_median"] if "derive_spread_bps_median" in row.keys() else _average(row["derive_spread_bps_sum"], observation_count),
                        "spread_bps_p90": row["derive_spread_bps_p90"],
                        "reference_fair_value": row["reference_fair_value_median"] if "reference_fair_value_median" in row.keys() else _average(row["reference_fair_value_sum"], observation_count),
                        "basis_bps": row["basis_bps_median"] if "basis_bps_median" in row.keys() else _average(row["basis_bps_sum"], observation_count),
                        "selected_reference": _dominant(row["selected_reference_occupancy_json"]),
                        "reference_observations": row["reference_observations"],
                        "reference_healthy_observations": row["reference_healthy_observations"],
                        "market_mode": _dominant(row["market_mode_occupancy_json"]),
                        "direction": _dominant(row["direction_occupancy_json"]),
                        "derive_trades": row["derive_trade_count"],
                        "derive_trade_notional": row["derive_trade_notional"],
                        "conservative_fills": row["conservative_fill_count"],
                        "touch_fills": row["touch_fill_count"],
                        "maker_volume": row["maker_volume"],
                        "creates": row["creates"],
                        "replaces": row["replaces"],
                        "cancels": row["cancels"],
                        "holds": row["holds"],
                        "blocks": row["blocks"],
                        "quote_uptime_pct": (
                            float(row["quote_uptime_observations"]) / int(quote_observations) * 100
                            if int(quote_observations or 0)
                            else None
                        ),
                        "inventory_notional_max": row["inventory_notional_max"],
                        "pnl_proxy_delta": row["pnl_proxy_delta"],
                        "error_count": row["error_count"],
                        "decision_detail_count": row["decision_detail_count"],
                        "decision_compressed_count": row["decision_compressed_count"],
                        "raw_window_seconds": row["raw_window_seconds"],
                        "action_counts": action_counts,
                    }
                )
            base.update({"available": True, "window_seconds": window_seconds, "rows": result})
            return base

        def _timeseries(self, asset: str, window_seconds: int = 300) -> dict:
            telemetry_path = telemetry_path_for("priority")
            if not asset or not telemetry_path.exists():
                return {"asset": asset, "rows": []}
            window_seconds = max(300, min(window_seconds, 3600))
            limit = max(120, min(20000, window_seconds * 4 + 20))
            try:
                connection = sqlite3.connect(f"file:{telemetry_path}?mode=ro", uri=True)
                rows = connection.execute(
                    "SELECT timestamp, payload_json FROM decisions WHERE asset = ? ORDER BY timestamp DESC LIMIT ?",
                    (asset, limit),
                ).fetchall()
                connection.close()
            except (OSError, sqlite3.Error):
                return {"asset": asset, "rows": []}
            result = []
            for timestamp, payload_json in reversed(rows):
                try:
                    payload = json.loads(payload_json)
                except (TypeError, json.JSONDecodeError):
                    payload = {}
                bid, ask = payload.get("derive_bid"), payload.get("derive_ask")
                derive_mid = None
                try:
                    derive_mid = (float(bid) + float(ask)) / 2
                except (TypeError, ValueError):
                    pass
                result.append(
                    {
                        "timestamp": timestamp,
                        "derive_mid": derive_mid,
                        "fair_value": payload.get("fair_value"),
                        "dispersion_bps": payload.get("reference_dispersion_bps"),
                    }
                )
            return {"asset": asset, "rows": result}

        def log_message(self, *_args) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the read-only Derive MM dashboard")
    parser.add_argument("--port", type=int, default=8770)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args(argv)
    root = Path.cwd()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(root))
    print(f"dashboard listening at http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
