"""Materialize compact shadow telemetry into human-readable and CSV reports."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .lead_lag import estimate_lead_lag
from .models import AssetMapping, json_safe
from .telemetry import TelemetryStore

REPORT_FILES = (
    "asset_reference_mapping.csv",
    "derive_trading_rules.csv",
    "capital_compatibility.csv",
    "fair_value_quality.csv",
    "basis_statistics.csv",
    "market_state_occupancy.csv",
    "direction_state_occupancy.csv",
    "inventory_state_occupancy.csv",
    "quote_activity.csv",
    "fills.csv",
    "binance_markouts.csv",
    "derive_markouts.csv",
    "toxicity.csv",
    "net_capture.csv",
    "asset_opportunity_scores.csv",
    "portfolio_exposure.csv",
    "portfolio_performance.csv",
    "data_health.csv",
    "latency.csv",
    "asset_comparison.csv",
    "lead_lag.csv",
    "protection_effectiveness.csv",
    "control_comparison.csv",
    "final_multi_asset_shadow_report.md",
    "final_multi_asset_shadow_report.json",
)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    rows = list(rows)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else ["status"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _json_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        result.append({"timestamp": row["timestamp"], "asset": row["asset"], **payload})
    return result


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def finalize_reports(
    *,
    config: Any,
    mappings: dict[str, AssetMapping],
    mapping_report: dict[str, Any],
    telemetry: TelemetryStore,
    run_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report_dir = Path(config.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    decisions = _json_decisions(telemetry.rows("decisions"))
    actions = telemetry.rows("actions")
    fills = telemetry.rows("fills")
    markouts = telemetry.rows("markouts")
    assets = [asset.symbol for asset in config.enabled_assets]

    mapping_rows = []
    rule_rows = []
    capital_rows = []
    for asset in assets:
        mapping = mappings.get(asset)
        mapping_rows.append(
            {
                "asset": asset,
                "derive_instrument": mapping.derive_instrument if mapping else None,
                "derive_pair": mapping.derive_pair if mapping else None,
                "binance_reference": mapping.binance_symbol if mapping else None,
                "reference_type": mapping.reference_type if mapping else "BINANCE_USDM_PERPETUAL",
                "reference_available": mapping.reference_available if mapping else False,
                "status": mapping.reason if mapping else "UNMAPPED",
            }
        )
        rules = mapping.rules if mapping else None
        rule_rows.append(
            {
                "asset": asset,
                "instrument": rules.instrument_name if rules else None,
                "tick_size": str(rules.tick_size) if rules else None,
                "amount_step": str(rules.amount_step) if rules else None,
                "minimum_amount": str(rules.minimum_amount) if rules else None,
                "minimum_notional": str(rules.minimum_notional) if rules else None,
                "maker_fee_bps": str(rules.maker_fee_bps) if rules and rules.maker_fee_bps is not None else None,
            }
        )
        min_notional = (rules.minimum_amount * rules.tick_size if rules else None)
        capital_rows.append(
            {
                "asset": asset,
                "minimum_order_notional_proxy": str(min_notional) if min_notional is not None else None,
                "capital_usdc": str(config.capital_usdc),
                "capital_pct": str(min_notional / config.capital_usdc * 100) if min_notional else None,
                "capital_compatible": bool(min_notional is not None and min_notional <= config.max_single_order_notional),
            }
        )
    _write_csv(report_dir / "asset_reference_mapping.csv", mapping_rows)
    _write_csv(report_dir / "derive_trading_rules.csv", rule_rows)
    _write_csv(report_dir / "capital_compatibility.csv", capital_rows)

    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in decisions:
        by_asset[row["asset"]].append(row)
    fair_rows = []
    basis_rows = []
    market_rows = []
    direction_rows = []
    inventory_rows = []
    health_rows = []
    latency_rows = []
    activity_rows = []
    opportunity_rows = []
    comparison_rows = []
    lead_lag_rows = []
    protection_rows = []
    control_rows = []
    for asset in assets:
        rows = by_asset.get(asset, [])
        fair_rows.append({"asset": asset, "observations": len(rows), "fair_value_ready": sum(bool(row.get("fair_value")) for row in rows)})
        basis_values = [float(row["basis_bps"]) for row in rows if row.get("basis_bps") is not None]
        basis_rows.append({"asset": asset, "observations": len(basis_values), "median_basis_bps": _mean(basis_values), "max_abs_basis_bps": max((abs(value) for value in basis_values), default=None)})
        market_counts: dict[str, int] = defaultdict(int)
        direction_counts: dict[str, int] = defaultdict(int)
        inventory_counts: dict[str, int] = defaultdict(int)
        for row in rows:
            market_counts[str(row.get("market_mode", "UNKNOWN"))] += 1
            direction_counts[str(row.get("direction", "UNKNOWN"))] += 1
            inventory_counts[str(row.get("inventory_mode", "UNKNOWN"))] += 1
        market_rows.extend({"asset": asset, "state": state, "count": count} for state, count in sorted(market_counts.items()))
        direction_rows.extend({"asset": asset, "state": state, "count": count} for state, count in sorted(direction_counts.items()))
        inventory_rows.extend({"asset": asset, "state": state, "count": count} for state, count in sorted(inventory_counts.items()))
        blocked = sum(1 for row in rows if row.get("block_reason"))
        activity_rows.append({"asset": asset, "decision_rows": len(rows), "blocked_rows": blocked, "active_rows": len(rows) - blocked})
        health_rows.append({"asset": asset, "reference_unavailable": sum(row.get("data_health") == "REFERENCE_UNAVAILABLE" for row in rows), "reference_stale": sum(row.get("data_health") == "REFERENCE_STALE" for row in rows), "derive_stale": sum(row.get("data_health") == "DERIVE_STALE" for row in rows)})
        latency_rows.append({"asset": asset, "observations": len(rows), "mean_processing_latency_ms": _mean([float(row["processing_latency_ms"]) for row in rows if row.get("processing_latency_ms") is not None])})
        asset_fills = [row for row in fills if row["asset"] == asset]
        asset_markouts = [row for row in markouts if row["asset"] == asset and int(row["horizon_seconds"]) == 30]
        opportunity_rows.append({"asset": asset, "score": _mean([float(row["opportunity_score"]) for row in rows if row.get("opportunity_score") is not None]), "observations": len(rows)})
        comparison_rows.append({"asset": asset, "derive_median_spread_bps": _mean([float(row["derive_spread_bps"]) for row in rows if row.get("derive_spread_bps") is not None]), "fills": len(asset_fills), "maker_volume": sum(float(row["amount"]) * float(row["fill_price"]) for row in asset_fills), "30s_binance_markout_bps": _mean([float(row["binance_markout_bps"]) for row in asset_markouts]), "quote_churn": sum(1 for row in actions if row["asset"] == asset and row["action"] in {"CREATE", "CANCEL"})})
        reference_points = [
            (float(row["timestamp"]), price)
            for row in rows
            if (price := _decimal_or_none(row.get("binance_mid"))) is not None
        ]
        derive_points = []
        for row in rows:
            bid = _decimal_or_none(row.get("derive_bid"))
            ask = _decimal_or_none(row.get("derive_ask"))
            if bid is not None and ask is not None:
                derive_points.append((float(row["timestamp"]), (bid + ask) / Decimal("2")))
        for lag_row in estimate_lead_lag(reference_points, derive_points):
            lead_lag_rows.append({"asset": asset, **lag_row})
        protected_count = sum(bool(row.get("divergence_protected")) for row in rows)
        fast_move_count = sum(bool(row.get("fast_move_protected")) for row in rows)
        paused_count = sum(str(row.get("market_mode")) == "PAUSED" for row in rows)
        protection_rows.append(
            {
                "asset": asset,
                "observations": len(rows),
                "divergence_protected_rows": protected_count,
                "fast_move_protected_rows": fast_move_count,
                "paused_rows": paused_count,
                "blocked_rows": sum(bool(row.get("block_reason")) for row in rows),
                "protection_rate": protected_count / len(rows) if rows else None,
            }
        )
        control_rows.append(
            {
                "asset": asset,
                "derive_only_control": "same Derive timestamps; no Binance reference",
                "derive_observations": len(derive_points),
                "binance_reference_observations": len(reference_points),
                "status": "COMPARABLE_TIMESTAMPS" if reference_points and derive_points else "INSUFFICIENT_OBSERVATIONS",
            }
        )
    _write_csv(report_dir / "fair_value_quality.csv", fair_rows)
    _write_csv(report_dir / "basis_statistics.csv", basis_rows)
    _write_csv(report_dir / "market_state_occupancy.csv", market_rows)
    _write_csv(report_dir / "direction_state_occupancy.csv", direction_rows)
    _write_csv(report_dir / "inventory_state_occupancy.csv", inventory_rows)
    _write_csv(report_dir / "quote_activity.csv", activity_rows)
    _write_csv(report_dir / "fills.csv", fills)
    _write_csv(
        report_dir / "binance_markouts.csv",
        [
            {
                "fill_timestamp": row["fill_timestamp"],
                "horizon_seconds": row["horizon_seconds"],
                "asset": row["asset"],
                "side": row["side"],
                "reference_price": row["reference_price"],
                "binance_markout_bps": row["binance_markout_bps"],
                "model": row["model"],
            }
            for row in markouts
        ],
        ["fill_timestamp", "horizon_seconds", "asset", "side", "reference_price", "binance_markout_bps", "model"],
    )
    _write_csv(
        report_dir / "derive_markouts.csv",
        [
            {
                "fill_timestamp": row["fill_timestamp"],
                "horizon_seconds": row["horizon_seconds"],
                "asset": row["asset"],
                "side": row["side"],
                "derive_mid": row["derive_mid"],
                "derive_markout_bps": row["derive_markout_bps"],
                "model": row["model"],
            }
            for row in markouts
        ],
        ["fill_timestamp", "horizon_seconds", "asset", "side", "derive_mid", "derive_markout_bps", "model"],
    )
    _write_csv(report_dir / "toxicity.csv", [{"asset": row["asset"], "model": row["model"], "horizon_seconds": row["horizon_seconds"], "binance_markout_bps": row["binance_markout_bps"], "derive_markout_bps": row["derive_markout_bps"]} for row in markouts])
    _write_csv(report_dir / "net_capture.csv", [{"asset": row["asset"], "model": row["model"], "horizon_seconds": row["horizon_seconds"], "quoted_edge_bps": next((fill["quoted_edge_bps"] for fill in fills if abs(float(fill["timestamp"]) - float(row["fill_timestamp"])) < 1e-6 and fill["asset"] == row["asset"] and fill["model"] == row["model"]), None), "maker_fee_bps": next((fill["maker_fee_bps"] for fill in fills if abs(float(fill["timestamp"]) - float(row["fill_timestamp"])) < 1e-6 and fill["asset"] == row["asset"] and fill["model"] == row["model"]), None), "binance_markout_bps": row["binance_markout_bps"], "derive_markout_bps": row["derive_markout_bps"]} for row in markouts])
    _write_csv(report_dir / "asset_opportunity_scores.csv", opportunity_rows)
    _write_csv(report_dir / "portfolio_exposure.csv", [{"asset": asset, "position_notional": None} for asset in assets])
    _write_csv(report_dir / "portfolio_performance.csv", [{"model": name, "fills": model_count} for name, model_count in _model_fill_counts(fills).items()])
    _write_csv(report_dir / "data_health.csv", health_rows)
    _write_csv(report_dir / "latency.csv", latency_rows)
    _write_csv(report_dir / "asset_comparison.csv", comparison_rows)
    _write_csv(report_dir / "lead_lag.csv", lead_lag_rows, ["asset", "lag_seconds", "return_horizon_seconds", "observations", "correlation", "status"])
    _write_csv(report_dir / "protection_effectiveness.csv", protection_rows, ["asset", "observations", "divergence_protected_rows", "fast_move_protected_rows", "paused_rows", "blocked_rows", "protection_rate"])
    _write_csv(report_dir / "control_comparison.csv", control_rows, ["asset", "derive_only_control", "derive_observations", "binance_reference_observations", "status"])

    safety = config.public_safety()
    counters = {
        "decision_rows": len(decisions),
        "action_rows": len(actions),
        "fill_rows": len(fills),
        "markout_rows": len(markouts),
        "real_orders": 0,
        "real_positions": 0,
    }
    reference_ready = [asset for asset, mapping in mappings.items() if mapping.valid]
    report = {
        "report_version": "derive-multi-asset-binance-mm-v2",
        "safety": safety,
        "counters": counters,
        "mappings": {asset: json_safe(mapping) for asset, mapping in mappings.items()},
        "reference_ready_assets": reference_ready,
        "control": {
            "DERIVE_ONLY": "same timestamps; fair value equals Derive mid",
            "BINANCE_REFERENCE": "same timestamps; Binance is data-only reference",
            "lead_lag_status": "see lead_lag.csv; no forward fill",
        },
        "classification": "NOT_READY_FOR_SMALL_MAINNET_CANARY",
        "primary_blocker": "No live canary was run during build; review causal shadow evidence and costs before any arming.",
        "run_metadata": run_metadata or {},
        "public_discovery": mapping_report,
    }
    with (report_dir / "final_multi_asset_shadow_report.json").open("w", encoding="utf-8") as handle:
        json.dump(json_safe(report), handle, indent=2, sort_keys=True)
    md = _render_markdown(report, comparison_rows)
    (report_dir / "final_multi_asset_shadow_report.md").write_text(md, encoding="utf-8")
    return report


def _model_fill_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["model"]] += 1
    return dict(counts)


def _render_markdown(report: dict[str, Any], comparison_rows: list[dict[str, Any]]) -> str:
    safety = report["safety"]
    lines = [
        "# Derive Multi-Asset Binance-Reference MM Shadow Report",
        "",
        "## Safety",
        "",
        f"- Environment: **{safety['environment'].upper()}**",
        f"- Mode: **{safety['mode']}**",
        f"- Mainnet armed: **{safety['mainnet_armed']}**",
        f"- Real orders: **{safety['real_orders']}**",
        f"- Real positions: **{safety['real_positions']}**",
        "- Binance execution: **False; data-only reference**",
        "",
        "## Asset comparison",
        "",
        "| Asset | Median spread bps | Fills | Maker volume | 30s markout bps | Churn |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in comparison_rows:
        lines.append(f"| {row['asset']} | {row['derive_median_spread_bps']} | {row['fills']} | {row['maker_volume']} | {row['30s_binance_markout_bps']} | {row['quote_churn']} |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "Shadow values are not realized PnL and public liquidity is not executable economics. "
            "Conservative fills require direction-aware strict trade-through evidence; touch sensitivity is reported separately.",
            "",
            f"Classification: **{report['classification']}**",
            f"Primary blocker: {report['primary_blocker']}",
        ]
    )
    return "\n".join(lines) + "\n"
