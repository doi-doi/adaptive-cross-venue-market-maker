"""Reports for the three-asset strict-priority reference shadow run."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from .markouts import net_capture_proxy_bps
from .models import AssetMapping, json_safe
from .reporting import _decimal, _health_rows, _json_decisions, _mean, _median, _write_csv
from .retained_reporting import (
    aggregate_artifact_rows,
    aggregate_json_counts,
    aggregate_metric_values,
    aggregate_model_metrics,
    aggregate_observation_count,
    has_retained_aggregates,
    latest_aggregate,
    retention_metadata,
    rollup_decision_rows,
)
from .telemetry import TelemetryStore

PRIORITY_REPORT_FILES = (
    "asset_rules.csv",
    "reference_health.csv",
    "reference_selection.csv",
    "reference_failovers.csv",
    "reference_recovery.csv",
    "reference_disagreement.csv",
    "spread_statistics.csv",
    "minute_aggregates.csv",
    "decision_rollups.csv",
    "quote_activity.csv",
    "quote_churn.csv",
    "shadow_fills_conservative.csv",
    "shadow_fills_touch.csv",
    "reference_markouts.csv",
    "derive_markouts.csv",
    "net_capture.csv",
    "model_comparison.csv",
    "asset_comparison.csv",
    "portfolio_exposure.csv",
    "final_report.md",
    "final_report.json",
)

_FRESH = {"HEALTHY", "DEGRADED"}
_DISABLED_ASSETS = ("CC", "SOL", "LINK", "BNB", "HYPE")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True), encoding="utf-8")


def _control(model: str) -> str:
    return str(model).split(":", 1)[0]


def _priority_rows(rows: list[dict[str, Any]], model: str | None = None) -> list[dict[str, Any]]:
    return [
        row for row in rows
        if _control(str(row.get("model", ""))) == "PRIORITY_FAILOVER"
        and (model is None or row.get("model") == model)
    ]


def _latest(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return max(rows, key=lambda row: float(row.get("timestamp", 0))) if rows else {}


def _retained_decisions(
    raw_decisions: list[dict[str, Any]],
    rollups: list[dict[str, Any]],
    aggregates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if has_retained_aggregates(aggregates) and rollups:
        return [
            {
                **row,
                **row.get("payload", {}),
                "payload": row.get("payload", {}),
            }
            for row in rollup_decision_rows(rollups)
        ]
    return raw_decisions


def _aggregate_health_counts(aggregates: list[dict[str, Any]], asset: str, venue: str) -> Counter[str]:
    counts = aggregate_json_counts(aggregates, asset, "reference_health_counts_json")
    prefix = f"{venue}:"
    return Counter({key.removeprefix(prefix): value for key, value in counts.items() if key.startswith(prefix)})


def _aggregate_source_counts(aggregates: list[dict[str, Any]], asset: str) -> Counter[str]:
    return aggregate_json_counts(aggregates, asset, "selected_reference_occupancy_json")


def _aggregate_decision_count(aggregates: list[dict[str, Any]], asset: str) -> int:
    return aggregate_observation_count(aggregates, asset)


def _aggregate_block_count(decisions: list[dict[str, Any]], asset: str, control: str) -> int:
    total = 0
    for row in decisions:
        if str(row.get("asset")) != asset:
            continue
        controls = (row.get("payload") or row).get("controls") or {}
        control_payload = controls.get(control) or {}
        plan = control_payload.get("plan") or {}
        consensus = control_payload.get("consensus") or {}
        if plan.get("block_reason") or consensus.get("pause_reason"):
            total += int(row.get("count") or 0)
    return total


def _aggregate_latest_metrics(aggregates: list[dict[str, Any]], asset: str) -> dict[str, Any]:
    return latest_aggregate(aggregates, asset)


def _metric_rows(
    *,
    model: str,
    fills: list[dict[str, Any]],
    markouts: list[dict[str, Any]],
    net_rows: list[dict[str, Any]],
    asset: str | None = None,
) -> dict[str, Any]:
    model_fills = [row for row in fills if row.get("model") == model and (asset is None or row.get("asset") == asset)]
    model_markouts = [row for row in markouts if row.get("model") == model and (asset is None or row.get("asset") == asset)]
    model_net = [row for row in net_rows if row.get("model") == model and (asset is None or row.get("asset") == asset)]
    return {
        "fill_rows": len(model_fills),
        "maker_volume": sum(
            (_decimal(row.get("amount")) or Decimal("0")) * (_decimal(row.get("fill_price")) or Decimal("0"))
            for row in model_fills
        ),
        "markout_30s_bps": _mean(
            row.get("derive_markout_bps")
            for row in model_markouts
            if int(row.get("horizon_seconds", 0)) == 30
        ),
        "markout_60s_bps": _mean(
            row.get("derive_markout_bps")
            for row in model_markouts
            if int(row.get("horizon_seconds", 0)) == 60
        ),
        "net_capture_bps": _mean(row.get("net_capture_proxy_bps") for row in model_net),
    }


def _pipeline_validation(
    *,
    assets: list[str],
    config: Any,
    mappings: dict[str, AssetMapping],
    decisions: dict[str, list[dict[str, Any]]],
    health_rows: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    aggregate_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    aggregate_rows = aggregate_rows or []
    health_by_asset_venue: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in health_rows:
        health_by_asset_venue[(str(row.get("asset")), str(row.get("venue")))].append(row)
    result = []
    for asset in assets:
        rows = decisions.get(asset, [])
        asset_aggregates = [row for row in aggregate_rows if str(row.get("asset")) == asset]
        aggregate_health = _aggregate_health_counts(aggregate_rows, asset, "binance")
        aggregate_metrics = aggregate_model_metrics(
            aggregate_rows,
            asset,
            "PRIORITY_FAILOVER:CONSERVATIVE",
        )
        aggregate_controls = [
            (row.get("payload") or row).get("controls") or {}
            for row in rows
        ]
        priority_actions = [
            row for row in actions
            if row.get("asset") == asset and _control(str(row.get("model", ""))) == "PRIORITY_FAILOVER"
        ]
        mapping = mappings.get(asset)
        checks: dict[str, bool] = {
            "derive_bbo": any(row.get("derive_bid") is not None and row.get("derive_ask") is not None for row in rows)
            or any(row.get("derive_mid_min") is not None for row in asset_aggregates),
            "binance": any(
                row.get("health") in _FRESH
                for row in health_by_asset_venue.get((asset, "binance"), [])
            ) or bool(aggregate_health["HEALTHY"] + aggregate_health["DEGRADED"]),
            "bybit": any(
                row.get("health") in _FRESH
                for row in health_by_asset_venue.get((asset, "bybit"), [])
            ) or bool(_aggregate_health_counts(aggregate_rows, asset, "bybit")["HEALTHY"] + _aggregate_health_counts(aggregate_rows, asset, "bybit")["DEGRADED"]),
            "okx": any(
                row.get("health") in _FRESH
                for row in health_by_asset_venue.get((asset, "okx"), [])
            ) or bool(_aggregate_health_counts(aggregate_rows, asset, "okx")["HEALTHY"] + _aggregate_health_counts(aggregate_rows, asset, "okx")["DEGRADED"]),
            "priority_source_selection": any(
                row.get("selected_reference") in config.reference_priority for row in rows
            ) or bool(sum(_aggregate_source_counts(aggregate_rows, asset).get(source, 0) for source in config.reference_priority)),
            "fair_value": any(row.get("fair_value") is not None for row in rows)
            or any(row.get("reference_fair_value_median") is not None for row in asset_aggregates),
            "basis": any(row.get("basis_bps") is not None for row in rows)
            or any(row.get("basis_bps_median") is not None for row in asset_aggregates),
            "market_mode": any(row.get("market_mode") for row in rows),
            "inventory_mode": any(row.get("inventory_mode") for row in rows),
            "shadow_bid": any(row.get("desired_bid") is not None for row in rows)
            or any(control.get("PRIORITY_FAILOVER", {}).get("plan", {}).get("bid_price") is not None for control in aggregate_controls),
            "shadow_ask": any(row.get("desired_ask") is not None for row in rows)
            or any(control.get("PRIORITY_FAILOVER", {}).get("plan", {}).get("ask_price") is not None for control in aggregate_controls),
            "hold": any(row.get("action") == "HOLD" for row in priority_actions)
            or bool(aggregate_metrics.get("holds", 0)),
            # The existing lifecycle represents a replace as the safe
            # cancel-then-create pair; count its refresh cancel as REPLACE.
            "replace": any(
                row.get("action") == "REPLACE"
                or (row.get("action") == "CANCEL" and row.get("reason") == "REFRESH_NEEDED")
                for row in priority_actions
            ) or bool(aggregate_metrics.get("replaces", 0)),
        }
        checks["mapping"] = bool(mapping and mapping.valid)
        result.append({"asset": asset, **checks, "status": "PASS" if all(checks.values()) else "FAIL"})
    return result


def finalize_priority_reports(
    *,
    config: Any,
    mappings: dict[str, AssetMapping],
    mapping_report: dict[str, Any],
    telemetry: TelemetryStore,
    run_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report_dir = Path(config.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_decisions = _json_decisions(telemetry.rows("decisions"))
    rollup_rows = telemetry.rows("decision_rollups")
    aggregate_rows = telemetry.rows("minute_aggregates")
    decisions = _retained_decisions(raw_decisions, rollup_rows, aggregate_rows)
    actions = telemetry.rows("actions")
    fills = telemetry.rows("fills")
    markouts = telemetry.rows("markouts")
    health_rows = _health_rows(telemetry)
    trades = telemetry.rows("trades")
    retention = retention_metadata(
        aggregates=aggregate_rows,
        rollups=rollup_rows,
        raw_decision_rows=len(raw_decisions),
    )
    assets = [asset.symbol for asset in config.enabled_assets]
    decisions_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in decisions:
        decisions_by_asset[str(row.get("asset"))].append(row)

    mapping_rows = []
    for asset in assets:
        mapping = mappings.get(asset)
        if mapping is None:
            mapping_rows.append({"asset": asset, "active": True, "status": "MAPPING_UNAVAILABLE"})
            continue
        for market in mapping.reference_markets:
            mapping_rows.append(
                {
                    "asset": asset,
                    "active": True,
                    "derive_instrument": mapping.derive_instrument,
                    "derive_pair": mapping.derive_pair,
                    "derive_status": mapping.reason,
                    "venue": market.venue,
                    "connector": market.connector,
                    "symbol": market.symbol,
                    "status": market.status,
                    "reason": market.reason,
                    "contract_type": market.contract_type,
                    "underlying": market.underlying,
                    "quote": market.quote,
                    "amount_multiplier": market.amount_multiplier,
                    "tick_size": market.tick_size,
                    "amount_step": market.amount_step,
                    "minimum_amount": market.minimum_amount,
                    "minimum_notional": market.minimum_notional,
                }
            )
    _write_csv(report_dir / "asset_rules.csv", mapping_rows or [{"status": "DATA_INSUFFICIENT"}])

    health_summary = []
    health_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in health_rows:
        health_by_key[(str(row.get("asset")), str(row.get("venue")))].append(row)
    if has_retained_aggregates(aggregate_rows):
        for asset in assets:
            for venue in (*config.reference_venues, "derive"):
                counts = _aggregate_health_counts(aggregate_rows, asset, venue)
                observations = sum(counts.values())
                healthy = counts["HEALTHY"]
                degraded = counts["DEGRADED"]
                stale = counts["STALE"]
                health_summary.append(
                    {
                        "asset": asset,
                        "venue": venue,
                        "observations": observations,
                        "latest_health": "AGGREGATE_ONLY" if observations else "UNOBSERVED",
                        "healthy_observations": healthy,
                        "degraded_observations": degraded,
                        "stale_observations": stale,
                        "uptime_pct": 100 * (healthy + degraded) / observations if observations else 0,
                        "latest_bbo_age": None,
                        "median_update_interval": None,
                        "p99_update_interval": None,
                        "maximum_recent_gap": None,
                        "sequence_gaps": None,
                        "duplicates": None,
                        "out_of_order": None,
                        "rejected_messages": None,
                        "parse_failures": None,
                        "reconnect_count": None,
                        "disconnect_duration": None,
                        "statistics_basis": "HEALTH_OBSERVATION_RATIO;WALL_CLOCK_DETAILS_PRUNED",
                    }
                )
    else:
        for asset in assets:
            for venue in (*config.reference_venues, "derive"):
                rows = health_by_key.get((asset, venue), [])
                latest = _latest(rows)
                statuses = [str(row.get("health", "UNOBSERVED")) for row in rows]
                health_summary.append(
                    {
                        "asset": asset,
                        "venue": venue,
                        "observations": len(rows),
                        "latest_health": latest.get("health", "UNOBSERVED"),
                        "healthy_observations": sum(status == "HEALTHY" for status in statuses),
                        "degraded_observations": sum(status == "DEGRADED" for status in statuses),
                        "stale_observations": sum(status == "STALE" for status in statuses),
                        "uptime_pct": None,
                        "latest_bbo_age": latest.get("bbo_age"),
                        "median_update_interval": latest.get("median_update_interval"),
                        "p99_update_interval": latest.get("p99_update_interval"),
                        "maximum_recent_gap": latest.get("maximum_recent_gap"),
                        "sequence_gaps": latest.get("sequence_gaps", 0),
                        "duplicates": latest.get("duplicates", 0),
                        "out_of_order": latest.get("out_of_order", 0),
                        "rejected_messages": latest.get("rejected_messages", 0),
                        "parse_failures": latest.get("parse_failures", 0),
                        "reconnect_count": latest.get("reconnect_count", 0),
                        "disconnect_duration": latest.get("disconnect_duration", 0),
                        "statistics_basis": "RAW_HEALTH_DETAIL",
                    }
                )
    _write_csv(report_dir / "reference_health.csv", health_summary)

    selection_rows = []
    failover_rows = []
    recovery_events = []
    disagreement_events = []
    for decision in decisions:
        asset = str(decision.get("asset"))
        if asset not in assets:
            continue
        selection_rows.append(
            {
                "timestamp": decision.get("timestamp"),
                "asset": asset,
                "selected_reference": decision.get("selected_reference"),
                "reference_fair_value": decision.get("reference_fair_value"),
                "reference_age_seconds": decision.get("reference_data_age_seconds"),
                "reference_health": decision.get("data_health"),
                "fresh_sources": ",".join(str(value) for value in decision.get("valid_reference_sources", []) or []),
                "priority_event": decision.get("priority_event", ""),
                "failover_event": decision.get("failover_event", ""),
                "recovery_event": decision.get("recovery_event", ""),
                "recovery_ready": decision.get("recovery_ready", False),
                "recovery_seconds": decision.get("recovery_seconds"),
                "time_using_binance": (decision.get("time_using") or {}).get("binance"),
                "time_using_bybit": (decision.get("time_using") or {}).get("bybit"),
                "time_using_okx": (decision.get("time_using") or {}).get("okx"),
                "time_paused": decision.get("time_paused"),
                "dispersion_bps": decision.get("reference_dispersion_bps"),
                "pause_reason": decision.get("reference_pause_reason", ""),
            }
        )
        event = str(decision.get("failover_event", ""))
        if event:
            parts = event.split("_TO_", 1)
            failover_rows.append(
                {
                    "timestamp": decision.get("timestamp"),
                    "asset": asset,
                    "from_source": parts[0] if parts else "",
                    "to_source": parts[1] if len(parts) > 1 else "",
                    "event": event,
                    "status": "FAILOVER_OBSERVED",
                }
            )
        if decision.get("recovery_event"):
            recovery_events.append(
                {
                    "timestamp": decision.get("timestamp"),
                    "asset": asset,
                    "event": decision.get("recovery_event"),
                    "selected_reference": decision.get("selected_reference"),
                    "recovery_ready": decision.get("recovery_ready"),
                    "recovery_seconds": decision.get("recovery_seconds"),
                    "status": "RECOVERY_OBSERVED",
                }
            )
        if decision.get("reference_pause_reason") == "REFERENCE_DISAGREEMENT_PAUSE":
            disagreement_events.append(
                {
                    "timestamp": decision.get("timestamp"),
                    "asset": asset,
                    "selected_reference": decision.get("selected_reference"),
                    "dispersion_bps": decision.get("reference_dispersion_bps"),
                    "pause_reason": decision.get("reference_pause_reason"),
                    "status": "PAUSED",
                }
            )
    _write_csv(report_dir / "reference_selection.csv", selection_rows or [{"status": "DATA_INSUFFICIENT"}])
    if not failover_rows:
        failover_rows = [
            {"asset": asset, "from_source": "", "to_source": "", "event": "", "status": "NO_FAILOVER_OBSERVED"}
            for asset in assets
        ]
    _write_csv(report_dir / "reference_failovers.csv", failover_rows)
    if not recovery_events:
        recovery_events = [
            {"asset": asset, "event": "", "recovery_ready": False, "status": "NO_RECOVERY_EVENT_OBSERVED"}
            for asset in assets
        ]
    _write_csv(report_dir / "reference_recovery.csv", recovery_events)
    if not disagreement_events:
        disagreement_events = [
            {"asset": asset, "dispersion_bps": None, "pause_reason": "", "status": "NO_DISAGREEMENT_OBSERVED"}
            for asset in assets
        ]
    _write_csv(report_dir / "reference_disagreement.csv", disagreement_events)
    _write_csv(report_dir / "minute_aggregates.csv", aggregate_artifact_rows(aggregate_rows))
    _write_csv(report_dir / "decision_rollups.csv", rollup_rows)

    spread_rows = []
    for asset in assets:
        if has_retained_aggregates(aggregate_rows):
            medians = aggregate_metric_values(aggregate_rows, asset, "derive_spread_bps_median")
            p90s = aggregate_metric_values(aggregate_rows, asset, "derive_spread_bps_p90")
            minimums = aggregate_metric_values(aggregate_rows, asset, "derive_spread_bps_min")
            maximums = aggregate_metric_values(aggregate_rows, asset, "derive_spread_bps_max")
            spread_rows.append(
                {
                    "asset": asset,
                    "observations": _aggregate_decision_count(aggregate_rows, asset),
                    "median_spread_bps": _median(medians) if medians else None,
                    "p95_spread_bps": None,
                    "min_spread_bps": min(minimums, default=None),
                    "max_spread_bps": max(maximums, default=None),
                    "minute_p90_median_bps": _median(p90s) if p90s else None,
                    "minute_p90_max_bps": max(p90s, default=None),
                    "aggregate_minutes": len([row for row in aggregate_rows if str(row.get("asset")) == asset]),
                    "statistics_basis": "MINUTE_MEDIANS;RUN_P95_NOT_IDENTIFIABLE_FROM_RETAINED_SUMMARIES",
                    "status": "AGGREGATE_DERIVED" if medians else "DATA_INSUFFICIENT",
                }
            )
            continue
        values = [_decimal(row.get("derive_spread_bps")) for row in decisions_by_asset[asset]]
        values = [value for value in values if value is not None]
        spread_rows.append(
            {
                "asset": asset,
                "observations": len(values),
                "median_spread_bps": _median(values),
                "p95_spread_bps": sorted(values)[min(len(values) - 1, int(len(values) * 0.95))] if values else None,
                "min_spread_bps": min(values) if values else None,
                "max_spread_bps": max(values) if values else None,
                "statistics_basis": "RAW_DECISION_DETAIL",
                "status": "OBSERVED" if values else "DATA_INSUFFICIENT",
            }
        )
    _write_csv(report_dir / "spread_statistics.csv", spread_rows)

    priority_actions = [
        row for row in actions
        if _control(str(row.get("model", ""))) == "PRIORITY_FAILOVER"
    ]
    activity_rows = []
    for asset in assets:
        rows = [row for row in priority_actions if row.get("asset") == asset]
        aggregate_metrics = {
            key: sum(
                float(aggregate_model_metrics(aggregate_rows, asset, f"PRIORITY_FAILOVER:{fill_model}").get(key, 0))
                for fill_model in ("CONSERVATIVE", "TOUCH_SENSITIVITY")
            )
            for key in ("action_rows", "creates", "holds", "replaces", "cancels")
        }
        aggregate_action_rows = int(aggregate_metrics["action_rows"])
        if has_retained_aggregates(aggregate_rows) and not aggregate_action_rows:
            aggregate_action_rows = len(rows) + int(aggregate_metrics["holds"])
        activity_rows.append(
            {
                "asset": asset,
                "action_rows": aggregate_action_rows if has_retained_aggregates(aggregate_rows) else len(rows),
                "creates": int(aggregate_metrics["creates"]) if has_retained_aggregates(aggregate_rows) else sum(row.get("action") == "CREATE" for row in rows),
                "holds": int(aggregate_metrics["holds"]) if has_retained_aggregates(aggregate_rows) else sum(row.get("action") == "HOLD" for row in rows),
                "replaces": int(aggregate_metrics["replaces"]) if has_retained_aggregates(aggregate_rows) else sum(
                    row.get("action") == "REPLACE"
                    or (row.get("action") == "CANCEL" and row.get("reason") == "REFRESH_NEEDED")
                    for row in rows
                ),
                "cancels": int(aggregate_metrics["cancels"]) if has_retained_aggregates(aggregate_rows) else sum(row.get("action") == "CANCEL" for row in rows),
                "data_basis": "MINUTE_AGGREGATES_AND_PERMANENT_MUTATIONS" if has_retained_aggregates(aggregate_rows) else "RAW_ACTION_DETAIL",
                "status": "AGGREGATE_DERIVED" if has_retained_aggregates(aggregate_rows) and aggregate_action_rows else "OBSERVED" if rows else "DATA_INSUFFICIENT",
            }
        )
    _write_csv(report_dir / "quote_activity.csv", activity_rows)
    _write_csv(
        report_dir / "quote_churn.csv",
        [
            {
                **row,
                "create_cancel_actions": row["creates"] + row["cancels"],
                "events_per_decision": (
                    Decimal(str(row["action_rows"]))
                    / Decimal(
                        str(
                            _aggregate_decision_count(aggregate_rows, row["asset"])
                            or len(decisions_by_asset[row["asset"]])
                        )
                    )
                    if _aggregate_decision_count(aggregate_rows, row["asset"]) or decisions_by_asset[row["asset"]]
                    else None
                ),
            }
            for row in activity_rows
        ],
    )

    priority_fills = _priority_rows(fills)
    conservative_fills = _priority_rows(fills, "PRIORITY_FAILOVER:CONSERVATIVE")
    touch_fills = _priority_rows(fills, "PRIORITY_FAILOVER:TOUCH_SENSITIVITY")
    fill_fields = [
        "timestamp", "asset", "side", "amount", "fill_price", "binance_fair_value",
        "derive_mid", "inventory_before", "inventory_after", "maker_fee_bps", "market_mode",
        "direction", "basis_bps", "quoted_edge_bps", "model", "reference_control",
    ]
    _write_csv(report_dir / "shadow_fills_conservative.csv", conservative_fills, fill_fields)
    _write_csv(report_dir / "shadow_fills_touch.csv", touch_fills, fill_fields)

    priority_markouts = _priority_rows(markouts)
    markout_fields = [
        "fill_timestamp", "horizon_seconds", "asset", "side", "reference_price", "derive_mid",
        "binance_markout_bps", "derive_markout_bps", "model", "reference_control",
    ]
    _write_csv(report_dir / "reference_markouts.csv", priority_markouts, markout_fields)
    _write_csv(report_dir / "derive_markouts.csv", priority_markouts, markout_fields)

    fill_lookup = {
        (str(row.get("asset")), str(row.get("model")), float(row.get("timestamp", 0))): row
        for row in priority_fills
    }
    net_rows = []
    for row in priority_markouts:
        fill = fill_lookup.get((str(row.get("asset")), str(row.get("model")), float(row.get("fill_timestamp", 0))))
        net_rows.append(
            {
                "asset": row.get("asset"),
                "model": row.get("model"),
                "fill_timestamp": row.get("fill_timestamp"),
                "horizon_seconds": row.get("horizon_seconds"),
                "quoted_edge_bps": fill.get("quoted_edge_bps") if fill else None,
                "maker_fee_bps": fill.get("maker_fee_bps") if fill else None,
                "derive_markout_bps": row.get("derive_markout_bps"),
                "selected_reference_markout_bps": row.get("binance_markout_bps"),
                "net_capture_proxy_bps": (
                    net_capture_proxy_bps(
                        Decimal(str(fill["quoted_edge_bps"])),
                        Decimal(str(fill["maker_fee_bps"])),
                        Decimal(str(row["derive_markout_bps"])),
                    )
                    if fill and row.get("derive_markout_bps") is not None
                    else None
                ),
                "status": "PROXY_NOT_REALIZED_PNL",
            }
        )
    _write_csv(report_dir / "net_capture.csv", net_rows or [{"status": "NO_FILL_MARKOUT_OBSERVED"}])

    model_names = tuple(config.control_models)
    model_rows = []
    for control in model_names:
        for fill_model in ("CONSERVATIVE", "TOUCH_SENSITIVITY"):
            model = f"{control}:{fill_model}"
            model_actions = [row for row in actions if row.get("model") == model]
            metrics = _metric_rows(model=model, fills=fills, markouts=markouts, net_rows=net_rows)
            aggregate_action_rows = sum(
                int(aggregate_model_metrics(aggregate_rows, asset, model).get("action_rows", 0))
                for asset in assets
            )
            aggregate_decision_rows = sum(_aggregate_decision_count(aggregate_rows, asset) for asset in assets)
            model_rows.append(
                {
                    "control": control,
                    "fill_model": fill_model,
                    "model": model,
                    "decision_rows": aggregate_decision_rows if has_retained_aggregates(aggregate_rows) else sum(
                        1 for row in decisions if isinstance((row.get("controls") or {}).get(control), dict)
                    ),
                    "action_rows": aggregate_action_rows if has_retained_aggregates(aggregate_rows) else len(model_actions),
                    **metrics,
                    "data_basis": "MINUTE_AGGREGATES_AND_PERMANENT_EVENTS" if has_retained_aggregates(aggregate_rows) else "RAW_EVENT_DETAIL",
                    "status": "HYPOTHETICAL_SHADOW_ONLY",
                }
            )
    _write_csv(report_dir / "model_comparison.csv", model_rows)

    asset_comparison = []
    for asset in assets:
        row: dict[str, Any] = {"asset": asset}
        for control in model_names:
            conservative = _metric_rows(
                model=f"{control}:CONSERVATIVE",
                fills=fills,
                markouts=markouts,
                net_rows=net_rows,
                asset=asset,
            )
            prefix = control.lower()
            row[f"{prefix}_30s_markout_bps"] = conservative["markout_30s_bps"]
            row[f"{prefix}_60s_markout_bps"] = conservative["markout_60s_bps"]
            row[f"{prefix}_net_capture_bps"] = conservative["net_capture_bps"]
            row[f"{prefix}_fills"] = sum(
                row_fill.get("asset") == asset and row_fill.get("model") == f"{control}:CONSERVATIVE"
                for row_fill in fills
            )
        asset_comparison.append(row)
    _write_csv(report_dir / "asset_comparison.csv", asset_comparison)

    asset_summary = []
    control_comparison = []
    for asset in assets:
        latest = _latest(decisions_by_asset[asset])
        aggregate_latest = _aggregate_latest_metrics(aggregate_rows, asset)
        asset_fills = [row for row in priority_fills if row.get("asset") == asset]
        asset_metrics = _metric_rows(
            model="PRIORITY_FAILOVER:CONSERVATIVE",
            fills=fills,
            markouts=markouts,
            net_rows=net_rows,
            asset=asset,
        )
        asset_summary.append(
            {
                "asset": asset,
                "derive_bid": latest.get("derive_bid"),
                "derive_ask": latest.get("derive_ask"),
                "derive_spread_bps": latest.get("derive_spread_bps") or aggregate_latest.get("derive_spread_bps_median"),
                "selected_reference": latest.get("selected_reference"),
                "reference_fair_value": latest.get("reference_fair_value") or aggregate_latest.get("reference_fair_value_median"),
                "reference_age_seconds": latest.get("reference_data_age_seconds"),
                "basis_bps": latest.get("basis_bps") or aggregate_latest.get("basis_bps_median"),
                "market_mode": latest.get("market_mode"),
                "direction": latest.get("direction"),
                "inventory_mode": latest.get("inventory_mode"),
                "shadow_bid": latest.get("desired_bid"),
                "shadow_ask": latest.get("desired_ask"),
                "fills": len(asset_fills),
                "maker_volume": sum(
                    (_decimal(row.get("amount")) or Decimal("0")) * (_decimal(row.get("fill_price")) or Decimal("0"))
                    for row in asset_fills
                ),
                "markout_30s_bps": asset_metrics["markout_30s_bps"],
                "markout_60s_bps": asset_metrics["markout_60s_bps"],
                "net_capture_bps": asset_metrics["net_capture_bps"],
                "status": "OBSERVED" if latest else "DATA_INSUFFICIENT",
            }
        )
        for control in model_names:
            metrics = _metric_rows(
                model=f"{control}:CONSERVATIVE",
                fills=fills,
                markouts=markouts,
                net_rows=net_rows,
                asset=asset,
            )
            control_comparison.append(
                {
                    "asset": asset,
                    "control": control,
                    "30s_markout_bps": metrics["markout_30s_bps"],
                    "60s_markout_bps": metrics["markout_60s_bps"],
                    "net_capture_bps": metrics["net_capture_bps"],
                    "fills": metrics["fill_rows"],
                    "status": "HYPOTHETICAL_SHADOW_ONLY",
                }
            )

    priority_failover_summary = []
    for asset in assets:
        rows = decisions_by_asset[asset]
        latest = _latest(rows)
        events = [str(row.get("failover_event")) for row in rows if row.get("failover_event")]
        switches = [str(row.get("priority_event")) for row in rows if row.get("priority_event")]
        source_counts = _aggregate_source_counts(aggregate_rows, asset)
        source_total = sum(source_counts.values())
        source_share = {
            source: 100 * source_counts[source] / source_total if source_total else None
            for source in ("binance", "bybit", "okx")
        }
        priority_failover_summary.append(
            {
                "asset": asset,
                "current_source": latest.get("selected_reference"),
                "time_using_binance": (latest.get("time_using") or {}).get("binance") or source_share["binance"],
                "time_using_bybit": (latest.get("time_using") or {}).get("bybit") or source_share["bybit"],
                "time_using_okx": (latest.get("time_using") or {}).get("okx") or source_share["okx"],
                "time_paused": latest.get("time_paused"),
                "binance_failures": sum(event.startswith("binance_TO_") for event in events),
                "bybit_failovers": sum(event.startswith("bybit_TO_") for event in events),
                "okx_failovers": sum(event.startswith("okx_TO_") for event in events),
                "recovery_events": sum(row.get("recovery_event") == "RECOVERY_TO_BINANCE" for row in rows),
                "source_switches": len(switches),
                "data_basis": "MINUTE_OCCUPANCY_AND_DECISION_ROLLUPS" if has_retained_aggregates(aggregate_rows) else "RAW_DECISION_DETAIL",
                "status": "OBSERVED" if rows else "DATA_INSUFFICIENT",
            }
        )

    runtime_state = telemetry.get_state("runtime") or {}
    exposure_rows = []
    for model, state in (runtime_state.get("models") or {}).items():
        exposure_rows.append(
            {
                "model": model,
                "reference_control": state.get("reference_control"),
                "fill_model": state.get("fill_model"),
                "equity": state.get("equity"),
                "gross_inventory": state.get("gross_inventory"),
                "net_inventory": state.get("net_inventory"),
                "fees": state.get("fees"),
                "fills": state.get("fills"),
                "max_drawdown": state.get("max_drawdown"),
            }
        )
    _write_csv(report_dir / "portfolio_exposure.csv", exposure_rows or [{"status": "NO_RUNTIME_STATE"}])

    pipeline = _pipeline_validation(
        assets=assets,
        config=config,
        mappings=mappings,
        decisions=decisions_by_asset,
        health_rows=health_rows,
        actions=actions,
        aggregate_rows=aggregate_rows,
    )
    passed_assets = sum(row["status"] == "PASS" for row in pipeline)
    failed_assets = len(pipeline) - passed_assets
    primary_blocker = (
        f"pipeline validation failed for: {', '.join(row['asset'] for row in pipeline if row['status'] == 'FAIL')}; "
        "public shadow evidence is hypothetical and no live canary is authorized"
        if failed_assets
        else "public shadow evidence is hypothetical and no live canary is authorized"
    )
    latest_health = {}
    for row in health_summary:
        key = (row["asset"], row["venue"])
        previous = latest_health.get(key)
        if previous is None or str(row.get("latest_health")) != "UNOBSERVED":
            latest_health[key] = row.get("latest_health")
    reference_health_counts = Counter(
        value for (asset, venue), value in latest_health.items() if venue in config.reference_venues
    )
    derive_health_counts = Counter(
        value for (asset, venue), value in latest_health.items() if venue == "derive"
    )
    if has_retained_aggregates(aggregate_rows):
        reference_health_counts = Counter()
        derive_health_counts = Counter()
        for asset in assets:
            for venue in config.reference_venues:
                reference_health_counts.update(_aggregate_health_counts(aggregate_rows, asset, venue))
            derive_health_counts.update(_aggregate_health_counts(aggregate_rows, asset, "derive"))
    current_source_counts = Counter(
        str(_latest(decisions_by_asset[asset]).get("selected_reference"))
        for asset in assets
        if _latest(decisions_by_asset[asset]).get("selected_reference")
    )
    priority_fills_count = len(priority_fills)
    priority_volume = sum(
        (_decimal(row.get("amount")) or Decimal("0")) * (_decimal(row.get("fill_price")) or Decimal("0"))
        for row in priority_fills
    )
    priority_model_summary = next(
        (
            row
            for row in model_rows
            if row.get("model") == "PRIORITY_FAILOVER:CONSERVATIVE"
        ),
        {},
    )
    priority_runtime = (runtime_state.get("models") or {}).get("PRIORITY_FAILOVER:CONSERVATIVE", {})
    report = {
        "report_version": "priority-reference-3asset-v1",
        "strategy": "DERIVE MULTI-ASSET MULTI-VENUE-REFERENCE ADAPTIVE MM",
        "reference_selection_mode": config.reference_selection_mode,
        "active_assets": assets,
        "disabled_assets": list(_DISABLED_ASSETS),
        "reference_priority": list(config.reference_priority),
        "bitget_primary_enabled": config.bitget_primary_enabled,
        "run_metadata": run_metadata or {},
        "mappings": mappings,
        "mapping_report": mapping_report,
        "pipeline_validation": pipeline,
        "asset_summary": asset_summary,
        "control_comparison": control_comparison,
        "priority_failover_summary": priority_failover_summary,
        "retention": retention,
        "classification": "NOT_READY_FOR_SMALL_MAINNET_CANARY",
        "primary_blocker": primary_blocker,
        "safety": config.public_safety(),
        "summary": {
            "status": (run_metadata or {}).get("status", "COMPLETE"),
            "total_assets": len(assets),
            "active_assets": len(assets),
            "pipeline_passed_assets": passed_assets,
            "pipeline_failed_assets": failed_assets,
            "reference_health": dict(reference_health_counts),
            "derive_health": dict(derive_health_counts),
            "current_reference": dict(current_source_counts),
            "shadow_fills": priority_fills_count,
            "shadow_conservative_fills": len(conservative_fills),
            "shadow_touch_fills": len(touch_fills),
            "shadow_volume": priority_volume,
            "net_capture_proxy_bps": priority_model_summary.get("net_capture_bps"),
            "max_drawdown": priority_runtime.get("max_drawdown"),
            "reference_failover_events": len(failover_rows) if failover_rows and failover_rows[0].get("event") else 0,
            "reference_recovery_events": len(recovery_events) if recovery_events and recovery_events[0].get("event") else 0,
            "reference_disagreement_events": len(disagreement_events) if disagreement_events and disagreement_events[0].get("pause_reason") else 0,
            "reference_pause_events": sum(bool(row.get("reference_pause_reason")) for row in decisions),
            "decision_observations": sum(_aggregate_decision_count(aggregate_rows, asset) for asset in assets),
            "decision_rollup_rows": len(rollup_rows),
            "minute_aggregate_rows": len(aggregate_rows),
        },
        "denominators": {
            "enabled_assets": len(assets),
            "configured_reference_venues": len(config.reference_venues),
            "priority_reference_venues": len(config.reference_priority),
            "decision_rows": len(raw_decisions),
            "retained_decision_rows": len(decisions),
            "action_rows": len(actions),
            "health_rows": len(health_rows),
            "fill_rows": len(priority_fills),
            "markout_rows": len(priority_markouts),
            "trade_rows": len([row for row in trades if row.get("source") == "derive"]),
        },
        "controls": model_rows,
        "artifact_files": list(PRIORITY_REPORT_FILES),
    }
    _write_json(report_dir / "final_report.json", report)

    control_lookup = {row["model"]: row for row in model_rows}
    markdown = [
        "# THREE-ASSET PRIORITY-REFERENCE UPDATE COMPLETE",
        "",
        "- Mode: `MAINNET_SHADOW`",
        "- Derive: `MAINNET` (sole execution venue)",
        "- Real orders: `0`",
        "- Real positions: `0`",
        "- Reference priority: `BINANCE -> BYBIT -> OKX -> PAUSE`",
        "- Bitget primary: `DISABLED` (diagnostics only)",
        "",
        "## Active assets",
        "",
        ", ".join(assets),
        "",
        "Disabled from this strategy: " + ", ".join(_DISABLED_ASSETS) + ". Historical reports/code are preserved.",
        "",
        "## Ten-minute pipeline checks",
        "",
        "| Asset | Derive BBO | Binance | Bybit | OKX | Priority | Fair value | Basis | Market | Inventory | Shadow bid | Shadow ask | HOLD | REPLACE | Status |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in pipeline:
        markdown.append(
            "| {asset} | {derive_bbo} | {binance} | {bybit} | {okx} | {priority_source_selection} | {fair_value} | {basis} | {market_mode} | {inventory_mode} | {shadow_bid} | {shadow_ask} | {hold} | {replace} | {status} |".format(
                **{key: "PASS" if value is True else "FAIL" if value is False else value for key, value in row.items()}
            )
        )
    markdown.extend(
        [
            "",
            "## Reference reliability",
            "",
            f"Observed failover events: `{report['summary']['reference_failover_events']}`.",
            f"Observed recovery events: `{report['summary']['reference_recovery_events']}`.",
            f"Observed disagreement pauses: `{report['summary']['reference_disagreement_events']}`.",
            f"Reference health snapshot: `{dict(reference_health_counts)}`.",
            "",
            "| Source | Current selected count |",
            "|---|---:|",
        ]
    )
    for venue in config.reference_venues:
        markdown.append(f"| {venue.upper()} | {current_source_counts.get(venue, 0)} |")
    markdown.extend(["", "## Model comparison", "", "| Model | Actions | Fills | 30s markout bps | 60s markout bps | Net capture proxy bps |", "|---|---:|---:|---:|---:|---:|"])
    for model, row in control_lookup.items():
        markdown.append(
            f"| {model} | {row['action_rows']} | {row['fill_rows']} | {row['markout_30s_bps'] or '—'} | {row['markout_60s_bps'] or '—'} | {row['net_capture_bps'] or '—'} |"
        )
    markdown.extend(["", "## Final classification", "", f"`{report['classification']}`", "", report["primary_blocker"], "", "READY FOR LIVE: `NO`", ""])
    (report_dir / "final_report.md").write_text("\n".join(markdown), encoding="utf-8")
    return report
