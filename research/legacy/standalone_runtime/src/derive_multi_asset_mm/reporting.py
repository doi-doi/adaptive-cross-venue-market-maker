"""Materialize multi-reference shadow telemetry into auditable artifacts."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any

from .lead_lag import estimate_lead_lag
from .markouts import net_capture_proxy_bps
from .models import AssetMapping, json_safe
from .telemetry import TelemetryStore

REPORT_FILES = (
    "reference_market_mapping.csv",
    "reference_source_health.csv",
    "reference_source_gaps.csv",
    "reference_reconnects.csv",
    "reference_sequence_audit.csv",
    "reference_outliers.csv",
    "reference_disagreement.csv",
    "reference_availability.csv",
    "consensus_fair_value.csv",
    "source_dispersion.csv",
    "derive_basis.csv",
    "derive_trading_rules.csv",
    "capital_compatibility.csv",
    "asset_spread_statistics.csv",
    "asset_activity.csv",
    "asset_quote_churn.csv",
    "shadow_fills.csv",
    "derive_markouts.csv",
    "reference_markouts.csv",
    "net_capture.csv",
    "reference_model_comparison.csv",
    "source_ablation.csv",
    "lead_lag.csv",
    "reference_failover_counters.csv",
    "reference_protection.csv",
    "asset_opportunity_scores.csv",
    "portfolio_exposure.csv",
    "portfolio_performance.csv",
    "data_health.csv",
    "final_multi_reference_shadow_report.md",
    "final_multi_reference_shadow_report.json",
)

# Compatibility outputs retained for readers of the earlier Binance-only run.
LEGACY_REPORT_FILES = (
    "asset_reference_mapping.csv",
    "fair_value_quality.csv",
    "basis_statistics.csv",
    "market_state_occupancy.csv",
    "direction_state_occupancy.csv",
    "inventory_state_occupancy.csv",
    "quote_activity.csv",
    "fills.csv",
    "binance_markouts.csv",
    "toxicity.csv",
    "asset_comparison.csv",
    "protection_effectiveness.csv",
    "control_comparison.csv",
    "latency.csv",
)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    materialized = list(rows)
    if fieldnames is None:
        fieldnames = list(materialized[0].keys()) if materialized else ["status"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def _json_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        result.append({"timestamp": row["timestamp"], "asset": row["asset"], **payload})
    return result


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _numbers(values: Iterable[Any]) -> list[Decimal]:
    return [number for value in values if (number := _decimal(value)) is not None]


def _mean(values: Iterable[Any]) -> Decimal | None:
    numbers = _numbers(values)
    return sum(numbers, Decimal("0")) / Decimal(len(numbers)) if numbers else None


def _median(values: Iterable[Any]) -> Decimal | None:
    numbers = _numbers(values)
    return Decimal(str(median(numbers))) if numbers else None


def _float_or_none(value: Any) -> float | None:
    number = _decimal(value)
    return float(number) if number is not None else None


def _controls(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    controls = row.get("controls", {})
    return controls if isinstance(controls, dict) else {}


def _control_name(model: str) -> str:
    return model.split(":", 1)[0] if ":" in model else "BINANCE_ONLY_REFERENCE"


def _health_rows(telemetry: TelemetryStore) -> list[dict[str, Any]]:
    result = []
    for row in telemetry.rows("reference_health"):
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        result.append({"timestamp": row["timestamp"], "asset": row["asset"], "venue": row["venue"], **payload})
    return result


def _reference_rows(mappings: dict[str, AssetMapping], assets: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for asset in assets:
        mapping = mappings.get(asset)
        markets = mapping.reference_markets if mapping else ()
        if not markets:
            rows.append({"asset": asset, "venue": "", "connector": "", "symbol": "", "status": "UNAVAILABLE", "reason": "NO_MAPPING"})
            continue
        for market in markets:
            rows.append(
                {
                    "asset": asset,
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
    return rows


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
    health_rows = _health_rows(telemetry)
    value_rows = telemetry.rows("reference_values")
    trades = telemetry.rows("trades")
    assets = [asset.symbol for asset in config.enabled_assets]
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in decisions:
        by_asset[str(row["asset"])].append(row)
    by_health: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in health_rows:
        by_health[(str(row["asset"]), str(row["venue"]))].append(row)

    mapping_rows = _reference_rows(mappings, assets)
    _write_csv(
        report_dir / "reference_market_mapping.csv",
        mapping_rows,
        [
            "asset", "venue", "connector", "symbol", "status", "reason", "contract_type",
            "underlying", "quote", "amount_multiplier", "tick_size", "amount_step",
            "minimum_amount", "minimum_notional",
        ],
    )
    _write_csv(report_dir / "asset_reference_mapping.csv", mapping_rows)

    health_summary: list[dict[str, Any]] = []
    gap_rows: list[dict[str, Any]] = []
    reconnect_rows: list[dict[str, Any]] = []
    sequence_rows: list[dict[str, Any]] = []
    availability_rows: list[dict[str, Any]] = []
    for asset in assets:
        venues = [venue for venue in config.reference_venues] + ["derive"]
        for venue in venues:
            rows = by_health.get((asset, venue), [])
            statuses = [str(row.get("health", "UNOBSERVED")) for row in rows]
            latest = rows[-1] if rows else {}
            observed = len(rows)
            healthy = sum(status == "HEALTHY" for status in statuses)
            degraded = sum(status == "DEGRADED" for status in statuses)
            stale = sum(status == "STALE" for status in statuses)
            health_summary.append(
                {
                    "asset": asset, "venue": venue, "observations": observed,
                    "latest_health": latest.get("health", "UNOBSERVED"),
                    "latest_bbo_age": latest.get("bbo_age"),
                    "median_update_interval": latest.get("median_update_interval"),
                    "p99_update_interval": latest.get("p99_update_interval"),
                    "maximum_recent_gap": latest.get("maximum_recent_gap"),
                    "updates": latest.get("updates", 0),
                    "rejected_messages": latest.get("rejected_messages", 0),
                    "parse_failures": latest.get("parse_failures", 0),
                    "duplicates": latest.get("duplicates", 0),
                    "out_of_order": latest.get("out_of_order", 0),
                    "sequence_gaps": latest.get("sequence_gaps", 0),
                    "reconnect_count": latest.get("reconnect_count", 0),
                    "disconnect_duration": latest.get("disconnect_duration", 0),
                    "sequence_policy": latest.get("sequence_policy", "UNKNOWN"),
                }
            )
            gap_rows.append(
                {
                    "asset": asset, "venue": venue, "health_observations": observed,
                    "healthy_observations": healthy, "degraded_observations": degraded,
                    "stale_observations": stale, "p99_update_interval": latest.get("p99_update_interval"),
                    "maximum_recent_gap": latest.get("maximum_recent_gap"),
                    "status": "READY" if healthy + degraded else "DATA_INSUFFICIENT",
                }
            )
            reconnect_rows.append(
                {
                    "asset": asset, "venue": venue, "reconnect_count": latest.get("reconnect_count", 0),
                    "disconnect_duration": latest.get("disconnect_duration", 0),
                    "observations": observed,
                }
            )
            sequence_rows.append(
                {
                    "asset": asset, "venue": venue, "sequence_policy": latest.get("sequence_policy", "UNKNOWN"),
                    "sequence_gaps": latest.get("sequence_gaps", 0), "duplicates": latest.get("duplicates", 0),
                    "out_of_order": latest.get("out_of_order", 0), "rejected_messages": latest.get("rejected_messages", 0),
                    "parse_failures": latest.get("parse_failures", 0),
                }
            )
            availability_rows.append(
                {
                    "asset": asset, "venue": venue, "observations": observed,
                    "healthy_rate": healthy / observed if observed else None,
                    "degraded_rate": degraded / observed if observed else None,
                    "stale_rate": stale / observed if observed else None,
                    "latest_health": latest.get("health", "UNOBSERVED"),
                }
            )
    _write_csv(report_dir / "reference_source_health.csv", health_summary)
    _write_csv(report_dir / "reference_source_gaps.csv", gap_rows)
    _write_csv(report_dir / "reference_reconnects.csv", reconnect_rows)
    _write_csv(report_dir / "reference_sequence_audit.csv", sequence_rows)
    _write_csv(report_dir / "reference_availability.csv", availability_rows)

    consensus_rows: list[dict[str, Any]] = []
    dispersion_rows: list[dict[str, Any]] = []
    outlier_rows: list[dict[str, Any]] = []
    disagreement_rows: list[dict[str, Any]] = []
    basis_rows: list[dict[str, Any]] = []
    protection_rows: list[dict[str, Any]] = []
    latency_rows: list[dict[str, Any]] = []
    spread_rows: list[dict[str, Any]] = []
    activity_rows: list[dict[str, Any]] = []
    opportunity_rows: list[dict[str, Any]] = []
    control_rows: list[dict[str, Any]] = []
    asset_summary_rows: list[dict[str, Any]] = []
    latest_selected_decisions: dict[str, dict[str, Any]] = {}
    reference_series: dict[tuple[str, str], list[tuple[float, Decimal]]] = defaultdict(list)
    derive_series: dict[str, list[tuple[float, Decimal]]] = defaultdict(list)
    rules_rows: list[dict[str, Any]] = []
    capital_rows: list[dict[str, Any]] = []
    capital_metrics: dict[str, dict[str, Any]] = {}

    for asset in assets:
        mapping = mappings.get(asset)
        rules = mapping.rules if mapping else None
        rows = by_asset.get(asset, [])
        latest_derive_mid = next(
            (
                (bid + ask) / Decimal("2")
                for decision in reversed(rows)
                if (bid := _decimal(decision.get("derive_bid"))) is not None
                and (ask := _decimal(decision.get("derive_ask"))) is not None
            ),
            None,
        )
        minimum_amount = rules.minimum_amount if rules else None
        rule_minimum_notional = rules.minimum_notional if rules and rules.minimum_notional > 0 else None
        implied_minimum_notional = (
            minimum_amount * latest_derive_mid
            if minimum_amount is not None and latest_derive_mid is not None
            else None
        )
        effective_minimum_notional = rule_minimum_notional or implied_minimum_notional
        minimum_notional_source = (
            "DERIVE_RULE"
            if rule_minimum_notional is not None
            else "MINIMUM_AMOUNT_X_LATEST_DERIVE_MID"
            if implied_minimum_notional is not None
            else "UNAVAILABLE"
        )
        rules_rows.append(
            {
                "asset": asset,
                "instrument": rules.instrument_name if rules else None,
                "base_asset": rules.base_asset if rules else None,
                "quote_asset": rules.quote_asset if rules else None,
                "tick_size": rules.tick_size if rules else None,
                "amount_step": rules.amount_step if rules else None,
                "minimum_amount": rules.minimum_amount if rules else None,
                "maximum_amount": rules.maximum_amount if rules else None,
                "minimum_notional": rules.minimum_notional if rules else None,
                "maker_fee_bps": rules.maker_fee_bps if rules else None,
            }
        )
        capital_rows.append(
            {
                "asset": asset,
                "minimum_amount": minimum_amount,
                "minimum_notional": effective_minimum_notional,
                "rule_minimum_notional": rule_minimum_notional,
                "implied_minimum_notional": implied_minimum_notional,
                "minimum_notional_source": minimum_notional_source,
                "capital_usdc": config.capital_usdc,
                "capital_pct": effective_minimum_notional / config.capital_usdc * 100 if effective_minimum_notional else None,
                "max_single_order_notional": config.max_single_order_notional,
                "capital_compatible": bool(
                    effective_minimum_notional is not None
                    and effective_minimum_notional <= config.max_single_order_notional
                ),
            }
        )
        capital_metrics[asset] = {
            "minimum_amount": minimum_amount,
            "minimum_notional": effective_minimum_notional,
            "minimum_notional_source": minimum_notional_source,
        }
        selected_rows = [row for row in rows if row.get("reference_control") == "MULTI_SOURCE_CONSENSUS"] or rows
        spreads = [row.get("derive_spread_bps") for row in selected_rows]
        spread_rows.append(
            {
                "asset": asset,
                "observations": len([value for value in spreads if _decimal(value) is not None]),
                "median_spread_bps": _median(spreads),
                "p95_spread_bps": (sorted(_numbers(spreads))[max(0, int(len(_numbers(spreads)) * 0.95) - 1)] if _numbers(spreads) else None),
                "min_spread_bps": min(_numbers(spreads), default=None),
                "max_spread_bps": max(_numbers(spreads), default=None),
            }
        )
        control_fill_counts: dict[str, int] = defaultdict(int)
        for fill in fills:
            if fill["asset"] == asset:
                control_fill_counts[_control_name(str(fill.get("model", "")))] += 1
        controls_seen = set()
        for decision in rows:
            timestamp = float(decision["timestamp"])
            derive_mid = None
            bid, ask = _decimal(decision.get("derive_bid")), _decimal(decision.get("derive_ask"))
            if bid is not None and ask is not None:
                derive_mid = (bid + ask) / 2
                derive_series[asset].append((timestamp, derive_mid))
            values = decision.get("source_mids", {})
            fair_values = decision.get("source_fair_values", {})
            deviations = decision.get("reference_deviations_bps", {})
            valid = set(decision.get("valid_reference_sources", []) or [])
            for venue, value in values.items() if isinstance(values, dict) else ():
                number = _decimal(value)
                if number is not None:
                    reference_series[(asset, str(venue))].append((timestamp, number))
            consensus_rows.append(
                {
                    "timestamp": timestamp,
                    "asset": asset,
                    "control": decision.get("reference_control"),
                    "raw_consensus_fair_value": decision.get("reference_fair_value"),
                    "derive_fair_value": decision.get("fair_value"),
                    "robust_median": decision.get("reference_robust_median"),
                    "source_count": len(valid),
                    "valid_sources": ",".join(sorted(str(item) for item in valid)),
                    "confidence": decision.get("reference_confidence"),
                    "pause_reason": decision.get("reference_pause_reason"),
                }
            )
            dispersion_rows.append(
                {
                    "timestamp": timestamp, "asset": asset,
                    "dispersion_bps": decision.get("reference_dispersion_bps"),
                    "source_count": len(valid), "confidence": decision.get("reference_confidence"),
                }
            )
            for venue, deviation in deviations.items() if isinstance(deviations, dict) else ():
                outlier_rows.append(
                    {
                        "timestamp": timestamp, "asset": asset, "venue": venue,
                        "fair_value": fair_values.get(venue) if isinstance(fair_values, dict) else None,
                        "deviation_bps": deviation,
                        "outlier": venue in set(decision.get("reference_outliers", []) or []),
                        "valid": venue in valid,
                    }
                )
            if decision.get("reference_pause_reason") == "REFERENCE_DISAGREEMENT_PAUSE":
                disagreement_rows.append(
                    {
                        "timestamp": timestamp, "asset": asset,
                        "dispersion_bps": decision.get("reference_dispersion_bps"),
                        "pause_threshold_bps": config.reference_disagreement_pause_bps,
                        "source_count": len(valid), "status": "PAUSED",
                    }
                )
            for control, control_payload in _controls(decision).items():
                controls_seen.add(control)
                fair_payload = control_payload.get("fair_value", {}) if isinstance(control_payload, dict) else {}
                basis_rows.append(
                    {
                        "timestamp": timestamp, "asset": asset, "control": control,
                        "basis_bps": fair_payload.get("basis_bps") if isinstance(fair_payload, dict) else None,
                        "baseline_basis_bps": fair_payload.get("baseline_basis_bps") if isinstance(fair_payload, dict) else None,
                        "ewma_basis_bps": fair_payload.get("ewma_basis_bps") if isinstance(fair_payload, dict) else None,
                        "raw_fair_value": fair_payload.get("fair_value_raw") if isinstance(fair_payload, dict) else None,
                        "derive_fair_value": fair_payload.get("derive_fair_value") if isinstance(fair_payload, dict) else None,
                    }
                )
            latency_rows.append({"asset": asset, "timestamp": timestamp, "processing_latency_ms": decision.get("processing_latency_ms")})
        for control in ("DERIVE_ONLY", "BINANCE_ONLY_REFERENCE", "MULTI_SOURCE_CONSENSUS"):
            control_decisions = [
                row for row in rows
                if isinstance(_controls(row).get(control), dict)
            ]
            control_rows.append(
                {
                    "asset": asset,
                    "control": control,
                    "decision_rows": len(control_decisions),
                    "fair_value_ready_rows": sum(
                        bool(_controls(row).get(control, {}).get("fair_value") or {})
                        for row in control_decisions
                    ),
                    "shadow_fills": control_fill_counts.get(control, 0),
                    "status": "COMPARABLE_SAME_DERIVE_TIMESTAMPS" if control_decisions else "DATA_INSUFFICIENT",
                }
            )
        blocked = sum(bool(row.get("block_reason")) for row in selected_rows)
        activity_rows.append(
            {
                "asset": asset, "decision_rows": len(selected_rows), "blocked_rows": blocked,
                "unblocked_rows": len(selected_rows) - blocked,
                "conservative_fills": sum(1 for row in fills if row["asset"] == asset and str(row.get("model", "")).endswith(":CONSERVATIVE")),
                "touch_sensitivity_fills": sum(1 for row in fills if row["asset"] == asset and str(row.get("model", "")).endswith(":TOUCH_SENSITIVITY")),
                "derive_trades": sum(1 for row in trades if row["asset"] == asset and row["source"] == "derive"),
            }
        )
        opportunity_rows.append(
            {
                "asset": asset,
                "observations": len(selected_rows),
                "median_score": _median(row.get("opportunity_score") for row in selected_rows),
                "max_score": max(_numbers(row.get("opportunity_score") for row in selected_rows), default=None),
            }
        )
        protection_rows.append(
            {
                "asset": asset,
                "observations": len(selected_rows),
                "divergence_protected_rows": sum(bool(row.get("divergence_protected")) for row in selected_rows),
                "fast_move_protected_rows": sum(bool(row.get("fast_move_protected")) for row in selected_rows),
                "disagreement_paused_rows": sum(row.get("reference_pause_reason") == "REFERENCE_DISAGREEMENT_PAUSE" for row in selected_rows),
                "blocked_rows": sum(bool(row.get("block_reason")) for row in selected_rows),
            }
        )

    _write_csv(report_dir / "consensus_fair_value.csv", consensus_rows)
    _write_csv(report_dir / "source_dispersion.csv", dispersion_rows)
    _write_csv(report_dir / "reference_outliers.csv", outlier_rows)
    _write_csv(report_dir / "reference_disagreement.csv", disagreement_rows, ["timestamp", "asset", "dispersion_bps", "pause_threshold_bps", "source_count", "status"])
    _write_csv(report_dir / "derive_trading_rules.csv", rules_rows)
    _write_csv(report_dir / "capital_compatibility.csv", capital_rows)
    _write_csv(report_dir / "asset_spread_statistics.csv", spread_rows)
    _write_csv(report_dir / "asset_activity.csv", activity_rows)
    churn_rows = []
    for asset in assets:
        asset_actions = [row for row in actions if row["asset"] == asset]
        churn_rows.append(
            {
                "asset": asset,
                "action_rows": len(asset_actions),
                "creates": sum(row["action"] == "CREATE" for row in asset_actions),
                "cancels": sum(row["action"] == "CANCEL" for row in asset_actions),
                "holds": sum(row["action"] == "HOLD" for row in asset_actions),
                "models": len({row.get("model", "") for row in asset_actions}),
            }
        )
    _write_csv(report_dir / "asset_quote_churn.csv", churn_rows)
    _write_csv(report_dir / "shadow_fills.csv", fills)
    _write_csv(report_dir / "derive_markouts.csv", markouts)
    _write_csv(report_dir / "reference_markouts.csv", markouts)

    fill_lookup = {
        (row["asset"], row["model"], float(row["timestamp"])): row
        for row in fills
    }
    net_rows = []
    for row in markouts:
        fill = fill_lookup.get((row["asset"], row["model"], float(row["fill_timestamp"])))
        net_rows.append(
            {
                "asset": row["asset"], "model": row["model"], "reference_control": row.get("reference_control", _control_name(str(row["model"]))),
                "fill_timestamp": row["fill_timestamp"], "horizon_seconds": row["horizon_seconds"],
                "quoted_edge_bps": fill.get("quoted_edge_bps") if fill else None,
                "maker_fee_bps": fill.get("maker_fee_bps") if fill else None,
                "derive_markout_bps": row.get("derive_markout_bps"),
                "reference_markout_bps": row.get("binance_markout_bps"),
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
    _write_csv(report_dir / "net_capture.csv", net_rows)

    # Add fill-dependent comparison fields after markouts/net capture have
    # been materialized.  Conservative and touch-sensitive observations stay
    # separate so a touch proxy cannot be mistaken for execution evidence.
    for row in control_rows:
        asset = str(row["asset"])
        control = str(row["control"])
        conservative_model = f"{control}:CONSERVATIVE"
        touch_model = f"{control}:TOUCH_SENSITIVITY"
        row.update(
            {
                "conservative_30s_markout_bps": _mean(
                    markout.get("derive_markout_bps")
                    for markout in markouts
                    if markout.get("asset") == asset
                    and markout.get("model") == conservative_model
                    and int(markout.get("horizon_seconds", 0)) == 30
                ),
                "touch_30s_markout_bps": _mean(
                    markout.get("derive_markout_bps")
                    for markout in markouts
                    if markout.get("asset") == asset
                    and markout.get("model") == touch_model
                    and int(markout.get("horizon_seconds", 0)) == 30
                ),
                "conservative_60s_markout_bps": _mean(
                    markout.get("derive_markout_bps")
                    for markout in markouts
                    if markout.get("asset") == asset
                    and markout.get("model") == conservative_model
                    and int(markout.get("horizon_seconds", 0)) == 60
                ),
                "touch_60s_markout_bps": _mean(
                    markout.get("derive_markout_bps")
                    for markout in markouts
                    if markout.get("asset") == asset
                    and markout.get("model") == touch_model
                    and int(markout.get("horizon_seconds", 0)) == 60
                ),
                "conservative_net_capture_proxy_bps": _mean(
                    net.get("net_capture_proxy_bps")
                    for net in net_rows
                    if net.get("asset") == asset and net.get("model") == conservative_model
                ),
                "touch_net_capture_proxy_bps": _mean(
                    net.get("net_capture_proxy_bps")
                    for net in net_rows
                    if net.get("asset") == asset and net.get("model") == touch_model
                ),
            }
        )

    for asset in assets:
        selected_rows = [
            row for row in by_asset.get(asset, [])
            if row.get("reference_control") == "MULTI_SOURCE_CONSENSUS"
        ] or by_asset.get(asset, [])
        latest = selected_rows[-1] if selected_rows else {}
        latest_selected_decisions[asset] = latest
        selected_fills = [
            fill for fill in fills
            if fill.get("asset") == asset and _control_name(str(fill.get("model", ""))) == "MULTI_SOURCE_CONSENSUS"
        ]
        conservative_model = "MULTI_SOURCE_CONSENSUS:CONSERVATIVE"
        selected_conservative_markouts = [
            markout for markout in markouts
            if markout.get("asset") == asset and markout.get("model") == conservative_model
        ]
        selected_conservative_net = [
            net for net in net_rows
            if net.get("asset") == asset and net.get("model") == conservative_model
        ]
        bid, ask = _decimal(latest.get("derive_bid")), _decimal(latest.get("derive_ask"))
        derive_mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        capital_metric = capital_metrics.get(asset, {})
        asset_summary_rows.append(
            {
                "asset": asset,
                "observations": len(selected_rows),
                "derive_mid": derive_mid,
                "derive_spread_bps": latest.get("derive_spread_bps"),
                "consensus_fair_value": latest.get("reference_fair_value"),
                "basis_bps": latest.get("basis_bps"),
                "valid_sources": ",".join(str(value) for value in latest.get("valid_reference_sources", []) or []),
                "source_count": len(latest.get("valid_reference_sources", []) or []),
                "source_dispersion_bps": latest.get("reference_dispersion_bps"),
                "volatility": latest.get("volatility"),
                "direction": latest.get("direction"),
                "market_mode": latest.get("market_mode"),
                "inventory_mode": latest.get("inventory_mode"),
                "minimum_order_amount": capital_metric.get("minimum_amount"),
                "minimum_order_notional": capital_metric.get("minimum_notional"),
                "minimum_notional_source": capital_metric.get("minimum_notional_source"),
                "buy_edge_bps": latest.get("buy_edge_bps"),
                "sell_edge_bps": latest.get("sell_edge_bps"),
                "shadow_bid": latest.get("desired_bid"),
                "shadow_ask": latest.get("desired_ask"),
                "fills": len(selected_fills),
                "conservative_fills": sum(fill.get("model") == conservative_model for fill in selected_fills),
                "touch_sensitivity_fills": sum(
                    fill.get("model") == "MULTI_SOURCE_CONSENSUS:TOUCH_SENSITIVITY" for fill in selected_fills
                ),
                "maker_volume": sum(
                    (_decimal(fill.get("amount")) or Decimal("0")) * (_decimal(fill.get("fill_price")) or Decimal("0"))
                    for fill in selected_fills
                ),
                "conservative_30s_markout_bps": _mean(
                    markout.get("derive_markout_bps")
                    for markout in selected_conservative_markouts
                    if int(markout.get("horizon_seconds", 0)) == 30
                ),
                "conservative_60s_markout_bps": _mean(
                    markout.get("derive_markout_bps")
                    for markout in selected_conservative_markouts
                    if int(markout.get("horizon_seconds", 0)) == 60
                ),
                "net_capture_proxy_bps": _mean(
                    net.get("net_capture_proxy_bps")
                    for net in selected_conservative_net
                    if int(net.get("horizon_seconds", 0)) == 30
                ),
                "opportunity_score": latest.get("opportunity_score"),
                "status": "OBSERVED" if selected_rows else "DATA_INSUFFICIENT",
            }
        )

    model_rows: list[dict[str, Any]] = []
    for control in ("DERIVE_ONLY", "BINANCE_ONLY_REFERENCE", "MULTI_SOURCE_CONSENSUS"):
        for fill_model in ("CONSERVATIVE", "TOUCH_SENSITIVITY"):
            model_name = f"{control}:{fill_model}"
            model_fills = [row for row in fills if row.get("model") == model_name]
            model_actions = [row for row in actions if row.get("model") == model_name]
            model_nets = [row.get("net_capture_proxy_bps") for row in net_rows if row.get("model") == model_name]
            model_rows.append(
                {
                    "reference_control": control, "fill_model": fill_model, "model": model_name,
                    "action_rows": len(model_actions), "fill_rows": len(model_fills),
                    "maker_volume": sum(Decimal(str(row["amount"])) * Decimal(str(row["fill_price"])) for row in model_fills),
                    "mean_quoted_edge_bps": _mean(row.get("quoted_edge_bps") for row in model_fills),
                    "mean_net_capture_proxy_bps": _mean(model_nets),
                    "status": "HYPOTHETICAL_SHADOW_ONLY",
                }
            )
    _write_csv(report_dir / "reference_model_comparison.csv", model_rows)

    # Source contribution is evaluated on the same causal observation groups:
    # all eligible source values versus each leave-one-source-out variant.
    ablation_rows = []
    for asset in assets:
        groups: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for value in value_rows:
            if value.get("asset") != asset or value.get("venue") not in config.reference_venues:
                continue
            if value.get("fair_value") is None:
                continue
            groups[str(value.get("timestamp"))][str(value["venue"])] = value
        for configuration, excluded in [
            ("ALL_SOURCES", None),
            *((f"NO_{venue.upper()}", venue) for venue in config.reference_venues),
        ]:
            group_medians: list[Decimal] = []
            group_dispersion: list[Decimal] = []
            group_counts: list[int] = []
            valid_observations = 0
            for group in groups.values():
                selected = [
                    row for venue, row in group.items()
                    if venue != excluded
                ]
                prices = [_decimal(row.get("fair_value")) for row in selected]
                prices = [price for price in prices if price is not None and price > 0]
                if not prices:
                    continue
                center = Decimal(str(median(prices)))
                group_medians.append(center)
                group_counts.append(len(prices))
                if len(prices) > 1:
                    group_dispersion.append((max(prices) - min(prices)) / center * Decimal("10000"))
                valid_observations += sum(bool(row.get("valid")) for row in selected)
            ablation_rows.append(
                {
                    "asset": asset,
                    "configuration": configuration,
                    "excluded_venue": excluded or "",
                    "observations": len(group_medians),
                    "valid_observations": valid_observations,
                    "median_source_count": _median(group_counts),
                    "p10_source_count": sorted(group_counts)[max(0, int(len(group_counts) * 0.10) - 1)] if group_counts else None,
                    "median_fair_value": _median(group_medians),
                    "median_dispersion_bps": _median(group_dispersion),
                    "status": "SOURCE_ABLATION_OBSERVED" if group_medians else "DATA_INSUFFICIENT",
                }
            )
    _write_csv(report_dir / "source_ablation.csv", ablation_rows)

    # Older telemetry predates the explicit EWMA field. Reconstruct it from
    # the observed basis sequence while preserving the same causal ordering.
    ewma_by_key: dict[tuple[str, str], Decimal] = {}
    alpha = config.basis_ewma_alpha
    for row in basis_rows:
        key = (str(row["asset"]), str(row["control"]))
        persisted = _decimal(row.get("ewma_basis_bps"))
        if persisted is not None:
            ewma_by_key[key] = persisted
            continue
        observed = _decimal(row.get("basis_bps"))
        if observed is None:
            continue
        prior = ewma_by_key.get(key)
        current = observed if prior is None else alpha * observed + (Decimal("1") - alpha) * prior
        ewma_by_key[key] = current
        row["ewma_basis_bps"] = current
    _write_csv(report_dir / "derive_basis.csv", basis_rows)

    lead_rows = []
    for asset in assets:
        for venue in config.reference_venues:
            rows = estimate_lead_lag(
                reference_series.get((asset, venue), []),
                derive_series.get(asset, []),
                lags_seconds=(0.1, 0.25, 0.5, 1, 2, 5),
                return_horizon_seconds=1,
            )
            lead_rows.extend({"asset": asset, "venue": venue, **row} for row in rows)
    _write_csv(report_dir / "lead_lag.csv", lead_rows, ["asset", "venue", "lag_seconds", "return_horizon_seconds", "observations", "correlation", "status"])
    _write_csv(report_dir / "reference_protection.csv", protection_rows)
    _write_csv(report_dir / "asset_opportunity_scores.csv", opportunity_rows)

    state = telemetry.get_state("runtime") or {}
    exposure_rows = []
    performance_rows = []
    for model_name, model_state in (state.get("models", {}) if isinstance(state, dict) else {}).items():
        performance_rows.append({"model": model_name, **model_state})
        for asset in assets:
            positions = model_state.get("positions", {}) if isinstance(model_state, dict) else {}
            notionals = model_state.get("position_notionals", {}) if isinstance(model_state, dict) else {}
            exposure_rows.append(
                {
                    "model": model_name, "asset": asset,
                    "position_amount": positions.get(asset) if isinstance(positions, dict) else None,
                    "position_notional": notionals.get(asset) if isinstance(notionals, dict) else None,
                    "status": "LATEST_RUNTIME_SNAPSHOT",
                }
            )
    if not performance_rows:
        performance_rows = [{"model": name, "fills": 0, "status": "NO_RUNTIME_STATE"} for name in (f"{control}:{fill}" for control in ("DERIVE_ONLY", "BINANCE_ONLY_REFERENCE", "MULTI_SOURCE_CONSENSUS") for fill in ("CONSERVATIVE", "TOUCH_SENSITIVITY"))]
    _write_csv(report_dir / "portfolio_exposure.csv", exposure_rows or [{"status": "NO_RUNTIME_STATE"}])
    _write_csv(report_dir / "portfolio_performance.csv", performance_rows)

    data_health_rows = []
    for asset in assets:
        rows = by_asset.get(asset, [])
        data_health_rows.append(
            {
                "asset": asset,
                "decision_rows": len(rows),
                "derive_trade_rows": sum(row["asset"] == asset and row["source"] == "derive" for row in trades),
                "healthy_rows": sum(row.get("data_health") == "HEALTHY" for row in rows),
                "reference_unavailable_rows": sum("REFERENCE" in str(row.get("data_health", "")) and row.get("data_health") != "HEALTHY" for row in rows),
                "derive_stale_rows": sum(row.get("data_health") == "DERIVE_STALE" for row in rows),
                "source_health_observations": sum(row["asset"] == asset for row in health_rows),
                "status": "READY" if rows else "DATA_INSUFFICIENT",
            }
        )
    _write_csv(report_dir / "data_health.csv", data_health_rows)
    _write_csv(report_dir / "latency.csv", latency_rows)

    latest_health_by_venue = {
        (str(row["asset"]), str(row["venue"])): str(row.get("latest_health", "UNOBSERVED"))
        for row in health_summary
    }
    reference_health_counts: dict[str, int] = defaultdict(int)
    derive_health_counts: dict[str, int] = defaultdict(int)
    for asset in assets:
        for venue in config.reference_venues:
            reference_health_counts[latest_health_by_venue.get((asset, venue), "UNOBSERVED")] += 1
        derive_health_counts[latest_health_by_venue.get((asset, "derive"), "UNOBSERVED")] += 1
    selected_multi_fills = [
        fill for fill in fills if _control_name(str(fill.get("model", ""))) == "MULTI_SOURCE_CONSENSUS"
    ]
    selected_multi_conservative = [
        fill for fill in selected_multi_fills if fill.get("model") == "MULTI_SOURCE_CONSENSUS:CONSERVATIVE"
    ]
    selected_multi_net_30 = [
        row for row in net_rows
        if row.get("model") == "MULTI_SOURCE_CONSENSUS:CONSERVATIVE"
        and int(row.get("horizon_seconds", 0)) == 30
    ]
    selected_model_state = (
        state.get("models", {}).get("MULTI_SOURCE_CONSENSUS:CONSERVATIVE", {})
        if isinstance(state, dict) and isinstance(state.get("models", {}), dict)
        else {}
    )
    pause_state: dict[str, bool] = {}
    reference_pause_events = 0
    for decision in decisions:
        asset = str(decision.get("asset", ""))
        paused = bool(decision.get("reference_pause_reason"))
        if paused and not pause_state.get(asset, False):
            reference_pause_events += 1
        pause_state[asset] = paused
    failover_rows = []
    failover_summary: dict[str, dict[str, int]] = {}
    for venue in config.reference_venues:
        venue_health_rows = [
            row for row in health_rows
            if row.get("venue") == venue
        ]
        latest_by_asset: dict[str, dict[str, Any]] = {}
        for row in venue_health_rows:
            asset_key = str(row.get("asset", ""))
            if asset_key not in assets:
                continue
            prior = latest_by_asset.get(asset_key)
            if prior is None or float(row.get("timestamp", 0)) >= float(prior.get("timestamp", 0)):
                latest_by_asset[asset_key] = row
        latest_reconnects = sum(
            int(row.get("reconnect_count", 0) or 0)
            for row in latest_by_asset.values()
        )
        eligible_rows = 0
        valid_rows = 0
        exclusion_rows = 0
        outlier_rows = 0
        stale_rows = 0
        single_source_periods = 0
        multi_source_periods = 0
        for decision in decisions:
            source_values = decision.get("source_fair_values", {})
            source_values = source_values if isinstance(source_values, dict) else {}
            valid_sources = set(decision.get("valid_reference_sources", []) or [])
            if venue in source_values:
                eligible_rows += 1
            if venue in valid_sources:
                valid_rows += 1
            else:
                exclusion_rows += 1
            if venue in set(decision.get("reference_outliers", []) or []):
                outlier_rows += 1
            source_health = decision.get("source_health", {})
            source_health = source_health if isinstance(source_health, dict) else {}
            if str((source_health.get(venue) or {}).get("health", "")) in {"STALE", "UNAVAILABLE"}:
                stale_rows += 1
            source_count = len(valid_sources)
            if source_count == 1:
                single_source_periods += 1
            elif source_count >= 2:
                multi_source_periods += 1
        failover_summary[venue] = {
            "eligible_rows": eligible_rows,
            "valid_rows": valid_rows,
            "exclusion_rows": exclusion_rows,
            "outlier_rows": outlier_rows,
            "stale_rows": stale_rows,
            "reconnect_count": latest_reconnects,
            "single_source_periods": single_source_periods,
            "multi_source_periods": multi_source_periods,
        }
        failover_rows.append({"venue": venue, **failover_summary[venue]})
    _write_csv(
        report_dir / "reference_failover_counters.csv",
        failover_rows,
        [
            "venue", "eligible_rows", "valid_rows", "exclusion_rows", "outlier_rows",
            "stale_rows", "reconnect_count", "single_source_periods", "multi_source_periods",
        ],
    )
    summary = {
        "total_assets": len(assets),
        "active_assets": sum(bool(mapping.valid) for mapping in mappings.values()),
        "reference_health": dict(reference_health_counts),
        "derive_health": dict(derive_health_counts),
        "shadow_fills": len(selected_multi_fills),
        "shadow_conservative_fills": len(selected_multi_conservative),
        "shadow_volume": sum(
            (_decimal(fill.get("amount")) or Decimal("0")) * (_decimal(fill.get("fill_price")) or Decimal("0"))
            for fill in selected_multi_fills
        ),
        "net_capture_proxy_bps": _mean(row.get("net_capture_proxy_bps") for row in selected_multi_net_30),
        "max_drawdown": selected_model_state.get("max_drawdown"),
        "reference_failover_counters": failover_summary,
        "reference_pause_events": reference_pause_events,
        "status": "LATEST_RUNTIME_SNAPSHOT",
    }
    pipeline_validation = []
    for row in asset_summary_rows:
        latest = latest_selected_decisions.get(row["asset"], {})
        checks = {
            "derive_bbo": row["derive_mid"] is not None,
            "reference_venue": row["source_count"] >= 1,
            "consensus": row["consensus_fair_value"] is not None,
            "fair_value": latest.get("fair_value") is not None,
            "market_mode": bool(row["market_mode"]),
            "inventory_mode": bool(row["inventory_mode"]),
            "shadow_quote": bool(row["shadow_bid"] or row["shadow_ask"]),
        }
        pipeline_validation.append(
            {
                "asset": row["asset"],
                **checks,
                "block_reason": latest.get("block_reason", ""),
                "status": "PASS" if all(checks.values()) else "FAIL",
            }
        )
    pipeline_failures = [row["asset"] for row in pipeline_validation if row["status"] != "PASS"]
    summary["pipeline_passed_assets"] = len(pipeline_validation) - len(pipeline_failures)
    summary["pipeline_failed_assets"] = len(pipeline_failures)

    # Compatibility summaries.
    _write_csv(report_dir / "fair_value_quality.csv", [{"asset": asset, "observations": len(by_asset.get(asset, [])), "fair_value_ready": sum(bool(row.get("fair_value")) for row in by_asset.get(asset, []))} for asset in assets])
    _write_csv(
        report_dir / "basis_statistics.csv",
        [
            {
                "asset": asset,
                "observations": len([row for row in basis_rows if row["asset"] == asset]),
                "median_basis_bps": _median(row.get("basis_bps") for row in basis_rows if row["asset"] == asset),
                "median_ewma_basis_bps": _median(row.get("ewma_basis_bps") for row in basis_rows if row["asset"] == asset),
            }
            for asset in assets
        ],
    )
    _write_csv(report_dir / "market_state_occupancy.csv", _occupancy(by_asset, "market_mode"))
    _write_csv(report_dir / "direction_state_occupancy.csv", _occupancy(by_asset, "direction"))
    _write_csv(report_dir / "inventory_state_occupancy.csv", _occupancy(by_asset, "inventory_mode"))
    _write_csv(report_dir / "quote_activity.csv", activity_rows)
    _write_csv(report_dir / "fills.csv", fills)
    _write_csv(report_dir / "binance_markouts.csv", markouts)
    _write_csv(report_dir / "toxicity.csv", [{"asset": row["asset"], "model": row["model"], "horizon_seconds": row["horizon_seconds"], "derive_markout_bps": row.get("derive_markout_bps"), "reference_markout_bps": row.get("binance_markout_bps")} for row in markouts])
    _write_csv(report_dir / "asset_comparison.csv", [{"asset": row["asset"], "median_spread_bps": row["median_spread_bps"], "fills": sum(fill["asset"] == row["asset"] for fill in fills), "quote_churn": next((item["creates"] + item["cancels"] for item in churn_rows if item["asset"] == row["asset"]), 0)} for row in spread_rows])
    _write_csv(report_dir / "protection_effectiveness.csv", protection_rows)
    _write_csv(report_dir / "control_comparison.csv", control_rows)

    safety = config.public_safety()
    metadata = run_metadata or {}
    duration = _decimal(metadata.get("duration_seconds"))
    classification = "NOT_READY_FOR_SMALL_MAINNET_CANARY"
    blockers = ["public shadow evidence is hypothetical and no live canary is authorized"]
    if not decisions:
        blockers.insert(0, "no decision observations")
    if not any(row.get("source_count", 0) for row in consensus_rows):
        blockers.insert(0, "no multi-source consensus observations")
    if not any(str(row.get("model", "")).endswith(":CONSERVATIVE") for row in fills):
        blockers.insert(0, "no conservative Derive trade-through fills; fill-dependent markouts are insufficient")
    if duration is not None and duration < 600:
        blockers.insert(0, "run duration is below the requested ten-minute pipeline denominator")
    if pipeline_failures:
        blockers.insert(0, f"pipeline quote gate failed for: {', '.join(pipeline_failures)}")
    report = {
        "report_version": "derive-multi-reference-shadow-v2",
        "safety": safety,
        "counters": {
            "decision_rows": len(decisions),
            "action_rows": len(actions),
            "fill_rows": len(fills),
            "markout_rows": len(markouts),
            "reference_health_rows": len(health_rows),
            "reference_value_rows": len(value_rows),
            "derive_trade_rows": len(trades),
            "real_orders": 0,
            "real_positions": 0,
        },
        "assets": assets,
        "mappings": {asset: json_safe(mapping) for asset, mapping in mappings.items()},
        "controls": {
            "DERIVE_ONLY": "same Derive observations; fair value equals Derive mid",
            "BINANCE_ONLY_REFERENCE": "same Derive observations; Binance public data only",
            "MULTI_SOURCE_CONSENSUS": "same Derive observations; robust median across eligible public sources",
        },
        "reference_venues": list(config.reference_venues),
        "summary": summary,
        "asset_summary": asset_summary_rows,
        "pipeline_validation": pipeline_validation,
        "control_comparison": control_rows,
        "classification": classification,
        "primary_blocker": "; ".join(dict.fromkeys(blockers)),
        "run_metadata": metadata,
        "public_discovery": mapping_report,
        "denominators": {
            "enabled_assets": len(assets),
            "configured_reference_venues": len(config.reference_venues),
            "control_portfolios": 6,
            "required_pipeline_seconds": 600,
            "reference_pause_events": reference_pause_events,
        },
        "artifact_files": list(REPORT_FILES),
    }
    (report_dir / "final_multi_reference_shadow_report.json").write_text(
        json.dumps(json_safe(report), indent=2, sort_keys=True), encoding="utf-8"
    )
    (report_dir / "final_multi_reference_shadow_report.md").write_text(
        _render_markdown(report, spread_rows, model_rows), encoding="utf-8"
    )
    return report


def _occupancy(by_asset: dict[str, list[dict[str, Any]]], key: str) -> list[dict[str, Any]]:
    rows = []
    for asset, decisions in by_asset.items():
        counts: dict[str, int] = defaultdict(int)
        for decision in decisions:
            counts[str(decision.get(key, "UNKNOWN"))] += 1
        rows.extend({"asset": asset, "state": state, "count": count} for state, count in sorted(counts.items()))
    return rows


def _render_markdown(report: dict[str, Any], spread_rows: list[dict[str, Any]], model_rows: list[dict[str, Any]]) -> str:
    safety = report["safety"]
    lines = [
        "# Derive Multi-Reference Adaptive MM Mainnet Shadow Report",
        "",
        "## Safety",
        "",
        f"- Environment: **{safety['environment'].upper()}**",
        f"- Mode: **{safety['mode']}**",
        f"- Mainnet armed: **{safety['mainnet_armed']}**",
        f"- Real orders: **{safety['real_orders']}**",
        f"- Real positions: **{safety['real_positions']}**",
        f"- Reference venues: **{', '.join(report['reference_venues'])}**",
        "- Derive is the only execution venue; all references are public data-only.",
        "",
        "## Asset observations",
        "",
        "| Asset | Decisions | Median Derive spread (bps) |",
        "|---|---:|---:|",
    ]
    for row in spread_rows:
        lines.append(f"| {row['asset']} | {row['observations']} | {row['median_spread_bps']} |")
    lines.extend(["", "## Control portfolios", "", "| Control | Fill model | Fills | Status |", "|---|---|---:|---|"])
    for row in model_rows:
        lines.append(f"| {row['reference_control']} | {row['fill_model']} | {row['fill_rows']} | {row['status']} |")
    lines.extend(
        [
            "",
            "## Pipeline validation",
            "",
            "| Asset | Derive BBO | Reference | Consensus | Fair value | Market | Inventory | Shadow quote | Status |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in report.get("pipeline_validation", []):
        lines.append(
            f"| {row['asset']} | {row['derive_bbo']} | {row['reference_venue']} | {row['consensus']} | "
            f"{row['fair_value']} | {row['market_mode']} | {row['inventory_mode']} | {row['shadow_quote']} | {row['status']} |"
        )
    lines.extend(
        [
            "",
            "## Reference reliability",
            "",
            "| Venue | Exclusions | Outliers | Stale rows | Reconnects | Single-source periods | Multi-source periods |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for venue, row in sorted(report["summary"].get("reference_failover_counters", {}).items()):
        lines.append(
            f"| {venue} | {row['exclusion_rows']} | {row['outlier_rows']} | {row['stale_rows']} | "
            f"{row['reconnect_count']} | {row['single_source_periods']} | {row['multi_source_periods']} |"
        )
    lines.extend(
        [
            "",
            f"Reference pause events: **{report['summary'].get('reference_pause_events', 0)}**",
            "",
            "## Interpretation",
            "",
            "Conservative fills require direction-aware strict Derive trade-through evidence. "
            "Touch sensitivity is reported separately and is not realized execution. "
            "Public reference liquidity is not executable Derive economics.",
            "",
            f"Classification: **{report['classification']}**",
            f"Primary blocker: {report['primary_blocker']}",
        ]
    )
    return "\n".join(lines) + "\n"
