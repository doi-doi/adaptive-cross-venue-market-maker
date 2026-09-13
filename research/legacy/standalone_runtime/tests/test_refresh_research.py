import json
from decimal import Decimal

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.lifecycle import QuoteReconciler
from derive_multi_asset_mm.models import Side, TradePrint
from derive_multi_asset_mm.refresh_governor import (
    ActionPriority,
    ActionRequest,
    OrderActionGovernor,
    is_adverse_fast_move,
)
from derive_multi_asset_mm.refresh_research import analyze_refresh_research
from derive_multi_asset_mm.telemetry import TelemetryStore


def test_governor_keeps_latest_normal_request_and_prioritizes_emergency():
    governor = OrderActionGovernor(max_actions_per_second=Decimal("1"), max_actions_per_minute=30)
    governor.submit(ActionRequest("XRP", "BUY", "CREATE", "NORMAL", ActionPriority.NORMAL_REFRESH, 1.0, price=Decimal("1")))
    governor.submit(ActionRequest("XRP", "BUY", "CREATE", "NORMAL", ActionPriority.NORMAL_REFRESH, 2.0, price=Decimal("2")))
    governor.submit(ActionRequest("XRP", "SELL", "CANCEL", "FAST_ADVERSE_MOVE_OVERRIDE", ActionPriority.EMERGENCY_CANCEL, 2.0))
    accepted = governor.drain(2.0)
    assert [request.reason for request in accepted] == ["FAST_ADVERSE_MOVE_OVERRIDE"]
    assert governor.pending()[0].price == Decimal("2")
    assert is_adverse_fast_move(Side.BUY, Decimal("-9"), Decimal("8"))
    assert not is_adverse_fast_move(Side.BUY, Decimal("9"), Decimal("8"))


def test_deadband_residency_and_adverse_override_are_asymmetric():
    reconciler = QuoteReconciler()
    create = reconciler.reconcile(
        asset="XRP",
        side=Side.BUY,
        desired_price=Decimal("99.00"),
        desired_amount=Decimal("10"),
        now=1.0,
        max_age_seconds=Decimal("30"),
        tolerance_bps=Decimal("200"),
        refresh_deadband_bps=Decimal("2"),
        minimum_normal_quote_residency_seconds=Decimal("1"),
        tick_size=Decimal("0.01"),
    )
    reconciler.acknowledge_create(create)
    small_move = reconciler.reconcile(
        asset="XRP", side=Side.BUY, desired_price=Decimal("99.01"), desired_amount=Decimal("10"), now=2.0,
        max_age_seconds=Decimal("30"), tolerance_bps=Decimal("200"), refresh_deadband_bps=Decimal("2"),
        minimum_normal_quote_residency_seconds=Decimal("1"), tick_size=Decimal("0.01"),
    )
    assert small_move.reason == "NO_OP_HOLD"
    large_move = reconciler.reconcile(
        asset="XRP", side=Side.BUY, desired_price=Decimal("99.10"), desired_amount=Decimal("10"), now=2.0,
        max_age_seconds=Decimal("30"), tolerance_bps=Decimal("200"), refresh_deadband_bps=Decimal("2"),
        minimum_normal_quote_residency_seconds=Decimal("1"), tick_size=Decimal("0.01"),
    )
    assert large_move.reason == "DEADBAND_REFRESH"
    reconciler.acknowledge_cancel(large_move)
    recreated = reconciler.reconcile(
        asset="XRP", side=Side.BUY, desired_price=Decimal("99.10"), desired_amount=Decimal("10"), now=3.0,
        max_age_seconds=Decimal("30"), tolerance_bps=Decimal("200"), refresh_deadband_bps=Decimal("2"),
        minimum_normal_quote_residency_seconds=Decimal("1"), tick_size=Decimal("0.01"),
    )
    reconciler.acknowledge_create(recreated)
    emergency = reconciler.reconcile(
        asset="XRP", side=Side.BUY, desired_price=Decimal("99.10"), desired_amount=Decimal("10"), now=3.1,
        max_age_seconds=Decimal("30"), tolerance_bps=Decimal("200"), refresh_deadband_bps=Decimal("30"),
        minimum_normal_quote_residency_seconds=Decimal("5"), fast_adverse_move_bps=Decimal("-8"),
        fast_adverse_move_threshold_bps=Decimal("8"), fast_adverse_move_override=True, tick_size=Decimal("0.01"),
    )
    assert emergency.reason == "FAST_ADVERSE_MOVE_OVERRIDE"


def test_refresh_analyzer_writes_complete_artifact_set_without_touching_telemetry(tmp_path):
    telemetry_path = tmp_path / "telemetry.sqlite"
    config = RuntimeConfig.from_mapping(
        {
            "assets": {"XRP": {"enabled": True}},
            "max_active_assets": 1,
            "report_dir": str(tmp_path / "reports"),
            "log_dir": str(tmp_path / "logs"),
            "database_path": str(telemetry_path),
            "refresh_deadband_bps": 0,
            "fast_adverse_move_override_enabled": True,
        }
    )
    payload = {
        "derive_bid": "99",
        "derive_ask": "101",
        "derive_spread_bps": "202.02",
        "reference_fair_value": "100",
        "fair_value": "100",
        "selected_reference": "binance",
        "controls": {
            "PRIORITY_FAILOVER": {
                "fair_value": {"derive_fair_value": "100"},
                "state": {"market_mode": "NORMAL", "return_1s": "0"},
                "plan": {"bid_price": "99", "bid_amount": "10", "ask_price": "101", "ask_amount": "10"},
            }
        },
    }
    with TelemetryStore(telemetry_path) as telemetry:
        for timestamp in (100.0, 101.0, 106.0):
            telemetry.insert_decision(timestamp, "XRP", payload)
        telemetry.insert_trade(TradePrint(100.5, Decimal("98.9"), Decimal("10"), Side.SELL, "t1", source="derive"), "XRP")
        telemetry.commit()
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"status": "COMPLETE", "started_at": 100.0, "ended_at": 106.0}), encoding="utf-8")
    mapping_path = tmp_path / "mapping.json"
    mapping_path.write_text(
        json.dumps({"mappings": {"XRP": {"rules": {"instrument_name": "XRP-PERP", "quote_asset": "USDC", "tick_size": "0.00001", "amount_step": "0.1", "minimum_amount": "10", "maximum_amount": "1000000", "minimum_notional": "0", "maker_fee_bps": "1", "taker_fee_bps": "3"}}}}),
        encoding="utf-8",
    )
    before = telemetry_path.stat().st_size
    out_dir = tmp_path / "refresh"
    result = analyze_refresh_research(config, [telemetry_path], out_dir, state_path=state_path, mapping_path=mapping_path)
    required = {
        "asset_trading_rules.csv", "capital_compatibility.csv", "derive_rate_limit_audit.md", "derive_rate_limit_audit.json",
        "trade_activity.csv", "spread_statistics.csv", "quote_lifetime.csv", "queue_residency_proxy.csv",
        "quote_mutation_rate.csv", "rate_limit_utilization.csv", "replacement_reasons.csv", "churn_missed_fills.csv",
        "deadband_comparison.csv", "residency_comparison.csv", "deadband_residency_matrix.csv", "markout_by_variant.csv",
        "net_capture_by_variant.csv", "stale_quote_risk.csv", "asset_recommendations.csv", "final_refresh_research.md", "final_refresh_research.json",
    }
    assert required.issubset({path.name for path in out_dir.iterdir()})
    assert result["classification"] == "REFRESH_RESEARCH_DATA_INSUFFICIENT"
    assert telemetry_path.stat().st_size == before
