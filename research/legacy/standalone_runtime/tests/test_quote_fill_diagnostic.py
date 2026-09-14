import csv
import json
from pathlib import Path

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.lifecycle import LifecycleAction
from derive_multi_asset_mm.models import Side, TradePrint
from derive_multi_asset_mm.quote_fill_diagnostic import (
    DIAGNOSTIC_FILES,
    export_diagnostics,
    reconstruct_quote_lifecycles,
)
from derive_multi_asset_mm.telemetry import TelemetryStore


def _config(tmp_path: Path) -> RuntimeConfig:
    return RuntimeConfig.from_mapping(
        {
            "reference_selection_mode": "PRIORITY_FAILOVER",
            "reference_venues": ["binance", "bybit", "okx"],
            "reference_priority": ["binance", "bybit", "okx"],
            "bitget_enabled": False,
            "assets": {"ADA": {}},
            "max_active_assets": 1,
            "database_path": str(tmp_path / "telemetry.sqlite"),
            "log_dir": str(tmp_path / "logs"),
            "report_dir": str(tmp_path / "reports"),
        }
    )


def test_reconstruction_links_refresh_to_new_order_id():
    quotes, unmatched = reconstruct_quote_lifecycles(
        [
            {"id": 1, "timestamp": 1.0, "asset": "ADA", "side": "BUY", "action": "CREATE", "reason": "READY_TO_CREATE", "order_id": "q1", "price": "1", "amount": "1", "model": "PRIORITY_FAILOVER:CONSERVATIVE"},
            {"id": 2, "timestamp": 2.0, "asset": "ADA", "side": "BUY", "action": "CANCEL", "reason": "REFRESH_NEEDED", "order_id": "q1", "price": None, "amount": "0", "model": "PRIORITY_FAILOVER:CONSERVATIVE"},
            {"id": 3, "timestamp": 2.1, "asset": "ADA", "side": "BUY", "action": "CREATE", "reason": "READY_TO_CREATE", "order_id": "q2", "price": "1.01", "amount": "1", "model": "PRIORITY_FAILOVER:CONSERVATIVE"},
        ],
        [],
        3.0,
    )
    assert unmatched == 0
    assert quotes[0]["end_reason"] == "REPLACED"
    assert quotes[0]["replace_time"] == 2.1
    assert quotes[0]["replacement_new_price"] == 1.01
    assert quotes[0]["replacement_quote_id"] == quotes[1]["quote_id"]


def test_diagnostic_writes_required_files_and_flags_strict_crossing_without_fill(tmp_path):
    config = _config(tmp_path)
    telemetry = TelemetryStore(config.database_path)
    telemetry.insert_action(
        1.0,
        "ADA",
        "BUY",
        LifecycleAction("CREATE", "ADA", Side.BUY, "READY_TO_CREATE", "q1", price=1, amount=1),
        "PRIORITY_FAILOVER:CONSERVATIVE",
    )
    telemetry.insert_decision(
        1.5,
        "ADA",
        {
            "derive_bid": "0.99",
            "derive_ask": "1.01",
            "derive_spread_bps": "201",
            "reference_fair_value": "1",
            "fair_value": "1",
            "basis_bps": "0",
            "selected_reference": "binance",
            "data_health": "HEALTHY",
            "desired_bid": "1",
            "quote_active": True,
        },
    )
    telemetry.insert_decision(
        2.0,
        "ADA",
        {
            "derive_bid": "0.99",
            "derive_ask": "1.01",
            "derive_spread_bps": "201",
            "reference_fair_value": "1",
            "fair_value": "1",
            "basis_bps": "0",
            "selected_reference": "binance",
            "data_health": "HEALTHY",
            "desired_bid": "1",
            "quote_active": True,
        },
    )
    telemetry.insert_trade(TradePrint(2.5, 0.99, 1, Side.SELL, "trade-1", source="derive"), "ADA")
    telemetry.commit()
    telemetry.close()

    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "COMPLETE",
                "mode": "MAINNET_SHADOW",
                "started_at": 1.0,
                "ended_at": 3.0,
                "mainnet_armed": False,
                "dry_run": True,
                "real_orders": 0,
                "real_positions": 0,
                "active_assets": ["ADA"],
            }
        ),
        encoding="utf-8",
    )
    out_dir = tmp_path / "diagnostic"
    summary = export_diagnostics(config, config.database_path, state_path, out_dir)
    assert summary["assets"][0]["strict_crossings"] == 1
    assert summary["assets"][0]["root_cause"] == "FILL_LOGIC_MISMATCH"
    assert summary["assets"][0]["sample_status"] == "MORE_DATA_REQUIRED"
    assert summary["safety"]["mainnet_armed"] is False
    assert summary["restart_required_to_remove_bitget"] is False
    assert all((out_dir / name).exists() for name in DIAGNOSTIC_FILES)
    with (out_dir / "asset_root_cause.csv").open(newline="", encoding="utf-8") as handle:
        root_rows = list(csv.DictReader(handle))
    assert root_rows[0]["asset"] == "ADA"
    assert root_rows[0]["root_cause"] == "FILL_LOGIC_MISMATCH"
