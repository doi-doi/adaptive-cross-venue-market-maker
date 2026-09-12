from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import yaml

from derive_multi_asset_mm.accounting_repair import _fifo_equity, build_repair


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path, Path]:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "mode": "MAINNET_SHADOW",
                "dry_run": True,
                "mainnet_armed": False,
                "capital_usdc": 800,
                "maker_fee_bps": 1,
                "raw_retention_seconds": 180,
                "assets": {"XRP": {"enabled": True}},
            }
        ),
        encoding="utf-8",
    )
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "RUNNING",
                "mode": "MAINNET_SHADOW",
                "dry_run": True,
                "mainnet_armed": False,
                "active_assets": ["XRP"],
                "real_orders": 0,
                "real_positions": 0,
                "last_update": 100.5,
                "asset_fill_counts": {
                    "XRP": {
                        "DERIVE_ONLY:CONSERVATIVE": 0,
                        "DERIVE_ONLY:TOUCH_SENSITIVITY": 0,
                        "BINANCE_ONLY_NO_FAILOVER:CONSERVATIVE": 0,
                        "BINANCE_ONLY_NO_FAILOVER:TOUCH_SENSITIVITY": 0,
                        "PRIORITY_FAILOVER:CONSERVATIVE": 0,
                        "PRIORITY_FAILOVER:TOUCH_SENSITIVITY": 1,
                    }
                },
                "asset_fill_volume": {
                    "XRP": {
                        "DERIVE_ONLY:CONSERVATIVE": "0",
                        "DERIVE_ONLY:TOUCH_SENSITIVITY": "0",
                        "BINANCE_ONLY_NO_FAILOVER:CONSERVATIVE": "0",
                        "BINANCE_ONLY_NO_FAILOVER:TOUCH_SENSITIVITY": "0",
                        "PRIORITY_FAILOVER:CONSERVATIVE": "0",
                        "PRIORITY_FAILOVER:TOUCH_SENSITIVITY": "10.001",
                    }
                },
                "models": {"PRIORITY_FAILOVER:TOUCH_SENSITIVITY": {"fills": 1, "equity": "799.99"}},
                "latest_decisions": {"XRP": {"derive_bid": "1.0000", "derive_ask": "1.0002"}},
                "source_health": {"XRP": {"derive": {"health": "HEALTHY", "bbo_age": 0.1}}},
                "errors": [],
            }
        ),
        encoding="utf-8",
    )
    metadata_path = tmp_path / "run_metadata.json"
    metadata_path.write_text(
        json.dumps({"run_id": "fixture-run", "start_time_epoch": 90, "status": "RUNNING"}),
        encoding="utf-8",
    )
    mapping_path = tmp_path / "mapping.json"
    mapping_path.write_text(json.dumps({"mappings": {"XRP": {"rules": {"quote_asset": "USDC"}}}}), encoding="utf-8")
    rate_path = tmp_path / "rate.json"
    rate_path.write_text(
        json.dumps(
            {
                "live_probes": {
                    "public_get_all_instruments": {
                        "body": {
                            "result": {
                                "instruments": [
                                    {
                                        "instrument_name": "XRP-PERP",
                                        "instrument_type": "perp",
                                        "base_currency": "XRP",
                                        "quote_currency": "USDC",
                                        "tick_size": "0.00001",
                                        "amount_step": "0.1",
                                        "minimum_amount": "10",
                                        "maximum_amount": "1000000",
                                        "maker_fee_rate": "0.0001",
                                        "taker_fee_rate": "0.0003",
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    telemetry_path = tmp_path / "telemetry.sqlite"
    connection = sqlite3.connect(telemetry_path)
    connection.executescript(
        """
        CREATE TABLE fills (
            id INTEGER PRIMARY KEY, timestamp REAL, asset TEXT, side TEXT, amount TEXT,
            fill_price TEXT, binance_fair_value TEXT, derive_mid TEXT, inventory_before TEXT,
            inventory_after TEXT, maker_fee_bps TEXT, market_mode TEXT, direction TEXT,
            basis_bps TEXT, quoted_edge_bps TEXT, model TEXT, reference_control TEXT
        );
        CREATE TABLE markouts (
            id INTEGER PRIMARY KEY, fill_timestamp REAL, horizon_seconds INTEGER, asset TEXT,
            side TEXT, reference_price TEXT, derive_mid TEXT, binance_markout_bps TEXT,
            derive_markout_bps TEXT, model TEXT, reference_control TEXT
        );
        CREATE TABLE actions (
            id INTEGER PRIMARY KEY, timestamp REAL, asset TEXT, side TEXT, action TEXT,
            reason TEXT, order_id TEXT, price TEXT, amount TEXT, model TEXT
        );
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY, timestamp REAL, asset TEXT, source TEXT, trade_id TEXT,
            side TEXT, amount TEXT, price TEXT, exchange_timestamp REAL
        );
        CREATE TABLE decisions (id INTEGER PRIMARY KEY, timestamp REAL, asset TEXT, payload_json TEXT);
        """
    )
    connection.execute(
        "INSERT INTO fills VALUES (1,100,'XRP','SELL','10','1.0001','1.0000','1.0000','0','-10','1','NORMAL','NEUTRAL','0','5','PRIORITY_FAILOVER:TOUCH_SENSITIVITY','PRIORITY_FAILOVER')"
    )
    for index, horizon in enumerate((60,), 1):
        connection.execute(
            "INSERT INTO markouts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (index, 100, horizon, "XRP", "SELL", "1.0000", "1.0000", "1", "1", "PRIORITY_FAILOVER:TOUCH_SENSITIVITY", "PRIORITY_FAILOVER"),
        )
    connection.execute("INSERT INTO actions VALUES (1,99,'XRP','SELL','CREATE','READY_TO_CREATE','q1','1.0001','10','PRIORITY_FAILOVER:TOUCH_SENSITIVITY')")
    connection.execute("INSERT INTO trades VALUES (1,100,'XRP','derive','t1','SELL','10','1.0001',100)")
    connection.commit()
    connection.close()
    return config_path, state_path, telemetry_path, mapping_path, rate_path, metadata_path


def test_fifo_equity_uses_fees_and_equity_for_drawdown() -> None:
    fills = [
        {
            "asset": "XRP",
            "side": "SELL",
            "amount": Decimal("10"),
            "fill_price": Decimal("1.0"),
            "fee": Decimal("0.001"),
            "timestamp": 1.0,
            "derive_mid_at_fill": Decimal("1.0"),
        }
    ]
    result = _fifo_equity(fills, {"XRP": Decimal("0.99")}, Decimal("800"))
    assert result["current_shadow_equity"] == Decimal("800.099")
    assert result["cash_mark_to_market_equity"] == result["current_shadow_equity"]
    assert result["equity_peak"] >= result["current_shadow_equity"]


def test_build_repair_preserves_explicit_missing_horizon_and_reconciles(tmp_path: Path) -> None:
    config, state, telemetry, mapping, rate, metadata = _fixture(tmp_path)
    output = tmp_path / "repair"
    summary = build_repair(
        config_path=config,
        state_path=state,
        telemetry_path=telemetry,
        mapping_path=mapping,
        rate_limit_audit_path=rate,
        metadata_path=metadata,
        output_dir=output,
        now=100.5,
    )
    assert summary["run_id"] == "fixture-run"
    assert summary["tests"]["1_asset_conservative_sums_to_portfolio"]["status"] == "PASS"
    assert summary["tests"]["2_asset_touch_sums_to_portfolio"]["status"] == "PASS"
    assert summary["tests"]["3_asset_volume_sums_to_shadow_volume"]["status"] == "PASS"
    assert summary["tests"]["4_60s_markout_complete_or_explicit_reason"]["status"] == "PASS"
    assert summary["tests"]["5_equity_reconciles"]["status"] == "PASS"
    assert summary["tests"]["6_drawdown_derives_from_equity"]["status"] == "PASS"
    assert summary["tests"]["7_active_assets_have_current_rules"]["status"] == "PASS"
    assert summary["tests"]["8_dashboard_assets_equal_config"]["status"] == "PASS"
    assert summary["tests"]["9_dashboard_panels_current_run"]["status"] == "PASS"
    assert summary["tests"]["10_recovered_reconnect_never_uses_stale_bbo"]["status"] == "PASS"
    text = (output / "markout_pipeline_audit.csv").read_text(encoding="utf-8")
    assert "horizon_seconds" in text
    assert (output / "repair_summary.json").exists()
