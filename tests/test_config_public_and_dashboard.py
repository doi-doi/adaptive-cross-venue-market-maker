import asyncio
import json
import os
import threading
import time
from decimal import Decimal
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.dashboard import make_handler
from derive_multi_asset_mm.models import AssetMapping
from derive_multi_asset_mm.public_data import (
    BinancePublicClient,
    DerivePublicClient,
    discover_mappings,
    parse_book_message,
    parse_trade_history_row,
)
from derive_multi_asset_mm.runner import ShadowRunner
from derive_multi_asset_mm.telemetry import TelemetryStore


def base_mapping_config(**overrides):
    raw = {"assets": {"ADA": {"enabled": True}}, "max_active_assets": 1, **overrides}
    return RuntimeConfig.from_mapping(raw)


def test_shadow_defaults_are_mainnet_and_unarmed():
    config = base_mapping_config()
    assert config.mode.value == "MAINNET_SHADOW"
    assert config.mainnet_armed is False
    assert config.derive_connector == "derive_perpetual"


def test_quote_refresh_policy_is_two_percent_from_derive_mid():
    config = base_mapping_config()
    assert config.refresh_tolerance_bps == Decimal("200")
    project_root = Path(__file__).resolve().parents[1]
    active = RuntimeConfig.from_yaml(project_root / "conf/mainnet_shadow_3asset_xrp_link_zec.yml")
    assert active.refresh_tolerance_bps == Decimal("200")


def test_no_bitget_successor_profile_excludes_bitget():
    project_root = Path(__file__).resolve().parents[1]
    config = RuntimeConfig.from_yaml(project_root / "conf/mainnet_shadow_3asset_6h_no_bitget.yml")
    assert config.reference_venues == ("binance", "bybit", "okx")
    assert config.reference_priority == ("binance", "bybit", "okx")
    assert config.bitget_enabled is False
    assert "bitget" not in config.reference_venues
    assert "bitget" not in config.reference_stale_overrides
    active = RuntimeConfig.from_yaml(project_root / "conf/mainnet_shadow_3asset_6h.yml")
    assert active.reference_venues == ("binance", "bybit", "okx")
    assert active.bitget_enabled is False


def test_bitget_disabled_config_cannot_schedule_bitget():
    with pytest.raises(ValueError, match="bitget_enabled=false"):
        base_mapping_config(bitget_enabled=False, reference_venues=["binance", "bybit", "okx", "bitget"])


def test_trade_history_poll_interval_is_configurable_and_positive():
    config = base_mapping_config(derive_trade_history_poll_seconds=7)
    assert config.derive_trade_history_poll_seconds == 7
    with pytest.raises(ValueError, match="derive_trade_history_poll_seconds"):
        base_mapping_config(derive_trade_history_poll_seconds=0)


def test_live_requires_explicit_arm_and_shadow_rejects_arm():
    with pytest.raises(ValueError, match="MAINNET_LIVE"):
        base_mapping_config(mode="MAINNET_LIVE", dry_run=False, mainnet_armed=False)
    with pytest.raises(ValueError, match="MAINNET_SHADOW"):
        base_mapping_config(mainnet_armed=True)
    with pytest.raises(ValueError, match="mainnet-only"):
        base_mapping_config(derive_connector="derive_perpetual_testnet")


def test_exact_binance_mapping_disables_missing_reference(monkeypatch):
    config = RuntimeConfig.from_mapping({"assets": {"ADA": {}, "CC": {}}, "max_active_assets": 2})
    monkeypatch.setattr(DerivePublicClient, "instruments", lambda self: [
        {"instrument_name": "ADA-PERP", "base_currency": "ADA", "instrument_type": "perp", "is_active": True, "tick_size": "0.0001", "amount_step": "0.1", "minimum_amount": "10"},
        {"instrument_name": "CC-PERP", "base_currency": "CC", "instrument_type": "perp", "is_active": True, "tick_size": "0.0001", "amount_step": "0.1", "minimum_amount": "10"},
    ])
    monkeypatch.setattr(DerivePublicClient, "ticker", lambda self, currency: {})
    monkeypatch.setattr(BinancePublicClient, "exchange_info", lambda self: [{"symbol": "ADAUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"}])
    mappings, _ = discover_mappings(config)
    assert mappings["ADA"].valid
    assert mappings["CC"].reason == "REFERENCE_MARKET_UNAVAILABLE"
    assert mappings["CC"].binance_symbol == "CCUSDT"


def test_derive_trade_history_paginates_with_direct_public_params(monkeypatch):
    calls = []

    def fake_post(method, params):
        calls.append((method, dict(params)))
        page = params["page"]
        return {
            "trades": [{"trade_id": f"trade-{page}"}],
            "pagination": {"num_pages": 2},
        }

    client = DerivePublicClient("https://api.lyra.finance")
    monkeypatch.setattr(client, "post", fake_post)
    rows = client.trade_history(
        "ada-perp",
        from_timestamp_ms=1000,
        to_timestamp_ms=2000,
        page_size=2000,
    )

    assert [row["trade_id"] for row in rows] == ["trade-1", "trade-2"]
    assert [method for method, _ in calls] == ["public/get_trade_history", "public/get_trade_history"]
    assert calls[0][1] == {
        "instrument_name": "ADA-PERP",
        "page": 1,
        "page_size": 1000,
        "from_timestamp": 1000,
        "to_timestamp": 2000,
    }
    assert calls[1][1]["page"] == 2


def test_derive_trade_history_accepts_only_taker_rows():
    taker = {
        "instrument_name": "ADA-PERP",
        "timestamp": 1788930000000,
        "trade_price": "0.34",
        "trade_amount": "25",
        "direction": "buy",
        "trade_id": "trade-1",
        "liquidity_role": "taker",
    }
    parsed = parse_trade_history_row(taker, receipt_timestamp=1788930001.0)
    assert parsed is not None
    assert parsed[0] == "ADA-PERP"
    assert parsed[1].source == "derive"
    assert parsed[1].exchange_timestamp == pytest.approx(1788930000.0)
    assert parse_trade_history_row({**taker, "liquidity_role": "maker"}) is None
    assert parse_trade_history_row(None) is None


def test_runner_backfills_taker_history_rows_into_trade_queue(tmp_path):
    config = base_mapping_config(
        database_path=str(tmp_path / "telemetry.sqlite"),
        log_dir=str(tmp_path / "logs"),
        report_dir=str(tmp_path / "reports"),
        derive_trade_history_poll_seconds=1,
    )
    runner = ShadowRunner(config)
    now_ms = int(time.time() * 1000)
    runner._trade_history_start_ms = now_ms - 1000

    class FakeDerivePublicClient:
        def trade_history(self, instrument_name, **kwargs):
            assert instrument_name == "ADA-PERP"
            assert kwargs["from_timestamp_ms"] <= now_ms
            return [
                {
                    "instrument_name": "ADA-PERP",
                    "timestamp": now_ms,
                    "trade_price": "0.34",
                    "trade_amount": "25",
                    "direction": "sell",
                    "trade_id": "trade-1",
                    "liquidity_role": "maker",
                },
                {
                    "instrument_name": "ADA-PERP",
                    "timestamp": now_ms,
                    "trade_price": "0.34",
                    "trade_amount": "25",
                    "direction": "buy",
                    "trade_id": "trade-1",
                    "liquidity_role": "taker",
                },
            ]

    runner.derive_public_client = FakeDerivePublicClient()
    queue = asyncio.Queue()
    mapping = AssetMapping("ADA", "ADA-PERP", "ADA-USDC", "ADAUSDT", valid=True)

    async def poll_once():
        await runner._poll_derive_trade_history(
            {"ADA": mapping}, queue, time.monotonic() + 0.05
        )

    try:
        asyncio.run(poll_once())
        kind, source, key, trade = queue.get_nowait()
        assert (kind, source, key) == ("trade", "derive", "ADA-PERP")
        assert trade.trade_id == "trade-1"
        assert runner.trade_feed_stats["ADA"]["rest_rows_seen"] == 2
        assert runner.trade_feed_stats["ADA"]["rest_taker_rows"] == 1
    finally:
        runner.telemetry.close()


def test_public_book_parser_normalizes_binance_depth():
    result = parse_book_message({"data": {"e": "depthUpdate", "s": "SOLUSDT", "E": 1000, "b": "100", "B": "2", "a": "101", "A": "3", "bids": [["100", "2"]], "asks": [["101", "3"]]}}, source="binance", receipt_timestamp=2)
    assert result is not None
    asset, book = result
    assert asset == "SOL"
    assert book.best_bid == Decimal("100")
    assert book.best_ask == Decimal("101")


def test_public_book_parser_accepts_binance_usdm_depth_arrays():
    result = parse_book_message(
        {
            "stream": "adausdt@depth5@100ms",
            "data": {"s": "ADAUSDT", "E": 1000, "b": [["100", "2"]], "a": [["101", "3"]]},
        },
        source="binance",
        receipt_timestamp=2,
    )
    assert result is not None
    assert result[1].best_bid == Decimal("100")
    assert result[1].ask_size == Decimal("3")


def test_dashboard_is_read_only_and_has_safety_banner():
    html = (Path(__file__).parents[1] / "dashboard/index.html").read_text(encoding="utf-8")
    assert "DERIVE MULTI-ASSET BINANCE-REFERENCE ADAPTIVE MM" in html
    assert "MAINNET SHADOW — NO REAL ORDERS." in html
    assert "setInterval(refresh, 1000)" in html
    assert "/api/minute-aggregates" in html
    assert "Bitget diagnostic" not in html
    assert "state.reference_venues || ['binance', 'bybit', 'okx']" in html
    assert "activeReferenceVenues" in html
    assert "allowedVenues.has(venue)" in html
    assert "/api/quote-fill-diagnostic" in html
    assert "queue residency proxy" in html.lower()
    assert "<button" not in html.lower()


def test_dashboard_serves_minute_aggregate_rows_from_six_hour_store(tmp_path):
    database = tmp_path / "logs" / "priority_reference_3asset_6h" / "telemetry.sqlite"
    config = base_mapping_config(database_path=str(database))
    timestamp = time.time()
    with TelemetryStore(database, storage_config=config) as telemetry:
        telemetry.insert_decision(
            timestamp,
            "ADA",
            {
                "derive_mid": "100",
                "derive_spread_bps": "4",
                "reference_fair_value": "100",
                "basis_bps": "0",
                "selected_reference": "binance",
                "desired_bid": "99",
                "desired_ask": "101",
                "quote_active": True,
            },
        )
        telemetry.commit()

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(tmp_path))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        connection.request("GET", "/api/minute-aggregates?asset=ADA&run=validation6h&window=3600")
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()

    assert response.status == 200
    assert payload["available"] is True
    assert payload["run"] == "validation6h"
    assert payload["rows"][0]["derive_mid"] == 100
    assert payload["rows"][0]["selected_reference"] == "binance"


def test_dashboard_selects_newest_isolated_six_hour_run(tmp_path):
    base_log = tmp_path / "logs" / "priority_reference_3asset_6h"
    base_report = tmp_path / "reports" / "priority_reference_3asset_6h"
    base_log.mkdir(parents=True)
    base_report.mkdir(parents=True)
    now = time.time()
    (base_log / "state.json").write_text(
        json.dumps({"status": "RUNNING", "started_at": now - 300, "last_update": now - 300, "pid": 999999}),
        encoding="utf-8",
    )

    run_id = "rerun_dashboard_test"
    child_log = base_log / run_id
    child_report = base_report / run_id
    child_log.mkdir()
    child_report.mkdir()
    database = child_log / "telemetry.sqlite"
    config = base_mapping_config(database_path=str(database))
    with TelemetryStore(database, storage_config=config) as telemetry:
        telemetry.insert_decision(
            now,
            "ADA",
            {
                "derive_mid": "100",
                "derive_spread_bps": "4",
                "reference_fair_value": "100",
                "basis_bps": "0",
                "selected_reference": "bybit",
                "desired_bid": "99",
                "desired_ask": "101",
                "quote_active": True,
            },
        )
        telemetry.commit()
    (child_log / "state.json").write_text(
        json.dumps(
            {
                "status": "RUNNING",
                "started_at": now,
                "last_update": now,
                "pid": os.getpid(),
                "active_assets": ["DOGE", "ADA", "XRP"],
                "mainnet_armed": False,
                "real_orders": 0,
                "real_positions": 0,
            }
        ),
        encoding="utf-8",
    )
    (child_report / "run_metadata.json").write_text(
        json.dumps({"run_id": run_id, "assets": ["DOGE", "ADA", "XRP"], "duration_seconds": 21600}),
        encoding="utf-8",
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(tmp_path))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        connection.request("GET", "/api/validation6h")
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        connection.request("GET", "/api/state")
        state_response = connection.getresponse()
        state_payload = json.loads(state_response.read())
        connection.close()
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()

    assert response.status == 200
    assert payload["run_id"] == run_id
    assert payload["aggregate_backed"] is True
    assert payload["assets"][1]["derive_mid"] == 100
    assert payload["assets"][1]["current_reference"] == "bybit"
    assert state_response.status == 200
    assert state_payload["pid"] == os.getpid()
