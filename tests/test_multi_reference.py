from decimal import Decimal as D

import derive_multi_asset_mm.multi_public as multi_public
from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.control import build_control_fair_value
from derive_multi_asset_mm.models import BookSnapshot
from derive_multi_asset_mm.multi_public import discover_references_with_report, parse_snapshot
from derive_multi_asset_mm.reference import RobustBasis
from derive_multi_asset_mm.shadow_engine import ShadowEngine
from derive_multi_asset_mm.source_health import SourceHealth
from derive_multi_asset_mm.telemetry import TelemetryStore


def book(timestamp=10, bid="99", ask="101", source="test"):
    return BookSnapshot(timestamp, D(bid), D(ask), D("2"), D("1"), ((D(bid), D("2")),), ((D(ask), D("1")),), source=source)


def test_reference_parsers_keep_exact_symbol_and_okx_contract_multiplier():
    mapping = {
        "CC-USDT-SWAP": {
            "asset": "CC", "status": "READY", "amount_multiplier": "10",
        },
        "CCUSDT": {"asset": "CC", "status": "READY", "amount_multiplier": "1"},
    }
    okx = {
        "arg": {"channel": "books5", "instId": "CC-USDT-SWAP"},
        "data": [{"asks": [["101", "3"]], "bids": [["99", "2"]], "seqId": 10, "ts": "10000"}],
    }
    asset, parsed, sequence, previous, repeat = parse_snapshot("okx", okx, 20, mapping)
    assert asset == "CC"
    assert parsed.ask_size == D("30")
    assert sequence == 10 and previous is None and not repeat

    bybit = {
        "topic": "orderbook.1.CCUSDT", "type": "snapshot", "ts": 10000,
        "data": {"s": "CCUSDT", "u": 12, "b": [["99", "2"]], "a": [["101", "1"]]},
    }
    assert parse_snapshot("bybit", bybit, 21, mapping)[-1] is True


def test_multi_source_control_uses_same_books_and_separate_basis():
    books = {venue: book(source=venue) for venue in ("binance", "bybit", "okx", "bitget")}
    health = {}
    for venue, snapshot in books.items():
        source = SourceHealth()
        source.connect(10)
        assert source.accept(snapshot)
        health[venue] = source
    fair, result = build_control_fair_value(
        "MULTI_SOURCE_CONSENSUS",
        derive_book=book(bid="98", ask="100", source="derive"),
        source_books=books,
        source_health=health,
        basis_tracker=RobustBasis(10, D("50")),
        now=10,
        healthy_seconds=2,
        stale_seconds=5,
        stale_overrides={},
        outlier_bps=D("50"),
        disagreement_bps=D("25"),
        minimum_sources=2,
        mid_weight=D("0.5"),
        microprice_weight=D("0.5"),
        max_levels=1,
    )
    assert fair is not None
    assert result["valid_sources"] == ["binance", "bybit", "okx", "bitget"]
    assert fair.reference_control == "MULTI_SOURCE_CONSENSUS"
    assert set(fair.source_fair_values) == set(books)


def test_shadow_engine_has_six_isolated_control_portfolios(tmp_path):
    with TelemetryStore(tmp_path / "telemetry.sqlite") as telemetry:
        engine = ShadowEngine(D("800"), D("1"), telemetry)
        assert set(engine.models) == {
            "DERIVE_ONLY:CONSERVATIVE", "DERIVE_ONLY:TOUCH_SENSITIVITY",
            "BINANCE_ONLY_REFERENCE:CONSERVATIVE", "BINANCE_ONLY_REFERENCE:TOUCH_SENSITIVITY",
            "MULTI_SOURCE_CONSENSUS:CONSERVATIVE", "MULTI_SOURCE_CONSENSUS:TOUCH_SENSITIVITY",
        }


def test_multi_reference_config_has_all_requested_venues_and_assets():
    config = RuntimeConfig.from_mapping({
        "multi_reference": True,
        "reference_venues": ["binance", "bybit", "okx", "bitget"],
        "minimum_reference_sources": 2,
        "assets": {asset: {} for asset in ("ADA", "CC", "XRP", "SOL", "LINK", "DOGE", "BNB", "HYPE")},
        "max_active_assets": 8,
    })
    assert config.reference_venues == ("binance", "bybit", "okx", "bitget")
    assert [asset.symbol for asset in config.enabled_assets] == ["ADA", "CC", "XRP", "SOL", "LINK", "DOGE", "BNB", "HYPE"]


def test_reference_discovery_falls_back_to_exact_public_metadata(monkeypatch):
    config = RuntimeConfig.from_mapping(
        {
            "reference_venues": ["binance"],
            "reference_priority": ["binance"],
            "assets": {"DOGE": {}},
            "max_active_assets": 1,
        }
    )
    monkeypatch.setattr(multi_public, "_connector_maps", lambda: ([], ["connector_discovery:daemon_unavailable"]))
    monkeypatch.setattr(
        multi_public,
        "_metadata_by_venue",
        lambda _config: (
            {
                "binance": {
                    "DOGEUSDT": {
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "quoteAsset": "USDT",
                        "baseAsset": "DOGE",
                    }
                }
            },
            [],
        ),
    )

    rows, report = discover_references_with_report(["DOGE"], config)

    assert rows[0]["symbol"] == "DOGEUSDT"
    assert rows[0]["status"] == "READY"
    assert rows[0]["symbol_source"] == "PUBLIC_METADATA_EXACT_FALLBACK"
    assert report["connector_errors"] == ["connector_discovery:daemon_unavailable"]
