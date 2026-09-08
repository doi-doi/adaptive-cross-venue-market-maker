from decimal import Decimal
from pathlib import Path

import pytest

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.public_data import (
    BinancePublicClient,
    DerivePublicClient,
    discover_mappings,
    parse_book_message,
)


def base_mapping_config(**overrides):
    raw = {"assets": {"ADA": {"enabled": True}}, "max_active_assets": 1, **overrides}
    return RuntimeConfig.from_mapping(raw)


def test_shadow_defaults_are_mainnet_and_unarmed():
    config = base_mapping_config()
    assert config.mode.value == "MAINNET_SHADOW"
    assert config.mainnet_armed is False
    assert config.derive_connector == "derive_perpetual"


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
    assert "<button" not in html.lower()
