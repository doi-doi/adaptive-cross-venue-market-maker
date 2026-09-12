import json
from dataclasses import replace
from decimal import Decimal as D

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.control import build_control_fair_value
from derive_multi_asset_mm.models import BookSnapshot
from derive_multi_asset_mm.priority import PriorityReferenceSelector
from derive_multi_asset_mm.priority_reporting import PRIORITY_REPORT_FILES, finalize_priority_reports
from derive_multi_asset_mm.reference import RobustBasis
from derive_multi_asset_mm.source_health import SourceHealth
from derive_multi_asset_mm.telemetry import TelemetryStore


def book(timestamp: float, price: str, source: str) -> BookSnapshot:
    return BookSnapshot(
        timestamp,
        D(price),
        D(price),
        D("1"),
        D("1"),
        ((D(price), D("1")),),
        ((D(price), D("1")),),
        source=source,
    )


def source(timestamp: float, price: str, venue: str) -> SourceHealth:
    health = SourceHealth()
    health.connect(timestamp)
    assert health.accept(book(timestamp, price, venue))
    return health


def select(selector, sources, now):
    books = {venue: health.book for venue, health in sources.items() if health.book is not None}
    return selector.select(
        "DOGE",
        books=books,
        health=sources,
        now=now,
        healthy_seconds=2,
        stale_seconds=5,
    )


def test_priority_failover_and_all_stale_pause_without_averaging():
    selector = PriorityReferenceSelector()
    sources = {
        "binance": source(0, "100", "binance"),
        "bybit": source(0, "101", "bybit"),
        "okx": source(0, "102", "okx"),
        "bitget": source(0, "99", "bitget"),
    }
    initial = select(selector, sources, 0)
    assert initial.selected_venue == "binance"
    assert initial.source_switch == ""
    assert initial.recovery_event == ""
    assert "bitget" in initial.fresh_venues
    assert initial.selected_venue != "bitget"
    assert initial.as_result()["robust_median"] is None
    assert initial.as_result()["selected_fair_value"] == D("100")

    assert sources["bybit"].accept(book(6, "101", "bybit"))
    assert sources["okx"].accept(book(6, "102", "okx"))
    failed_binance = select(selector, sources, 6)
    assert failed_binance.selected_venue == "bybit"
    assert failed_binance.failover_event == "binance_TO_bybit"

    assert sources["okx"].accept(book(12, "102", "okx"))
    failed_binance_bybit = select(selector, sources, 12)
    assert failed_binance_bybit.selected_venue == "okx"
    assert failed_binance_bybit.failover_event == "bybit_TO_okx"

    paused = select(selector, sources, 18)
    assert paused.selected_venue is None
    assert paused.pause_reason == "NO_FRESH_REFERENCE"


def test_binance_recovery_requires_healthy_hysteresis():
    selector = PriorityReferenceSelector(recovery_min_healthy_seconds=3)
    sources = {
        "binance": source(0, "100", "binance"),
        "bybit": source(0, "100", "bybit"),
        "okx": source(0, "100", "okx"),
    }
    assert select(selector, sources, 0).selected_venue == "binance"
    assert sources["bybit"].accept(book(6, "100", "bybit"))
    assert select(selector, sources, 6).selected_venue == "bybit"

    assert sources["binance"].accept(book(7, "100", "binance"))
    recovering = select(selector, sources, 7)
    assert recovering.selected_venue == "bybit"
    assert not recovering.recovery_ready
    assert sources["binance"].accept(book(9.9, "100", "binance"))
    assert select(selector, sources, 9.9).selected_venue == "bybit"
    assert sources["binance"].accept(book(10, "100", "binance"))
    recovered = select(selector, sources, 10)
    assert recovered.selected_venue == "binance"
    assert recovered.recovery_event == "RECOVERY_TO_BINANCE"


def test_priority_disagreement_pauses_primary_without_median():
    selector = PriorityReferenceSelector()
    sources = {
        "binance": source(0, "100", "binance"),
        "bybit": source(0, "101", "bybit"),
        "okx": source(0, "100", "okx"),
    }
    selection = select(selector, sources, 0)
    fair, result = build_control_fair_value(
        "PRIORITY_FAILOVER",
        derive_book=book(0, "100", "derive"),
        source_books={venue: health.book for venue, health in sources.items() if health.book is not None},
        source_health=sources,
        basis_tracker=RobustBasis(10, D("50")),
        now=0,
        healthy_seconds=2,
        stale_seconds=5,
        stale_overrides={},
        outlier_bps=D("50"),
        disagreement_bps=D("50"),
        minimum_sources=1,
        mid_weight=D("1"),
        microprice_weight=D("0"),
        max_levels=1,
        priority_selection=selection,
    )
    assert fair is None
    assert result["pause_reason"] == "REFERENCE_DISAGREEMENT_PAUSE"
    assert result["robust_median"] is None


def test_priority_config_defaults_to_three_assets_and_three_source_order():
    config = RuntimeConfig.from_mapping(
        {
            "reference_selection_mode": "PRIORITY_FAILOVER",
            "bitget_enabled": True,
            "reference_venues": ["binance", "bybit", "okx", "bitget"],
            "reference_priority": ["binance", "bybit", "okx"],
            "bitget_primary_enabled": False,
            "assets": {"DOGE": {}, "ADA": {}, "XRP": {}},
            "max_active_assets": 3,
        }
    )
    assert config.is_priority_failover
    assert config.reference_priority == ("binance", "bybit", "okx")
    assert config.control_models == ("DERIVE_ONLY", "BINANCE_ONLY_NO_FAILOVER", "PRIORITY_FAILOVER")


def test_priority_report_contract_writes_all_required_files(tmp_path):
    config = RuntimeConfig.from_mapping(
        {
            "reference_selection_mode": "PRIORITY_FAILOVER",
            "bitget_enabled": True,
            "reference_venues": ["binance", "bybit", "okx", "bitget"],
            "assets": {"DOGE": {}, "ADA": {}, "XRP": {}},
            "max_active_assets": 3,
        }
    )
    config = replace(
        config,
        report_dir=tmp_path / "reports",
        log_dir=tmp_path / "logs",
        database_path=tmp_path / "logs" / "telemetry.sqlite",
    )
    with TelemetryStore(config.database_path) as telemetry:
        report = finalize_priority_reports(
            config=config,
            mappings={},
            mapping_report={},
            telemetry=telemetry,
            run_metadata={"status": "DATA_INSUFFICIENT"},
        )
    assert set(PRIORITY_REPORT_FILES).issubset({path.name for path in config.report_dir.iterdir()})
    assert report["active_assets"] == ["DOGE", "ADA", "XRP"]
    assert report["reference_priority"] == ["binance", "bybit", "okx"]
    assert report["bitget_primary_enabled"] is False
    assert json.loads((config.report_dir / "final_report.json").read_text())['classification'] == "NOT_READY_FOR_SMALL_MAINNET_CANARY"


def test_priority_report_uses_retained_aggregates_after_raw_detail_pruning(tmp_path):
    config = RuntimeConfig.from_mapping(
        {
            "reference_selection_mode": "PRIORITY_FAILOVER",
            "reference_venues": ["binance", "bybit", "okx"],
            "assets": {"ADA": {"enabled": True}},
            "max_active_assets": 1,
            "raw_retention_seconds": 180,
            "feature_persist_interval_seconds": 1,
            "aggregate_interval_seconds": 60,
        }
    )
    config = replace(
        config,
        report_dir=tmp_path / "reports",
        log_dir=tmp_path / "logs",
        database_path=tmp_path / "logs" / "telemetry.sqlite",
    )
    payload = {
        "derive_mid": "100",
        "derive_spread_bps": "4",
        "reference_fair_value": "100",
        "basis_bps": "0",
        "selected_reference": "binance",
        "data_health": "HEALTHY",
        "controls": {},
    }
    with TelemetryStore(config.database_path, storage_config=config) as telemetry:
        telemetry.insert_decision(1.0, "ADA", payload)
        telemetry.insert_decision(2.0, "ADA", payload)
        telemetry.insert_decision(200.0, "ADA", payload)
        telemetry.insert_reference_health(1.0, "ADA", "binance", {"health": "HEALTHY"})
        telemetry.commit()

    from derive_multi_asset_mm.storage import prune_expired_raw_rows

    assert prune_expired_raw_rows(config.database_path, 100.0) >= 2
    with TelemetryStore(config.database_path, storage_config=config) as telemetry:
        report = finalize_priority_reports(
            config=config,
            mappings={},
            mapping_report={},
            telemetry=telemetry,
            run_metadata={"status": "DATA_INSUFFICIENT"},
        )

    spread = (config.report_dir / "spread_statistics.csv").read_text(encoding="utf-8")
    health = (config.report_dir / "reference_health.csv").read_text(encoding="utf-8")
    assert "AGGREGATE_DERIVED" in spread
    assert "healthy_observations" in health
    assert ",1,0,0,100.0," in health
    assert report["retention"]["aggregate_backed"] is True
    assert report["summary"]["decision_observations"] == 3
    assert (config.report_dir / "minute_aggregates.csv").is_file()
    assert (config.report_dir / "decision_rollups.csv").is_file()
