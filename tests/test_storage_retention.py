import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.lifecycle import LifecycleAction
from derive_multi_asset_mm.models import Side
from derive_multi_asset_mm.storage import (
    StorageGovernor,
    StorageLevel,
    StoragePolicy,
    prune_expired_raw_rows,
)
from derive_multi_asset_mm.telemetry import TelemetryStore

_STORAGE_MODULE_SPEC = importlib.util.spec_from_file_location(
    "storage_maintenance", Path(__file__).parents[1] / "scripts" / "storage_maintenance.py"
)
assert _STORAGE_MODULE_SPEC and _STORAGE_MODULE_SPEC.loader
_STORAGE_MODULE = importlib.util.module_from_spec(_STORAGE_MODULE_SPEC)
_STORAGE_MODULE_SPEC.loader.exec_module(_STORAGE_MODULE)
build_manifest = _STORAGE_MODULE.build_manifest
execute_closed_cleanup = _STORAGE_MODULE.execute_closed_cleanup


def _config(tmp_path):
    return RuntimeConfig.from_mapping(
        {
            "assets": {"ADA": {"enabled": True}},
            "max_active_assets": 1,
            "database_path": str(tmp_path / "telemetry.sqlite"),
            "storage_warning_free_gb": 15,
            "storage_critical_free_gb": 10,
            "storage_emergency_free_gb": 5,
            "raw_retention_seconds": 180,
            "feature_persist_interval_seconds": 1,
            "aggregate_interval_seconds": 60,
            "chunk_rotation_minutes": 10,
        }
    )


def _payload():
    return {
        "derive_mid": "100",
        "derive_spread_bps": "4",
        "reference_fair_value": "100",
        "basis_bps": "0",
        "reference_control": "PRIORITY_FAILOVER",
        "selected_reference": "binance",
        "market_mode": "NORMAL",
        "direction": "NEUTRAL",
        "volatility": "NORMAL_VOL",
        "inventory_mode": "FLAT",
        "desired_bid": "99",
        "desired_ask": "101",
        "desired_bid_amount": "1",
        "desired_ask_amount": "1",
        "data_health": "HEALTHY",
        "controls": {},
    }


def test_storage_fields_have_required_defaults_and_validation(tmp_path):
    config = _config(tmp_path)
    assert config.raw_retention_seconds == 180
    assert config.feature_persist_interval_seconds == 1
    assert config.aggregate_interval_seconds == 60
    assert config.chunk_rotation_minutes == 10
    assert config.warning_run_storage_gb == 2
    assert config.critical_run_storage_gb == 2.5
    assert config.max_run_storage_gb == 3
    assert config.max_project_generated_data_gb == 10


def test_run_storage_budget_promotes_governor(monkeypatch, tmp_path):
    database = tmp_path / "logs" / "run" / "telemetry.sqlite"
    database.parent.mkdir(parents=True)
    database.write_bytes(b"x" * (2 * 1024 * 1024))
    monkeypatch.setattr(
        "derive_multi_asset_mm.storage.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=100 * 1024**3, total=200 * 1024**3, used=100 * 1024**3),
    )
    governor = StorageGovernor(
        database,
        StoragePolicy(
            warning_run_storage_gb=0.0005,
            critical_run_storage_gb=0.001,
            max_run_storage_gb=0.003,
            max_project_generated_data_gb=10,
        ),
    )

    assert governor.refresh(force=True) == StorageLevel.CRITICAL
    assert governor.current_run_storage_gb >= 0.001
    assert governor.snapshot()["event"] == "STORAGE_RUN_CRITICAL_BUDGET"


def test_project_generated_budget_promotes_governor(monkeypatch, tmp_path):
    database = tmp_path / "logs" / "run" / "telemetry.sqlite"
    database.parent.mkdir(parents=True)
    (tmp_path / "reports").mkdir()
    (tmp_path / "logs" / "raw.jsonl").write_bytes(b"x" * 2048)
    monkeypatch.setattr(
        "derive_multi_asset_mm.storage.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=100 * 1024**3, total=200 * 1024**3, used=100 * 1024**3),
    )
    governor = StorageGovernor(
        database,
        StoragePolicy(
            warning_run_storage_gb=1,
            critical_run_storage_gb=2,
            max_run_storage_gb=3,
            max_project_generated_data_gb=0.000001,
        ),
    )

    assert governor.refresh(force=True) == StorageLevel.CRITICAL
    assert governor.project_generated_data_gb > 0
    assert governor.snapshot()["event"] == "STORAGE_PROJECT_MAX_BUDGET"


def test_governor_compresses_unchanged_decisions_and_holds(monkeypatch, tmp_path):
    config = _config(tmp_path)
    monkeypatch.setattr(
        "derive_multi_asset_mm.storage.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=100 * 1024**3, total=200 * 1024**3, used=100 * 1024**3),
    )
    action = LifecycleAction("HOLD", "ADA", Side.BUY, "NO_OP_HOLD", "shadow-1")
    with TelemetryStore(config.database_path, storage_config=config) as telemetry:
        for timestamp in (100.0, 100.25, 100.5, 101.25):
            telemetry.insert_decision(timestamp, "ADA", _payload())
        for timestamp in (100.0, 100.25, 100.5):
            telemetry.insert_action(timestamp, "ADA", "BUY", action, "PRIORITY_FAILOVER:CONSERVATIVE")
        telemetry.commit()
        assert telemetry.count("decisions") == 2
        assert telemetry.count("decision_rollups") == 1
        assert telemetry.count("actions") == 0
        assert telemetry.count("minute_aggregates") == 1
        aggregate = telemetry.rows("minute_aggregates")[0]

    assert aggregate["observation_count"] == 4
    assert aggregate["decision_compressed_count"] == 2
    assert aggregate["holds"] == 3
    assert aggregate["feature_snapshot_count"] == 2
    assert json.loads(aggregate["action_counts_json"]) == {"HOLD": 3}
    assert aggregate["derive_mid_median"] == 100
    assert aggregate["derive_spread_bps_median"] == 4


def test_emergency_level_keeps_rollup_but_drops_unchanged_detail(monkeypatch, tmp_path):
    config = _config(tmp_path)
    monkeypatch.setattr(
        "derive_multi_asset_mm.storage.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=1 * 1024**3, total=200 * 1024**3, used=199 * 1024**3),
    )
    with TelemetryStore(config.database_path, storage_config=config) as telemetry:
        for timestamp in (100.0, 100.25, 100.5):
            telemetry.insert_decision(timestamp, "ADA", _payload())
        assert telemetry.governor.level_name == "EMERGENCY"
        assert telemetry.count("decisions") == 1
        assert telemetry.count("decision_rollups") == 1
        assert telemetry.rows("decision_rollups")[0]["count"] == 3


def test_minute_aggregate_persists_median_market_metrics(tmp_path):
    config = _config(tmp_path)
    with TelemetryStore(config.database_path, storage_config=config) as telemetry:
        for timestamp, mid, spread, fair, basis in (
            (100.0, "101", "5", "102", "1"),
            (100.25, "99", "3", "98", "-1"),
            (100.5, "100", "4", "100", "0"),
        ):
            payload = {
                **_payload(),
                "derive_mid": mid,
                "derive_spread_bps": spread,
                "reference_fair_value": fair,
                "basis_bps": basis,
            }
            telemetry.insert_decision(timestamp, "ADA", payload)
        aggregate = telemetry.rows("minute_aggregates")[0]

    assert aggregate["derive_mid_median"] == 100
    assert aggregate["derive_spread_bps_median"] == 4
    assert aggregate["reference_fair_value_median"] == 100
    assert aggregate["basis_bps_median"] == 0


def test_minute_aggregate_retains_trade_and_fill_counters(tmp_path):
    config = _config(tmp_path)
    with TelemetryStore(config.database_path, storage_config=config) as telemetry:
        snapshot = telemetry.storage_snapshot()
        assert snapshot["governor_enabled"] is True
        assert snapshot["policy"]["raw_retention_seconds"] == 180


def test_raw_pruner_requires_rollup_and_keeps_critical_decisions(tmp_path):
    config = _config(tmp_path)
    database = Path(config.database_path)
    with TelemetryStore(database, storage_config=config) as telemetry:
        ordinary = _payload()
        critical = {**ordinary, "block_reason": "REFERENCE_PAUSED"}
        telemetry.insert_decision(1.0, "ADA", ordinary)
        telemetry.insert_decision(2.0, "ADA", critical)
        telemetry.insert_decision(200.0, "ADA", ordinary)
        telemetry.insert_reference_health(1.0, "ADA", "binance", {"health": "HEALTHY"})
        telemetry.insert_reference_value(
            timestamp=1.0,
            asset="ADA",
            venue="binance",
            fair_value="100",
            mid="100",
            microprice="100",
            health="HEALTHY",
            bbo_age="0",
            deviation_bps="0",
            valid=True,
        )
        telemetry.commit()

    assert prune_expired_raw_rows(database, 100.0, batch_size=50) == 3
    with TelemetryStore(database) as telemetry:
        assert telemetry.count("decisions") == 2
        assert telemetry.count("decision_rollups") == 3
        assert telemetry.count("reference_health") == 0
        assert telemetry.count("reference_values") == 0


def test_closed_completed_database_is_archived_only_after_manifest(tmp_path):
    root = Path(tmp_path)
    log_dir = root / "logs" / "completed"
    report_dir = root / "reports" / "completed"
    database = log_dir / "telemetry.sqlite"
    log_dir.mkdir(parents=True)
    report_dir.mkdir(parents=True)
    (log_dir / "state.json").write_text(json.dumps({"status": "COMPLETE", "pid": 1}), encoding="utf-8")
    (report_dir / "final_report.json").write_text("{}", encoding="utf-8")
    with TelemetryStore(database) as telemetry:
        telemetry.set_state("runtime", {"status": "COMPLETE"}, 1)

    manifest = build_manifest(root)
    entry = next(item for item in manifest["entries"] if item["path"] == "logs/completed/telemetry.sqlite")
    assert entry["planned_action"] == "ARCHIVE_ZSTD_VERIFY_THEN_REMOVE_SOURCE"
    result = execute_closed_cleanup(root, manifest)
    assert result["recovered_bytes"] > 0
    assert not database.exists()
    assert Path(str(database) + ".zst").is_file()
