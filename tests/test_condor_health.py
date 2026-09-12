import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

PATH = Path(__file__).parents[1] / "condor/derive_mm_health.py"
spec = spec_from_file_location("derive_mm_health", PATH)
module = module_from_spec(spec)
sys.modules[spec.name] = module
assert spec.loader is not None
spec.loader.exec_module(module)


def payload(xrp_state="SHADOW", link_state="SHADOW"):
    return {
        "status": "RUNNING",
        "controllers": {
            "xrp": {"custom_info": {"asset": "XRP", "operational_state": xrp_state, "market_state": "NORMAL"}},
            "link": {"custom_info": {"asset": "LINK", "operational_state": link_state, "market_state": "NORMAL"}},
        },
        "error_logs": [],
    }


def test_health_mapping_and_stale_alerts():
    assert module.health_snapshot(payload())["overall"] == "HEALTHY"
    snapshot = module.health_snapshot(payload(xrp_state="REFERENCE_PAUSED"))
    assert snapshot["overall"] == "PAUSED"
    assert any(alert.startswith("BINANCE_STALE:XRP") for alert in snapshot["alerts"])
    snapshot = module.health_snapshot(payload(link_state="DERIVE_PAUSED"))
    assert any(alert.startswith("DERIVE_STALE:LINK") for alert in snapshot["alerts"])


def test_process_down_is_critical_and_routine_is_read_only():
    assert module.health_snapshot({"status": "STOPPED"})["overall"] == "CRITICAL"
    assert module.Config().execution_enabled is False
    with pytest.raises(ValueError, match="read-only"):
        module.Config(execution_enabled=True)
