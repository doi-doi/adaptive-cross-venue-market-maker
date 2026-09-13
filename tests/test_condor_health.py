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
            "xrp": {
                "custom_info": {
                    "asset": "XRP",
                    "operational_state": xrp_state,
                    "market_state": "NORMAL",
                    "shadow_mode": True,
                    "pnl": 1,
                    "volume": 10,
                }
            },
            "link": {
                "custom_info": {
                    "asset": "LINK",
                    "operational_state": link_state,
                    "market_state": "NORMAL",
                    "shadow_mode": True,
                    "pnl": 2,
                    "volume": 20,
                }
            },
        },
        "error_logs": [],
    }


def test_health_mapping_and_stale_alerts():
    assert module.health_snapshot(payload())["overall"] == "HEALTHY"
    assert module.health_snapshot(payload())["overview"]["total_pnl"] == 3
    assert module.health_snapshot(payload())["overview"]["total_volume"] == 30
    snapshot = module.health_snapshot(payload(xrp_state="REFERENCE_PAUSED"))
    assert snapshot["overall"] == "PAUSED"
    assert any(alert.startswith("BINANCE_STALE:XRP") for alert in snapshot["alerts"])
    snapshot = module.health_snapshot(payload(link_state="DERIVE_PAUSED"))
    assert any(alert.startswith("DERIVE_STALE:LINK") for alert in snapshot["alerts"])


def test_installed_api_success_envelope_is_unwrapped():
    snapshot = module.health_snapshot({"status": "success", "data": payload()})
    assert snapshot["bot_status"] == "RUNNING"
    assert snapshot["overall"] == "HEALTHY"


def test_process_down_is_critical_and_routine_is_read_only():
    assert module.health_snapshot({"status": "STOPPED"})["overall"] == "CRITICAL"
    assert module.Config().execution_enabled is False
    with pytest.raises(ValueError, match="read-only"):
        module.Config(execution_enabled=True)


def test_account_metrics_are_not_double_counted_across_controllers():
    data = payload()
    for controller_row in data["controllers"].values():
        controller_row["custom_info"].update(
            {
                "account_equity": 800,
                "account_collateral_balance": 780,
                "account_realized_pnl": 2,
                "account_unrealized_pnl": 3,
                "account_drawdown": 5,
                "account_gross_position_exposure": 120,
                "account_net_position_exposure": 20,
                "available_collateral": 700,
            }
        )
    overview = module.health_snapshot(data)["overview"]
    assert overview["account_equity"] == 800
    assert overview["account_collateral_balance"] == 780
    assert overview["account_realized_pnl"] == 2
    assert overview["account_unrealized_pnl"] == 3
    assert overview["account_drawdown"] == 5
    assert overview["account_gross_position_exposure"] == 120
    assert overview["account_net_position_exposure"] == 20
    assert overview["available_collateral"] == 700
    assert overview["strategy_executor_pnl"] == 3
