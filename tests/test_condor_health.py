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


def payload(xrp_state="SHADOW"):
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
        },
        "error_logs": [],
    }


def test_health_mapping_and_stale_alerts():
    assert module.health_snapshot(payload())["overall"] == "HEALTHY"
    assert module.health_snapshot(payload())["overview"]["total_pnl"] == 1
    assert module.health_snapshot(payload())["overview"]["total_volume"] == 10
    snapshot = module.health_snapshot(payload(xrp_state="REFERENCE_PAUSED"))
    assert snapshot["overall"] == "PAUSED"
    assert any(alert.startswith("BINANCE_STALE:XRP") for alert in snapshot["alerts"])
    assert set(module.health_snapshot(payload())["assets"]) == {"XRP"}
    assert all(alert.split(":", 2)[1] in module.ASSETS for alert in module.health_snapshot(payload())["alerts"] if ":" in alert)


def test_installed_api_success_envelope_is_unwrapped():
    snapshot = module.health_snapshot({"status": "success", "data": payload()})
    assert snapshot["bot_status"] == "RUNNING"
    assert snapshot["overall"] == "HEALTHY"


def test_process_down_is_critical_and_routine_is_read_only():
    snapshot = module.health_snapshot({"status": "STOPPED"})
    assert snapshot["overall"] == "CRITICAL"
    assert snapshot["alerts"] == ["PROCESS_DOWN"]
    assert module.Config().execution_enabled is False
    with pytest.raises(ValueError, match="read-only"):
        module.Config(execution_enabled=True)


def test_account_metrics_are_reported_from_xrp_controller():
    data = payload()
    data["controllers"]["xrp"]["custom_info"].update(
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
    assert overview["strategy_executor_pnl"] == 1


def test_unknown_strategy_pnl_and_fees_are_not_coerced_to_zero():
    data = payload()
    data["controllers"]["xrp"]["custom_info"].update(
        {
            "pnl": None,
            "strategy_executor_pnl": None,
            "drawdown": None,
            "strategy_executor_drawdown": None,
            "maker_fees_quote": None,
        }
    )

    overview = module.health_snapshot(data)["overview"]

    assert overview["strategy_executor_pnl"] is None
    assert overview["total_pnl"] is None
    assert overview["strategy_executor_drawdown"] is None
    assert overview["drawdown"] is None
    assert overview["maker_fees_quote"] is None
