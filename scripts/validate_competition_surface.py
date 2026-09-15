"""Fail closed if retired runtime artifacts leak into the competition surface."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CURRENT_CONFIGS = {
    "derive_binance_adaptive_mm_xrp.yml": ("XRP", "XRP-USDC", "XRP-USDT"),
}
CANARY_CONFIGS = {
    "derive_binance_adaptive_mm_xrp_live_canary.yml": ("XRP", "XRP-USDC", "XRP-USDT"),
}


def main() -> None:
    for retired_root in ("src", "conf", "dashboard"):
        if (ROOT / retired_root).exists():
            raise SystemExit(f"retired root-level runtime surface exists: {retired_root}")

    reports = ROOT / "reports"
    if reports.exists():
        allowed_reports = {"volume_focused_mm_report.md", "volume_focused_mm_report.json"}
        unexpected_reports = {
            path.name for path in reports.iterdir() if path.is_file() and path.name not in allowed_reports
        }
        if unexpected_reports:
            raise SystemExit(f"unexpected root-level reports: {sorted(unexpected_reports)}")

    config_dir = ROOT / "configs"
    controller_files = {path.name for path in config_dir.glob("derive_binance_adaptive_mm_*.yml")}
    if controller_files != set(CURRENT_CONFIGS) | set(CANARY_CONFIGS):
        raise SystemExit(f"unexpected current controller configs: {sorted(controller_files)}")
    for filename, (asset, execution_pair, reference_pair) in CURRENT_CONFIGS.items():
        row = yaml.safe_load((config_dir / filename).read_text(encoding="utf-8"))
        expected = {
            "asset": asset,
            "connector_name": "derive_perpetual",
            "reference_connector_name": "binance_perpetual",
            "trading_pair": execution_pair,
            "reference_trading_pair": reference_pair,
            "shadow_mode": True,
            "mainnet_armed": False,
            "allow_position_flips": False,
            "max_account_drawdown_quote": 40,
        }
        mismatches = {key: (row.get(key), value) for key, value in expected.items() if row.get(key) != value}
        if mismatches:
            raise SystemExit(f"unsafe or unexpected {filename}: {mismatches}")

    for filename, (asset, execution_pair, reference_pair) in CANARY_CONFIGS.items():
        row = yaml.safe_load((config_dir / filename).read_text(encoding="utf-8"))
        expected = {
            "asset": asset,
            "connector_name": "derive_perpetual",
            "reference_connector_name": "binance_perpetual",
            "trading_pair": execution_pair,
            "reference_trading_pair": reference_pair,
            "shadow_mode": True,
            "mainnet_armed": False,
            "allow_position_flips": False,
            "order_amount_quote": 25,
            "one_sided_inventory_ratio": 0.5,
            "max_asset_inventory_quote": 50,
            "max_asset_open_order_quote": 50,
            "max_total_inventory_quote": 50,
            "max_total_open_order_quote": 50,
        }
        mismatches = {key: (row.get(key), value) for key, value in expected.items() if row.get(key) != value}
        if mismatches:
            raise SystemExit(f"unsafe or unexpected {filename}: {mismatches}")

    bot = yaml.safe_load((config_dir / "v2_with_controllers.yml").read_text(encoding="utf-8"))
    if bot.get("controllers_config") != list(CURRENT_CONFIGS):
        raise SystemExit("v2_with_controllers.yml must load exactly the XRP config")

    legacy = ROOT / "research" / "legacy" / "standalone_runtime"
    for required in ("src", "conf", "dashboard", "scripts", "tests"):
        if not (legacy / required).is_dir():
            raise SystemExit(f"legacy archive incomplete: {required}")
    print("competition surface: XRP-only, Binance reference, Derive execution, shadow/disarmed")


if __name__ == "__main__":
    main()
