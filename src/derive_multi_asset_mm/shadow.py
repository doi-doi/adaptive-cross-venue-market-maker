"""Detached mainnet-shadow CLI.

The command surface deliberately has no live-start or order command. A future
live activation remains a separately authorized Hummingbot operation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from .config import RuntimeConfig
from .models import AssetMapping, DeriveRules
from .reporting import finalize_reports
from .runner import ShadowRunner
from .telemetry import TelemetryStore


def parse_duration(value: str) -> float:
    text = str(value).strip().lower()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if text[-1:] in units:
        number, unit = text[:-1], text[-1]
        result = float(number) * units[unit]
    else:
        result = float(text)
    if result <= 0:
        raise ValueError("duration must be positive")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Derive multi-asset Binance-reference mainnet shadow")
    parser.add_argument("command", choices=("start", "status", "audit", "finalize"))
    parser.add_argument("--config", default="conf/mainnet_shadow.yml")
    parser.add_argument("--duration", default="30m")
    parser.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    return parser


def _state_path(config: RuntimeConfig) -> Path:
    return config.log_dir / "state.json"


def _load_mapping_report(path: Path) -> tuple[dict[str, AssetMapping], dict]:
    if not path.exists():
        return {}, {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw_mappings = raw.get("mappings", {}) if isinstance(raw, dict) else {}
    mappings = {}
    for asset, value in raw_mappings.items():
        rules_raw = value.get("rules") if isinstance(value, dict) else None
        rules = None
        if isinstance(rules_raw, dict):
            rules = DeriveRules(
                instrument_name=str(rules_raw.get("instrument_name", "")),
                base_asset=str(rules_raw.get("base_asset", asset)),
                quote_asset=str(rules_raw.get("quote_asset", "USD")),
                tick_size=Decimal(str(rules_raw.get("tick_size", "0"))),
                amount_step=Decimal(str(rules_raw.get("amount_step", "0"))),
                minimum_amount=Decimal(str(rules_raw.get("minimum_amount", "0"))),
                maximum_amount=(Decimal(str(rules_raw["maximum_amount"])) if rules_raw.get("maximum_amount") is not None else None),
                minimum_notional=Decimal(str(rules_raw.get("minimum_notional", "0"))),
                maker_fee_bps=(Decimal(str(rules_raw["maker_fee_bps"])) if rules_raw.get("maker_fee_bps") is not None else None),
                taker_fee_bps=(Decimal(str(rules_raw["taker_fee_bps"])) if rules_raw.get("taker_fee_bps") is not None else None),
            )
        mappings[asset] = AssetMapping(
            asset=asset,
            derive_instrument=value.get("derive_instrument"),
            derive_pair=value.get("derive_pair"),
            binance_symbol=value.get("binance_symbol"),
            reference_type=value.get("reference_type", "BINANCE_USDM_PERPETUAL"),
            reference_available=bool(value.get("reference_available", False)),
            valid=bool(value.get("valid", False)),
            reason=value.get("reason", "UNVALIDATED"),
            rules=rules,
        )
    return mappings, raw


def start(config: RuntimeConfig, duration: float, foreground: bool, config_path: str | Path) -> int:
    if not foreground:
        config.log_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = config.log_dir / "runner.stdout.log"
        stderr_path = config.log_dir / "runner.stderr.log"
        command = [sys.executable, "-m", "derive_multi_asset_mm.shadow", "start", "--foreground", "--config", str(config_path), "--duration", str(duration)]
        with stdout_path.open("a", encoding="utf-8") as stdout, stderr_path.open("a", encoding="utf-8") as stderr:
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
        (config.log_dir / "runner.pid").write_text(str(process.pid), encoding="utf-8")
        print(f"MAINNET SHADOW STARTED pid={process.pid} duration_seconds={duration}")
        print(f"STATE: {config.log_dir / 'state.json'}")
        return 0
    report = asyncio.run(ShadowRunner(config).run(duration))
    print("DERIVE MULTI-ASSET BINANCE-REFERENCE MM BUILD COMPLETE")
    print(json.dumps({"status": report.get("run_metadata", {}).get("status"), "classification": report.get("classification"), "real_orders": 0, "real_positions": 0}, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "status":
        config = RuntimeConfig.from_yaml(args.config)
        state_path = _state_path(config)
        print(state_path.read_text(encoding="utf-8") if state_path.exists() else "NO_SHADOW_STATE")
        return 0
    config = RuntimeConfig.from_yaml(args.config)
    if args.command == "start":
        return start(config, parse_duration(args.duration), args.foreground, args.config)
    if args.command == "audit":
        state_path = _state_path(config)
        if not state_path.exists():
            print("NO_SHADOW_STATE")
            return 1
        state = json.loads(state_path.read_text(encoding="utf-8"))
        violations = []
        if state.get("mode") != "MAINNET_SHADOW":
            violations.append("mode_not_mainnet_shadow")
        if state.get("mainnet_armed") is not False:
            violations.append("mainnet_armed_not_false")
        if state.get("real_orders") != 0:
            violations.append("real_orders_nonzero")
        if state.get("real_positions") != 0:
            violations.append("real_positions_nonzero")
        print(json.dumps({"passed": not violations, "violations": violations, "state": state}, indent=2))
        return 0 if not violations else 1
    if args.command == "finalize":
        mappings, mapping_report = _load_mapping_report(config.report_dir / "asset_reference_mapping.json")
        run_metadata = {}
        state_path = _state_path(config)
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8"))
            run_metadata = {
                "status": state.get("status"),
                "started_at": state.get("started_at"),
                "ended_at": state.get("ended_at"),
                "duration_seconds": (
                    state.get("ended_at") - state.get("started_at")
                    if state.get("ended_at") is not None and state.get("started_at") is not None
                    else None
                ),
            }
        with TelemetryStore(config.database_path) as telemetry:
            report = finalize_reports(
                config=config,
                mappings=mappings,
                mapping_report=mapping_report,
                telemetry=telemetry,
                run_metadata=run_metadata,
            )
        print(json.dumps({"report": str(config.report_dir / "final_multi_asset_shadow_report.md"), "classification": report["classification"]}, indent=2))
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
