"""Export causal quote-lifetime and Derive trade-crossing diagnostics."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.quote_fill_diagnostic import export_diagnostics


def _display(value: object) -> str:
    if value is None or value == "":
        return "N/A"
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def _print_console_summary(summary: dict) -> None:
    assets = summary.get("assets", [])
    storage = (summary.get("storage_health") or [{}])[0]
    print("DERIVE QUOTE/FILL DIAGNOSTIC UPDATE COMPLETE")
    print()
    print("ACTIVE ASSETS")
    for row in assets:
        print(str(row.get("asset")))
    print()
    print("REFERENCE PRIORITY")
    print("1. BINANCE")
    print("2. BYBIT")
    print("3. OKX")
    print("4. PAUSE")
    bitget_status = 'DISABLED' if summary.get('bitget_runtime_status') == 'INACTIVE' else 'ACTIVE UNTIL RESTART (next profile disabled)'
    print(f"BITGET: {bitget_status}")
    print()
    print("CURRENT RUN")
    print(f"RUN ID: {_display(summary.get('run_id'))}")
    print(f"PID: {_display(summary.get('pid'))}")
    print(f"STATUS: {_display(summary.get('run_status') or summary.get('diagnostic_status'))}")
    print(f"RESTART REQUIRED: {'YES' if summary.get('restart_required_to_remove_bitget') else 'NO'}")
    print()
    print("STORAGE")
    print(f"FREE SPACE: {_display(storage.get('free_disk_gb'))} GB")
    print(f"CURRENT RUN SIZE: {_display(storage.get('current_run_size_bytes'))} bytes")
    print(f"RAW RETENTION: {_display(storage.get('raw_retention_seconds'))}S")
    print("HOLD LOGGING: AGGREGATED")
    print("MARKET DATA: DEDUPLICATED")
    print()
    for row in assets:
        asset = row.get("asset")
        churn_missed = summary.get("potential_churn_missed_fill_count_by_asset", {}).get(asset, 0)
        print(str(asset))
        print(f"DERIVE TRADES/H: {_display(row.get('trades_per_hour'))}")
        print(f"MEDIAN QUOTE LIFETIME: {_display(row.get('median_lifetime_ms'))} ms")
        print(f"REPLACES/MIN: {_display(row.get('quote_mutations_per_min'))}")
        print(f"TRADE-THROUGHS: {_display(row.get('strict_crossings'))}")
        print(f"CHURN-MISSED FILLS: {_display(churn_missed)}")
        print(f"CONSERVATIVE FILLS: {_display(row.get('conservative_fills'))}")
        print(f"ROOT CAUSE: {_display(row.get('root_cause'))}")
        print()
    roots = sorted({str(row.get("root_cause")) for row in assets if row.get("root_cause")})
    threshold_roots = {"MICRO_CHURN_HIGH", "QUOTE_LIFETIME_TOO_SHORT"}
    threshold_recommended = any(row.get("sample_status") == "SUFFICIENT" and row.get("root_cause") in threshold_roots for row in assets)
    print(f"PRIMARY ZERO-FILL CAUSE: {', '.join(roots) if roots else 'MORE_DATA_REQUIRED'}")
    print(f"REFRESH-THRESHOLD TEST RECOMMENDED: {'YES' if threshold_recommended else 'NO'}")
    print("RECOMMENDED NEXT TEST: COLLECT MORE CAUSAL DERIVE TRADES AND 30S/60S MARKOUTS BEFORE TUNING" if not threshold_recommended else "RECOMMENDED NEXT TEST: ISOLATED HYPOTHETICAL REFRESH-THRESHOLD CONTROL")
    print()
    print(f"MAINNET ARMED: {'TRUE' if summary.get('safety', {}).get('mainnet_armed') else 'FALSE'}")
    print(f"REAL ORDERS: {_display(summary.get('safety', {}).get('real_orders', 0))}")
    print(f"REAL POSITIONS: {_display(summary.get('safety', {}).get('real_positions', 0))}")
    print("READY FOR LIVE: NO")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--telemetry", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--run-metadata", default=None)
    parser.add_argument("--watch-pid", type=int, default=None, help="refresh snapshots until this shadow runner exits")
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    config = RuntimeConfig.from_yaml(args.config)
    telemetry = Path(args.telemetry)
    state = Path(args.state)
    metadata = Path(args.run_metadata) if args.run_metadata else None

    def snapshot() -> dict:
        summary = export_diagnostics(config, telemetry, state, Path(args.out_dir), metadata)
        print(json.dumps({
            "run_id": summary.get("run_id"),
            "diagnostic_status": summary.get("diagnostic_status"),
            "root_cause_by_asset": summary.get("root_cause_by_asset", {}),
            "out_dir": args.out_dir,
            "restart_required_to_remove_bitget": summary.get("restart_required_to_remove_bitget"),
            "real_orders": summary.get("safety", {}).get("real_orders", 0),
            "real_positions": summary.get("safety", {}).get("real_positions", 0),
        }, sort_keys=True), flush=True)
        return summary

    summary = snapshot()
    if args.watch_pid is not None:
        while True:
            try:
                runner_alive = os.kill(args.watch_pid, 0) is None
            except (OSError, TypeError, ValueError):
                runner_alive = False
            state_payload = {}
            try:
                state_payload = json.loads(state.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
            if not runner_alive and state_payload.get("status") in {"COMPLETE", "DATA_INSUFFICIENT"}:
                break
            time.sleep(max(1.0, min(float(args.poll_seconds), 60.0)))
            summary = snapshot()
    if summary.get("restart_required_to_remove_bitget"):
        print("RESTART_REQUIRED_TO_REMOVE_BITGET", flush=True)
    _print_console_summary(summary)
    print(json.dumps({
        "run_id": summary.get("run_id"),
        "diagnostic_status": summary.get("diagnostic_status"),
        "root_cause_by_asset": summary.get("root_cause_by_asset", {}),
        "out_dir": args.out_dir,
        "restart_required_to_remove_bitget": summary.get("restart_required_to_remove_bitget"),
        "real_orders": summary.get("safety", {}).get("real_orders", 0),
        "real_positions": summary.get("safety", {}).get("real_positions", 0),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
