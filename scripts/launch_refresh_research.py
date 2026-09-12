#!/usr/bin/env python3
"""Launch the isolated, shadow-only ZEC/XRP/LINK refresh research phase."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from derive_multi_asset_mm.config import RuntimeConfig
from derive_multi_asset_mm.shadow import _scoped_config, parse_duration


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="conf/mainnet_shadow_refresh_research.yml")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--duration", default="6h")
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args()

    project_root = Path.cwd()
    config_path = Path(args.config).resolve()
    base_config = RuntimeConfig.from_yaml(config_path)
    config = _scoped_config(base_config, args.run_id)
    duration = parse_duration(args.duration)
    if not config.dry_run or config.mainnet_armed:
        raise SystemExit("refusing to launch a non-shadow configuration")
    if config.log_dir.exists() and any(config.log_dir.iterdir()):
        raise SystemExit(f"refusing to reuse existing log directory: {config.log_dir}")
    if config.report_dir.exists() and any(config.report_dir.iterdir()):
        raise SystemExit(f"refusing to reuse existing report directory: {config.report_dir}")
    config.log_dir.mkdir(parents=True, exist_ok=False)
    config.report_dir.mkdir(parents=True, exist_ok=False)

    started_at = time.time()
    runner_stdout = config.log_dir / "runner.stdout.log"
    runner_stderr = config.log_dir / "runner.stderr.log"
    runner_command = [
        sys.executable,
        "-m",
        "derive_multi_asset_mm.shadow",
        "start",
        "--foreground",
        "--config",
        str(config_path),
        "--duration",
        str(duration),
        "--run-id",
        args.run_id,
    ]
    with runner_stdout.open("a", encoding="utf-8") as stdout, runner_stderr.open("a", encoding="utf-8") as stderr:
        runner = subprocess.Popen(
            runner_command,
            cwd=project_root,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
    (config.log_dir / "runner.pid").write_text(str(runner.pid), encoding="utf-8")

    report_metadata = config.report_dir / "run_metadata.json"
    viability_stdout = config.log_dir / "viability_watcher.stdout.log"
    viability_stderr = config.log_dir / "viability_watcher.stderr.log"
    viability_command = [
        sys.executable,
        str(project_root / "scripts/six_hour_viability_report.py"),
        "--config",
        str(config_path),
        "--telemetry",
        str(config.database_path),
        "--state",
        str(config.log_dir / "state.json"),
        "--out-dir",
        str(config.report_dir),
        "--run-metadata",
        str(report_metadata),
        "--wait-for-pid",
        str(runner.pid),
        "--poll-seconds",
        str(max(5.0, min(args.poll_seconds, 60.0))),
    ]
    with viability_stdout.open("a", encoding="utf-8") as stdout, viability_stderr.open("a", encoding="utf-8") as stderr:
        viability = subprocess.Popen(
            viability_command,
            cwd=project_root,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
    (config.log_dir / "viability_watcher.pid").write_text(str(viability.pid), encoding="utf-8")

    research_stdout = config.log_dir / "refresh_research_watcher.stdout.log"
    research_stderr = config.log_dir / "refresh_research_watcher.stderr.log"
    mapping_path = config.report_dir / "asset_reference_mapping.json"
    # The configured directories are phase roots.  _scoped_config() keeps the
    # runner in an isolated child while the aggregate research handoff stays
    # at the documented phase root for the dashboard and reviewer.
    phase_report_dir = base_config.report_dir
    research_command = [
        sys.executable,
        str(project_root / "scripts/refresh_research_watcher.py"),
        "--config",
        str(config_path),
        "--telemetry",
        str(config.database_path),
        "--state",
        str(config.log_dir / "state.json"),
        "--mapping",
        str(mapping_path),
        "--out-dir",
        str(phase_report_dir),
        "--wait-for-pid",
        str(runner.pid),
        "--poll-seconds",
        str(max(5.0, min(args.poll_seconds, 60.0))),
    ]
    with research_stdout.open("a", encoding="utf-8") as stdout, research_stderr.open("a", encoding="utf-8") as stderr:
        research = subprocess.Popen(
            research_command,
            cwd=project_root,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
    (config.log_dir / "refresh_research_watcher.pid").write_text(str(research.pid), encoding="utf-8")

    metadata = {
        "run_id": args.run_id,
        "status": "RUNNING",
        "phase": "ZEC_XRP_LINK_REFRESH_DEADBAND_RESEARCH",
        "start_time_utc": _iso(started_at),
        "planned_end_time_utc": _iso(started_at + duration),
        "start_time_epoch": started_at,
        "planned_end_epoch": started_at + duration,
        "duration_seconds": duration,
        "planned_duration_seconds": duration,
        "pid": runner.pid,
        "viability_watcher_pid": viability.pid,
        "refresh_research_watcher_pid": research.pid,
        "config_path": str(config_path),
        "assets": [asset.symbol for asset in config.enabled_assets],
        "reference_venues": list(config.reference_venues),
        "reference_priority": list(config.reference_priority) + ["pause"],
        "bitget_enabled": False,
        "bitget_runtime_status": "INACTIVE",
        "derive_execution_only": True,
        "mainnet_armed": False,
        "dry_run": True,
        "real_orders": 0,
        "real_positions": 0,
        "reference_execution": False,
        "refresh_policy": {
            "deadband_grid_bps": ["0", "2", "5", "10", "15", "20", "30"],
            "residency_grid_seconds": ["0", "0.5", "1", "2", "3", "5"],
            "fast_adverse_move_override": True,
            "max_order_actions_per_second": str(config.max_order_actions_per_second),
            "max_order_actions_per_minute": config.max_actions_per_minute,
            "max_order_actions_per_instrument_per_second": str(config.max_order_actions_per_instrument_per_second),
            "rate_limit_status": config.rate_limit_status,
        },
        "telemetry_locations": {
            "database": str(config.database_path),
            "state": str(config.log_dir / "state.json"),
            "run_report_dir": str(config.report_dir),
            "refresh_report_dir": str(phase_report_dir),
        },
        "runner_command": runner_command,
        "viability_watcher_command": viability_command,
        "refresh_research_watcher_command": research_command,
    }
    _write_json(report_metadata, metadata)
    print(
        json.dumps(
            {
                "status": "RUNNING",
                "run_id": args.run_id,
                "runner_pid": runner.pid,
                "viability_watcher_pid": viability.pid,
                "refresh_research_watcher_pid": research.pid,
                "state": str(config.log_dir / "state.json"),
                "telemetry": str(config.database_path),
                "reports": str(phase_report_dir),
                "run_reports": str(config.report_dir),
                "dry_run": True,
                "mainnet_armed": False,
                "real_orders": 0,
                "real_positions": 0,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
