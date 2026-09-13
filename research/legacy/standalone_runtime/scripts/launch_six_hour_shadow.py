"""Launch an isolated, measurement-only six-hour Derive shadow run.

The launcher keeps prior run directories intact, records the run metadata used
by the dashboard, and starts the report exporter as a separate read-only
process. It never enables live execution or submits orders.
"""

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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--duration", default="6h")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    project_root = Path.cwd()
    config_path = Path(args.config).resolve()
    duration = parse_duration(args.duration)
    config = _scoped_config(RuntimeConfig.from_yaml(config_path), args.run_id)
    if not config.dry_run or config.mainnet_armed:
        raise SystemExit("refusing to launch a non-shadow configuration")
    if config.log_dir.exists() and any(config.log_dir.iterdir()):
        raise SystemExit(f"refusing to reuse existing log directory: {config.log_dir}")
    if config.report_dir.exists() and any(config.report_dir.iterdir()):
        raise SystemExit(f"refusing to reuse existing report directory: {config.report_dir}")

    config.log_dir.mkdir(parents=True, exist_ok=False)
    config.report_dir.mkdir(parents=True, exist_ok=False)
    runner_stdout = config.log_dir / "runner.stdout.log"
    runner_stderr = config.log_dir / "runner.stderr.log"
    started_at = time.time()
    command = [
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
            command,
            cwd=project_root,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
    (config.log_dir / "runner.pid").write_text(str(runner.pid), encoding="utf-8")

    metadata_path = config.report_dir / "run_metadata.json"
    watcher_stdout = config.log_dir / "report_watcher.stdout.log"
    watcher_stderr = config.log_dir / "report_watcher.stderr.log"
    watcher_command = [
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
        str(metadata_path),
        "--wait-for-pid",
        str(runner.pid),
        "--poll-seconds",
        "30",
    ]
    with watcher_stdout.open("a", encoding="utf-8") as stdout, watcher_stderr.open("a", encoding="utf-8") as stderr:
        watcher = subprocess.Popen(
            watcher_command,
            cwd=project_root,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
    (config.log_dir / "report_watcher.pid").write_text(str(watcher.pid), encoding="utf-8")

    metadata = {
        "run_id": args.run_id,
        "status": "RUNNING",
        "start_time_utc": _iso(started_at),
        "planned_end_time_utc": _iso(started_at + duration),
        "start_time_epoch": started_at,
        "planned_end_epoch": started_at + duration,
        "duration_seconds": duration,
        "planned_duration_seconds": duration,
        "pid": runner.pid,
        "report_watcher_pid": watcher.pid,
        "config_path": str(config_path),
        "mode": config.mode.value,
        "assets": [asset.symbol for asset in config.enabled_assets],
        "reference_venues": list(config.reference_venues),
        "reference_priority": list(config.reference_priority) + ["pause"],
        "bitget_enabled": config.bitget_enabled,
        "bitget_runtime_status": "INACTIVE" if not config.bitget_enabled else "CONFIGURED",
        "derive_execution_only": True,
        "mainnet_armed": False,
        "dry_run": True,
        "real_orders": 0,
        "real_positions": 0,
        "reference_execution": False,
        "telemetry_locations": {
            "database": str(config.database_path),
            "state": str(config.log_dir / "state.json"),
            "report_dir": str(config.report_dir),
        },
        "status_command": f"{sys.executable} -m derive_multi_asset_mm.shadow status --config {config_path} --run-id {args.run_id}",
        "audit_command": f"{sys.executable} -m derive_multi_asset_mm.shadow audit --config {config_path} --run-id {args.run_id}",
        "runner_command": command,
        "report_watcher_command": watcher_command,
    }
    _write_json(metadata_path, metadata)
    print(
        json.dumps(
            {
                "status": "RUNNING",
                "run_id": args.run_id,
                "runner_pid": runner.pid,
                "report_watcher_pid": watcher.pid,
                "state": str(config.log_dir / "state.json"),
                "telemetry": str(config.database_path),
                "reports": str(config.report_dir),
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
