#!/usr/bin/env python3
"""Refresh the read-only deadband report while an isolated shadow run lives."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def _alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _run_analysis(args: argparse.Namespace) -> None:
    command = [
        sys.executable,
        str(Path(__file__).resolve().parent / "run_refresh_research.py"),
        "--config",
        args.config,
        "--telemetry",
        args.telemetry,
        "--state",
        args.state,
        "--mapping",
        args.mapping,
        "--out-dir",
        args.out_dir,
    ]
    subprocess.run(command, cwd=Path.cwd(), check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--telemetry", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--mapping", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--wait-for-pid", type=int, required=True)
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args()
    poll = max(5.0, min(float(args.poll_seconds), 60.0))
    while _alive(args.wait_for_pid):
        _run_analysis(args)
        time.sleep(poll)
    _run_analysis(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
