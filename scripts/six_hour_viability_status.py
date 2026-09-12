"""Immediate read-only status for the detached three-asset shadow run."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _status(state_path: Path, telemetry_path: Path, metadata_path: Path) -> dict[str, Any]:
    state = _read_json(state_path)
    metadata = _read_json(metadata_path)
    assets = [str(asset) for asset in (metadata.get("assets") or state.get("active_assets") or ["DOGE", "ADA", "XRP"])]
    started = float(state.get("started_at") or time.time())
    ended = state.get("ended_at")
    now = float(ended or time.time())
    planned_end = float(metadata.get("planned_end_epoch") or (started + float(metadata.get("duration_seconds") or 21600)))
    connection = sqlite3.connect(f"file:{telemetry_path}?mode=ro", uri=True)
    try:
        asset_rows = {}
        for asset in assets:
            trade_count = connection.execute(
                "SELECT COUNT(*) FROM trades WHERE asset=? AND source='derive'", (asset,)
            ).fetchone()[0]
            fill_count = connection.execute(
                "SELECT COUNT(*) FROM fills WHERE asset=? AND model='PRIORITY_FAILOVER:CONSERVATIVE'", (asset,)
            ).fetchone()[0]
            latest = connection.execute(
                "SELECT MAX(timestamp) FROM decisions WHERE asset=?", (asset,)
            ).fetchone()[0]
            consensus = (state.get("latest_consensus") or {}).get(asset) or {}
            asset_rows[asset] = {
                "trades": int(trade_count),
                "conservative_fills": int(fill_count),
                "current_reference": consensus.get("selected_reference"),
                "last_telemetry_age_seconds": round(max(0.0, now - float(latest)), 3) if latest else None,
            }
    finally:
        connection.close()
    status = str(state.get("status", "UNKNOWN"))
    return {
        "run_id": metadata.get("run_id"),
        "status": status,
        "elapsed_seconds": round(max(0.0, now - started), 3),
        "remaining_seconds": round(max(0.0, planned_end - now), 3) if status not in {"COMPLETE", "DATA_INSUFFICIENT"} else 0.0,
        "assets": asset_rows,
        "error_count": len(state.get("errors") or []),
        "mainnet_armed": state.get("mainnet_armed"),
        "real_orders": state.get("real_orders", 0),
        "real_positions": state.get("real_positions", 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--telemetry", required=True)
    parser.add_argument("--metadata", required=True)
    args = parser.parse_args()
    print(json.dumps(_status(Path(args.state), Path(args.telemetry), Path(args.metadata)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
