#!/usr/bin/env python3
"""Materialize the read-only accounting repair for one shadow run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from derive_multi_asset_mm.accounting_repair import build_repair


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--telemetry", type=Path, required=True)
    parser.add_argument("--mapping", type=Path, default=None)
    parser.add_argument("--rate-limit-audit", type=Path, default=None)
    parser.add_argument("--run-metadata", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    summary = build_repair(
        config_path=args.config,
        state_path=args.state,
        telemetry_path=args.telemetry,
        mapping_path=args.mapping,
        rate_limit_audit_path=args.rate_limit_audit,
        output_dir=args.output_dir,
        metadata_path=args.run_metadata,
    )
    print(json.dumps({"status": summary["status"], "run_id": summary["run_id"], "output_dir": str(args.output_dir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
