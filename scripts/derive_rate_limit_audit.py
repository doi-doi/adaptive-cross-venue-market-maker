#!/usr/bin/env python3
"""Write the read-only Derive rate-limit audit artifact."""

from __future__ import annotations

import argparse
from pathlib import Path

from derive_multi_asset_mm.rate_limit_audit import write_rate_limit_audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="reports/zec_xrp_link_refresh_research")
    parser.add_argument("--no-live-probe", action="store_true")
    args = parser.parse_args()
    audit = write_rate_limit_audit(Path(args.out_dir), probe_live=not args.no_live_probe)
    print(f"{audit['classification']} -> {Path(args.out_dir).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
