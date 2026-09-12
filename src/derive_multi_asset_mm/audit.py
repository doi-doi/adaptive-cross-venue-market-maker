"""Read-only preflight audit entry point."""

from __future__ import annotations

import argparse
import json

from .config import RuntimeConfig
from .multi_public import discover_references_with_report
from .public_data import discover_mappings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit public mappings and shadow safety")
    parser.add_argument("--config", default="conf/mainnet_shadow.yml")
    args = parser.parse_args(argv)
    config = RuntimeConfig.from_yaml(args.config)
    mappings, derive_report = discover_mappings(config, require_binance_reference=False)
    reference_rows, reference_report = discover_references_with_report(
        [asset.symbol for asset in config.enabled_assets], config
    )
    rows_by_asset = {}
    for row in reference_rows:
        rows_by_asset.setdefault(row["asset"], []).append(row)
    output = {
        "safety": config.public_safety(),
        "mappings": {
            asset: {
                "derive_instrument": mapping.derive_instrument,
                "binance_symbol": mapping.binance_symbol,
                "status": mapping.reason,
                "valid": mapping.valid,
                "reference_markets": rows_by_asset.get(asset, []),
            }
            for asset, mapping in mappings.items()
        },
        "errors": derive_report.get("errors", []) + reference_report.get("connector_errors", []),
        "configured_reference_venues": list(config.reference_venues),
    }
    # RuntimeConfig keeps monetary/risk values as Decimal for exact sizing.
    # The audit is a JSON report, so serialize those values explicitly rather
    # than letting a read-only preflight fail after all probes completed.
    print(json.dumps(output, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
