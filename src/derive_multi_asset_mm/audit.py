"""Read-only preflight audit entry point."""

from __future__ import annotations

import argparse
import json

from .config import RuntimeConfig
from .public_data import discover_mappings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit public mappings and shadow safety")
    parser.add_argument("--config", default="conf/mainnet_shadow.yml")
    args = parser.parse_args(argv)
    config = RuntimeConfig.from_yaml(args.config)
    mappings, report = discover_mappings(config)
    output = {
        "safety": config.public_safety(),
        "mappings": {
            asset: {
                "derive_instrument": mapping.derive_instrument,
                "binance_symbol": mapping.binance_symbol,
                "status": mapping.reason,
                "valid": mapping.valid,
            }
            for asset, mapping in mappings.items()
        },
        "errors": report.get("errors", []),
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
