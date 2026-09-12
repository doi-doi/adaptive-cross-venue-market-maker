"""Run with the installed Hummingbot container interpreter; public metadata only.

No network lifecycle or account connector is started. Connector-owned public
symbol-map initialization supplies canonical exchange symbol validation.
"""

import asyncio
import importlib
import json

ASSETS = ("ADA", "CC", "XRP", "SOL", "LINK", "DOGE", "BNB", "HYPE")
CLASSES = {
    "binance_perpetual": "BinancePerpetualDerivative",
    "bybit_perpetual": "BybitPerpetualDerivative",
    "okx_perpetual": "OkxPerpetualDerivative",
    "bitget_perpetual": "BitgetPerpetualDerivative",
}


async def discover(name, class_name):
    try:
        module = importlib.import_module(f"hummingbot.connector.derivative.{name}.{name}_derivative")
        connector = getattr(module, class_name)(trading_required=False, trading_pairs=[])
        # ``trading_pair_symbol_map`` is the connector's public async
        # discovery surface and performs its own public exchange-info fetch.
        mapping = await asyncio.wait_for(connector.trading_pair_symbol_map(), 40)
        return {"connector": name, "installed": True, "trading_required": False,
                "symbols": {asset: [symbol for symbol, pair in mapping.items() if pair == f"{asset}-USDT"] for asset in ASSETS}}
    except Exception as exc:
        return {"connector": name, "error": f"{type(exc).__name__}: {exc}", "symbols": {}}


async def main():
    result = await asyncio.gather(*(discover(name, cls) for name, cls in CLASSES.items()))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
