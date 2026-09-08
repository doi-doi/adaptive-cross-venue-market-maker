# Architecture

The project has four deliberately separated boundaries:

1. `public_data.py` discovers current Derive perpetuals and exact Binance
   USD-M perpetual references. It has no credential or mutation path.
2. `reference.py`, `market_state.py`, `inventory.py`, `portfolio.py`, and
   `quote_engine.py` are deterministic decision modules. They operate on
   causal snapshots and use Decimal arithmetic for exchange rounding.
3. `shadow_engine.py` and `lifecycle.py` simulate one bid and one ask per
   asset. Conservative and touch-sensitivity fills use separate portfolios;
   BBO touch is never presented as conservative execution evidence.
4. `controllers/market_making/derive_multi_asset_binance_reference_mm.py`
   adapts the same decision layer to current Hummingbot Strategy V2 executor
   actions. The installed market-making base is single-pair, so the adapter
   uses `ControllerBase` and manages per-asset level IDs explicitly.

The standalone runner is the build-time path. It subscribes only to public
Derive and Binance market-data channels, persists compact SQLite telemetry,
and never calls Hummingbot or an order endpoint.
