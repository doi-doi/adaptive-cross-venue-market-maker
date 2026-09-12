# Architecture

The project has four separated boundaries:

1. `public_data.py` discovers active Derive perpetuals and the standalone
   `multi_public.py` loads exact symbols from the installed
   `binance_perpetual`, `bybit_perpetual`, `okx_perpetual`, and
   `bitget_perpetual` connectors. Public REST metadata validates the exact
   USDT perpetual and contract convention before a stream is accepted. New
   default runs are limited to DOGE, ADA, and XRP; the eight-asset consensus
   path remains available through its archived config and reports.
2. `multi_public.py` normalizes complete BBO snapshots from the four public
   WebSocket feeds. `source_health.py` tracks freshness, update gaps, sequence
   evidence, rejects, parsing failures, and reconnect invalidation per
   asset/source. A disconnect clears that source's book; stale observations
   cannot enter priority selection or the historical consensus path.
3. `priority.py`, `reference.py`, `market_state.py`, `inventory.py`,
   `portfolio.py`, and `quote_engine.py` are deterministic decision modules.
   The new controls (`DERIVE_ONLY`, `BINANCE_ONLY_NO_FAILOVER`, and
   `PRIORITY_FAILOVER`) see the same causal Derive observation. The priority
   path selects exactly one fresh source in `BINANCE -> BYBIT -> OKX` order;
   it never averages or median-combines primary prices, never forward-fills,
   pauses on fresh-source disagreement, and requires a configurable three
   seconds of fresh Binance health before recovery. Bitget is diagnostics-only.
   The historical `MULTI_SOURCE_CONSENSUS` control remains intact for the
   archived eight-asset path.
4. `shadow_engine.py` gives each control/fill-model pair its own reconciler,
   inventory, cash, fills, and markouts. Conservative fills require a
   direction-aware strict Derive trade-through; touch sensitivity is a separate
   diagnostic. `telemetry.py` persists all reference health/value rows and
   Derive trade provenance in SQLite.
5. `refresh_governor.py` and `refresh_research.py` implement the isolated next
   phase. The governor orders emergency/risk/stale/normal mutations and
   coalesces latest-desired requests. The research replay applies the same
   causal observations to the ZEC/XRP/LINK deadband/residency matrix without
   copying raw telemetry or treating touch events as fills.

The standalone runner is the build-time path. It subscribes only to public
Derive and reference market-data channels, uses Derive trades alone as fill
evidence, and never calls an order endpoint. The Strategy V2 adapter preserves
the Hummingbot entry point and includes all four reference connectors in its
market-data set; its live executor path remains gated by explicit
`MAINNET_LIVE`, `dry_run: false`, and `mainnet_armed: true` configuration.
