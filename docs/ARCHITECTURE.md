# Final architecture

The permanent ownership boundary is:

| Layer | Owns |
|---|---|
| Binance perpetual | public price discovery, microprice, short return, volatility through Hummingbot's credentialless paper wrapper |
| Derive perpetual | execution BBO, trading rules, positions, orders, fills |
| `derive_binance_adaptive_mm` | fair value, basis, deterministic state/mode, inventory override, quote intent |
| Hummingbot V2 | connectors, throttling, quantization, executor lifecycle, orders, positions, PnL |
| Condor | deploy, status, health, pause/resume/stop, alerts |

The controller is deliberately single-asset and configured only for XRP in the
competition bot. No controller action may target Binance.

The runtime uses Hummingbot's `binance_perpetual_paper_trade` connector as a
credentialless wrapper around the native Binance perpetual public order-book
tracker. A version-gated compatibility shim works around the known Hummingbot
2.16.0 Derive initial-snapshot queue race without changing the connector's
transport or any execution method.

In shadow mode, Derive BBO and trading rules come from the equivalent native
`derive_perpetual_paper_trade` wrapper. The real connector is registered only
when shadow mode is disabled, and executor actions remain permanently targeted
to `derive_perpetual` behind the independent arming key.
The controller also omits `position_mode` and `leverage` from its serialized
shadow configuration so Hummingbot's generic V2 script does not attempt
live-account initialization against the logical connector name. Both typed
fields remain present on the controller config, and live mode serializes them
normally.

Strategy state (`market_state`, `mm_mode`, `inventory_mode`, fair value and
desired quotes) is separate from operational state (`SHADOW`,
`LIVE_DISARMED`, `LIVE_ARMED`, data/risk pauses, `ERROR`).

The controller is suitable for deterministic V2 logic tests. Candle backtests
cannot validate queue residency, rapid cross-venue moves, trade-through, or
markouts; use recorded BBO replay or native shadow mode for those questions.

## Shared safety boundary

Before returning a create action, the controller signs the proposed fill,
projects the resulting position, applies asset and portfolio caps, quantizes a
safe residual size with Derive's native rule, and records a pending reservation.
XRP uses one locked class-level registry keyed by `portfolio_id`. Active bid,
active ask, pending creates, current inventory, portfolio inventory, and capital
minus reserve are all part of the decision.

Market-data transport freshness comes from Hummingbot 2.16.0 tracker message
metrics when available. BBO-change age is retained as a separate activity
metric and conservative fallback. Account-level native telemetry is likewise
separate from the sum of strategy executor PnL.
