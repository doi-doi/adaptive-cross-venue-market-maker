# Final architecture

The permanent ownership boundary is:

| Layer | Owns |
|---|---|
| Binance perpetual | public price discovery, microprice, short return, volatility |
| Derive perpetual | execution BBO, trading rules, positions, orders, fills |
| `derive_binance_adaptive_mm` | fair value, basis, deterministic state/mode, inventory override, quote intent |
| Hummingbot V2 | connectors, throttling, quantization, executor lifecycle, orders, positions, PnL |
| Condor | deploy, status, health, pause/resume/stop, alerts |

The controller is deliberately single-asset and reusable. XRP and LINK are two
configs in one `v2_with_controllers` bot and share `portfolio_id` plus identical
portfolio ceilings. No controller action may target Binance.

Strategy state (`market_state`, `mm_mode`, `inventory_mode`, fair value and
desired quotes) is separate from operational state (`SHADOW`,
`LIVE_DISARMED`, `LIVE_ARMED`, data/risk pauses, `ERROR`).

The controller is suitable for deterministic V2 logic tests. Candle backtests
cannot validate queue residency, rapid cross-venue moves, trade-through, or
markouts; use recorded BBO replay or native shadow mode for those questions.
