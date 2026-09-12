# Current active universe

This is the final active scope for new research runs in this repository.

| Boundary | Current value |
|---|---|
| Assets | `XRP`, `LINK` |
| Execution venue | Derive perpetual only |
| Reference venues | Binance, Bybit, OKX; public data-only |
| Selection | `BINANCE -> BYBIT -> OKX -> PAUSE` |
| Bitget | Disabled; not scheduled or eligible |
| Mode | `MAINNET_SHADOW` |
| Dry run | `true` |
| Mainnet armed | `false` |
| Hedging/news | Not used |

The canonical configuration is
[`conf/mainnet_shadow.yml`](../../conf/mainnet_shadow.yml). The
refresh-research profile is
[`conf/mainnet_shadow_refresh_research.yml`](../../conf/mainnet_shadow_refresh_research.yml)
and has the same XRP/LINK universe.

## Selection and execution boundary

Only one fresh reference is selected at a time. Prices are not averaged or
median-combined, and no price is forward-filled across a causal data gap. A
stale primary fails over immediately. Binance recovery requires the configured
fresh-health duration; disagreement pauses the quote plan.

Reference observations determine research fair value only. The standalone
runner uses Derive BBO and Derive public trades for fill diagnostics and never
calls an order endpoint. A quote-active decision is not an exchange order, a
touch is not a fill, and shadow results do not establish realized PnL.

## Evidence status

The retained XRP/LINK snapshots are operationally informative but
`DATA_INSUFFICIENT` for viability: the observations do not provide the required
trade, conservative-fill, and 30/60-second markout denominators. The known
SQLite lock failure remains a separate repair item. No parameter promotion or
live canary is authorized by this repository state.
