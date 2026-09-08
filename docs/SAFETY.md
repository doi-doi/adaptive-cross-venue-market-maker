# Safety contract

- `MAINNET_SHADOW` is the default and requires `dry_run: true` and
  `mainnet_armed: false`.
- The standalone shadow runner rejects every non-shadow mode. It never loads
  credentials, reads private balances, creates orders, cancels orders, or
  hedges on Binance/Hyperliquid.
- `MAINNET_LIVE` requires both `dry_run: false` and `mainnet_armed: true` in
  the Strategy V2 adapter. It is a separate operator decision and is not
  started by any project command.
- Dynamic mapping is exact: a missing Derive instrument or exact Binance
  USD-M perpetual is classified and disabled; no substitute symbol is used.
- Stale Binance or Derive data, invalid rules, extreme basis divergence,
  extreme volatility, and hard risk limits fail closed.
- Shadow PnL and touch-sensitivity fills are diagnostic. They are not
  realized PnL or proof of deployable alpha.
