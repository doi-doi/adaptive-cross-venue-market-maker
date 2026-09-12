# Safety contract

- `MAINNET_SHADOW` is the default and requires `dry_run: true` and
  `mainnet_armed: false`.
- The standalone runner is public-data-only. It uses Derive as the only
  execution venue and never loads credentials, reads private balances, places
  or cancels orders, or hedges on Binance, Bybit, OKX, or Bitget.
- The reference connectors are data-only. Their exact markets are discovered
  from the installed Hummingbot connector maps and independently validated
  against public contract metadata. Missing or inactive markets remain
  unavailable; no substitute symbol is selected.
- New runs are DOGE, ADA, and XRP only. CC, SOL, LINK, BNB, and HYPE are
  disabled from the priority path; historical eight-asset consensus artifacts
  are retained separately.
- Primary reference selection is strict `BINANCE -> BYBIT -> OKX -> PAUSE`.
  It does not average or median-combine source prices and does not
  forward-fill. Stale-source failover is immediate; Binance recovery requires
  a fresh snapshot plus the configured healthy duration (default three
  seconds). Bitget is diagnostics-only and cannot become primary.
- Source health is independent per asset and venue. A source is excluded when
  stale, its prior book is cleared on disconnect, and no price is forward-filled
  into priority selection or lead/lag analysis.
- `MAINNET_LIVE` requires both `dry_run: false` and `mainnet_armed: true` in
  the Strategy V2 adapter. No project command starts it.
- Conservative fills require a later public Derive trade with the correct
  aggressor and strict trade-through. Touch-sensitivity fills are diagnostic
  and are never presented as realized execution.
- Shadow PnL, public liquidity, catalog breadth, source liveness, and completed
  code are not proof of deployable alpha or live readiness. Any missing
  denominator remains `DATA_INSUFFICIENT` or `NOT_READY_FOR_SMALL_MAINNET_CANARY`.
- The next refresh-deadband phase is ZEC/XRP/LINK only. DOGE and ADA are
  historical-only for that phase and their prior artifacts are preserved.
- Normal quote changes are evaluated over a deadband/residency grid and are
  subject to a conservative 1 action/second and 30 actions/minute research
  budget. Emergency adverse-move cancellations bypass normal throttling.
- The Derive limit audit is explicitly
  `DERIVE_RATE_LIMIT_NOT_FULLY_VERIFIED` until account-specific and deployed
  per-instrument limits are authenticated. A rate-limit report is not a live
  execution authorization.
