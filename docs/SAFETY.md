# Safety contract

- `MAINNET_SHADOW` is the default and requires `dry_run: true` and
  `mainnet_armed: false`.
- New runs use XRP and LINK only. DOGE, ADA, ZEC, CC, SOL, BNB, and HYPE are
  retired from the active scope; historical files remain for reproducibility.
- The standalone runner uses public data only. Derive is the sole execution
  venue in the design. No credentials, private balances, order endpoint,
  cancel endpoint, or reference-venue execution path is used.
- Binance, Bybit, and OKX are data-only references. Bitget is disabled and is
  neither scheduled nor eligible as a primary source.
- Reference selection is strict `BINANCE -> BYBIT -> OKX -> PAUSE`. It never
  averages or median-combines prices and never forward-fills across a causal
  data gap. A disconnected source loses its book; stale sources are excluded.
- Binance recovery requires a fresh snapshot and the configured healthy
  duration. Fresh-source disagreement pauses the quote plan.
- The refresh deadband is strictly greater than 2% (`200` bps) from the
  current causal Derive midpoint. Quote age and small fair-value changes do
  not trigger normal replacement. Invalid, stale, paused, or risk-invalid
  plans still cancel protectively.
- Conservative fills require a later public Derive trade with correct
  aggressor and strict trade-through. Touch sensitivity is a separate
  diagnostic and is never reported as a realized fill.
- No live configuration is shipped armed. `conf/mainnet_live.example.yml` is
  intentionally disarmed; this repository does not start live execution.
- Shadow PnL, public liquidity, source liveness, catalog breadth, and a passing
  import or dashboard check do not prove deployable alpha. Missing denominators
  remain `DATA_INSUFFICIENT` or `NOT_READY_FOR_SMALL_MAINNET_CANARY`.
- The current research evidence is insufficient for a live canary. The known
  SQLite `database is locked` failure is preserved for a separate single-
  writer repair phase and is not silently treated as resolved.
- Testnet workarounds, hedging, news filters, grids, ladders, and martingale
  logic are outside this project boundary.
