# Final submission status

## Identity

- Title: **Adaptive XRP Cross-Venue Market Maker**
- Active controller: `derive_binance_adaptive_mm_xrp`
- Active execution market: Derive `XRP-USDC`
- Reference market: Binance `XRP-USDT` (public data only)
- `shadow_mode: true`; `mainnet_armed: false`

Summary: Adaptive XRP perpetual market-making strategy for Derive using
Binance XRP perpetual market data as a real-time fair-value and market-state
reference. The strategy dynamically adjusts quote bias and risk based on trend,
volatility, inventory, and cross-venue conditions while maintaining strict
execution and exposure controls.

Markets: execution venue Derive Perpetuals (`XRP-USDC`); reference venue Binance
Perpetuals (`XRP-USDT`). Binance is market data only; all executable orders are
routed exclusively to Derive through Hummingbot's native `derive_perpetual`
connector.

## Safety contract

- Self-cross guard, cancel-confirm-before-create, shutdown-state protection,
  native-price normalization, unique Derive nonce, and fail-closed rejection
  handling are preserved.
- Position mapping is `INCREASE_SAME_DIRECTION -> OPEN`,
  `FLIP_DIRECTION -> OPEN`, `REDUCE -> CLOSE`, and `FLATTEN -> CLOSE`.
- Hummingbot 2.16.0 still sends `reduce_only: false`; `CLOSE` is not claimed as
  native reduce-only behavior.
- Position flips remain disabled by default and projected inventory/open-order
  protections remain active.
- Fixed-time one-second Binance volatility sampling records unchanged fresh
  prices as zero returns and pauses without synthesis when stale.

## Deferred or unsupported

Native account equity and realized PnL remain `N/A` when unavailable. No live
trading or parameter search was performed.

## Verification

The final XRP-only surface is validated by the repository test suite, Ruff,
competition-surface validator, and the pinned Hummingbot 2.16.0 contract probe.
Native shadow validation is the required runtime evidence; live canary remains
an operator-gated follow-up.
