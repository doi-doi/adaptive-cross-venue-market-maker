# XRP live-safety hardening

The final submission surface is XRP-only: Binance perpetual public reference,
Derive perpetual execution, Hummingbot V2 controller, and a read-only Condor
health routine. Only the root controller/configuration surface is active.

The following safety fixes remain in the XRP controller:

- signed projected-position and inventory/open-order limits;
- atomic pending-create reservations;
- native Derive price normalization and self-cross prevention;
- cancel confirmation before replacement, including `SHUTTING_DOWN` executors;
- unique authenticated action nonces for the audited Hummingbot 2.16.0 path;
- fail-closed exchange-rejection handling with diagnostics;
- native feed-age gates, fixed one-second Binance volatility sampling, adverse
  move cancellation, deadband, residency, and mutation limits.

The pinned Hummingbot 2.16.0 contract probe confirms Derive's native
`reduce_only: false` payload. Controller `PositionAction.CLOSE` is therefore
budget semantics only and is not described as native reduce-only protection.

Native account equity and realized PnL are reported as `N/A` when unavailable.
No live trading was started for this conversion.
