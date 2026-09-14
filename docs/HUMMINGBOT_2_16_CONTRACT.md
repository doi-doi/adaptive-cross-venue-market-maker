# Hummingbot 2.16.0 contract audit

The competition contract probe is pinned to:

- version tag: `version-2.16.0`
- immutable image: `hummingbot/hummingbot@sha256:e222f070d42814013fb5ea7fe537926f790b259512950369da1e15a69dcbd38f`
- image source revision label: `a32b8c1e5b0e4acdea45d05aebf91d0b65bfda32`

The earlier `sha256:632d2b07aa156b761310f2f7258a78c9660a1c28b6df4b33874e09a0c7d06c85`
was rechecked and belongs to the locally cached floating `latest` image, not
the current immutable `version-2.16.0` tag. It is therefore not used.

## Audited interfaces

`ControllerBase`, `ControllerConfigBase`, `CreateExecutorAction`,
`StopExecutorAction`, `OrderExecutorConfig`, `OrderExecutor`,
`ExecutorOrchestrator`, `MarketDataProvider`, `TradingRule`, Derive position
tracking, and paper-trade wrappers were inspected from the pinned image.

`OrderExecutor` passes its configured `position_action` to the connector and
uses `position_action == CLOSE` for Hummingbot's perpetual budget candidate.
In the Derive connector `_place_order`, both `OPEN` and `CLOSE` still result in
the same payload field `reduce_only: false`. For a limit order, `CLOSE` selects
`gtc`; `LIMIT_MAKER` is also `gtc`. There is no true reduce-only behavior to
delegate to in this connector version.

`OrderBookTracker.metrics.per_pair_metrics` records `time.perf_counter()` for
accepted diffs, snapshots, and trades. The controller uses the most recent
diff/snapshot timestamp for feed freshness. Paper wrappers share the native
tracker, so this does not open a duplicate WebSocket.

The native probe checks the exact installed version, Derive payload source,
guarded snapshot shim activation, controller import, quote calculation, and
zero shadow actions.

The connector's balance updater maps Derive collateral `amount` into both
Hummingbot total and available balances. It does not expose an account-equity
field or realized PnL. The controller therefore labels this value as collateral
balance, leaves account equity/realized PnL as `N/A`, and does not arm an equity
drawdown gate from a weaker proxy.
