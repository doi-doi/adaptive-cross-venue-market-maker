# Runbook

## Before start

- Confirm Docker/Hummingbot API and Condor are healthy.
- Confirm the existing `master_account` profile exposes `derive_perpetual`.
- Confirm Binance public `XRP-USDT` and `LINK-USDT` books are fresh.
- Confirm Derive `XRP-USDC` and `LINK-USDC` books and native trading rules load.
- Read Derive balance and positions; unexpected XRP/LINK exposure is a stop gate.
- Confirm both configs say `shadow_mode: true`, `mainnet_armed: false`.
- Confirm shared limits: 800 capital, 200 reserve, 300 cap per asset.
- Check disk space and that no stale SQLite writer from legacy research is running.

## Start

Deploy through Condor exactly as shown in `CONDOR.md`. Start the
`derive_mm_health` routine. Do not use the legacy standalone runner.

## Monitor

For 15–30 minutes verify both controller diagnostics update, data recovery
hysteresis completes, state/mode transitions do not flap, desired quotes
change, tick-aware holds occur, and mutations remain within budget. Confirm
Hummingbot reports zero orders, fills, and newly created XRP/LINK positions.

Treat a BBO touch as diagnostics only, never a fill. Markouts remain `N/A` until
a real native fill exists; a shadow quote is not exchange execution evidence.

## Stop

Use `stop_controllers` for both IDs, wait one config reload interval, then
verify both controller states are stopped and no active executors remain. Stop
the health routine with its returned routine instance ID.

## Emergency

Use `stop_controllers` immediately. If the process cannot apply the kill
switch, archive/stop the named bot through Condor and independently verify no
matching active executor remains. Do not modify unrelated bots or credentials.
