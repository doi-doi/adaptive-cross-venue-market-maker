# Derive adaptive MM live-safety hardening

Date: 2026-09-13

Scope: XRP and LINK, Binance perpetual public reference, Derive perpetual
execution, Hummingbot Strategy V2 controller, and read-only Condor health.
Strategy architecture and research parameters were not redesigned or optimized.

## Audit disposition

| Finding | Old behavior | New behavior | Evidence | Status |
|---|---|---|---|---|
| Projected asset inventory | Create checks covered current inventory and open-order notional, not the signed post-fill position | Every bid/ask is signed, projected, conservatively valued, resized, quantized, or blocked below native minimum | deterministic flat/long/short/increase/reduce/flatten/flip and residual-size tests | FIXED |
| Projected portfolio inventory | Only current shared gross inventory was checked | The candidate fill is included in worst-direction shared gross inventory before reservation; current mark, active-order, pending-order, and candidate prices are valued at the highest applicable price | portfolio-cap resize and conservative-price tests | FIXED |
| Asset-cap semantics | Position and order exposure could be evaluated separately | `asset_cap_quote` means absolute current position plus active and pending order notional | active-opposite-order plus new-quote test | FIXED |
| XRP/LINK race | Two controllers could check the same remaining headroom before either action became active | One locked registry atomically records pending create exposure; pending levels cannot create twice, and a peer cannot expire a stalled controller's reservation | concurrent shared reservation, owner-expiry, and duplicate-create tests | FIXED |
| Position flips | Both sides always used full configured size with `PositionAction.OPEN` | Fill effect is classified; committed default caps reductions at flatten; explicit NORMAL-state flips still obey all projected caps | classification, default flatten, and controlled-flip tests | FIXED |
| Derive reduce-only semantics | `PositionAction.OPEN` was used without an explicit audited connector contract | Pinned 2.16.0 source proves both `OPEN` and `CLOSE` submit `reduce_only: false`; safety is controller-side and never described as native reduce-only | pinned native source probe and `docs/HUMMINGBOT_2_16_CONTRACT.md` | MITIGATED |
| Floating Hummingbot CI | Native probe used `hummingbot/hummingbot:latest` | CI and documented probe use immutable 2.16.0 digest `sha256:e222f070d42814013fb5ea7fe537926f790b259512950369da1e15a69dcbd38f` | image pull/inspect and native contract probe | FIXED |
| Snapshot private shim | Shape detection searched for one source fragment | Activation requires VERSION `2.16.0` plus the complete audited private-member shape, logs once, and exposes a diagnostic | affected/unaffected/unknown unit guard plus native activation probe | FIXED |
| Feed freshness | BBO/update-ID change age was presented as venue age | Native tracker message age and BBO-change age are distinct; absent metrics use explicit `BBO_CHANGE_FALLBACK` | unchanged-BBO native message, changing BBO, stale venue, fallback, and recovery tests | FIXED |
| Account versus strategy PnL | Condor primarily showed summed executor PnL/drawdown | Strategy telemetry is named separately; native account values are a single non-summed account view | controller/Condor separation and deduplication tests | FIXED |
| Account equity and realized PnL | No account-level path | Path and drawdown gate exist when native equity is supplied; exact Derive 2.16.0 exposes collateral balance/unrealized PnL but not equity or realized PnL, so both display `N/A` | native source audit and supported/unsupported account tests | MITIGATED |
| Legacy root runtime | Retired `src/`, `conf/`, dashboard, reports, scripts, tests, and multi-reference material looked current | All are preserved under `research/legacy/standalone_runtime/`; root validation permits only current XRP/LINK configs | competition-surface validator | FIXED |

## Static verification

- `pytest -q`: 56 passed.
- `ruff check controllers condor tests scripts`: passed.
- `python scripts/validate_competition_surface.py`: passed.
- `git diff --check`: passed.
- pinned native contract probe: passed on Hummingbot 2.16.0; shadow emitted zero actions; snapshot shim active; Derive `reduce_only=false` contract confirmed.
- CI includes current config validation, legacy/current separation, secret scan, and a 10 MiB tracked-worktree scan.

## Native shadow validation

Only the final run below qualifies as validation. Shorter pre-final starts were
stopped and archived after the controller changed and are intentionally
excluded.

- Instance: `derive-binance-adaptive-mm-safety-final3-20260913-084231`.
- Image: `hummingbot/hummingbot@sha256:e222f070d42814013fb5ea7fe537926f790b259512950369da1e15a69dcbd38f`;
  runtime image ID matched the same digest.
- Controller source SHA-256 inside the running container:
  `9fee0db0d24a11bff6fe63bad8c291515414fed62e58e2b5519e59b43ea952fc`,
  identical to the checkout.
- Started: `2026-09-13T08:42:31Z`; clean stop completed at
  `2026-09-13T09:00:01Z` (17m30s wall time). Final controller uptime was
  1022.8 seconds; the final Condor frame showed 1004 seconds.
- Both XRP and LINK loaded their Derive USDC and Binance USDT books. Sampled
  native message ages remained about 0.01–1.02 seconds and used
  `NATIVE_MESSAGE_TIMESTAMP`; BBO-change age remained a distinct diagnostic.
- Fair values, risk-adjusted bid/ask sizes, and conservative projected
  notionals updated throughout. Final XRP size was 18.5 with projected
  +24.9232/-24.93245 USDC; final LINK size was 11.051 with projected
  +124.9923355/-125.064167 USDC.
- Natural state transitions were observed without injected data:
  `NORMAL/NEUTRAL -> UP_TREND/LONG_BIAS -> NORMAL/NEUTRAL`.
- Portfolio inventory, portfolio open orders, mutations, fills, volume, and
  strategy executor PnL all remained zero, as required in shadow mode.
- Condor instance `b0efdbde` ran with `execution_enabled=false`. Report
  `179cfe` remained `HEALTHY`, displayed account KPIs exactly once, and is at
  `/Users/wilfred/Documents/Hummingbot/condor/reports/20260913_084247_derive_xrplink_adaptive_mm_health_179cfe.html`.
- SQLite stayed 204,800 bytes, passed `PRAGMA integrity_check`, emitted no lock
  error, and contained zero orders, positions, executors, and fills. Final
  container usage was 1.94% CPU, 219.1 MiB memory, and 5.29 MB block writes.
- Controller `errors.log` had zero lines and the runtime logs contained no
  error, exception, traceback, rejection, or `database is locked` match.
- Independent Hummingbot API reads showed zero active Derive XRP/LINK orders
  and zero Derive positions before shutdown and again after archival.
- Hummingbot stopped cleanly, the container was removed, the bot reported
  `stopped`, and Condor was stopped. No live trading was started.

## Remaining limitations

- Hummingbot 2.16.0's Derive connector does not support true reduce-only payloads.
- Derive 2.16.0 connector state does not expose reliable account equity or
  account realized PnL. Collateral balance is labeled separately and is not
  substituted for equity.
- A shadow validation proves runtime wiring and fail-closed behavior, not alpha,
  profitability, exchange fills, or authorization for live trading.

Live canary readiness: **NO**. Native equity is unavailable and no live canary was
authorized or started.
