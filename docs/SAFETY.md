# Live-safety contract

The committed system is shadow-only and disarmed:

- `shadow_mode: true`
- `mainnet_armed: false`
- XRP and LINK only
- Binance perpetual is public reference data only
- Derive perpetual is the only possible execution connector
- at most one maker bid and one maker ask per asset

No live order is authorized by this repository state. A future operator must
change both arming keys deliberately and repeat the runbook gates.

## Projected inventory

Every proposed fill is signed (`BUY = +amount`, `SELL = -amount`) and added to
the current signed Derive position. The controller values a proposal at the
more conservative of its quote price and current Derive midpoint. It safely
resizes before quantization and blocks the side when the residual size cannot
meet Derive's native minimum.

The atomic portfolio reservation covers active orders plus create actions that
have been returned to Hummingbot but are not yet visible as active executors.
It enforces:

- projected absolute asset inventory at or below `max_asset_inventory_quote`;
- projected portfolio gross inventory at or below `max_total_inventory_quote`;
- absolute position plus open/pending orders at or below `asset_cap_quote`;
- asset and portfolio open-order limits;
- gross current inventory plus open/pending orders at or below capital minus reserve.

Pending reservations reconcile when the executor becomes active. Only their
owning controller may expire them after 30 seconds if creation fails; a stalled
controller's peer retains its reservation and therefore fails closed. Registry
decisions are serialized by one process-wide lock, closing the XRP/LINK
check-then-create race.

## Position action and flips

The pinned Hummingbot 2.16.0 Derive `_place_order` payload hardcodes
`reduce_only: false`. `PositionAction.OPEN` and `PositionAction.CLOSE` affect
Hummingbot budget semantics and limit time-in-force selection, but neither
creates a true Derive reduce-only order. The controller continues to use
`PositionAction.OPEN` for symmetric maker quotes and does not claim native
reduce-only protection.

Each quote is classified as `INCREASE_SAME_DIRECTION`, `REDUCE`, `FLATTEN`, or
`FLIP_DIRECTION`. The committed `allow_position_flips: false` default caps an
inventory-reducing order at flatten. A deliberate future opt-in permits a flip
only in `NORMAL` market state and still applies every projected cap. This
preserves two-sided quoting whenever limits permit without allowing an
oversized maker quote to flip accidentally.

## Data and account risk

Hummingbot 2.16.0 `OrderBookTracker.metrics` provides per-pair monotonic
timestamps for accepted diff and snapshot messages. The controller reports
that as `NATIVE_MESSAGE_TIMESTAMP` and separately reports BBO-change age. If
the metric is absent or incompatible it fails back to the conservative
`BBO_CHANGE_FALLBACK`; it never labels price activity as proven transport
freshness.

Strategy executor PnL/drawdown remains research telemetry. Authenticated live
connectors separately expose native USDC collateral balance, available
collateral, unrealized PnL, and marked gross/net position exposure. Hummingbot
2.16.0 exposes neither reliable account realized PnL nor account equity, so both
are `N/A` rather than inferred. Collateral-balance drawdown is reported under
its own name and is not substituted for equity drawdown. The account-equity
kill path is implemented but cannot engage unless the connector exposes a
native `account_equity`; shadow mode reports all account fields as `N/A`.

## Evidence boundary

Shadow health, catalog breadth, passing imports, proxy fills, and controller
PnL do not prove deployable alpha or exchange fills. `TOUCH != FILL`, and this
hardening task does not authorize a live canary.
