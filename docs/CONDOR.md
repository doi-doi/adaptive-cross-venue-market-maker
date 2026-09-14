# Condor operations

These commands match the locally installed Condor MCP contract. Controller
file/config installation is a reviewed filesystem deployment step; the bot is
then created and controlled through Condor.

## Launch a shadow instance

After installing the runtime artifacts listed in `LOCAL_ENVIRONMENT.md`, upsert
the XRP controller config with `manage_controllers(target="config",
action="upsert", ...)`. Keep `shadow_mode=true`, `mainnet_armed=false`, and use
the existing `master_account` profile.

```text
manage_bots(
  action="deploy",
  bot_name="derive-binance-adaptive-mm-xrp-shadow",
  controllers_config=["derive_binance_adaptive_mm_xrp"],
  account_name="master_account",
  max_global_drawdown_quote=40,
  max_controller_drawdown_quote=25
)
```

Deployment calls Hummingbot API `deploy_v2_controllers`; Hummingbot remains the
strategy and execution runtime.

## Health routine

Install `condor/derive_mm_health.py` into the local Condor `routines/` folder,
then run:

```text
manage_routines(
  action="start",
  name="derive_mm_health",
  config={
    "bot_name": "derive-binance-adaptive-mm-xrp-shadow",
    "poll_interval_seconds": 3,
    "execution_enabled": false
  }
)
```

The routine reads `get_bot_status()` only. It does not poll exchanges, create
executors, change configs, or arm mainnet. It shows XRP BBO, fair value, basis,
state, inventory, desired/active quotes, quote age, mutation counts, fills,
volume, PnL, and markout availability.

## Status and controls

```text
manage_bots(action="status")

manage_bots(
  action="stop_controllers",
  bot_name="derive-binance-adaptive-mm-xrp-shadow",
  controller_names=["derive_binance_adaptive_mm_xrp"]
)

manage_bots(
  action="start_controllers",
  bot_name="derive-binance-adaptive-mm-xrp-shadow",
  controller_names=["derive_binance_adaptive_mm_xrp"]
)
```

`stop_controllers` is the normal pause/stop: it sets the XRP controller's
`manual_kill_switch=true`, is reversible, and is applied on Hummingbot's next
config reload. Recheck `status` after about 10 seconds.

Emergency stop uses the same controller stop first. If Hummingbot is
unresponsive, use `manage_bots(action="stop_bot", bot_name=...)`; this archives
the bot and is therefore a last resort. Neither operation arms mainnet.

There is intentionally no unattended `ARM_MAINNET` action.

## Health metric ownership

Condor reports XRP strategy executor PnL and position diagnostics. Derive
account equity (when available), collateral balance, available collateral,
account unrealized PnL, gross/net account exposure, and account drawdown are
copied from that controller.
Unsupported native values display as `N/A`.

Each asset row shows native feed age separately from last BBO-change age plus
the risk-adjusted bid/ask sizes and projected post-fill notionals.
