# Local environment audit

Audited again on 2026-09-13 during live-safety hardening. No live order or
exchange mutation was performed.

| Component | Observed local source of truth |
|---|---|
| Hummingbot package | `2.16.0` read from `/home/hummingbot/hummingbot/VERSION` in the pinned image |
| Isolated Hummingbot CI image | `hummingbot/hummingbot:version-2.16.0`, immutable digest `sha256:e222f070d42814013fb5ea7fe537926f790b259512950369da1e15a69dcbd38f`; the old `632d...` value was verified as floating `latest` and rejected |
| Hummingbot API checkout | `/Users/wilfred/Documents/Hummingbot/hummingbot-api`, commit `73e5400c960004a22695583e7201108ec44d6ab4` |
| Controller base | `hummingbot.strategy_v2.controllers.controller_base.ControllerBase` / `ControllerConfigBase` |
| Market-making base | Installed but single-pair convenience behavior is not used; this controller uses `ControllerBase` |
| Market data | `MarketDataProvider.get_order_book`, `get_trading_rules`, `get_connector`, native quantization |
| Actions | `CreateExecutorAction`, `StopExecutorAction` |
| Executor | `OrderExecutorConfig` with `ExecutionStrategy.LIMIT_MAKER` |
| Orchestration | native `ExecutorOrchestrator` through `v2_with_controllers.py` |
| Connectors | `derive_perpetual` execution; native `binance_perpetual` data through Hummingbot's `binance_perpetual_paper_trade` public wrapper |
| Rate limits | native connector throttlers; controller adds only a quote-mutation budget |
| Backtesting | V2 controller framework available; two-venue BBO behavior is only partially representable |
| Condor checkout | `/Users/wilfred/Documents/Hummingbot/condor`, commit `11198d688a1c2082d5ed538f3e647fed3a405d8d` |
| Condor package | `0.1.0`; upstream uses continuous `main`, not fixed releases |
| Condor routine contract | `Config` Pydantic model plus async `run(config, context)`; `CONTINUOUS = True` |
| Condor deployment | `manage_bots(action="deploy", ...)` -> `deploy_v2_controllers` |
| Existing account reference | credentials profile `master_account` contains a `derive_perpetual` connector file; secret values were not copied or committed |

A retained public-metadata snapshot from 2026-09-10 recorded XRP tick `0.00001`,
amount step `0.1`, and minimum amount `10`. Those values are time-sensitive
evidence, not hardcoded rules. The controller reads the current native rules, quantizes both fields,
and pauses the side when its configured order cannot meet the native minimum.

The local Hummingbot API and Condor checkouts were already dirty and behind
their remotes. They were inspected read-only and not modified. The final work
is isolated in this repository/worktree.

The logical reference venue remains `binance_perpetual`. At runtime the
controller registers `binance_perpetual_paper_trade`, Hummingbot's
credentialless wrapper around the native Binance perpetual order-book tracker.
This avoids a private Binance account while retaining native public BBO data.
No executor is ever created for that wrapper.

Shadow mode similarly registers `derive_perpetual_paper_trade` for Derive BBO
and native trading rules, so validation cannot be blocked by or mutate account
state. Switching `shadow_mode=false` changes that market back to the real
`derive_perpetual` connector; executor configs are always hard-wired to the real
connector and still require `mainnet_armed=true` before creation.

Hummingbot 2.16.0 also contains a Derive startup race where the initial-book
loader and parsed-snapshot listener consume the same raw queue. The controller
installs a guarded compatibility shim only when that exact queue-consuming
method is detected. It waits for the connector's own parsed snapshot cache; it
does not replace Derive transport, authentication, trading rules, or execution.

## Deployment paths

Copy only these reviewed files into the installed API/Condor trees:

```text
controllers/market_making/derive_binance_adaptive_mm.py
  -> hummingbot-api/bots/controllers/market_making/derive_binance_adaptive_mm.py

configs/derive_binance_adaptive_mm_xrp.yml
  -> hummingbot-api/bots/conf/controllers/

condor/derive_mm_health.py
  -> condor/routines/derive_mm_health.py
```

Do not copy `.env`, connector YAML, wallet material, logs, or databases.
