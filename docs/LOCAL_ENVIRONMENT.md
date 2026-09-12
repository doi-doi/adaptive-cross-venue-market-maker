# Local environment audit

Audited on 2026-09-12 before implementation. No bot or exchange mutation was
performed.

| Component | Observed local source of truth |
|---|---|
| Hummingbot package | `20260729` from the last installed-image contract probe; Docker was stopped during this audit, so live re-probe remains required |
| Isolated Hummingbot CI image | `hummingbot/hummingbot:latest`, digest `sha256:632d2b07aa156b761310f2f7258a78c9660a1c28b6df4b33874e09a0c7d06c85`; native contract passed in run `34702709151` |
| Hummingbot API checkout | `/Users/wilfred/Documents/Hummingbot/hummingbot-api`, commit `73e5400c960004a22695583e7201108ec44d6ab4` |
| Controller base | `hummingbot.strategy_v2.controllers.controller_base.ControllerBase` / `ControllerConfigBase` |
| Market-making base | Installed but single-pair convenience behavior is not used; this controller uses `ControllerBase` |
| Market data | `MarketDataProvider.get_order_book`, `get_trading_rules`, `get_connector`, native quantization |
| Actions | `CreateExecutorAction`, `StopExecutorAction` |
| Executor | `OrderExecutorConfig` with `ExecutionStrategy.LIMIT_MAKER` |
| Orchestration | native `ExecutorOrchestrator` through `v2_with_controllers.py` |
| Connectors | `derive_perpetual` execution, `binance_perpetual` public reference |
| Rate limits | native connector throttlers; controller adds only a quote-mutation budget |
| Backtesting | V2 controller framework available; two-venue BBO behavior is only partially representable |
| Condor checkout | `/Users/wilfred/Documents/Hummingbot/condor`, commit `11198d688a1c2082d5ed538f3e647fed3a405d8d` |
| Condor package | `0.1.0`; upstream uses continuous `main`, not fixed releases |
| Condor routine contract | `Config` Pydantic model plus async `run(config, context)`; `CONTINUOUS = True` |
| Condor deployment | `manage_bots(action="deploy", ...)` -> `deploy_v2_controllers` |
| Existing account reference | credentials profile `master_account` contains a `derive_perpetual` connector file; secret values were not copied or committed |

A retained public-metadata snapshot from 2026-09-10 recorded XRP tick `0.00001`,
amount step `0.1`, minimum amount `10`; LINK tick `0.0001`, amount step `0.001`,
minimum amount `10`. Those values are time-sensitive evidence, not hardcoded
rules. The controller reads the current native rules, quantizes both fields,
and pauses the side when its configured order cannot meet the native minimum.

The local Hummingbot API and Condor checkouts were already dirty and behind
their remotes. They were inspected read-only and not modified. The final work
is isolated in this repository/worktree.

## Deployment paths

Copy only these reviewed files into the installed API/Condor trees:

```text
controllers/market_making/derive_binance_adaptive_mm.py
  -> hummingbot-api/bots/controllers/market_making/derive_binance_adaptive_mm.py

configs/derive_binance_adaptive_mm_xrp.yml
configs/derive_binance_adaptive_mm_link.yml
  -> hummingbot-api/bots/conf/controllers/

condor/derive_mm_health.py
  -> condor/routines/derive_mm_health.py
```

Do not copy `.env`, connector YAML, wallet material, logs, or databases.
