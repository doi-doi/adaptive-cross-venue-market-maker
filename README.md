# Derive Multi-Asset Binance-Reference MM V2

This is an isolated Hummingbot Strategy V2 research project for a bounded,
multi-asset Derive perpetual market-making decision layer. Binance USD-M
perpetuals are used only as public external fair-value and market-state
references. The project never hedges on Binance, never uses news filters, and
does not claim that public shadow fills are executable economics.

## Goal

Build a deterministic, audit-friendly controller and public mainnet shadow
pipeline for ADA, CC, XRP, and SOL, with dynamic market validation, fair value,
basis protection, volatility and direction state, inventory-aware quoting,
portfolio limits, conservative and touch-sensitivity shadow fills, markouts,
and a read-only dashboard.

## Safety boundary

- The default configuration is `MAINNET_SHADOW`, `dry_run: true`, and
  `mainnet_armed: false`.
- The shadow runner uses only public Derive and Binance endpoints. It has no
  credential loader, private API path, order placement, cancel path, or Binance
  execution path.
- Shadow lifecycle actions are internal records only. The continuous safety
  state asserts `real_orders: 0` and `real_positions: 0`.
- A live controller config must explicitly use `MAINNET_LIVE`,
  `dry_run: false`, and `mainnet_armed: true`; this build does not start it.
- The strategy emits at most one post-only bid and one post-only ask per asset.
  It has no grid, ladder, martingale, hedging, or news component.
- Testnet is intentionally unsupported. Do not add a testnet workaround to
  make a connector check pass.

This is not ready for a small mainnet canary merely because the code imports or
the dashboard renders. Promote only after a separately approved evidence
review covering causal data availability, exact exchange rules, trade-through
fill evidence, markouts, costs, action-rate behavior, and capital limits.

## Layout

```text
controllers/market_making/       Current Hummingbot Strategy V2 adapter
src/derive_multi_asset_mm/        Pure decision layer and public shadow runner
conf/                              Mainnet shadow config and unarmed live template
dashboard/                         Read-only local dashboard
scripts/                           Detached startup and status helpers
tests/                             Deterministic unit and contract tests
reports/mainnet_shadow/            Generated reports, ignored by Git
logs/mainnet_shadow/               State, SQLite telemetry, and runner logs
docs/                              Architecture and safety notes
```

## Install and deterministic checks

The project targets Python 3.11 or newer. From this directory:

```bash
uv venv --python python3.11 .venv
uv pip install --python .venv/bin/python -e '.[dev]'
.venv/bin/pytest -q
.venv/bin/ruff check src controllers scripts dashboard tests
```

The adapter is designed for the current Hummingbot image. The build-time
contract probe uses the installed image and must remain a shadow/no-action
probe:

```bash
docker run --rm \
  --entrypoint python \
  -v "$PWD:/workspace:ro" \
  -w /workspace \
  hummingbot/hummingbot-api:latest \
  /workspace/scripts/controller_contract_probe.py
```

The adapter uses `ControllerBase` because the installed market-making base is
single-pair. It owns the multi-asset state and uses the current executor action
contracts only on the explicitly armed live path. The shadow path returns an
empty Hummingbot action list.

## Public mapping audit

The mapping audit dynamically enumerates active Derive perpetual instruments
using `public/get_all_instruments` and validates an exact Binance USD-M
perpetual `<ASSET>USDT` reference. There are no hard-coded claims that CC or
any other asset is available.

```bash
.venv/bin/python -m derive_multi_asset_mm.audit --config conf/mainnet_shadow.yml
```

An unavailable exact reference disables that asset with
`REFERENCE_MARKET_UNAVAILABLE`; no substitute symbol is selected.

## Ten-minute public mainnet shadow check

Run it detached so the shell is not held open:

```bash
.venv/bin/python -m derive_multi_asset_mm.shadow start \
  --config conf/mainnet_shadow.yml \
  --duration 10m

.venv/bin/python -m derive_multi_asset_mm.shadow status \
  --config conf/mainnet_shadow.yml

.venv/bin/python -m derive_multi_asset_mm.shadow audit \
  --config conf/mainnet_shadow.yml
```

The check requires public Derive and Binance BBOs before a decision row is
written. It records fair value, robust rolling basis, direction, volatility,
market mode, inventory mode, quote plan, lifecycle create/hold/replace/cancel
events, and processing latency. It does not require a shadow fill to be
considered operational, but fill-dependent markout conclusions remain
`INSUFFICIENT_OBSERVATIONS` when no fill occurs.

Conservative fills require direction-aware strict trade-through. The separate
`TOUCH_SENSITIVITY` model is explicitly labeled and must not be presented as
realized execution.

Generated artifacts include:

- `reports/mainnet_shadow/asset_reference_mapping.csv`
- `reports/mainnet_shadow/derive_trading_rules.csv`
- `reports/mainnet_shadow/capital_compatibility.csv`
- `reports/mainnet_shadow/fair_value_quality.csv`
- `reports/mainnet_shadow/basis_statistics.csv`
- `reports/mainnet_shadow/market_state_occupancy.csv`
- `reports/mainnet_shadow/direction_state_occupancy.csv`
- `reports/mainnet_shadow/inventory_state_occupancy.csv`
- `reports/mainnet_shadow/quote_activity.csv`
- `reports/mainnet_shadow/fills.csv`
- `reports/mainnet_shadow/binance_markouts.csv`
- `reports/mainnet_shadow/derive_markouts.csv`
- `reports/mainnet_shadow/toxicity.csv`
- `reports/mainnet_shadow/net_capture.csv`
- `reports/mainnet_shadow/asset_opportunity_scores.csv`
- `reports/mainnet_shadow/portfolio_exposure.csv`
- `reports/mainnet_shadow/portfolio_performance.csv`
- `reports/mainnet_shadow/data_health.csv`
- `reports/mainnet_shadow/latency.csv`
- `reports/mainnet_shadow/lead_lag.csv`
- `reports/mainnet_shadow/protection_effectiveness.csv`
- `reports/mainnet_shadow/control_comparison.csv`
- `reports/mainnet_shadow/final_multi_asset_shadow_report.md`
- `reports/mainnet_shadow/final_multi_asset_shadow_report.json`

`lead_lag.csv` uses explicit timestamp matching and no forward fill. The
Derive-only control is measured on the same Derive timestamps and is not
silently replaced by the Binance reference.

## Dashboard

Start the local read-only monitor on port `8770`:

```bash
.venv/bin/python -m derive_multi_asset_mm.dashboard --host 127.0.0.1 --port 8770
open http://127.0.0.1:8770/
```

The banner continuously shows mainnet, mode, armed state, real orders, and real
positions. The page refreshes state every second and exposes no order or arm
controls. It is a monitor, not an execution console.

## Condor / Hummingbot integration

Use the project’s controller file with the current Hummingbot installation:

```text
controllers/market_making/derive_multi_asset_binance_reference_mm.py
```

`conf/mainnet_live.example.yml` is only an explicit, human-gated template for
the current Strategy V2 adapter. It is not used by the public shadow runner and
must not be treated as authorization to start live execution. Before any live
use, independently verify the connector’s current symbol mapping, trading
rules, account state, executor lifecycle, fee schedule, action-rate behavior,
and the evidence gates in `docs/SAFETY.md`.

## Completion classification

The project intentionally writes `NOT_READY_FOR_SMALL_MAINNET_CANARY` during
this build. That classification is a truthful stop gate: code completion,
catalog breadth, public liquidity, or a copied runtime contract is not proof of
deployable alpha or safe live execution.
