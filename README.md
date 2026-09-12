# Derive Multi-Asset Priority-Reference MM V2

This is an isolated Hummingbot Strategy V2 research project for a bounded,
multi-asset Derive perpetual market-making decision layer for DOGE, ADA, and
XRP. Binance, Bybit, OKX, and Bitget perpetuals are used only as public
external market-data references. The primary path is strict `BINANCE -> BYBIT
-> OKX -> PAUSE`; Bitget is diagnostics-only. Derive remains the sole
execution venue. The project never hedges on a reference venue, never uses
news filters, and does not claim that public shadow fills are executable
economics.

The next active research phase is isolated in
`conf/mainnet_shadow_refresh_research.yml` and uses only ZEC, XRP, and LINK.
The DOGE/ADA results and the currently running mid-price baseline remain
historical evidence and are not overwritten.

## Goal

Build a deterministic, audit-friendly controller and public mainnet shadow
pipeline for DOGE, ADA, and XRP, with dynamic Derive and installed-connector
market validation, strict priority selection, immediate stale-source failover,
three-second Binance recovery hysteresis, disagreement pauses, basis
protection, volatility and direction state, inventory-aware quoting, portfolio
limits, conservative and touch-sensitivity shadow fills, markouts, source
health, and a read-only dashboard. Historical eight-asset consensus code and
reports remain preserved for comparison.

## Safety boundary

- The default configuration is `MAINNET_SHADOW`, `dry_run: true`, and
  `mainnet_armed: false`.
- The shadow runner uses only public Derive, Binance, Bybit, OKX, and Bitget
  endpoints. It has no credential loader, private API path, order placement,
  cancel path, or reference-venue execution path.
- The default config enables only DOGE, ADA, and XRP. CC, SOL, LINK, BNB, and
  HYPE are disabled from new runs; the old eight-asset consensus config is
  retained at `conf/mainnet_shadow_8asset_consensus.yml`.
- Primary reference selection never averages or median-combines sources and
  never forward-fills. A stale primary fails over immediately; Binance is
  selected again only after a fresh snapshot remains healthy for three seconds.
- Bitget remains available for diagnostics but is never eligible as the
  primary reference.
- Shadow lifecycle actions are internal records only. The continuous safety
  state asserts `real_orders: 0` and `real_positions: 0`.
- A live controller config must explicitly use `MAINNET_LIVE`,
  `dry_run: false`, and `mainnet_armed: true`; this build does not start it.
- The strategy emits at most one post-only bid and one post-only ask per asset.
  It has no grid, ladder, martingale, hedging, or news component.
- Active quotes refresh only when their distance from the current causal Derive
  midpoint is strictly greater than 2% (200 bps). Quote age and small changes
  in the desired fair-value quote do not trigger normal refreshes; paused or
  invalid quote plans still cancel protectively.
- Testnet is intentionally unsupported. Do not add a testnet workaround to
  make a connector check pass.

The refresh-deadband phase is documented in
`docs/REFRESH_DEADBAND_RESEARCH.md`. It measures the 7×6 deadband/residency
grid on one shared observation stream, with separate conservative and touch
fill views, action-rate governance, and adverse-move cancellation overrides.

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
reports/priority_reference_3asset/ Generated three-asset priority reports
logs/priority_reference_3asset/    State, SQLite telemetry, and runner logs
reports/zec_xrp_link_refresh_research/ Refresh deadband/residency artifacts
logs/zec_xrp_link_refresh_research/    Isolated ZEC/XRP/LINK shadow telemetry
reports/multi_reference_shadow/     Preserved historical consensus reports
logs/multi_reference_shadow/        Preserved historical consensus logs
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
using `public/get_all_instruments`, loads exact symbols from the installed
Hummingbot derivative connectors with `trading_required=False`, and validates
each exact DOGE/ADA/XRP Binance/Bybit/OKX/Bitget public USDT perpetual. There
are no new-run claims for the disabled assets.

```bash
.venv/bin/python -m derive_multi_asset_mm.audit --config conf/mainnet_shadow.yml
```

An unavailable exact reference disables that asset with
`REFERENCE_MARKET_UNAVAILABLE`; no substitute symbol is selected.

## Ten-minute public mainnet shadow validation

Run it detached so the shell is not held open:

```bash
.venv/bin/python -m derive_multi_asset_mm.shadow start \
  --config conf/mainnet_shadow.yml \
  --duration 10m \
  --run-id validation_10m_<timestamp>

.venv/bin/python -m derive_multi_asset_mm.shadow status \
  --config conf/mainnet_shadow.yml \
  --run-id validation_10m_<timestamp>

.venv/bin/python -m derive_multi_asset_mm.shadow audit \
  --config conf/mainnet_shadow.yml \
  --run-id validation_10m_<timestamp>
```

The check requires a public Derive BBO before a decision row is written and
uses fresh source observations according to each venue's configured TTL. It
records the selected source, priority transitions, failovers, recovery
hysteresis, disagreement pauses, per-source health, update gaps,
sequence/reject counters, selected fair value, basis, direction, volatility,
market mode, inventory mode, quote plan, lifecycle events, and processing
latency. It does not require a shadow fill to be considered operational, but
fill-dependent markout conclusions remain
`INSUFFICIENT_OBSERVATIONS` when no fill occurs.

Conservative fills require direction-aware strict trade-through. The separate
`TOUCH_SENSITIVITY` model is explicitly labeled and must not be presented as
realized execution.

If the ten-minute pipeline passes for all three assets, start the detached
six-hour capture with the default report/log directory:

```bash
.venv/bin/python -m derive_multi_asset_mm.shadow start \
  --config conf/mainnet_shadow.yml \
  --duration 6h
```

The required priority artifacts are generated under
`reports/priority_reference_3asset/`:

- `asset_rules.csv`, `reference_health.csv`, `reference_selection.csv`
- `reference_failovers.csv`, `reference_recovery.csv`, `reference_disagreement.csv`
- `spread_statistics.csv`, `quote_activity.csv`, `quote_churn.csv`
- `shadow_fills_conservative.csv`, `shadow_fills_touch.csv`
- `reference_markouts.csv`, `derive_markouts.csv`, `net_capture.csv`
- `model_comparison.csv`, `asset_comparison.csv`, `portfolio_exposure.csv`
- `final_report.md`, `final_report.json`

Historical consensus artifacts remain under `reports/multi_reference_shadow/`:

- `reference_market_mapping.csv`, `reference_source_health.csv`, `reference_source_gaps.csv`
- `reference_reconnects.csv`, `reference_sequence_audit.csv`, `reference_outliers.csv`
- `reference_disagreement.csv`, `reference_availability.csv`, `consensus_fair_value.csv`
- `source_dispersion.csv`, `derive_basis.csv`, `derive_trading_rules.csv`
- `capital_compatibility.csv`, `asset_spread_statistics.csv`, `asset_activity.csv`
- `asset_quote_churn.csv`, `shadow_fills.csv`, `derive_markouts.csv`, `reference_markouts.csv`
- `net_capture.csv`, `reference_model_comparison.csv`, `source_ablation.csv`, `lead_lag.csv`
- `reference_failover_counters.csv`, `reference_protection.csv`, `asset_opportunity_scores.csv`
- `portfolio_exposure.csv`
- `portfolio_performance.csv`, `data_health.csv`
- `final_multi_reference_shadow_report.md`, `final_multi_reference_shadow_report.json`

`lead_lag.csv` uses explicit timestamp matching and no forward fill. The
Derive-only control is measured on the same Derive timestamps and is not
silently replaced by the Binance reference.

## Dashboard

Start the local read-only monitor on port `8770`:

```bash
.venv/bin/python -m derive_multi_asset_mm.dashboard --host 127.0.0.1 --port 8770
open http://127.0.0.1:8770/
```

The banner continuously shows mainnet, mode, armed state, real orders, real
positions, the explicit priority flow, and Bitget's diagnostics-only status.
The page refreshes state every second and exposes no order or arm controls. It
is a monitor, not an execution console.

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
