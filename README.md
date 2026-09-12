# Derive Multi-Asset Adaptive Market Maker

This repository contains an isolated Hummingbot Strategy V2 research and
mainnet-shadow project for a bounded, multi-asset Derive perpetual market
maker. The canonical active universe is **XRP and LINK**. Binance, Bybit, and
OKX provide public reference data only; Derive is the sole execution venue in
the design. The reference path is strict `BINANCE -> BYBIT -> OKX -> PAUSE`.

The project is research infrastructure, not a profitability or deployment
claim. Public shadow fills are hypothetical, and no live order or account
mutation is performed by the standalone runner.

## Current scope

- Default profile: `conf/mainnet_shadow.yml`.
- Active assets: `XRP`, `LINK` only.
- Reference venues: Binance, Bybit, and OKX only.
- Bitget: disabled and absent from the active reference schedule.
- Execution: Derive perpetual only; reference venues are data-only.
- Mode: `MAINNET_SHADOW`, `dry_run: true`, `mainnet_armed: false`.
- Hedging, news filters, testnet workarounds, grids, and martingale logic are
  outside the project.
- Historical DOGE, ADA, ZEC, CC, SOL, BNB, and HYPE artifacts are retained for
  audit and reproducibility but are not part of the default active universe.

See [the current-universe record](docs/research/current_universe.md),
[retired-asset record](docs/research/retired_assets.md), and
[safety contract](docs/SAFETY.md) for the authoritative boundaries.

## Safety boundary

The public shadow path has no credential loader, private API path, order
placement, cancel endpoint, or reference-venue execution path. It records
internal quote and fill-model events only and asserts `real_orders: 0` and
`real_positions: 0`.

Reference selection never averages or median-combines sources and never
forward-fills across a gap. A stale primary fails over immediately; Binance
must remain fresh for the configured recovery duration before it is selected
again. A disagreement pause is fail-closed.

Active quotes refresh only when their distance from the current causal Derive
midpoint is strictly greater than 2% (`200` bps). Quote age and small fair-value
changes do not trigger normal refreshes; invalid, stale, paused, or risk
violations still cancel protectively.

The live-shaped example is deliberately disarmed. This checkout does not
authorize or start live execution. Completion, catalog coverage, public
liquidity, and shadow PnL are not evidence of deployable alpha.

## Repository layout

```text
controllers/market_making/        Hummingbot Strategy V2 adapter
src/derive_multi_asset_mm/         Decision layer and public shadow runner
conf/                               Active and historical configuration profiles
dashboard/                          Read-only local dashboard
scripts/                            Audits, reports, and maintenance helpers
tests/                              Deterministic unit and contract tests
docs/                               Architecture, safety, research, and retention
reports/published/                  Compact reviewed summaries safe to publish
logs/                               Local runtime telemetry; ignored by Git
reports/                            Local generated evidence; ignored by Git
```

Only compact reviewed Markdown summaries under `reports/published/` are
versioned. Raw telemetry, SQLite databases and sidecars, JSONL/NDJSON, logs,
runtime state, caches, raw BBO/tick/order-book payloads, and generated report
trees stay local. See [storage retention](docs/STORAGE_RETENTION.md).

## Install and deterministic checks

The project targets Python 3.11 or newer:

```bash
uv venv --python python3.11 .venv
uv pip install --python .venv/bin/python -e '.[dev]'
.venv/bin/pytest -q
.venv/bin/ruff check src controllers scripts dashboard tests
```

The Hummingbot adapter is checked against the installed image without
starting a bot:

```bash
docker run --rm \
  --entrypoint python \
  -v "$PWD:/workspace:ro" \
  -w /workspace \
  hummingbot/hummingbot-api:latest \
  /workspace/scripts/controller_contract_probe.py
```

The adapter reuses current Hummingbot V2 transport, controller, executor
action, market-data, connector-rule, quantization, and recording contracts.
The custom layer owns the multi-asset reference policy, fair value, basis,
adaptive deadband, inventory/portfolio risk, conservative fill diagnostics,
markouts, and research accounting. See [architecture](docs/architecture.md)
and the retained [Hummingbot reuse audit](reports/published/hummingbot_v2_reuse_summary.md).

## Public mapping audit

The audit dynamically discovers active Derive perpetual instruments through
`public/get_all_instruments`, loads exact symbols from installed Hummingbot
derivative connector maps with `trading_required=False`, and validates the
XRP/LINK mappings against public Binance, Bybit, and OKX contract metadata.
No substitute symbol is selected when an exact reference is unavailable.

```bash
.venv/bin/python -m derive_multi_asset_mm.audit \
  --config conf/mainnet_shadow.yml
```

## Shadow validation

Run a bounded public-data-only probe before any longer capture:

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

The runner records source health, selected priority source, failovers,
recovery hysteresis, disagreement pauses, causal Derive BBO/trade provenance,
basis, volatility, risk state, quote plans, lifecycle events, and action-rate
telemetry. A quote-active row is not an order submission, and a touch is not a
fill. Conservative fills require direction-aware strict Derive trade-through;
markout and net-capture conclusions remain insufficient when denominators are
missing.

Only after an independently reviewed preflight may a human choose to run a
longer shadow capture:

```bash
.venv/bin/python -m derive_multi_asset_mm.shadow start \
  --config conf/mainnet_shadow.yml \
  --duration 6h
```

The default output paths are `logs/xrp_link_mainnet_shadow/` and
`reports/xrp_link_mainnet_shadow/`. These generated trees are intentionally
ignored by Git.

## Dashboard

The dashboard is a read-only local monitor:

```bash
.venv/bin/python -m derive_multi_asset_mm.dashboard \
  --host 127.0.0.1 --port 8770
open http://127.0.0.1:8770/
```

It displays mode, armed state, real-order/position counters, active assets,
reference priority, source health, and research accounting. It exposes no
order or arm controls.

## Configuration profiles

`conf/mainnet_shadow.yml` is the canonical active profile. The two-asset
refresh profile is `conf/mainnet_shadow_refresh_research.yml`; it also contains
only XRP and LINK and is disarmed. Older profiles containing DOGE, ADA, ZEC,
CC, SOL, BNB, or HYPE are explicitly historical/reproducibility profiles and
must not be used for a new run. See [configuration boundaries](conf/README.md).

## Research status

The retained XRP/LINK observations show that the strategy can reach internal
quote planning for XRP, while LINK was blocked by a pre-quote notional gate in
the short snapshot. Both assets had insufficient direct Derive trade and
markout evidence for a viability conclusion. The later ZEC/XRP/LINK refresh
research was also insufficient and is now historical because ZEC was retired.

The known failed-run evidence records a SQLite `database is locked` error in
the decision-rollup write path. It was captured and archived for a separate
single-writer repair phase; this publication does not claim that the issue is
fixed. See the compact [current status](reports/published/current_status.md)
and [failure summary](reports/published/sqlite_failure_summary.md).

## Repository

The published repository is
[`doi-doi/derive-multi-asset-adaptive-mm`](https://github.com/doi-doi/derive-multi-asset-adaptive-mm).

Final classification remains `NOT_READY_FOR_SMALL_MAINNET_CANARY` until an
evidence review demonstrates sufficient causal data, exact exchange rules,
trade-through observations, markouts, costs, action-rate behavior, and capital
controls. This build is intentionally live-disarmed.
