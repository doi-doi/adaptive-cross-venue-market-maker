# Adaptive Cross-Venue Market Maker

**Hummingbot Strategy V2 + Condor | XRP / LINK | Derive perpetual execution |
Binance perpetual reference**

This is the final Agent Builders Cup architecture. The competition bot runs
inside Hummingbot. A single reusable `derive_binance_adaptive_mm` controller is
instantiated once for XRP and once for LINK; Condor launches, monitors, pauses,
resumes, and stops the Hummingbot instance.

```text
Binance perpetual -> fair value + market state
                           |
Derive BBO ---------------+-> mode -> inventory override -> desired quotes
                                                        |
                                                        v
Condor -> Hummingbot V2 -> OrderExecutor -> derive_perpetual -> Derive
```

Binance is public market data only. Every `CreateExecutorAction` is hard-wired
to `derive_perpetual`; there is no custom private REST/WebSocket execution.

## Strategy

1. Read Binance BBO, microprice, short return, and short volatility.
2. Estimate causal Derive fair value from Binance plus rolling Derive/Binance basis.
3. Observe Derive BBO and native trading rules.
4. Select a deterministic market-making mode.
5. Let inventory and portfolio risk override directional skew.
6. Project each fill against signed inventory, safely resize it, and atomically reserve shared XRP/LINK risk.
7. Produce at most one post-only bid and one post-only ask.
8. Preserve queue residency with tick-aware hold, deadband, and minimum residency.
9. Cancel the vulnerable side immediately on a fast adverse Binance move.
10. Pause on stale Binance, stale Derive, extreme conditions, or risk limits.

| Market state | MM mode |
|---|---|
| `NORMAL` | `NEUTRAL` |
| `UP_TREND` | `LONG_BIAS` |
| `DOWN_TREND` | `SHORT_BIAS` |
| `HIGH_VOL` | `DEFENSIVE` |
| `EXTREME` or stale | `PAUSED` |

Inventory override may modify or disable either side in every active mode. Bias
is a modest maker-quote skew, never a directional position target.

## Safety defaults

- `shadow_mode: true`
- `mainnet_armed: false`
- total portfolio: `800 USDC`, with `200 USDC` reserve
- XRP cap: `300 USDC`; LINK cap: `300 USDC`
- Derive execution only; Binance reference only
- other assets and reference exchanges disabled
- no automatic mainnet arming

Real creates require both `shadow_mode: false` and `mainnet_armed: true`.
Shadow mode computes the full state and desired quotes but emits no creates.
The committed configs disable position flips and add an account-equity drawdown
gate where authenticated native account state is available. Hummingbot 2.16.0's
Derive connector always sends `reduce_only: false`, including for
`PositionAction.CLOSE`; reducing quotes are capped at flatten by controller
sizing, not represented as exchange-native reduce-only orders.

## Repository layout

```text
controllers/market_making/derive_binance_adaptive_mm.py  native V2 controller
configs/derive_binance_adaptive_mm_xrp.yml               XRP instance
configs/derive_binance_adaptive_mm_link.yml              LINK instance
condor/derive_mm_health.py                               read-only health routine
docs/                                                    architecture and operations
research/legacy/standalone_runtime/                      retired standalone runtime
submission/                                              competition status
tests/                                                   deterministic invariants
```

The older standalone `src`, configs, dashboard, scripts, reports, and tests are
preserved together under `research/legacy/standalone_runtime/`. They are not
the competition runtime.

## Verify

```bash
python -m pytest -q
ruff check controllers condor tests scripts
python scripts/validate_competition_surface.py
docker run --rm --volume "$PWD:/workspace:ro" --workdir /workspace \
  --entrypoint /opt/conda/envs/hummingbot/bin/python \
  hummingbot/hummingbot@sha256:e222f070d42814013fb5ea7fe537926f790b259512950369da1e15a69dcbd38f \
  /workspace/scripts/controller_contract_probe.py
```

The native contract probe must run inside the installed Hummingbot API image.
See [local environment](docs/LOCAL_ENVIRONMENT.md), [Condor operations](docs/CONDOR.md),
[runbook](docs/RUNBOOK.md), and [hackathon compliance](docs/HACKATHON_COMPLIANCE.md).

Repository: [doi-doi/adaptive-cross-venue-market-maker](https://github.com/doi-doi/adaptive-cross-venue-market-maker)
