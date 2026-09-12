# Current project status

## Classification

`NOT_READY_FOR_SMALL_MAINNET_CANARY`

The project is finalized as a clean, live-disarmed research repository. The
active universe is XRP and LINK. No new shadow run was started during this
cleanup/publication pass.

| Boundary | Status |
|---|---|
| Default mode | `MAINNET_SHADOW` |
| Dry run | `true` |
| Mainnet armed | `false` |
| Real orders / positions | `0 / 0` |
| Execution | Derive perpetual only |
| References | Binance -> Bybit -> OKX -> Pause |
| Bitget | Disabled and not scheduled |
| Current evidence | Insufficient for viability or live promotion |

## Observed XRP/LINK snapshot

The retained `xrp_link_replacement_20260909_125108Z` snapshot covered only
11.3 minutes and was explicitly provisional:

| Measure | XRP | LINK |
|---|---:|---:|
| Decision cycles | 674 | 2,419 |
| Healthy decisions | 674 (100%) | 2,419 (100%) |
| Quote-active cycles | 661 (98.07%) | 0 (0%) |
| Derive trades | 0 | 0 |
| Conservative shadow fills | 0 | 0 |
| Main block | 13 fast-reference moves | 2,411 single-order-notional blocks; 8 fast-reference moves |

XRP reached internal quote planning, but had no direct Derive trade-through
evidence. LINK was blocked before quote creation by a notional guard in that
short window. Neither asset has enough trade/fill and 30/60-second markout
denominators for a viability conclusion.

The later refresh-research report remains `REFRESH_RESEARCH_DATA_INSUFFICIENT`:
its retained aggregate denominators were 49 Derive trades, 2 observed shadow
fills, and 10 markout rows against the project's larger sufficiency gate.

## Known issue

The failed refresh run recorded `sqlite3.OperationalError: database is locked`
while writing the decision rollup in `telemetry.py`. The failure evidence was
captured and the raw database was archived locally after readability and archive
verification. The single-writer repair phase is still outstanding; this
publication does not claim the issue is resolved.

## Storage and publication boundary

The prior project-local emergency cleanup recovered approximately 14.132 GiB
of filesystem space and left approximately 22.835 GiB free at its after-check.
Eleven verified Zstandard archives preserve the large historical telemetry
source. A later audit found no project process or open project path and kept
non-empty ambiguous SQLite sidecars protected. Current generated evidence is
still local and ignored. The final project-local check measured approximately
22.673 GiB free and 0.790 GiB of project data; the safe cache pass removed 180
regenerable files and removed no raw market data, database, or Bitget evidence.

Source evidence retained locally includes the XRP/LINK snapshot under
`reports/priority_reference_3asset_6h/`, refresh evidence under
`reports/zec_xrp_link_refresh_research/`, the reuse audit under
`reports/hummingbot_v2_reuse_audit/`, and the cleanup audit under
`reports/storage_cleanup_20260912/`.
