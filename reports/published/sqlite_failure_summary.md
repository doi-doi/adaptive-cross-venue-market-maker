# SQLite failure summary

## Observed failure

The run `zec_xrp_link_refresh_20260909T142815Z` ended with:

```text
sqlite3.OperationalError: database is locked
```

The traceback points to the decision-rollup write in
`src/derive_multi_asset_mm/telemetry.py`, reached from
`ShadowRunner.process_asset()` while persisting a decision.

## Captured evidence

Before local compression, the database passed `PRAGMA quick_check(1)` with
`ok`. Captured row counts were:

| Table | Rows |
|---|---:|
| actions | 42,388 |
| decision_rollups | 151,494 |
| decisions | 65,931 |
| fills | 2 |
| markouts | 10 |
| minute_aggregates | 735 |
| reference_health | 433 |
| reference_values | 1,113 |
| state | 2 |
| trades | 49 |

The raw source bundle was archived locally only after SQLite readability,
Zstandard-frame, and tar-member checks. The error log, metadata, reports,
canonical fills/markouts, and verified archive remain available under the
project's local cleanup evidence.

## Interpretation and next gate

This establishes a persistence/concurrency failure at the decision-rollup
write. It does not by itself prove which competing connection or transaction
caused the lock. The required next work is a read-only Phase 1 SQLite audit,
then a single-writer repair and concurrency stress test. No strategy tuning,
live execution, or parameter promotion should be used to bypass this gate.
