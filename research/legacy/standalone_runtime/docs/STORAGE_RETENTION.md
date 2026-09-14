# Shadow storage retention

This project keeps strategy evaluation and storage policy separate. The
storage governor changes what is persisted, never the 250 ms strategy loop,
quote inputs, order sizing, lifecycle state machine, fill rules, or markout
calculation.

## Policy

The configured defaults are:

- warning / critical / emergency free space: 15 / 10 / 5 GB;
- raw detail buffer: 180 seconds;
- feature snapshot interval: 1 second;
- permanent aggregate interval: 60 seconds;
- closed-file rotation/archive interval: 10 minutes.

The generated-data budgets are also explicit and persistence-only:

- per-run warning at 2 GB, critical at 2.5 GB, and a hard maximum at 3 GB;
- project generated-data maximum at 10 GB, measured from `logs/` and
  `reports/` only (source, configuration, and environment files are excluded).

The governor measures each run's SQLite database plus `-wal`, `-shm`, and
`-journal` sidecars. At the warning budget it continues normal collection while
the maintenance worker prunes expired noncritical raw detail. At the critical
budget it suppresses unchanged optional detail; at the hard maximum it enters
emergency persistence behavior. When the project budget is reached, closed-run
maintenance must archive/prune the oldest noncritical generated data before a
new run is started. These controls never delete final reports, canonical fills,
markouts, trade/PnL evidence, or active/incomplete data automatically.

Each configured runner creates one `minute_aggregates` row per asset and
minute. Shared Derive BBO, median mid/spread/fair-value/basis, spread P90,
selected-reference occupancy, reference health/value counters, market state,
inventory, quote uptime, action, trade, fill, maker-volume, PnL proxy, and
error counters are stored once per asset-minute. Control/model differences are
kept in compact JSON counters, so the same BBO is not copied once per model.

Every decision is counted in `decision_rollups` with a semantic signature and
first/last timestamps. Full feature payloads are sampled at the configured
interval and on semantic changes. HOLD actions are not written as individual
rows when the governor is enabled; their model/action counts remain permanent
in the minute aggregate. CREATE/CANCEL/REPLACE, fills, markouts, Derive trades,
errors, inventory/PnL state, and metadata remain row-level.

At critical and emergency levels the governor suppresses unchanged decision
detail and keeps only sparse reference-health continuity samples. A daemon
maintenance worker uses bounded batched deletes to prune only expired
decision/reference detail after the 180-second window; it requires the
permanent minute table and retains decision rows carrying failover, pause,
block, or protection semantics. It never deletes fills, markouts, trades,
mutations, rollups, aggregates, or active rows. Closed-run cleanup is performed by
`scripts/storage_maintenance.py`, which writes a before-manifest, refuses
open/incomplete/ambiguous paths, checkpoints only closed SQLite databases,
verifies schema/row counts/readability, tests the Zstandard frame, and only
then removes the original source. Final reports remain uncompressed.

Parquet is not enabled because the current project environment has no
`pyarrow`/Parquet runtime. SQLite plus Zstandard is the compatible archive
format used here, and the manifest records that capability decision.

Incomplete or ambiguous historical runtime state remains protected until its
root cause and evidence dependencies are resolved. For a healthy future run,
the storage worker archives eligible old completed runs on a rolling schedule
and attempts the target run only after its final report exists, the runner exits,
and no process has the DB or WAL open. Ordinary successful full raw detail is
eligible for archival/pruning after 24 hours; failed infrastructure raw detail
is retained until its root-cause review is complete.
