# Quote/fill diagnostics

The project now has a measurement-only quote-lifetime and Derive-trade
crossing audit. It reconstructs quote intervals from retained CREATE, CANCEL,
FILL, and Derive trade events. It does not submit orders, change quote
parameters, or write to the run's SQLite telemetry database.

For a live or completed run:

```text
./.venv/bin/python scripts/quote_fill_diagnostic.py \
  --config conf/mainnet_shadow.yml \
  --telemetry logs/xrp_link_mainnet_shadow/<run_id>/telemetry.sqlite \
  --state logs/xrp_link_mainnet_shadow/<run_id>/state.json \
  --run-metadata reports/xrp_link_mainnet_shadow/<run_id>/run_metadata.json \
  --out-dir reports/quote_fill_diagnostic/<run_id>
```

Add `--watch-pid <runner_pid> --poll-seconds 30` to refresh the snapshot until
the runner exits. The six-hour exporter invokes the same diagnostic at terminal
run finalization.

Each per-run output also includes `asset_root_cause.csv`, preserving the
classification, evidence status, trade/crossing/fill counts, lifetime, churn,
and potential churn-missed-fill denominators for XRP and LINK. Historical
reports may contain the retired assets they measured at the time.

The diagnostic uses receipt-time causality, strict conservative trade-through,
separate touch sensitivity, and no forward-fill across an observation gap. The
queue field is explicitly a continuous same-price residency proxy, not actual
exchange queue position. A sufficient evidence status requires at least 30
Derive trades or 20 conservative fills, plus 20 usable 30-second and 20 usable
60-second markouts. Otherwise the report remains `MORE_DATA_REQUIRED` and does
not stop the collector or authorize tuning.

The current replacement profile uses a strict quote-to-causal-Derive-mid
refresh threshold of 2% (200 bps). Quote age and small desired-price changes
are not normal refresh triggers; paused or invalid plans remain protective
cancellation triggers. Historical run reports retain the policy that was active
when those runs were collected.

## Bitget boundary

The canonical `conf/mainnet_shadow.yml` and the current successor profiles set
`bitget_enabled: false`, configure only Binance, Bybit, and OKX, and preserve
the priority order Binance -> Bybit -> OKX -> PAUSE. Historical run processes
may have loaded an older venue list; changing a file cannot alter an already
running process, so a new run is required to apply the active profile.

The dashboard reads `reports/quote_fill_diagnostic/<run_id>/diagnostic_summary.json`
and exposes the quote/fill and storage panels at `http://127.0.0.1:8770/`.
