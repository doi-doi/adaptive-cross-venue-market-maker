# Quote/fill diagnostics

The project now has a measurement-only quote-lifetime and Derive-trade
crossing audit. It reconstructs quote intervals from retained CREATE, CANCEL,
FILL, and Derive trade events. It does not submit orders, change quote
parameters, or write to the run's SQLite telemetry database.

For a live or completed run:

```text
./.venv/bin/python scripts/quote_fill_diagnostic.py \
  --config conf/mainnet_shadow_3asset_6h.yml \
  --telemetry logs/priority_reference_3asset_6h/<run_id>/telemetry.sqlite \
  --state logs/priority_reference_3asset_6h/<run_id>/state.json \
  --run-metadata reports/priority_reference_3asset_6h/<run_id>/run_metadata.json \
  --out-dir reports/quote_fill_diagnostic/<run_id>
```

Add `--watch-pid <runner_pid> --poll-seconds 30` to refresh the snapshot until
the runner exits. The six-hour exporter invokes the same diagnostic at terminal
run finalization.

Each per-run output also includes `asset_root_cause.csv`, preserving the
classification, evidence status, trade/crossing/fill counts, lifetime, churn,
and potential churn-missed-fill denominators for DOGE, ADA, and XRP.

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

`conf/mainnet_shadow_3asset_6h.yml` and its explicit successor copy
`conf/mainnet_shadow_3asset_6h_no_bitget.yml` set `bitget_enabled: false`,
configure only Binance, Bybit, and OKX, and preserve the priority order
Binance -> Bybit -> OKX -> PAUSE. The currently running PID was loaded with
Bitget in its venue list; changing the file cannot cancel that process's
websocket/reconnect task. The update therefore reports
`RESTART_REQUIRED_TO_REMOVE_BITGET` and leaves the current fixed run intact.

The dashboard reads `reports/quote_fill_diagnostic/<run_id>/diagnostic_summary.json`
and exposes the quote/fill and storage panels at `http://127.0.0.1:8770/`.
