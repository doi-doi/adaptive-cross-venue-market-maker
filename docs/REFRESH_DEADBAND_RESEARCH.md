# ZEC / XRP / LINK refresh-deadband research phase

This is the next active research phase. It preserves the currently running
six-hour mid-price baseline and all historical DOGE/ADA evidence; it does not
rewrite or delete those artifacts.

## Scope

- Active universe: `ZEC`, `XRP`, `LINK` only.
- Reference path: `Binance -> Bybit -> OKX -> Pause`.
- Bitget is disabled. References are data-only; Derive is the only execution
  venue in the unarmed shadow design.
- Runtime safety: `MAINNET_SHADOW`, `dry_run=true`, `mainnet_armed=false`,
  no credentials, private API, or order endpoint.

## Action policy

Market-data cadence remains fast. Only order mutations are governed:

1. emergency cancellation;
2. risk or inventory reduction;
3. stale-quote replacement;
4. normal fair-value refresh.

Normal requests are coalesced by asset and side, with the latest desired quote
winning. Before a replacement, the candidate is tick-rounded and must remain
post-only, risk-valid, above minimum edge, and different from the current
rounded tick. A normal replacement requires a strict move greater than the
configured deadband and the minimum residency time. A fast adverse move can
cancel the vulnerable side immediately: a rapid down move protects bids and a
rapid up move protects asks. Favorable movement does not trigger a symmetric
cancel.

The current conservative research budget is 1 action/second, 30 actions/minute,
and 1 action/second/instrument, with a 50% target utilization and a 6/minute
emergency reserve. It remains classified
`DERIVE_RATE_LIMIT_NOT_FULLY_VERIFIED` until account-specific limits and the
deployed per-instrument tier are verified.

## Replay grid and definitions

One shared causal observation stream is replayed over:

- deadband: `0, 2, 5, 10, 15, 20, 30 bps`;
- minimum normal residency: `0, 0.5, 1, 2, 3, 5 seconds`;
- fill views: `CONSERVATIVE` and `TOUCH_SENSITIVITY`.

The conservative view requires the correct Derive aggressor and strict
trade-through. A touch is a separate sensitivity event. Queue residency is
only continuous same-price observed time; it is not actual exchange queue
position. Churn-missed fills are hypothetical lookbacks at 100ms, 250ms,
500ms, 1s, 2s, and 5s.

Markouts are measured at 1s, 5s, 15s, 30s, and 60s against both Derive mid and
reference fair value when a causal future observation exists. Net capture is
`quoted edge - maker fee + maker-perspective Derive markout`. No winner is
selected merely from fill count or touch events.

## Artifacts

The required report directory is
`reports/zec_xrp_link_refresh_research/`. The analyzer writes the complete
CSV set plus `final_refresh_research.md` and
`final_refresh_research.json`. It can be rerun without modifying telemetry:

```bash
.venv/bin/python scripts/derive_rate_limit_audit.py \
  --out-dir reports/zec_xrp_link_refresh_research

.venv/bin/python scripts/run_refresh_research.py \
  --config conf/mainnet_shadow_refresh_research.yml \
  --telemetry logs/zec_xrp_link_refresh_research/<run-id>/telemetry.sqlite \
  --state logs/zec_xrp_link_refresh_research/<run-id>/state.json \
  --mapping reports/zec_xrp_link_refresh_research/<run-id>/asset_reference_mapping.json \
  --out-dir reports/zec_xrp_link_refresh_research
```

The final classification remains `REFRESH_RESEARCH_DATA_INSUFFICIENT` until
each asset meets the stated trade/fill and 30s/60s markout denominators. The
report is a stop gate, not a live-parameter auto-apply or deployment approval.
