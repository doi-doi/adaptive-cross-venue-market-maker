# Multi-reference extension

Goal: extend this project in place to compare Derive-only, Binance-only and
four-venue consensus references across ADA, CC, XRP, SOL, LINK, DOGE, BNB, HYPE.

Context: all four requested perpetual connectors are installed in the local
hummingbot-api container. Existing shadow observations are not a qualified
baseline: the old runner admitted reference-exchange trades to its fill queue.

Constraints: public mainnet shadow only; Derive trades alone establish fill
evidence; separate reference and fill-model portfolios; no stale price reuse;
preserve the existing V2 adapter, inventory, lifecycle and Condor entry points.

Implementation sequence:
1. Correct trade provenance and causal order eligibility with regression tests.
2. Inspect installed connector public-data APIs and implement validated discovery
   and adapters, with per-asset source health and reconnect invalidation.
3. Add robust median consensus, outliers, disagreement gating and causal basis.
4. Integrate isolated reference/fill controls with existing quote/risk engines.
5. Extend SQLite evidence, reports and existing dashboard; verify rendering.
6. Run ten-minute eight-asset pipeline validation, then launch detached 30-minute
   capture only when every required asset passes. Finalization is automatic.

Done when: tests and pipeline pass; dashboard displays source health and model
comparisons; reports retain denominators and insufficient-evidence labels; the
30-minute run is launched without blocking Codex and generates its final report.
Statistical superiority or live readiness is never inferred from liveness.

Progress:
- Installed connector discovery executed successfully with trading_required=False:
  all eight requested symbols are mapped on all four venues.
- OKX base-amount multipliers verified from public contract metadata.
- Reference trades rejected by the fill engine and legacy queue; pre-creation
  trade timestamps rejected. The historical ADA fill claim is unvalidated.
- Source health/consensus foundation added; stale exclusion, reconnect invalidation,
  sequence policies and no-forward-fill tests are covered.
- Integration, paired controls, read-only dashboard, exact report set, and the
  10-minute pipeline validation are implemented. The required detached 30-minute
  run remains the final operational evidence gate.

Likely files: public_data, runner, reference, shadow_engine, telemetry, reporting,
dashboard, config, controller adapter, CLI, dashboard/index.html and tests.

Risks: venue snapshot/delta semantics, misleading sequence gaps on filtered
streams, source-switch basis jumps, quote/trade causality, thin Derive samples,
and separate control portfolios accidentally sharing inventory or trade budgets.
Liveness does not establish fill economics or live readiness; the final report
must preserve any insufficient-evidence classification.
