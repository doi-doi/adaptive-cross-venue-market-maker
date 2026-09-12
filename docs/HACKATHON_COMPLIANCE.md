# Agent Builders Cup compliance

Verified on 2026-09-13 against the official Hummingbot v2.16.0 release notes
and current Botcamp Agent Builders Cup page. Entrants build a Hummingbot V2
Controller or Condor Agent and apply to an exchange sponsor team; Derive is
listed as a sponsor. The current page specifies $800 USDC per selected agent,
a 48-hour final, and a September 30 code freeze. Derive's two team seats are
displayed as open at verification time. The entrant's private application or
selection state cannot be inferred from that public page.

Source: https://hummingbot.org/release-notes/2.16.0/#agent-builders-cup-hackathon

Current rules: https://www.botcamp.xyz/hackathons/agent-builders-cup-1

| Requirement | Status | Evidence |
|---|---|---|
| Hummingbot V2 Controller or Condor Agent | PASS | Native `ControllerBase` controller plus Condor routine |
| Derive sponsor alignment | PASS | `derive_perpetual` is the sole executor target |
| Runs inside Hummingbot | PASS | Native executor actions/configs plus local dual-controller runtime |
| Official image contract probe | PASS | Import, instantiate, process books, calculate quotes, and emit zero shadow actions in CI run `34702709151` |
| Condor operation/monitoring | PASS | local bot control plus running read-only health routine |
| Exact application submitted | NOT VERIFIED | External entrant/application state was not inspected |
| 48-hour live final readiness | NOT VERIFIED | No live trading or long validation authorized/performed |
| Current local native import | PASS | official image loaded controller and both native public feeds |
| 15–30 minute shadow validation | PASS | 16m53s wall-clock run; final controller uptime 961.48s; evidence in `submission/SHADOW_VALIDATION_20260912.md` |

No PASS above is a profitability, exchange-fill, or competition-selection
claim.
