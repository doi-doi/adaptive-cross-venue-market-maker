# Agent Builders Cup compliance

Verified on 2026-09-12 against the official Hummingbot v2.16.0 release notes.
The published requirement says entrants build a Hummingbot V2 Controller or
Condor Agent and apply to an exchange sponsor team; Derive is listed as a
sponsor. The notes describe a 48-hour live final for selected teams. Full
application/judging rules live on Botcamp and must be confirmed by the entrant.

Source: https://hummingbot.org/release-notes/2.16.0/#agent-builders-cup-hackathon

| Requirement | Status | Evidence |
|---|---|---|
| Hummingbot V2 Controller or Condor Agent | PASS | Native `ControllerBase` controller plus Condor routine |
| Derive sponsor alignment | PASS | `derive_perpetual` is the sole executor target |
| Runs inside Hummingbot | PASS (contract) | Native executor actions/configs plus official-image import and quote probe |
| Official image contract probe | PASS | Import, instantiate, process books, calculate quotes, and emit zero shadow actions in CI run `34702709151` |
| Condor operation/monitoring | PASS (static) | deploy/control instructions and read-only health routine |
| Exact application submitted | NOT VERIFIED | External entrant/application state was not inspected |
| 48-hour live final readiness | NOT VERIFIED | No live trading or long validation authorized/performed |
| Current local native import | PENDING | Docker daemon was stopped; isolated official-image CI passed instead |
| 15–30 minute shadow validation | PENDING | Requires installed artifacts and healthy local services |

No PASS above is a profitability, exchange-fill, or competition-selection
claim.
