"""Read-only Derive rate-limit evidence and conservative budget report."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DOCS_URL = "https://docs.derive.xyz/rate-limits"
INTROSPECTION_URL = "https://docs.derive.xyz/api-reference/system/publicgetratelimits"
ACCOUNT_URL = "https://docs.derive.xyz/api-reference/account/privateget_account"
V3_BASE = "https://api.derive.xyz/v3"


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _post_public(path: str, payload: dict[str, Any], timeout: float = 12.0) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{V3_BASE}/{path.lstrip('/')}",
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "derive-mm-v2-rate-limit-audit",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            try:
                body: Any = json.loads(raw)
            except json.JSONDecodeError:
                body = raw[:1000]
            return {
                "status_code": getattr(response, "status", None),
                "headers": {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if "rate" in key.lower() or key.lower() in {"retry-after", "content-type"}
                },
                "body": body,
            }
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        return {"status_code": None, "headers": {}, "body": None, "error": type(exc).__name__}


def build_rate_limit_audit(*, probe_live: bool = True, observed_at_utc: str | None = None) -> dict[str, Any]:
    """Build an auditable snapshot without using credentials or private calls."""

    observed_at_utc = observed_at_utc or _iso_now()
    probes: dict[str, Any] = {}
    if probe_live:
        probes["public_getRateLimits"] = _post_public("public/getRateLimits", {})
        probes["public_get_all_instruments"] = _post_public(
            "public/get_all_instruments",
            {"instrument_type": "perp", "expired": False, "page_size": 1000},
        )
    else:
        probes["probe_status"] = "SKIPPED_BY_REQUEST"

    return {
        "audit_version": "refresh-deadband-rate-limits-v1",
        "verified": False,
        "classification": "DERIVE_RATE_LIMIT_NOT_FULLY_VERIFIED",
        "observed_at_utc": observed_at_utc,
        "project": "derive-multi-asset-binance-mm-v2",
        "scope": "Derive mainnet public API and uncredentialed observations; account-specific limits are not inferred",
        "execution_safety": {
            "mode": "MAINNET_SHADOW",
            "dry_run": True,
            "mainnet_armed": False,
            "private_api_used": False,
            "real_orders": 0,
            "real_positions": 0,
        },
        "sources": [
            {
                "source": "Derive current rate-limit documentation",
                "url": DOCS_URL,
                "limit_type": "documented deployment reference",
                "confidence": "DOCUMENTED_REFERENCE_NOT_ACCOUNT_VERIFIED",
                "notes": "The documentation says limits are deployment-specific and recommends live introspection.",
            },
            {
                "source": "Derive public/getRateLimits",
                "url": INTROSPECTION_URL,
                "limit_type": "live public request-budget introspection",
                "confidence": "LIVE_UNCREDENTIALLED_PUBLIC_SCOPE",
                "notes": "This exposes current request-budget fields but does not expose the account's per-instrument order tier.",
            },
            {
                "source": "Derive private/get_account",
                "url": ACCOUNT_URL,
                "limit_type": "account-specific websocket and endpoint fields",
                "confidence": "UNAVAILABLE_WITHOUT_AUTHENTICATED_TIMESTAMP",
                "notes": "No credentials or private request were used in this research phase.",
            },
            {
                "source": "Installed Hummingbot connector image",
                "url": "local Docker image hummingbot/hummingbot-api:latest",
                "limit_type": "connector configuration evidence",
                "confidence": "IMPLEMENTATION_EVIDENCE_NOT_EXCHANGE_AUTHORITY",
                "notes": "The inspected connector targets the legacy api.lyra.finance endpoint and exposes order/cancel request weights; it has no private/replace implementation.",
            },
        ],
        "documented_limits": [
            {
                "scope": "per-wallet matching request budget",
                "limit_type": "fixed-window request budget",
                "sustained": "1 TPS",
                "burst": "5 points",
                "window": "5 seconds",
                "confidence": "DOCUMENTED_REFERENCE",
            },
            {
                "scope": "per-wallet non-matching request budget",
                "limit_type": "fixed-window request budget",
                "sustained": "5 TPS",
                "burst": "25 points",
                "window": "5 seconds",
                "confidence": "DOCUMENTED_REFERENCE",
            },
            {
                "scope": "public requests per IP",
                "limit_type": "fixed-window request budget",
                "sustained": "5 TPS",
                "burst": "25 points",
                "window": "5 seconds",
                "confidence": "DOCUMENTED_REFERENCE",
            },
            {
                "scope": "per-instrument perpetual/spot order limiter",
                "limit_type": "token bucket",
                "sustained": "1 token/second",
                "burst": "capacity 5",
                "window": "continuous refill",
                "confidence": "DOCUMENTED_REFERENCE",
            },
            {
                "scope": "matching methods",
                "limit_type": "shared matching budget",
                "sustained": "order, replace, cancel, cancel_by_instrument, cancel_by_nonce share the matching budget",
                "burst": "deployment-specific",
                "window": "see live account/deployment introspection",
                "confidence": "DOCUMENTED_REFERENCE",
            },
        ],
        "live_probes": probes,
        "internal_budget": {
            "max_order_actions_per_second": "1",
            "max_order_actions_per_minute": 30,
            "max_order_actions_per_instrument_per_second": "1",
            "target_action_utilization": "0.50",
            "emergency_cancel_budget_per_minute": 6,
            "headroom_policy": "reject or flag normal refreshes before the conservative budget is exceeded; emergency cancellations bypass normal throttling",
            "rationale": "Account-specific limits and the deployed per-instrument tier are not verified. The budget is deliberately below the documented 1 TPS matching reference and averages 0.5 TPS over a minute.",
        },
        "interpretation": "The public live response is evidence of the uncredentialed public connection only. It is not proof of this wallet's matching allowance and must not be used to authorize live orders.",
    }


def _markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Derive rate-limit audit",
        "",
        f"- Observed: `{audit['observed_at_utc']}`",
        f"- Classification: `{audit['classification']}`",
        "- Scope: public/uncredentialed evidence only; no private API or order call was used.",
        "",
        "## Evidence",
        "",
        "| Source | Limit type | Confidence | Notes |",
        "|---|---|---|---|",
    ]
    for source in audit["sources"]:
        lines.append(
            f"| {source['source']} | {source['limit_type']} | {source['confidence']} | {source['notes']} |"
        )
    lines.extend(["", "## Documented reference limits", "", "| Scope | Type | Sustained | Burst | Window | Confidence |", "|---|---|---|---|---|---|"])
    for row in audit["documented_limits"]:
        lines.append(
            f"| {row['scope']} | {row['limit_type']} | {row['sustained']} | {row['burst']} | {row['window']} | {row['confidence']} |"
        )
    budget = audit["internal_budget"]
    lines.extend(
        [
            "",
            "## Conservative internal research budget",
            "",
            f"- `max_order_actions_per_second`: `{budget['max_order_actions_per_second']}`",
            f"- `max_order_actions_per_minute`: `{budget['max_order_actions_per_minute']}`",
            f"- `max_order_actions_per_instrument_per_second`: `{budget['max_order_actions_per_instrument_per_second']}`",
            f"- Target utilization: `{budget['target_action_utilization']}`",
            f"- Emergency cancellation reserve: `{budget['emergency_cancel_budget_per_minute']}/min` (reserve, not a hard safety cap)",
            "",
            budget["rationale"],
            "",
            "## Live probe interpretation",
            "",
            audit["interpretation"],
            "",
            "The next phase remains `MAINNET_SHADOW`, `dry_run=true`, `mainnet_armed=false`. The audit is a measurement artifact, not a deployment approval.",
            "",
        ]
    )
    return "\n".join(lines)


def write_rate_limit_audit(out_dir: str | Path, *, probe_live: bool = True) -> dict[str, Any]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    audit = build_rate_limit_audit(probe_live=probe_live)
    (out_dir / "derive_rate_limit_audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (out_dir / "derive_rate_limit_audit.md").write_text(_markdown(audit), encoding="utf-8")
    return audit
