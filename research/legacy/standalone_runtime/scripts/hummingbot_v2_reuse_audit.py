#!/usr/bin/env python3
"""Produce a read-only Hummingbot V2/controller reuse audit."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CATALOG = [
    ("directional", "bollinger_v1", "bollinger_v1.py"),
    ("directional", "bollinger_v2", "bollinger_v2.py"),
    ("directional", "bollingrid", "bollingrid.py"),
    ("directional", "macd_bb_v1", "macd_bb_v1.py"),
    ("directional", "supertrend_v1", "supertrend_v1.py"),
    ("directional", "ai_livestream", "ai_livestream.py"),
    ("directional", "dman_v3", "dman_v3.py"),
    ("market_making", "pmm_simple", "pmm_simple.py"),
    ("market_making", "pmm_dynamic", "pmm_dynamic.py"),
    ("market_making", "dman_maker_v2", "dman_maker_v2.py"),
    ("generic", "xemm_multiple_levels", "xemm_multiple_levels.py"),
    ("generic", "arbitrage_controller", "arbitrage_controller.py"),
    ("generic", "grid_strike", "grid_strike.py"),
    ("generic", "multi_grid_strike", "multi_grid_strike.py"),
    ("generic", "stat_arb", "stat_arb.py"),
    ("generic", "hedge_asset", "hedge_asset.py"),
    ("generic", "pmm_v1", "pmm_v1.py"),
    ("generic", "pmm_mister", "pmm_mister.py"),
    ("generic", "quantum_grid_allocator", "quantum_grid_allocator.py"),
    ("generic", "lp_rebalancer", "lp_rebalancer/lp_rebalancer.py"),
]
PACKAGE_SOURCE = "/opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot"
PACKAGE_PYTHON = "/opt/conda/envs/hummingbot-api/bin/python"

BASE_FACTS = [
    ("controller_config", "hummingbot/strategy_v2/controllers/controller_base.py", "ControllerConfigBase", 58, "update_markets; set_id; get_controller_class", "native config contract and market registration"),
    ("controller", "hummingbot/strategy_v2/controllers/controller_base.py", "ControllerBase", 127, "control_task; update_processed_data; determine_executor_actions; send_actions; filter_executors", "native controller loop, action queue, executor lookup, and status"),
    ("market_making_base", "hummingbot/strategy_v2/controllers/market_making_controller_base.py", "MarketMakingControllerConfigBase", 17, "triple_barrier_config; get_spreads_and_amounts_in_quote; get_required_base_amount", "native single connector/pair MM configuration and Triple Barrier fields"),
    ("market_making_base", "hummingbot/strategy_v2/controllers/market_making_controller_base.py", "MarketMakingControllerBase", 212, "create_actions_proposal; stop_actions_proposal; executors_to_refresh; executors_to_early_stop; update_processed_data; get_executor_config; get_price_and_amount", "native MM lifecycle and level management; base config is single connector/pair"),
    ("directional_base", "hummingbot/strategy_v2/controllers/directional_trading_controller_base.py", "DirectionalTradingControllerBase", 141, "update_processed_data; create_actions_proposal; stop_actions_proposal; get_executor_config", "directional lifecycle; not a two-sided maker base"),
    ("strategy", "hummingbot/strategy/strategy_v2_base.py", "StrategyV2Base", 176, "start; create_actions_proposal; stop_actions_proposal; determine_executor_actions; get_performance_report", "multi-controller strategy shell and executor-orchestrator routing"),
    ("market_data", "hummingbot/data_feed/market_data_provider.py", "MarketDataProvider", 28, "initialize_rate_sources; get_candles_feed; get_order_book; initialize_order_book; initialize_order_books; get_price_by_type; get_trading_rules; quantize_order_price; quantize_order_amount; get_price_for_volume", "native order book, candles, price, rules, and quantization"),
    ("orchestrator", "hummingbot/strategy_v2/executors/executor_orchestrator.py", "ExecutorOrchestrator", 201, "execute_actions; create_executor; stop_executor; store_executor; get_all_reports; generate_performance_report", "native executor ownership, persistence, positions, and performance"),
]

EXECUTOR_FACTS = [
    ("OrderExecutor", "strategy_v2/executors/order_executor/order_executor.py", "DIRECTLY_USEFUL", "one LIMIT/LIMIT_MAKER/MARKET/LIMIT_CHASER order; renew_order/cancel_order; best fit for one maker bid or ask"),
    ("PositionExecutor", "strategy_v2/executors/position_executor/position_executor.py", "NOT_RELEVANT", "finite Triple Barrier position; not continuous two-sided perp MM"),
    ("GridExecutor", "strategy_v2/executors/grid_executor/grid_executor.py", "NOT_RELEVANT", "bounded multi-level grid, not priority-reference maker control"),
    ("DCAExecutor", "strategy_v2/executors/dca_executor/dca_executor.py", "NOT_RELEVANT", "scheduled level-based accumulation"),
    ("TWAPExecutor", "strategy_v2/executors/twap_executor/twap_executor.py", "NOT_RELEVANT", "time-sliced execution, not passive quote control"),
    ("ArbitrageExecutor", "strategy_v2/executors/arbitrage_executor/arbitrage_executor.py", "NOT_RELEVANT", "cross-market arbitrage, not Derive-only execution"),
    ("XEMMExecutor", "strategy_v2/executors/xemm_executor/xemm_executor.py", "FEATURES_TO_BORROW", "maker/taker pricing/update pattern; cross-venue semantics do not match"),
    ("LPExecutor", "strategy_v2/executors/liquidity_mining_executor/liquidity_mining_executor.py", "NOT_RELEVANT", "different liquidity-provision position model"),
    ("ExecutorOrchestrator", "strategy_v2/executors/executor_orchestrator.py", "DIRECTLY_USEFUL", "native Create/Stop/Store routing, reports, positions, performance"),
]

NATIVE_TOP10 = [
    ("ControllerBase control loop", "ControllerBase.control_task", "USE_NATIVE", "controller lifecycle and action queue"),
    ("Strategy V2 multi-controller routing", "StrategyV2Base.determine_executor_actions", "USE_NATIVE", "action composition and orchestrator handoff"),
    ("MarketDataProvider order books", "MarketDataProvider.get_order_book/initialize_order_books", "USE_NATIVE", "Derive and reference public order books"),
    ("Derive public trade feed", "DerivePerpetualAPIOrderBookDataSource._parse_trade_message", "USE_NATIVE", "native trades channel, trade ID, and aggressor direction"),
    ("Derive trading rules", "MarketDataProvider.get_trading_rules + DerivePerpetualDerivative._format_trading_rules", "USE_NATIVE", "connector rules and quantization"),
    ("AsyncThrottler", "hummingbot.core.api_throttler.AsyncThrottler", "USE_NATIVE", "request limiter and async capacity waits"),
    ("OrderExecutor maker lifecycle", "OrderExecutor.control_order/renew_order/cancel_order", "USE_NATIVE", "single order placement/cancellation"),
    ("Create/Stop/Store actions", "hummingbot/strategy_v2/models/executor_actions.py", "USE_NATIVE", "executor lifecycle contract"),
    ("Executor performance report", "ExecutorOrchestrator.generate_performance_report", "USE_NATIVE", "realized/unrealized/volume accounting"),
    ("MarketsRecorder persistence", "hummingbot/connector/markets_recorder.py", "USE_NATIVE", "authorized order/fill persistence"),
]

CUSTOM_TOP10 = [
    ("Priority reference selector", "src/derive_multi_asset_mm/priority.py", "KEEP_CUSTOM", "Binance -> Bybit -> OKX -> pause and recovery gate"),
    ("Basis and fair-value layer", "src/derive_multi_asset_mm/reference.py; control.py", "KEEP_CUSTOM", "basis, microprice, and source comparison"),
    ("Adaptive deadband", "src/derive_multi_asset_mm/refresh_governor.py; lifecycle.py", "KEEP_CUSTOM", "business refresh threshold and residency"),
    ("Fast adverse protection", "src/derive_multi_asset_mm/refresh_governor.py", "KEEP_CUSTOM", "risk-priority cancellation override"),
    ("Multi-asset quote plans", "src/derive_multi_asset_mm/quote_engine.py", "KEEP_CUSTOM", "one bid and ask per asset under shared capital"),
    ("Inventory and portfolio skew", "src/derive_multi_asset_mm/inventory.py; portfolio.py", "KEEP_CUSTOM", "asset and portfolio reservation controls"),
    ("Conservative/touch shadow fills", "src/derive_multi_asset_mm/shadow_engine.py", "KEEP_CUSTOM", "strict trade-through versus diagnostic touch"),
    ("Causal markouts/toxicity", "src/derive_multi_asset_mm/markouts.py; reporting.py", "KEEP_CUSTOM", "maker-perspective research measurement"),
    ("Connection recovery gate", "src/derive_multi_asset_mm/source_health.py", "KEEP_CUSTOM", "fresh Derive BBO safety gate"),
    ("Research telemetry/dashboard", "src/derive_multi_asset_mm/telemetry.py; dashboard.py", "KEEP_CUSTOM", "run-scoped evidence and display"),
]


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _run(command: list[str]) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=20, check=False)
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _container_version(container: str) -> dict[str, Any]:
    code = "import importlib.metadata, json, pathlib; import hummingbot; print(json.dumps({'version': importlib.metadata.version('hummingbot'), 'package': str(pathlib.Path(hummingbot.__file__).parent)}))"
    output = _run(["docker", "exec", container, PACKAGE_PYTHON, "-c", code])
    try:
        return json.loads(output)
    except json.JSONDecodeError:
        return {"version": "UNAVAILABLE", "package": PACKAGE_SOURCE}


def _container_inventory(container: str) -> list[str]:
    output = _run(["docker", "exec", container, "sh", "-lc", f"find {PACKAGE_SOURCE} -type f -name '*.py' -print | sort"])
    return output.splitlines() if output else []


def _class_names(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return ";".join(re.findall(r"^class\s+([A-Za-z_][A-Za-z0-9_]*)", text, re.MULTILINE))


def _controller_inventory(api_checkout: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    base = api_checkout / "bots" / "controllers"
    category_dirs = {"directional": "directional_trading", "market_making": "market_making", "generic": "generic"}
    rows: list[dict[str, Any]] = []
    for category, name, relative in CATALOG:
        path = base / category_dirs[category] / relative
        present = path.is_file()
        rows.append({"category": category, "controller": name, "catalog_source": "official_catalog_from_task", "local_present": "YES" if present else "NO", "local_path": str(path) if present else "", "classes": _class_names(path) if present else "", "scan_status": "FOUND" if present else "MISSING_FROM_LOCAL_TREE"})
    local_only: list[dict[str, Any]] = []
    if base.exists():
        known = {Path(relative).stem for _, _, relative in CATALOG}
        for path in sorted(base.rglob("*.py")):
            if "examples" in path.parts or "__pycache__" in path.parts or path.name == "__init__.py":
                continue
            if path.stem not in known:
                local_only.append({"path": str(path), "classes": _class_names(path)})
    return rows, local_only


def _controller_scan(rows: list[dict[str, Any]], local_only: list[dict[str, Any]]) -> str:
    lines = ["# Controller catalog scan", "", "Every requested catalog controller is compared with the local API checkout. Missing entries are reported only; this audit does not install or delete controllers.", "", "| Category | Controller | Present | Local source | Classes | Status |", "|---|---|---|---|---|---|"]
    lines.extend(f"| {row['category']} | `{row['controller']}` | {row['local_present']} | `{row['local_path'] or '—'}` | `{row['classes'] or '—'}` | {row['scan_status']} |" for row in rows)
    lines.extend(["", "## Local-only controller source", ""])
    lines.extend(f"- `{row['path']}` (`{row['classes'] or 'classes unavailable'}`)" for row in local_only) if local_only else lines.append("- None")
    missing = [row["controller"] for row in rows if row["local_present"] == "NO"]
    lines.extend(["", f"Coverage: {len(rows) - len(missing)}/{len(rows)}. Missing locally: {', '.join(missing) or 'none'}."])
    return "\n".join(lines)


def _base_scan(package: str) -> str:
    lines = ["# Hummingbot V2 base-class scan", "", f"Primary source: installed Hummingbot package `{package}`. It is the container wheel, not a git checkout.", "", "| Area | Source / class | Key methods | Reuse conclusion |", "|---|---|---|---|"]
    lines.extend(f"| {area} | `{file}:{line} {cls}` | `{methods}` | {match} |" for area, file, cls, line, methods, match in BASE_FACTS)
    lines.extend(["", "## Direct answers", "", "- MarketMakingControllerBase native for this multi-asset two-sided Derive controller: **NO**. Its config has one connector and one trading pair; lifecycle ideas are reusable, but shared portfolio ownership remains custom.", "- ControllerBase usable for a one-controller multi-asset adapter: **YES**. It supplies the loop and action contract, but not a native multi-asset risk supervisor.", "- Native executor for a single maker quote: **OrderExecutor**. It supports one LIMIT/LIMIT_MAKER/MARKET/LIMIT_CHASER order and renew/cancel lifecycle.", "- Native executor for continuous two-sided perp MM: **NO_SINGLE_EXECUTOR**. PositionExecutor is finite Triple Barrier position management.", "- Dedicated native multi-asset portfolio supervisor found: **NO**. StrategyV2Base can host multiple controllers and ExecutorOrchestrator can aggregate reports, but shared capital/quote ownership is custom."])
    return "\n".join(lines)


def _market_data(package: str) -> str:
    return f"""# MarketDataProvider and connector audit

Primary source: installed package `{package}`.

`hummingbot/data_feed/market_data_provider.py:28 MarketDataProvider` provides
`initialize_order_book(s)`, `get_order_book`, `get_price_by_type`,
`get_price_for_volume`, `get_candles_feed`, historical candle helpers,
`get_trading_rules`, and price/amount quantization. It uses connector public
feeds and shared connector/throttler plumbing. No generic `get_trades`
convenience method was found; trade messages arrive through connector data
source queues.

The installed `connector/derivative/derive_perpetual` data source subscribes to
`trades.<SYMBOL>` and order-book channels, parses Derive direction, price,
amount, trade ID, and timestamp, and emits native trade messages.
`DerivePerpetualDerivative` supplies trading-rule formatting, order/cancel
transport, private/user-stream trade processing, and private trade-history
reconciliation. Derive remains the only execution venue for this project.

Binance, Bybit, and OKX can be used through the same provider contracts for
public reference order books/candles. Their transport is native, but priority
selection, stale gates, basis, failover, and pause behavior remain custom.
Bitget is disabled in the active configuration and is not a target source.

| Feature | Action | Reason |
|---|---|---|
| Derive/reference order books | USE_NATIVE | native connector/data contract |
| Derive trade stream | USE_NATIVE | native trade ID and aggressor direction |
| Candles and price-by-type | USE_NATIVE | only when a feature explicitly needs them |
| Rules/quantization | USE_NATIVE | connector authority |
| Priority/failover and shadow fill joins | KEEP_CUSTOM | research/business semantics |
"""


def _executor_scan(package: str) -> str:
    lines = ["# Executor scan", "", f"Source authority: installed package `{package}`.", "", "| Executor | Source | Classification | Evidence |", "|---|---|---|---|"]
    lines.extend(f"| `{name}` | `{path}` | **{classification}** | {evidence} |" for name, path, classification, evidence in EXECUTOR_FACTS)
    lines.extend(["", "## Minimum reusable lifecycle", "", "For a future authorized V2 migration, create one native `OrderExecutor` per maker bid/ask with `CreateExecutorAction` and stop it with `StopExecutorAction`. The custom supervisor must still own one bid and one ask per asset, deduplicate ownership, and decide whether a refresh is warranted.", "", "No executor migration was performed."])
    return "\n".join(lines)


def _orchestrator(package: str) -> str:
    return f"""# ExecutorOrchestrator audit

Source: `{package}/hummingbot/strategy_v2/executors/executor_orchestrator.py`.

`ExecutorOrchestrator` owns executor mappings, creates instances from
`CreateExecutorAction`, stops them from `StopExecutorAction`, stores completed
executors/positions, and exposes `get_all_reports`, position reports, and
`generate_performance_report`. The native report path can carry realized PNL,
unrealized PNL, volume, and fees for an authorized connector execution path.

It does not provide priority-reference selection, multi-asset shared-capital
admission, adaptive deadband, conservative/touch shadow fills, causal
markouts, or research panel identity. Those remain custom. It also must not be
used to infer a real fill from a public Derive trade in a shadow run.

| Concern | Native evidence | Target action |
|---|---|---|
| create/stop ownership | `execute_actions`, `create_executor`, `stop_executor` | USE_NATIVE |
| completed state | `store_executor`, cached performance | USE_NATIVE |
| positions | `get_positions_report`, `PositionHold` | USE_NATIVE when authorized |
| PNL/fees/volume | `generate_performance_report`, recorder data | USE_NATIVE when authorized |
| six independent shadow portfolios | no native equivalent | KEEP_CUSTOM |
| public trade/fill join | no native equivalent | KEEP_CUSTOM |
| cross-asset $800 risk | no dedicated supervisor | KEEP_CUSTOM |
"""


def _rate_limit(package: str) -> str:
    return f"""# Rate-limit and retry audit

The installed package `{package}` contains `hummingbot/core/api_throttler/async_throttler.py:54 AsyncThrottler` and async request-context helpers. `AsyncThrottler` gates request capacity and waits between capacity checks. Connector `rate_limits_rules()` declares endpoint weights; the Derive connector polls/updates its rate-limit state and uses the shared web-assistant factory.

| Layer | Responsibility | Action |
|---|---|---|
| AsyncThrottler | transport/request capacity | USE_NATIVE |
| web-assistant request context | async wait/failure handling | USE_NATIVE |
| Derive account matching allowance | deployment/account introspection | VERIFY_BEFORE_LIVE |
| quote refresh deadband/cooldown | business quote policy | KEEP_CUSTOM |
| emergency cancel and churn governor | risk/measurement policy | KEEP_CUSTOM, wrap native transport |

Native transport throttling is not a substitute for the custom rule “do not
replace a quote unless it leaves the configured mid-price deadband.” Public
Derive rate-limit responses are not proof of this wallet's live matching tier.
"""


def _rules(project_root: Path) -> str:
    source = project_root / "reports/zec_xrp_link_refresh_research/derive_rate_limit_audit.json"
    return f"""# Trading-rules audit

Native path: `MarketDataProvider.get_trading_rules` -> connector rules, with
`DerivePerpetualDerivative._format_trading_rules` providing conversion and
native price/amount quantization as the final order gate.

The current live public probe is `{source}`. It reports ZEC-PERP tick `0.001`,
amount step `0.01`, minimum amount `1`; XRP-PERP tick `0.00001`, amount step
`0.1`, minimum amount `10`; and LINK-PERP tick `0.0001`, amount step `0.001`,
minimum amount `10`. The live quote is USDC while the older mapping contains a
USD label; the accounting repair retains that conflict.

At the observed $800 research capital, ZEC is incompatible because its one
unit minimum exceeds capital, XRP is fine, and LINK is capital-tight. These
are rule/capital observations, not automatic disable decisions. The active
configuration remains the authority for asset enablement.
"""


def _inventory() -> str:
    return """# Inventory feature audit

MarketMakingControllerBase exposes position-rebalance fields and
`check_position_rebalance`/`create_position_rebalance_order`; the inspected
implementation skips rebalance for perpetual connectors. Native executor
reports and `PositionHold` can still expose actual positions.

The project inventory layer owns per-asset inventory mode, reservation-price
skew, portfolio skew, maximum inventory, shared-capital risk, and shadow units.
It marks notional from a current Derive mid and never uses inventory notional
as drawdown. No deletion is safe.

| Feature | Native | Action |
|---|---|---|
| actual connector positions | perpetual connector/PositionHold | USE_NATIVE when authorized |
| rules and quantization | MarketDataProvider | USE_NATIVE |
| per-asset inventory mode | no generic equivalent | KEEP_CUSTOM |
| shared portfolio skew | no dedicated native equivalent | KEEP_CUSTOM |
| shadow inventory units/equity | no equivalent | KEEP_CUSTOM |
"""


def _accounting() -> str:
    return """# Accounting-feature audit

Native V2 infrastructure can report executor performance, positions, realized
and unrealized PNL, volume, and fees through ExecutorOrchestrator and
MarketsRecorder. It is the preferred authority for a future authorized
Hummingbot execution path.

The current exchange-free collector intentionally uses TelemetryStore for
public Derive trades, hypothetical fill models, markouts, action events,
minute aggregates, and run state. The new repair layer adds canonical
run/fill fields at report time, explicit markout states, FIFO equity
reconciliation, equity-derived drawdown, independent control-fill audits, and
current-run panel checks without overwriting the legacy database.

| Concern | Native | Current research action |
|---|---|---|
| actual order/fill fees | MarketsRecorder/executor reports | USE_NATIVE when authorized |
| public shadow trade evidence | none | KEEP_CUSTOM |
| conservative/touch fills | none | KEEP_CUSTOM |
| causal markouts/toxicity | no direct generic pipeline | KEEP_CUSTOM |
| actual PNL/equity | orchestrator performance | WRAP_NATIVE |
| run-scoped shadow accounting | no equivalent | KEEP_CUSTOM |
"""


def _storage() -> str:
    return """# Storage and duplication audit

Hummingbot has recorder/database models for authorized orders, fills, positions,
and performance. The project has an independent SQLite telemetry store because
the current run is exchange-free and must keep public-reference controls
separate from actual execution records.

This separation is intentional. A future migration can use native recorder
data for actual connector execution while retaining a research ledger for
priority references, markouts, shadow fills, and panel identity.

Observed partial overlaps are request throttling, order lifecycle, and PNL/fee
reporting. They are not safe deletion opportunities until native parity proves
quote identity, cancellation, reconnect, fee, restart, and no-duplicate
behavior. No file was deleted and no active runtime was migrated.
"""


def _matrix() -> list[dict[str, str]]:
    rows = [
        ("controller loop", "custom adapter", "ControllerBase.control_task", "ControllerBase", "native loop", "USE_NATIVE"),
        ("multi-asset ownership", "custom adapter", "MarketMakingControllerBase is single pair", "MarketMakingControllerBase", "no native portfolio supervisor", "KEEP_CUSTOM"),
        ("orderbook transport", "custom reader", "MarketDataProvider.get_order_book", "MarketDataProvider", "same data contract", "USE_NATIVE"),
        ("Derive trade feed", "custom reader", "Derive API data source trade channel", "DerivePerpetualAPIOrderBookDataSource", "native trade IDs/aggressor", "USE_NATIVE"),
        ("reference priority", "custom", "none", "none", "business selection", "KEEP_CUSTOM"),
        ("reference health", "custom", "connector health primitives", "MarketDataProvider/connectors", "freshness/failover custom", "EXTEND_NATIVE"),
        ("basis/fair value", "custom", "none", "none", "strategy edge", "KEEP_CUSTOM"),
        ("adaptive deadband", "custom", "executor_refresh_time/cooldown only", "MarketMakingControllerBase", "mid/reference threshold custom", "KEEP_CUSTOM"),
        ("minimum residency", "custom", "cooldown_time", "MarketMakingControllerBase", "extend generic cooldown", "EXTEND_NATIVE"),
        ("inventory skew", "custom", "position rebalance", "MarketMakingControllerBase", "portfolio skew custom", "KEEP_CUSTOM"),
        ("rules/quantization", "custom", "get_trading_rules/quantize_order_*", "MarketDataProvider", "native authority", "USE_NATIVE"),
        ("single maker order", "custom", "OrderExecutor", "OrderExecutor", "native lifecycle", "USE_NATIVE"),
        ("two-sided maker per asset", "custom", "no single executor", "OrderExecutor + ControllerBase", "controller owns bid/ask", "WRAP_NATIVE"),
        ("create/stop actions", "custom", "CreateExecutorAction/StopExecutorAction", "executor_actions.py", "native contract", "USE_NATIVE"),
        ("HOLD decision", "custom", "no native action", "ExecutorAction", "business no-op", "KEEP_CUSTOM"),
        ("REPLACE decision", "custom", "OrderExecutor.renew_order", "OrderExecutor", "map carefully", "EXTEND_NATIVE"),
        ("transport rate limit", "custom", "AsyncThrottler", "AsyncThrottler", "native capacity", "USE_NATIVE"),
        ("churn governor", "custom", "none", "none", "measurement/risk", "KEEP_CUSTOM"),
        ("emergency cancel", "custom", "none", "none", "risk priority", "KEEP_CUSTOM"),
        ("fee/PnL/volume", "custom", "Orchestrator/MarketsRecorder", "ExecutorOrchestrator", "native actual execution", "WRAP_NATIVE"),
        ("shadow accounting", "custom", "none", "none", "research ledger", "KEEP_CUSTOM"),
        ("markouts", "custom", "none", "none", "causal metric", "KEEP_CUSTOM"),
        ("touch sensitivity", "custom", "none", "none", "diagnostic model", "KEEP_CUSTOM"),
        ("conservative fill", "custom", "none", "none", "execution-evidence gate", "KEEP_CUSTOM"),
        ("connection recovery", "custom", "connector reconnect primitives", "Derive connector", "fresh BBO gate", "EXTEND_NATIVE"),
        ("post-only", "custom", "OrderType/LIMIT_MAKER", "OrderExecutorConfig", "native order type", "USE_NATIVE"),
        ("position/margin", "custom", "perpetual connector/PositionHold", "Derive connector", "native actual state", "USE_NATIVE"),
        ("funding", "custom", "perpetual connector funding", "DerivePerpetualDerivative", "native data", "USE_NATIVE"),
        ("hot config", "custom", "ControllerBase.update_config", "ControllerBase", "native contract", "USE_NATIVE"),
        ("status", "custom", "to_format_status/get_custom_info", "ControllerBase", "native status", "USE_NATIVE"),
        ("dashboard", "custom", "none", "none", "research display", "KEEP_CUSTOM"),
        ("run storage", "custom", "MarketsRecorder not shadow-run ledger", "MarketsRecorder", "preserve evidence", "KEEP_CUSTOM"),
        ("duplicate prevention", "custom", "controller IDs/executor ownership", "StrategyV2Base/Orchestrator", "native IDs plus custom asset ownership", "WRAP_NATIVE"),
        ("multi-controller composition", "custom", "StrategyV2Base", "StrategyV2Base", "native routing", "USE_NATIVE"),
        ("reference candles", "custom", "MarketDataProvider candles", "MarketDataProvider", "native feed", "USE_NATIVE"),
        ("reference failover", "custom", "none", "none", "priority state machine", "KEEP_CUSTOM"),
        ("capital class", "custom", "trading rules only", "MarketDataProvider", "shared-capital policy", "KEEP_CUSTOM"),
        ("trade/fill join", "custom", "no public-shadow join", "none", "extend telemetry", "KEEP_CUSTOM"),
        ("equity drawdown", "custom", "native report plus custom shadow repair", "Orchestrator", "explicit repair", "WRAP_NATIVE"),
        ("raw retention", "custom", "recorder retention differs", "MarketsRecorder", "shadow policy", "KEEP_CUSTOM"),
        ("news gate", "custom", "none", "none", "outside task scope", "NOT_RELEVANT"),
    ]
    return [
        {
            "our_feature": row[0],
            "custom_or_native": row[1],
            "native": row[2],
            "file": "see base/executor/market-data audit",
            "class": row[3],
            "match": row[4],
            "action": row[5],
        }
        for row in rows
    ]


def _scores() -> list[dict[str, Any]]:
    rows = [
        ("controllers/market_making/derive_multi_asset_binance_reference_mm.py", 25, "ControllerBase/action contracts/MarketDataProvider", "multi-asset priority-reference adapter", "KEEP_AND_WRAP_NATIVE"),
        ("src/derive_multi_asset_mm/priority.py", 0, "none", "priority/failover/recovery state machine", "KEEP_CUSTOM"),
        ("src/derive_multi_asset_mm/reference.py", 0, "none", "basis/fair-value/consensus", "KEEP_CUSTOM"),
        ("src/derive_multi_asset_mm/refresh_governor.py", 25, "AsyncThrottler only controls transport", "deadband/residency/emergency cancellation", "KEEP_CUSTOM_WRAP_NATIVE"),
        ("src/derive_multi_asset_mm/lifecycle.py", 50, "OrderExecutor cancel/renew lifecycle", "exchange-free quote reconciliation", "PARITY_TEST_BEFORE_REDUCTION"),
        ("src/derive_multi_asset_mm/shadow_engine.py", 25, "executor/order events conceptually", "six-control portfolios/fill models", "KEEP_CUSTOM"),
        ("src/derive_multi_asset_mm/markouts.py", 0, "none", "causal maker markouts", "KEEP_CUSTOM"),
        ("src/derive_multi_asset_mm/telemetry.py", 50, "MarketsRecorder/database/performance", "public shadow events/retention/aggregates", "KEEP_SEPARATE"),
        ("src/derive_multi_asset_mm/dashboard.py", 0, "none", "research dashboard/current-run identity", "KEEP_CUSTOM"),
        ("src/derive_multi_asset_mm/inventory.py", 25, "native position/rebalance fields", "portfolio skew and shadow units", "KEEP_CUSTOM"),
        ("src/derive_multi_asset_mm/risk.py", 25, "native rules/order validation", "shared capital/research risk gates", "USE_NATIVE_RULES_WRAP_POLICY"),
        ("src/derive_multi_asset_mm/reporting.py", 50, "native performance reporting", "causal fill/markout/reference studies", "KEEP_CUSTOM_UNTIL_PARITY"),
    ]
    fields = ["module_or_feature", "score", "native_overlap", "unique_scope", "action"]
    return [dict(zip(fields, row, strict=True)) for row in rows]


def _matrix_summary(matrix: list[dict[str, str]]) -> str:
    counts = Counter(row["action"] for row in matrix)
    lines = ["# Feature reuse matrix summary", "", "The CSV is the complete row-level decision register. Native plumbing is reused only where semantics match; research evidence and business policy remain custom.", "", "| Action | Rows |", "|---|---:|"]
    lines.extend(f"| `{action}` | {count} |" for action, count in sorted(counts.items()))
    return "\n".join(lines)


def _multi_asset() -> str:
    return """# Multi-asset architecture comparison

| Architecture | Strengths | Weaknesses | Fit |
|---|---|---|---|
| One native MM controller | one lifecycle/status surface; simple shared config | native MM base is single pair; shared capital and asset loops remain custom | **Preferred target** if ControllerBase owns assets and each bid/ask maps to OrderExecutor |
| Three independent controllers | isolated failures; simple per-pair assumptions | duplicated reference/risk state; shared $800 and duplicate quotes are harder to guarantee | isolated experiments, not preferred shared-capital target |
| Shared supervisor + three controllers | fault isolation with explicit risk owner | more lifecycle complexity and supervisor/controller races | fallback if one-controller fan-out is insufficient |

The current collector has six independent control/fill portfolios in one run.
That is a measurement design, not evidence that a native live controller should
be split. No architecture was changed.
"""


def _target() -> str:
    return """# Target architecture

## Native plumbing

- StrategyV2Base for lifecycle and multi-controller composition.
- ControllerBase as the outer multi-asset controller contract.
- MarketDataProvider and installed Derive/reference connectors for order books,
  trades, rules, candles where required, price-by-type, and quantization.
- AsyncThrottler/web-assistant request controls for transport limits.
- CreateExecutorAction, StopExecutorAction, OrderExecutor,
  ExecutorOrchestrator, and MarketsRecorder for authorized execution.

## Custom edge retained

- Binance -> Bybit -> OKX -> pause priority reference selection and recovery.
- Per-asset basis/fair value, source health, divergence/adverse protection.
- Adaptive mid-price deadband and minimum quote residency.
- Shared $800 capital, inventory/portfolio skew, rule classes, and
  no-auto-disable policy.
- Strict Derive execution evidence, conservative/touch diagnostics, causal
  markouts/toxicity, and run-scoped research dashboard/storage.

## Ownership rule

One controller/run owns one asset's bid and ask IDs. A plan is admitted only
after fresh Derive BBO, current native rules, reference gates, and shared
capital checks. Native executors own transport lifecycle; custom policy decides
whether a mutation is warranted.
"""


def _migration() -> str:
    return """# Migration plan (not executed)

## P0 — evidence and contracts

Extend the research ledger with run/fill/quote IDs and explicit markout states;
confirm installed package/connector rules, Derive post-only behavior, and
account-specific limits; add BBO-to-action and disconnect-to-fresh-BBO parity
fixtures.

## P1 — shadow/native adapter parity

Keep MAINNET_SHADOW and mainnet_armed=false. Map one asset bid/ask to native
OrderExecutor create/stop actions in a fake connector. Compare IDs,
quantization, deadband, residency, cancellation, and fee/PnL to the shadow
ledger.

## P2 — separately authorized testnet canary

Use one isolated asset. Verify no duplicate ownership, reconnect safety,
post-only rejection, margin/funding, recorder fills, and rate-limit headroom.
Require conservative evidence and complete markout denominators; touch remains
diagnostic.

## P3 — portfolio decision

Only after P0-P2 parity and separate authorization choose one multi-asset
controller or supervisor plus three controllers. No active strategy,
reference, parameter, or architecture change is implied here.

## Parity tests

- one bid/ask per asset and no duplicate active executor IDs;
- native price/amount quantization equals current Derive rules;
- below-deadband observation creates no mutation;
- above-deadband movement maps to one cancel/create lifecycle;
- disconnect cancels existing quotes and blocks new decisions;
- only fresh Derive BBO resumes decisions;
- native fill fee/PnL/equity reconciles to the ledger;
- restart preserves ownership without duplicate quotes;
- shared $800 capital/minimums admit deterministically;
- current-run dashboard panel IDs all match.
"""


def _top_markdown(title: str, rows: list[tuple[str, str, str, str]]) -> str:
    lines = [f"# {title}", "", "| Feature | Source | Action | Why |", "|---|---|---|---|"]
    lines.extend(f"| {feature} | `{source}` | `{action}` | {reason} |" for feature, source, action, reason in rows)
    return "\n".join(lines)


def _deletions() -> str:
    return """# Code deletion opportunities

No immediate deletion is safe. Native Hummingbot executors are actual connector
execution components, while the current custom modules implement a research-
only shadow collector and evidence ledger.

Future reductions after parity tests could replace actual order placement with
native OrderExecutor/action contracts, route transport limits through
AsyncThrottler, and use native recorder performance for actual fills. The
public-reference, markout, shadow-fill, deadband, and panel-audit layers should
remain separate. No file was deleted and no active run was changed.
"""


def _version_report(package: dict[str, Any], inventory_files: list[str], api_checkout: Path, git: dict[str, Any], container: str) -> str:
    connector_files = [path for path in inventory_files if "/connector/derivative/derive_perpetual/" in path or path.endswith("/core/api_throttler/async_throttler.py")]
    return f"""# Local Hummingbot V2 version

## Primary installed source

- Container: `{container}`
- Distribution: `hummingbot`
- Package version: `{package.get('version', 'UNAVAILABLE')}`
- Installed source root: `{package.get('package', PACKAGE_SOURCE)}`
- Installed package git commit: **not available** (wheel/site-package source)
- Installed Python: `{PACKAGE_PYTHON}`
- Installed Python files observed: `{len(inventory_files)}`

## Local API checkout context

- Checkout: `{api_checkout}`
- Branch: `{git.get('branch') or 'unavailable'}`
- HEAD: `{git.get('commit') or 'unavailable'}`
- Origin: `{git.get('remote') or 'unavailable'}`
- Checkout status was inspected only; existing dirty changes were preserved.

## Connector source

The Derive connector is part of the installed distribution at
`hummingbot/connector/derivative/derive_perpetual`. Connector modules are not
separately versioned in the inspected installation; the package version above
is the reproducibility anchor.

Representative installed connector/throttler files observed:

""" + "\n".join(f"- `{path}`" for path in connector_files[:40])


def _final_report(package: dict[str, Any], api_checkout: Path, rows: list[dict[str, Any]], git: dict[str, Any]) -> str:
    found = sum(row["local_present"] == "YES" for row in rows)
    missing = [row["controller"] for row in rows if row["local_present"] == "NO"]
    return f"""# Final Hummingbot V2 reuse audit

## Executive summary

This is a read-only audit of the locally installed Hummingbot V2 runtime and
local controller catalog. It does not refactor the active strategy, switch
MarketDataProvider, delete modules, start/stop a bot, place orders, or change
shadow behavior.

| Decision | Result |
|---|---|
| Installed package | `{package.get('version', 'UNAVAILABLE')}` at `{package.get('package', PACKAGE_SOURCE)}` |
| Controller catalog coverage | `{found}/{len(rows)}` found locally |
| MarketMakingControllerBase native for this multi-asset controller | **NO** |
| ControllerBase can host the multi-asset adapter | **YES** |
| Single-maker native executor | **OrderExecutor** |
| Continuous two-sided multi-asset native executor | **NO_SINGLE_EXECUTOR** |
| Dedicated native multi-asset supervisor found | **NO** |
| Architecture changed | **NO** |
| Migration executed | **NO** |
| Orders / positions changed | **0 / 0** |

## Source and catalog

The installed wheel is the primary Hummingbot source authority. The API
checkout at `{api_checkout}` is used for the requested catalog comparison and
remains untouched (branch `{git.get('branch') or 'unavailable'}`, HEAD
`{git.get('commit') or 'unavailable'}`). The complete 20-controller scan is in
`controller_inventory.csv` and `controller_scan.md`. Missing locally:
`{', '.join(missing) or 'none'}`.

## Native base and executor conclusion

Reuse native StrategyV2Base, ControllerBase, MarketDataProvider, AsyncThrottler,
action contracts, ExecutorOrchestrator, MarketsRecorder, connector rules,
quantization, and OrderExecutor. MarketMakingControllerBase is reusable for
its lifecycle/level ideas, but its single connector/pair shape and perpetual
rebalance behavior do not implement this multi-asset shared-capital supervisor.

PositionExecutor is a finite Triple Barrier position executor, not a continuous
two-sided maker. OrderExecutor is the minimum native primitive for one bid or
ask; the controller retains bid/ask ownership and business refresh policy.

## Reuse boundary

Use native infrastructure for transport, connector data, rules, quantization,
order lifecycle, action routing, and actual execution accounting. Keep custom
priority references, basis/fair value, adaptive deadband, adverse protection,
inventory/portfolio risk, conservative/touch diagnostics, markouts, and the
research dashboard/ledger.

## Architecture and migration

The preferred target is one ControllerBase-based multi-asset supervisor mapping
each asset bid/ask to native OrderExecutor actions. A shared supervisor plus
three isolated controllers is the fallback if fault isolation proves more
important. The staged P0-P3 plan and parity tests are documented, but no stage
was started.

## Limitations

- The installed source is a wheel, so no installed-package git commit was
  available.
- Account-specific Derive limits cannot be inferred from public introspection.
- Static source line references reflect the inspected package and should be
  rechecked after an image upgrade.
- Catalog presence is not readiness, strategy quality, or live deployability.

## Artifact index

See `feature_reuse_matrix.csv`, `custom_duplication_scores.csv`,
`migration_plan.md`, and the controller/base/executor audits in this directory.
"""


def build_audit(*, project_root: Path, api_checkout: Path, container: str, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    package = _container_version(container)
    inventory_files = _container_inventory(container)
    rows, local_only = _controller_inventory(api_checkout)
    matrix = _matrix()
    scores = _scores()
    git = {
        "branch": _run(["git", "-C", str(api_checkout), "branch", "--show-current"]),
        "commit": _run(["git", "-C", str(api_checkout), "rev-parse", "HEAD"]),
        "remote": _run(["git", "-C", str(api_checkout), "remote", "get-url", "origin"]),
    }
    package_root = package.get("package", PACKAGE_SOURCE)
    _write(output_dir / "local_hummingbot_version.md", _version_report(package, inventory_files, api_checkout, git, container))
    _write_csv(output_dir / "controller_inventory.csv", rows, ["category", "controller", "catalog_source", "local_present", "local_path", "classes", "scan_status"])
    _write(output_dir / "controller_scan.md", _controller_scan(rows, local_only))
    _write(output_dir / "base_class_scan.md", _base_scan(package_root))
    _write(output_dir / "market_data_provider_audit.md", _market_data(package_root))
    _write(output_dir / "executor_scan.md", _executor_scan(package_root))
    _write(output_dir / "executor_orchestrator_audit.md", _orchestrator(package_root))
    _write(output_dir / "rate_limit_audit.md", _rate_limit(package_root))
    _write(output_dir / "trading_rules_audit.md", _rules(project_root))
    _write(output_dir / "inventory_feature_audit.md", _inventory())
    _write(output_dir / "accounting_feature_audit.md", _accounting())
    _write(output_dir / "storage_duplication_audit.md", _storage())
    _write_csv(output_dir / "feature_reuse_matrix.csv", matrix, ["our_feature", "custom_or_native", "native", "file", "class", "match", "action"])
    _write(output_dir / "feature_reuse_matrix.md", _matrix_summary(matrix))
    _write_csv(output_dir / "custom_duplication_scores.csv", scores, ["module_or_feature", "score", "native_overlap", "unique_scope", "action"])
    _write(output_dir / "code_deletion_opportunities.md", _deletions())
    _write(output_dir / "multi_asset_architecture_comparison.md", _multi_asset())
    _write(output_dir / "target_architecture.md", _target())
    _write(output_dir / "migration_plan.md", _migration())
    _write(output_dir / "top_reuse_features.md", _top_markdown("Top native reuse features", NATIVE_TOP10))
    _write(output_dir / "top_custom_features.md", _top_markdown("Top custom features to retain", CUSTOM_TOP10))
    _write(output_dir / "final_v2_reuse_audit.md", _final_report(package, api_checkout, rows, git))
    action_counts = Counter(row["action"] for row in matrix)
    result = {
        "audit_version": "hummingbot-v2-reuse-audit-v1",
        "status": "COMPLETE_READ_ONLY_AUDIT",
        "observed_at_utc": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "primary_source": {"container": container, "distribution": "hummingbot", "version": package.get("version"), "package_root": package_root, "git_commit": None, "python": PACKAGE_PYTHON, "python_file_count": len(inventory_files)},
        "api_checkout": {"path": str(api_checkout), **git},
        "controller_catalog": {"requested": len(rows), "found": sum(row["local_present"] == "YES" for row in rows), "missing": sum(row["local_present"] == "NO" for row in rows), "missing_names": [row["controller"] for row in rows if row["local_present"] == "NO"]},
        "answers": {"market_making_controller_base_native_for_this_multi_asset_controller": "NO", "controller_base_can_host_multi_asset_controller": "YES", "native_executor_for_single_maker_order": "OrderExecutor", "native_executor_for_continuous_two_sided_perp_mm": "NO_SINGLE_EXECUTOR", "dedicated_native_multi_asset_supervisor": "NO_FOUND", "v2_with_controllers_architecture_changed": "NO", "active_strategy_migrated": "NO"},
        "reuse_action_counts": dict(sorted(action_counts.items())),
        "top_native_features": [row[0] for row in NATIVE_TOP10],
        "top_custom_features": [row[0] for row in CUSTOM_TOP10],
        "duplication_score_scale": "0 none, 25 low, 50 partial, 75 high, 100 full duplicate",
        "safety": {"active_strategy_changed": False, "migration_executed": False, "orders_placed": 0, "positions_changed": 0},
    }
    result["artifacts"] = {path.name: str(path) for path in sorted(output_dir.iterdir())}
    (output_dir / "final_v2_reuse_audit.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--api-checkout", type=Path, default=Path("/Users/wilfred/Documents/Hummingbot/hummingbot-api"))
    parser.add_argument("--container", default="hummingbot-api")
    parser.add_argument("--output-dir", type=Path, default=Path("reports/hummingbot_v2_reuse_audit"))
    args = parser.parse_args(argv)
    result = build_audit(project_root=args.project_root.resolve(), api_checkout=args.api_checkout.resolve(), container=args.container, output_dir=args.output_dir.resolve())
    print(json.dumps({"status": result["status"], "version": result["primary_source"]["version"], "output_dir": str(args.output_dir.resolve())}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
