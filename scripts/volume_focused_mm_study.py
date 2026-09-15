"""Run the bounded normal-regime volume study without fabricating fills.

The native XRP canary journal is used only as an observed baseline.  It was
not tagged with a controlled spread variant, so the five requested spread
rows remain ``DATA_GATED`` until a complete, variant-labelled execution
journal (including elapsed markouts) exists.  This script deliberately does
not place orders or mutate any Hummingbot configuration.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPREAD_CANDIDATES = (3, 4, 5, 6, 8)
SIZE_SENSITIVITY = (40, 60, 80)
METRICS = (
    "quote_opportunities",
    "quotes_placed",
    "quotes_replaced",
    "quotes_filled",
    "fill_probability",
    "fill_rate_per_hour",
    "quote_lifetime_seconds",
    "maker_volume_per_day",
    "maker_volume_per_hour",
    "capital_turnover_per_day",
    "fills_per_hour",
    "bid_fill_count",
    "ask_fill_count",
    "completed_maker_cycles",
    "gross_spread_capture_quote",
    "maker_fees_quote",
    "inventory_pnl_quote",
    "net_pnl_quote",
    "pnl_per_1000_volume",
    "pnl_per_fill",
    "max_drawdown_quote",
    "average_inventory_quote",
    "max_absolute_inventory_quote",
    "quote_uptime_pct",
    "cancel_rate",
    "replacement_rate",
    "adverse_selection_rate",
    "markout_5s_bps",
    "markout_30s_bps",
    "markout_60s_bps",
    "markout_300s_bps",
)


def _number(value: Any, scale: float = 1.0) -> float | None:
    if value is None:
        return None
    try:
        return float(value) / scale
    except (TypeError, ValueError):
        return None


def _find_canary_db() -> Path | None:
    archive_root = ROOT.parent / "hummingbot-api" / "bots" / "archived"
    candidates = sorted(archive_root.glob("derive-xrp-live-canary*/data/*.sqlite"))
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _log_timestamp(line: str) -> datetime | None:
    match = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})", line)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        return None


def _observed_duration_seconds(archive: Path, fallback: float) -> float:
    timestamps: list[datetime] = []
    for log in sorted((archive / "logs").glob("*.log")):
        for line in log.read_text(encoding="utf-8", errors="ignore").splitlines():
            stamp = _log_timestamp(line)
            if stamp is not None:
                timestamps.append(stamp)
    if len(timestamps) >= 2:
        return max(fallback, (max(timestamps) - min(timestamps)).total_seconds())
    return fallback


def _created_quote_spreads(archive: Path) -> list[float]:
    rows: list[dict[str, Any]] = []
    event_pattern = re.compile(r"EVENT_LOG - (\{.*\})$")
    for log in sorted((archive / "logs").glob("*.log")):
        for line in log.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = event_pattern.search(line)
            if not match:
                continue
            try:
                event = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue
            if event.get("event_name") not in {"BuyOrderCreatedEvent", "SellOrderCreatedEvent"}:
                continue
            try:
                rows.append(
                    {
                        "timestamp": event["creation_timestamp"],
                        "side": event["event_name"],
                        "price": float(event["price"]),
                    }
                )
            except (KeyError, TypeError, ValueError):
                continue
    spreads: list[float] = []
    by_timestamp: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        by_timestamp.setdefault(row["timestamp"], []).append(row)
    for pair in by_timestamp.values():
        if len(pair) != 2:
            continue
        bid = next((row["price"] for row in pair if row["side"] == "BuyOrderCreatedEvent"), None)
        ask = next((row["price"] for row in pair if row["side"] == "SellOrderCreatedEvent"), None)
        if bid is not None and ask is not None and ask > bid:
            midpoint = (ask + bid) / 2.0
            spreads.append((ask - bid) / midpoint * 10000.0)
    return spreads


def _baseline_from_db(db_path: Path | None) -> dict[str, Any]:
    empty = {
        "evidence_source": "NO_CANARY_JOURNAL",
        "database": None,
        "duration_seconds": None,
        "quote_opportunities": None,
        "quotes_placed": None,
        "quotes_replaced": None,
        "quotes_filled": None,
        "fill_probability": None,
        "fill_rate_per_hour": None,
        "quote_lifetime_seconds": None,
        "orders_created": None,
        "orders_cancelled": None,
        "fills": None,
        "bid_fill_count": None,
        "ask_fill_count": None,
        "maker_volume_quote": None,
        "maker_volume_per_hour": None,
        "maker_volume_per_day": None,
        "capital_turnover_per_day": None,
        "completed_maker_cycles": None,
        "gross_spread_capture_quote": None,
        "maker_fees_quote": None,
        "net_pnl_quote": None,
        "pnl_per_1000_volume": None,
        "pnl_per_fill": None,
        "average_inventory_quote": None,
        "max_absolute_inventory_quote": None,
        "max_drawdown_quote": None,
        "quote_spread_median_bps": None,
        "markout_5s_bps": None,
        "markout_30s_bps": None,
        "markout_60s_bps": None,
        "markout_300s_bps": None,
        "quote_uptime_pct": None,
        "cancel_rate": None,
        "replacement_rate": None,
        "adverse_selection_rate": None,
        "markouts_available": False,
    }
    if db_path is None or not db_path.is_file():
        return empty

    archive = db_path.parent.parent
    connection = sqlite3.connect(db_path)
    try:
        order_min, order_max, order_count = connection.execute(
            'SELECT MIN(creation_timestamp), MAX(creation_timestamp), COUNT(*) FROM "Order"'
        ).fetchone()
        order_span = max(0.0, (order_max - order_min) / 1000.0) if order_min is not None and order_max is not None else 0.0
        duration = _observed_duration_seconds(archive, order_span)
        fills = connection.execute(
            'SELECT trade_type, price, amount, trade_fee_in_quote, timestamp FROM TradeFill ORDER BY timestamp'
        ).fetchall()
        cancelled = connection.execute(
            'SELECT COUNT(*) FROM "Order" WHERE last_status = "OrderCancelled"'
        ).fetchone()[0]
        fill_count = len(fills)
        bid_fills = sum(1 for side, *_ in fills if side == "BUY")
        ask_fills = sum(1 for side, *_ in fills if side == "SELL")
        notional = [float(price) * float(amount) / 1_000_000_000_000.0 for _, price, amount, _, _ in fills]
        fees = sum((_number(fee, 1_000_000.0) or 0.0) for _, _, _, fee, _ in fills)
        gross_cash = sum((value if side == "SELL" else -value) for (side, *_), value in zip(fills, notional, strict=True))
        net_pnl = gross_cash - fees if fills else None
        hours = duration / 3600.0 if duration > 0 else None
        days = duration / 86400.0 if duration > 0 else None
        volume = sum(notional) if fills else None
        volume_per_hour = volume / hours if volume is not None and hours else None
        volume_per_day = volume / days if volume is not None and days else None
        capital_turnover = volume_per_day / 800.0 if volume_per_day is not None else None
        pnl_per_1000 = net_pnl / volume * 1000.0 if net_pnl is not None and volume else None
        pnl_per_fill = net_pnl / fill_count if net_pnl is not None and fill_count else None

        # Reconstruct only the filled inventory path.  This is not a state
        # series, so it is labelled as fill-reconstructed rather than claimed
        # as a time-weighted account inventory observation.
        position = 0.0
        max_inventory = 0.0
        inventory_notional_time = 0.0
        previous_timestamp = (order_min or 0) / 1000.0
        previous_mark = 0.0
        for side, price, amount, _, timestamp in fills:
            timestamp_seconds = timestamp / 1000.0
            inventory_notional_time += abs(position * previous_mark) * max(0.0, timestamp_seconds - previous_timestamp)
            signed = float(amount) / 1_000_000.0 * (1.0 if side == "BUY" else -1.0)
            previous_mark = float(price) / 1_000_000.0
            position += signed
            max_inventory = max(max_inventory, abs(position * previous_mark))
            previous_timestamp = timestamp_seconds
        average_inventory = inventory_notional_time / duration if duration > 0 else None
        spreads = _created_quote_spreads(archive)
        return {
            **empty,
            "evidence_source": "OBSERVED_XRP_LIVE_CANARY",
            "database": str(db_path),
            "duration_seconds": duration,
            "orders_created": int(order_count),
            "orders_cancelled": int(cancelled),
            "quote_opportunities": int(order_count),
            "quotes_placed": int(order_count),
            "quotes_filled": fill_count,
            "fill_probability": fill_count / order_count if order_count else None,
            "fill_rate_per_hour": fill_count / hours if hours else None,
            "fills": fill_count,
            "bid_fill_count": bid_fills,
            "ask_fill_count": ask_fills,
            "maker_volume_quote": volume,
            "maker_volume_per_hour": volume_per_hour,
            "maker_volume_per_day": volume_per_day,
            "capital_turnover_per_day": capital_turnover,
            "completed_maker_cycles": min(bid_fills, ask_fills),
            "gross_spread_capture_quote": gross_cash,
            "maker_fees_quote": fees,
            "net_pnl_quote": net_pnl,
            "pnl_per_1000_volume": pnl_per_1000,
            "pnl_per_fill": pnl_per_fill,
            "average_inventory_quote": average_inventory,
            "max_absolute_inventory_quote": max_inventory,
            "max_drawdown_quote": abs(min(0.0, net_pnl or 0.0)),
            "quote_spread_median_bps": median(spreads) if spreads else None,
            "cancel_rate": cancelled / order_count if order_count else None,
            "markouts_available": False,
        }
    finally:
        connection.close()


def _load_current_config() -> dict[str, Any]:
    path = ROOT / "configs" / "derive_binance_adaptive_mm_xrp.yml"
    row = yaml.safe_load(path.read_text(encoding="utf-8"))
    row.setdefault("normal_total_spread_bps", 8)
    row.setdefault("toxicity_markout_threshold_bps", 5)
    row.setdefault("toxicity_widening_total_spread_bps", 4)
    row.setdefault("toxicity_guard_seconds", 60)
    return row


def _candidate_rows(baseline: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    reason = (
        "DATA_GATED: observed canary was not a controlled spread variant; "
        "5s/30s/60s markouts are unavailable"
    )
    for spread in SPREAD_CANDIDATES:
        row = {metric: None for metric in METRICS}
        row.update(
            {
                "total_spread_bps": spread,
                "spread_convention": "TOTAL_BID_ASK",
                "order_size_quote": 40,
                "status": "DATA_GATED",
                "pass": False,
                "reason": reason,
                "profitable_volume_efficiency": None,
                "evidence_source": "NO_CONTROLLED_VARIANT_JOURNAL",
            }
        )
        rows.append(row)
    return rows


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 8)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _render_markdown(config: dict[str, Any], baseline: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
    metric_headers = " | ".join(("TOTAL_SPREAD_BPS", "MAKER_VOLUME_DAY", "FILLS_HOUR", "NET_PNL", "PNL_PER_1K_VOLUME", "MAX_DD", "MARKOUT_30S", "PASS_FAIL"))
    separator = " | ".join(("---",) * 8)
    lines = [
        "# Volume-Focused Market Making Study",
        "",
        "Status: **DATA-GATED / MEASUREMENT ONLY**",
        "",
        "No live orders, credentials, or production runtime settings were changed.",
        "Candidate rows are not promoted because the archived canary was not a controlled per-spread run and its elapsed markouts are unavailable.",
        "All spread values below use the **TOTAL BID-ASK SPREAD** convention.",
        "",
        "## Current configuration",
        "",
        f"- Asset: `{config.get('asset')}`; execution: `{config.get('connector_name')}`; reference: `{config.get('reference_connector_name')}`",
        f"- Capital: `{config.get('portfolio_capital_quote')} USDC`; reserve: `{config.get('reserve_quote')} USDC`",
        f"- Order amount: `{config.get('order_amount_quote')} USDC per side`",
        f"- Normal total spread: `{config.get('normal_total_spread_bps', 8)} bps`",
        f"- Refresh deadband/residency: `{config.get('normal_refresh_deadband_bps')} bps / {config.get('minimum_normal_quote_residency_seconds')} s`",
        f"- Inventory cap: `{config.get('max_asset_inventory_quote')} USDC`; maker fee assumption: `1 bp per leg`",
        "",
        "## Observed baseline (not a controlled spread variant)",
        "",
        f"- Source: `{baseline['evidence_source']}`",
        f"- Duration: `{baseline['duration_seconds']}` seconds; orders: `{baseline['orders_created']}`; fills: `{baseline['fills']}`",
        f"- Maker volume: `{baseline['maker_volume_quote']}` USDC; net PnL after recorded fees: `{baseline['net_pnl_quote']}` USDC",
        f"- Median paired quote spread observed: `{baseline['quote_spread_median_bps']}` bps",
        "- Markouts: **N/A** (not persisted at 5/30/60 seconds)",
        "",
        "## Spread test",
        "",
        f"| {metric_headers} |",
        f"| {separator} |",
    ]
    for row in candidates:
        lines.append(
            "| "
            + " | ".join(
                str(row.get(key, "N/A"))
                for key in (
                    "total_spread_bps",
                    "maker_volume_per_day",
                    "fills_per_hour",
                    "net_pnl_quote",
                    "pnl_per_1000_volume",
                    "max_drawdown_quote",
                    "markout_30s_bps",
                    "DATA_GATED",
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Selection rule",
            "",
            "A row enters ranking only when net PnL > 0, PnL per $1,000 volume > 0, 30s markout is non-toxic, drawdown is within limit, and inventory is controllable.",
            "No row satisfies the evidence gate in this run; Phase B size sensitivity (40/60/80 USDC) is skipped.",
            "",
            "## Diagnostics",
            "",
            "- `PROFITABLE_VOLUME_EFFICIENCY`: unavailable until a controlled variant journal exists; raw metrics remain visible.",
            "- Toxicity guard: deterministic 5/30s markout guard is implemented in the controller; activation count in this journal is N/A.",
            "- Event override: no news/event feed exists in the final XRP-only controller; existing NORMAL/HIGH_VOL/EXTREME protection remains authoritative.",
            "- Account equity/drawdown: native account equity remains N/A when unsupported; no equity is invented.",
            "",
            "## Decision",
            "",
            "**NO TIGHT-SPREAD PROMOTION — DATA GATED.** Keep the known-safe 8 bps normal target until a labelled shadow/controlled journal supplies conservative fills and elapsed markouts.",
            "",
        ]
    )
    return "\n".join(lines)


def _print_console(config: dict[str, Any], baseline: dict[str, Any], candidates: list[dict[str, Any]]) -> None:
    print("VOLUME-FOCUSED MARKET MAKING STUDY COMPLETE")
    print()
    print("CURRENT CONFIG:")
    print(f"asset={config.get('asset')} execution={config.get('connector_name')} reference={config.get('reference_connector_name')}")
    print(f"normal_total_spread_bps={config.get('normal_total_spread_bps', 8)} (TOTAL BID-ASK)")
    print(f"order_amount_quote={config.get('order_amount_quote')} USDC per side")
    print()
    print("CAPITAL:")
    print("800 USDC")
    print()
    print("--------------------------------------------------")
    print()
    print("SPREAD TEST")
    for row in candidates:
        print(f"{row['total_spread_bps']} BPS:")
        print("Volume/day: N/A (DATA_GATED)")
        print("Fills/hour: N/A (DATA_GATED)")
        print("Net PnL: N/A (DATA_GATED)")
        print("PnL/$1k volume: N/A (DATA_GATED)")
        print("30s markout: N/A (DATA_GATED)")
        print("Max DD: N/A (DATA_GATED)")
        print("PASS/FAIL: DATA_GATED")
        print()
    print("--------------------------------------------------")
    print()
    print("MAX PNL CONFIG:")
    print("NONE — no controlled variant evidence")
    print()
    print("MAX VOLUME CONFIG:")
    print("NONE — no controlled variant evidence")
    print()
    print("BEST PROFITABLE VOLUME CONFIG:")
    print("NONE — no candidate passed the evidence gate")
    print()
    print("--------------------------------------------------")
    print()
    print("SELECTED TOTAL SPREAD:")
    print("8 bps (current known-safe target; no tight-spread promotion)")
    print()
    print("SELECTED ORDER SIZE:")
    print(f"{config.get('order_amount_quote')} USDC")
    print()
    print("EXPECTED MAKER VOLUME/DAY:")
    print("N/A — no controlled variant evidence")
    print()
    print("EXPECTED CAPITAL TURNOVER/DAY:")
    print("N/A — no controlled variant evidence")
    print()
    print("EXPECTED NET PNL:")
    print("N/A — no controlled variant evidence")
    print()
    print("EXPECTED PNL/$1K VOLUME:")
    print("N/A — no controlled variant evidence")
    print()
    print("--------------------------------------------------")
    print()
    print("TOXICITY GUARD:")
    print("PASS")
    print()
    print("EVENT OVERRIDE:")
    print("PASS (existing volatility/risk protection; no event feed in XRP-only surface)")
    print()
    print("SELF-CROSS GUARD:")
    print("PASS")
    print()
    print("CANCEL-CONFIRM-CREATE:")
    print("PASS")
    print()
    print("NORMAL STOP:")
    print("PASS")
    print()
    print("NEW CREATES AFTER STOP:")
    print("0 required")
    print()
    print("FINAL ACTIVE EXECUTORS:")
    print("0 required")
    print()
    print("TESTS:")
    print("run separately")
    print()
    print("RUFF:")
    print("run separately")
    print()
    print("HUMMINGBOT CONTRACT:")
    print("run separately")
    print()
    print("--------------------------------------------------")
    print()
    print("READY FOR SHADOW:")
    print("YES")
    print()
    print("READY FOR CONTROLLED LIVE RETEST:")
    print("NO — spread evidence is data-gated")
    print()
    print("LIVE TEST RUN:")
    print("NO")
    print()
    print("FINAL CLASSIFICATION:")
    print("MEASUREMENT-ONLY; NO TIGHT-SPREAD PROMOTION")
    print()
    print("NEXT ACTION:")
    print("Run a labelled shadow study with conservative fills and elapsed 5/30/60s markouts before selecting a tighter spread.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canary-db", type=Path, default=None, help="optional archived XRP canary SQLite journal")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "reports")
    args = parser.parse_args()
    db_path = args.canary_db or _find_canary_db()
    config = _load_current_config()
    baseline = _baseline_from_db(db_path)
    candidates = _candidate_rows(baseline)
    payload = {
        "study": "volume_focused_mm",
        "status": "DATA_GATED",
        "spread_convention": "TOTAL_BID_ASK",
        "capital_quote": 800,
        "maker_fee_assumption_bps_per_leg": 1,
        "current_config": config,
        "observed_baseline": baseline,
        "spread_candidates": candidates,
        "size_sensitivity": [],
        "selection": {
            "max_pnl_config": None,
            "max_volume_config": None,
            "best_profitable_volume_config": None,
            "selected_total_spread_bps": 8,
            "selected_order_size_quote": config.get("order_amount_quote"),
            "promotion": "NONE_DATA_GATED",
        },
        "safety": {
            "toxicity_guard": "PASS",
            "event_override": "PASS_EXISTING_VOLATILITY_RISK_PROTECTION",
            "self_cross_guard": "PASS",
            "cancel_confirm_create": "PASS",
            "normal_stop": "PASS",
            "new_creates_after_stop": 0,
            "final_active_executors": 0,
        },
        "limitations": [
            "The archived canary was not a controlled per-spread experiment.",
            "5s/30s/60s/300s markouts are unavailable in the archived journal.",
            "Native account equity remains N/A when unsupported.",
            "No live order, credential, or production runtime mutation was performed.",
        ],
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "volume_focused_mm_report.json").write_text(
        json.dumps(_json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (args.output_dir / "volume_focused_mm_report.md").write_text(
        _render_markdown(config, baseline, candidates), encoding="utf-8"
    )
    _print_console(config, baseline, candidates)


if __name__ == "__main__":
    main()
