"""Offline refresh-deadband/residency research for ZEC, XRP, and LINK.

This module reads retained SQLite telemetry in read-only mode.  It replays one
causal observation stream for every deadband/residency pair and writes only
aggregates, so the raw telemetry is not copied once per variant.  Candidate
fills and markouts are explicitly hypothetical; they are never presented as
Derive execution evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from pathlib import Path
from typing import Any

from .config import RuntimeConfig
from .markouts import maker_perspective_markout
from .models import Side
from .public_data import DerivePublicClient
from .quote_engine import round_down, round_up
from .quote_fill_diagnostic import (
    _observations,
    _payload_values,
    _read_json,
    _read_telemetry,
    _write_csv,
    _write_json,
)
from .rate_limit_audit import write_rate_limit_audit
from .refresh_governor import CHURN_LOOKBACKS_MS, DEADBAND_GRID_BPS, RESIDENCY_GRID_SECONDS, is_adverse_fast_move

PRIMARY_CONTROL = "PRIORITY_FAILOVER"
FILL_MODELS = ("CONSERVATIVE", "TOUCH_SENSITIVITY")
MARKOUT_HORIZONS = (1, 5, 15, 30, 60)
MAX_OBSERVATION_GAP_SECONDS = 5.0
EVIDENCE_RULE = (
    "sufficient only when Derive trades >=30 OR conservative fills >=20, plus >=20 30s and >=20 60s markouts"
)


def _decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value in (None, ""):
        return default
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def _float(value: Any, default: float | None = None) -> float | None:
    number = _decimal(value)
    return float(number) if number is not None else default


def _round(value: Any, places: int = 6) -> float | None:
    number = _float(value)
    return round(number, places) if number is not None and math.isfinite(number) else None


def _mean(values: Iterable[Any]) -> float | None:
    numbers = [number for value in values if (number := _float(value)) is not None]
    return sum(numbers) / len(numbers) if numbers else None


def _quantile(values: Iterable[Any], probability: float) -> float | None:
    numbers = sorted(number for value in values if (number := _float(value)) is not None)
    if not numbers:
        return None
    position = (len(numbers) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return numbers[lower]
    return numbers[lower] + (numbers[upper] - numbers[lower]) * (position - lower)


def _pct(numerator: int | float, denominator: int | float) -> float:
    return round(float(numerator) / float(denominator) * 100.0, 6) if denominator else 0.0


def _iso(timestamp: Any) -> str | None:
    number = _float(timestamp)
    return datetime.fromtimestamp(number, UTC).isoformat().replace("+00:00", "Z") if number is not None else None


def _safe_asset(value: Any) -> str:
    return str(value or "").strip().upper()


def _obs_values(observation: dict[str, Any]) -> dict[str, Any]:
    values = _payload_values(observation)
    payload = observation.get("payload") or {}
    controls = payload.get("controls") or {}
    selected = controls.get(PRIMARY_CONTROL) or {}
    state = selected.get("state") or {}
    plan = selected.get("plan") or {}
    fair = selected.get("fair_value") or {}
    if not isinstance(fair, dict):
        fair = {}
    values.update(
        {
            "return_1s": _decimal(state.get("return_1s"), _decimal(payload.get("return_1s"), Decimal("0"))),
            "return_5s": _decimal(state.get("return_5s"), _decimal(payload.get("return_5s"), Decimal("0"))),
            "market_mode": state.get("market_mode", payload.get("market_mode", "")),
            "state_reason": state.get("reason", ""),
            "desired_bid": _decimal(plan.get("bid_price"), values.get("desired_bid")),
            "desired_ask": _decimal(plan.get("ask_price"), values.get("desired_ask")),
            "desired_bid_amount": _decimal(plan.get("bid_amount"), _decimal(payload.get("desired_bid_amount"), Decimal("0"))),
            "desired_ask_amount": _decimal(plan.get("ask_amount"), _decimal(payload.get("desired_ask_amount"), Decimal("0"))),
            "fair_value": _decimal(fair.get("derive_fair_value"), values.get("fair_value")),
            "reference_fair_value": _decimal(payload.get("reference_fair_value"), values.get("fair_value")),
        }
    )
    return values


def _rounded_price(price: Decimal | None, side: str, tick_size: Decimal | None) -> Decimal | None:
    if price is None or price <= 0:
        return None
    if tick_size is None or tick_size <= 0:
        return price
    return round_down(price, tick_size) if side == Side.BUY.value else round_up(price, tick_size)


def _quote_location(quote: dict[str, Any], values: dict[str, Any], tick_size: Decimal | None) -> tuple[str, float | None]:
    price = _decimal(quote.get("price"))
    bid, ask = values.get("derive_bid"), values.get("derive_ask")
    touch = bid if quote.get("side") == Side.BUY.value else ask
    if price is None or touch is None or touch <= 0:
        return "DATA_UNAVAILABLE", None
    distance_bps = abs(price - touch) / touch * Decimal("10000")
    if tick_size is not None and tick_size > 0:
        ticks = abs(price - touch) / tick_size
        if ticks <= Decimal("0.1"):
            return "AT_TOUCH", float(distance_bps)
        if ticks < Decimal("1.5"):
            return "ONE_TICK", float(distance_bps)
        return "TWO_PLUS_TICKS", float(distance_bps)
    return "AWAY_FROM_TOUCH", float(distance_bps)


def _advance_quote(quote: dict[str, Any], values: dict[str, Any], gap: float, tick_size: Decimal | None, threshold_bps: Decimal) -> None:
    if gap <= 0 or gap > MAX_OBSERVATION_GAP_SECONDS:
        return
    quote["observed_seconds"] += gap
    quote["same_price_observed_seconds"] += gap
    location, distance = _quote_location(quote, values, tick_size)
    if distance is not None:
        quote["distance_touch_bps"].append(distance)
    quote[f"time_{location.lower()}_seconds"] = quote.get(f"time_{location.lower()}_seconds", 0.0) + gap
    mid = values.get("derive_mid")
    price = _decimal(quote.get("price"))
    if mid is not None and mid > 0 and price is not None:
        adverse = (
            (price - mid) / mid * Decimal("10000")
            if quote.get("side") == Side.BUY.value
            else (mid - price) / mid * Decimal("10000")
        )
        if adverse > threshold_bps:
            quote["stale_exposure_seconds"] += gap
            if not quote["stale_open"]:
                quote["stale_incidents"] += 1
            quote["stale_open"] = True
        else:
            quote["stale_open"] = False


def _plan_values(observation: dict[str, Any], side: str) -> tuple[Decimal | None, Decimal, str, Decimal | None, Decimal]:
    values = _obs_values(observation)
    if side == Side.BUY.value:
        return values.get("desired_bid"), values.get("desired_bid_amount") or Decimal("0"), str(values.get("market_mode") or ""), values.get("fair_value"), values.get("return_1s") or Decimal("0")
    return values.get("desired_ask"), values.get("desired_ask_amount") or Decimal("0"), str(values.get("market_mode") or ""), values.get("fair_value"), values.get("return_1s") or Decimal("0")


def _new_quote(asset: str, side: str, price: Decimal, amount: Decimal, timestamp: float, fair_value: Decimal | None, sequence: int) -> dict[str, Any]:
    edge = None
    if fair_value is not None and fair_value > 0:
        edge = (
            (fair_value - price) / fair_value * Decimal("10000")
            if side == Side.BUY.value
            else (price - fair_value) / fair_value * Decimal("10000")
        )
    return {
        "quote_id": f"research-{sequence}",
        "asset": asset,
        "side": side,
        "price": price,
        "amount": amount,
        "create_time": timestamp,
        "end_time": None,
        "end_reason": None,
        "replacement_reason": None,
        "edge_bps": edge,
        "fair_value_at_create": fair_value,
        "observed_seconds": 0.0,
        "same_price_observed_seconds": 0.0,
        "distance_touch_bps": [],
        "stale_exposure_seconds": 0.0,
        "stale_incidents": 0,
        "stale_open": False,
        "time_at_touch_seconds": 0.0,
        "time_one_tick_seconds": 0.0,
        "time_two_plus_ticks_seconds": 0.0,
    }


def _replay_variant(
    asset: str,
    observations: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    deadband_bps: Decimal,
    residency_seconds: Decimal,
    tick_size: Decimal | None,
    config: RuntimeConfig,
    analysis_end: float,
) -> dict[str, Any]:
    active: dict[str, dict[str, Any]] = {}
    quotes: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    sequence = 0
    previous_timestamp: float | None = None
    previous_values: dict[str, Any] | None = None
    replacement_reasons: Counter[str] = Counter()

    def close(side: str, timestamp: float, reason: str) -> None:
        quote = active.pop(side, None)
        if quote is None:
            return
        quote["end_time"] = max(quote["create_time"], timestamp)
        quote["end_reason"] = reason
        quote["replacement_reason"] = reason
        quotes.append(quote)
        if reason not in {"FILL", "RUN_END_OPEN"}:
            actions.append({"timestamp": timestamp, "action": "CANCEL", "reason": reason, "side": side})
        if reason == "DEADBAND_REFRESH":
            replacement_reasons[reason] += 1

    def create(side: str, timestamp: float, desired: Decimal | None, amount: Decimal, fair: Decimal | None) -> None:
        nonlocal sequence
        if desired is None or amount <= 0:
            return
        rounded = _rounded_price(desired, side, tick_size)
        if rounded is None or rounded <= 0:
            return
        sequence += 1
        active[side] = _new_quote(asset, side, rounded, amount, timestamp, fair, sequence)
        actions.append({"timestamp": timestamp, "action": "CREATE", "reason": "READY_TO_CREATE", "side": side})

    for observation in observations:
        timestamp = float(observation["timestamp"])
        values = _obs_values(observation)
        if previous_timestamp is not None and previous_values is not None:
            gap = timestamp - previous_timestamp
            for quote in active.values():
                _advance_quote(quote, previous_values, gap, tick_size, config.fast_adverse_move_threshold_bps)
        for side in (Side.BUY.value, Side.SELL.value):
            desired, amount, market_mode, fair, signed_return = _plan_values(observation, side)
            current = active.get(side)
            if current is None:
                if market_mode != "PAUSED":
                    create(side, timestamp, desired, amount, fair)
                continue
            if market_mode == "PAUSED" or desired is None or amount <= 0:
                close(side, timestamp, "PROTECTIVE_PLAN_INVALID")
                continue
            if config.fast_adverse_move_override_enabled and is_adverse_fast_move(
                side, signed_return, config.fast_adverse_move_threshold_bps
            ):
                close(side, timestamp, "FAST_ADVERSE_MOVE_OVERRIDE")
                continue
            candidate = _rounded_price(desired, side, tick_size)
            age = Decimal(str(max(0.0, timestamp - float(current["create_time"]))))
            movement = (
                abs(candidate - _decimal(current["price"], Decimal("0")))
                / _decimal(current["price"], Decimal("1"))
                * Decimal("10000")
                if candidate is not None and _decimal(current["price"], Decimal("0")) > 0
                else Decimal("0")
            )
            if (
                candidate is not None
                and candidate != current["price"]
                and movement > deadband_bps
                and age >= residency_seconds
            ):
                close(side, timestamp, "DEADBAND_REFRESH")
                create(side, timestamp, candidate, amount, fair)
        previous_timestamp, previous_values = timestamp, values

    for side in list(active):
        close(side, analysis_end, "RUN_END_OPEN")
    for quote in quotes:
        quote["lifetime_seconds"] = max(0.0, float(quote["end_time"]) - float(quote["create_time"]))
        quote["lifetime_ms"] = quote["lifetime_seconds"] * 1000.0
        quote["active_at_end"] = quote["end_reason"] == "RUN_END_OPEN"
    return {
        "asset": asset,
        "deadband_bps": deadband_bps,
        "residency_seconds": residency_seconds,
        "quotes": quotes,
        "actions": actions,
        "replacement_reasons": replacement_reasons,
        "analysis_end": analysis_end,
    }


def _trade_hit(trade: dict[str, Any], quote: dict[str, Any], *, strict: bool) -> bool:
    price = _decimal(trade.get("price"))
    quote_price = _decimal(quote.get("price"))
    if price is None or quote_price is None:
        return False
    trade_side = str(trade.get("side") or "").upper()
    quote_side = str(quote.get("side") or "").upper()
    if quote_side == Side.BUY.value:
        return trade_side == Side.SELL.value and (price < quote_price if strict else price <= quote_price)
    return trade_side == Side.BUY.value and (price > quote_price if strict else price >= quote_price)


def _future_observation(observations: list[dict[str, Any]], target: float) -> dict[str, Any] | None:
    for observation in observations:
        timestamp = float(observation["timestamp"])
        if timestamp < target:
            continue
        if timestamp - target > MAX_OBSERVATION_GAP_SECONDS:
            return None
        return observation
    return None


def _fill_metrics(
    replay: dict[str, Any],
    observations: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    fill_model: str,
    maker_fee_bps: Decimal,
    duration_seconds: float,
) -> dict[str, Any]:
    strict_crossings = 0
    touch_events = 0
    fills: list[dict[str, Any]] = []
    churn_missed: dict[int, int] = {window: 0 for window in CHURN_LOOKBACKS_MS}
    markouts: dict[int, list[dict[str, Any]]] = {horizon: [] for horizon in MARKOUT_HORIZONS}
    for quote in replay["quotes"]:
        quote_trades = [
            trade
            for trade in trades
            if float(quote["create_time"]) < float(trade.get("timestamp") or 0) < float(quote["end_time"])
        ]
        strict_rows = [trade for trade in quote_trades if _trade_hit(trade, quote, strict=True)]
        touch_rows = [trade for trade in quote_trades if _trade_hit(trade, quote, strict=False)]
        strict_crossings += len(strict_rows)
        touch_events += len(touch_rows)
        selected = strict_rows if fill_model == "CONSERVATIVE" else touch_rows
        if selected:
            trade = min(selected, key=lambda row: float(row.get("timestamp") or 0))
            fill_timestamp = float(trade["timestamp"])
            fill_amount = min(
                _decimal(quote.get("amount"), Decimal("0")) or Decimal("0"),
                _decimal(trade.get("amount"), Decimal("0")) or Decimal("0"),
            )
            fills.append({"timestamp": fill_timestamp, "quote": quote, "amount": fill_amount})
        if quote.get("end_reason") == "DEADBAND_REFRESH":
            for window in CHURN_LOOKBACKS_MS:
                end = float(quote["end_time"]) + window / 1000.0
                if any(
                    float(trade.get("timestamp") or 0) > float(quote["end_time"])
                    and float(trade.get("timestamp") or 0) <= end
                    and _trade_hit(trade, quote, strict=fill_model == "CONSERVATIVE")
                    for trade in trades
                ):
                    churn_missed[window] += 1
    for fill in fills:
        quote = fill["quote"]
        fill_price = _decimal(quote.get("price"))
        if fill_price is None or fill_price <= 0:
            continue
        for horizon in MARKOUT_HORIZONS:
            future = _future_observation(observations, fill["timestamp"] + horizon)
            if future is None:
                continue
            values = _obs_values(future)
            derive_mid = values.get("derive_mid")
            reference_fair = values.get("reference_fair_value") or values.get("fair_value")
            if derive_mid is None or derive_mid <= 0:
                continue
            derive_markout = maker_perspective_markout(
                Side.BUY if quote["side"] == Side.BUY.value else Side.SELL,
                fill_price,
                derive_mid,
            )
            reference_markout = (
                maker_perspective_markout(
                    Side.BUY if quote["side"] == Side.BUY.value else Side.SELL,
                    fill_price,
                    reference_fair,
                )
                if reference_fair is not None and reference_fair > 0
                else None
            )
            edge = _decimal(quote.get("edge_bps"))
            net = edge - maker_fee_bps + derive_markout if edge is not None else None
            markouts[horizon].append(
                {
                    "derive_markout_bps": derive_markout,
                    "reference_markout_bps": reference_markout,
                    "net_capture_bps": net,
                }
            )
    duration_minutes = max(duration_seconds / 60.0, 1.0 / 60.0)
    lifetimes = [float(quote["lifetime_seconds"]) for quote in replay["quotes"]]
    active_seconds = sum(lifetimes)
    actions = replay["actions"]
    creates = sum(row["action"] == "CREATE" for row in actions)
    cancels = sum(row["action"] == "CANCEL" for row in actions)
    actual_replacement_cancels = sum(row["reason"] == "DEADBAND_REFRESH" for row in actions if row["action"] == "CANCEL")
    replacements = actual_replacement_cancels
    maker_volume = sum(
        (fill["amount"] or Decimal("0")) * (_decimal(fill["quote"].get("price"), Decimal("0")) or Decimal("0"))
        for fill in fills
    )
    result: dict[str, Any] = {
        "fill_model": fill_model,
        "quote_count": len(replay["quotes"]),
        "creates": creates,
        "cancels": cancels,
        "replacements": replacements,
        "replacement_cancel_actions": actual_replacement_cancels,
        "total_actions": len(actions),
        "conservative_fills": len(fills) if fill_model == "CONSERVATIVE" else 0,
        "touch_sensitivity_fills": len(fills) if fill_model == "TOUCH_SENSITIVITY" else 0,
        "fill_count": len(fills),
        "maker_volume": maker_volume,
        "fills_per_hour": len(fills) / max(duration_seconds / 3600.0, 1 / 3600.0),
        "strict_crossings": strict_crossings,
        "touch_events": touch_events,
        "lifetimes": lifetimes,
        "active_seconds": active_seconds,
        "queue_residency_seconds": [float(quote["same_price_observed_seconds"]) for quote in replay["quotes"]],
        "time_at_touch_seconds": sum(float(quote.get("time_at_touch_seconds", 0.0)) for quote in replay["quotes"]),
        "time_one_tick_seconds": sum(float(quote.get("time_one_tick_seconds", 0.0)) for quote in replay["quotes"]),
        "time_two_plus_ticks_seconds": sum(float(quote.get("time_two_plus_ticks_seconds", 0.0)) for quote in replay["quotes"]),
        "stale_exposure_seconds": sum(float(quote.get("stale_exposure_seconds", 0.0)) for quote in replay["quotes"]),
        "stale_incidents": sum(int(quote.get("stale_incidents", 0)) for quote in replay["quotes"]),
        "churn_missed": churn_missed,
        "markouts": markouts,
        "action_timestamps": [float(row["timestamp"]) for row in actions],
        "replacement_reasons": dict(replay["replacement_reasons"]),
        "duration_seconds": duration_seconds,
        "duration_minutes": duration_minutes,
    }
    return result


def _rolling_peak(timestamps: list[float], window_seconds: float) -> int:
    ordered = sorted(timestamps)
    best = 0
    left = 0
    for right, timestamp in enumerate(ordered):
        while ordered[left] < timestamp - window_seconds:
            left += 1
        best = max(best, right - left + 1)
    return best


def _load_rules(
    config: RuntimeConfig,
    mapping_path: Path | None,
    state: dict[str, Any],
    audit_path: Path | None = None,
) -> dict[str, Any]:
    raw = _read_json(mapping_path) if mapping_path else {}
    result: dict[str, Any] = {}
    for asset, item in (raw.get("mappings") or {}).items():
        rules = item.get("rules") if isinstance(item, dict) else None
        if isinstance(rules, dict):
            result[_safe_asset(asset)] = rules
    for asset, item in (state.get("mappings") or {}).items():
        if _safe_asset(asset) in result:
            continue
        rules = item.get("rules") if isinstance(item, dict) else None
        if isinstance(rules, dict):
            result[_safe_asset(asset)] = rules
    # Prefer the current v3 instrument snapshot captured by the standalone
    # rate-limit audit over legacy connector mappings, which may label Derive
    # perpetual collateral as USD even though the live instrument is USDC.
    audit = _read_json(audit_path) if audit_path else {}
    live_probe = (audit.get("live_probes") or {}).get("public_get_all_instruments") or {}
    instrument_rows = ((live_probe.get("body") or {}).get("result") or {}).get("instruments") or []
    for row in instrument_rows:
        if not isinstance(row, dict) or str(row.get("instrument_type", "")).lower() != "perp":
            continue
        asset = _safe_asset(row.get("base_currency") or row.get("base_asset"))
        if asset not in {spec.symbol for spec in config.enabled_assets}:
            continue
        result[asset] = {
            "instrument_name": row.get("instrument_name"),
            "base_asset": row.get("base_currency") or row.get("base_asset"),
            "quote_asset": row.get("quote_currency") or row.get("quote_asset", "USD"),
            "tick_size": row.get("tick_size"),
            "amount_step": row.get("amount_step"),
            "minimum_amount": row.get("minimum_amount"),
            "maximum_amount": row.get("maximum_amount"),
            "minimum_notional": row.get("minimum_order_notional", row.get("minimum_notional", "0")),
            "maker_fee_bps": _decimal(row.get("maker_fee_rate"), Decimal("0")) * Decimal("10000") if row.get("maker_fee_rate") is not None else None,
            "taker_fee_bps": _decimal(row.get("taker_fee_rate"), Decimal("0")) * Decimal("10000") if row.get("taker_fee_rate") is not None else None,
        }
    if len(result) < len(config.enabled_assets):
        try:
            for asset in config.enabled_assets:
                if asset.symbol in result:
                    continue
                for row in DerivePublicClient(config.derive_public_url).instruments():
                    if _safe_asset(row.get("base_currency") or row.get("base_asset")) == asset.symbol:
                        result[asset.symbol] = {
                            "instrument_name": row.get("instrument_name"),
                            "base_asset": row.get("base_currency") or row.get("base_asset"),
                            "quote_asset": row.get("quote_currency") or row.get("quote_asset", "USD"),
                            "tick_size": row.get("tick_size"),
                            "amount_step": row.get("amount_step"),
                            "minimum_amount": row.get("minimum_amount"),
                            "maximum_amount": row.get("maximum_amount"),
                            "minimum_notional": row.get("minimum_order_notional", row.get("minimum_notional")),
                            "maker_fee_bps": _decimal(row.get("maker_fee_rate"), Decimal("0")) * Decimal("10000") if row.get("maker_fee_rate") is not None else None,
                            "taker_fee_bps": _decimal(row.get("taker_fee_rate"), Decimal("0")) * Decimal("10000") if row.get("taker_fee_rate") is not None else None,
                        }
                        break
        except Exception:
            pass
    return result


def _rules_rows(config: RuntimeConfig, rules_by_asset: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Decimal | None]]:
    rows: list[dict[str, Any]] = []
    ticks: dict[str, Decimal | None] = {}
    for asset_spec in config.enabled_assets:
        asset = asset_spec.symbol
        raw = rules_by_asset.get(asset) or {}
        tick = _decimal(raw.get("tick_size"))
        ticks[asset] = tick
        rows.append(
            {
                "asset": asset,
                "instrument_name": raw.get("instrument_name"),
                "quote_asset": raw.get("quote_asset", "USD"),
                "tick_size": raw.get("tick_size"),
                "amount_step": raw.get("amount_step"),
                "minimum_amount": raw.get("minimum_amount"),
                "maximum_amount": raw.get("maximum_amount"),
                "minimum_notional": raw.get("minimum_notional"),
                "maker_fee_bps": raw.get("maker_fee_bps"),
                "taker_fee_bps": raw.get("taker_fee_bps"),
                "status": "OBSERVED" if tick is not None and _decimal(raw.get("amount_step")) is not None else "DATA_INSUFFICIENT",
                "source": "Derive public instrument metadata",
            }
        )
    return rows, ticks


def _capital_rows(config: RuntimeConfig, rules_by_asset: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for asset_spec in config.enabled_assets:
        asset = asset_spec.symbol
        raw = rules_by_asset.get(asset) or {}
        minimum_amount = _decimal(raw.get("minimum_amount"), Decimal("0")) or Decimal("0")
        minimum_notional = _decimal(raw.get("minimum_notional"), Decimal("0")) or Decimal("0")
        compatible = minimum_amount > 0 and config.max_single_order_notional > 0 and config.capital_usdc >= minimum_notional
        rows.append(
            {
                "asset": asset,
                "capital_usdc": config.capital_usdc,
                "max_single_order_notional": config.max_single_order_notional,
                "minimum_amount": minimum_amount,
                "minimum_notional": minimum_notional,
                "minimum_order_compatible": compatible,
                "capital_fraction_at_minimum": (minimum_notional / config.capital_usdc if config.capital_usdc > 0 else None),
                "zec_special_check": "ZEC_SPECIAL_CHECK_PASS" if asset == "ZEC" and compatible else ("NOT_APPLICABLE" if asset != "ZEC" else "ZEC_SPECIAL_CHECK_FAIL"),
                "status": "PASS" if compatible else "DATA_INSUFFICIENT",
            }
        )
    return rows


def _trade_activity_rows(config: RuntimeConfig, trades: list[dict[str, Any]], start: float, end: float) -> list[dict[str, Any]]:
    duration_hours = max((end - start) / 3600.0, 1 / 3600.0)
    rows = []
    for asset_spec in config.enabled_assets:
        asset = asset_spec.symbol
        asset_trades = sorted(
            [row for row in trades if _safe_asset(row.get("asset")) == asset and str(row.get("source")) == "derive"],
            key=lambda row: float(row.get("timestamp") or 0),
        )
        timestamps = [float(row["timestamp"]) for row in asset_trades]
        gaps = [following - current for current, following in pairwise(timestamps) if following > current]
        notional = sum(
            (_decimal(row.get("amount"), Decimal("0")) or Decimal("0")) * (_decimal(row.get("price"), Decimal("0")) or Decimal("0"))
            for row in asset_trades
        )
        rows.append(
            {
                "asset": asset,
                "trade_count": len(asset_trades),
                "trade_notional": notional,
                "trades_per_hour": len(asset_trades) / duration_hours,
                "notional_per_hour": notional / Decimal(str(duration_hours)),
                "intertrade_gap_median_seconds": _quantile(gaps, 0.5),
                "intertrade_gap_p90_seconds": _quantile(gaps, 0.9),
                "last_trade_time_utc": _iso(max(timestamps) if timestamps else None),
                "status": "OBSERVED" if asset_trades else "DATA_INSUFFICIENT",
            }
        )
    return rows


def _spread_rows(config: RuntimeConfig, observations: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for asset_spec in config.enabled_assets:
        asset = asset_spec.symbol
        spreads = [_obs_values(row).get("spread_bps") for row in observations.get(asset, [])]
        numbers = [value for value in spreads if value is not None]
        rows.append(
            {
                "asset": asset,
                "observations": len(numbers),
                "median_spread_bps": _quantile(numbers, 0.5),
                "spread_p75_bps": _quantile(numbers, 0.75),
                "spread_p90_bps": _quantile(numbers, 0.9),
                "spread_p95_bps": _quantile(numbers, 0.95),
                "persistence_observations": sum(1 for value in numbers if value is not None),
                "status": "OBSERVED" if numbers else "DATA_INSUFFICIENT",
            }
        )
    return rows


def _variant_row(asset: str, replay: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    duration_minutes = metrics["duration_minutes"]
    return {
        "asset": asset,
        "control": PRIMARY_CONTROL,
        "fill_model": metrics["fill_model"],
        "deadband_bps": replay["deadband_bps"],
        "minimum_residency_seconds": replay["residency_seconds"],
        "quote_count": metrics["quote_count"],
        "creates": metrics["creates"],
        "replacements": metrics["replacements"],
        "cancels": metrics["cancels"],
        "total_actions": metrics["total_actions"],
        "creates_per_minute": metrics["creates"] / duration_minutes,
        "replacements_per_minute": metrics["replacements"] / duration_minutes,
        "cancels_per_minute": metrics["cancels"] / duration_minutes,
        "total_actions_per_minute": metrics["total_actions"] / duration_minutes,
        "median_lifetime_ms": (_quantile(metrics["lifetimes"], 0.5) or 0.0) * 1000.0 if metrics["lifetimes"] else None,
        "p90_lifetime_ms": (_quantile(metrics["lifetimes"], 0.9) or 0.0) * 1000.0 if metrics["lifetimes"] else None,
        "queue_residency_proxy_median_ms": (_quantile(metrics["queue_residency_seconds"], 0.5) or 0.0) * 1000.0 if metrics["queue_residency_seconds"] else None,
        "queue_residency_proxy_definition": "continuous same-price observed time; not exchange queue position",
        "time_at_touch_seconds": metrics["time_at_touch_seconds"],
        "time_one_tick_seconds": metrics["time_one_tick_seconds"],
        "time_two_plus_ticks_seconds": metrics["time_two_plus_ticks_seconds"],
        "strict_crossings": metrics["strict_crossings"],
        "touch_events": metrics["touch_events"],
        "fill_count": metrics["fill_count"],
        "maker_volume": metrics["maker_volume"],
        "fills_per_hour": metrics["fills_per_hour"],
        "stale_exposure_seconds": metrics["stale_exposure_seconds"],
        "stale_incidents": metrics["stale_incidents"],
        "status": "OBSERVED" if metrics["quote_count"] else "DATA_INSUFFICIENT",
    }


def _rate_row(config: RuntimeConfig, row: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    timestamps = metrics["action_timestamps"]
    per_second_peak = _rolling_peak(timestamps, 1.0)
    per_ten_second_peak = _rolling_peak(timestamps, 10.0)
    per_minute_peak = _rolling_peak(timestamps, 60.0)
    avg_per_second = metrics["total_actions"] / max(metrics["duration_seconds"], 1.0)
    budget = float(config.max_order_actions_per_second)
    target = float(config.target_action_utilization)
    utilization = avg_per_second / budget * 100.0 if budget else 100.0
    if per_second_peak > math.floor(budget) or per_minute_peak > config.max_actions_per_minute:
        classification = "RATE_LIMIT_RISK"
    elif utilization > 90 or per_minute_peak > config.max_actions_per_minute * 0.9:
        classification = "HIGH"
    elif utilization > target * 100:
        classification = "HEALTHY_ABOVE_TARGET"
    else:
        classification = "HEALTHY"
    return {
        "asset": row["asset"],
        "control": row["control"],
        "fill_model": row["fill_model"],
        "deadband_bps": row["deadband_bps"],
        "minimum_residency_seconds": row["minimum_residency_seconds"],
        "creates_per_minute": row["creates_per_minute"],
        "replacements_per_minute": row["replacements_per_minute"],
        "cancels_per_minute": row["cancels_per_minute"],
        "total_actions_per_minute": row["total_actions_per_minute"],
        "rolling_peak_1s": per_second_peak,
        "rolling_peak_10s": per_ten_second_peak,
        "rolling_peak_60s": per_minute_peak,
        "average_actions_per_second": avg_per_second,
        "max_order_actions_per_second": config.max_order_actions_per_second,
        "max_order_actions_per_minute": config.max_actions_per_minute,
        "target_action_utilization": config.target_action_utilization,
        "utilization_pct_average": utilization,
        "headroom_actions_per_second": max(Decimal("0"), config.max_order_actions_per_second - Decimal(str(avg_per_second))),
        "rate_limit_status": config.rate_limit_status,
        "classification": classification,
        "emergency_override_enabled": config.fast_adverse_move_override_enabled,
        "emergency_cancellation_reserve_per_minute": config.emergency_cancel_budget_per_minute,
    }


def _recommendation(asset: str, rows: list[dict[str, Any]], trade_count: int) -> dict[str, Any]:
    conservative = [row for row in rows if row["fill_model"] == "CONSERVATIVE"]
    markout_30 = max((int(row.get("markout_30s_count") or 0) for row in conservative), default=0)
    markout_60 = max((int(row.get("markout_60s_count") or 0) for row in conservative), default=0)
    fills = max((int(row.get("fill_count") or 0) for row in conservative), default=0)
    sufficient = (trade_count >= 30 or fills >= 20) and markout_30 >= 20 and markout_60 >= 20
    candidates = [
        row
        for row in conservative
        if row.get("rate_classification") not in {"RATE_LIMIT_RISK", "HIGH"}
        and _float(row.get("net_capture_30s_mean_bps")) is not None
        and (_float(row.get("net_capture_30s_mean_bps")) or 0) >= 0
    ]
    selected = min(candidates, key=lambda row: (float(row["deadband_bps"]), float(row["minimum_residency_seconds"])), default=None)
    if not sufficient:
        decision = "INSUFFICIENT_SAMPLE"
        recommendation = "COLLECT_MORE_DATA_WITHOUT_PARAMETER_PROMOTION"
    elif selected is None:
        decision = "NO_VARIANT_PASSES_COST_AND_RATE_GATES"
        recommendation = "DO_NOT_PROMOTE"
    else:
        decision = "CANDIDATE_REQUIRES_REVIEW"
        recommendation = "REVIEW_SELECTED_VARIANT_OFFLINE_ONLY"
    return {
        "asset": asset,
        "decision": decision,
        "recommendation": recommendation,
        "best_deadband_bps": selected.get("deadband_bps") if selected else None,
        "best_minimum_residency_seconds": selected.get("minimum_residency_seconds") if selected else None,
        "best_fill_model": selected.get("fill_model") if selected else None,
        "trade_count_denominator": trade_count,
        "conservative_fill_denominator": fills,
        "markout_30s_denominator": markout_30,
        "markout_60s_denominator": markout_60,
        "evidence_rule": EVIDENCE_RULE,
        "live_auto_apply": False,
    }


def analyze_refresh_research(
    config: RuntimeConfig,
    telemetry_paths: Iterable[str | Path],
    out_dir: str | Path,
    *,
    state_path: str | Path | None = None,
    mapping_path: str | Path | None = None,
) -> dict[str, Any]:
    """Read telemetry and write the complete refresh-research artifact set."""

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    audit_path = out_dir / "derive_rate_limit_audit.json"
    if not audit_path.exists():
        # Keep the analyzer self-contained for isolated/replay-only runs.  This
        # fallback deliberately skips the network; the launcher performs the
        # separate live public audit before the research phase.
        write_rate_limit_audit(out_dir, probe_live=False)
    paths = [Path(path) for path in telemetry_paths]
    merged: dict[str, Any] = {
        "decisions": [],
        "rollups": [],
        "actions": [],
        "fills": [],
        "markouts": [],
        "trades": [],
        "health": [],
        "reference_values": [],
        "aggregates": [],
        "runtime": {},
        "tables": set(),
        "read_error": None,
    }
    for path in paths:
        data = _read_telemetry(path)
        for key in ("decisions", "rollups", "actions", "fills", "markouts", "trades", "health", "reference_values", "aggregates"):
            merged[key].extend(data.get(key, []))
        merged["tables"].update(data.get("tables", set()))
        if data.get("runtime"):
            merged["runtime"] = data["runtime"]
        if data.get("read_error"):
            merged["read_error"] = data["read_error"]
    state = _read_json(Path(state_path)) if state_path else (merged.get("runtime") or {})
    assets = [asset.symbol for asset in config.enabled_assets]
    observations = _observations(merged, assets)
    event_times = [
        float(row["timestamp"])
        for table in (merged["decisions"], merged["actions"], merged["trades"], merged["fills"])
        for row in table
        if _float(row.get("timestamp")) is not None
    ]
    start = _float(state.get("started_at")) or (min(event_times) if event_times else time.time())
    end = _float(state.get("ended_at")) or (max(event_times) if event_times else start)
    end = max(end, start)
    duration = max(0.001, end - start)
    rules_by_asset = _load_rules(
        config,
        Path(mapping_path) if mapping_path else None,
        state,
        audit_path=audit_path,
    )
    rule_rows, tick_sizes = _rules_rows(config, rules_by_asset)
    _write_csv(out_dir / "asset_trading_rules.csv", rule_rows)
    _write_csv(out_dir / "capital_compatibility.csv", _capital_rows(config, rules_by_asset))
    _write_csv(out_dir / "trade_activity.csv", _trade_activity_rows(config, merged["trades"], start, end))
    _write_csv(out_dir / "spread_statistics.csv", _spread_rows(config, observations))

    all_variant_rows: list[dict[str, Any]] = []
    quote_lifetime_rows: list[dict[str, Any]] = []
    queue_rows: list[dict[str, Any]] = []
    mutation_rows: list[dict[str, Any]] = []
    utilization_rows: list[dict[str, Any]] = []
    replacement_rows: list[dict[str, Any]] = []
    churn_rows: list[dict[str, Any]] = []
    matrix_rows: list[dict[str, Any]] = []
    markout_rows: list[dict[str, Any]] = []
    net_rows: list[dict[str, Any]] = []
    stale_rows: list[dict[str, Any]] = []
    per_asset_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for asset in assets:
        asset_observations = observations.get(asset, [])
        asset_trades = [
            row for row in merged["trades"] if _safe_asset(row.get("asset")) == asset and str(row.get("source")) == "derive"
        ]
        for deadband in DEADBAND_GRID_BPS:
            for residency in RESIDENCY_GRID_SECONDS:
                replay = _replay_variant(
                    asset,
                    asset_observations,
                    asset_trades,
                    deadband,
                    residency,
                    tick_sizes.get(asset),
                    config,
                    end,
                )
                for fill_model in FILL_MODELS:
                    metrics = _fill_metrics(replay, asset_observations, asset_trades, fill_model, config.maker_fee_bps, duration)
                    row = _variant_row(asset, replay, metrics)
                    for horizon in MARKOUT_HORIZONS:
                        values = metrics["markouts"][horizon]
                        derive_values = [item["derive_markout_bps"] for item in values]
                        reference_values = [item["reference_markout_bps"] for item in values]
                        net_values = [item["net_capture_bps"] for item in values]
                        row[f"markout_{horizon}s_count"] = len(derive_values)
                        row[f"markout_{horizon}s_median_bps"] = _quantile(derive_values, 0.5)
                        row[f"markout_{horizon}s_mean_bps"] = _mean(derive_values)
                        row[f"reference_markout_{horizon}s_median_bps"] = _quantile(reference_values, 0.5)
                        row[f"net_capture_{horizon}s_count"] = len(net_values)
                        row[f"net_capture_{horizon}s_median_bps"] = _quantile(net_values, 0.5)
                        row[f"net_capture_{horizon}s_mean_bps"] = _mean(net_values)
                        markout_rows.append(
                            {
                                "asset": asset,
                                "control": PRIMARY_CONTROL,
                                "fill_model": fill_model,
                                "deadband_bps": deadband,
                                "minimum_residency_seconds": residency,
                                "horizon_seconds": horizon,
                                "derive_markout_median_bps": _quantile(derive_values, 0.5),
                                "derive_markout_mean_bps": _mean(derive_values),
                                "reference_markout_median_bps": _quantile(reference_values, 0.5),
                                "sample_count": len(derive_values),
                                "negative_markout_rate_pct": _pct(sum((value or 0) < 0 for value in derive_values), len(derive_values)),
                                "status": "OBSERVED" if derive_values else "DATA_INSUFFICIENT",
                            }
                        )
                        net_rows.append(
                            {
                                "asset": asset,
                                "control": PRIMARY_CONTROL,
                                "fill_model": fill_model,
                                "deadband_bps": deadband,
                                "minimum_residency_seconds": residency,
                                "horizon_seconds": horizon,
                                "net_capture_median_bps": _quantile(net_values, 0.5),
                                "net_capture_mean_bps": _mean(net_values),
                                "sample_count": len(net_values),
                                "maker_fee_bps": config.maker_fee_bps,
                                "formula": "quoted_edge - maker_fee + Derive maker-perspective markout",
                                "status": "OBSERVED" if net_values else "DATA_INSUFFICIENT",
                            }
                        )
                    rate_row = _rate_row(config, row, metrics)
                    row["rate_classification"] = rate_row["classification"]
                    all_variant_rows.append(row)
                    per_asset_rows[asset].append(row)
                    quote_lifetime_rows.append(
                        {
                            **{key: row[key] for key in ("asset", "control", "fill_model", "deadband_bps", "minimum_residency_seconds")},
                            "quote_count": row["quote_count"],
                            "median_lifetime_ms": row["median_lifetime_ms"],
                            "p90_lifetime_ms": row["p90_lifetime_ms"],
                            "active_quote_seconds": metrics["active_seconds"],
                            "status": row["status"],
                        }
                    )
                    queue_rows.append(
                        {
                            **{key: row[key] for key in ("asset", "control", "fill_model", "deadband_bps", "minimum_residency_seconds")},
                            "quote_count": row["quote_count"],
                            "queue_residency_proxy_median_ms": row["queue_residency_proxy_median_ms"],
                            "queue_residency_proxy_p90_ms": (_quantile(metrics["queue_residency_seconds"], 0.9) or 0) * 1000 if metrics["queue_residency_seconds"] else None,
                            "definition": "same-price observed time only; no exchange queue position",
                            "status": row["status"],
                        }
                    )
                    mutation_rows.append(
                        {
                            **{key: row[key] for key in ("asset", "control", "fill_model", "deadband_bps", "minimum_residency_seconds")},
                            "creates": row["creates"],
                            "replacements": row["replacements"],
                            "cancels": row["cancels"],
                            "total_actions": row["total_actions"],
                            "creates_per_minute": row["creates_per_minute"],
                            "replacements_per_minute": row["replacements_per_minute"],
                            "cancels_per_minute": row["cancels_per_minute"],
                            "total_actions_per_minute": row["total_actions_per_minute"],
                        }
                    )
                    utilization_rows.append(rate_row)
                    for reason, count in metrics["replacement_reasons"].items():
                        replacement_rows.append(
                            {
                                "asset": asset,
                                "control": PRIMARY_CONTROL,
                                "fill_model": fill_model,
                                "deadband_bps": deadband,
                                "minimum_residency_seconds": residency,
                                "replacement_reason": reason,
                                "count": count,
                                "pct_of_replacements": _pct(count, metrics["replacements"]),
                                "denominator_replacements": metrics["replacements"],
                            }
                        )
                    for window, count in metrics["churn_missed"].items():
                        churn_rows.append(
                            {
                                "asset": asset,
                                "control": PRIMARY_CONTROL,
                                "fill_model": fill_model,
                                "deadband_bps": deadband,
                                "minimum_residency_seconds": residency,
                                "lookback_ms": window,
                                "hypothetical_churn_missed_fills": count,
                                "replacement_denominator": metrics["replacements"],
                                "status": "HYPOTHETICAL_ONLY",
                            }
                        )
                    stale_rows.append(
                        {
                            **{key: row[key] for key in ("asset", "control", "fill_model", "deadband_bps", "minimum_residency_seconds")},
                            "stale_exposure_seconds": row["stale_exposure_seconds"],
                            "stale_incidents": row["stale_incidents"],
                            "stale_threshold_bps": config.fast_adverse_move_threshold_bps,
                            "fast_adverse_override_enabled": config.fast_adverse_move_override_enabled,
                            "status": "OBSERVED_PROXY",
                        }
                    )
                    if fill_model == "CONSERVATIVE":
                        matrix_rows.append(
                            {
                                "asset": asset,
                                "control": PRIMARY_CONTROL,
                                "fill_model": fill_model,
                                "deadband_bps": deadband,
                                "minimum_residency_seconds": residency,
                                "quotes": row["quote_count"],
                                "creates_per_minute": row["creates_per_minute"],
                                "replacements_per_minute": row["replacements_per_minute"],
                                "cancels_per_minute": row["cancels_per_minute"],
                                "median_lifetime_ms": row["median_lifetime_ms"],
                                "queue_residency_proxy_median_ms": row["queue_residency_proxy_median_ms"],
                                "strict_crossings": row["strict_crossings"],
                                "conservative_fills": row["fill_count"],
                                "fills_per_hour": row["fills_per_hour"],
                                "markout_30s_median_bps": row["markout_30s_median_bps"],
                                "markout_60s_median_bps": row["markout_60s_median_bps"],
                                "net_capture_30s_median_bps": row["net_capture_30s_median_bps"],
                                "net_capture_60s_median_bps": row["net_capture_60s_median_bps"],
                                "rate_classification": rate_row["classification"],
                                "utilization_pct_average": rate_row["utilization_pct_average"],
                                "stale_exposure_seconds": row["stale_exposure_seconds"],
                                "status": row["status"],
                            }
                        )

    _write_csv(out_dir / "quote_lifetime.csv", quote_lifetime_rows)
    _write_csv(out_dir / "queue_residency_proxy.csv", queue_rows)
    _write_csv(out_dir / "quote_mutation_rate.csv", mutation_rows)
    _write_csv(out_dir / "rate_limit_utilization.csv", utilization_rows)
    _write_csv(out_dir / "replacement_reasons.csv", replacement_rows)
    _write_csv(out_dir / "churn_missed_fills.csv", churn_rows)
    _write_csv(out_dir / "deadband_residency_matrix.csv", matrix_rows)
    _write_csv(out_dir / "markout_by_variant.csv", markout_rows)
    _write_csv(out_dir / "net_capture_by_variant.csv", net_rows)
    _write_csv(out_dir / "stale_quote_risk.csv", stale_rows)

    deadband_rows: list[dict[str, Any]] = []
    residency_rows: list[dict[str, Any]] = []
    for asset in assets:
        conservative = [row for row in per_asset_rows[asset] if row["fill_model"] == "CONSERVATIVE"]
        for deadband in DEADBAND_GRID_BPS:
            group = [row for row in conservative if row["deadband_bps"] == deadband]
            deadband_rows.append(
                {
                    "asset": asset,
                    "deadband_bps": deadband,
                    "residency_variants": len(group),
                    "median_replacements_per_minute": _mean(row["replacements_per_minute"] for row in group),
                    "median_quote_lifetime_ms": _quantile([row["median_lifetime_ms"] for row in group], 0.5),
                    "total_conservative_fills": sum(int(row["fill_count"]) for row in group),
                    "median_markout_30s_bps": _quantile([row["markout_30s_median_bps"] for row in group], 0.5),
                    "median_markout_60s_bps": _quantile([row["markout_60s_median_bps"] for row in group], 0.5),
                    "median_net_capture_30s_bps": _quantile([row["net_capture_30s_median_bps"] for row in group], 0.5),
                    "rate_limit_risk_variants": sum(row.get("rate_classification") == "RATE_LIMIT_RISK" for row in group),
                    "status": "OBSERVED" if group else "DATA_INSUFFICIENT",
                }
            )
        for residency in RESIDENCY_GRID_SECONDS:
            group = [row for row in conservative if row["minimum_residency_seconds"] == residency]
            residency_rows.append(
                {
                    "asset": asset,
                    "minimum_residency_seconds": residency,
                    "deadband_variants": len(group),
                    "median_replacements_per_minute": _mean(row["replacements_per_minute"] for row in group),
                    "median_quote_lifetime_ms": _quantile([row["median_lifetime_ms"] for row in group], 0.5),
                    "total_conservative_fills": sum(int(row["fill_count"]) for row in group),
                    "median_markout_30s_bps": _quantile([row["markout_30s_median_bps"] for row in group], 0.5),
                    "median_markout_60s_bps": _quantile([row["markout_60s_median_bps"] for row in group], 0.5),
                    "median_net_capture_30s_bps": _quantile([row["net_capture_30s_median_bps"] for row in group], 0.5),
                    "rate_limit_risk_variants": sum(row.get("rate_classification") == "RATE_LIMIT_RISK" for row in group),
                    "status": "OBSERVED" if group else "DATA_INSUFFICIENT",
                }
            )
    _write_csv(out_dir / "deadband_comparison.csv", deadband_rows)
    _write_csv(out_dir / "residency_comparison.csv", residency_rows)

    recommendations = []
    for asset in assets:
        rec_rows = []
        for row in per_asset_rows[asset]:
            if row["fill_model"] != "CONSERVATIVE":
                continue
            rec_rows.append(row)
        recommendations.append(_recommendation(asset, rec_rows, sum(str(trade.get("asset", "")).upper() == asset and trade.get("source") == "derive" for trade in merged["trades"])))
    _write_csv(out_dir / "asset_recommendations.csv", recommendations)

    audit = _read_json(audit_path)
    evidence = {
        "decisions": len(merged["decisions"]),
        "rollups": len(merged["rollups"]),
        "derive_trades": sum(str(row.get("source")) == "derive" for row in merged["trades"]),
        "observed_fills": len(merged["fills"]),
        "observed_markouts": len(merged["markouts"]),
        "assets": {asset: len(observations.get(asset, [])) for asset in assets},
    }
    sufficient_assets = [row["decision"] not in {"INSUFFICIENT_SAMPLE"} for row in recommendations]
    run_status = str(state.get("status") or "UNKNOWN")
    classification = "REFRESH_RESEARCH_DATA_INSUFFICIENT" if not all(sufficient_assets) else "REFRESH_RESEARCH_COMPLETE_PENDING_REVIEW"
    final = {
        "research_version": "refresh-deadband-v1",
        "status": "IN_PROGRESS" if run_status == "RUNNING" else classification,
        "classification": classification,
        "run_status": run_status,
        "observed_at_utc": _iso(time.time()),
        "analysis_start_utc": _iso(start),
        "analysis_end_utc": _iso(end),
        "duration_seconds": duration,
        "active_universe": assets,
        "historical_assets_excluded_from_this_phase": ["DOGE", "ADA"],
        "reference_path": ["binance", "bybit", "okx", "pause"],
        "bitget_enabled": False,
        "derive_execution_only": True,
        "mainnet_armed": False,
        "dry_run": True,
        "live_auto_apply": False,
        "rate_limit_audit": {
            "classification": audit.get("classification", config.rate_limit_status),
            "verified": audit.get("verified", False),
            "internal_budget": audit.get("internal_budget", {}),
        },
        "grid": {
            "deadband_bps": [str(value) for value in DEADBAND_GRID_BPS],
            "minimum_residency_seconds": [str(value) for value in RESIDENCY_GRID_SECONDS],
            "churn_missed_fill_lookbacks_ms": list(CHURN_LOOKBACKS_MS),
            "control": PRIMARY_CONTROL,
            "fill_models": list(FILL_MODELS),
        },
        "evidence_denominators": evidence,
        "evidence_rule": EVIDENCE_RULE,
        "recommendations": recommendations,
        "source_telemetry": [str(path) for path in paths],
        "source_state": str(state_path) if state_path else None,
        "source_mapping": str(mapping_path) if mapping_path else None,
        "limitations": [
            "Candidate fills and markouts are hypothetical; observed Derive public trades are not proof of fills.",
            "Strict conservative crossings require correct aggressor and strict trade-through; touch events remain separate.",
            "Queue residency is a same-price observation proxy, not actual exchange queue position.",
            "Gaps larger than five seconds are not forward-filled for residency or markout sampling.",
            "No live parameter auto-apply or order execution is performed.",
        ],
    }
    _write_json(out_dir / "final_refresh_research.json", final)
    md_lines = [
        "# ZEC / XRP / LINK refresh-deadband research",
        "",
        f"- Status: `{final['status']}`",
        f"- Classification: `{final['classification']}`",
        f"- Run status: `{run_status}`",
        f"- Analysis window: `{_iso(start)}` to `{_iso(end)}` ({duration:.3f}s observed)",
        f"- Active universe: `{', '.join(assets)}`",
        "- Historical DOGE/ADA artifacts are preserved and excluded from this active phase.",
        "- Reference path: `Binance -> Bybit -> OKX -> Pause`; Bitget is disabled.",
        "- Execution: Derive-only shadow; `dry_run=true`, `mainnet_armed=false`, no live orders.",
        "",
        "## Rate-limit gate",
        "",
        f"- Classification: `{final['rate_limit_audit']['classification']}`",
        f"- Verified: `{final['rate_limit_audit']['verified']}`",
        "- Internal budget: 1 action/s, 30 actions/min, 1 action/s/instrument; target utilization 50%; emergency cancellation reserve 6/min.",
        "",
        "## Grid",
        "",
        "The same retained causal observations are replayed over deadbands `0, 2, 5, 10, 15, 20, 30 bps` and minimum normal residencies `0, 0.5, 1, 2, 3, 5s`. Conservative and touch-sensitive fills are separate sensitivity rows.",
        "",
        "## Per-asset decisions",
        "",
        "| Asset | Decision | Recommendation | Trades | Conservative fills | 30s markouts | 60s markouts |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for row in recommendations:
        md_lines.append(
            f"| {row['asset']} | {row['decision']} | {row['recommendation']} | {row['trade_count_denominator']} | {row['conservative_fill_denominator']} | {row['markout_30s_denominator']} | {row['markout_60s_denominator']} |"
        )
    md_lines.extend(
        [
            "",
            "## Evidence and limitations",
            "",
            f"- Denominators: `{json.dumps(evidence, sort_keys=True)}`",
            f"- Evidence rule: {EVIDENCE_RULE}.",
            *[f"- {item}" for item in final["limitations"]],
            "",
            "The output stops at research classification. It does not select a production parameter or authorize live execution.",
            "",
        ]
    )
    (out_dir / "final_refresh_research.md").write_text("\n".join(md_lines), encoding="utf-8")
    return final


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--telemetry", required=True, action="append")
    parser.add_argument("--out-dir", default="reports/zec_xrp_link_refresh_research")
    parser.add_argument("--state")
    parser.add_argument("--mapping")
    args = parser.parse_args(argv)
    config = RuntimeConfig.from_yaml(args.config)
    result = analyze_refresh_research(
        config,
        args.telemetry,
        args.out_dir,
        state_path=args.state,
        mapping_path=args.mapping,
    )
    print(json.dumps({"status": result["status"], "classification": result["classification"], "out_dir": str(Path(args.out_dir).resolve())}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
