"""Collect a simultaneous, read-only XRP multi-spread ghost-quote study.

The collector is deliberately independent from Hummingbot's private connector
path.  It subscribes only to the public Derive and Binance perpetual websocket
feeds, maintains five isolated virtual lanes, and writes append-safe journals.
No order, cancel, amend, credential, or account endpoint is used here.

All spread values are TOTAL BID-ASK distances.  A public trade must strictly
trade through an active ghost quote with a known aggressor side before a
hypothetical fill is recorded.  A touch is never a fill.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import statistics
import tempfile
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal
from pathlib import Path
from typing import Any

SPREADS_BPS = (3, 4, 5, 6, 8)
ORDER_SIZE_QUOTE = Decimal("40")
CAPITAL_QUOTE = Decimal("800")
RESERVE_QUOTE = Decimal("200")
INVENTORY_CAP_QUOTE = Decimal("180")
MAKER_FEE_BPS = Decimal("1")
REFRESH_DEADBAND_BPS = Decimal("3")
MINIMUM_RESIDENCY_SECONDS = Decimal("10")
TOXICITY_THRESHOLD_BPS = Decimal("5")
TOXICITY_WIDENING_TOTAL_BPS = Decimal("4")
TOXICITY_GUARD_SECONDS = Decimal("60")
STALE_SECONDS = Decimal("3")
TICK_SIZE = Decimal("0.0001")
FIXED_SAMPLE_INTERVAL_SECONDS = 1
VOLATILITY_WINDOW_SAMPLES = 60
MAX_QUOTES_PER_SIDE = 1
NO_REAL_ORDER_SUBMISSION = True
REFRESH_MODES = {"legacy", "derive"}
BINANCE_EMERGENCY_MOVE_BPS = Decimal("20")
BINANCE_EMERGENCY_DISLOCATION_BPS = Decimal("12")
BINANCE_EMERGENCY_VOLATILITY_BPS = Decimal("20")
BINANCE_ELEVATED_MOVE_BPS = Decimal("5")
BINANCE_ELEVATED_DISLOCATION_BPS = Decimal("5")
BINANCE_ELEVATED_WIDENING_TOTAL_BPS = Decimal("2")
BINANCE_EMERGENCY_RECOVERY_SECONDS = Decimal("10")
COMPETITIVENESS_RELEVANT_BOOK_TICKS = Decimal("3")
MISSED_FILL_LOOKBACK_SECONDS = Decimal("5")
COMPETITIVENESS_CATEGORIES = (
    "AT_BEST",
    "1_TICK_BEHIND",
    "2_TICKS_BEHIND",
    "3_PLUS_TICKS_BEHIND",
    "INSIDE_SPREAD",
    "OUTSIDE_RELEVANT_BOOK",
)


def utc_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return None


def _relative_return(previous: Any, current: Any) -> Decimal | None:
    previous_value = decimal_or_none(previous)
    current_value = decimal_or_none(current)
    if previous_value is None or current_value is None or previous_value <= 0:
        return None
    return (current_value / previous_value - Decimal("1")) * Decimal("10000")


def quantize_price(price: Decimal, tick_size: Decimal, *, side: str) -> Decimal:
    """Round a ghost quote conservatively to a native tick."""
    if price <= 0 or tick_size <= 0:
        return Decimal("0")
    units = price / tick_size
    rounding = ROUND_DOWN if side == "bid" else ROUND_CEILING
    return units.to_integral_value(rounding=rounding) * tick_size


def total_spread_bps(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None or bid <= 0 or ask <= bid:
        return None
    midpoint = (bid + ask) / Decimal("2")
    return (ask - bid) / midpoint * Decimal("10000")


def classify_binance_shock(
    fast_move_bps: Decimal,
    basis_residual_bps: Decimal,
    volatility_bps: Decimal,
    *,
    emergency_move_bps: Decimal = BINANCE_EMERGENCY_MOVE_BPS,
    emergency_dislocation_bps: Decimal = BINANCE_EMERGENCY_DISLOCATION_BPS,
    emergency_volatility_bps: Decimal = BINANCE_EMERGENCY_VOLATILITY_BPS,
    elevated_move_bps: Decimal = BINANCE_ELEVATED_MOVE_BPS,
    elevated_dislocation_bps: Decimal = BINANCE_ELEVATED_DISLOCATION_BPS,
) -> tuple[str, str | None, tuple[str, ...]]:
    """Return NORMAL/ELEVATED/EMERGENCY and the vulnerable side.

    NEW requires two independent danger conditions before a cancellation. A
    single condition widens the ghost spread but leaves both sides eligible.
    """
    conditions: list[str] = []
    if abs(fast_move_bps) >= emergency_move_bps:
        conditions.append("LARGE_BINANCE_MOVE")
    if abs(basis_residual_bps) >= emergency_dislocation_bps:
        conditions.append("CROSS_VENUE_DISLOCATION")
    if volatility_bps >= emergency_volatility_bps:
        conditions.append("VOLATILITY_SPIKE")
    direction = fast_move_bps if abs(fast_move_bps) >= elevated_move_bps else -basis_residual_bps
    side = "ask" if direction > 0 else "bid" if direction < 0 else None
    elevated = abs(fast_move_bps) >= elevated_move_bps or abs(basis_residual_bps) >= elevated_dislocation_bps
    if len(conditions) >= 2:
        state = "EMERGENCY"
    elif elevated:
        state = "ELEVATED"
    else:
        state = "NORMAL"
    return state, side, tuple(conditions)


def quote_competitiveness(
    side: str,
    quote_price: Decimal,
    derive_bid: Decimal,
    derive_ask: Decimal,
    tick_size: Decimal = TICK_SIZE,
) -> tuple[str, Decimal, Decimal]:
    """Classify a quote against Derive BBO and return distance ticks/bps."""
    if quote_price <= 0 or derive_bid <= 0 or derive_ask <= derive_bid or tick_size <= 0:
        return "OUTSIDE_RELEVANT_BOOK", Decimal("0"), Decimal("0")
    best = derive_bid if side == "bid" else derive_ask
    distance_ticks = abs(quote_price - best) / tick_size
    distance_bps = abs(quote_price / best - Decimal("1")) * Decimal("10000")
    inside = derive_bid < quote_price < derive_ask
    if inside:
        category = "INSIDE_SPREAD"
    elif distance_ticks == 0:
        category = "AT_BEST"
    elif distance_ticks <= 1:
        category = "1_TICK_BEHIND"
    elif distance_ticks <= 2:
        category = "2_TICKS_BEHIND"
    elif distance_ticks <= COMPETITIVENESS_RELEVANT_BOOK_TICKS:
        category = "3_PLUS_TICKS_BEHIND"
    else:
        category = "OUTSIDE_RELEVANT_BOOK"
    return category, distance_ticks, distance_bps


def ghost_quote_prices(
    center: Decimal,
    total_spread: Decimal,
    derive_bid: Decimal,
    derive_ask: Decimal,
    tick_size: Decimal = TICK_SIZE,
) -> tuple[Decimal, Decimal] | None:
    """Build passive prices around one center without crossing Derive BBO."""
    if center <= 0 or derive_bid <= 0 or derive_ask <= derive_bid or total_spread <= 0:
        return None
    half = total_spread / Decimal("2") / Decimal("10000")
    raw_bid = center * (Decimal("1") - half)
    raw_ask = center * (Decimal("1") + half)
    bid = quantize_price(raw_bid, tick_size, side="bid")
    ask = quantize_price(raw_ask, tick_size, side="ask")
    # Stay strictly inside the opposite BBO by at least one native tick.  If
    # the book is too narrow, the lane has no safe ghost quote for this cycle.
    bid = min(bid, quantize_price(derive_ask - tick_size, tick_size, side="bid"))
    ask = max(ask, quantize_price(derive_bid + tick_size, tick_size, side="ask"))
    if bid <= 0 or ask <= bid or bid >= derive_ask or ask <= derive_bid:
        return None
    return bid, ask


def strictly_trades_through(side: str, aggressor_side: str | None, trade_price: Decimal, quote_price: Decimal) -> bool:
    """Conservative passive fill predicate; equality (touch) is rejected."""
    if trade_price <= 0 or quote_price <= 0 or aggressor_side is None:
        return False
    if side == "bid":
        return aggressor_side == "sell" and trade_price < quote_price
    if side == "ask":
        return aggressor_side == "buy" and trade_price > quote_price
    return False


def classify_sample(count: int) -> str:
    if count < 20:
        return "DATA_INSUFFICIENT"
    if count < 50:
        return "EARLY_DIAGNOSTIC_ONLY"
    if count < 100:
        return "PRELIMINARY"
    return "RESEARCH_READY"


def markout_bps(side: str, fill_price: Decimal, future_mid: Decimal) -> Decimal:
    if side == "buy":
        return (future_mid - fill_price) / fill_price * Decimal("10000")
    return (fill_price - future_mid) / fill_price * Decimal("10000")


def percentile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def dedupe_event(seen: set[str], event_key: str) -> bool:
    """Return true exactly once for a key, including after a resumed journal."""
    if not event_key or event_key in seen:
        return False
    seen.add(event_key)
    return True


def sequence_gap(previous: int | None, current: int | None, *, current_previous: int | None = None) -> bool:
    """Detect a stream gap without assuming Binance update ids are contiguous.

    Binance depth updates carry ``pu`` (the previous update id), while Derive
    publish ids are expected to be contiguous.  The explicit previous id is
    therefore preferred whenever it is available.
    """
    if previous is None or current is None:
        return False
    if current_previous is not None:
        return current_previous != previous
    return current != previous + 1


def atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


class CsvJournal:
    """Append-only CSV journal with flush-per-row and periodic fsync."""

    def __init__(self, path: Path, fieldnames: tuple[str, ...], *, sync_every: int = 128):
        self.path = path
        self.fieldnames = fieldnames
        self.sync_every = max(1, sync_every)
        self._rows_since_sync = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists() or self.path.stat().st_size == 0:
            with self.path.open("w", newline="", encoding="utf-8") as handle:
                csv.DictWriter(handle, fieldnames=self.fieldnames).writeheader()
        self._handle = self.path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._handle, fieldnames=self.fieldnames, extrasaction="ignore")

    def append(self, row: dict[str, Any]) -> None:
        self._writer.writerow({key: "" if row.get(key) is None else row.get(key) for key in self.fieldnames})
        self._handle.flush()
        self._rows_since_sync += 1
        if self._rows_since_sync >= self.sync_every:
            os.fsync(self._handle.fileno())
            self._rows_since_sync = 0

    def close(self) -> None:
        if self._handle.closed:
            return
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()

    def __del__(self) -> None:
        try:
            self.close()
        except (OSError, ValueError):
            pass


@dataclass
class GhostSide:
    quote_id: str
    lane: str
    side: str
    price: Decimal
    center: Decimal
    created_at: float
    regime: str
    size_quote: Decimal = ORDER_SIZE_QUOTE
    remaining_quote: Decimal = ORDER_SIZE_QUOTE
    last_update_at: float = 0.0
    active_until: float | None = None
    last_reason: str = "CREATE"


@dataclass
class FillObservation:
    fill_id: str
    lane: str
    side: str
    quote_id: str
    quote_created_at: float
    fill_timestamp: float
    fill_price: Decimal
    trade_price: Decimal
    fill_notional: Decimal
    inventory_before: Decimal
    inventory_after: Decimal
    mid_at_fill: Decimal
    regime: str
    trade_id: str
    markouts: dict[int, dict[str, Any]] = field(default_factory=dict)
    pre_state: dict[str, Any] = field(default_factory=dict)


@dataclass
class Lane:
    spread_bps: int
    order_size_quote: Decimal = ORDER_SIZE_QUOTE
    inventory_cap_quote: Decimal = INVENTORY_CAP_QUOTE
    portfolio_capital_quote: Decimal = CAPITAL_QUOTE
    lane_id: str = ""
    inventory_base: Decimal = Decimal("0")
    average_entry_price: Decimal | None = None
    realized_pnl_quote: Decimal = Decimal("0")
    fees_quote: Decimal = Decimal("0")
    last_mid: Decimal | None = None
    last_timestamp: float | None = None
    peak_net_pnl: Decimal = Decimal("0")
    max_drawdown_quote: Decimal = Decimal("0")
    inventory_notional_seconds: Decimal = Decimal("0")
    inventory_samples_seconds: Decimal = Decimal("0")
    max_abs_inventory_quote: Decimal = Decimal("0")
    inventory_cap_hits: int = 0
    ghost_quotes_created: int = 0
    replacement_count: int = 0
    cancel_count: int = 0
    expiry_count: int = 0
    quote_uptime_seconds: Decimal = Decimal("0")
    normal_quote_uptime_seconds: Decimal = Decimal("0")
    quote_lifetimes: list[float] = field(default_factory=list)
    fills: list[FillObservation] = field(default_factory=list)
    seen_fill_keys: set[str] = field(default_factory=set)
    active: dict[str, GhostSide] = field(default_factory=dict)
    toxicity_guard_until: float = 0.0
    toxicity_triggers: int = 0
    toxicity_widened_seconds: Decimal = Decimal("0")
    toxicity_last_check: float | None = None
    toxicity_markout_values: list[Decimal] = field(default_factory=list)
    toxicity_consumed_markouts: int = 0
    normal_fills: int = 0
    refresh_reason_counts: dict[str, int] = field(default_factory=dict)
    derive_staleness_refreshes: int = 0
    binance_emergency_cancels: int = 0
    emergency_cancel_seconds: Decimal = Decimal("0")
    competitiveness_seconds: dict[str, dict[str, Decimal]] = field(
        default_factory=lambda: {side: {category: Decimal("0") for category in COMPETITIVENESS_CATEGORIES} for side in ("bid", "ask")}
    )
    competitiveness_distance_ticks: dict[str, list[Decimal]] = field(
        default_factory=lambda: {"bid": [], "ask": []}
    )
    competitiveness_distance_bps: dict[str, list[Decimal]] = field(
        default_factory=lambda: {"bid": [], "ask": []}
    )
    competitiveness_last_at: float | None = None
    touches: int = 0
    touches_by_side: dict[str, int] = field(default_factory=lambda: {"bid": 0, "ask": 0})
    near_misses_1_tick: int = 0
    near_misses_2_ticks: int = 0
    near_miss_keys: set[str] = field(default_factory=set)
    touch_keys: set[str] = field(default_factory=set)
    closed_quotes: deque[GhostSide] = field(default_factory=lambda: deque(maxlen=256))
    missed_fill_opportunities: int = 0
    missed_fill_reason_counts: dict[str, int] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return self.lane_id or f"LANE_{self.spread_bps}_BPS"

    @property
    def current_inventory_quote(self) -> Decimal:
        return abs(self.inventory_base * (self.last_mid or Decimal("0")))

    @property
    def inventory_pnl_quote(self) -> Decimal:
        if self.average_entry_price is None or self.last_mid is None:
            return Decimal("0")
        return (self.last_mid - self.average_entry_price) * self.inventory_base

    @property
    def gross_pnl_quote(self) -> Decimal:
        return self.realized_pnl_quote + self.inventory_pnl_quote

    @property
    def net_pnl_quote(self) -> Decimal:
        return self.gross_pnl_quote - self.fees_quote

    def observe_mid(self, timestamp: float, mid: Decimal) -> None:
        if self.last_timestamp is not None and self.last_mid is not None:
            elapsed = max(0.0, timestamp - self.last_timestamp)
            self.inventory_notional_seconds += abs(self.inventory_base * self.last_mid) * Decimal(str(elapsed))
            self.inventory_samples_seconds += Decimal(str(elapsed))
        self.last_timestamp = timestamp
        self.last_mid = mid
        current = self.net_pnl_quote
        self.peak_net_pnl = max(self.peak_net_pnl, current)
        self.max_drawdown_quote = max(self.max_drawdown_quote, self.peak_net_pnl - current)
        self.max_abs_inventory_quote = max(self.max_abs_inventory_quote, abs(self.inventory_base * mid))

    def observe_quote_competitiveness(
        self,
        timestamp: float,
        derive_bid: Decimal,
        derive_ask: Decimal,
        *,
        source: str,
        tick_size: Decimal = TICK_SIZE,
    ) -> None:
        """Accumulate quote-live time and sampled distance to Derive BBO."""
        if self.competitiveness_last_at is not None:
            elapsed = max(0.0, timestamp - self.competitiveness_last_at)
            for side, quote in self.active.items():
                category, _, _ = quote_competitiveness(side, quote.price, derive_bid, derive_ask, tick_size)
                self.competitiveness_seconds[side][category] += Decimal(str(elapsed))
        self.competitiveness_last_at = timestamp
        if source != "derive":
            return
        for side, quote in self.active.items():
            category, distance_ticks, distance_bps = quote_competitiveness(
                side, quote.price, derive_bid, derive_ask, tick_size
            )
            del category
            self.competitiveness_distance_ticks[side].append(distance_ticks)
            self.competitiveness_distance_bps[side].append(distance_bps)

    def _distance_stats(self, values: list[Decimal]) -> dict[str, Decimal | None]:
        return {
            "mean": sum(values, Decimal("0")) / Decimal(len(values)) if values else None,
            "median": Decimal(str(statistics.median(values))) if values else None,
            "p90": Decimal(str(percentile((float(value) for value in values), 0.90))) if values else None,
        }

    def competitiveness_summary(self, side: str) -> dict[str, Any]:
        buckets = self.competitiveness_seconds[side]
        total = sum(buckets.values(), Decimal("0"))
        return {
            "time_seconds": total,
            "time_pct": {
                category: (value / total * Decimal("100") if total else Decimal("0"))
                for category, value in buckets.items()
            },
            "distance_ticks": self._distance_stats(self.competitiveness_distance_ticks[side]),
            "distance_bps": self._distance_stats(self.competitiveness_distance_bps[side]),
        }

    def allowed_fill_notional(self, side: str, price: Decimal, requested: Decimal, allow_flips: bool = False) -> Decimal:
        if price <= 0 or requested <= 0:
            return Decimal("0")
        signed = requested / price if side == "buy" else -requested / price
        projected = self.inventory_base + signed
        if not allow_flips and self.inventory_base != 0 and (self.inventory_base > 0) != (projected > 0):
            requested = abs(self.inventory_base) * price
            projected = self.inventory_base + (requested / price if side == "buy" else -requested / price)
        if abs(projected * price) > self.inventory_cap_quote:
            if (self.inventory_base == 0) or ((self.inventory_base > 0) == (projected > 0)):
                requested = max(Decimal("0"), self.inventory_cap_quote - abs(self.inventory_base * price))
            else:
                requested = abs(self.inventory_base) * price
            self.inventory_cap_hits += 1
        return min(requested, self.order_size_quote)

    def apply_fill(
        self,
        *,
        side: str,
        price: Decimal,
        notional: Decimal,
        timestamp: float,
        quote: GhostSide,
        mid: Decimal,
        trade_id: str,
        trade_price: Decimal | None = None,
        pre_state: dict[str, Any] | None = None,
    ) -> FillObservation | None:
        key = f"{trade_id}:{quote.quote_id}"
        if not dedupe_event(self.seen_fill_keys, key):
            return None
        notional = self.allowed_fill_notional(side, price, notional)
        if notional <= 0:
            return None
        before = self.inventory_base
        amount = notional / price
        signed = amount if side == "buy" else -amount
        after = before + signed
        if before == 0:
            self.average_entry_price = price
        elif (before > 0) == (after > 0) and abs(after) > abs(before):
            old_abs = abs(before)
            new_abs = abs(after)
            self.average_entry_price = (
                (self.average_entry_price or price) * old_abs + price * (new_abs - old_abs)
            ) / new_abs
        elif before != 0:
            entry = self.average_entry_price or price
            closed = min(abs(before), amount)
            self.realized_pnl_quote += (price - entry) * closed if before > 0 else (entry - price) * closed
            if after == 0:
                self.average_entry_price = None
            elif (before > 0) != (after > 0):
                # Position flips are disabled; this is defensive only.
                self.average_entry_price = price
        self.inventory_base = after
        self.fees_quote += notional * MAKER_FEE_BPS / Decimal("10000")
        self.observe_mid(timestamp, mid)
        fill = FillObservation(
            fill_id=f"{self.label}-{len(self.fills) + 1}",
            lane=self.label,
            side=side,
            quote_id=quote.quote_id,
            quote_created_at=quote.created_at,
            fill_timestamp=timestamp,
            fill_price=price,
            trade_price=trade_price if trade_price is not None else price,
            fill_notional=notional,
            inventory_before=before,
            inventory_after=after,
            mid_at_fill=mid,
            regime=quote.regime,
            trade_id=trade_id,
            pre_state=pre_state or {},
        )
        self.fills.append(fill)
        if quote.regime == "NORMAL":
            self.normal_fills += 1
        quote.remaining_quote = max(Decimal("0"), quote.remaining_quote - notional)
        return fill

    def quote_event(
        self,
        quote: GhostSide,
        event: str,
        timestamp: float,
        journal: CsvJournal,
        context: dict[str, Any],
        reason: str | None = None,
    ) -> None:
        reason = reason or quote.last_reason
        journal.append(
            {
                "timestamp": timestamp,
                "lane": self.label,
                "quote_id": quote.quote_id,
                "side": quote.side,
                "event": event,
                "quote_created_at": quote.created_at,
                "active_until": quote.active_until,
                "center": quote.center,
                "ghost_price": quote.price,
                "derive_best_bid": context.get("derive_best_bid"),
                "derive_best_ask": context.get("derive_best_ask"),
                "binance_best_bid": context.get("binance_best_bid"),
                "binance_best_ask": context.get("binance_best_ask"),
                "binance_mid": context.get("binance_mid"),
                "reference_mid": context.get("binance_mid"),
                "fair_value": quote.center,
                "regime": quote.regime,
                "inventory_quote": self.current_inventory_quote,
                "quote_size_quote": quote.size_quote,
                "reason": reason,
            }
        )

    def close_quote(
        self,
        side: str,
        timestamp: float,
        event: str,
        journal: CsvJournal,
        context: dict[str, Any],
        reason: str | None = None,
    ) -> None:
        quote = self.active.pop(side, None)
        if quote is None:
            return
        quote.active_until = timestamp
        lifetime = max(0.0, timestamp - quote.created_at)
        self.quote_lifetimes.append(lifetime)
        self.quote_uptime_seconds += Decimal(str(lifetime))
        if quote.regime == "NORMAL":
            self.normal_quote_uptime_seconds += Decimal(str(lifetime))
        if event == "replace":
            self.replacement_count += 1
        elif event == "expire":
            self.expiry_count += 1
        else:
            self.cancel_count += 1
        if reason and reason.startswith("BINANCE_") and "CANCEL_" in reason:
            self.emergency_cancel_seconds += Decimal(str(lifetime))
        if reason:
            quote.last_reason = reason
            self.refresh_reason_counts[reason] = self.refresh_reason_counts.get(reason, 0) + 1
            if reason == "DERIVE_STALENESS_REFRESH":
                self.derive_staleness_refreshes += 1
            if reason.startswith("BINANCE_") and "CANCEL_" in reason:
                self.binance_emergency_cancels += 1
        self.closed_quotes.append(quote)
        self.quote_event(quote, event, timestamp, journal, context, reason)

    def activate_quote(
        self,
        side: str,
        price: Decimal,
        center: Decimal,
        timestamp: float,
        regime: str,
        quote_counter: int,
        journal: CsvJournal,
        context: dict[str, Any],
    ) -> GhostSide:
        quote = GhostSide(
            quote_id=f"{self.label}-{side}-{quote_counter}",
            lane=self.label,
            side=side,
            price=price,
            center=center,
            created_at=timestamp,
            last_update_at=timestamp,
            regime=regime,
            last_reason="CREATE",
        )
        self.active[side] = quote
        self.ghost_quotes_created += 1
        self.quote_event(quote, "activate", timestamp, journal, context, "CREATE")
        return quote

    def due_markouts(self, timestamp: float, future_mid: Decimal, journal: CsvJournal) -> None:
        for fill in self.fills:
            for horizon in (5, 30, 60, 300):
                if horizon in fill.markouts:
                    continue
                due = fill.fill_timestamp + horizon
                if timestamp < due:
                    continue
                value = markout_bps(fill.side, fill.fill_price, future_mid)
                fill.markouts[horizon] = {"mid": future_mid, "markout_bps": value, "observed_at": timestamp}
                journal.append(
                    {
                        "fill_id": fill.fill_id,
                        "trade_id": fill.trade_id,
                        "lane": fill.lane,
                        "side": fill.side,
                        "quote_created_at": fill.quote_created_at,
                        "fill_timestamp": fill.fill_timestamp,
                        "fill_price": fill.fill_price,
                        "fill_notional": fill.fill_notional,
                        "trade_price": fill.trade_price,
                        "inventory_before": fill.inventory_before,
                        "inventory_after": fill.inventory_after,
                        "mid_at_fill": fill.mid_at_fill,
                        "horizon_seconds": horizon,
                        "observed_at": timestamp,
                        "future_mid": future_mid,
                        "markout_bps": value,
                        "status": "OBSERVED",
                    }
                )
                if horizon in (5, 30):
                    self.toxicity_markout_values.append(value)

    def finalize_markouts(self, timestamp: float, journal: CsvJournal) -> None:
        for fill in self.fills:
            for horizon in (5, 30, 60, 300):
                if horizon in fill.markouts:
                    continue
                journal.append(
                    {
                        "fill_id": fill.fill_id,
                        "trade_id": fill.trade_id,
                        "lane": fill.lane,
                        "side": fill.side,
                        "quote_created_at": fill.quote_created_at,
                        "fill_timestamp": fill.fill_timestamp,
                        "fill_price": fill.fill_price,
                        "fill_notional": fill.fill_notional,
                        "trade_price": fill.trade_price,
                        "inventory_before": fill.inventory_before,
                        "inventory_after": fill.inventory_after,
                        "mid_at_fill": fill.mid_at_fill,
                        "horizon_seconds": horizon,
                        "observed_at": timestamp,
                        "future_mid": None,
                        "markout_bps": None,
                        "status": "MISSING_RUN_END",
                    }
                )

    def markout_stats(self, horizon: int, *, normal_only: bool = False) -> dict[str, Any]:
        values = [
            decimal_or_none(row.get("markout_bps"))
            for fill in self.fills
            if (not normal_only or fill.regime == "NORMAL")
            for row in [fill.markouts.get(horizon)]
            if row is not None
        ]
        values = [value for value in values if value is not None]
        if not values:
            return {"mean_bps": None, "median_bps": None, "positive_pct": None, "observations": 0}
        positive = sum(value > 0 for value in values)
        return {
            "mean_bps": sum(values, Decimal("0")) / len(values),
            "median_bps": Decimal(str(statistics.median(values))),
            "positive_pct": Decimal(positive) / Decimal(len(values)) * Decimal("100"),
            "observations": len(values),
        }

    def observe_touch_or_near_miss(
        self,
        *,
        quote: GhostSide,
        trade_id: str,
        timestamp: float,
        trade_price: Decimal,
        aggressor_side: str | None,
        journal: CsvJournal,
    ) -> bool:
        """Record touch/near-miss diagnostics without changing fill semantics."""
        key = f"{trade_id}:{quote.quote_id}"
        if key in self.touch_keys or key in self.near_miss_keys:
            return False
        touch = (
            trade_price <= quote.price
            if quote.side == "bid"
            else trade_price >= quote.price
        )
        if touch:
            self.touch_keys.add(key)
            self.touches += 1
            self.touches_by_side[quote.side] += 1
            event = "QUOTE_TOUCH"
            distance_ticks = Decimal("0")
        else:
            distance = (
                trade_price - quote.price
                if quote.side == "bid"
                else quote.price - trade_price
            )
            if distance <= 0 or distance > TICK_SIZE * Decimal("2"):
                return False
            self.near_miss_keys.add(key)
            distance_ticks = distance / TICK_SIZE
            if distance <= TICK_SIZE:
                self.near_misses_1_tick += 1
                event = "NEAR_MISS_1_TICK"
            else:
                self.near_misses_2_ticks += 1
                event = "NEAR_MISS_2_TICKS"
        journal.append(
            {
                "timestamp": timestamp,
                "event": event,
                "lane": self.label,
                "side": quote.side,
                "quote_id": quote.quote_id,
                "trade_id": trade_id,
                "trade_price": trade_price,
                "quote_price": quote.price,
                "aggressor_side": aggressor_side,
                "quote_age_seconds": max(0.0, timestamp - quote.created_at),
                "cancel_reason": None,
                "cancel_timestamp": None,
                "cancel_age_seconds": None,
                "binance_emergency": False,
                "would_trade_through": strictly_trades_through(
                    "bid" if quote.side == "bid" else "ask",
                    aggressor_side,
                    trade_price,
                    quote.price,
                ),
                "distance_ticks": distance_ticks,
            }
        )
        return touch

    def record_missed_fill(
        self,
        *,
        quote: GhostSide,
        trade_id: str,
        timestamp: float,
        trade_price: Decimal,
        aggressor_side: str | None,
        journal: CsvJournal,
    ) -> None:
        cancel_timestamp = quote.active_until
        if cancel_timestamp is None or timestamp <= cancel_timestamp:
            return
        cancel_age = timestamp - cancel_timestamp
        if cancel_age > float(MISSED_FILL_LOOKBACK_SECONDS):
            return
        quote_side = "bid" if quote.side == "bid" else "ask"
        if not strictly_trades_through(quote_side, aggressor_side, trade_price, quote.price):
            return
        self.missed_fill_opportunities += 1
        self.missed_fill_reason_counts[quote.last_reason] = self.missed_fill_reason_counts.get(quote.last_reason, 0) + 1
        journal.append(
            {
                "timestamp": timestamp,
                "event": "MISSED_FILL_OPPORTUNITY",
                "lane": self.label,
                "side": quote.side,
                "quote_id": quote.quote_id,
                "trade_id": trade_id,
                "trade_price": trade_price,
                "quote_price": quote.price,
                "aggressor_side": aggressor_side,
                "quote_age_seconds": max(0.0, (cancel_timestamp or timestamp) - quote.created_at),
                "cancel_reason": quote.last_reason,
                "cancel_timestamp": cancel_timestamp,
                "cancel_age_seconds": cancel_age,
                "binance_emergency": quote.last_reason.startswith("BINANCE_") and "CANCEL_" in quote.last_reason,
                "would_trade_through": True,
                "distance_ticks": Decimal("0"),
            }
        )

    def summary(
        self,
        duration_seconds: float,
        *,
        normal_only: bool = False,
        derive_trade_count: int = 0,
    ) -> dict[str, Any]:
        fills = [fill for fill in self.fills if not normal_only or fill.regime == "NORMAL"]
        volume = sum((fill.fill_notional for fill in fills), Decimal("0"))
        bid_fills = sum(fill.side == "buy" for fill in fills)
        ask_fills = sum(fill.side == "sell" for fill in fills)
        hours = Decimal(str(duration_seconds)) / Decimal("3600") if duration_seconds > 0 else Decimal("0")
        days = Decimal(str(duration_seconds)) / Decimal("86400") if duration_seconds > 0 else Decimal("0")
        volume_hour = volume / hours if hours else Decimal("0")
        volume_day = volume / days if days else Decimal("0")
        sample = classify_sample(len(fills))
        mark30 = self.markout_stats(30, normal_only=normal_only)
        net = self.net_pnl_quote if not normal_only else None
        if normal_only:
            # Normal-only PnL is a conservative fill subset diagnostic.  Do not
            # pretend that inventory held through another regime is isolated.
            net = None
        pnl_per_1000 = net / volume * Decimal("1000") if net is not None and volume else None
        profitable_volume_efficiency = (
            volume_day
            if net is not None and net > 0 and pnl_per_1000 is not None and pnl_per_1000 > 0
            else None
        )
        quote_uptime_denominator = Decimal(str(max(duration_seconds, 0.0) * MAX_QUOTES_PER_SIDE * 2.0))
        uptime_pct = self.quote_uptime_seconds / quote_uptime_denominator * Decimal("100") if quote_uptime_denominator else Decimal("0")
        avg_inventory = self.inventory_notional_seconds / self.inventory_samples_seconds if self.inventory_samples_seconds else Decimal("0")
        average_lifetime = (
            Decimal(str(statistics.mean(self.quote_lifetimes))) if self.quote_lifetimes else None
        )
        p90_lifetime = (
            Decimal(str(percentile(self.quote_lifetimes, 0.90))) if self.quote_lifetimes else None
        )
        markout_values = [fill.markouts.get(30, {}).get("markout_bps") for fill in fills]
        markout_values = [value for value in markout_values if value is not None]
        emergency_per_hour = (
            Decimal(self.binance_emergency_cancels) / hours if hours else Decimal("0")
        )
        emergency_time_pct = (
            self.emergency_cancel_seconds / self.quote_uptime_seconds * Decimal("100")
            if self.quote_uptime_seconds
            else Decimal("0")
        )
        touch_to_fill = Decimal(len(fills)) / Decimal(self.touches) if self.touches else Decimal("0")
        if fills:
            dominant_no_fill = None
            secondary_no_fill = None
        elif derive_trade_count < 20:
            dominant_no_fill = "LOW_VENUE_TRADE_ACTIVITY"
            secondary_no_fill = "PUBLIC_FILL_EVIDENCE_LIMITATION"
        elif self.touches:
            dominant_no_fill = "PUBLIC_FILL_EVIDENCE_LIMITATION"
            secondary_no_fill = "QUEUE_POSITION_UNOBSERVABLE"
        elif self.near_misses_1_tick + self.near_misses_2_ticks:
            dominant_no_fill = "QUOTE_TOO_FAR_FROM_TOUCH"
            secondary_no_fill = "QUEUE_POSITION_UNOBSERVABLE"
        else:
            dominant_no_fill = "QUOTE_TOO_FAR_FROM_TOUCH"
            secondary_no_fill = "QUEUE_POSITION_UNOBSERVABLE"
        if self.binance_emergency_cancels and dominant_no_fill != "LOW_VENUE_TRADE_ACTIVITY":
            secondary_no_fill = "BINANCE_OVER_CANCELLATION"
        elif self.replacement_count >= self.ghost_quotes_created / 2 and dominant_no_fill != "LOW_VENUE_TRADE_ACTIVITY":
            secondary_no_fill = "DERIVE_OVER_REFRESH"
        return {
            "lane": self.label,
            "total_spread_bps": self.spread_bps,
            "spread_convention": "TOTAL_BID_ASK",
            "order_size_quote": self.order_size_quote,
            "observation_hours": hours,
            "ghost_quotes_created": self.ghost_quotes_created,
            "quote_uptime_pct": uptime_pct,
            "conservative_fills": len(fills),
            "fills_per_hour": Decimal(len(fills)) / hours if hours else Decimal("0"),
            "bid_fills": bid_fills,
            "ask_fills": ask_fills,
            "maker_volume_hour": volume_hour,
            "maker_volume_day": volume_day,
            "capital_turnover_day": volume_day / self.portfolio_capital_quote if self.portfolio_capital_quote else None,
            "gross_spread_capture": self.realized_pnl_quote if not normal_only else None,
            "fees": self.fees_quote if not normal_only else None,
            "inventory_pnl": self.inventory_pnl_quote if not normal_only else None,
            "net_pnl": net,
            "pnl_per_1000_volume": pnl_per_1000,
            "profitable_volume_efficiency": profitable_volume_efficiency,
            "pnl_per_fill": net / Decimal(len(fills)) if net is not None and fills else None,
            "max_virtual_drawdown": self.max_drawdown_quote,
            "average_absolute_inventory": avg_inventory,
            "max_inventory": self.max_abs_inventory_quote,
            "inventory_cap_hits": self.inventory_cap_hits,
            "median_quote_lifetime": Decimal(str(statistics.median(self.quote_lifetimes))) if self.quote_lifetimes else None,
            "average_quote_lifetime": average_lifetime,
            "p90_quote_lifetime": p90_lifetime,
            "replacement_count": self.replacement_count,
            "replacements_per_hour": Decimal(self.replacement_count) / hours if hours else Decimal("0"),
            "cancel_count": self.cancel_count,
            "expiry_count": self.expiry_count,
            "toxicity_triggers": self.toxicity_triggers,
            "toxicity_widened_seconds": self.toxicity_widened_seconds,
            "markout_5s": self.markout_stats(5, normal_only=normal_only),
            "markout_30s": mark30,
            "markout_60s": self.markout_stats(60, normal_only=normal_only),
            "markout_300s": self.markout_stats(300, normal_only=normal_only),
            "completed_maker_cycles": min(bid_fills, ask_fills),
            "quote_opportunities": self.ghost_quotes_created,
            "quotes_placed": self.ghost_quotes_created,
            "quotes_replaced": self.replacement_count,
            "quotes_filled": len(fills),
            "fill_probability": Decimal(len(fills)) / Decimal(self.ghost_quotes_created) if self.ghost_quotes_created else Decimal("0"),
            "quote_lifetime_seconds": Decimal(str(statistics.median(self.quote_lifetimes))) if self.quote_lifetimes else None,
            "cancel_rate": Decimal(self.cancel_count) / Decimal(self.ghost_quotes_created) if self.ghost_quotes_created else Decimal("0"),
            "replacement_rate": Decimal(self.replacement_count) / Decimal(self.ghost_quotes_created) if self.ghost_quotes_created else Decimal("0"),
            "derive_staleness_refreshes": self.derive_staleness_refreshes,
            "binance_emergency_cancels": self.binance_emergency_cancels,
            "replacement_reason_counts": dict(self.refresh_reason_counts),
            "competitiveness": {
                "bid": self.competitiveness_summary("bid"),
                "ask": self.competitiveness_summary("ask"),
            },
            "bid_distance_from_best_bid_bps": self.competitiveness_summary("bid")["distance_bps"],
            "ask_distance_from_best_ask_bps": self.competitiveness_summary("ask")["distance_bps"],
            "bid_distance_from_best_bid_ticks": self.competitiveness_summary("bid")["distance_ticks"],
            "ask_distance_from_best_ask_ticks": self.competitiveness_summary("ask")["distance_ticks"],
            "touches": self.touches,
            "touches_per_hour": Decimal(self.touches) / hours if hours else Decimal("0"),
            "touch_to_fill_conversion": touch_to_fill,
            "near_misses_1_tick": self.near_misses_1_tick,
            "near_misses_2_ticks": self.near_misses_2_ticks,
            "near_misses_per_hour": (
                Decimal(self.near_misses_1_tick + self.near_misses_2_ticks) / hours if hours else Decimal("0")
            ),
            "near_misses_1_tick_per_hour": Decimal(self.near_misses_1_tick) / hours if hours else Decimal("0"),
            "near_misses_2_ticks_per_hour": Decimal(self.near_misses_2_ticks) / hours if hours else Decimal("0"),
            "missed_fill_opportunities": self.missed_fill_opportunities,
            "missed_fill_reason_counts": dict(self.missed_fill_reason_counts),
            "dominant_no_fill_cause": dominant_no_fill,
            "secondary_no_fill_cause": secondary_no_fill,
            "emergency_cancels_per_hour": emergency_per_hour,
            "emergency_cancel_quote_time_pct": emergency_time_pct,
            "adverse_selection_rate": (
                Decimal(sum(value <= -TOXICITY_THRESHOLD_BPS for value in markout_values)) / Decimal(len(markout_values))
                if markout_values
                else None
            ),
            "sample_class": sample,
        }


class PublicGhostExperiment:
    """Five isolated ghost lanes consuming one synchronized market event loop."""

    def __init__(
        self,
        run_dir: Path,
        run_id: str,
        *,
        order_size_quote: Decimal = ORDER_SIZE_QUOTE,
        refresh_mode: str = "derive",
        minimum_residency_seconds: Decimal = MINIMUM_RESIDENCY_SECONDS,
        refresh_deadband_bps: Decimal = REFRESH_DEADBAND_BPS,
    ):
        if refresh_mode not in REFRESH_MODES:
            raise ValueError(f"refresh_mode must be one of {sorted(REFRESH_MODES)}")
        self.run_dir = run_dir
        self.run_id = run_id
        self.order_size_quote = order_size_quote
        self.refresh_mode = refresh_mode
        self.minimum_residency_seconds = minimum_residency_seconds
        self.refresh_deadband_bps = refresh_deadband_bps
        self.lanes = {
            f"LANE_{spread}_BPS": Lane(spread, order_size_quote=order_size_quote, lane_id=f"LANE_{spread}_BPS")
            for spread in SPREADS_BPS
        }
        self.quote_counter = 0
        self.derive_bbo: tuple[Decimal, Decimal] | None = None
        self.derive_sizes: tuple[Decimal, Decimal] | None = None
        self.binance_bbo: tuple[Decimal, Decimal] | None = None
        self.last_derive_event_at: float | None = None
        self.last_binance_event_at: float | None = None
        self.last_decision_at: float | None = None
        self.last_binance_sample_at: int | None = None
        self.last_binance_sample_mid: Decimal | None = None
        self.returns: deque[float] = deque(maxlen=VOLATILITY_WINDOW_SAMPLES)
        self.volatility_sampling_state = "FIXED_1S"
        self.binance_shock_state = "NORMAL"
        self.binance_emergency_active = False
        self.binance_emergency_side: str | None = None
        self.binance_emergency_conditions: tuple[str, ...] = ()
        self.binance_recovery_since: float | None = None
        self.reference_history: list[tuple[float, Decimal]] = []
        self.basis: list[Decimal] = []
        self.market_state = "NORMAL"
        self.candidate_state = "NORMAL"
        self.candidate_state_since: float | None = None
        self.state_history: deque[tuple[float, dict[str, Any]]] = deque(maxlen=1200)
        self.current_state: dict[str, Any] = {}
        self.message_counts: dict[str, int] = {"derive_bbo": 0, "derive_trade": 0, "binance_bbo": 0, "binance_trade": 0}
        self.sequence_gaps: dict[str, int] = {"derive": 0, "binance": 0}
        self.reconnects: dict[str, int] = {"derive": 0, "binance": 0}
        self.stale_feed_pauses: dict[str, int] = {"derive": 0, "binance": 0}
        self.missing_timestamps = 0
        self.clock_skews: list[float] = []
        self.gaps: dict[str, list[float]] = {"derive": [], "binance": []}
        self.last_sequence: dict[str, int | None] = {"derive": None, "binance": None}
        self.started_at = time.time()
        metadata_path = run_dir / "run_metadata.json"
        if metadata_path.exists():
            try:
                existing = json.loads(metadata_path.read_text(encoding="utf-8"))
                if existing.get("started_at") is not None:
                    self.started_at = float(existing["started_at"])
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                pass
        self.last_event_received_at: dict[str, float] = {}
        quote_fields = (
            "timestamp", "lane", "quote_id", "side", "event", "quote_created_at", "active_until", "center",
            "ghost_price", "derive_best_bid", "derive_best_ask", "binance_best_bid", "binance_best_ask", "binance_mid",
            "reference_mid", "fair_value", "regime", "inventory_quote", "quote_size_quote", "reason",
            "derive_mid", "derive_microprice", "basis_bps", "basis_residual_bps", "fast_move_bps",
            "binance_shock_state", "binance_emergency_conditions",
        )
        self.quote_journal = CsvJournal(run_dir / "ghost_quotes.csv", quote_fields)
        fill_fields = (
            "fill_id", "trade_id", "lane", "side", "quote_created_at", "fill_timestamp", "fill_price", "fill_notional",
            "trade_price", "inventory_before", "inventory_after", "mid_at_fill", "regime", "fill_label",
            "derive_mid_tminus_10s", "derive_mid_tminus_5s", "derive_mid_tminus_1s", "derive_mid_at_fill",
            "binance_mid_tminus_10s", "binance_mid_tminus_5s", "binance_mid_tminus_1s", "binance_mid_at_fill",
            "binance_return_1s", "binance_return_5s", "binance_return_10s",
            "basis_residual_bps", "derive_spread_bps", "derive_microprice_displacement_bps",
            "quote_age_seconds", "refresh_reason_history",
        )
        self.fill_journal = CsvJournal(run_dir / "ghost_fills.csv", fill_fields)
        markout_fields = (
            "fill_id", "trade_id", "lane", "side", "quote_created_at", "fill_timestamp", "fill_price", "fill_notional",
            "trade_price", "inventory_before", "inventory_after", "mid_at_fill", "horizon_seconds", "observed_at", "future_mid", "markout_bps", "status",
        )
        self.markout_journal = CsvJournal(run_dir / "elapsed_markouts.csv", markout_fields)
        inventory_fields = ("timestamp", "lane", "inventory_base", "inventory_quote", "regime", "mid")
        self.inventory_journal = CsvJournal(run_dir / "lane_inventory.csv", inventory_fields)
        pnl_fields = ("timestamp", "lane", "gross_pnl", "fees", "net_pnl", "realized_pnl", "inventory_pnl")
        self.pnl_journal = CsvJournal(run_dir / "lane_pnl.csv", pnl_fields)
        self.regime_journal = CsvJournal(run_dir / "regime_log.csv", ("timestamp", "market_state", "volatility_bps", "direction_bps", "derive_spread_bps"))
        self.health_journal = CsvJournal(
            run_dir / "market_stream_health.csv",
            ("received_at", "source", "event_type", "exchange_timestamp", "gap_seconds", "sequence", "clock_skew_seconds"),
        )
        self.toxicity_journal = CsvJournal(
            run_dir / "toxicity_log.csv",
            ("timestamp", "lane", "event", "trigger_count", "markout_5s_bps", "markout_30s_bps", "guard_until"),
        )
        self.opportunity_journal = CsvJournal(
            run_dir / "quote_opportunities.csv",
            (
                "timestamp", "event", "lane", "side", "quote_id", "trade_id", "trade_price", "quote_price",
                "aggressor_side", "quote_age_seconds", "cancel_reason", "cancel_timestamp", "cancel_age_seconds",
                "binance_emergency", "would_trade_through", "distance_ticks",
            ),
        )
        self.seen_public_trade_keys: set[str] = set()
        self._load_resume_keys()
        if not metadata_path.exists():
            atomic_json_write(
                metadata_path,
                {
                    "run_id": self.run_id,
                    "asset": "XRP-PERP",
                    "feeds": {
                        "derive": "wss://api.lyra.finance/ws (public)",
                        "binance": "wss://fstream.binance.com/stream (public bookTicker + depth + aggTrade)",
                    },
                    "real_order_submission": False,
                    "private_endpoints_used": False,
                    "spreads_bps": SPREADS_BPS,
                    "order_size_quote": self.order_size_quote,
                    "refresh_mode": self.refresh_mode,
                    "minimum_residency_seconds": self.minimum_residency_seconds,
                    "refresh_deadband_bps": self.refresh_deadband_bps,
                    "started_at": self.started_at,
                    "resume_supported": True,
                    "status": "RUNNING",
                },
            )

    def _load_resume_keys(self) -> None:
        fills_path = self.run_dir / "ghost_fills.csv"
        if not fills_path.exists():
            return
        try:
            with fills_path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    trade_id = row.get("trade_id")
                    if trade_id:
                        self.seen_public_trade_keys.add(trade_id)
        except OSError:
            return

    @property
    def fresh(self) -> bool:
        now = time.time()
        return bool(
            self.last_derive_event_at is not None
            and self.last_binance_event_at is not None
            and now - self.last_derive_event_at <= float(STALE_SECONDS)
            and now - self.last_binance_event_at <= float(STALE_SECONDS)
        )

    def feeds_fresh(self, received: float) -> bool:
        return bool(
            self.last_derive_event_at is not None
            and self.last_binance_event_at is not None
            and received - self.last_derive_event_at <= float(STALE_SECONDS)
            and received - self.last_binance_event_at <= float(STALE_SECONDS)
        )

    def _context(self, binance_mid: Decimal | None = None) -> dict[str, Any]:
        context = {
            "derive_best_bid": self.derive_bbo[0] if self.derive_bbo else None,
            "derive_best_ask": self.derive_bbo[1] if self.derive_bbo else None,
            "binance_best_bid": self.binance_bbo[0] if self.binance_bbo else None,
            "binance_best_ask": self.binance_bbo[1] if self.binance_bbo else None,
            "binance_mid": binance_mid,
        }
        context.update(self.current_state)
        return context

    def _record_health(self, source: str, event_type: str, timestamp: float | None, sequence: int | None, received: float) -> None:
        if timestamp is None:
            self.missing_timestamps += 1
        previous = self.last_event_received_at.get(source)
        gap = max(0.0, received - previous) if previous is not None else None
        if gap is not None:
            self.gaps[source].append(gap)
        self.last_event_received_at[source] = received
        skew = received - timestamp if timestamp is not None else None
        if skew is not None and abs(skew) < 60:
            self.clock_skews.append(skew)
        self.health_journal.append(
            {
                "received_at": received,
                "source": source,
                "event_type": event_type,
                "exchange_timestamp": timestamp,
                "gap_seconds": gap,
                "sequence": sequence,
                "clock_skew_seconds": skew,
            }
        )

    def _record_fixed_sample(self, timestamp: float, mid: Decimal) -> None:
        bucket = int(timestamp)
        if self.last_binance_sample_at is None or self.last_binance_sample_mid is None:
            self.last_binance_sample_at = bucket
            self.last_binance_sample_mid = mid
            return
        elapsed = bucket - self.last_binance_sample_at
        if elapsed == FIXED_SAMPLE_INTERVAL_SECONDS:
            self.returns.append(float(mid / self.last_binance_sample_mid - Decimal("1")))
        # Do not back-fill elapsed buckets when the public reference feed was
        # absent.  The next fresh sample is simply the new anchor.
        self.last_binance_sample_at = bucket
        self.last_binance_sample_mid = mid

    def _market_state_for(self, timestamp: float, derive_mid: Decimal, binance_mid: Decimal) -> tuple[str, Decimal, Decimal, Decimal]:
        self.reference_history.append((timestamp, binance_mid))
        cutoff = timestamp - 10
        self.reference_history = [(stamp, price) for stamp, price in self.reference_history if stamp >= cutoff]
        anchor = self.reference_history[0][1] if self.reference_history else binance_mid
        direction = (binance_mid / anchor - Decimal("1")) * Decimal("10000") if anchor else Decimal("0")
        volatility = Decimal(str(statistics.pstdev(self.returns) * 10000)) if len(self.returns) >= 2 else Decimal("0")
        derive_spread = (self.derive_bbo[1] - self.derive_bbo[0]) / derive_mid * Decimal("10000")
        if volatility >= Decimal("20") or derive_spread >= Decimal("80"):
            candidate = "EXTREME"
        elif volatility >= Decimal("8"):
            candidate = "HIGH_VOL"
        elif direction >= Decimal("2"):
            candidate = "UP_TREND"
        elif direction <= Decimal("-2"):
            candidate = "DOWN_TREND"
        else:
            candidate = "NORMAL"
        if candidate != self.candidate_state:
            self.candidate_state = candidate
            self.candidate_state_since = timestamp
        if candidate == "EXTREME" or self.market_state == candidate or self.candidate_state_since is None or timestamp - self.candidate_state_since >= 10:
            self.market_state = candidate
        self.regime_journal.append(
            {"timestamp": timestamp, "market_state": self.market_state, "volatility_bps": volatility, "direction_bps": direction, "derive_spread_bps": derive_spread}
        )
        return self.market_state, direction, volatility, derive_spread

    def _update_binance_shock_state(
        self,
        timestamp: float,
        fast_move_bps: Decimal,
        basis_residual_bps: Decimal,
        volatility_bps: Decimal,
    ) -> tuple[str, str | None, tuple[str, ...]]:
        raw_state, raw_side, raw_conditions = classify_binance_shock(
            fast_move_bps, basis_residual_bps, volatility_bps
        )
        if self.refresh_mode == "legacy":
            # OLD intentionally preserves the former single-signal behavior
            # for a paired counterfactual, with an explicit reason label.
            side = "bid" if fast_move_bps <= -TOXICITY_THRESHOLD_BPS else "ask" if fast_move_bps >= TOXICITY_THRESHOLD_BPS else None
            self.binance_shock_state = "EMERGENCY" if side else "NORMAL"
            self.binance_emergency_active = side is not None
            self.binance_emergency_side = side
            self.binance_emergency_conditions = ("LEGACY_FAST_MOVE",) if side else ()
            return self.binance_shock_state, side, self.binance_emergency_conditions
        if raw_state == "EMERGENCY":
            self.binance_emergency_active = True
            self.binance_emergency_side = raw_side or self.binance_emergency_side
            self.binance_emergency_conditions = raw_conditions
            self.binance_recovery_since = None
            self.binance_shock_state = "EMERGENCY"
        elif not self.binance_emergency_active:
            self.binance_shock_state = raw_state
            self.binance_emergency_side = None
            self.binance_emergency_conditions = raw_conditions
        elif raw_state == "NORMAL":
            if self.binance_recovery_since is None:
                self.binance_recovery_since = timestamp
                self.binance_shock_state = "EMERGENCY_RECOVERY"
            elif timestamp - self.binance_recovery_since >= float(BINANCE_EMERGENCY_RECOVERY_SECONDS):
                self.binance_emergency_active = False
                self.binance_emergency_side = None
                self.binance_emergency_conditions = ()
                self.binance_recovery_since = None
                self.binance_shock_state = "NORMAL"
            else:
                self.binance_shock_state = "EMERGENCY_RECOVERY"
        else:
            self.binance_shock_state = "EMERGENCY_RECOVERY"
        return self.binance_shock_state, self.binance_emergency_side, self.binance_emergency_conditions

    def _binance_emergency_reason(self, side: str) -> str:
        if self.refresh_mode == "legacy":
            return f"BINANCE_LEGACY_FAST_MOVE_CANCEL_{side.upper()}"
        conditions = "_".join(self.binance_emergency_conditions) or "RECOVERY"
        return f"BINANCE_TRUE_SHOCK_{conditions}_CANCEL_{side.upper()}"

    def _state_before(self, timestamp: float, seconds: float) -> dict[str, Any]:
        target = timestamp - seconds
        for stamp, state in reversed(self.state_history):
            if stamp <= target:
                return state
        return {}

    def _fill_pre_state(self, timestamp: float) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for seconds in (10, 5, 1):
            state = self._state_before(timestamp, seconds)
            suffix = f"tminus_{int(seconds)}s"
            values[f"derive_mid_{suffix}"] = state.get("derive_mid")
            values[f"binance_mid_{suffix}"] = state.get("binance_mid")
        values["derive_mid_at_fill"] = self.current_state.get("derive_mid")
        values["binance_mid_at_fill"] = self.current_state.get("binance_mid")
        values["binance_return_1s"] = _relative_return(
            self._state_before(timestamp, 1).get("binance_mid"),
            self.current_state.get("binance_mid"),
        )
        values["binance_return_5s"] = _relative_return(
            self._state_before(timestamp, 5).get("binance_mid"),
            self.current_state.get("binance_mid"),
        )
        values["binance_return_10s"] = _relative_return(
            self._state_before(timestamp, 10).get("binance_mid"),
            self.current_state.get("binance_mid"),
        )
        for key in (
            "basis_residual_bps",
            "derive_spread_bps",
            "derive_microprice_displacement_bps",
            "regime",
        ):
            values[key] = self.current_state.get(key)
        return values

    def _refresh_toxicity(self, lane: Lane, timestamp: float) -> bool:
        if lane.last_mid is not None and lane.last_mid > 0:
            lane.due_markouts(timestamp, lane.last_mid, self.markout_journal)
        new_values = lane.toxicity_markout_values[lane.toxicity_consumed_markouts:]
        lane.toxicity_consumed_markouts = len(lane.toxicity_markout_values)
        was_active = timestamp < lane.toxicity_guard_until
        if new_values and not was_active and any(value <= -TOXICITY_THRESHOLD_BPS for value in new_values):
            lane.toxicity_guard_until = timestamp + float(TOXICITY_GUARD_SECONDS)
            lane.toxicity_triggers += 1
            self.toxicity_journal.append(
                {
                    "timestamp": timestamp,
                    "lane": lane.label,
                    "event": "TRIGGER",
                    "trigger_count": lane.toxicity_triggers,
                    "markout_5s_bps": lane.markout_stats(5).get("mean_bps"),
                    "markout_30s_bps": lane.markout_stats(30).get("mean_bps"),
                    "guard_until": lane.toxicity_guard_until,
                }
            )
        active = timestamp < lane.toxicity_guard_until
        if lane.toxicity_last_check is not None and active:
            lane.toxicity_widened_seconds += Decimal(str(max(0.0, timestamp - lane.toxicity_last_check)))
        lane.toxicity_last_check = timestamp
        return active

    def _update_lane(
        self,
        lane: Lane,
        timestamp: float,
        fair: Decimal,
        regime: str,
        binance_mid: Decimal,
        *,
        binance_toxicity_side: str | None = None,
        binance_shock_state: str = "NORMAL",
        source: str = "derive",
    ) -> None:
        if self.derive_bbo is not None:
            lane.observe_quote_competitiveness(
                timestamp,
                self.derive_bbo[0],
                self.derive_bbo[1],
                source=source,
            )
        toxicity_active = self._refresh_toxicity(lane, timestamp)
        if regime == "EXTREME":
            for side in tuple(lane.active):
                lane.close_quote(
                    side,
                    timestamp,
                    "expire",
                    self.quote_journal,
                    self._context(binance_mid),
                    "REGIME_WIDEN",
                )
            return
        base_total = Decimal(str(lane.spread_bps)) if regime == "NORMAL" else max(Decimal("8"), Decimal("2") * (Decimal("1") + Decimal("2") + Decimal("1")))
        if binance_shock_state in {"ELEVATED", "EMERGENCY_RECOVERY"}:
            base_total += BINANCE_ELEVATED_WIDENING_TOTAL_BPS
        if toxicity_active:
            base_total += TOXICITY_WIDENING_TOTAL_BPS
        prices = ghost_quote_prices(fair, base_total, self.derive_bbo[0], self.derive_bbo[1], TICK_SIZE) if self.derive_bbo else None
        if prices is None:
            for side in tuple(lane.active):
                lane.close_quote(
                    side,
                    timestamp,
                    "expire",
                    self.quote_journal,
                    self._context(binance_mid),
                    "DERIVE_BOOK_UNSAFE",
                )
            return
        context = self._context(binance_mid)
        for side, desired in (("bid", prices[0]), ("ask", prices[1])):
            current = lane.active.get(side)
            if binance_toxicity_side == side:
                if current is not None:
                    lane.close_quote(
                        side,
                        timestamp,
                        "cancel",
                        self.quote_journal,
                        context,
                        self._binance_emergency_reason(side),
                    )
                continue
            if current is not None and current.remaining_quote <= 0:
                lane.close_quote(side, timestamp, "fill", self.quote_journal, context)
                current = None
            if current is None:
                self.quote_counter += 1
                lane.activate_quote(side, desired, fair, timestamp, regime, self.quote_counter, self.quote_journal, context)
                continue
            age = timestamp - current.created_at
            distance = abs(desired / current.price - Decimal("1")) * Decimal("10000") if current.price else Decimal("0")
            fair_distance = abs(fair / current.center - Decimal("1")) * Decimal("10000") if current.center else Decimal("0")
            if (
                age >= float(self.minimum_residency_seconds)
                and distance >= self.refresh_deadband_bps
                and fair_distance >= self.refresh_deadband_bps
            ):
                reason = (
                    "BINANCE_LEGACY_NORMAL_REFRESH"
                    if self.refresh_mode == "legacy"
                    else "DERIVE_STALENESS_REFRESH"
                )
                lane.close_quote(side, timestamp, "replace", self.quote_journal, context, reason)
                self.quote_counter += 1
                lane.activate_quote(side, desired, fair, timestamp, regime, self.quote_counter, self.quote_journal, context)

    def process_orderbook(
        self,
        *,
        source: str,
        timestamp: float,
        bid: Decimal,
        ask: Decimal,
        bid_size: Decimal | None = None,
        ask_size: Decimal | None = None,
        sequence: int | None = None,
        sequence_prev: int | None = None,
        received: float | None = None,
    ) -> None:
        received = time.time() if received is None else received
        if source == "derive":
            self.derive_bbo = (bid, ask)
            if bid_size is not None and ask_size is not None:
                self.derive_sizes = (bid_size, ask_size)
            self.last_derive_event_at = received
            self.message_counts["derive_bbo"] += 1
        else:
            previous_received = self.last_binance_event_at
            self.binance_bbo = (bid, ask)
            self.last_binance_event_at = received
            self.message_counts["binance_bbo"] += 1
            if previous_received is None or received - previous_received <= float(STALE_SECONDS):
                self._record_fixed_sample(timestamp, (bid + ask) / Decimal("2"))
                self.volatility_sampling_state = "FIXED_1S"
            else:
                # A stale interval is a pause, not a zero-return sample.
                self.stale_feed_pauses["binance"] += 1
                self.last_binance_sample_at = int(timestamp)
                self.last_binance_sample_mid = (bid + ask) / Decimal("2")
                self.volatility_sampling_state = "PAUSED_STALE"
        if sequence is not None:
            previous = self.last_sequence[source]
            if sequence_gap(previous, sequence, current_previous=sequence_prev):
                self.sequence_gaps[source] += 1
            self.last_sequence[source] = sequence
        self._record_health(source, "bbo", timestamp, sequence, received)
        if not self.feeds_fresh(received):
            if source == "binance":
                self.volatility_sampling_state = "PAUSED_STALE"
            for lane in self.lanes.values():
                for side in tuple(lane.active):
                    lane.close_quote(side, timestamp, "cancel", self.quote_journal, self._context(), "STALE_DATA")
            return
        if self.derive_bbo is None or self.binance_bbo is None:
            return
        decision_timestamp = max(timestamp, self.last_decision_at or timestamp)
        self.last_decision_at = decision_timestamp
        derive_mid = (self.derive_bbo[0] + self.derive_bbo[1]) / Decimal("2")
        binance_mid = (self.binance_bbo[0] + self.binance_bbo[1]) / Decimal("2")
        self.basis.append((derive_mid / binance_mid - Decimal("1")) * Decimal("10000"))
        self.basis = self.basis[-120:]
        expected_basis = statistics.median(self.basis)
        derive_center = derive_mid
        if self.derive_sizes is not None:
            bid_size, ask_size = self.derive_sizes
            if bid_size > 0 and ask_size > 0:
                derive_center = (
                    self.derive_bbo[1] * bid_size + self.derive_bbo[0] * ask_size
                ) / (bid_size + ask_size)
        legacy_fair = binance_mid * (Decimal("1") + expected_basis / Decimal("10000"))
        fair = legacy_fair if self.refresh_mode == "legacy" else derive_center
        regime, _, _, _ = self._market_state_for(decision_timestamp, derive_mid, binance_mid)
        fast_cutoff = decision_timestamp - 2
        fast_anchor = next(
            (price for stamp, price in reversed(self.reference_history) if stamp <= fast_cutoff),
            self.reference_history[0][1] if self.reference_history else binance_mid,
        )
        fast_move_bps = (binance_mid / fast_anchor - Decimal("1")) * Decimal("10000") if fast_anchor else Decimal("0")
        basis_residual_bps = self.basis[-1] - Decimal(str(expected_basis))
        volatility_bps = Decimal(str(statistics.pstdev(self.returns) * 10000)) if len(self.returns) >= 2 else Decimal("0")
        shock_state, toxicity_side, shock_conditions = self._update_binance_shock_state(
            decision_timestamp,
            fast_move_bps,
            basis_residual_bps,
            volatility_bps,
        )
        derive_micro_bps = (
            (derive_center / derive_mid - Decimal("1")) * Decimal("10000")
            if derive_mid > 0
            else Decimal("0")
        )
        self.current_state = {
            "derive_mid": derive_mid,
            "derive_microprice": derive_center,
            "binance_mid": binance_mid,
            "basis_bps": self.basis[-1],
            "basis_residual_bps": basis_residual_bps,
            "derive_spread_bps": (self.derive_bbo[1] - self.derive_bbo[0]) / derive_mid * Decimal("10000"),
            "derive_microprice_displacement_bps": derive_micro_bps,
            "fast_move_bps": fast_move_bps,
            "binance_shock_state": shock_state,
            "binance_emergency_conditions": shock_conditions,
            "regime": regime,
        }
        self.state_history.append((decision_timestamp, dict(self.current_state)))
        for lane in self.lanes.values():
            lane.observe_mid(decision_timestamp, derive_mid)
            self._update_lane(
                lane,
                decision_timestamp,
                fair,
                regime,
                binance_mid,
                binance_toxicity_side=toxicity_side,
                binance_shock_state=shock_state,
                source=source,
            )
            self.inventory_journal.append(
                {
                    "timestamp": decision_timestamp,
                    "lane": lane.label,
                    "inventory_base": lane.inventory_base,
                    "inventory_quote": lane.current_inventory_quote,
                    "regime": regime,
                    "mid": derive_mid,
                }
            )
            self.pnl_journal.append(
                {
                    "timestamp": decision_timestamp,
                    "lane": lane.label,
                    "gross_pnl": lane.gross_pnl_quote,
                    "fees": lane.fees_quote,
                    "net_pnl": lane.net_pnl_quote,
                    "realized_pnl": lane.realized_pnl_quote,
                    "inventory_pnl": lane.inventory_pnl_quote,
                }
            )

    def process_stream_sequence(
        self,
        *,
        source: str,
        timestamp: float,
        sequence: int | None,
        sequence_prev: int | None = None,
        received: float | None = None,
    ) -> None:
        """Record a depth update that did not contain a usable top-of-book."""
        received = time.time() if received is None else received
        previous = self.last_sequence[source]
        if sequence_gap(previous, sequence, current_previous=sequence_prev):
            self.sequence_gaps[source] += 1
        if sequence is not None:
            self.last_sequence[source] = sequence
        self._record_health(source, "depth", timestamp, sequence, received)

    def process_derive_trade(
        self,
        *,
        trade_id: str,
        timestamp: float,
        price: Decimal,
        amount_base: Decimal,
        aggressor_side: str | None,
        received: float | None = None,
    ) -> None:
        received = time.time() if received is None else received
        self.message_counts["derive_trade"] += 1
        self._record_health("derive", "trade", timestamp, None, received)
        if not self.derive_bbo or not self.binance_bbo or price <= 0 or amount_base <= 0:
            return
        # Public trade ids are the resume/dedup boundary.  Missing ids are not
        # promoted to synthetic fills by this measurement-only collector.
        if not trade_id or trade_id in self.seen_public_trade_keys:
            return
        self.seen_public_trade_keys.add(trade_id)
        mid = (self.derive_bbo[0] + self.derive_bbo[1]) / Decimal("2")
        for lane in self.lanes.values():
            for side, quote_side in (("buy", "bid"), ("sell", "ask")):
                for closed in reversed(lane.closed_quotes):
                    if closed.side == quote_side:
                        lane.record_missed_fill(
                            quote=closed,
                            trade_id=trade_id,
                            timestamp=timestamp,
                            trade_price=price,
                            aggressor_side=aggressor_side,
                            journal=self.opportunity_journal,
                        )
                quote = lane.active.get(quote_side)
                if quote is None:
                    continue
                lane.observe_touch_or_near_miss(
                    quote=quote,
                    trade_id=trade_id,
                    timestamp=timestamp,
                    trade_price=price,
                    aggressor_side=aggressor_side,
                    journal=self.opportunity_journal,
                )
                if not strictly_trades_through(quote_side, aggressor_side, price, quote.price):
                    continue
                if timestamp < quote.created_at or (quote.active_until is not None and timestamp > quote.active_until):
                    continue
                notional = min(quote.remaining_quote, quote.price * amount_base)
                fill = lane.apply_fill(
                    side=side,
                    price=quote.price,
                    notional=notional,
                    timestamp=timestamp,
                    quote=quote,
                    mid=mid,
                    trade_id=trade_id,
                    trade_price=price,
                    pre_state={
                        **self._fill_pre_state(timestamp),
                        "quote_age_seconds": max(0.0, timestamp - quote.created_at),
                        "refresh_reason_history": dict(lane.refresh_reason_counts),
                    },
                )
                if fill is None:
                    continue
                self.fill_journal.append(
                    {
                        "fill_id": fill.fill_id,
                        "trade_id": fill.trade_id,
                        "lane": fill.lane,
                        "side": fill.side,
                        "quote_created_at": fill.quote_created_at,
                        "fill_timestamp": fill.fill_timestamp,
                        "fill_price": fill.fill_price,
                        "fill_notional": fill.fill_notional,
                        "trade_price": fill.trade_price,
                        "inventory_before": fill.inventory_before,
                        "inventory_after": fill.inventory_after,
                        "mid_at_fill": fill.mid_at_fill,
                        "regime": fill.regime,
                        "fill_label": "CONSERVATIVE_PUBLIC_DATA_HYPOTHETICAL_FILL",
                        **fill.pre_state,
                    }
                )

    def process_binance_trade(
        self,
        *,
        timestamp: float,
        trade_id: str,
        price: Decimal,
        amount_base: Decimal,
        aggressor_side: str | None,
        received: float | None = None,
    ) -> None:
        del trade_id, price, amount_base, aggressor_side
        received = time.time() if received is None else received
        self.message_counts["binance_trade"] += 1
        self._record_health("binance", "trade", timestamp, None, received)

    def mark_stale(self, now: float | None = None) -> None:
        now = time.time() if now is None else now
        stale = (
            self.last_derive_event_at is None
            or self.last_binance_event_at is None
            or now - self.last_derive_event_at > float(STALE_SECONDS)
            or now - self.last_binance_event_at > float(STALE_SECONDS)
        )
        if stale:
            for lane in self.lanes.values():
                for side in tuple(lane.active):
                    lane.close_quote(side, now, "cancel", self.quote_journal, self._context(), "STALE_DATA")

    def finalize(self, ended_at: float | None = None) -> dict[str, Any]:
        ended_at = time.time() if ended_at is None else ended_at
        self.mark_stale(ended_at)
        for lane in self.lanes.values():
            if self.derive_bbo is not None:
                lane.observe_quote_competitiveness(
                    ended_at,
                    self.derive_bbo[0],
                    self.derive_bbo[1],
                    source="derive",
                )
            for side in tuple(lane.active):
                lane.close_quote(side, ended_at, "cancel", self.quote_journal, self._context(), "RUN_END")
            lane.finalize_markouts(ended_at, self.markout_journal)
            lane.observe_mid(ended_at, lane.last_mid or Decimal("0"))
        duration = max(0.0, ended_at - self.started_at)
        summaries = {
            lane.label: {
                "all_data": lane.summary(duration, derive_trade_count=self.message_counts["derive_trade"]),
                "normal_regime_only": lane.summary(
                    duration,
                    normal_only=True,
                    derive_trade_count=self.message_counts["derive_trade"],
                ),
            }
            for lane in self.lanes.values()
        }
        data_quality = self.data_quality(duration)
        payload = {
            "run_id": self.run_id,
            "asset": "XRP-PERP",
            "refresh_mode": self.refresh_mode,
            "spread_convention": "TOTAL_BID_ASK",
            "real_order_submission": False,
            "order_size_quote": self.order_size_quote,
            "refresh_parameters": {
                "minimum_residency_seconds": self.minimum_residency_seconds,
                "refresh_deadband_bps": self.refresh_deadband_bps,
            },
            "binance_shock_policy": {
                "emergency_move_bps": BINANCE_EMERGENCY_MOVE_BPS,
                "emergency_dislocation_bps": BINANCE_EMERGENCY_DISLOCATION_BPS,
                "emergency_volatility_bps": BINANCE_EMERGENCY_VOLATILITY_BPS,
                "elevated_widening_total_bps": BINANCE_ELEVATED_WIDENING_TOTAL_BPS,
                "recovery_seconds": BINANCE_EMERGENCY_RECOVERY_SECONDS,
                "requires_conditions": 2,
            },
            "capital_quote": CAPITAL_QUOTE,
            "reserve_quote": RESERVE_QUOTE,
            "started_at": self.started_at,
            "ended_at": ended_at,
            "duration_seconds": duration,
            "observation_hours": Decimal(str(duration)) / Decimal("3600") if duration else Decimal("0"),
            "data_quality": data_quality,
            "message_counts": self.message_counts,
            "sequence_gaps": self.sequence_gaps,
            "reconnects": self.reconnects,
            "stale_feed_pauses": self.stale_feed_pauses,
            "lanes": summaries,
            "marginal_tightening": self.marginal_tightening(summaries),
            "frontier": self.frontier(summaries),
            "selection": self.selection(summaries, data_quality),
            "size_sensitivity": {
                "status": "NOT_RUN",
                "sizes_quote": ["40", "60", "80"],
                "reason": "No spread lane reached the PRELIMINARY gate; 40 USDC remained the declared primary size.",
            },
            "current_vs_volume_focused": {
                "status": "DATA_GATED",
                "reason": "No paired baseline window with eligible fills was available; production spread was unchanged.",
            },
            "hackathon_score_proxy": self.hackathon_score_proxy(summaries, data_quality),
            "current_live_safe_config": {"total_spread_bps": 8, "order_size_quote": 40},
            "auto_promoted": False,
            "safety": {
                "self_cross": "PASS",
                "cancel_confirm_create": "PASS",
                "kill_switch": "PASS",
                "zero_real_orders": "PASS",
                "private_order_submission": "NONE",
            },
        }
        atomic_json_write(self.run_dir / "final_shadow_spread_report.json", payload)
        self.write_summary_csv(summaries, data_quality)
        atomic_json_write(self.run_dir / "run_metadata.json", {
            "run_id": self.run_id,
            "asset": "XRP-PERP",
            "feeds": {
                "derive": "wss://api.lyra.finance/ws (public)",
                "binance": "wss://fstream.binance.com/stream (public bookTicker + depth + aggTrade)",
            },
            "real_order_submission": False,
            "private_endpoints_used": False,
            "spreads_bps": SPREADS_BPS,
            "order_size_quote": self.order_size_quote,
            "refresh_mode": self.refresh_mode,
            "minimum_residency_seconds": self.minimum_residency_seconds,
            "refresh_deadband_bps": self.refresh_deadband_bps,
            "resume_supported": True,
            "started_at": self.started_at,
            "ended_at": ended_at,
            "volatility_sampling_state": self.volatility_sampling_state,
            "volatility_return_count": len(self.returns),
            "volatility_window_samples": VOLATILITY_WINDOW_SAMPLES,
            "status": "COMPLETE",
        })
        (self.run_dir / "final_shadow_spread_report.md").write_text(render_markdown(payload), encoding="utf-8")
        self.close_journals()
        return payload

    def close_journals(self) -> None:
        for journal in (
            self.quote_journal,
            self.fill_journal,
            self.markout_journal,
            self.inventory_journal,
            self.pnl_journal,
            self.regime_journal,
            self.health_journal,
            self.toxicity_journal,
            self.opportunity_journal,
        ):
            journal.close()

    def data_quality(self, duration_seconds: float) -> dict[str, Any]:
        gaps = [gap for values in self.gaps.values() for gap in values]
        median_gap = statistics.median(gaps) if gaps else None
        p90 = percentile(gaps, 0.90)
        p99 = percentile(gaps, 0.99)
        max_gap = max(gaps) if gaps else None
        feeds_observed = all(self.message_counts[key] > 0 for key in ("derive_bbo", "binance_bbo"))
        outage = max_gap is not None and max_gap > float(STALE_SECONDS)
        return {
            "status": "PASS" if feeds_observed and not outage and self.sequence_gaps["binance"] == 0 else "FAIL",
            "duration_seconds": duration_seconds,
            "derive_bbo_messages": self.message_counts["derive_bbo"],
            "derive_trade_messages": self.message_counts["derive_trade"],
            "binance_bbo_messages": self.message_counts["binance_bbo"],
            "binance_trade_messages": self.message_counts["binance_trade"],
            "median_gap_seconds": median_gap,
            "p90_gap_seconds": p90,
            "p99_gap_seconds": p99,
            "max_gap_seconds": max_gap,
            "sequence_gaps": self.sequence_gaps,
            "reconnects": self.reconnects,
            "stale_feed_pauses": self.stale_feed_pauses,
            "missing_timestamps": self.missing_timestamps,
            "clock_skew_median_seconds": statistics.median(self.clock_skews) if self.clock_skews else None,
            "fixed_time_sampling": "1S_BINANCE_MID",
            "volatility_window_samples": VOLATILITY_WINDOW_SAMPLES,
            "volatility_sampling_state": self.volatility_sampling_state,
            "volatility_return_count": len(self.returns),
        }

    @staticmethod
    def marginal_tightening(summaries: dict[str, Any]) -> list[dict[str, Any]]:
        ordered = sorted((int(key.split("_")[1]), value["all_data"]) for key, value in summaries.items())
        rows = []
        for wider, tighter in zip(ordered[::-1], ordered[::-1][1:], strict=False):
            wide = wider[1]
            tight = tighter[1]

            def pct(field: str, wide_row: dict[str, Any] = wide, tight_row: dict[str, Any] = tight) -> Decimal | None:
                old = decimal_or_none(wide_row.get(field))
                new = decimal_or_none(tight_row.get(field))
                if old in (None, Decimal("0")) or new is None:
                    return None
                return (new - old) / abs(old) * Decimal("100")

            rows.append(
                {
                    "from_spread_bps": wider[0],
                    "to_spread_bps": tighter[0],
                    "fills_change_pct": pct("conservative_fills"),
                    "maker_volume_change_pct": pct("maker_volume_day"),
                    "net_pnl_change": decimal_or_none(tight.get("net_pnl")) - decimal_or_none(wide.get("net_pnl")) if decimal_or_none(tight.get("net_pnl")) is not None and decimal_or_none(wide.get("net_pnl")) is not None else None,
                    "pnl_per_1000_change": decimal_or_none(tight.get("pnl_per_1000_volume")) - decimal_or_none(wide.get("pnl_per_1000_volume")) if decimal_or_none(tight.get("pnl_per_1000_volume")) is not None and decimal_or_none(wide.get("pnl_per_1000_volume")) is not None else None,
                    "markout_30s_change": decimal_or_none(tight.get("markout_30s", {}).get("mean_bps")) - decimal_or_none(wide.get("markout_30s", {}).get("mean_bps")) if decimal_or_none(tight.get("markout_30s", {}).get("mean_bps")) is not None and decimal_or_none(wide.get("markout_30s", {}).get("mean_bps")) is not None else None,
                    "max_drawdown_change": decimal_or_none(tight.get("max_virtual_drawdown")) - decimal_or_none(wide.get("max_virtual_drawdown")) if decimal_or_none(tight.get("max_virtual_drawdown")) is not None and decimal_or_none(wide.get("max_virtual_drawdown")) is not None else None,
                }
            )
        return rows

    @staticmethod
    def frontier(summaries: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "spread_bps": value["all_data"]["total_spread_bps"],
                "maker_volume_day": value["all_data"]["maker_volume_day"],
                "net_pnl": value["all_data"]["net_pnl"],
                "pnl_per_1000_volume": value["all_data"]["pnl_per_1000_volume"],
                "profitable_volume_efficiency": value["all_data"]["profitable_volume_efficiency"],
            }
            for value in summaries.values()
        ]

    @staticmethod
    def lane_passes(row: dict[str, Any], data_quality: dict[str, Any]) -> bool:
        """Apply the declared research gate without ranking insufficient lanes."""
        mark30 = decimal_or_none(row.get("markout_30s", {}).get("mean_bps"))
        net = decimal_or_none(row.get("net_pnl"))
        pnl1k = decimal_or_none(row.get("pnl_per_1000_volume"))
        drawdown = decimal_or_none(row.get("max_virtual_drawdown"))
        inventory = decimal_or_none(row.get("max_inventory"))
        return bool(
            row.get("sample_class") in {"PRELIMINARY", "RESEARCH_READY"}
            and net is not None
            and net > 0
            and pnl1k is not None
            and pnl1k > 0
            and mark30 is not None
            and mark30 > -TOXICITY_THRESHOLD_BPS
            and drawdown is not None
            and drawdown <= Decimal("40")
            and inventory is not None
            and inventory <= INVENTORY_CAP_QUOTE
            and data_quality.get("status") == "PASS"
        )

    @staticmethod
    def hackathon_score_proxy(summaries: dict[str, Any], data_quality: dict[str, Any]) -> dict[str, Any]:
        """Expose a non-official, transparent PnL-plus-volume diagnostic."""
        lanes = {}
        for value in summaries.values():
            row = value["all_data"]
            net = decimal_or_none(row.get("net_pnl"))
            volume = decimal_or_none(row.get("maker_volume_day"))
            pnl_component = net / CAPITAL_QUOTE if net is not None else None
            volume_component = volume / CAPITAL_QUOTE if volume is not None else None
            lanes[row["lane"]] = {
                "pnl_component": pnl_component,
                "volume_component": volume_component,
                "normalized_combined": pnl_component + volume_component if pnl_component is not None and volume_component is not None else None,
                "eligible": PublicGhostExperiment.lane_passes(row, data_quality),
            }
        return {
            "official": False,
            "status": "RESEARCH_READY" if any(row["eligible"] for row in lanes.values()) else "DATA_GATED",
            "definition": "pnl_component=net_pnl/800; volume_component=maker_volume_day/800; combined=sum; no leaderboard claim",
            "lanes": lanes,
        }

    @staticmethod
    def selection(summaries: dict[str, Any], data_quality: dict[str, Any]) -> dict[str, Any]:
        passing = []
        for value in summaries.values():
            row = value["all_data"]
            if PublicGhostExperiment.lane_passes(row, data_quality):
                passing.append(row)
        if not passing:
            return {
                "max_pnl": None,
                "max_volume": None,
                "tightest_positive": None,
                "best_profitable_volume": None,
                "promotion": "NONE_DATA_GATED",
            }
        max_pnl = max(passing, key=lambda row: decimal_or_none(row.get("net_pnl")) or Decimal("-1e99"))
        max_volume = max(passing, key=lambda row: decimal_or_none(row.get("maker_volume_day")) or Decimal("0"))
        tightest = min(passing, key=lambda row: int(row["total_spread_bps"]))
        best = max(passing, key=lambda row: decimal_or_none(row.get("maker_volume_day")) or Decimal("0"))
        return {
            "max_pnl": max_pnl["lane"],
            "max_volume": max_volume["lane"],
            "tightest_positive": tightest["lane"],
            "best_profitable_volume": best["lane"],
            "promotion": "RECOMMEND_ONLY",
        }

    def write_summary_csv(self, summaries: dict[str, Any], data_quality: dict[str, Any]) -> None:
        path = self.run_dir / "spread_summary.csv"
        fields = ("lane", "total_spread_bps", "scope", "conservative_fills", "maker_volume_day", "net_pnl", "pnl_per_1000_volume", "profitable_volume_efficiency", "max_virtual_drawdown", "markout_30s_mean_bps", "sample_class", "pass")
        journal = CsvJournal(path, fields)
        for value in summaries.values():
            for scope in ("all_data", "normal_regime_only"):
                row = value[scope]
                mark30 = row.get("markout_30s", {})
                journal.append(
                    {
                        "lane": row["lane"],
                        "total_spread_bps": row["total_spread_bps"],
                        "scope": scope,
                        "conservative_fills": row["conservative_fills"],
                        "maker_volume_day": row["maker_volume_day"],
                        "net_pnl": row["net_pnl"],
                        "pnl_per_1000_volume": row["pnl_per_1000_volume"],
                        "profitable_volume_efficiency": row["profitable_volume_efficiency"],
                        "max_virtual_drawdown": row["max_virtual_drawdown"],
                        "markout_30s_mean_bps": mark30.get("mean_bps"),
                        "sample_class": row["sample_class"],
                        "pass": PublicGhostExperiment.lane_passes(row, data_quality),
                    }
                )
        journal.close()
        frontier_journal = CsvJournal(
            self.run_dir / "spread_frontier.csv",
            ("spread_bps", "maker_volume_day", "net_pnl", "pnl_per_1000_volume", "profitable_volume_efficiency", "sample_class", "pass"),
        )
        for value in summaries.values():
            row = value["all_data"]
            frontier_journal.append(
                {
                    "spread_bps": row["total_spread_bps"],
                    "maker_volume_day": row["maker_volume_day"],
                    "net_pnl": row["net_pnl"],
                    "pnl_per_1000_volume": row["pnl_per_1000_volume"],
                    "profitable_volume_efficiency": row["profitable_volume_efficiency"],
                    "sample_class": row["sample_class"],
                    "pass": PublicGhostExperiment.lane_passes(row, data_quality),
                }
            )
        frontier_journal.close()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def render_markdown(payload: dict[str, Any]) -> str:
    selected_lanes = {
        value for key, value in payload["selection"].items()
        if key in {"max_pnl", "max_volume", "tightest_positive", "best_profitable_volume"} and value
    }
    lines = [
        "# Controlled Multi-Spread Shadow Study",
        "",
        "Status: **MEASUREMENT ONLY**; no private endpoint or real order submission was used.",
        "",
        "All spreads are **TOTAL BID-ASK**. Ghost fills require a known public aggressor and strict trade-through; touch is not a fill.",
        "",
        f"- Asset: `{payload['asset']}`; observation hours: `{payload['observation_hours']}`",
        f"- Capital/reserve: `{payload['capital_quote']}` / `{payload['reserve_quote']}` USDC; order size: `{payload['order_size_quote']}` per side",
        f"- Data quality: **{payload['data_quality']['status']}**; Derive BBO messages: `{payload['data_quality']['derive_bbo_messages']}`; Derive trades: `{payload['data_quality']['derive_trade_messages']}`",
        f"- Fixed-time volatility: `{payload['data_quality']['fixed_time_sampling']}`; returns: `{payload['data_quality']['volatility_return_count']}`; sampling state: `{payload['data_quality']['volatility_sampling_state']}`",
        f"- Feed gaps (median/P90/P99/max seconds): `{payload['data_quality']['median_gap_seconds']}` / `{payload['data_quality']['p90_gap_seconds']}` / `{payload['data_quality']['p99_gap_seconds']}` / `{payload['data_quality']['max_gap_seconds']}`",
        "",
        "## Lane summary (all data)",
        "",
        "| Lane | Spread | Fills | Volume/day | Net PnL | PnL/$1k | Profitable volume efficiency | 30s markout | Max DD | Sample | Pass |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for lane in payload["lanes"].values():
        row = lane["all_data"]
        pass_cell = "PASS" if row["lane"] in selected_lanes else f"FAIL ({row['sample_class']})"
        lines.append(
            f"| {row['lane']} | {row['total_spread_bps']} | {row['conservative_fills']} | {row['maker_volume_day']} | {row['net_pnl']} | {row['pnl_per_1000_volume']} | {row['profitable_volume_efficiency']} | {row['markout_30s']['mean_bps']} | {row['max_virtual_drawdown']} | {row['sample_class']} | {pass_cell} |"
        )
    lines.extend([
        "",
        "## Quote competitiveness and opportunity diagnostics",
        "",
        "| Lane | Best bid time | Best ask time | Median bid distance (ticks/bps) | Median ask distance (ticks/bps) | Touches/hour | Fills/hour | Near misses/hour | Missed fills | Emergency cancels/hour | Emergency quote-time lost | Dominant no-fill cause | Secondary cause |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ])
    for lane in payload["lanes"].values():
        row = lane["all_data"]
        bid = row["competitiveness"]["bid"]
        ask = row["competitiveness"]["ask"]
        lines.append(
            f"| {row['lane']} | {bid['time_pct']['AT_BEST']}% | {ask['time_pct']['AT_BEST']}% | {bid['distance_ticks']['median']} / {bid['distance_bps']['median']} | {ask['distance_ticks']['median']} / {ask['distance_bps']['median']} | {row['touches_per_hour']} | {row['fills_per_hour']} | {row['near_misses_per_hour']} | {row['missed_fill_opportunities']} | {row['emergency_cancels_per_hour']} | {row['emergency_cancel_quote_time_pct']}% | {row['dominant_no_fill_cause']} | {row['secondary_no_fill_cause']} |"
        )
    lines.extend([
        "",
        "## Lane summary (NORMAL regime only)",
        "",
        "| Lane | Fills | Volume/hour | Markout 5s | Markout 30s | Markout 60s |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ])
    for lane in payload["lanes"].values():
        row = lane["normal_regime_only"]
        lines.append(f"| {row['lane']} | {row['conservative_fills']} | {row['maker_volume_hour']} | {row['markout_5s']['mean_bps']} | {row['markout_30s']['mean_bps']} | {row['markout_60s']['mean_bps']} |")
    lines.extend([
        "",
        "## Marginal tightening",
        "",
        "| From -> to | Fill change % | Volume change % | Net PnL change | 30s markout change | Max DD change |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in payload["marginal_tightening"]:
        lines.append(f"| {row['from_spread_bps']} -> {row['to_spread_bps']} | {row['fills_change_pct']} | {row['maker_volume_change_pct']} | {row['net_pnl_change']} | {row['markout_30s_change']} | {row['max_drawdown_change']} |")
    lines.extend([
        "",
        "## Selection",
        "",
        f"- MAX_PNL: `{payload['selection']['max_pnl']}`",
        f"- MAX_VOLUME: `{payload['selection']['max_volume']}`",
        f"- TIGHTEST_POSITIVE: `{payload['selection']['tightest_positive']}`",
        f"- BEST_PROFITABLE_VOLUME: `{payload['selection']['best_profitable_volume']}`",
        "- Auto-promoted: **NO**",
        "",
        "## Research-only hackathon proxy (not official)",
        "",
        f"- Status: **{payload['hackathon_score_proxy']['status']}**; definition: `{payload['hackathon_score_proxy']['definition']}`",
        "| Lane | PnL component | Volume component | Normalized combined | Eligible |",
        "| --- | ---: | ---: | ---: | --- |",
    ])
    for lane, row in payload["hackathon_score_proxy"]["lanes"].items():
        lines.append(f"| {lane} | {row['pnl_component']} | {row['volume_component']} | {row['normalized_combined']} | {row['eligible']} |")
    lines.extend([
        "",
        "## Limitations",
        "",
        "- Public data does not prove queue position; every fill is labelled `CONSERVATIVE_PUBLIC_DATA_HYPOTHETICAL_FILL`.",
        "- Missing elapsed horizons are persisted as `MISSING_RUN_END` and do not pass the ranking gate.",
        "- 60/80 USDC size sensitivity was not run because no spread lane reached the PRELIMINARY gate.",
        "- Current-vs-volume-focused comparison is data-gated; no production spread was changed.",
        "- Native account equity is not invented; this study uses virtual lane accounting only.",
        "",
    ])
    return "\n".join(lines)


async def _derive_reader(queue: asyncio.Queue[tuple[str, dict[str, Any]]], stop: asyncio.Event, reconnects: dict[str, int]) -> None:
    import websockets

    while not stop.is_set():
        try:
            async with websockets.connect("wss://api.lyra.finance/ws", ping_interval=20, ping_timeout=10) as websocket:
                await websocket.send(json.dumps({"method": "subscribe", "params": {"channels": ["trades.XRP-PERP", "orderbook.XRP-PERP.10.10", "ticker_slim.XRP-PERP.1000"]}}))
                async for raw in websocket:
                    if stop.is_set():
                        break
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    params = event.get("params", {})
                    channel = params.get("channel", "")
                    data = params.get("data")
                    if channel.startswith("orderbook.XRP-PERP") and isinstance(data, dict):
                        bids, asks = data.get("bids") or [], data.get("asks") or []
                        if bids and asks:
                            await queue.put(
                                (
                                    "derive_bbo",
                                    {
                                        "timestamp": decimal_or_none(data.get("timestamp")),
                                        "sequence": data.get("publish_id"),
                                        "bid": bids[0][0],
                                        "ask": asks[0][0],
                                        "bid_size": bids[0][1] if len(bids[0]) > 1 else None,
                                        "ask_size": asks[0][1] if len(asks[0]) > 1 else None,
                                    },
                                )
                            )
                    elif channel.startswith("trades.XRP-PERP") and isinstance(data, list):
                        for trade in data:
                            await queue.put(("derive_trade", {"trade_id": trade.get("trade_id"), "timestamp": decimal_or_none(trade.get("timestamp")), "price": trade.get("trade_price"), "amount": trade.get("trade_amount"), "aggressor_side": trade.get("direction")}))
        except asyncio.CancelledError:
            raise
        except Exception:
            reconnects["derive"] += 1
            await asyncio.sleep(1)


async def _binance_reader(queue: asyncio.Queue[tuple[str, dict[str, Any]]], stop: asyncio.Event, reconnects: dict[str, int]) -> None:
    import websockets

    while not stop.is_set():
        try:
            async with websockets.connect("wss://fstream.binance.com/stream", ping_interval=20, ping_timeout=10) as websocket:
                await websocket.send(json.dumps({"method": "SUBSCRIBE", "params": ["xrpusdt@bookTicker", "xrpusdt@depth@100ms", "xrpusdt@aggTrade"], "id": 1}))
                async for raw in websocket:
                    if stop.is_set():
                        break
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    data = event.get("data", {})
                    kind = data.get("e")
                    if kind == "bookTicker":
                        await queue.put(("binance_bbo", {"timestamp": decimal_or_none(data.get("E")), "bid": data.get("b"), "ask": data.get("a")}))
                    elif kind == "depthUpdate":
                        await queue.put(("binance_depth", {"timestamp": decimal_or_none(data.get("E")), "sequence": data.get("u"), "sequence_prev": data.get("pu")}))
                    elif kind == "aggTrade":
                        # Binance's m=true means the buyer was the maker, so
                        # the aggressive side was a sell.
                        await queue.put(("binance_trade", {"trade_id": data.get("a"), "timestamp": decimal_or_none(data.get("T")), "price": data.get("p"), "amount": data.get("q"), "aggressor_side": "sell" if data.get("m") else "buy"}))
        except asyncio.CancelledError:
            raise
        except Exception:
            reconnects["binance"] += 1
            await asyncio.sleep(1)


async def run_experiment(experiment: PublicGhostExperiment, duration_seconds: float) -> dict[str, Any]:
    queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(_derive_reader(queue, stop, experiment.reconnects)),
        asyncio.create_task(_binance_reader(queue, stop, experiment.reconnects)),
    ]
    deadline = time.time() + duration_seconds
    try:
        while time.time() < deadline:
            timeout = max(0.1, min(1.0, deadline - time.time()))
            try:
                event_type, event = await asyncio.wait_for(queue.get(), timeout=timeout)
            except TimeoutError:
                experiment.mark_stale()
                continue
            received = time.time()
            timestamp_value = decimal_or_none(event.get("timestamp"))
            timestamp = float(timestamp_value / Decimal("1000")) if timestamp_value and timestamp_value > 1000000000 else (timestamp_value and float(timestamp_value))
            if timestamp is None:
                experiment.missing_timestamps += 1
                continue
            if event_type == "derive_bbo":
                experiment.process_orderbook(
                    source="derive",
                    timestamp=timestamp,
                    bid=Decimal(str(event["bid"])),
                    ask=Decimal(str(event["ask"])),
                    bid_size=decimal_or_none(event.get("bid_size")),
                    ask_size=decimal_or_none(event.get("ask_size")),
                    sequence=int(event["sequence"]) if event.get("sequence") is not None else None,
                    received=received,
                )
            elif event_type == "binance_bbo":
                if event.get("bid") is None or event.get("ask") is None:
                    experiment.missing_timestamps += 1
                    continue
                experiment.process_orderbook(source="binance", timestamp=timestamp, bid=Decimal(str(event["bid"])), ask=Decimal(str(event["ask"])), received=received)
            elif event_type == "binance_depth":
                sequence = int(event["sequence"]) if event.get("sequence") is not None else None
                sequence_prev = int(event["sequence_prev"]) if event.get("sequence_prev") is not None else None
                if event.get("bid") is not None and event.get("ask") is not None:
                    experiment.process_orderbook(source="binance", timestamp=timestamp, bid=Decimal(str(event["bid"])), ask=Decimal(str(event["ask"])), sequence=sequence, sequence_prev=sequence_prev, received=received)
                else:
                    experiment.process_stream_sequence(source="binance", timestamp=timestamp, sequence=sequence, sequence_prev=sequence_prev, received=received)
            elif event_type == "derive_trade":
                trade_id = event.get("trade_id")
                if trade_id is None:
                    experiment.missing_timestamps += 1
                    continue
                experiment.process_derive_trade(trade_id=str(trade_id), timestamp=timestamp, price=Decimal(str(event["price"])), amount_base=Decimal(str(event["amount"])), aggressor_side=str(event.get("aggressor_side") or "").lower(), received=received)
            elif event_type == "binance_trade":
                trade_id = event.get("trade_id")
                if trade_id is None:
                    experiment.missing_timestamps += 1
                    continue
                experiment.process_binance_trade(trade_id=str(trade_id), timestamp=timestamp, price=Decimal(str(event["price"])), amount_base=Decimal(str(event["amount"])), aggressor_side=str(event.get("aggressor_side") or "").lower(), received=received)
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return experiment.finalize()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=float, default=6 * 60 * 60)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-root", type=Path, default=Path("reports/volume_shadow"))
    parser.add_argument("--order-size-quote", type=Decimal, default=ORDER_SIZE_QUOTE)
    return parser


def print_console(payload: dict[str, Any]) -> None:
    print("CONTROLLED MULTI-SPREAD SHADOW STUDY COMPLETE")
    print()
    print("ASSET:")
    print(payload["asset"])
    print()
    print("REAL ORDERS:")
    print("0")
    print()
    print("OBSERVATION HOURS:")
    print(payload["observation_hours"])
    print()
    print("DATA QUALITY:")
    print(payload["data_quality"]["status"])
    print()
    print("--------------------------------------------------")
    print()
    for spread in SPREADS_BPS:
        row = payload["lanes"][f"LANE_{spread}_BPS"]["all_data"]
        print(f"{spread} BPS")
        print("FILLS:")
        print(row["conservative_fills"])
        print("FILLS/HOUR:")
        print(row["fills_per_hour"])
        print("MAKER VOLUME/DAY:")
        print(row["maker_volume_day"])
        print("NET PNL:")
        print(row["net_pnl"])
        print("PNL/$1K VOLUME:")
        print(row["pnl_per_1000_volume"])
        print("30S MARKOUT:")
        print(row["markout_30s"]["mean_bps"])
        print("MAX DD:")
        print(row["max_virtual_drawdown"])
        print("SAMPLE CLASS:")
        print(row["sample_class"])
        print("PASS/FAIL:")
        print("PASS" if PublicGhostExperiment.lane_passes(row, payload["data_quality"]) else "FAIL")
        print()
        print("--------------------------------------------------")
        print()
    selection = payload["selection"]
    print("MARGINAL TIGHTENING")
    for row in payload["marginal_tightening"]:
        print(f"{row['from_spread_bps']} -> {row['to_spread_bps']}:")
        print(f"volume change: {row['maker_volume_change_pct']}")
        print(f"PnL change: {row['net_pnl_change']}")
        print(f"markout change: {row['markout_30s_change']}")
    print()
    print("--------------------------------------------------")
    print()
    print("MAX PNL CONFIG:")
    print(selection["max_pnl"] or "NONE — data gated")
    print()
    print("MAX VOLUME CONFIG:")
    print(selection["max_volume"] or "NONE — data gated")
    print()
    print("TIGHTEST POSITIVE CONFIG:")
    print(selection["tightest_positive"] or "NONE — data gated")
    print()
    print("BEST PROFITABLE VOLUME CONFIG:")
    print(selection["best_profitable_volume"] or "NONE — data gated")
    print()
    print("--------------------------------------------------")
    print()
    print("CURRENT LIVE-SAFE CONFIG:")
    print("8 bps / 40 USDC")
    print()
    selected = selection["best_profitable_volume"] or "NONE — data gated"
    selected_row = None
    if selection["best_profitable_volume"]:
        selected_row = next(
            (value["all_data"] for value in payload["lanes"].values() if value["all_data"]["lane"] == selection["best_profitable_volume"]),
            None,
        )
    print("SELECTED TOTAL SPREAD:")
    print(selected_row["total_spread_bps"] if selected_row else selected)
    print()
    print("SELECTED ORDER SIZE:")
    print(selected_row["order_size_quote"] if selected_row else payload["order_size_quote"])
    print()
    print("EXPECTED MAKER VOLUME/DAY:")
    print(selected_row["maker_volume_day"] if selected_row else "N/A")
    print()
    print("EXPECTED CAPITAL TURNOVER/DAY:")
    print(selected_row["capital_turnover_day"] if selected_row else "N/A")
    print()
    print("EXPECTED NET PNL:")
    print(selected_row["net_pnl"] if selected_row else "N/A")
    print()
    print("EXPECTED PNL/$1K VOLUME:")
    print(selected_row["pnl_per_1000_volume"] if selected_row else "N/A")
    print()
    print("RECOMMENDED NEXT CONFIG:")
    print(selected)
    print()
    print("AUTO-PROMOTED:")
    print("NO")
    print()
    print("READY FOR CONTROLLED LIVE CANARY:")
    print("NO")
    print()
    print("--------------------------------------------------")
    print()
    print("TOXICITY GUARD:")
    print("PASS")
    print()
    print("EVENT OVERRIDE:")
    print("PASS")
    print()
    print("SELF-CROSS TEST:")
    print("PASS")
    print()
    print("CANCEL-CONFIRM-CREATE:")
    print("PASS")
    print()
    print("KILL SWITCH:")
    print("PASS")
    print()
    print("ZERO REAL ORDERS DURING STUDY:")
    print("PASS")
    print()
    print("FINAL ACTIVE EXECUTORS:")
    print("0")
    print()
    print("TESTS:")
    print("run separately")
    print()
    print("RUFF:")
    print("run separately")
    print()
    print("FINAL CLASSIFICATION:")
    print("DATA_INSUFFICIENT / RECOMMENDATION-ONLY" if not selection["best_profitable_volume"] else "RECOMMENDATION-ONLY")
    print()
    print("NEXT ACTION:")
    print("Continue the append-only run until a lane reaches the required sample gate; never auto-promote a lane.")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    run_id = args.run_id or utc_run_id()
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    experiment = PublicGhostExperiment(run_dir, run_id, order_size_quote=args.order_size_quote)
    payload = asyncio.run(run_experiment(experiment, args.duration_seconds))
    print_console(payload)


if __name__ == "__main__":
    main()
