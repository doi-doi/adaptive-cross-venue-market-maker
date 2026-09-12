"""Strict priority reference selection with causal failover and recovery hysteresis."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .models import BPS, ZERO, BookSnapshot, SourceFairValue
from .reference import source_fair_value
from .source_health import SourceHealth

FRESH_HEALTH = {"HEALTHY", "DEGRADED"}


@dataclass(frozen=True)
class PrioritySelection:
    """One causal priority decision plus diagnostic values from fresh sources."""

    asset: str
    selected_venue: str | None
    selected_book: BookSnapshot | None
    selected_fair_value: SourceFairValue | None
    fresh_venues: tuple[str, ...]
    source_values: dict[str, SourceFairValue]
    source_health: dict[str, dict[str, Any]]
    dispersion_bps: Decimal | None
    pause_reason: str
    source_switch: str = ""
    failover_event: str = ""
    recovery_event: str = ""
    recovery_ready: bool = False
    recovery_seconds: Decimal | None = None
    time_using: dict[str, Decimal] = field(default_factory=dict)
    time_paused: Decimal = ZERO

    def as_result(self) -> dict[str, Any]:
        selected = self.selected_fair_value
        selected_value = selected.fair_value if selected else None
        deviations = {
            venue: (value.fair_value / selected_value - Decimal("1")) * BPS
            for venue, value in self.source_values.items()
            if selected_value and selected_value > ZERO
        }
        return {
            "selected_source": self.selected_venue,
            "selected_reference": self.selected_venue,
            "selected_fair_value": selected_value,
            "source_fair_values": {venue: value.fair_value for venue, value in self.source_values.items()},
            "source_mids": {venue: value.mid for venue, value in self.source_values.items()},
            "source_microprices": {venue: value.microprice for venue, value in self.source_values.items()},
            "valid_sources": list(self.fresh_venues),
            "fresh_sources": list(self.fresh_venues),
            "outliers": [],
            "deviations_bps": deviations,
            "robust_median": None,
            "fair_value": None if self.pause_reason else selected_value,
            "dispersion_bps": self.dispersion_bps,
            "source_count": len(self.fresh_venues),
            "confidence": "PAUSED" if self.pause_reason else f"PRIMARY_{self.selected_venue.upper()}",
            "pause_reason": self.pause_reason,
            "source_health": self.source_health,
            "priority_mode": True,
            "priority_event": self.source_switch,
            "failover_event": self.failover_event,
            "recovery_event": self.recovery_event,
            "recovery_ready": self.recovery_ready,
            "recovery_seconds": self.recovery_seconds,
            "time_using": self.time_using,
            "time_paused": self.time_paused,
        }


@dataclass
class _SelectionState:
    initialized: bool = False
    current: str | None = None
    last_timestamp: float | None = None
    fresh_previous: dict[str, bool] = field(default_factory=dict)
    healthy_previous: dict[str, bool] = field(default_factory=dict)
    recovery_since: dict[str, float | None] = field(default_factory=dict)
    time_using: dict[str, Decimal] = field(default_factory=lambda: defaultdict(lambda: ZERO))
    time_paused: Decimal = ZERO


class PriorityReferenceSelector:
    """Select Binance, then Bybit, then OKX without averaging primary prices."""

    def __init__(
        self,
        *,
        priority: tuple[str, ...] = ("binance", "bybit", "okx"),
        recovery_min_healthy_seconds: float = 3.0,
    ) -> None:
        if not priority:
            raise ValueError("priority must contain at least one venue")
        if len(set(priority)) != len(priority):
            raise ValueError("priority cannot contain duplicates")
        self.priority = tuple(priority)
        self.recovery_min_healthy_seconds = float(recovery_min_healthy_seconds)
        self._states: dict[str, _SelectionState] = {}

    def _state(self, asset: str) -> _SelectionState:
        return self._states.setdefault(asset, _SelectionState())

    def _advance_clock(self, state: _SelectionState, now: float) -> None:
        if state.last_timestamp is None:
            state.last_timestamp = now
            return
        elapsed = Decimal(str(max(0.0, now - state.last_timestamp)))
        if state.current is None:
            state.time_paused += elapsed
        else:
            state.time_using[state.current] += elapsed
        state.last_timestamp = now

    def _recovery_ready(self, state: _SelectionState, venue: str, now: float) -> tuple[bool, Decimal | None]:
        since = state.recovery_since.get(venue)
        if since is None:
            return False, None
        elapsed = Decimal(str(max(0.0, now - since)))
        return elapsed >= Decimal(str(self.recovery_min_healthy_seconds)), elapsed

    def select(
        self,
        asset: str,
        *,
        books: dict[str, BookSnapshot],
        health: dict[str, SourceHealth],
        now: float,
        healthy_seconds: float,
        stale_seconds: float,
        stale_overrides: dict[str, float] | None = None,
        mid_weight: Decimal = Decimal("0.5"),
        microprice_weight: Decimal = Decimal("0.5"),
        max_levels: int = 5,
    ) -> PrioritySelection:
        """Return a fresh primary source and diagnostic source observations."""

        state = self._state(asset)
        self._advance_clock(state, now)
        was_initialized = state.initialized
        overrides = stale_overrides or {}
        source_health: dict[str, dict[str, Any]] = {}
        source_values: dict[str, SourceFairValue] = {}
        fresh: list[str] = []
        healthy: set[str] = set()
        for venue, source in health.items():
            limit = float(overrides.get(venue, stale_seconds))
            snapshot = source.snapshot(now, min(healthy_seconds, limit), limit)
            source_health[venue] = snapshot
            if snapshot["health"] in FRESH_HEALTH and source.book is not None and venue in books:
                value = source_fair_value(
                    venue,
                    books[venue],
                    mid_weight=mid_weight,
                    microprice_weight=microprice_weight,
                    max_levels=max_levels,
                    now=now,
                    health=snapshot["health"],
                )
                source_values[venue] = value
                fresh.append(venue)
                if snapshot["health"] == "HEALTHY":
                    healthy.add(venue)

        fresh_set = set(fresh)
        for venue in health:
            is_fresh = venue in fresh_set
            if venue == "binance":
                was_healthy = state.healthy_previous.get(venue, False)
                is_healthy = venue in healthy
                if is_healthy and not was_healthy:
                    state.recovery_since[venue] = now
                elif not is_healthy:
                    state.recovery_since[venue] = None
                state.healthy_previous[venue] = is_healthy
            state.fresh_previous[venue] = is_fresh

        previous = state.current
        chosen: str | None = None
        recovery_ready = False
        recovery_seconds: Decimal | None = None
        if not state.initialized:
            chosen = next((venue for venue in self.priority if venue in fresh_set), None)
        elif previous in fresh_set:
            if previous != "binance" and "binance" in fresh_set:
                recovery_ready, recovery_seconds = self._recovery_ready(state, "binance", now)
                if recovery_ready:
                    chosen = "binance"
                elif previous != "bybit" and "bybit" in fresh_set:
                    chosen = "bybit"
                else:
                    chosen = previous
            elif previous != "binance" and "bybit" in fresh_set and previous != "bybit":
                chosen = "bybit"
            else:
                chosen = previous
        else:
            for venue in self.priority:
                if venue not in fresh_set:
                    continue
                if venue == "binance" and previous != "binance":
                    recovery_ready, recovery_seconds = self._recovery_ready(state, "binance", now)
                    if not recovery_ready:
                        continue
                chosen = venue
                break

        dispersion_bps: Decimal | None = None
        pause_reason = ""
        if len(source_values) >= 2:
            prices = [value.fair_value for value in source_values.values()]
            low, high = min(prices), max(prices)
            center = (low + high) / Decimal("2")
            dispersion_bps = (high - low) / center * BPS if center > ZERO else None
            if dispersion_bps is not None and dispersion_bps > Decimal("0"):
                # The caller applies the configured threshold. Keeping the
                # raw dispersion here makes this object useful for diagnostics.
                pass

        source_switch = ""
        failover_event = ""
        recovery_event = ""
        if chosen is None:
            pause_reason = "NO_FRESH_REFERENCE"
        if was_initialized and chosen != previous:
            source_switch = f"{previous or 'PAUSE'}_TO_{chosen or 'PAUSE'}"
            if previous in self.priority and chosen in self.priority:
                if self.priority.index(chosen) > self.priority.index(previous):
                    failover_event = source_switch
                if chosen == "binance" and previous != "binance":
                    recovery_event = "RECOVERY_TO_BINANCE"
            elif chosen == "binance" and previous != "binance":
                recovery_event = "RECOVERY_TO_BINANCE"

        state.current = chosen
        state.initialized = True
        selected = source_values.get(chosen) if chosen else None
        return PrioritySelection(
            asset=asset,
            selected_venue=chosen,
            selected_book=books.get(chosen) if chosen else None,
            selected_fair_value=selected,
            fresh_venues=tuple(fresh),
            source_values=source_values,
            source_health=source_health,
            dispersion_bps=dispersion_bps,
            pause_reason=pause_reason,
            source_switch=source_switch,
            failover_event=failover_event,
            recovery_event=recovery_event,
            recovery_ready=recovery_ready,
            recovery_seconds=recovery_seconds,
            time_using=dict(state.time_using),
            time_paused=state.time_paused,
        )

    def apply_disagreement_pause(
        self,
        selection: PrioritySelection,
        *,
        disagreement_pause_bps: Decimal,
    ) -> PrioritySelection:
        """Pause the asset after selection when fresh sanity-check values diverge."""

        if (
            selection.dispersion_bps is None
            or selection.dispersion_bps <= disagreement_pause_bps
            or selection.pause_reason
        ):
            return selection
        return PrioritySelection(
            **{
                **selection.__dict__,
                "pause_reason": "REFERENCE_DISAGREEMENT_PAUSE",
            }
        )
