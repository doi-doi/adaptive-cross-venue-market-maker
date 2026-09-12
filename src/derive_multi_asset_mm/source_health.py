"""Independent source health. Disconnect invalidates the prior book immediately."""

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from statistics import median

from .models import BookSnapshot


@dataclass
class SourceHealth:
    available: bool = True
    connected: bool = False
    book: BookSnapshot | None = None
    last_message: float | None = None
    last_accepted: float | None = None
    sequence: int | None = None
    intervals: deque = field(default_factory=lambda: deque(maxlen=1000))
    rejected_messages: int = 0
    parse_failures: int = 0
    duplicates: int = 0
    out_of_order: int = 0
    sequence_gaps: int = 0
    reconnects: int = 0
    updates: int = 0
    disconnected_at: float | None = None
    disconnect_duration: float = 0
    sequence_policy: str = "MONOTONIC_ONLY"

    def record_parse_failure(self, timestamp: float | None = None) -> None:
        self.parse_failures += 1
        if timestamp is not None:
            self.last_message = timestamp

    def record_rejection(self, timestamp: float | None = None) -> None:
        self.rejected_messages += 1
        if timestamp is not None:
            self.last_message = timestamp

    def disconnect(self, now: float) -> None:
        self.connected = False
        self.book = None
        self.sequence = None
        if self.disconnected_at is None:
            self.disconnected_at = now

    def connect(self, now: float) -> None:
        if self.disconnected_at is not None:
            self.disconnect_duration += max(0, now - self.disconnected_at)
            self.reconnects += 1
        self.disconnected_at = None
        self.connected = True
        self.book = None
        self.sequence = None

    def accept(self, book: BookSnapshot, sequence: int | None = None, previous: int | None = None,
               verified_snapshot_repeat: bool = False) -> bool:
        self.last_message = book.timestamp
        if not self.connected or not book.valid():
            self.rejected_messages += 1
            return False
        if self.sequence is not None and sequence is not None:
            if sequence <= self.sequence:
                if sequence == self.sequence:
                    self.duplicates += 1
                    if verified_snapshot_repeat and self.book is not None and book.timestamp > self.book.timestamp:
                        if (book.best_bid, book.best_ask, book.bid_size, book.ask_size) == (self.book.best_bid, self.book.best_ask, self.book.bid_size, self.book.ask_size):
                            self.intervals.append(book.timestamp-self.book.timestamp)
                            self.book = book
                            self.last_accepted = book.timestamp
                            self.updates += 1
                            return True
                else:
                    self.out_of_order += 1
                self.rejected_messages += 1
                return False
            # Filtered snapshot streams legitimately skip sequence numbers.
            # Only an exchange-provided previous-sequence link proves a gap.
            if previous is not None and previous != self.sequence:
                self.sequence_gaps += 1
        if self.book is not None:
            if book.timestamp <= self.book.timestamp:
                self.out_of_order += 1
                self.rejected_messages += 1
                return False
            self.intervals.append(book.timestamp - self.book.timestamp)
        self.book = book
        self.sequence = sequence
        self.last_accepted = book.timestamp
        self.updates += 1
        return True

    def status(self, now: float, healthy: float = 2, stale: float = 5) -> str:
        if not self.available:
            return "UNAVAILABLE"
        if not self.connected or self.book is None:
            return "STALE"
        age = max(0, now - self.book.timestamp)
        return "STALE" if age > stale else "DEGRADED" if age > healthy else "HEALTHY"

    def snapshot(self, now: float, healthy: float = 2, stale: float = 5) -> dict:
        intervals = sorted(self.intervals)
        return {
            "health": self.status(now, healthy, stale), "connected": self.connected,
            "last_message_timestamp": self.last_message,
            "last_accepted_timestamp": self.last_accepted,
            "bbo_age": max(0, now - self.book.timestamp) if self.book else None,
            "median_update_interval": median(intervals) if intervals else None,
            "p99_update_interval": intervals[min(len(intervals)-1, int(len(intervals)*.99))] if intervals else None,
            "maximum_recent_gap": max(intervals) if intervals else None,
            "updates": self.updates, "reconnect_count": self.reconnects,
            "sequence_gaps": self.sequence_gaps, "sequence_policy": self.sequence_policy,
            "duplicates": self.duplicates, "out_of_order": self.out_of_order,
            "rejected_messages": self.rejected_messages, "parse_failures": self.parse_failures,
            "disconnect_duration": self.disconnect_duration + (now-self.disconnected_at if self.disconnected_at is not None else 0),
        }


def consensus(sources: dict[str, SourceHealth], now: float, *, stale: float = 5,
              healthy: float = 2, outlier_bps: Decimal = Decimal("50"),
              disagreement_bps: Decimal = Decimal("25"), minimum_sources: int = 1,
              mid_weight: Decimal = Decimal("0.5"),
              micro_weight: Decimal = Decimal("0.5"), overrides: dict | None = None) -> dict:
    from .reference import microprice, mid_price, weighted_fair_value

    prices = {}
    mids = {}
    microprices = {}
    health = {}
    for venue, source in sources.items():
        limit = float((overrides or {}).get(venue, stale))
        health[venue] = source.snapshot(now, min(healthy, limit), limit)
        if health[venue]["health"] in {"HEALTHY", "DEGRADED"} and source.book is not None:
            prices[venue] = weighted_fair_value(source.book, mid_weight, micro_weight)
            mids[venue] = mid_price(source.book)
            microprices[venue] = microprice(source.book)
    center = median(prices.values()) if prices else None
    deviations = {v: (p/center-1)*10000 for v, p in prices.items()} if center else {}
    # With only two sources there is no majority to identify the bad source.
    outliers = {v: prices[v] for v, dev in deviations.items() if len(prices) >= 3 and abs(dev) > outlier_bps}
    valid = {v: p for v, p in prices.items() if v not in outliers}
    center = median(valid.values()) if valid else None
    dispersion = (max(valid.values())-min(valid.values()))/center*10000 if center else None
    count = len(valid)
    reason = ""
    if count < minimum_sources:
        reason = "NO_FRESH_REFERENCE" if count == 0 else "INSUFFICIENT_REFERENCE_SOURCES"
    elif dispersion is not None and dispersion > disagreement_bps:
        reason = "REFERENCE_DISAGREEMENT_PAUSE"
    confidence = "FULL_REFERENCE_CONFIDENCE" if count >= 3 else "NORMAL_REFERENCE_CONFIDENCE" if count == 2 else "SINGLE_SOURCE_REFERENCE" if count == 1 else "PAUSED"
    return {"source_fair_values": prices, "source_mids": mids, "source_microprices": microprices,
            "valid_sources": list(valid), "outliers": outliers,
            "deviations_bps": deviations, "robust_median": center,
            "fair_value": None if reason else center, "dispersion_bps": dispersion,
            "source_count": count, "confidence": confidence, "pause_reason": reason,
            "source_health": health}
