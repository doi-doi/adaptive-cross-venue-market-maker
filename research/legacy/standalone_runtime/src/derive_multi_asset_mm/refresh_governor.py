"""Action governance used by the refresh-deadband research phase.

The governor is intentionally independent of market-data cadence.  It only
controls order mutations, preserves emergency cancellation priority, and
coalesces pending normal requests by asset/side so the latest desired quote
wins.  It is suitable for the exchange-free shadow path and has no connector
or private-API dependency.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import IntEnum

from .models import ZERO, Side

DEADBAND_GRID_BPS = (Decimal("0"), Decimal("2"), Decimal("5"), Decimal("10"), Decimal("15"), Decimal("20"), Decimal("30"))
RESIDENCY_GRID_SECONDS = (
    Decimal("0"),
    Decimal("0.5"),
    Decimal("1"),
    Decimal("2"),
    Decimal("3"),
    Decimal("5"),
)
CHURN_LOOKBACKS_MS = (100, 250, 500, 1000, 2000, 5000)


class ActionPriority(IntEnum):
    EMERGENCY_CANCEL = 0
    RISK_REDUCTION = 1
    STALE_REPLACEMENT = 2
    NORMAL_REFRESH = 3


@dataclass(frozen=True)
class ActionRequest:
    """A candidate order mutation before exchange-side acknowledgement."""

    asset: str
    side: str
    action: str
    reason: str
    priority: ActionPriority
    timestamp: float
    order_id: str | None = None
    price: Decimal | None = None
    amount: Decimal = ZERO

    @property
    def key(self) -> tuple[str, str]:
        return (self.asset.upper(), self.side.upper())


def action_priority(action: str, reason: str) -> ActionPriority:
    """Map lifecycle reasons to the required safety ordering."""

    action = str(action).upper()
    reason = str(reason).upper()
    if action == "CANCEL" and (
        reason.startswith("FAST_ADVERSE")
        or reason.startswith("EMERGENCY")
        or reason in {"STALE_PROTECTIVE_CANCEL", "PROTECTIVE_RISK_CANCEL"}
    ):
        return ActionPriority.EMERGENCY_CANCEL
    if action == "CANCEL" and reason in {
        "PROTECTIVE_PLAN_INVALID",
        "PAUSED",
        "INVENTORY_RISK",
        "RISK_REDUCTION",
    }:
        return ActionPriority.RISK_REDUCTION
    if action in {"CANCEL", "REPLACE"}:
        return ActionPriority.STALE_REPLACEMENT
    return ActionPriority.NORMAL_REFRESH


def is_adverse_fast_move(side: str | Side, signed_return_bps: Decimal, threshold_bps: Decimal) -> bool:
    """Return true when a fast move makes the quoted side vulnerable."""

    if threshold_bps <= ZERO or abs(signed_return_bps) < threshold_bps:
        return False
    side_name = side.value if isinstance(side, Side) else str(side).upper()
    return (side_name == Side.BUY.value and signed_return_bps < ZERO) or (
        side_name == Side.SELL.value and signed_return_bps > ZERO
    )


class OrderActionGovernor:
    """Rolling global/instrument action budget with latest-request coalescing."""

    def __init__(
        self,
        *,
        max_actions_per_second: Decimal = Decimal("1"),
        max_actions_per_minute: int = 30,
        max_actions_per_instrument_per_second: Decimal = Decimal("1"),
        emergency_cancel_budget_per_minute: int = 6,
    ) -> None:
        if max_actions_per_second <= ZERO or max_actions_per_instrument_per_second <= ZERO:
            raise ValueError("action budgets per second must be positive")
        if max_actions_per_minute < 1 or emergency_cancel_budget_per_minute < 1:
            raise ValueError("action budgets per minute must be positive")
        self.max_actions_per_second = max_actions_per_second
        self.max_actions_per_minute = int(max_actions_per_minute)
        self.max_actions_per_instrument_per_second = max_actions_per_instrument_per_second
        self.emergency_cancel_budget_per_minute = int(emergency_cancel_budget_per_minute)
        self._timestamps: deque[float] = deque()
        self._second_timestamps: deque[float] = deque()
        self._instrument_timestamps: dict[str, deque[float]] = {}
        self._emergency_timestamps: deque[float] = deque()
        self._pending: dict[tuple[str, str], ActionRequest] = {}
        self._accepted = 0
        self._rejected = 0
        self._coalesced = 0
        self._emergency_accepted = 0
        self._emergency_over_reserve = 0

    def submit(self, request: ActionRequest) -> None:
        """Coalesce by asset/side, retaining higher-priority safety work."""

        key = request.key
        previous = self._pending.get(key)
        if previous is None:
            self._pending[key] = request
            return
        if request.priority < previous.priority or request.priority == previous.priority:
            self._pending[key] = request
            self._coalesced += 1
        else:
            self._coalesced += 1

    def pending(self) -> tuple[ActionRequest, ...]:
        return tuple(self._pending.values())

    def latest(self, key: tuple[str, str]) -> ActionRequest | None:
        """Return the current coalesced request for one asset/side."""

        return self._pending.get((str(key[0]).upper(), str(key[1]).upper()))

    def clear_non_safety(self, key: tuple[str, str]) -> None:
        """Drop obsolete refresh work while retaining emergency/risk work."""

        normalized = (str(key[0]).upper(), str(key[1]).upper())
        request = self._pending.get(normalized)
        if request is not None and request.priority >= ActionPriority.STALE_REPLACEMENT:
            self._pending.pop(normalized, None)

    def allow(self, request: ActionRequest, now: float) -> bool:
        """Check a request without recording it as accepted."""

        self._prune(now)
        if request.priority <= ActionPriority.RISK_REDUCTION:
            return True
        if Decimal(len(self._second_timestamps) + 1) > self.max_actions_per_second:
            return False
        if len(self._timestamps) + 1 > self.max_actions_per_minute:
            return False
        instrument_count = len(self._instrument_timestamps.get(request.asset.upper(), ()))
        return Decimal(instrument_count + 1) <= self.max_actions_per_instrument_per_second

    def record(self, request: ActionRequest, now: float) -> None:
        """Record an accepted mutation and consume its pending key."""

        self._prune(now)
        self._timestamps.append(now)
        self._second_timestamps.append(now)
        self._instrument_timestamps.setdefault(request.asset.upper(), deque()).append(now)
        self._accepted += 1
        if request.priority == ActionPriority.EMERGENCY_CANCEL:
            self._emergency_timestamps.append(now)
            self._emergency_accepted += 1
            if len(self._emergency_timestamps) > self.emergency_cancel_budget_per_minute:
                self._emergency_over_reserve += 1
        self._pending.pop(request.key, None)

    def drain(self, now: float) -> tuple[ActionRequest, ...]:
        """Return accepted pending requests in priority order.

        Requests rejected by the normal budget remain pending so the next
        latest-desired submission can be considered when capacity returns.
        """

        accepted: list[ActionRequest] = []
        for request in sorted(self._pending.values(), key=lambda item: (item.priority, item.timestamp)):
            if not self.allow(request, now):
                self._rejected += 1
                continue
            self.record(request, now)
            accepted.append(request)
        return tuple(accepted)

    def snapshot(self, now: float) -> dict[str, object]:
        self._prune(now)
        return {
            "actions_last_1s": len(self._second_timestamps),
            "actions_last_60s": len(self._timestamps),
            "instrument_actions_last_1s": {
                asset: len(timestamps) for asset, timestamps in self._instrument_timestamps.items() if timestamps
            },
            "pending_requests": len(self._pending),
            "accepted": self._accepted,
            "rejected": self._rejected,
            "coalesced": self._coalesced,
            "emergency_accepted": self._emergency_accepted,
            "emergency_over_reserve": self._emergency_over_reserve,
            "max_actions_per_second": str(self.max_actions_per_second),
            "max_actions_per_minute": self.max_actions_per_minute,
            "max_actions_per_instrument_per_second": str(self.max_actions_per_instrument_per_second),
            "emergency_cancel_budget_per_minute": self.emergency_cancel_budget_per_minute,
        }

    def _prune(self, now: float) -> None:
        cutoff_second = now - 1.0
        cutoff_minute = now - 60.0
        while self._timestamps and self._timestamps[0] < cutoff_minute:
            self._timestamps.popleft()
        while self._emergency_timestamps and self._emergency_timestamps[0] < cutoff_minute:
            self._emergency_timestamps.popleft()
        for asset, timestamps in list(self._instrument_timestamps.items()):
            while timestamps and timestamps[0] < cutoff_second:
                timestamps.popleft()
            if not timestamps:
                self._instrument_timestamps.pop(asset, None)
        # The global second budget needs a separate one-second view; keeping
        # it in the same deque would make the minute count incorrect.
        while self._second_timestamps and self._second_timestamps[0] < cutoff_second:
            self._second_timestamps.popleft()


def requests_from_actions(rows: Iterable[dict[str, object]]) -> list[ActionRequest]:
    """Convert telemetry rows into governor requests for audit/test tooling."""

    result: list[ActionRequest] = []
    for row in rows:
        result.append(
            ActionRequest(
                asset=str(row.get("asset") or "").upper(),
                side=str(row.get("side") or "").upper(),
                action=str(row.get("action") or "").upper(),
                reason=str(row.get("reason") or ""),
                priority=action_priority(str(row.get("action") or ""), str(row.get("reason") or "")),
                timestamp=float(row.get("timestamp") or 0),
                order_id=str(row.get("order_id")) if row.get("order_id") is not None else None,
            )
        )
    return result
