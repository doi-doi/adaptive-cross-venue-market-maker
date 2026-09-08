"""Explicit per-side quote reconciliation state machine."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .models import ZERO, LifecycleState, Side


@dataclass(frozen=True)
class ShadowOrder:
    order_id: str
    asset: str
    side: Side
    price: Decimal
    amount: Decimal
    created_at: float


@dataclass(frozen=True)
class LifecycleAction:
    kind: str
    asset: str
    side: Side
    reason: str
    order_id: str | None = None
    price: Decimal | None = None
    amount: Decimal = ZERO


@dataclass
class SideLifecycle:
    state: LifecycleState = LifecycleState.NO_ORDER
    order: ShadowOrder | None = None
    pending_order: ShadowOrder | None = None


class QuoteReconciler:
    """Reconcile exactly one bid and one ask per asset without duplicate creates."""

    def __init__(self) -> None:
        self._states: dict[tuple[str, Side], SideLifecycle] = {}
        self._sequence = 0

    def state(self, asset: str, side: Side) -> SideLifecycle:
        return self._states.setdefault((asset, side), SideLifecycle())

    def reconcile(
        self,
        *,
        asset: str,
        side: Side,
        desired_price: Decimal | None,
        desired_amount: Decimal,
        now: float,
        max_age_seconds: Decimal,
        tolerance_bps: Decimal,
        paused: bool = False,
    ) -> LifecycleAction:
        lifecycle = self.state(asset, side)
        current = lifecycle.order
        if lifecycle.state in {LifecycleState.CANCEL_REQUESTED, LifecycleState.WAITING_CANCEL}:
            lifecycle.state = LifecycleState.WAITING_CANCEL
            return LifecycleAction("HOLD", asset, side, "WAITING_CANCEL", current.order_id if current else None)
        if current is not None:
            needs_refresh = paused or desired_price is None or desired_amount <= ZERO
            if not needs_refresh:
                movement_bps = abs(desired_price - current.price) / current.price * Decimal("10000")
                age = Decimal(str(max(0.0, now - current.created_at)))
                needs_refresh = age >= max_age_seconds or movement_bps >= tolerance_bps
            if needs_refresh:
                lifecycle.state = LifecycleState.CANCEL_REQUESTED
                return LifecycleAction("CANCEL", asset, side, "REFRESH_NEEDED" if not paused else "PAUSED", current.order_id)
            lifecycle.state = LifecycleState.ACTIVE_MATCHING
            return LifecycleAction("HOLD", asset, side, "NO_OP_HOLD", current.order_id, current.price, current.amount)
        if desired_price is None or desired_amount <= ZERO or paused:
            lifecycle.state = LifecycleState.NO_ORDER
            return LifecycleAction("HOLD", asset, side, "NO_DESIRED_QUOTE")
        self._sequence += 1
        pending = ShadowOrder(
            order_id=f"shadow-{self._sequence}",
            asset=asset,
            side=side,
            price=desired_price,
            amount=desired_amount,
            created_at=now,
        )
        lifecycle.pending_order = pending
        lifecycle.state = LifecycleState.CREATE_REQUESTED
        return LifecycleAction("CREATE", asset, side, "READY_TO_CREATE", pending.order_id, desired_price, desired_amount)

    def acknowledge_create(self, action: LifecycleAction) -> ShadowOrder:
        if action.kind != "CREATE" or action.order_id is None or action.price is None:
            raise ValueError("acknowledge_create expects a CREATE action")
        lifecycle = self.state(action.asset, action.side)
        if lifecycle.state != LifecycleState.CREATE_REQUESTED or lifecycle.pending_order is None:
            raise RuntimeError("create acknowledgement is not expected")
        order = lifecycle.pending_order
        lifecycle.order = order
        lifecycle.pending_order = None
        lifecycle.state = LifecycleState.ACTIVE_MATCHING
        return order

    def acknowledge_cancel(self, action: LifecycleAction) -> None:
        if action.kind != "CANCEL":
            raise ValueError("acknowledge_cancel expects a CANCEL action")
        lifecycle = self.state(action.asset, action.side)
        lifecycle.order = None
        lifecycle.pending_order = None
        lifecycle.state = LifecycleState.READY_TO_CREATE

    def remove_filled_order(self, asset: str, side: Side, order_id: str) -> None:
        lifecycle = self.state(asset, side)
        if lifecycle.order is not None and lifecycle.order.order_id == order_id:
            lifecycle.order = None
            lifecycle.state = LifecycleState.READY_TO_CREATE

    def active_orders(self) -> list[ShadowOrder]:
        return [value.order for value in self._states.values() if value.order is not None]
