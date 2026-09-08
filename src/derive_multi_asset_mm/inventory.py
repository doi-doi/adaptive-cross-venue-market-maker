"""Per-asset inventory state and bounded local skew."""

from __future__ import annotations

from decimal import Decimal

from .models import ZERO, DirectionState, InventoryMode, InventorySnapshot, Side


def clamp(value: Decimal, lower: Decimal = Decimal("-1"), upper: Decimal = Decimal("1")) -> Decimal:
    return max(lower, min(upper, value))


def classify_inventory(
    amount: Decimal,
    mid_price: Decimal,
    max_inventory_notional: Decimal,
    *,
    flat_band: Decimal = Decimal("0.10"),
    skew_enter: Decimal = Decimal("0.15"),
    one_sided_enter: Decimal = Decimal("0.50"),
) -> InventorySnapshot:
    if max_inventory_notional <= ZERO or mid_price <= ZERO:
        raise ValueError("inventory limits and prices must be positive")
    position_notional = amount * mid_price
    ratio = clamp(position_notional / max_inventory_notional)
    absolute = abs(ratio)
    if absolute >= one_sided_enter:
        mode = InventoryMode.ASK_ONLY if ratio > ZERO else InventoryMode.BID_ONLY
    elif absolute >= skew_enter:
        mode = InventoryMode.LONG_SKEW if ratio > ZERO else InventoryMode.SHORT_SKEW
    elif absolute <= flat_band:
        mode = InventoryMode.FLAT
    else:
        mode = InventoryMode.LONG_SKEW if ratio > ZERO else InventoryMode.SHORT_SKEW
    return InventorySnapshot(
        amount=amount,
        mid_price=mid_price,
        position_notional=position_notional,
        ratio=ratio,
        mode=mode,
    )


def local_inventory_skew_bps(snapshot: InventorySnapshot, maximum_bps: Decimal) -> Decimal:
    """Return a reservation-price shift: long is negative, short is positive."""

    return -snapshot.ratio * max(ZERO, maximum_bps)


def side_allowed(mode: InventoryMode, side: Side) -> bool:
    if mode == InventoryMode.ASK_ONLY:
        return side == Side.SELL
    if mode == InventoryMode.BID_ONLY:
        return side == Side.BUY
    return True


def directional_skew_bps(direction: DirectionState, maximum_bps: Decimal) -> Decimal:
    maximum_bps = max(ZERO, maximum_bps)
    if direction == DirectionState.BULLISH:
        return maximum_bps
    if direction == DirectionState.BEARISH:
        return -maximum_bps
    return ZERO
