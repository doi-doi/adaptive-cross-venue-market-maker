"""Fail-closed rounded-order validation and rolling action-rate controls."""

from __future__ import annotations

from collections import deque
from decimal import Decimal

from .models import ZERO, DeriveRules, RiskDecision, Side
from .quote_engine import round_down, round_up


def rounded_order(
    *,
    price: Decimal,
    amount: Decimal,
    side: Side,
    rules: DeriveRules,
) -> tuple[Decimal, Decimal]:
    rounded_price = round_down(price, rules.tick_size) if side == Side.BUY else round_up(price, rules.tick_size)
    rounded_amount = round_down(amount, rules.amount_step)
    return rounded_price, rounded_amount


def validate_rounded_order(
    *,
    price: Decimal,
    amount: Decimal,
    side: Side,
    rules: DeriveRules,
    max_single_order_notional: Decimal,
    current_position_amount: Decimal = ZERO,
    max_inventory_notional: Decimal = Decimal("1e18"),
    portfolio_gross_inventory: Decimal = ZERO,
    max_portfolio_inventory: Decimal = Decimal("1e18"),
) -> RiskDecision:
    rounded_price, rounded_amount = rounded_order(price=price, amount=amount, side=side, rules=rules)
    notional = rounded_price * rounded_amount
    if rounded_price <= ZERO or rounded_amount <= ZERO:
        return RiskDecision(False, "ROUNDED_ORDER_NON_POSITIVE", notional, rounded_price, rounded_amount)
    if rounded_amount < rules.minimum_amount:
        return RiskDecision(False, "ROUNDED_AMOUNT_BELOW_MINIMUM", notional, rounded_price, rounded_amount)
    if rules.maximum_amount is not None and rounded_amount > rules.maximum_amount:
        return RiskDecision(False, "ROUNDED_AMOUNT_ABOVE_MAXIMUM", notional, rounded_price, rounded_amount)
    if rules.minimum_notional > ZERO and notional < rules.minimum_notional:
        return RiskDecision(False, "ROUNDED_NOTIONAL_BELOW_MINIMUM", notional, rounded_price, rounded_amount)
    if notional > max_single_order_notional:
        return RiskDecision(False, "MAX_SINGLE_ORDER_NOTIONAL", notional, rounded_price, rounded_amount)
    next_position = (current_position_amount + rounded_amount) * rounded_price if side == Side.BUY else (current_position_amount - rounded_amount) * rounded_price
    if abs(next_position) > max_inventory_notional:
        return RiskDecision(False, "MAX_ASSET_INVENTORY", notional, rounded_price, rounded_amount)
    if portfolio_gross_inventory + notional > max_portfolio_inventory:
        return RiskDecision(False, "MAX_PORTFOLIO_INVENTORY", notional, rounded_price, rounded_amount)
    return RiskDecision(True, "VALID", notional, rounded_price, rounded_amount)


class ActionRateWindow:
    """True rolling-window action count; no per-cycle counter ambiguity."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds = window_seconds
        self._timestamps: deque[float] = deque()

    def record(self, timestamp: float) -> None:
        self._timestamps.append(timestamp)
        self._prune(timestamp)

    def count(self, now: float) -> int:
        self._prune(now)
        return len(self._timestamps)

    def allowed(self, now: float, maximum: int, additional: int = 1) -> bool:
        return self.count(now) + additional <= maximum

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
