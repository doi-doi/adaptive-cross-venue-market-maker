"""Shared portfolio exposure and deterministic opportunity scoring."""

from __future__ import annotations

from decimal import Decimal

from .inventory import clamp
from .models import ZERO, InventorySnapshot


def aggregate_exposure(inventories: dict[str, InventorySnapshot]) -> dict[str, Decimal]:
    gross = sum((abs(value.position_notional) for value in inventories.values()), ZERO)
    net = sum((value.position_notional for value in inventories.values()), ZERO)
    return {"gross_inventory": gross, "net_inventory": net}


def portfolio_skew_bps(
    inventories: dict[str, InventorySnapshot],
    max_portfolio_inventory: Decimal,
    maximum_bps: Decimal,
) -> Decimal:
    if max_portfolio_inventory <= ZERO:
        raise ValueError("max_portfolio_inventory must be positive")
    net = sum((value.position_notional for value in inventories.values()), ZERO)
    return -clamp(net / max_portfolio_inventory) * max(ZERO, maximum_bps)


def capital_efficiency(minimum_notional: Decimal, capital_usdc: Decimal) -> Decimal:
    if minimum_notional <= ZERO or capital_usdc <= ZERO:
        return ZERO
    return clamp(Decimal("1") - minimum_notional / capital_usdc, ZERO, Decimal("1"))


def opportunity_score(
    *,
    fair_value_edge_bps: Decimal,
    derive_spread_bps: Decimal,
    activity: Decimal,
    reference_health: Decimal,
    capital_efficiency_value: Decimal,
    quote_churn: Decimal,
    toxicity: Decimal,
) -> Decimal:
    """An interpretable 0-100 score; it is not a PnL optimizer."""

    edge_component = clamp(fair_value_edge_bps / Decimal("20"), ZERO, Decimal("1"))
    spread_component = clamp(derive_spread_bps / Decimal("20"), ZERO, Decimal("1"))
    activity_component = clamp(activity, ZERO, Decimal("1"))
    churn_component = Decimal("1") - clamp(quote_churn, ZERO, Decimal("1"))
    toxicity_component = Decimal("1") - clamp(toxicity, ZERO, Decimal("1"))
    value = (
        edge_component * Decimal("0.25")
        + spread_component * Decimal("0.20")
        + activity_component * Decimal("0.15")
        + clamp(reference_health, ZERO, Decimal("1")) * Decimal("0.15")
        + clamp(capital_efficiency_value, ZERO, Decimal("1")) * Decimal("0.15")
        + churn_component * Decimal("0.05")
        + toxicity_component * Decimal("0.05")
    )
    return value * Decimal("100")
