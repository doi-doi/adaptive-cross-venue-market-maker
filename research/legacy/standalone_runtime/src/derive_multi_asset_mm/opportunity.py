"""Small, interpretable opportunity-score adapter."""

from __future__ import annotations

from decimal import Decimal

from .models import ZERO, MarketState, QuotePlan
from .portfolio import capital_efficiency, opportunity_score


def score_asset(plan: QuotePlan, state: MarketState, health: str, capital_usdc: Decimal) -> Decimal:
    reference_health = Decimal("1") if health == "HEALTHY" else ZERO
    edge = max(plan.buy_edge_bps, plan.sell_edge_bps)
    spread = plan.edge.total_required_bps
    activity = Decimal("1") if state.market_mode.value != "PAUSED" else ZERO
    return opportunity_score(
        fair_value_edge_bps=edge,
        derive_spread_bps=spread,
        activity=activity,
        reference_health=reference_health,
        capital_efficiency_value=capital_efficiency(max(plan.bid_notional, plan.ask_notional), capital_usdc),
        quote_churn=ZERO,
        toxicity=ZERO,
    )
