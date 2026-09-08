from decimal import Decimal

from derive_multi_asset_mm.inventory import classify_inventory
from derive_multi_asset_mm.models import (
    AssetSpec,
    BookSnapshot,
    DeriveRules,
    DirectionState,
    FairValue,
    InventoryMode,
    MarketMode,
    MarketState,
    QuotePlacement,
    Side,
)
from derive_multi_asset_mm.quote_engine import QuoteInputs, build_quote_plan
from derive_multi_asset_mm.risk import validate_rounded_order


def derive_book() -> BookSnapshot:
    return BookSnapshot(
        timestamp=100,
        best_bid=Decimal("99.90"),
        best_ask=Decimal("100.10"),
        bid_size=Decimal("10"),
        ask_size=Decimal("10"),
        bids=((Decimal("99.90"), Decimal("10")),),
        asks=((Decimal("100.10"), Decimal("10")),),
    )


def fair() -> FairValue:
    return FairValue(Decimal("100"), Decimal("100"), Decimal("0"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("100"))


def state(mode: MarketMode = MarketMode.NORMAL) -> MarketState:
    return MarketState(mode, DirectionState.NEUTRAL, __import__("derive_multi_asset_mm.models", fromlist=["VolatilityState"]).VolatilityState.NORMAL_VOL)


def rules() -> DeriveRules:
    return DeriveRules("SOL-PERP", "SOL", "USDC", Decimal("0.01"), Decimal("0.1"), Decimal("0.1"), minimum_notional=Decimal("1"))


def inputs() -> QuoteInputs:
    return QuoteInputs(Decimal("1"), Decimal("4"), Decimal("2"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("2"), Decimal("12"), Decimal("4"), Decimal("120"), Decimal("1"), QuotePlacement.AT_TOUCH)


def test_quote_plan_is_one_bid_one_ask_and_edge_checked_after_tick_rounding():
    inventory = classify_inventory(Decimal("0"), Decimal("100"), Decimal("200"))
    plan = build_quote_plan(
        asset=AssetSpec("SOL"),
        derive_pair="SOL-USDC",
        derive_book=derive_book(),
        fair_value=fair(),
        market_state=state(),
        inventory=inventory,
        portfolio_skew_bps=Decimal("0"),
        rules=rules(),
        inputs=inputs(),
    )
    assert plan.bid_price is not None and plan.ask_price is not None
    assert plan.bid_price < Decimal("100.10")
    assert plan.ask_price > Decimal("99.90")
    assert plan.buy_edge_bps >= plan.edge.total_required_bps
    assert plan.sell_edge_bps >= plan.edge.total_required_bps
    assert plan.bid_amount == Decimal("0.1")


def test_inventory_mode_can_disable_only_the_exposure_worsening_side():
    inventory = classify_inventory(Decimal("2"), Decimal("100"), Decimal("200"))
    assert inventory.mode == InventoryMode.ASK_ONLY
    plan = build_quote_plan(
        asset=AssetSpec("SOL"), derive_pair="SOL-USDC", derive_book=derive_book(), fair_value=fair(),
        market_state=state(), inventory=inventory, portfolio_skew_bps=Decimal("0"), rules=rules(), inputs=inputs()
    )
    assert plan.bid_price is None
    assert plan.ask_price is not None


def test_risk_rejects_minimum_notional_after_rounding():
    result = validate_rounded_order(
        price=Decimal("100.009"), amount=Decimal("0.101"), side=Side.BUY,
        rules=DeriveRules("SOL-PERP", "SOL", "USDC", Decimal("0.01"), Decimal("0.1"), Decimal("0.1"), minimum_notional=Decimal("10.1")),
        max_single_order_notional=Decimal("120"),
    )
    assert not result.allowed
    assert result.reason == "ROUNDED_NOTIONAL_BELOW_MINIMUM"


def test_paused_market_produces_no_quote():
    inventory = classify_inventory(Decimal("0"), Decimal("100"), Decimal("200"))
    plan = build_quote_plan(
        asset=AssetSpec("SOL"), derive_pair="SOL-USDC", derive_book=derive_book(), fair_value=fair(),
        market_state=state(MarketMode.PAUSED), inventory=inventory, portfolio_skew_bps=Decimal("0"), rules=rules(), inputs=inputs()
    )
    assert plan.bid_price is None and plan.ask_price is None
    assert plan.block_reason
