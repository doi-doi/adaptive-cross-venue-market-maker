from decimal import Decimal

from derive_multi_asset_mm.lifecycle import QuoteReconciler
from derive_multi_asset_mm.models import (
    BookSnapshot,
    DirectionState,
    FairValue,
    MarketMode,
    MarketState,
    Side,
    TradePrint,
)
from derive_multi_asset_mm.shadow_engine import ShadowModel
from derive_multi_asset_mm.telemetry import TelemetryStore


def test_lifecycle_is_create_hold_cancel_then_create():
    reconciler = QuoteReconciler()
    create = reconciler.reconcile(asset="SOL", side=Side.BUY, desired_price=Decimal("99"), desired_amount=Decimal("1"), now=1, max_age_seconds=Decimal("30"), tolerance_bps=Decimal("3"))
    assert create.kind == "CREATE"
    reconciler.acknowledge_create(create)
    hold = reconciler.reconcile(asset="SOL", side=Side.BUY, desired_price=Decimal("99"), desired_amount=Decimal("1"), now=2, max_age_seconds=Decimal("30"), tolerance_bps=Decimal("3"))
    assert hold.kind == "HOLD" and hold.reason == "NO_OP_HOLD"
    cancel = reconciler.reconcile(asset="SOL", side=Side.BUY, desired_price=Decimal("100"), desired_amount=Decimal("1"), now=3, max_age_seconds=Decimal("30"), tolerance_bps=Decimal("3"))
    assert cancel.kind == "CANCEL"
    reconciler.acknowledge_cancel(cancel)
    recreated = reconciler.reconcile(asset="SOL", side=Side.BUY, desired_price=Decimal("100"), desired_amount=Decimal("1"), now=4, max_age_seconds=Decimal("30"), tolerance_bps=Decimal("3"))
    assert recreated.kind == "CREATE"


def test_conservative_fill_requires_strict_aggressor_trade_through_and_markouts_are_separate(tmp_path):
    telemetry = TelemetryStore(tmp_path / "telemetry.sqlite")
    model = ShadowModel("CONSERVATIVE", Decimal("800"), Decimal("1"), telemetry)
    action = model.reconcile(asset="SOL", side=Side.BUY, price=Decimal("99"), amount=Decimal("1"), now=10, max_age_seconds=Decimal("30"), tolerance_bps=Decimal("3"), paused=False)
    assert action.kind == "CREATE"
    fair = FairValue(Decimal("100"), Decimal("100"), Decimal("0"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("100"))
    state = MarketState(MarketMode.NORMAL, DirectionState.NEUTRAL, __import__("derive_multi_asset_mm.models", fromlist=["VolatilityState"]).VolatilityState.NORMAL_VOL)
    book = BookSnapshot(10, Decimal("99"), Decimal("101"), Decimal("1"), Decimal("1"))
    touch = TradePrint(11, Decimal("99"), Decimal("1"), Side.SELL, "touch")
    assert model.process_trades(asset="SOL", trades=[touch], derive_book=book, fair_value=fair, market_state=state, basis_bps=Decimal("0"), now=11) == []
    through = TradePrint(12, Decimal("98.99"), Decimal("1"), Side.SELL, "through")
    fills = model.process_trades(asset="SOL", trades=[through], derive_book=book, fair_value=fair, market_state=state, basis_bps=Decimal("0"), now=12)
    assert len(fills) == 1
    markouts = model.record_markouts(asset="SOL", now=13, binance_fair_value=Decimal("101"), derive_mid=Decimal("100"), horizons=(1,))
    assert markouts[0].binance_markout_bps > 0
    assert markouts[0].derive_markout_bps > 0
    telemetry.close()
