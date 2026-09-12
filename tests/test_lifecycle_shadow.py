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


def test_mid_deviation_is_the_only_normal_refresh_trigger_when_mid_is_available():
    reconciler = QuoteReconciler()
    create = reconciler.reconcile(
        asset="SOL",
        side=Side.BUY,
        desired_price=Decimal("98"),
        desired_amount=Decimal("1"),
        now=1,
        max_age_seconds=Decimal("30"),
        tolerance_bps=Decimal("200"),
        mid_price=Decimal("100"),
    )
    reconciler.acknowledge_create(create)

    # A large desired-price change and an old quote do not refresh it while the
    # existing quote remains exactly 2% from the causal Derive mid.
    boundary = reconciler.reconcile(
        asset="SOL",
        side=Side.BUY,
        desired_price=Decimal("101"),
        desired_amount=Decimal("1"),
        now=1000,
        max_age_seconds=Decimal("30"),
        tolerance_bps=Decimal("200"),
        mid_price=Decimal("100"),
    )
    assert boundary.kind == "HOLD"
    assert boundary.reason == "NO_OP_HOLD"

    beyond = reconciler.reconcile(
        asset="SOL",
        side=Side.BUY,
        desired_price=Decimal("101"),
        desired_amount=Decimal("1"),
        now=1001,
        max_age_seconds=Decimal("30"),
        tolerance_bps=Decimal("200"),
        mid_price=Decimal("100.01"),
    )
    assert beyond.kind == "CANCEL"
    assert beyond.reason == "REFRESH_NEEDED"


def test_shadow_model_passes_mid_policy_through_action_rate_guard(tmp_path):
    telemetry = TelemetryStore(tmp_path / "telemetry.sqlite")
    model = ShadowModel("CONSERVATIVE", Decimal("800"), Decimal("1"), telemetry)
    try:
        create = model.reconcile(
            asset="SOL",
            side=Side.BUY,
            price=Decimal("98"),
            amount=Decimal("1"),
            now=1,
            max_age_seconds=Decimal("30"),
            tolerance_bps=Decimal("200"),
            mid_price=Decimal("100"),
            paused=False,
        )
        assert create.kind == "CREATE"

        hold = model.reconcile(
            asset="SOL",
            side=Side.BUY,
            price=Decimal("101"),
            amount=Decimal("1"),
            now=1000,
            max_age_seconds=Decimal("30"),
            tolerance_bps=Decimal("200"),
            mid_price=Decimal("100"),
            paused=False,
        )
        assert hold.kind == "HOLD"
        assert hold.reason == "NO_OP_HOLD"

        cancel = model.reconcile(
            asset="SOL",
            side=Side.BUY,
            price=Decimal("101"),
            amount=Decimal("1"),
            now=1001,
            max_age_seconds=Decimal("30"),
            tolerance_bps=Decimal("200"),
            mid_price=Decimal("100.01"),
            paused=False,
        )
        assert cancel.kind == "CANCEL"
        assert cancel.reason == "REFRESH_NEEDED"
    finally:
        telemetry.close()


def test_conservative_fill_requires_strict_aggressor_trade_through_and_markouts_are_separate(tmp_path):
    telemetry = TelemetryStore(tmp_path / "telemetry.sqlite")
    model = ShadowModel("CONSERVATIVE", Decimal("800"), Decimal("1"), telemetry)
    action = model.reconcile(asset="SOL", side=Side.BUY, price=Decimal("99"), amount=Decimal("1"), now=10, max_age_seconds=Decimal("30"), tolerance_bps=Decimal("3"), paused=False)
    assert action.kind == "CREATE"
    fair = FairValue(Decimal("100"), Decimal("100"), Decimal("0"), Decimal("100"), Decimal("0"), Decimal("0"), Decimal("100"))
    state = MarketState(MarketMode.NORMAL, DirectionState.NEUTRAL, __import__("derive_multi_asset_mm.models", fromlist=["VolatilityState"]).VolatilityState.NORMAL_VOL)
    book = BookSnapshot(10, Decimal("99"), Decimal("101"), Decimal("1"), Decimal("1"))
    touch = TradePrint(11, Decimal("99"), Decimal("1"), Side.SELL, "touch", source="derive")
    assert model.process_trades(asset="SOL", trades=[touch], derive_book=book, fair_value=fair, market_state=state, basis_bps=Decimal("0"), now=11) == []
    for source, timestamp in [("binance", 12), ("bybit", 12), ("okx", 12), ("bitget", 12), ("unknown", 12), ("derive", 9)]:
        invalid = TradePrint(timestamp, Decimal("98"), Decimal("1"), Side.SELL, f"{source}-{timestamp}", source=source)
        assert model.process_trades(asset="SOL", trades=[invalid], derive_book=book, fair_value=fair, market_state=state, basis_bps=Decimal("0"), now=12) == []
    through = TradePrint(12, Decimal("98.99"), Decimal("1"), Side.SELL, "through", source="derive")
    fills = model.process_trades(asset="SOL", trades=[through], derive_book=book, fair_value=fair, market_state=state, basis_bps=Decimal("0"), now=12)
    assert len(fills) == 1
    markouts = model.record_markouts(asset="SOL", now=13, binance_fair_value=Decimal("101"), derive_mid=Decimal("100"), horizons=(1,))
    assert markouts[0].binance_markout_bps > 0
    assert markouts[0].derive_markout_bps > 0
    telemetry.close()
