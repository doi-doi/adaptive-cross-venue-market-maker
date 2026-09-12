from decimal import Decimal

from derive_multi_asset_mm.market_state import MarketStateEngine, classify_direction, classify_volatility
from derive_multi_asset_mm.models import BookSnapshot, DirectionState, VolatilityState
from derive_multi_asset_mm.reference import RobustBasis, build_fair_value, microprice, top_n_imbalance


def book(timestamp: float, bid: str = "99", ask: str = "101") -> BookSnapshot:
    return BookSnapshot(
        timestamp=timestamp,
        best_bid=Decimal(bid),
        best_ask=Decimal(ask),
        bid_size=Decimal("3"),
        ask_size=Decimal("1"),
        bids=((Decimal(bid), Decimal("3")), (Decimal("98"), Decimal("2"))),
        asks=((Decimal(ask), Decimal("1")), (Decimal("102"), Decimal("2"))),
    )


def test_microprice_and_depth_imbalance_are_size_weighted():
    snapshot = book(1)
    assert microprice(snapshot) == Decimal("100.5")
    assert top_n_imbalance(snapshot, 2) == Decimal("0.25")


def test_basis_tracker_rejects_extreme_observation_from_baseline():
    tracker = RobustBasis(window=5, max_deviation_bps=Decimal("10"))
    tracker.update(Decimal("0"))
    tracker.update(Decimal("4"))
    current, baseline, deviation, protected = tracker.update(Decimal("100"))
    assert current == Decimal("100")
    assert protected
    assert baseline == Decimal("2")
    assert deviation == Decimal("98")


def test_basis_tracker_maintains_causal_ewma_diagnostic():
    tracker = RobustBasis(window=5, max_deviation_bps=Decimal("10"), ewma_alpha=Decimal("0.5"))
    tracker.update(Decimal("2"))
    tracker.update(Decimal("6"))
    assert tracker.ewma_bps == Decimal("4")


def test_fair_value_uses_binance_reference_and_baseline_basis():
    tracker = RobustBasis(window=5, max_deviation_bps=Decimal("20"))
    fair = build_fair_value(
        derive_book=book(1, "99", "101"),
        binance_book=book(1, "100", "102"),
        basis_tracker=tracker,
        mid_weight=Decimal("0.5"),
        microprice_weight=Decimal("0.5"),
        max_levels=2,
    )
    assert fair.binance_mid == Decimal("101")
    assert fair.binance_microprice == Decimal("101.5")
    assert fair.fair_value_raw == Decimal("101.25")
    assert fair.derive_fair_value == Decimal("100")


def test_state_classifiers_keep_direction_and_volatility_separate():
    assert classify_direction(Decimal("2"), Decimal("1")) == DirectionState.BULLISH
    assert classify_direction(Decimal("-2"), Decimal("1")) == DirectionState.BEARISH
    assert classify_volatility(Decimal("0.5"), high_threshold_bps=Decimal("8"), extreme_threshold_bps=Decimal("20")) == VolatilityState.LOW_VOL
    assert classify_volatility(Decimal("25"), high_threshold_bps=Decimal("8"), extreme_threshold_bps=Decimal("20")) == VolatilityState.EXTREME_VOL


def test_state_engine_does_not_require_forward_filled_reference_for_pause():
    engine = MarketStateEngine()
    current = book(100)
    state = engine.update(
        derive_book=current,
        binance_book=book(100),
        now=100,
        bbo_stale_seconds=Decimal("5"),
        reference_stale_seconds=Decimal("5"),
        direction_threshold_bps=Decimal("1"),
        high_vol_threshold_bps=Decimal("8"),
        extreme_vol_threshold_bps=Decimal("20"),
        aggressive_spread_max_bps=Decimal("15"),
        defensive_spread_min_bps=Decimal("3"),
        divergence_protected=True,
        max_levels=2,
    )
    assert state.market_mode.value == "PAUSED"
    assert state.reason == "REFERENCE_DIVERGENCE_PROTECTION"


def test_state_engine_pauses_on_fast_reference_move():
    engine = MarketStateEngine()
    common = dict(
        bbo_stale_seconds=Decimal("5"),
        reference_stale_seconds=Decimal("5"),
        direction_threshold_bps=Decimal("1"),
        high_vol_threshold_bps=Decimal("8"),
        extreme_vol_threshold_bps=Decimal("20"),
        aggressive_spread_max_bps=Decimal("15"),
        defensive_spread_min_bps=Decimal("3"),
        divergence_protected=False,
        max_levels=2,
        fast_move_threshold_bps=Decimal("8"),
    )
    engine.update(derive_book=book(100), binance_book=book(100), now=100, **common)
    state = engine.update(derive_book=book(101), binance_book=book(101, "109", "111"), now=101, **common)
    assert state.market_mode.value == "PAUSED"
    assert state.reason == "FAST_REFERENCE_MOVE"
