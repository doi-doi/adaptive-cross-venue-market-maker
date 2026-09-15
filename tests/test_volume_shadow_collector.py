from __future__ import annotations

from decimal import Decimal

import pytest
from scripts.volume_shadow_collector import (
    NO_REAL_ORDER_SUBMISSION,
    TICK_SIZE,
    CsvJournal,
    GhostSide,
    Lane,
    PublicGhostExperiment,
    classify_binance_shock,
    ghost_quote_prices,
    markout_bps,
    quote_competitiveness,
    sequence_gap,
    strictly_trades_through,
    total_spread_bps,
)


def _journal(tmp_path, name: str, fields: tuple[str, ...]) -> CsvJournal:
    return CsvJournal(tmp_path / name, fields)


def _quote(tmp_path, side: str = "bid", price: str = "99") -> GhostSide:
    return GhostSide(
        quote_id="q-1",
        lane="LANE_4_BPS",
        side=side,
        price=Decimal(price),
        center=Decimal("100"),
        created_at=0.0,
        regime="NORMAL",
    )


def test_total_spread_and_tick_safe_ghost_quotes_are_passive():
    assert total_spread_bps(Decimal("99"), Decimal("101")) == Decimal("200")
    bid, ask = ghost_quote_prices(
        Decimal("100"), Decimal("4"), Decimal("99"), Decimal("101"), Decimal("0.01")
    )
    assert bid == Decimal("99.98")
    assert ask == Decimal("100.02")
    assert Decimal("99") < bid < ask < Decimal("101")


def test_touch_is_not_a_conservative_fill():
    assert not strictly_trades_through("bid", "sell", Decimal("99"), Decimal("99"))
    assert strictly_trades_through("bid", "sell", Decimal("98.99"), Decimal("99"))
    assert not strictly_trades_through("ask", "sell", Decimal("101.01"), Decimal("101"))
    assert strictly_trades_through("ask", "buy", Decimal("101.01"), Decimal("101"))


def test_lane_isolation_fee_and_inventory_cap(tmp_path):
    lane_a = Lane(3, lane_id="LANE_3_BPS")
    lane_b = Lane(8, lane_id="LANE_8_BPS")
    quote = _quote(tmp_path, price="100")
    fill = lane_a.apply_fill(
        side="buy",
        price=quote.price,
        notional=Decimal("40"),
        timestamp=1.0,
        quote=quote,
        mid=Decimal("100"),
        trade_id="trade-1",
    )
    assert fill is not None
    assert lane_a.inventory_base == Decimal("0.4")
    assert lane_a.fees_quote == Decimal("0.004")
    assert lane_b.inventory_base == Decimal("0")
    # Repeated same-direction fills cannot breach the virtual hard cap.
    for index in range(20):
        lane_a.apply_fill(
            side="buy",
            price=quote.price,
            notional=Decimal("40"),
            timestamp=2.0 + index,
            quote=_quote(tmp_path, price="100"),
            mid=Decimal("100"),
            trade_id=f"trade-{index + 2}",
        )
    assert abs(lane_a.inventory_base * Decimal("100")) <= lane_a.inventory_cap_quote


def test_elapsed_markouts_persist_observed_and_missing_horizons(tmp_path):
    lane = Lane(4, lane_id="LANE_4_BPS")
    quote = _quote(tmp_path, price="100")
    fill = lane.apply_fill(
        side="buy",
        price=quote.price,
        notional=Decimal("40"),
        timestamp=0.0,
        quote=quote,
        mid=Decimal("100"),
        trade_id="trade-1",
    )
    assert fill is not None
    journal = _journal(
        tmp_path,
        "markouts.csv",
        (
            "fill_id",
            "trade_id",
            "lane",
            "side",
            "quote_created_at",
            "fill_timestamp",
            "fill_price",
            "fill_notional",
            "trade_price",
            "inventory_before",
            "inventory_after",
            "mid_at_fill",
            "horizon_seconds",
            "observed_at",
            "future_mid",
            "markout_bps",
            "status",
        ),
    )
    lane.due_markouts(5.0, Decimal("100.10"), journal)
    lane.due_markouts(30.0, Decimal("99.90"), journal)
    lane.due_markouts(60.0, Decimal("100.00"), journal)
    assert set(fill.markouts) == {5, 30, 60}
    assert markout_bps("buy", Decimal("100"), Decimal("100.10")) == Decimal("10")
    lane.finalize_markouts(61.0, journal)
    assert set(fill.markouts) == {5, 30, 60}
    rows = list((tmp_path / "markouts.csv").read_text(encoding="utf-8").splitlines())
    assert any("MISSING_RUN_END" in row and ",300," in row for row in rows)


def test_fixed_time_sampling_uses_zero_for_fresh_unchanged_and_no_gap_fill(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "sampling")
    experiment._record_fixed_sample(100.0, Decimal("100"))
    experiment._record_fixed_sample(101.0, Decimal("100"))
    assert list(experiment.returns) == [0.0]
    experiment._record_fixed_sample(103.0, Decimal("101"))
    assert list(experiment.returns) == [0.0]
    experiment._record_fixed_sample(104.0, Decimal("101"))
    assert list(experiment.returns) == [0.0, 0.0]


def test_stale_feed_pauses_sampling_and_closes_ghost_quotes(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "stale")
    experiment.process_orderbook(
        source="derive", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    experiment.process_orderbook(
        source="binance", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    assert any(lane.active for lane in experiment.lanes.values())
    experiment.mark_stale(104.0)
    assert all(not lane.active for lane in experiment.lanes.values())


def test_stale_peer_blocks_new_ghost_quotes_until_recovery(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "stale-peer")
    experiment.process_orderbook(
        source="derive", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    experiment.process_orderbook(
        source="binance", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    assert any(lane.active for lane in experiment.lanes.values())
    experiment.process_orderbook(
        source="binance", timestamp=104.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=104.0, sequence=2
    )
    assert all(not lane.active for lane in experiment.lanes.values())
    experiment.process_orderbook(
        source="derive", timestamp=104.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=104.0, sequence=2
    )
    assert any(lane.active for lane in experiment.lanes.values())


def test_sequence_gap_uses_binance_previous_update_id():
    assert not sequence_gap(100, 105, current_previous=100)
    assert sequence_gap(100, 105, current_previous=99)
    assert sequence_gap(100, 102)
    assert not sequence_gap(None, 102)


def test_public_experiment_lane_isolation_and_resume_dedup(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "dedup")
    experiment.process_orderbook(
        source="derive", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    experiment.process_orderbook(
        source="binance", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    bid_quote = experiment.lanes["LANE_3_BPS"].active["bid"]
    trade_price = bid_quote.price - Decimal("0.01")
    experiment.process_derive_trade(
        trade_id="trade-unique",
        timestamp=101.0,
        price=trade_price,
        amount_base=Decimal("1"),
        aggressor_side="sell",
        received=101.0,
    )
    first_counts = {key: len(lane.fills) for key, lane in experiment.lanes.items()}
    experiment.process_derive_trade(
        trade_id="trade-unique",
        timestamp=101.1,
        price=trade_price,
        amount_base=Decimal("1"),
        aggressor_side="sell",
        received=101.1,
    )
    assert first_counts == {key: len(lane.fills) for key, lane in experiment.lanes.items()}
    assert first_counts["LANE_3_BPS"] == 1
    resumed = PublicGhostExperiment(tmp_path, "dedup")
    assert "trade-unique" in resumed.seen_public_trade_keys
    assert NO_REAL_ORDER_SUBMISSION is True


def test_quote_replacement_and_toxicity_guard_are_deterministic(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "replacement")
    experiment.process_orderbook(
        source="derive", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    experiment.process_orderbook(
        source="binance", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    lane = experiment.lanes["LANE_3_BPS"]
    original = lane.active["bid"].quote_id
    lane.toxicity_markout_values.append(Decimal("-6"))
    assert experiment._refresh_toxicity(lane, 101.0) is True
    assert lane.toxicity_triggers == 1
    experiment.process_orderbook(
        source="derive", timestamp=110.0, bid=Decimal("99.8"), ask=Decimal("100.0"), received=102.0, sequence=2
    )
    experiment.process_orderbook(
        source="binance", timestamp=110.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=102.0, sequence=2
    )
    assert lane.active["bid"].quote_id != original
    assert lane.replacement_count >= 1


@pytest.mark.parametrize("residency", [Decimal("10"), Decimal("15"), Decimal("20")])
def test_shadow_residency_sensitivity_is_configurable_without_early_refresh(tmp_path, residency):
    experiment = PublicGhostExperiment(
        tmp_path / str(residency),
        "residency",
        minimum_residency_seconds=residency,
    )
    experiment.process_orderbook(
        source="derive", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    experiment.process_orderbook(
        source="binance", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    lane = experiment.lanes["LANE_3_BPS"]
    before = lane.active["bid"].quote_id
    t = 100.0 + float(residency) - 1.0
    experiment.process_orderbook(
        source="derive", timestamp=t, bid=Decimal("99.8"), ask=Decimal("100.0"), received=100.5, sequence=2
    )
    assert lane.active["bid"].quote_id == before


def test_binance_emergency_requires_two_conditions_and_elevated_widens_only():
    state, side, conditions = classify_binance_shock(Decimal("8"), Decimal("0"), Decimal("0"))
    assert state == "ELEVATED" and side == "ask" and conditions == ()
    state, side, conditions = classify_binance_shock(Decimal("25"), Decimal("15"), Decimal("0"))
    assert state == "EMERGENCY" and side == "ask"
    assert conditions == ("LARGE_BINANCE_MOVE", "CROSS_VENUE_DISLOCATION")


def test_new_shock_hysteresis_latches_then_recovers(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "shock")
    state, side, conditions = experiment._update_binance_shock_state(
        100.0, Decimal("25"), Decimal("15"), Decimal("0")
    )
    assert (state, side, conditions) == (
        "EMERGENCY",
        "ask",
        ("LARGE_BINANCE_MOVE", "CROSS_VENUE_DISLOCATION"),
    )
    state, side, _ = experiment._update_binance_shock_state(
        101.0, Decimal("0"), Decimal("0"), Decimal("0")
    )
    assert state == "EMERGENCY_RECOVERY" and side == "ask"
    state, side, _ = experiment._update_binance_shock_state(
        112.0, Decimal("0"), Decimal("0"), Decimal("0")
    )
    assert state == "NORMAL" and side is None


def test_quote_competitiveness_and_touch_are_separate(tmp_path):
    assert quote_competitiveness("bid", Decimal("100"), Decimal("99"), Decimal("101"), Decimal("0.01"))[0] == "INSIDE_SPREAD"
    assert quote_competitiveness("bid", Decimal("99"), Decimal("99"), Decimal("101"), Decimal("0.01"))[0] == "AT_BEST"
    assert quote_competitiveness("bid", Decimal("98.99"), Decimal("99"), Decimal("101"), Decimal("0.01"))[0] == "1_TICK_BEHIND"
    assert quote_competitiveness("bid", Decimal("98.97"), Decimal("99"), Decimal("101"), Decimal("0.01"))[0] == "3_PLUS_TICKS_BEHIND"
    assert quote_competitiveness("bid", Decimal("98.95"), Decimal("99"), Decimal("101"), Decimal("0.01"))[0] == "OUTSIDE_RELEVANT_BOOK"
    experiment = PublicGhostExperiment(tmp_path, "touch")
    experiment.process_orderbook(
        source="derive", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    experiment.process_orderbook(
        source="binance", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    lane = experiment.lanes["LANE_3_BPS"]
    quote = lane.active["bid"]
    experiment.process_derive_trade(
        trade_id="touch-only",
        timestamp=101.0,
        price=quote.price,
        amount_base=Decimal("1"),
        aggressor_side="sell",
        received=101.0,
    )
    assert lane.touches == 1
    assert lane.fills == []
    assert lane.competitiveness_summary("bid")["time_pct"]


def test_near_miss_and_missed_fill_opportunity_are_diagnostic_only(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "opportunity")
    experiment.process_orderbook(
        source="derive", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    experiment.process_orderbook(
        source="binance", timestamp=100.0, bid=Decimal("99.9"), ask=Decimal("100.1"), received=100.0, sequence=1
    )
    lane = experiment.lanes["LANE_3_BPS"]
    quote = lane.active["bid"]
    experiment.process_derive_trade(
        trade_id="near-one",
        timestamp=101.0,
        price=quote.price + TICK_SIZE,
        amount_base=Decimal("1"),
        aggressor_side="sell",
        received=101.0,
    )
    assert lane.near_misses_1_tick == 1
    lane.close_quote(
        "bid",
        102.0,
        "cancel",
        experiment.quote_journal,
        experiment._context(),
        "BINANCE_TRUE_SHOCK_LARGE_BINANCE_MOVE_CROSS_VENUE_DISLOCATION_CANCEL_BID",
    )
    experiment.process_derive_trade(
        trade_id="missed",
        timestamp=103.0,
        price=quote.price - Decimal("0.01"),
        amount_base=Decimal("1"),
        aggressor_side="sell",
        received=103.0,
    )
    assert lane.missed_fill_opportunities == 1
    assert lane.fills == []
    assert lane.missed_fill_reason_counts["BINANCE_TRUE_SHOCK_LARGE_BINANCE_MOVE_CROSS_VENUE_DISLOCATION_CANCEL_BID"] == 1


def test_no_fill_diagnosis_is_data_gated_by_public_trade_count(tmp_path):
    experiment = PublicGhostExperiment(tmp_path, "no-fill")
    payload = experiment.finalize(experiment.started_at + 3600.0)
    row = payload["lanes"]["LANE_3_BPS"]["all_data"]
    assert row["sample_class"] == "DATA_INSUFFICIENT"
    assert row["dominant_no_fill_cause"] == "LOW_VENUE_TRADE_ACTIVITY"
