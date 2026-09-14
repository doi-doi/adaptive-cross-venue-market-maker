from decimal import Decimal as D

import pytest

from derive_multi_asset_mm.models import BookSnapshot
from derive_multi_asset_mm.source_health import SourceHealth, consensus


def source(price, timestamp=10):
    health = SourceHealth()
    health.connect(timestamp)
    health.accept(BookSnapshot(timestamp, D(str(price)), D(str(price)), D(1), D(1)))
    return health


@pytest.mark.parametrize("count", [0, 1, 2, 3, 4])
def test_confidence_and_no_source_pause(count):
    result = consensus({str(i): source(100) for i in range(count)}, 11)
    assert result["source_count"] == count
    assert (result["fair_value"] is None) == (count == 0)


def test_cc_can_continue_without_binance_and_expired_prices_never_enter_median():
    sources = {"binance": source(300, 1), "bybit": source(100), "okx": source(100.01)}
    result = consensus(sources, 11)
    assert result["valid_sources"] == ["bybit", "okx"]
    assert result["fair_value"] == D("100.005")
    assert consensus(sources, 16)["fair_value"] is None


def test_outlier_and_disagreement_are_distinct():
    result = consensus({str(i): source(p) for i, p in enumerate([100, 100.01, 100.02, 103])}, 11)
    assert result["outliers"] == {"3": D(103)}
    assert result["fair_value"] == D("100.01")
    result = consensus({"a": source(100), "b": source(103)}, 11)
    assert result["pause_reason"] == "REFERENCE_DISAGREEMENT_PAUSE"
    assert result["fair_value"] is None


def test_reconnect_requires_new_book_and_resets_sequence():
    health = source(100)
    health.disconnect(11)
    assert health.book is None
    health.connect(12)
    assert health.status(12) == "STALE"
    assert health.reconnects == 1
    assert health.disconnect_duration == 1
    assert health.accept(BookSnapshot(13, D(100), D(101), D(1), D(1)), 1)
    assert health.status(13) == "HEALTHY"


def test_filtered_sequence_jumps_are_not_proven_gaps():
    health = source(100)
    def book(t):
        return BookSnapshot(t, D(100), D(101), D(1), D(1))
    assert health.accept(book(11), 1)
    assert health.accept(book(12), 100)
    assert health.sequence_gaps == 0
    assert not health.accept(book(13), 100)
    assert not health.accept(book(14), 99)
    assert health.duplicates == 1 and health.out_of_order == 1
    assert health.accept(book(15), 102, previous=101)
    assert health.sequence_gaps == 1


def test_per_venue_stale_override_and_minimum():
    sources = {"bybit": source(100), "okx": source(100)}
    result = consensus(sources, 13, overrides={"bybit": 2}, minimum_sources=2)
    assert result["source_count"] == 1
    assert result["fair_value"] is None
