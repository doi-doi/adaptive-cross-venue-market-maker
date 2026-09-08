from decimal import Decimal

from derive_multi_asset_mm.control import control_contract, derive_only_fair_value
from derive_multi_asset_mm.lead_lag import estimate_lead_lag
from derive_multi_asset_mm.markouts import maker_perspective_markout, net_capture_proxy_bps
from derive_multi_asset_mm.models import BookSnapshot, Side


def test_maker_markout_signs_are_side_aware():
    assert maker_perspective_markout(Side.BUY, Decimal("100"), Decimal("101")) > 0
    assert maker_perspective_markout(Side.SELL, Decimal("100"), Decimal("99")) > 0
    assert maker_perspective_markout(Side.BUY, Decimal("100"), Decimal("99")) < 0


def test_net_capture_proxy_is_not_realized_pnl():
    assert net_capture_proxy_bps(Decimal("8"), Decimal("1"), Decimal("2")) == Decimal("9")


def test_derive_only_control_has_zero_basis():
    book = BookSnapshot(1, Decimal("99"), Decimal("101"), Decimal("1"), Decimal("1"))
    fair = derive_only_fair_value(book)
    assert fair.derive_fair_value == book.mid
    assert fair.baseline_basis_bps == 0
    assert control_contract()["MODEL_A"] == "DERIVE_ONLY"


def test_lead_lag_uses_observed_points_without_forward_fill():
    reference = [(float(index), Decimal(str(100 + index))) for index in range(8)]
    derive = [(float(index + 2), Decimal(str(200 + index))) for index in range(8)]
    rows = estimate_lead_lag(reference, derive, lags_seconds=(2,), return_horizon_seconds=1)
    assert rows[0]["status"] == "READY"
    assert rows[0]["observations"] >= 3
    assert rows[0]["correlation"] > Decimal("0.99")
