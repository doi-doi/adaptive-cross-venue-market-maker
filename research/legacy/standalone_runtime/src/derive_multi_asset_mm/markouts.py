"""Causal markout and net-capture calculations."""

from __future__ import annotations

from decimal import Decimal

from .models import BPS, ZERO, Side


def maker_perspective_markout(side: Side, fill_price: Decimal, future_price: Decimal) -> Decimal:
    if fill_price <= ZERO or future_price <= ZERO:
        raise ValueError("markout prices must be positive")
    if side == Side.BUY:
        return (future_price - fill_price) / fill_price * BPS
    return (fill_price - future_price) / fill_price * BPS


def net_capture_proxy_bps(
    quoted_edge_bps: Decimal,
    maker_fee_bps: Decimal,
    maker_markout_bps: Decimal,
) -> Decimal:
    return quoted_edge_bps - maker_fee_bps + maker_markout_bps
