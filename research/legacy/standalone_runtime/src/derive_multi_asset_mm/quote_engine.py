"""One-level, post-only quote selection with visible edge components."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from .inventory import directional_skew_bps, local_inventory_skew_bps, side_allowed
from .models import (
    BPS,
    ZERO,
    AssetSpec,
    BookSnapshot,
    DeriveRules,
    EdgeBreakdown,
    FairValue,
    InventoryMode,
    InventorySnapshot,
    MarketMode,
    MarketState,
    QuotePlacement,
    QuotePlan,
    Side,
)


@dataclass(frozen=True)
class QuoteInputs:
    maker_fee_bps: Decimal
    min_edge_bps: Decimal
    volatility_buffer_bps: Decimal
    latency_buffer_bps: Decimal
    toxicity_buffer_bps: Decimal
    minimum_profit_bps: Decimal
    directional_skew_max_bps: Decimal
    inventory_skew_max_bps: Decimal
    portfolio_skew_max_bps: Decimal
    max_single_order_notional: Decimal
    order_size_multiplier: Decimal
    placement: QuotePlacement


def round_down(value: Decimal, increment: Decimal) -> Decimal:
    if increment <= ZERO:
        raise ValueError("increment must be positive")
    return (value / increment).to_integral_value(rounding=ROUND_FLOOR) * increment


def round_up(value: Decimal, increment: Decimal) -> Decimal:
    if increment <= ZERO:
        raise ValueError("increment must be positive")
    return (value / increment).to_integral_value(rounding=ROUND_CEILING) * increment


def _mode_adjustment(mode: MarketMode) -> Decimal:
    if mode == MarketMode.AGGRESSIVE:
        return Decimal("-1")
    if mode == MarketMode.DEFENSIVE:
        return Decimal("2")
    return ZERO


def _edge_breakdown(state: MarketState, inputs: QuoteInputs) -> EdgeBreakdown:
    mode_adjustment = _mode_adjustment(state.market_mode)
    gross = (
        inputs.maker_fee_bps
        + inputs.volatility_buffer_bps
        + inputs.latency_buffer_bps
        + inputs.toxicity_buffer_bps
        + inputs.minimum_profit_bps
        + mode_adjustment
    )
    return EdgeBreakdown(
        maker_fee_bps=inputs.maker_fee_bps,
        volatility_buffer_bps=inputs.volatility_buffer_bps,
        latency_buffer_bps=inputs.latency_buffer_bps,
        toxicity_buffer_bps=inputs.toxicity_buffer_bps,
        minimum_profit_bps=inputs.minimum_profit_bps,
        mode_adjustment_bps=mode_adjustment,
        total_required_bps=max(inputs.min_edge_bps, gross),
    )


def _amount_for_rules(rules: DeriveRules, inputs: QuoteInputs, asset: AssetSpec, price: Decimal) -> Decimal:
    multiplier = inputs.order_size_multiplier * asset.order_size_multiplier
    amount = round_up(rules.minimum_amount * multiplier, rules.amount_step)
    if rules.minimum_notional > ZERO and amount * price < rules.minimum_notional:
        amount = round_up(rules.minimum_notional / price, rules.amount_step)
    return amount


def _buy_candidate(book: BookSnapshot, ceiling: Decimal, tick: Decimal, placement: QuotePlacement) -> Decimal:
    if placement == QuotePlacement.IMPROVE_BY_ONE_TICK:
        upper = min(book.best_bid + tick, book.best_ask - tick, ceiling)
    else:
        upper = min(book.best_bid, book.best_ask - tick, ceiling)
    return round_down(upper, tick)


def _sell_candidate(book: BookSnapshot, floor: Decimal, tick: Decimal, placement: QuotePlacement) -> Decimal:
    if placement == QuotePlacement.IMPROVE_BY_ONE_TICK:
        lower = max(book.best_ask - tick, book.best_bid + tick, floor)
    else:
        lower = max(book.best_ask, book.best_bid + tick, floor)
    return round_up(lower, tick)


def _edge_for(side: Side, price: Decimal, fair: Decimal) -> Decimal:
    if fair <= ZERO or price <= ZERO:
        return ZERO
    if side == Side.BUY:
        return (fair - price) / fair * BPS
    return (price - fair) / fair * BPS


def _disabled_plan(
    asset: AssetSpec,
    derive_pair: str | None,
    state: MarketState,
    inventory_mode: InventoryMode,
    edge: EdgeBreakdown,
    reason: str,
    reservation: Decimal = ZERO,
    local_skew: Decimal = ZERO,
    portfolio_skew: Decimal = ZERO,
) -> QuotePlan:
    return QuotePlan(
        asset=asset.symbol,
        derive_pair=derive_pair,
        market_mode=state.market_mode,
        direction=state.direction,
        inventory_mode=inventory_mode,
        bid_price=None,
        bid_amount=ZERO,
        ask_price=None,
        ask_amount=ZERO,
        buy_edge_bps=ZERO,
        sell_edge_bps=ZERO,
        edge=edge,
        reservation_price=reservation,
        local_inventory_skew_bps=local_skew,
        portfolio_skew_bps=portfolio_skew,
        block_reason=reason,
        bid_reason=reason,
        ask_reason=reason,
    )


def build_quote_plan(
    *,
    asset: AssetSpec,
    derive_pair: str | None,
    derive_book: BookSnapshot,
    fair_value: FairValue | None,
    market_state: MarketState,
    inventory: InventorySnapshot,
    portfolio_skew_bps: Decimal,
    rules: DeriveRules | None,
    inputs: QuoteInputs,
) -> QuotePlan:
    edge = _edge_breakdown(market_state, inputs)
    local_skew = local_inventory_skew_bps(inventory, inputs.inventory_skew_max_bps)
    direction_skew = directional_skew_bps(market_state.direction, inputs.directional_skew_max_bps)
    total_skew = direction_skew + local_skew + max(-inputs.portfolio_skew_max_bps, min(inputs.portfolio_skew_max_bps, portfolio_skew_bps))
    if fair_value is None or rules is None or not rules.valid():
        return _disabled_plan(asset, derive_pair, market_state, inventory.mode, edge, "REFERENCE_OR_RULES_UNAVAILABLE", local_skew=local_skew, portfolio_skew=portfolio_skew_bps)
    if market_state.market_mode == MarketMode.PAUSED:
        return _disabled_plan(asset, derive_pair, market_state, inventory.mode, edge, market_state.reason or "PAUSED", fair_value.derive_fair_value, local_skew, portfolio_skew_bps)
    if not derive_book.valid() or derive_book.mid <= ZERO:
        return _disabled_plan(asset, derive_pair, market_state, inventory.mode, edge, "DERIVE_BBO_INVALID", fair_value.derive_fair_value, local_skew, portfolio_skew_bps)

    fair = fair_value.derive_fair_value
    reservation = fair * (Decimal("1") + total_skew / BPS)
    bid_ceiling = reservation * (Decimal("1") - edge.total_required_bps / BPS)
    ask_floor = reservation * (Decimal("1") + edge.total_required_bps / BPS)
    amount_bid = _amount_for_rules(rules, inputs, asset, max(derive_book.best_bid, fair))
    amount_ask = _amount_for_rules(rules, inputs, asset, max(derive_book.best_ask, fair))
    bid_price = _buy_candidate(derive_book, bid_ceiling, rules.tick_size, inputs.placement)
    ask_price = _sell_candidate(derive_book, ask_floor, rules.tick_size, inputs.placement)
    bid_edge = _edge_for(Side.BUY, bid_price, fair)
    ask_edge = _edge_for(Side.SELL, ask_price, fair)
    bid_reason = ""
    ask_reason = ""

    if not side_allowed(inventory.mode, Side.BUY):
        bid_price, amount_bid, bid_reason = None, ZERO, "INVENTORY_MODE_BID_DISABLED"
    elif bid_price <= ZERO or bid_price >= derive_book.best_ask:
        bid_price, amount_bid, bid_reason = None, ZERO, "POST_ONLY_BID_INVALID"
    elif bid_edge < edge.total_required_bps:
        bid_price, amount_bid, bid_reason = None, ZERO, "BUY_EDGE_INSUFFICIENT_AFTER_ROUNDING"
    elif bid_price * amount_bid > inputs.max_single_order_notional:
        bid_price, amount_bid, bid_reason = None, ZERO, "MAX_SINGLE_ORDER_NOTIONAL"

    if not side_allowed(inventory.mode, Side.SELL):
        ask_price, amount_ask, ask_reason = None, ZERO, "INVENTORY_MODE_ASK_DISABLED"
    elif ask_price <= derive_book.best_bid:
        ask_price, amount_ask, ask_reason = None, ZERO, "POST_ONLY_ASK_INVALID"
    elif ask_edge < edge.total_required_bps:
        ask_price, amount_ask, ask_reason = None, ZERO, "SELL_EDGE_INSUFFICIENT_AFTER_ROUNDING"
    elif ask_price * amount_ask > inputs.max_single_order_notional:
        ask_price, amount_ask, ask_reason = None, ZERO, "MAX_SINGLE_ORDER_NOTIONAL"

    if bid_price is None and ask_price is None:
        block = bid_reason or ask_reason or "NO_VALID_POST_ONLY_QUOTE"
    else:
        block = ""
    return QuotePlan(
        asset=asset.symbol,
        derive_pair=derive_pair,
        market_mode=market_state.market_mode,
        direction=market_state.direction,
        inventory_mode=inventory.mode,
        bid_price=bid_price,
        bid_amount=amount_bid,
        ask_price=ask_price,
        ask_amount=amount_ask,
        buy_edge_bps=bid_edge if bid_price is not None else ZERO,
        sell_edge_bps=ask_edge if ask_price is not None else ZERO,
        edge=edge,
        reservation_price=reservation,
        local_inventory_skew_bps=local_skew,
        portfolio_skew_bps=portfolio_skew_bps,
        block_reason=block,
        bid_reason=bid_reason,
        ask_reason=ask_reason,
    )
