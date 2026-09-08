"""Exchange-free shadow orders, separate fill models, inventory, and markouts."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .lifecycle import LifecycleAction, QuoteReconciler, ShadowOrder
from .markouts import maker_perspective_markout
from .models import (
    ZERO,
    BookSnapshot,
    FairValue,
    FillRecord,
    MarketMode,
    MarketState,
    MarkoutRecord,
    RuntimeCounters,
    Side,
    TradePrint,
)
from .telemetry import TelemetryStore


@dataclass
class ShadowPortfolio:
    starting_capital: Decimal
    fee_bps: Decimal
    cash: Decimal = field(init=False)
    fees: Decimal = ZERO
    positions: dict[str, Decimal] = field(default_factory=dict)
    fills: list[FillRecord] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.cash = self.starting_capital

    def position(self, asset: str) -> Decimal:
        return self.positions.get(asset, ZERO)

    def apply_fill(self, fill: FillRecord) -> FillRecord:
        before = self.position(fill.asset)
        signed = fill.amount if fill.side == Side.BUY else -fill.amount
        after = before + signed
        notional = fill.amount * fill.fill_price
        fee = notional * self.fee_bps / Decimal("10000")
        if fill.side == Side.BUY:
            self.cash -= notional + fee
        else:
            self.cash += notional - fee
        self.fees += fee
        corrected = FillRecord(
            timestamp=fill.timestamp,
            asset=fill.asset,
            side=fill.side,
            amount=fill.amount,
            fill_price=fill.fill_price,
            binance_fair_value=fill.binance_fair_value,
            derive_mid=fill.derive_mid,
            inventory_before=before,
            inventory_after=after,
            maker_fee_bps=self.fee_bps,
            market_mode=fill.market_mode,
            direction=fill.direction,
            basis_bps=fill.basis_bps,
            quoted_edge_bps=fill.quoted_edge_bps,
            model=fill.model,
        )
        self.positions[fill.asset] = after
        self.fills.append(corrected)
        return corrected

    def equity(self, mids: dict[str, Decimal]) -> Decimal:
        return self.cash + sum((self.position(asset) * mids.get(asset, ZERO) for asset in self.positions), ZERO)

    def gross_inventory(self, mids: dict[str, Decimal]) -> Decimal:
        return sum((abs(self.position(asset) * mids.get(asset, ZERO)) for asset in self.positions), ZERO)

    def net_inventory(self, mids: dict[str, Decimal]) -> Decimal:
        return sum((self.position(asset) * mids.get(asset, ZERO) for asset in self.positions), ZERO)


@dataclass
class PendingMarkouts:
    fill: FillRecord
    observed: set[int] = field(default_factory=set)


class ShadowModel:
    def __init__(self, name: str, capital: Decimal, fee_bps: Decimal, telemetry: TelemetryStore) -> None:
        self.name = name
        self.reconciler = QuoteReconciler()
        self.portfolio = ShadowPortfolio(capital, fee_bps)
        self.telemetry = telemetry
        self.pending: list[PendingMarkouts] = []
        self.counters = RuntimeCounters()
        self._seen_trade_ids: set[str] = set()

    def reconcile(
        self,
        *,
        asset: str,
        side: Side,
        price: Decimal | None,
        amount: Decimal,
        now: float,
        max_age_seconds: Decimal,
        tolerance_bps: Decimal,
        paused: bool,
    ) -> LifecycleAction:
        action = self.reconciler.reconcile(
            asset=asset,
            side=side,
            desired_price=price,
            desired_amount=amount,
            now=now,
            max_age_seconds=max_age_seconds,
            tolerance_bps=tolerance_bps,
            paused=paused,
        )
        self.telemetry.insert_action(now, asset, side.value, action)
        if action.kind == "CREATE":
            self.reconciler.acknowledge_create(action)
            self.counters.quote_creates += 1
        elif action.kind == "CANCEL":
            self.reconciler.acknowledge_cancel(action)
            self.counters.cancels += 1
        elif action.kind == "HOLD":
            self.counters.holds += 1
            self.counters.no_op_holds += 1
        self.telemetry.commit()
        return action

    def active_order(self, asset: str, side: Side) -> ShadowOrder | None:
        lifecycle = self.reconciler.state(asset, side)
        return lifecycle.order

    def process_trades(
        self,
        *,
        asset: str,
        trades: list[TradePrint],
        derive_book: BookSnapshot,
        fair_value: FairValue,
        market_state: MarketState,
        basis_bps: Decimal,
        now: float,
    ) -> list[FillRecord]:
        fills: list[FillRecord] = []
        for trade in sorted(trades, key=lambda item: item.timestamp):
            if trade.trade_id and trade.trade_id in self._seen_trade_ids:
                continue
            if trade.trade_id:
                self._seen_trade_ids.add(trade.trade_id)
            for side in (Side.BUY, Side.SELL):
                order = self.active_order(asset, side)
                if order is None:
                    continue
                hit = False
                if self.name == "CONSERVATIVE":
                    hit = (side == Side.BUY and trade.side == Side.SELL and trade.price < order.price) or (
                        side == Side.SELL and trade.side == Side.BUY and trade.price > order.price
                    )
                elif self.name == "TOUCH_SENSITIVITY":
                    hit = (side == Side.BUY and trade.side == Side.SELL and trade.price <= order.price) or (
                        side == Side.SELL and trade.side == Side.BUY and trade.price >= order.price
                    )
                if not hit:
                    continue
                fill = FillRecord(
                    timestamp=trade.timestamp,
                    asset=asset,
                    side=side,
                    amount=min(order.amount, trade.amount),
                    fill_price=order.price,
                    binance_fair_value=fair_value.derive_fair_value,
                    derive_mid=derive_book.mid,
                    inventory_before=ZERO,
                    inventory_after=ZERO,
                    maker_fee_bps=self.portfolio.fee_bps,
                    market_mode=market_state.market_mode,
                    direction=market_state.direction,
                    basis_bps=basis_bps,
                    quoted_edge_bps=(fair_value.derive_fair_value - order.price) / fair_value.derive_fair_value * Decimal("10000") if side == Side.BUY else (order.price - fair_value.derive_fair_value) / fair_value.derive_fair_value * Decimal("10000"),
                    model=self.name,
                )
                corrected = self.portfolio.apply_fill(fill)
                self.telemetry.insert_fill(corrected)
                self.pending.append(PendingMarkouts(corrected))
                self.reconciler.remove_filled_order(asset, side, order.order_id)
                fills.append(corrected)
                if self.name == "CONSERVATIVE":
                    self.counters.conservative_fills += 1
                else:
                    self.counters.touch_fills += 1
                break
        self.telemetry.commit()
        return fills

    def record_markouts(
        self,
        *,
        asset: str,
        now: float,
        binance_fair_value: Decimal,
        derive_mid: Decimal,
        horizons: tuple[int, ...] = (1, 5, 15, 30, 60),
    ) -> list[MarkoutRecord]:
        result: list[MarkoutRecord] = []
        for pending in self.pending:
            if pending.fill.asset != asset:
                continue
            for horizon in horizons:
                if horizon in pending.observed or now < pending.fill.timestamp + horizon:
                    continue
                pending.observed.add(horizon)
                markout = MarkoutRecord(
                    fill_timestamp=pending.fill.timestamp,
                    horizon_seconds=horizon,
                    asset=asset,
                    side=pending.fill.side,
                    reference_price=binance_fair_value,
                    derive_mid=derive_mid,
                    binance_markout_bps=maker_perspective_markout(pending.fill.side, pending.fill.fill_price, binance_fair_value),
                    derive_markout_bps=maker_perspective_markout(pending.fill.side, pending.fill.fill_price, derive_mid),
                    model=self.name,
                )
                self.telemetry.insert_markout(markout)
                result.append(markout)
        self.pending = [pending for pending in self.pending if len(pending.observed) < len(horizons)]
        if result:
            self.telemetry.commit()
        return result


class ShadowEngine:
    """Runs conservative and touch sensitivity models as separate portfolios."""

    def __init__(self, capital: Decimal, fee_bps: Decimal, telemetry: TelemetryStore) -> None:
        self.models = {
            "CONSERVATIVE": ShadowModel("CONSERVATIVE", capital, fee_bps, telemetry),
            "TOUCH_SENSITIVITY": ShadowModel("TOUCH_SENSITIVITY", capital, fee_bps, telemetry),
        }

    def reconcile_plan(self, plan: Any, now: float, max_age_seconds: Decimal, tolerance_bps: Decimal) -> list[LifecycleAction]:
        actions = []
        for model in self.models.values():
            paused = plan.market_mode == MarketMode.PAUSED
            actions.append(
                model.reconcile(
                    asset=plan.asset,
                    side=Side.BUY,
                    price=plan.bid_price,
                    amount=plan.bid_amount,
                    now=now,
                    max_age_seconds=max_age_seconds,
                    tolerance_bps=tolerance_bps,
                    paused=paused,
                )
            )
            actions.append(
                model.reconcile(
                    asset=plan.asset,
                    side=Side.SELL,
                    price=plan.ask_price,
                    amount=plan.ask_amount,
                    now=now,
                    max_age_seconds=max_age_seconds,
                    tolerance_bps=tolerance_bps,
                    paused=paused,
                )
            )
        return actions
