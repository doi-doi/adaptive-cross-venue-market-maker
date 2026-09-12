"""Exchange-free shadow orders, control portfolios, and markouts."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .lifecycle import (
    LifecycleAction,
    QuoteReconciler,
    ShadowOrder,
    quote_is_outside_mid_threshold,
)
from .markouts import maker_perspective_markout
from .models import (
    ZERO,
    BookSnapshot,
    FairValue,
    FillRecord,
    LifecycleState,
    MarketMode,
    MarketState,
    MarkoutRecord,
    ReferenceControl,
    RuntimeCounters,
    Side,
    TradePrint,
)
from .refresh_governor import (
    ActionPriority,
    ActionRequest,
    OrderActionGovernor,
    action_priority,
    is_adverse_fast_move,
)
from .risk import ActionRateWindow
from .telemetry import TelemetryStore

FILL_MODELS = ("CONSERVATIVE", "TOUCH_SENSITIVITY")
CONTROL_MODELS = (
    ReferenceControl.DERIVE_ONLY.value,
    ReferenceControl.BINANCE_ONLY_REFERENCE.value,
    ReferenceControl.MULTI_SOURCE_CONSENSUS.value,
)
PRIORITY_CONTROL_MODELS = (
    ReferenceControl.DERIVE_ONLY.value,
    ReferenceControl.BINANCE_ONLY_NO_FAILOVER.value,
    ReferenceControl.PRIORITY_FAILOVER.value,
)


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
            reference_control=fill.reference_control,
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
    """One independent reference-control/fill-model portfolio."""

    def __init__(
        self,
        name: str,
        capital: Decimal,
        fee_bps: Decimal,
        telemetry: TelemetryStore,
        *,
        reference_control: str | None = None,
        fill_model: str | None = None,
        max_actions_per_minute: int = 30,
        max_actions_per_second: Decimal = Decimal("1"),
        max_actions_per_instrument_per_second: Decimal = Decimal("1"),
        emergency_cancel_budget_per_minute: int = 6,
    ) -> None:
        self.name = name
        self.reference_control = reference_control or (
            name.split(":", 1)[0] if ":" in name else ReferenceControl.BINANCE_ONLY_REFERENCE.value
        )
        self.fill_model = fill_model or (name.split(":", 1)[1] if ":" in name else name)
        if self.fill_model not in FILL_MODELS:
            raise ValueError(f"unknown shadow fill model: {self.fill_model}")
        self.reconciler = QuoteReconciler()
        self.portfolio = ShadowPortfolio(capital, fee_bps)
        self.telemetry = telemetry
        self.pending: list[PendingMarkouts] = []
        self.counters = RuntimeCounters()
        self._seen_trade_ids: set[str] = set()
        self._action_rate = ActionRateWindow(60)
        self.max_actions_per_minute = max_actions_per_minute
        self.action_governor = OrderActionGovernor(
            max_actions_per_second=max_actions_per_second,
            max_actions_per_minute=max_actions_per_minute,
            max_actions_per_instrument_per_second=max_actions_per_instrument_per_second,
            emergency_cancel_budget_per_minute=emergency_cancel_budget_per_minute,
        )

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
        mid_price: Decimal | None = None,
        paused: bool,
        refresh_deadband_bps: Decimal | None = None,
        minimum_normal_quote_residency_seconds: Decimal = ZERO,
        fast_adverse_move_bps: Decimal = ZERO,
        fast_adverse_move_threshold_bps: Decimal = ZERO,
        fast_adverse_move_override: bool = False,
        tick_size: Decimal | None = None,
    ) -> LifecycleAction:
        lifecycle = self.reconciler.state(asset, side)
        current = lifecycle.order
        mutation_expected = False
        expected_reason = "NORMAL_REFRESH"
        if lifecycle.state not in {LifecycleState.CANCEL_REQUESTED, LifecycleState.WAITING_CANCEL}:
            if current is None:
                mutation_expected = not paused and price is not None and amount > ZERO
                expected_reason = "NORMAL_REFRESH"
            elif paused or price is None or amount <= ZERO:
                mutation_expected = True
                expected_reason = "PROTECTIVE_PLAN_INVALID"
            elif refresh_deadband_bps is not None:
                if fast_adverse_move_override and is_adverse_fast_move(
                    side, fast_adverse_move_bps, fast_adverse_move_threshold_bps
                ):
                    mutation_expected = True
                    expected_reason = "FAST_ADVERSE_MOVE_OVERRIDE"
                else:
                    candidate_price = price
                    if tick_size is not None and tick_size > ZERO:
                        from .quote_engine import round_down, round_up

                        candidate_price = (
                            round_down(price, tick_size)
                            if side == Side.BUY
                            else round_up(price, tick_size)
                        )
                    age = Decimal(str(max(0.0, now - current.created_at)))
                    movement_bps = (
                        abs(candidate_price - current.price) / current.price * Decimal("10000")
                        if current.price > ZERO
                        else Decimal("1e18")
                    )
                    mutation_expected = (
                        candidate_price != current.price
                        and movement_bps > refresh_deadband_bps
                        and age >= minimum_normal_quote_residency_seconds
                    )
                    expected_reason = "DEADBAND_REFRESH"
            elif mid_price is not None:
                mutation_expected = quote_is_outside_mid_threshold(
                    current.price, mid_price, tolerance_bps
                )
            else:
                movement_bps = abs(price - current.price) / current.price * Decimal("10000")
                age = Decimal(str(max(0.0, now - current.created_at)))
                mutation_expected = age >= max_age_seconds or movement_bps >= tolerance_bps
        request = ActionRequest(
            asset=asset,
            side=side.value,
            action="CANCEL" if current is not None else "CREATE",
            reason=expected_reason,
            priority=action_priority("CANCEL" if current is not None else "CREATE", expected_reason),
            timestamp=now,
            order_id=current.order_id if current else None,
            price=price,
            amount=amount,
        )
        governed_request: ActionRequest | None = None
        if refresh_deadband_bps is not None:
            if mutation_expected:
                # Keep only the latest desired request for this asset/side. A
                # higher-priority safety request remains authoritative.
                self.action_governor.submit(request)
                governed_request = self.action_governor.latest(request.key)
            else:
                pending = self.action_governor.latest(request.key)
                if pending is None or pending.priority > ActionPriority.RISK_REDUCTION:
                    self.action_governor.clear_non_safety(request.key)
                elif pending.action == "CANCEL" and current is not None:
                    # A queued risk/emergency cancellation is still safe and
                    # must not be lost merely because the next observation is
                    # no longer adverse.
                    governed_request = pending
                    mutation_expected = True
            if governed_request is not None and governed_request.priority <= ActionPriority.RISK_REDUCTION:
                if governed_request.action == "CANCEL" and current is not None:
                    action = LifecycleAction(
                        "CANCEL", asset, side, governed_request.reason, current.order_id
                    )
                else:
                    action = LifecycleAction(
                        "HOLD",
                        asset,
                        side,
                        "ACTION_RATE_LIMIT_NORMAL",
                        current.order_id if current else None,
                        current.price if current else None,
                        current.amount if current else ZERO,
                    )
                    governed_request = None
            elif governed_request is not None and not self.action_governor.allow(governed_request, now):
                action = LifecycleAction(
                    "HOLD",
                    asset,
                    side,
                    "ACTION_RATE_LIMIT_NORMAL",
                    current.order_id if current else None,
                    current.price if current else None,
                    current.amount if current else ZERO,
                )
                self.telemetry.insert_action(now, asset, side.value, action, self.name)
                self.counters.holds += 1
                self.counters.no_op_holds += 1
                self.telemetry.commit()
                return action
        if refresh_deadband_bps is None and mutation_expected and not self._action_rate.allowed(now, self.max_actions_per_minute):
            action = LifecycleAction(
                "HOLD",
                asset,
                side,
                "MAX_ACTIONS_PER_MINUTE",
                current.order_id if current else None,
                current.price if current else None,
                current.amount if current else ZERO,
            )
            self.telemetry.insert_action(now, asset, side.value, action, self.name)
            self.counters.holds += 1
            self.counters.no_op_holds += 1
            self.telemetry.commit()
            return action
        action = self.reconciler.reconcile(
            asset=asset,
            side=side,
            desired_price=price,
            desired_amount=amount,
            now=now,
            max_age_seconds=max_age_seconds,
            tolerance_bps=tolerance_bps,
            mid_price=mid_price,
            paused=paused,
            refresh_deadband_bps=refresh_deadband_bps,
            minimum_normal_quote_residency_seconds=minimum_normal_quote_residency_seconds,
            fast_adverse_move_bps=fast_adverse_move_bps,
            fast_adverse_move_threshold_bps=fast_adverse_move_threshold_bps,
            fast_adverse_move_override=fast_adverse_move_override,
            tick_size=tick_size,
        )
        self.telemetry.insert_action(now, asset, side.value, action, self.name)
        if action.kind == "CREATE":
            self.reconciler.acknowledge_create(action)
            self.counters.quote_creates += 1
            self.counters.actions += 1
        elif action.kind == "CANCEL":
            self.reconciler.acknowledge_cancel(action)
            self.counters.cancels += 1
            self.counters.actions += 1
        if action.kind in {"CREATE", "CANCEL"}:
            self._action_rate.record(now)
            if refresh_deadband_bps is not None:
                self.action_governor.record(
                    governed_request
                    or ActionRequest(
                        asset=asset,
                        side=side.value,
                        action=action.kind,
                        reason=action.reason,
                        priority=action_priority(action.kind, action.reason),
                        timestamp=now,
                        order_id=action.order_id,
                        price=action.price,
                        amount=action.amount,
                    ),
                    now,
                )
        elif action.kind == "HOLD":
            self.counters.holds += 1
            self.counters.no_op_holds += 1
        self.telemetry.commit()
        return action

    def active_order(self, asset: str, side: Side) -> ShadowOrder | None:
        return self.reconciler.state(asset, side).order

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
        del now
        fills: list[FillRecord] = []
        for trade in sorted(trades, key=lambda item: item.timestamp):
            # Reference prints never establish Derive execution evidence.
            if trade.source != "derive":
                continue
            if trade.trade_id and trade.trade_id in self._seen_trade_ids:
                continue
            if trade.trade_id:
                self._seen_trade_ids.add(trade.trade_id)
            for side in (Side.BUY, Side.SELL):
                order = self.active_order(asset, side)
                if order is None:
                    continue
                # A queued print cannot fill a quote created after that print.
                if trade.timestamp <= order.created_at:
                    continue
                if trade.exchange_timestamp is not None and trade.exchange_timestamp <= order.created_at:
                    continue
                if self.fill_model == "CONSERVATIVE":
                    hit = (side == Side.BUY and trade.side == Side.SELL and trade.price < order.price) or (
                        side == Side.SELL and trade.side == Side.BUY and trade.price > order.price
                    )
                else:
                    hit = (side == Side.BUY and trade.side == Side.SELL and trade.price <= order.price) or (
                        side == Side.SELL and trade.side == Side.BUY and trade.price >= order.price
                    )
                if not hit:
                    continue
                reference_value = fair_value.derive_fair_value
                edge = (
                    (reference_value - order.price) / reference_value * Decimal("10000")
                    if side == Side.BUY
                    else (order.price - reference_value) / reference_value * Decimal("10000")
                )
                fill = FillRecord(
                    timestamp=trade.timestamp,
                    asset=asset,
                    side=side,
                    amount=min(order.amount, trade.amount),
                    fill_price=order.price,
                    binance_fair_value=reference_value,
                    derive_mid=derive_book.mid,
                    inventory_before=ZERO,
                    inventory_after=ZERO,
                    maker_fee_bps=self.portfolio.fee_bps,
                    market_mode=market_state.market_mode,
                    direction=market_state.direction,
                    basis_bps=basis_bps,
                    quoted_edge_bps=edge,
                    model=self.name,
                    reference_control=self.reference_control,
                )
                corrected = self.portfolio.apply_fill(fill)
                self.telemetry.insert_fill(corrected)
                self.pending.append(PendingMarkouts(corrected))
                self.reconciler.remove_filled_order(asset, side, order.order_id)
                fills.append(corrected)
                if self.fill_model == "CONSERVATIVE":
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
                    binance_markout_bps=maker_perspective_markout(
                        pending.fill.side, pending.fill.fill_price, binance_fair_value
                    ),
                    derive_markout_bps=maker_perspective_markout(
                        pending.fill.side, pending.fill.fill_price, derive_mid
                    ),
                    model=self.name,
                    reference_control=self.reference_control,
                )
                self.telemetry.insert_markout(markout)
                result.append(markout)
        self.pending = [pending for pending in self.pending if len(pending.observed) < len(horizons)]
        if result:
            self.telemetry.commit()
        return result


class ShadowEngine:
    """Run all reference controls and fill sensitivities independently."""

    def __init__(
        self,
        capital: Decimal,
        fee_bps: Decimal,
        telemetry: TelemetryStore,
        controls: tuple[str, ...] = CONTROL_MODELS,
        max_actions_per_minute: int = 30,
        max_actions_per_second: Decimal = Decimal("1"),
        max_actions_per_instrument_per_second: Decimal = Decimal("1"),
        emergency_cancel_budget_per_minute: int = 6,
    ) -> None:
        self.models = {
            f"{control}:{fill_model}": ShadowModel(
                f"{control}:{fill_model}",
                capital,
                fee_bps,
                telemetry,
                reference_control=control,
                fill_model=fill_model,
                max_actions_per_minute=max_actions_per_minute,
                max_actions_per_second=max_actions_per_second,
                max_actions_per_instrument_per_second=max_actions_per_instrument_per_second,
                emergency_cancel_budget_per_minute=emergency_cancel_budget_per_minute,
            )
            for control in controls
            for fill_model in FILL_MODELS
        }

    def reconcile_plans(
        self,
        plans: dict[str, Any],
        now: float,
        max_age_seconds: Decimal,
        tolerance_bps: Decimal,
        mid_price: Decimal | None = None,
        refresh_deadband_bps: Decimal | None = None,
        minimum_normal_quote_residency_seconds: Decimal = ZERO,
        fast_adverse_move_bps: Decimal = ZERO,
        fast_adverse_move_threshold_bps: Decimal = ZERO,
        fast_adverse_move_override: bool = False,
        tick_size: Decimal | None = None,
    ) -> list[LifecycleAction]:
        actions: list[LifecycleAction] = []
        for _name, model in self.models.items():
            control = model.reference_control
            plan = plans.get(control)
            if plan is None:
                continue
            paused = plan.market_mode == MarketMode.PAUSED
            for side, price, amount in (
                (Side.BUY, plan.bid_price, plan.bid_amount),
                (Side.SELL, plan.ask_price, plan.ask_amount),
            ):
                actions.append(
                    model.reconcile(
                        asset=plan.asset,
                        side=side,
                        price=price,
                        amount=amount,
                        now=now,
                        max_age_seconds=max_age_seconds,
                        tolerance_bps=tolerance_bps,
                        mid_price=mid_price,
                        paused=paused,
                        refresh_deadband_bps=refresh_deadband_bps,
                        minimum_normal_quote_residency_seconds=minimum_normal_quote_residency_seconds,
                        fast_adverse_move_bps=fast_adverse_move_bps,
                        fast_adverse_move_threshold_bps=fast_adverse_move_threshold_bps,
                        fast_adverse_move_override=fast_adverse_move_override,
                        tick_size=tick_size,
                    )
                )
        return actions

    def reconcile_plan(
        self,
        plan: Any,
        now: float,
        max_age_seconds: Decimal,
        tolerance_bps: Decimal,
        mid_price: Decimal | None = None,
        refresh_deadband_bps: Decimal | None = None,
        minimum_normal_quote_residency_seconds: Decimal = ZERO,
        fast_adverse_move_bps: Decimal = ZERO,
        fast_adverse_move_threshold_bps: Decimal = ZERO,
        fast_adverse_move_override: bool = False,
        tick_size: Decimal | None = None,
    ) -> list[LifecycleAction]:
        """Compatibility method for callers that have one common plan."""

        return self.reconcile_plans(
            {ReferenceControl.BINANCE_ONLY_REFERENCE.value: plan},
            now,
            max_age_seconds,
            tolerance_bps,
            mid_price=mid_price,
            refresh_deadband_bps=refresh_deadband_bps,
            minimum_normal_quote_residency_seconds=minimum_normal_quote_residency_seconds,
            fast_adverse_move_bps=fast_adverse_move_bps,
            fast_adverse_move_threshold_bps=fast_adverse_move_threshold_bps,
            fast_adverse_move_override=fast_adverse_move_override,
            tick_size=tick_size,
        )
