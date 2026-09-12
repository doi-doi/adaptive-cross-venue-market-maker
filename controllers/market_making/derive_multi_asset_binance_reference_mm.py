"""Current Hummingbot Strategy V2 adapter for the multi-asset decision layer.

The installed market-making base is single-pair, so this adapter intentionally
uses ``ControllerBase`` and owns one bid/ask level per logical asset. It uses
the current executor action contracts and never sends shadow actions.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from itertools import islice
from typing import Any

from hummingbot.core.data_type.common import MarketDict, PositionAction, TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from pydantic import Field, field_validator, model_validator

from derive_multi_asset_mm.control import build_control_fair_value
from derive_multi_asset_mm.inventory import classify_inventory
from derive_multi_asset_mm.lifecycle import quote_is_outside_mid_threshold
from derive_multi_asset_mm.market_state import MarketStateEngine
from derive_multi_asset_mm.models import (
    BPS,
    ZERO,
    AssetSpec,
    BookSnapshot,
    DeriveRules,
    QuotePlacement,
    ReferenceControl,
    Side,
)
from derive_multi_asset_mm.portfolio import portfolio_skew_bps
from derive_multi_asset_mm.priority import PriorityReferenceSelector
from derive_multi_asset_mm.quote_engine import QuoteInputs, build_quote_plan
from derive_multi_asset_mm.reference import RobustBasis, aggregate_reference_book
from derive_multi_asset_mm.risk import ActionRateWindow, validate_rounded_order
from derive_multi_asset_mm.source_health import SourceHealth


class DeriveMultiAssetBinanceMMConfig(ControllerConfigBase):
    controller_name: str = Field(default="derive_multi_asset_binance_reference_mm")
    controller_type: str = Field(default="market_making")
    mode: str = Field(default="MAINNET_SHADOW")
    dry_run: bool = Field(default=True)
    mainnet_armed: bool = Field(default=False)
    derive_connector: str = Field(default="derive_perpetual")
    binance_connector: str = Field(default="binance_perpetual")
    reference_connectors: list[str] = Field(default_factory=lambda: ["binance_perpetual", "bybit_perpetual", "okx_perpetual"])
    multi_reference: bool = Field(default=False)
    reference_selection_mode: str = Field(default="PRIORITY_FAILOVER")
    reference_priority: list[str] = Field(default_factory=lambda: ["binance", "bybit", "okx"])
    bitget_enabled: bool = Field(default=False)
    bitget_primary_enabled: bool = Field(default=False)
    recovery_min_healthy_seconds: float = Field(default=3.0, gt=0)
    reference_healthy_seconds: Decimal = Field(default=Decimal("2"), gt=0)
    minimum_reference_sources: int = Field(default=1, ge=1, le=4)
    reference_outlier_bps: Decimal = Field(default=Decimal("50"), gt=0)
    reference_disagreement_pause_bps: Decimal = Field(default=Decimal("25"), gt=0)
    assets: list[str] = Field(default_factory=lambda: ["XRP", "LINK"])
    capital_usdc: Decimal = Field(default=Decimal("800"), gt=0)
    max_active_assets: int = Field(default=2, ge=1)
    order_size_multiplier: Decimal = Field(default=Decimal("1"), gt=0)
    maker_fee_bps: Decimal = Field(default=Decimal("1"), ge=0)
    fair_value_mid_weight: Decimal = Field(default=Decimal("0.5"), ge=0)
    fair_value_microprice_weight: Decimal = Field(default=Decimal("0.5"), ge=0)
    basis_window: int = Field(default=120, ge=1)
    basis_max_deviation_bps: Decimal = Field(default=Decimal("50"), gt=0)
    min_edge_bps: Decimal = Field(default=Decimal("4"), ge=0)
    volatility_buffer: Decimal = Field(default=Decimal("2"), ge=0)
    latency_buffer: Decimal = Field(default=Decimal("1"), ge=0)
    toxicity_buffer: Decimal = Field(default=Decimal("1"), ge=0)
    minimum_profit_buffer: Decimal = Field(default=Decimal("1"), ge=0)
    directional_skew_max_bps: Decimal = Field(default=Decimal("2"), ge=0)
    inventory_skew_max_bps: Decimal = Field(default=Decimal("12"), ge=0)
    portfolio_skew_max_bps: Decimal = Field(default=Decimal("4"), ge=0)
    bbo_stale_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    reference_stale_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    refresh_tolerance_bps: Decimal = Field(default=Decimal("200"), gt=0)
    quote_max_age_seconds: Decimal = Field(default=Decimal("30"), gt=0)
    max_single_order_notional: Decimal = Field(default=Decimal("120"), gt=0)
    max_open_order_notional: Decimal = Field(default=Decimal("400"), gt=0)
    max_inventory_per_asset: Decimal = Field(default=Decimal("200"), gt=0)
    max_portfolio_inventory: Decimal = Field(default=Decimal("400"), gt=0)
    max_actions_per_minute: int = Field(default=30, ge=1)
    quote_placement: str = Field(default="AT_TOUCH")
    max_book_levels: int = Field(default=5, ge=1)
    direction_threshold_bps: Decimal = Field(default=Decimal("1"), ge=0)
    high_vol_threshold_bps: Decimal = Field(default=Decimal("8"), gt=0)
    extreme_vol_threshold_bps: Decimal = Field(default=Decimal("20"), gt=0)
    aggressive_spread_max_bps: Decimal = Field(default=Decimal("15"), gt=0)
    defensive_spread_min_bps: Decimal = Field(default=Decimal("3"), ge=0)
    fast_move_threshold_bps: Decimal = Field(default=Decimal("8"), gt=0)
    leverage: int = Field(default=1, ge=1)

    @field_validator("mode", mode="before")
    @classmethod
    def normalize_mode(cls, value: Any) -> str:
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_safety(self) -> DeriveMultiAssetBinanceMMConfig:
        mode = self.mode.upper()
        if mode not in {"MAINNET_SHADOW", "MAINNET_LIVE"}:
            raise ValueError("mode must be MAINNET_SHADOW or MAINNET_LIVE")
        if self.derive_connector != "derive_perpetual":
            raise ValueError("derive_connector must be derive_perpetual; this project is mainnet-only")
        if mode == "MAINNET_SHADOW" and (not self.dry_run or self.mainnet_armed):
            raise ValueError("MAINNET_SHADOW requires dry_run=true and mainnet_armed=false")
        if mode == "MAINNET_LIVE" and (self.dry_run or not self.mainnet_armed):
            raise ValueError("MAINNET_LIVE requires dry_run=false and mainnet_armed=true")
        if len(self.assets) == 0 or self.max_active_assets > len(self.assets):
            raise ValueError("assets must be non-empty and cover max_active_assets")
        if len(set(self.reference_connectors)) != len(self.reference_connectors):
            raise ValueError("reference_connectors cannot contain duplicates")
        selection_mode = self.reference_selection_mode.strip().upper()
        if selection_mode not in {"LEGACY", "PRIORITY_FAILOVER"}:
            raise ValueError("reference_selection_mode must be LEGACY or PRIORITY_FAILOVER")
        priority = tuple(str(venue).strip().lower() for venue in self.reference_priority)
        configured = {connector.removesuffix("_perpetual") for connector in self.reference_connectors}
        if not self.bitget_enabled and "bitget" in configured:
            raise ValueError("bitget_enabled=false requires bitget_perpetual to be absent from reference_connectors")
        if not priority or len(set(priority)) != len(priority) or any(venue not in configured for venue in priority):
            raise ValueError("reference_priority must contain unique configured venues")
        if selection_mode == "PRIORITY_FAILOVER" and priority != ("binance", "bybit", "okx"):
            raise ValueError("PRIORITY_FAILOVER requires reference_priority binance, bybit, okx")
        if selection_mode == "PRIORITY_FAILOVER" and self.bitget_primary_enabled:
            raise ValueError("Bitget cannot be enabled as the primary reference")
        if self.fair_value_mid_weight + self.fair_value_microprice_weight <= ZERO:
            raise ValueError("fair value weights must sum to a positive value")
        if self.max_open_order_notional < self.max_single_order_notional:
            raise ValueError("max_open_order_notional must cover one single order")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for asset in self.assets:
            symbol = str(asset).strip().upper()
            result = markets.add_or_update(self.derive_connector, f"{symbol}-USDC")
            if result is not None:
                markets = result
            for connector in self.reference_connectors:
                result = markets.add_or_update(connector, f"{symbol}-USDT")
                if result is not None:
                    markets = result
        return markets


class DeriveMultiAssetBinanceMMController(ControllerBase):
    """One controller, independently validated two-sided maker quotes per asset."""

    _logger = None

    @classmethod
    def logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: DeriveMultiAssetBinanceMMConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self._assets = tuple(str(asset).strip().upper() for asset in config.assets)
        self._basis = {asset: RobustBasis(config.basis_window, config.basis_max_deviation_bps) for asset in self._assets}
        self._states = {asset: MarketStateEngine() for asset in self._assets}
        self._reference_books: dict[str, dict[str, BookSnapshot]] = {}
        self._reference_health: dict[str, dict[str, SourceHealth]] = {
            asset: {connector.removesuffix("_perpetual"): SourceHealth() for connector in config.reference_connectors}
            for asset in self._assets
        }
        self._plans: dict[str, Any] = {}
        self._books: dict[str, BookSnapshot] = {}
        self._last_processed: dict[str, float] = {}
        self._priority_selector = (
            PriorityReferenceSelector(
                priority=tuple(str(venue).strip().lower() for venue in config.reference_priority),
                recovery_min_healthy_seconds=config.recovery_min_healthy_seconds,
            )
            if config.reference_selection_mode.strip().upper() == "PRIORITY_FAILOVER"
            else None
        )
        self._pending_stop_ids: set[str] = set()
        self._actions = ActionRateWindow(60)
        self.processed_data = {}

    def _derive_pair(self, asset: str) -> str:
        return f"{asset}-USDC"

    def _binance_pair(self, asset: str) -> str:
        return f"{asset}-USDT"

    def _book(self, connector_name: str, pair: str, now: float) -> BookSnapshot:
        order_book = self.market_data_provider.get_order_book(connector_name, pair)
        bids = [(Decimal(str(row.price)), Decimal(str(row.amount))) for row in islice(order_book.bid_entries(), self.config.max_book_levels)]
        asks = [(Decimal(str(row.price)), Decimal(str(row.amount))) for row in islice(order_book.ask_entries(), self.config.max_book_levels)]
        if not bids or not asks or asks[0][0] < bids[0][0]:
            raise ValueError("empty_or_crossed_order_book")
        return BookSnapshot(now, bids[0][0], asks[0][0], bids[0][1], asks[0][1], tuple(bids), tuple(asks), source=connector_name)

    def _rules(self, asset: str) -> DeriveRules:
        pair = self._derive_pair(asset)
        raw = self.market_data_provider.get_trading_rules(self.config.derive_connector, pair)
        return DeriveRules(
            instrument_name=f"{asset}-PERP",
            base_asset=asset,
            quote_asset="USDC",
            tick_size=Decimal(str(raw.min_price_increment)),
            amount_step=Decimal(str(raw.min_base_amount_increment)),
            minimum_amount=Decimal(str(raw.min_order_size)),
            maximum_amount=(Decimal(str(raw.max_order_size)) if getattr(raw, "max_order_size", None) is not None else None),
            minimum_notional=Decimal(
                str(
                    max(
                        Decimal(str(getattr(raw, "min_notional_size", 0) or 0)),
                        Decimal(str(getattr(raw, "min_order_value", 0) or 0)),
                    )
                )
            ),
            maker_fee_bps=self.config.maker_fee_bps,
        )

    def _position(self, asset: str, mid: Decimal) -> Decimal:
        if self.config.mode == "MAINNET_SHADOW":
            return ZERO
        connector = self.market_data_provider.get_connector(self.config.derive_connector)
        positions = getattr(connector, "account_positions", {}) or {}
        total = ZERO
        for position in positions.values():
            if getattr(position, "trading_pair", "") == self._derive_pair(asset):
                total += Decimal(str(position.amount))
        return total

    def _runtime_inputs(self) -> QuoteInputs:
        return QuoteInputs(
            maker_fee_bps=self.config.maker_fee_bps,
            min_edge_bps=self.config.min_edge_bps,
            volatility_buffer_bps=self.config.volatility_buffer,
            latency_buffer_bps=self.config.latency_buffer,
            toxicity_buffer_bps=self.config.toxicity_buffer,
            minimum_profit_bps=self.config.minimum_profit_buffer,
            directional_skew_max_bps=self.config.directional_skew_max_bps,
            inventory_skew_max_bps=self.config.inventory_skew_max_bps,
            portfolio_skew_max_bps=self.config.portfolio_skew_max_bps,
            max_single_order_notional=self.config.max_single_order_notional,
            order_size_multiplier=self.config.order_size_multiplier,
            placement=QuotePlacement(self.config.quote_placement.upper()),
        )

    async def update_processed_data(self):
        now = self.market_data_provider.time()
        snapshots: dict[str, tuple[BookSnapshot, dict[str, BookSnapshot], DeriveRules]] = {}
        inventories = {}
        for asset in self._assets:
            try:
                derive_book = self._book(self.config.derive_connector, self._derive_pair(asset), now)
                reference_books: dict[str, BookSnapshot] = {}
                for connector in self.config.reference_connectors:
                    venue = connector.removesuffix("_perpetual")
                    try:
                        book = self._book(connector, self._binance_pair(asset), now)
                    except Exception:
                        continue
                    health = self._reference_health[asset][venue]
                    if not health.connected:
                        health.connect(now)
                    if health.accept(book):
                        reference_books[venue] = book
                rules = self._rules(asset)
                snapshots[asset] = (derive_book, reference_books, rules)
                inventories[asset] = classify_inventory(self._position(asset, derive_book.mid), derive_book.mid, self.config.max_inventory_per_asset)
            except Exception as exc:
                self.processed_data[asset] = {"asset": asset, "data_health": "REFERENCE_UNAVAILABLE", "block_reason": type(exc).__name__}
        portfolio_shift = portfolio_skew_bps(inventories, self.config.max_portfolio_inventory, self.config.portfolio_skew_max_bps)
        for asset, (derive_book, reference_books, rules) in snapshots.items():
            health = self._reference_health[asset]
            priority_selection = (
                self._priority_selector.select(
                    asset,
                    books=reference_books,
                    health=health,
                    now=now,
                    healthy_seconds=float(self.config.reference_healthy_seconds),
                    stale_seconds=float(self.config.reference_stale_seconds),
                    stale_overrides={},
                    mid_weight=self.config.fair_value_mid_weight,
                    microprice_weight=self.config.fair_value_microprice_weight,
                    max_levels=self.config.max_book_levels,
                )
                if self._priority_selector is not None
                else None
            )
            control = (
                ReferenceControl.PRIORITY_FAILOVER.value
                if priority_selection is not None
                else ReferenceControl.MULTI_SOURCE_CONSENSUS.value
                if self.config.multi_reference
                else ReferenceControl.BINANCE_ONLY_NO_FAILOVER.value
            )
            fair, reference_result = build_control_fair_value(
                control,
                derive_book=derive_book,
                source_books=reference_books,
                source_health=health,
                basis_tracker=self._basis[asset],
                now=now,
                healthy_seconds=float(self.config.reference_healthy_seconds),
                stale_seconds=float(self.config.reference_stale_seconds),
                stale_overrides={},
                outlier_bps=self.config.reference_outlier_bps,
                disagreement_bps=self.config.reference_disagreement_pause_bps,
                minimum_sources=self.config.minimum_reference_sources,
                mid_weight=self.config.fair_value_mid_weight,
                microprice_weight=self.config.fair_value_microprice_weight,
                max_levels=self.config.max_book_levels,
                priority_selection=priority_selection,
            )
            reference_book = (
                priority_selection.selected_book
                if priority_selection is not None and priority_selection.selected_book is not None
                else aggregate_reference_book(reference_books) or derive_book
            )
            protected = fair is None or bool(reference_result.get("pause_reason")) or abs(fair.basis_bps - fair.baseline_basis_bps) > self.config.basis_max_deviation_bps
            state = self._states[asset].update(
                derive_book=derive_book,
                binance_book=reference_book,
                now=now,
                bbo_stale_seconds=self.config.bbo_stale_seconds,
                reference_stale_seconds=self.config.reference_stale_seconds,
                direction_threshold_bps=self.config.direction_threshold_bps,
                high_vol_threshold_bps=self.config.high_vol_threshold_bps,
                extreme_vol_threshold_bps=self.config.extreme_vol_threshold_bps,
                aggressive_spread_max_bps=self.config.aggressive_spread_max_bps,
                defensive_spread_min_bps=self.config.defensive_spread_min_bps,
                divergence_protected=protected,
                max_levels=self.config.max_book_levels,
                fast_move_threshold_bps=self.config.fast_move_threshold_bps,
            )
            plan = build_quote_plan(
                asset=AssetSpec(asset),
                derive_pair=self._derive_pair(asset),
                derive_book=derive_book,
                fair_value=fair,
                market_state=state,
                inventory=inventories[asset],
                portfolio_skew_bps=portfolio_shift,
                rules=rules,
                inputs=self._runtime_inputs(),
            )
            self._plans[asset] = plan
            self._books[asset] = derive_book
            self.processed_data[asset] = {
                "asset": asset,
                "derive_bid": derive_book.best_bid,
                "derive_ask": derive_book.best_ask,
                "derive_spread_bps": derive_book.spread / derive_book.mid * BPS,
                "binance_mid": fair.binance_mid if fair else None,
                "binance_microprice": fair.binance_microprice if fair else None,
                "fair_value": fair.derive_fair_value if fair else None,
                "basis_bps": fair.basis_bps if fair else None,
                "reference_control": control,
                "reference_selection_mode": self.config.reference_selection_mode,
                "reference_priority": self.config.reference_priority,
                "selected_reference": reference_result.get("selected_reference"),
                "priority_event": reference_result.get("priority_event", ""),
                "failover_event": reference_result.get("failover_event", ""),
                "recovery_event": reference_result.get("recovery_event", ""),
                "recovery_ready": reference_result.get("recovery_ready", False),
                "time_using": reference_result.get("time_using", {}),
                "time_paused": reference_result.get("time_paused", ZERO),
                "reference_fair_value": fair.fair_value_raw if fair else None,
                "reference_sources": reference_result.get("valid_sources", []),
                "reference_dispersion_bps": reference_result.get("dispersion_bps"),
                "reference_pause_reason": reference_result.get("pause_reason", ""),
                "buy_edge_bps": plan.buy_edge_bps,
                "sell_edge_bps": plan.sell_edge_bps,
                "market_mode": state.market_mode.value,
                "direction": state.direction.value,
                "inventory_mode": inventories[asset].mode.value,
                "position": inventories[asset].amount,
                "bid_order": f"{plan.bid_amount}@{plan.bid_price}" if plan.bid_price else "NONE",
                "ask_order": f"{plan.ask_amount}@{plan.ask_price}" if plan.ask_price else "NONE",
                "data_health": (
                    reference_result.get("source_health", {}).get(reference_result.get("selected_reference"), {}).get("health", "")
                    if fair and reference_result.get("selected_reference")
                    else reference_result.get("pause_reason", "REFERENCE_UNAVAILABLE")
                ),
                "block_reason": plan.block_reason,
            }
        self._last_update = now

    @staticmethod
    def _level_id(executor: Any) -> str:
        custom = getattr(executor, "custom_info", {}) or {}
        return str(custom.get("level_id") or getattr(getattr(executor, "config", None), "level_id", ""))

    def _live_executors(self) -> dict[str, Any]:
        return {self._level_id(executor): executor for executor in self.executors_info if executor.is_active}

    def determine_executor_actions(self) -> list[ExecutorAction]:
        """Emit only armed live actions; shadow always returns an empty list."""

        if self.config.mode != "MAINNET_LIVE" or self.config.dry_run or not self.config.mainnet_armed:
            return []
        now = self.market_data_provider.time()
        active = self._live_executors()
        actions: list[ExecutorAction] = []
        stop_levels: set[str] = set()
        for level_id, executor in active.items():
            asset, side_name = level_id.split(":", 1) if ":" in level_id else ("", "")
            plan = self._plans.get(asset)
            desired_price = plan.bid_price if side_name == "bid" and plan else plan.ask_price if side_name == "ask" and plan else None
            desired_amount = plan.bid_amount if side_name == "bid" and plan else plan.ask_amount if side_name == "ask" and plan else ZERO
            old_price = Decimal(str(getattr(getattr(executor, "config", None), "price", ZERO)))
            derive_book = self._books.get(asset)
            outside_mid = quote_is_outside_mid_threshold(
                old_price,
                derive_book.mid if derive_book is not None else None,
                self.config.refresh_tolerance_bps,
            )
            if desired_price is None or desired_amount <= ZERO or outside_mid:
                if executor.id not in self._pending_stop_ids and self._actions.allowed(now, self.config.max_actions_per_minute):
                    self._pending_stop_ids.add(executor.id)
                    stop_levels.add(level_id)
                    self._actions.record(now)
                    actions.append(StopExecutorAction(controller_id=self.config.id, executor_id=executor.id, keep_position=True))
        if stop_levels:
            return actions
        planned_notional = sum((Decimal(str(getattr(getattr(executor, "config", None), "amount", ZERO))) * Decimal(str(getattr(getattr(executor, "config", None), "price", ZERO))) for executor in active.values()), ZERO)
        for asset, plan in self._plans.items():
            for side_name, side, price, amount in (("bid", TradeType.BUY, plan.bid_price, plan.bid_amount), ("ask", TradeType.SELL, plan.ask_price, plan.ask_amount)):
                level_id = f"{asset}:{side_name}"
                if level_id in active or price is None or amount <= ZERO:
                    continue
                if planned_notional + price * amount > self.config.max_open_order_notional:
                    continue
                if not self._actions.allowed(now, self.config.max_actions_per_minute):
                    continue
                try:
                    rules = self._rules(asset)
                    decision = validate_rounded_order(price=price, amount=amount, side=Side.BUY if side == TradeType.BUY else Side.SELL, rules=rules, max_single_order_notional=self.config.max_single_order_notional, max_inventory_notional=self.config.max_inventory_per_asset, portfolio_gross_inventory=planned_notional, max_portfolio_inventory=self.config.max_portfolio_inventory)
                except Exception:
                    continue
                if not decision.allowed:
                    continue
                action_config = OrderExecutorConfig(timestamp=now, controller_id=self.config.id, level_id=level_id, connector_name=self.config.derive_connector, trading_pair=self._derive_pair(asset), side=side, amount=decision.rounded_amount, price=decision.rounded_price, position_action=PositionAction.OPEN, execution_strategy=ExecutionStrategy.LIMIT_MAKER, leverage=self.config.leverage)
                actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=action_config))
                self._actions.record(now)
                planned_notional += decision.notional
        return actions

    def to_format_status(self) -> list[str]:
        if not self.processed_data:
            return ["Derive Multi-Asset Binance-Reference MM: waiting for data"]
        lines = [f"DERIVE MULTI-ASSET BINANCE-REFERENCE MM mode={self.config.mode} armed={self.config.mainnet_armed}"]
        for asset in self._assets:
            row = self.processed_data.get(asset, {})
            lines.append(
                f"{asset} derive={row.get('derive_bid', '—')}/{row.get('derive_ask', '—')} "
                f"references={row.get('reference_sources', '—')} fair={row.get('fair_value', '—')} "
                f"basis={row.get('basis_bps', '—')} edge={row.get('buy_edge_bps', '—')}/{row.get('sell_edge_bps', '—')} "
                f"state={row.get('market_mode', '—')}/{row.get('direction', '—')}/{row.get('inventory_mode', '—')} "
                f"position={row.get('position', '—')} health={row.get('data_health', '—')}"
            )
        return lines

    def get_custom_info(self) -> dict[str, Any]:
        return {"safety": {"mode": self.config.mode, "mainnet_armed": self.config.mainnet_armed, "real_orders": 0 if self.config.mode == "MAINNET_SHADOW" else "ARMED_ONLY"}, "assets": self.processed_data}
