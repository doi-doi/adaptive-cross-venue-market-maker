"""Public mainnet shadow runner for multi-venue reference controls."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from .config import RuntimeConfig
from .control import build_control_fair_value
from .inventory import classify_inventory
from .market_state import MarketStateEngine
from .models import (
    ZERO,
    AssetMapping,
    BookSnapshot,
    ReferenceControl,
    ReferenceMarket,
    TradePrint,
    json_safe,
)
from .multi_public import (
    discover_references_with_report,
    parse_snapshot,
    subscription,
    symbol_from_payload,
)
from .opportunity import score_asset
from .portfolio import portfolio_skew_bps
from .priority import PriorityReferenceSelector, PrioritySelection
from .priority_reporting import finalize_priority_reports
from .public_data import (
    DerivePublicClient,
    discover_mappings,
    parse_book_message,
    parse_trade_history_row,
    parse_trade_message,
)
from .quote_engine import QuoteInputs, build_quote_plan
from .reference import RobustBasis, aggregate_reference_book, microprice, source_fair_value
from .reporting import finalize_reports
from .shadow_engine import ShadowEngine
from .source_health import SourceHealth
from .telemetry import TelemetryStore


def _atomic_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _finalize_for_config(
    *,
    config: RuntimeConfig,
    mappings: dict[str, AssetMapping],
    mapping_report: dict[str, Any],
    telemetry: TelemetryStore,
    run_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if config.is_priority_failover:
        return finalize_priority_reports(
            config=config,
            mappings=mappings,
            mapping_report=mapping_report,
            telemetry=telemetry,
            run_metadata=run_metadata,
        )
    return finalize_reports(
        config=config,
        mappings=mappings,
        mapping_report=mapping_report,
        telemetry=telemetry,
        run_metadata=run_metadata,
    )


class ShadowRunner:
    """One detached, public-only run. Derive is the sole execution venue."""

    def __init__(self, config: RuntimeConfig) -> None:
        if config.mode.value != "MAINNET_SHADOW" or not config.dry_run or config.mainnet_armed:
            raise ValueError("shadow runner requires MAINNET_SHADOW, dry_run=true, mainnet_armed=false")
        self.config = config
        self.control_models = config.control_models
        self.config.log_dir.mkdir(parents=True, exist_ok=True)
        self.config.report_dir.mkdir(parents=True, exist_ok=True)
        self.telemetry = TelemetryStore(config.database_path, storage_config=config)
        self.mappings: dict[str, AssetMapping] = {}
        self.mapping_report: dict[str, Any] = {}
        self.reference_rows: list[dict[str, Any]] = []
        self.reference_rows_by_venue: dict[str, dict[str, dict[str, Any]]] = {}
        self.reference_markets: dict[str, dict[str, ReferenceMarket]] = defaultdict(dict)
        self.basis: dict[str, dict[str, RobustBasis]] = {}
        self.states: dict[str, dict[str, MarketStateEngine]] = {}
        self.books_derive: dict[str, BookSnapshot] = {}
        self.books_reference: dict[str, dict[str, BookSnapshot]] = defaultdict(dict)
        self.health: dict[str, dict[str, SourceHealth]] = defaultdict(dict)
        self.trades: dict[str, list[TradePrint]] = defaultdict(list)
        self.started_at = time.time()
        self.trade_feed_stats: dict[str, dict[str, Any]] = {}
        self._seen_trade_keys: set[tuple[str, str]] = set()
        self._trade_history_start_ms = int(self.started_at * 1000)
        self.derive_public_client = DerivePublicClient(self.config.derive_public_url)
        self.shadow = ShadowEngine(
            config.capital_usdc,
            config.maker_fee_bps,
            self.telemetry,
            controls=self.control_models,
            max_actions_per_minute=config.max_actions_per_minute,
            max_actions_per_second=config.max_order_actions_per_second,
            max_actions_per_instrument_per_second=config.max_order_actions_per_instrument_per_second,
            emergency_cancel_budget_per_minute=config.emergency_cancel_budget_per_minute,
        )
        self.priority_selector = (
            PriorityReferenceSelector(
                priority=config.reference_priority,
                recovery_min_healthy_seconds=config.recovery_min_healthy_seconds,
            )
            if config.is_priority_failover
            else None
        )
        self.errors: list[str] = []
        self.last_update = self.started_at
        self.last_process: dict[str, float] = {}
        self.last_health_persist: dict[tuple[str, str], float] = {}
        self.latest_decisions: dict[str, dict[str, Any]] = {}
        self.latest_consensus: dict[str, dict[str, Any]] = {}
        self.peak_equity = {name: config.capital_usdc for name in self.shadow.models}
        self.max_drawdown = {name: ZERO for name in self.shadow.models}
        self.pid = os.getpid()
        self.state_path = config.log_dir / "state.json"

    def bootstrap(self) -> dict[str, AssetMapping]:
        derive_mappings, derive_report = discover_mappings(self.config, require_binance_reference=False)
        self.reference_rows, reference_report = discover_references_with_report(
            [asset.symbol for asset in self.config.enabled_assets], self.config
        )
        self.reference_rows_by_venue = {
            venue: {str(row["symbol"]).upper(): row for row in self.reference_rows if row.get("venue") == venue and row.get("symbol")}
            for venue in self.config.reference_venues
        }
        rows_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in self.reference_rows:
            rows_by_asset[str(row["asset"]).upper()].append(row)
        for asset_spec in self.config.enabled_assets:
            asset = asset_spec.symbol
            mapping = derive_mappings.get(asset, AssetMapping(asset, None, None, f"{asset}USDT", valid=False, reason="DERIVE_INSTRUMENT_UNAVAILABLE"))
            markets: list[ReferenceMarket] = []
            for row in rows_by_asset.get(asset, []):
                market = ReferenceMarket(
                    asset=asset,
                    venue=str(row["venue"]),
                    connector=str(row["connector"]),
                    symbol=row.get("symbol"),
                    status=str(row.get("status", "REFERENCE_UNAVAILABLE")),
                    reason=str(row.get("reason", "")),
                    contract_type=str(row.get("contract_type", "perpetual")),
                    underlying=str(row.get("underlying") or asset),
                    quote=str(row.get("quote", "USDT")),
                    amount_multiplier=Decimal(str(row.get("amount_multiplier") or "1")),
                    tick_size=self._decimal_optional(row.get("tick_size")),
                    amount_step=self._decimal_optional(row.get("amount_step")),
                    minimum_amount=self._decimal_optional(row.get("minimum_amount")),
                    minimum_notional=self._decimal_optional(row.get("minimum_notional")),
                )
                markets.append(market)
                self.reference_markets[asset][market.venue] = market
                self.health[asset][market.venue] = SourceHealth(available=market.ready)
            self.health[asset]["derive"] = SourceHealth(available=mapping.valid)
            ready_count = sum(market.ready for market in markets)
            reason = mapping.reason
            primary_ready_count = sum(
                market.ready and market.venue in self.config.reference_priority for market in markets
            )
            if mapping.valid and self.config.is_priority_failover and primary_ready_count == 0:
                reason = "PRIMARY_REFERENCE_UNAVAILABLE"
            elif mapping.valid and ready_count < self.config.minimum_reference_sources:
                reason = "INSUFFICIENT_REFERENCE_SOURCES"
            elif mapping.valid and self.config.is_priority_failover:
                reason = "READY_PRIORITY_REFERENCE"
            elif mapping.valid:
                reason = "READY_MULTI_REFERENCE"
            self.mappings[asset] = replace(
                mapping,
                reference_available=ready_count > 0,
                reason=reason,
                reference_markets=tuple(markets),
            )
            self.basis[asset] = {
                control: RobustBasis(
                    self.config.basis_window,
                    self.config.basis_max_deviation_bps,
                    self.config.basis_ewma_alpha,
                )
                for control in self.control_models
            }
            self.states[asset] = {control: MarketStateEngine() for control in self.control_models}
            self.trade_feed_stats[asset] = {
                "websocket_trade_rows": 0,
                "rest_polls": 0,
                "rest_rows_seen": 0,
                "rest_taker_rows": 0,
                "deduplicated_rows": 0,
                "last_trade_exchange_timestamp": None,
                "last_trade_receipt_timestamp": None,
                "last_rest_poll_timestamp": None,
                "last_rest_error": None,
            }
        self.mapping_report = {
            "observed_at": time.time(),
            "derive": derive_report,
            "reference": reference_report,
            "mappings": self.mappings,
            "credentials_loaded": False,
            "private_api_used": False,
            "real_orders_created": 0,
            "real_orders_cancelled": 0,
            "real_positions": 0,
        }
        _atomic_json_write(self.config.report_dir / "asset_reference_mapping.json", self.mapping_report)
        self._write_state(status="BOOTSTRAPPED")
        return self.mappings

    @staticmethod
    def _decimal_optional(value: Any) -> Decimal | None:
        if value is None or value == "":
            return None
        try:
            result = Decimal(str(value))
        except (TypeError, ValueError):
            return None
        return result if result.is_finite() and result > 0 else None

    def _write_state(self, *, status: str, ended_at: float | None = None) -> None:
        now = time.time()
        mappings = {
            asset: {
                "derive_instrument": mapping.derive_instrument,
                "derive_pair": mapping.derive_pair,
                "binance_symbol": mapping.binance_symbol,
                "reference_available": mapping.reference_available,
                "valid": mapping.valid,
                "reason": mapping.reason,
                "reference_markets": mapping.reference_markets,
            }
            for asset, mapping in self.mappings.items()
        }
        model_state: dict[str, Any] = {}
        asset_fill_counts: dict[str, dict[str, int]] = defaultdict(dict)
        asset_fill_volume: dict[str, dict[str, Decimal]] = defaultdict(dict)
        mids = {asset: book.mid for asset, book in self.books_derive.items()}
        for name, model in self.shadow.models.items():
            equity = model.portfolio.equity(mids)
            self.peak_equity[name] = max(self.peak_equity[name], equity)
            self.max_drawdown[name] = max(self.max_drawdown[name], self.peak_equity[name] - equity)
            model_state[name] = {
                "reference_control": model.reference_control,
                "fill_model": model.fill_model,
                "quote_creates": model.counters.quote_creates,
                "holds": model.counters.holds,
                "replaces": model.counters.replaces,
                "cancels": model.counters.cancels,
                "fills": len(model.portfolio.fills),
                "fees": model.portfolio.fees,
                "gross_inventory": model.portfolio.gross_inventory(mids),
                "net_inventory": model.portfolio.net_inventory(mids),
                "equity": equity,
                "max_drawdown": self.max_drawdown[name],
                "positions": model.portfolio.positions,
                "position_notionals": {
                    asset: model.portfolio.position(asset) * mids.get(asset, ZERO)
                    for asset in model.portfolio.positions
                },
            }
            for asset in self.mappings:
                fills = [fill for fill in model.portfolio.fills if fill.asset == asset]
                asset_fill_counts[asset][name] = len(fills)
                asset_fill_volume[asset][name] = sum(
                    (fill.amount * fill.fill_price for fill in fills), ZERO
                )
        health_state = {
            asset: {
                venue: source.snapshot(
                    now,
                    self.config.reference_healthy_seconds,
                    float(self.config.reference_stale_overrides.get(venue, self.config.reference_stale_seconds)),
                )
                for venue, source in venues.items()
            }
            for asset, venues in self.health.items()
        }
        payload = {
            **self.config.public_safety(),
            "pid": self.pid,
            "status": status,
            "started_at": self.started_at,
            "last_update": self.last_update,
            "ended_at": ended_at,
            "active_assets": [asset for asset, mapping in self.mappings.items() if mapping.valid],
            "mappings": mappings,
            "models": model_state,
            "asset_fill_counts": dict(asset_fill_counts),
            "asset_fill_volume": dict(asset_fill_volume),
            "source_health": health_state,
            "latest_decisions": self.latest_decisions,
            "latest_consensus": self.latest_consensus,
            "trade_feed": self.trade_feed_stats,
            "errors": self.errors[-30:],
            "dashboard_url": "http://127.0.0.1:8770/",
        }
        _atomic_json_write(self.state_path, payload)
        self.telemetry.set_state("runtime", payload, self.last_update)

    def _quote_inputs(self) -> QuoteInputs:
        return QuoteInputs(
            maker_fee_bps=self.config.maker_fee_bps,
            min_edge_bps=self.config.min_edge_bps,
            volatility_buffer_bps=self.config.volatility_buffer_bps,
            latency_buffer_bps=self.config.latency_buffer_bps,
            toxicity_buffer_bps=self.config.toxicity_buffer_bps,
            minimum_profit_bps=self.config.minimum_profit_bps,
            directional_skew_max_bps=self.config.directional_skew_max_bps,
            inventory_skew_max_bps=self.config.inventory_skew_max_bps,
            portfolio_skew_max_bps=self.config.portfolio_skew_max_bps,
            max_single_order_notional=self.config.max_single_order_notional,
            order_size_multiplier=self.config.order_size_multiplier,
            placement=self.config.quote_placement,
        )

    def _max_inventory_for(self, asset: str) -> Decimal:
        override = self.config.asset(asset).max_inventory_per_asset
        return override if override is not None else self.config.max_inventory_per_asset

    def _source_books(self, asset: str) -> dict[str, BookSnapshot]:
        return {
            venue: book
            for venue, book in self.books_reference.get(asset, {}).items()
            if self.health[asset].get(venue) is not None
        }

    def _state_book(
        self,
        asset: str,
        control: str,
        now: float,
        priority_selection: PrioritySelection | None = None,
    ) -> BookSnapshot:
        if control == ReferenceControl.DERIVE_ONLY.value:
            return self.books_derive[asset]
        if control == ReferenceControl.PRIORITY_FAILOVER.value:
            if priority_selection is not None and priority_selection.selected_book is not None:
                return priority_selection.selected_book
            return self.books_derive[asset]
        source_books = self._source_books(asset)
        if control in {
            ReferenceControl.BINANCE_ONLY_REFERENCE.value,
            ReferenceControl.BINANCE_ONLY_NO_FAILOVER.value,
        }:
            book = source_books.get("binance")
            if book is not None and self.health[asset]["binance"].status(now, self.config.reference_healthy_seconds, float(self.config.reference_stale_seconds)) != "STALE":
                return book
        else:
            eligible = {
                venue: book
                for venue, book in source_books.items()
                if self.health[asset][venue].status(
                    now,
                    self.config.reference_healthy_seconds,
                    float(self.config.reference_stale_overrides.get(venue, self.config.reference_stale_seconds)),
                )
                in {"HEALTHY", "DEGRADED"}
            }
            aggregate = aggregate_reference_book(eligible)
            if aggregate is not None:
                return aggregate
        # The state engine still receives a causal Derive observation when a
        # reference is unavailable; divergence protection then pauses quotes.
        return self.books_derive[asset]

    def _persist_source_telemetry(self, asset: str, now: float, consensus_result: dict[str, Any]) -> None:
        deviations = consensus_result.get("deviations_bps", {})
        for venue, source in self.health[asset].items():
            if now - self.last_health_persist.get((asset, venue), 0.0) < 1.0:
                continue
            limit = float(self.config.reference_stale_overrides.get(venue, self.config.reference_stale_seconds))
            snapshot = source.snapshot(now, self.config.reference_healthy_seconds, limit)
            self.telemetry.insert_reference_health(now, asset, venue, snapshot)
            book = self.books_derive.get(asset) if venue == "derive" else self.books_reference.get(asset, {}).get(venue)
            value = None
            if book is not None and snapshot["health"] in {"HEALTHY", "DEGRADED"}:
                value = source_fair_value(
                    venue,
                    book,
                    mid_weight=self.config.fair_value_mid_weight,
                    microprice_weight=self.config.fair_value_microprice_weight,
                    max_levels=self.config.max_book_levels,
                    now=now,
                    health=snapshot["health"],
                    deviation_bps=deviations.get(venue),
                )
            excluded_reason = "" if value is not None else snapshot["health"]
            self.telemetry.insert_reference_value(
                timestamp=now,
                asset=asset,
                venue=venue,
                fair_value=value.fair_value if value else None,
                mid=value.mid if value else (book.mid if book else None),
                microprice=value.microprice if value else (microprice(book) if book else None),
                health=snapshot["health"],
                bbo_age=snapshot["bbo_age"],
                deviation_bps=deviations.get(venue),
                valid=value is not None and venue in consensus_result.get("valid_sources", []),
                excluded_reason=excluded_reason,
            )
            self.last_health_persist[(asset, venue)] = now

    def process_asset(self, asset: str, now: float) -> None:
        derive_book = self.books_derive.get(asset)
        if derive_book is None or not self.health[asset]["derive"].connected:
            return
        if now - self.last_process.get(asset, 0.0) < 0.25:
            return
        self.last_process[asset] = now
        processing_started = time.perf_counter()
        derive_age = Decimal(str(max(0.0, now - derive_book.timestamp)))
        source_books = self._source_books(asset)
        source_health = {venue: self.health[asset][venue] for venue in self.config.reference_venues}
        priority_selection = (
            self.priority_selector.select(
                asset,
                books=source_books,
                health=source_health,
                now=now,
                healthy_seconds=self.config.reference_healthy_seconds,
                stale_seconds=float(self.config.reference_stale_seconds),
                stale_overrides={venue: float(value) for venue, value in self.config.reference_stale_overrides.items()},
                mid_weight=self.config.fair_value_mid_weight,
                microprice_weight=self.config.fair_value_microprice_weight,
                max_levels=self.config.max_book_levels,
            )
            if self.priority_selector is not None
            else None
        )
        fairs: dict[str, Any] = {}
        details: dict[str, dict[str, Any]] = {}
        states: dict[str, Any] = {}
        plans: dict[str, Any] = {}
        inventory_by_control: dict[str, Any] = {}
        for control in self.control_models:
            fair, result = build_control_fair_value(
                control,
                derive_book=derive_book,
                source_books=source_books,
                source_health=source_health,
                basis_tracker=self.basis[asset][control],
                now=now,
                healthy_seconds=self.config.reference_healthy_seconds,
                stale_seconds=float(self.config.reference_stale_seconds),
                stale_overrides={venue: float(value) for venue, value in self.config.reference_stale_overrides.items()},
                outlier_bps=self.config.reference_outlier_bps,
                disagreement_bps=self.config.reference_disagreement_pause_bps,
                minimum_sources=self.config.minimum_reference_sources,
                mid_weight=self.config.fair_value_mid_weight,
                microprice_weight=self.config.fair_value_microprice_weight,
                max_levels=self.config.max_book_levels,
                priority_selection=priority_selection,
            )
            fairs[control] = fair
            details[control] = result
            state_book = self._state_book(asset, control, now, priority_selection)
            divergence_protected = control != ReferenceControl.DERIVE_ONLY.value and (
                fair is None
                or abs(fair.basis_bps - fair.baseline_basis_bps) > self.config.basis_max_deviation_bps
                or bool(result.get("pause_reason"))
            )
            state = self.states[asset][control].update(
                derive_book=derive_book,
                binance_book=state_book,
                now=now,
                bbo_stale_seconds=self.config.bbo_stale_seconds,
                reference_stale_seconds=self.config.reference_stale_seconds,
                direction_threshold_bps=self.config.direction_threshold_bps,
                high_vol_threshold_bps=self.config.high_vol_threshold_bps,
                extreme_vol_threshold_bps=self.config.extreme_vol_threshold_bps,
                aggressive_spread_max_bps=self.config.aggressive_spread_max_bps,
                defensive_spread_min_bps=self.config.defensive_spread_min_bps,
                divergence_protected=divergence_protected,
                max_levels=self.config.max_book_levels,
                fast_move_threshold_bps=self.config.fast_move_threshold_bps,
            )
            states[control] = state
            model = self.shadow.models[f"{control}:CONSERVATIVE"]
            inventories = {
                known_asset: classify_inventory(
                    model.portfolio.position(known_asset),
                    book.mid,
                    self._max_inventory_for(known_asset),
                )
                for known_asset, book in self.books_derive.items()
            }
            inventory_by_control[control] = inventories.get(asset) or classify_inventory(
                ZERO,
                derive_book.mid,
                self._max_inventory_for(asset),
            )
            shift = portfolio_skew_bps(
                inventories,
                self.config.max_portfolio_inventory,
                self.config.portfolio_skew_max_bps,
            )
            plans[control] = build_quote_plan(
                asset=self.config.asset(asset),
                derive_pair=self.mappings[asset].derive_pair,
                derive_book=derive_book,
                fair_value=fair,
                market_state=state,
                inventory=inventory_by_control[control],
                portfolio_skew_bps=shift,
                rules=self.mappings[asset].rules,
                inputs=self._quote_inputs(),
            )
        actions = self.shadow.reconcile_plans(
            plans,
            now,
            self.config.quote_max_age_seconds,
            self.config.refresh_tolerance_bps,
            mid_price=derive_book.mid,
            refresh_deadband_bps=self.config.refresh_deadband_bps,
            minimum_normal_quote_residency_seconds=self.config.minimum_normal_quote_residency_seconds,
            fast_adverse_move_bps=(
                states[self.config.primary_reference_control].return_1s
                if self.config.fast_adverse_move_override_enabled
                else ZERO
            ),
            fast_adverse_move_threshold_bps=self.config.fast_adverse_move_threshold_bps,
            fast_adverse_move_override=self.config.fast_adverse_move_override_enabled,
            tick_size=self.mappings[asset].rules.tick_size if self.mappings[asset].rules else None,
        )
        asset_trades = self.trades.pop(asset, [])
        fills: list[Any] = []
        for control in self.control_models:
            fair = fairs[control]
            state = states[control]
            if fair is None:
                continue
            for model in self.shadow.models.values():
                if model.reference_control != control:
                    continue
                fills.extend(
                    model.process_trades(
                        asset=asset,
                        trades=asset_trades,
                        derive_book=derive_book,
                        fair_value=fair,
                        market_state=state,
                        basis_bps=fair.basis_bps,
                        now=now,
                    )
                )
                model.record_markouts(
                    asset=asset,
                    now=now,
                    binance_fair_value=fair.derive_fair_value,
                    derive_mid=derive_book.mid,
                )
        selected_control = self.config.primary_reference_control
        selected_fair = fairs[selected_control]
        selected_state = states[selected_control]
        selected_plan = plans[selected_control]
        selected_result = details[selected_control]
        self.latest_consensus[asset] = selected_result
        self._persist_source_telemetry(asset, now, selected_result)
        selected_health = "HEALTHY" if selected_fair is not None else (
            selected_result.get("pause_reason") or "REFERENCE_UNAVAILABLE"
        )
        score = score_asset(selected_plan, selected_state, selected_health, self.config.capital_usdc)
        selected_model = self.shadow.models.get(f"{selected_control}:CONSERVATIVE")
        selected_pnl_proxy = None
        if selected_model is not None:
            selected_pnl_proxy = selected_model.portfolio.equity(
                {known_asset: book.mid for known_asset, book in self.books_derive.items()}
            ) - self.config.capital_usdc
        selected_actions = [action.kind for action in actions if action.asset == asset]
        selected_reference_book = self._state_book(asset, selected_control, now, priority_selection)
        reference_age = (
            Decimal(str(max(0.0, now - selected_reference_book.timestamp)))
            if selected_fair is not None and selected_result.get("selected_source")
            else None
        )
        source_bbo = {
            venue: {"bid": book.best_bid, "ask": book.best_ask}
            for venue, book in self.books_reference.get(asset, {}).items()
        }
        source_bbo["derive"] = {"bid": derive_book.best_bid, "ask": derive_book.best_ask}
        binance_book = self.books_reference.get(asset, {}).get("binance")
        decision: dict[str, Any] = {
            "derive_bid": derive_book.best_bid,
            "derive_ask": derive_book.best_ask,
            "derive_spread_bps": derive_book.spread / derive_book.mid * Decimal("10000") if derive_book.mid > 0 else ZERO,
            "binance_bid": binance_book.best_bid if binance_book else None,
            "binance_ask": binance_book.best_ask if binance_book else None,
            "binance_mid": selected_fair.binance_mid if selected_fair and selected_control in {ReferenceControl.BINANCE_ONLY_REFERENCE.value, ReferenceControl.BINANCE_ONLY_NO_FAILOVER.value} else (binance_book.mid if binance_book else None),
            "binance_microprice": selected_fair.binance_microprice if selected_fair and selected_control in {ReferenceControl.BINANCE_ONLY_REFERENCE.value, ReferenceControl.BINANCE_ONLY_NO_FAILOVER.value} else (microprice(binance_book) if binance_book else None),
            "fair_value": selected_fair.derive_fair_value if selected_fair else None,
            "basis_bps": selected_fair.basis_bps if selected_fair else None,
            "baseline_basis_bps": selected_fair.baseline_basis_bps if selected_fair else None,
            "reference_control": selected_control,
            "reference_selection_mode": self.config.reference_selection_mode,
            "reference_priority": self.config.reference_priority,
            "selected_reference": selected_result.get("selected_source"),
            "priority_event": selected_result.get("priority_event", ""),
            "failover_event": selected_result.get("failover_event", ""),
            "recovery_event": selected_result.get("recovery_event", ""),
            "recovery_ready": selected_result.get("recovery_ready", False),
            "recovery_seconds": selected_result.get("recovery_seconds"),
            "time_using": selected_result.get("time_using", {}),
            "time_paused": selected_result.get("time_paused", ZERO),
            "reference_fair_value": selected_fair.fair_value_raw if selected_fair else None,
            "reference_robust_median": selected_result.get("robust_median"),
            "source_fair_values": selected_fair.source_fair_values if selected_fair else selected_result.get("source_fair_values", {}),
            "source_mids": selected_result.get("source_mids", selected_fair.source_mids if selected_fair else {}),
            "source_microprices": {
                **selected_result.get("source_microprices", {}),
                "derive": microprice(derive_book),
            },
            "source_bbo": source_bbo,
            "valid_reference_sources": selected_result.get("valid_sources", []),
            "reference_outliers": selected_result.get("outliers", []),
            "reference_deviations_bps": selected_result.get("deviations_bps", {}),
            "reference_dispersion_bps": selected_result.get("dispersion_bps"),
            "reference_confidence": selected_result.get("confidence"),
            "reference_pause_reason": selected_result.get("pause_reason", ""),
            "divergence_protected": bool(selected_result.get("pause_reason")) or bool(
                selected_fair and abs(selected_fair.basis_bps - selected_fair.baseline_basis_bps) > self.config.basis_max_deviation_bps
            ),
            "market_mode": selected_state.market_mode,
            "direction": selected_state.direction,
            "volatility": selected_state.volatility,
            "return_1s": selected_state.return_1s,
            "return_5s": selected_state.return_5s,
            "return_15s": selected_state.return_15s,
            "realized_vol_30s": selected_state.realized_vol_30s,
            "realized_vol_60s": selected_state.realized_vol_60s,
            "velocity_bps": selected_state.velocity_bps,
            "inventory_mode": inventory_by_control[selected_control].mode,
            "position_amount": inventory_by_control[selected_control].amount,
            "position_notional": inventory_by_control[selected_control].position_notional,
            "buy_edge_bps": selected_plan.buy_edge_bps,
            "sell_edge_bps": selected_plan.sell_edge_bps,
            "required_edge_bps": selected_plan.edge.total_required_bps,
            "desired_bid": selected_plan.bid_price,
            "desired_ask": selected_plan.ask_price,
            "desired_bid_amount": selected_plan.bid_amount,
            "desired_ask_amount": selected_plan.ask_amount,
            "block_reason": selected_plan.block_reason,
            "bid_reason": selected_plan.bid_reason,
            "ask_reason": selected_plan.ask_reason,
            "data_health": selected_health,
            "derive_data_age_seconds": derive_age,
            "reference_data_age_seconds": reference_age,
            "derive_exchange_timestamp": derive_book.exchange_timestamp,
            "binance_exchange_timestamp": binance_book.exchange_timestamp if binance_book else None,
            "fast_move_protected": selected_state.reason == "FAST_REFERENCE_MOVE",
            "selected_action": selected_actions,
            "opportunity_score": score,
            "processing_latency_ms": Decimal(str((time.perf_counter() - processing_started) * 1000)),
            "fill_count_this_cycle": len(fills),
            "quote_active": selected_plan.bid_price is not None or selected_plan.ask_price is not None,
            "pnl_proxy": selected_pnl_proxy,
            "error_count_total": len(self.errors),
            "source_health": {
                venue: self.health[asset][venue].snapshot(
                    now,
                    self.config.reference_healthy_seconds,
                    float(self.config.reference_stale_overrides.get(venue, self.config.reference_stale_seconds)),
                )
                for venue in (*self.config.reference_venues, "derive")
            },
            "controls": {
                control: {
                    "fair_value": fairs[control],
                    "state": states[control],
                    "plan": plans[control],
                    "consensus": details[control],
                }
                for control in self.control_models
            },
        }
        self.telemetry.insert_decision(now, asset, decision)
        self.telemetry.commit()
        self.latest_decisions[asset] = decision

    def _source_url(self, venue: str) -> str:
        return {
            "binance": self.config.binance_websocket_url,
            "bybit": self.config.bybit_websocket_url,
            "okx": self.config.okx_websocket_url,
            "bitget": self.config.bitget_websocket_url,
        }[venue]

    def _mark_disconnected(self, venue: str, now: float) -> None:
        for asset in self.mappings:
            health = self.health[asset].get(venue)
            if health is not None:
                health.disconnect(now)
            if venue == "derive":
                self.books_derive.pop(asset, None)
            else:
                self.books_reference.get(asset, {}).pop(venue, None)

    async def _stream_reference(self, venue: str, queue: asyncio.Queue, deadline: float) -> None:
        try:
            from websockets.asyncio.client import connect
        except ImportError:  # pragma: no cover
            from websockets import connect
        rows = [row for row in self.reference_rows if row.get("venue") == venue and row.get("status") == "READY"]
        mapping = {str(row["symbol"]).upper(): row for row in rows if row.get("symbol")}
        if not mapping:
            return
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            try:
                async with connect(
                    self._source_url(venue),
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=2,
                    max_size=4 * 1024 * 1024,
                ) as socket:
                    now = time.time()
                    for row in rows:
                        self.health[str(row["asset"])][venue].connect(now)
                    await socket.send(json.dumps(subscription(venue, rows)))
                    while time.monotonic() < deadline:
                        raw = await asyncio.wait_for(socket.recv(), timeout=max(0.1, deadline - time.monotonic()))
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        received = time.time()
                        try:
                            parsed = parse_snapshot(venue, payload, received, mapping)
                        except (TypeError, ValueError, KeyError) as exc:
                            symbol = symbol_from_payload(venue, payload)
                            row = mapping.get(symbol or "")
                            if row:
                                self.health[str(row["asset"])][venue].record_parse_failure(received)
                            self.errors.append(f"{venue}:parse:{type(exc).__name__}")
                            continue
                        if parsed is not None:
                            asset, book, sequence, previous, repeat = parsed
                            await queue.put(("reference_book", venue, asset, (book, sequence, previous, repeat)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._mark_disconnected(venue, time.time())
                self.errors.append(f"{venue}:{type(exc).__name__}")
                await asyncio.sleep(min(2.0, max(0.1, deadline - time.monotonic())))

    async def _stream_derive(self, channels: list[str], queue: asyncio.Queue, deadline: float) -> None:
        try:
            from websockets.asyncio.client import connect
        except ImportError:  # pragma: no cover
            from websockets import connect
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            try:
                async with connect(
                    self.config.derive_websocket_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=2,
                    max_size=4 * 1024 * 1024,
                ) as socket:
                    now = time.time()
                    for asset in self.mappings:
                        self.health[asset]["derive"].connect(now)
                    await socket.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": attempt,
                                "method": "subscribe",
                                "params": {"channels": channels},
                            }
                        )
                    )
                    while time.monotonic() < deadline:
                        raw = await asyncio.wait_for(socket.recv(), timeout=max(0.1, deadline - time.monotonic()))
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        received = time.time()
                        try:
                            parsed_book = parse_book_message(payload, source="derive", receipt_timestamp=received)
                            parsed_trade = parse_trade_message(payload, source="derive", receipt_timestamp=received)
                        except (TypeError, ValueError, KeyError) as exc:
                            self.errors.append(f"derive:parse:{type(exc).__name__}")
                            continue
                        if parsed_book is not None:
                            await queue.put(("derive_book", "derive", parsed_book[0], parsed_book[1]))
                        if parsed_trade is not None:
                            asset = self._asset_from_derive_key(parsed_trade[0])
                            if asset is not None:
                                stats = self.trade_feed_stats.setdefault(asset, {})
                                stats["websocket_trade_rows"] = int(stats.get("websocket_trade_rows", 0)) + 1
                            await queue.put(("trade", "derive", parsed_trade[0], parsed_trade[1]))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._mark_disconnected("derive", time.time())
                self.errors.append(f"derive:{type(exc).__name__}")
                await asyncio.sleep(min(2.0, max(0.1, deadline - time.monotonic())))

    async def _poll_derive_trade_history(
        self,
        ready_mappings: dict[str, AssetMapping],
        queue: asyncio.Queue,
        deadline: float,
    ) -> None:
        """Backfill public Derive executions missed by the websocket stream."""

        cursors_ms = {asset: self._trade_history_start_ms for asset in ready_mappings}
        next_poll = time.monotonic()
        while time.monotonic() < deadline:
            delay = next_poll - time.monotonic()
            if delay > 0:
                await asyncio.sleep(min(delay, max(0.1, deadline - time.monotonic())))
            if time.monotonic() >= deadline:
                break
            poll_receipt = time.time()
            to_timestamp_ms = int(poll_receipt * 1000)
            requests = []
            request_meta = []
            for asset, mapping in ready_mappings.items():
                from_timestamp_ms = max(
                    self._trade_history_start_ms,
                    cursors_ms.get(asset, self._trade_history_start_ms) - 2000,
                )
                requests.append(
                    asyncio.to_thread(
                        self.derive_public_client.trade_history,
                        mapping.derive_instrument,
                        from_timestamp_ms=from_timestamp_ms,
                        to_timestamp_ms=to_timestamp_ms,
                        page_size=1000,
                    )
                )
                request_meta.append((asset, mapping, from_timestamp_ms))
            results = await asyncio.gather(*requests, return_exceptions=True)
            for (asset, mapping, _from_timestamp_ms), result in zip(request_meta, results, strict=True):
                stats = self.trade_feed_stats.setdefault(asset, {})
                stats["rest_polls"] = int(stats.get("rest_polls", 0)) + 1
                stats["last_rest_poll_timestamp"] = poll_receipt
                if isinstance(result, Exception):
                    stats["last_rest_error"] = f"{type(result).__name__}: {result}"
                    self.errors.append(f"derive_trade_history:{asset}:{type(result).__name__}")
                    continue
                stats["last_rest_error"] = None
                rows = result if isinstance(result, list) else []
                stats["rest_rows_seen"] = int(stats.get("rest_rows_seen", 0)) + len(rows)
                max_exchange_ms = cursors_ms.get(asset, self._trade_history_start_ms)
                for row in rows:
                    parsed = parse_trade_history_row(row, receipt_timestamp=time.time())
                    if parsed is None:
                        continue
                    key, trade = parsed
                    if key != mapping.derive_instrument:
                        continue
                    exchange_ms = (
                        int(trade.exchange_timestamp * 1000)
                        if trade.exchange_timestamp is not None
                        else to_timestamp_ms
                    )
                    if exchange_ms < self._trade_history_start_ms or exchange_ms > to_timestamp_ms + 1000:
                        continue
                    stats["rest_taker_rows"] = int(stats.get("rest_taker_rows", 0)) + 1
                    stats["last_trade_exchange_timestamp"] = trade.exchange_timestamp
                    stats["last_trade_receipt_timestamp"] = trade.timestamp
                    max_exchange_ms = max(max_exchange_ms, exchange_ms)
                    await queue.put(("trade", "derive", key, trade))
                cursors_ms[asset] = max(max_exchange_ms, to_timestamp_ms)
            next_poll = time.monotonic() + self.config.derive_trade_history_poll_seconds

    def _asset_from_derive_key(self, key: str) -> str | None:
        for asset, mapping in self.mappings.items():
            if mapping.derive_instrument == key:
                return asset
        return None

    async def run(self, duration_seconds: float) -> dict[str, Any]:
        if duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        self.bootstrap()
        ready_mappings = {asset: mapping for asset, mapping in self.mappings.items() if mapping.valid}
        if not ready_mappings:
            self._write_state(status="DATA_INSUFFICIENT", ended_at=time.time())
            report = _finalize_for_config(
                config=self.config,
                mappings=self.mappings,
                mapping_report=self.mapping_report,
                telemetry=self.telemetry,
                run_metadata={"status": "DATA_INSUFFICIENT"},
            )
            self.telemetry.close()
            return report
        reference_tasks = []
        deadline = time.monotonic() + duration_seconds
        queue: asyncio.Queue = asyncio.Queue()
        for venue in self.config.reference_venues:
            reference_tasks.append(asyncio.create_task(self._stream_reference(venue, queue, deadline)))
        derive_channels = sorted(
            {
                f"orderbook.{mapping.derive_instrument}.1.20"
                for mapping in ready_mappings.values()
                if mapping.derive_instrument
            }
            | {
                f"trades.{mapping.derive_instrument}"
                for mapping in ready_mappings.values()
                if mapping.derive_instrument
            }
        )
        tasks = [
            *reference_tasks,
            asyncio.create_task(self._stream_derive(derive_channels, queue, deadline)),
            asyncio.create_task(self._poll_derive_trade_history(ready_mappings, queue, deadline)),
        ]
        self._write_state(status="RUNNING")
        try:
            while time.monotonic() < deadline:
                timeout = max(0.05, min(1.0, deadline - time.monotonic()))
                try:
                    kind, source, key, value = await asyncio.wait_for(queue.get(), timeout=timeout)
                    if kind == "reference_book":
                        book, sequence, previous, repeat = value
                        health = self.health[key][source]
                        if health.accept(book, sequence, previous, repeat):
                            self.books_reference[key][source] = book
                    elif kind == "derive_book":
                        asset = self._asset_from_derive_key(key) or (key if key in self.mappings else None)
                        if asset is not None:
                            health = self.health[asset]["derive"]
                            if health.accept(value):
                                self.books_derive[asset] = value
                    elif kind == "trade":
                        asset = self._asset_from_derive_key(key)
                        if asset is not None:
                            trade_key = (asset, str(value.trade_id))
                            stats = self.trade_feed_stats.setdefault(asset, {})
                            if trade_key in self._seen_trade_keys:
                                stats["deduplicated_rows"] = int(stats.get("deduplicated_rows", 0)) + 1
                                continue
                            self._seen_trade_keys.add(trade_key)
                            self.trades[asset].append(value)
                            self.telemetry.insert_trade(value, asset)
                except TimeoutError:
                    pass
                now = time.time()
                for asset in ready_mappings:
                    self.process_asset(asset, now)
                if now - self.last_update >= 1.0:
                    self.last_update = now
                    self._write_state(status="RUNNING")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        ended = time.time()
        self._write_state(status="COMPLETE", ended_at=ended)
        report = _finalize_for_config(
            config=self.config,
            mappings=self.mappings,
            mapping_report=self.mapping_report,
            telemetry=self.telemetry,
            run_metadata={"status": "COMPLETE", "duration_seconds": duration_seconds},
        )
        self.telemetry.close()
        return report
