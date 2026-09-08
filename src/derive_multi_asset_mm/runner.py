"""Public mainnet shadow runner and causal decision loop."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from .config import RuntimeConfig
from .inventory import classify_inventory
from .market_state import MarketStateEngine
from .models import ZERO, AssetMapping, BookSnapshot, TradePrint, json_safe
from .opportunity import score_asset
from .portfolio import portfolio_skew_bps
from .public_data import discover_mappings, parse_book_message, parse_trade_message, subscriptions
from .quote_engine import QuoteInputs, build_quote_plan
from .reference import RobustBasis, build_fair_value
from .reporting import finalize_reports
from .shadow_engine import ShadowEngine
from .telemetry import TelemetryStore


def _atomic_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


class ShadowRunner:
    """One detached, public-only run. No credentials or exchange mutations are used."""

    def __init__(self, config: RuntimeConfig) -> None:
        if config.mode.value != "MAINNET_SHADOW" or not config.dry_run or config.mainnet_armed:
            raise ValueError("shadow runner requires MAINNET_SHADOW, dry_run=true, mainnet_armed=false")
        self.config = config
        self.config.log_dir.mkdir(parents=True, exist_ok=True)
        self.config.report_dir.mkdir(parents=True, exist_ok=True)
        self.telemetry = TelemetryStore(config.database_path)
        self.mappings: dict[str, AssetMapping] = {}
        self.mapping_report: dict[str, Any] = {}
        self.basis = {}
        self.states = {}
        self.books_derive: dict[str, BookSnapshot] = {}
        self.books_binance: dict[str, BookSnapshot] = {}
        self.trades: dict[str, list[TradePrint]] = defaultdict(list)
        self.engines = {}
        self.shadow = ShadowEngine(config.capital_usdc, config.maker_fee_bps, self.telemetry)
        self.errors: list[str] = []
        self.started_at = time.time()
        self.last_update = self.started_at
        self.last_process: dict[str, float] = {}
        self.latest_decisions: dict[str, dict[str, Any]] = {}
        self.peak_equity = {name: config.capital_usdc for name in self.shadow.models}
        self.max_drawdown = {name: ZERO for name in self.shadow.models}
        self.pid = os.getpid()
        self.state_path = config.log_dir / "state.json"

    def bootstrap(self) -> dict[str, AssetMapping]:
        self.mappings, self.mapping_report = discover_mappings(self.config)
        for asset, _mapping in self.mappings.items():
            self.basis[asset] = RobustBasis(self.config.basis_window, self.config.basis_max_deviation_bps)
            self.states[asset] = MarketStateEngine()
        _atomic_json_write(self.config.report_dir / "asset_reference_mapping.json", self.mapping_report)
        self._write_state(status="BOOTSTRAPPED")
        return self.mappings

    def _write_state(self, *, status: str, ended_at: float | None = None) -> None:
        mappings = {
            asset: {
                "derive_instrument": mapping.derive_instrument,
                "derive_pair": mapping.derive_pair,
                "binance_symbol": mapping.binance_symbol,
                "reference_available": mapping.reference_available,
                "valid": mapping.valid,
                "reason": mapping.reason,
            }
            for asset, mapping in self.mappings.items()
        }
        model_state = {}
        mids = {asset: book.mid for asset, book in self.books_derive.items()}
        for name, model in self.shadow.models.items():
            equity = model.portfolio.equity(mids)
            self.peak_equity[name] = max(self.peak_equity[name], equity)
            self.max_drawdown[name] = max(self.max_drawdown[name], self.peak_equity[name] - equity)
            model_state[name] = {
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
            "latest_decisions": self.latest_decisions,
            "errors": self.errors[-20:],
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

    def process_asset(self, asset: str, now: float) -> None:
        derive_book = self.books_derive.get(asset)
        binance_book = self.books_binance.get(asset)
        if derive_book is None or binance_book is None:
            return
        if now - self.last_process.get(asset, 0.0) < 0.25:
            return
        self.last_process[asset] = now
        processing_started = time.perf_counter()
        derive_age = Decimal(str(max(0.0, now - derive_book.timestamp)))
        reference_age = Decimal(str(max(0.0, now - binance_book.timestamp)))
        fresh = derive_age <= self.config.bbo_stale_seconds and reference_age <= self.config.reference_stale_seconds
        fair = None
        divergence_protected = False
        if fresh:
            fair = build_fair_value(
                derive_book,
                binance_book,
                self.basis[asset],
                mid_weight=self.config.fair_value_mid_weight,
                microprice_weight=self.config.fair_value_microprice_weight,
                max_levels=self.config.max_book_levels,
            )
            divergence_protected = abs(fair.basis_bps - fair.baseline_basis_bps) > self.config.basis_max_deviation_bps
        state = self.states[asset].update(
            derive_book=derive_book,
            binance_book=binance_book,
            now=now,
            bbo_stale_seconds=self.config.bbo_stale_seconds,
            reference_stale_seconds=self.config.reference_stale_seconds,
            direction_threshold_bps=self.config.direction_threshold_bps,
            high_vol_threshold_bps=self.config.high_vol_threshold_bps,
            extreme_vol_threshold_bps=self.config.extreme_vol_threshold_bps,
            aggressive_spread_max_bps=self.config.aggressive_spread_max_bps,
            defensive_spread_min_bps=self.config.defensive_spread_min_bps,
            divergence_protected=divergence_protected or not fresh,
            max_levels=self.config.max_book_levels,
            fast_move_threshold_bps=self.config.fast_move_threshold_bps,
        )
        if not fresh:
            fair = None
            health = "DERIVE_STALE" if derive_age > self.config.bbo_stale_seconds else "REFERENCE_STALE"
        elif fair is None:
            health = "REFERENCE_UNAVAILABLE"
        else:
            health = "HEALTHY"
        conservative_portfolio = self.shadow.models["CONSERVATIVE"].portfolio
        inventories = {}
        for known_asset, book in self.books_derive.items():
            inventories[known_asset] = classify_inventory(
                conservative_portfolio.position(known_asset),
                book.mid,
                self._max_inventory_for(known_asset),
            )
        inventory = inventories.get(asset) or classify_inventory(ZERO, derive_book.mid, self._max_inventory_for(asset))
        portfolio_shift = portfolio_skew_bps(inventories, self.config.max_portfolio_inventory, self.config.portfolio_skew_max_bps)
        plan = build_quote_plan(
            asset=self.config.asset(asset),
            derive_pair=self.mappings[asset].derive_pair,
            derive_book=derive_book,
            fair_value=fair,
            market_state=state,
            inventory=inventory,
            portfolio_skew_bps=portfolio_shift,
            rules=self.mappings[asset].rules,
            inputs=self._quote_inputs(),
        )
        actions = self.shadow.reconcile_plan(plan, now, self.config.quote_max_age_seconds, self.config.refresh_tolerance_bps)
        asset_trades = self.trades.pop(asset, [])
        fills = []
        if fair is not None:
            for model in self.shadow.models.values():
                fills.extend(model.process_trades(asset=asset, trades=asset_trades, derive_book=derive_book, fair_value=fair, market_state=state, basis_bps=fair.basis_bps, now=now))
                model.record_markouts(asset=asset, now=now, binance_fair_value=fair.derive_fair_value, derive_mid=derive_book.mid)
        score = score_asset(plan, state, health, self.config.capital_usdc)
        selected_actions = [action.kind for action in actions if action.asset == asset]
        decision = {
            "derive_bid": derive_book.best_bid,
            "derive_ask": derive_book.best_ask,
            "derive_spread_bps": derive_book.spread / derive_book.mid * Decimal("10000") if derive_book.mid > 0 else ZERO,
            "binance_bid": binance_book.best_bid,
            "binance_ask": binance_book.best_ask,
            "binance_mid": fair.binance_mid if fair else None,
            "binance_microprice": fair.binance_microprice if fair else None,
            "fair_value": fair.derive_fair_value if fair else None,
            "basis_bps": fair.basis_bps if fair else None,
            "divergence_protected": divergence_protected,
            "market_mode": state.market_mode,
            "direction": state.direction,
            "volatility": state.volatility,
            "return_1s": state.return_1s,
            "return_5s": state.return_5s,
            "return_15s": state.return_15s,
            "realized_vol_30s": state.realized_vol_30s,
            "realized_vol_60s": state.realized_vol_60s,
            "velocity_bps": state.velocity_bps,
            "inventory_mode": inventory.mode,
            "position_amount": inventory.amount,
            "position_notional": inventory.position_notional,
            "buy_edge_bps": plan.buy_edge_bps,
            "sell_edge_bps": plan.sell_edge_bps,
            "required_edge_bps": plan.edge.total_required_bps,
            "desired_bid": plan.bid_price,
            "desired_ask": plan.ask_price,
            "desired_bid_amount": plan.bid_amount,
            "desired_ask_amount": plan.ask_amount,
            "block_reason": plan.block_reason,
            "bid_reason": plan.bid_reason,
            "ask_reason": plan.ask_reason,
            "data_health": health,
            "derive_data_age_seconds": derive_age,
            "reference_data_age_seconds": reference_age,
            "derive_exchange_timestamp": derive_book.exchange_timestamp,
            "binance_exchange_timestamp": binance_book.exchange_timestamp,
            "fast_move_protected": state.reason == "FAST_REFERENCE_MOVE",
            "selected_action": selected_actions,
            "opportunity_score": score,
            "processing_latency_ms": Decimal(str((time.perf_counter() - processing_started) * 1000)),
            "fill_count_this_cycle": len(fills),
        }
        self.telemetry.insert_decision(now, asset, decision)
        self.telemetry.commit()
        self.latest_decisions[asset] = decision

    async def _stream(self, *, source: str, url: str, channels: list[str], queue: asyncio.Queue, deadline: float) -> None:
        try:
            from websockets.asyncio.client import connect
        except ImportError:  # pragma: no cover
            from websockets import connect
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            try:
                async with connect(url, ping_interval=20, ping_timeout=20, close_timeout=2, max_size=4 * 1024 * 1024) as socket:
                    if source == "binance":
                        await socket.send(json.dumps({"method": "SUBSCRIBE", "params": channels, "id": attempt}))
                    else:
                        await socket.send(json.dumps({"jsonrpc": "2.0", "id": attempt, "method": "subscribe", "params": {"channels": channels}}))
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
                        receipt = time.time()
                        parsed_book = parse_book_message(payload, source=source, receipt_timestamp=receipt)
                        if parsed_book is not None:
                            await queue.put(("book", source, parsed_book[0], parsed_book[1]))
                        parsed_trade = parse_trade_message(payload, source=source, receipt_timestamp=receipt)
                        if parsed_trade is not None:
                            await queue.put(("trade", source, parsed_trade[0], parsed_trade[1]))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.errors.append(f"{source}:{type(exc).__name__}")
                await asyncio.sleep(min(2.0, max(0.1, deadline - time.monotonic())))

    def _asset_from_key(self, source: str, key: str) -> str | None:
        if source == "binance":
            return key if key in self.mappings else None
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
            report = finalize_reports(config=self.config, mappings=self.mappings, mapping_report=self.mapping_report, telemetry=self.telemetry, run_metadata={"status": "DATA_INSUFFICIENT"})
            self.telemetry.close()
            return report
        binance_channels, derive_channels = subscriptions(ready_mappings)
        queue: asyncio.Queue = asyncio.Queue()
        deadline = time.monotonic() + duration_seconds
        tasks = [
            asyncio.create_task(self._stream(source="binance", url=self.config.binance_websocket_url, channels=binance_channels, queue=queue, deadline=deadline)),
            asyncio.create_task(self._stream(source="derive", url=self.config.derive_websocket_url, channels=derive_channels, queue=queue, deadline=deadline)),
        ]
        self._write_state(status="RUNNING")
        try:
            while time.monotonic() < deadline:
                timeout = max(0.05, min(1.0, deadline - time.monotonic()))
                try:
                    kind, source, key, value = await asyncio.wait_for(queue.get(), timeout=timeout)
                    asset = self._asset_from_key(source, key)
                    if asset is None:
                        continue
                    if kind == "book":
                        if source == "binance":
                            self.books_binance[asset] = value
                        else:
                            self.books_derive[asset] = value
                    else:
                        self.trades[asset].append(value)
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
        report = finalize_reports(config=self.config, mappings=self.mappings, mapping_report=self.mapping_report, telemetry=self.telemetry, run_metadata={"status": "COMPLETE", "duration_seconds": duration_seconds})
        self.telemetry.close()
        return report
