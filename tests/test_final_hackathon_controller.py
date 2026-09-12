from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel


class TradeType(Enum):
    BUY = "BUY"
    SELL = "SELL"


class PositionAction(Enum):
    OPEN = "OPEN"


class PositionMode(Enum):
    ONEWAY = "ONEWAY"


class MarketDict:
    def __init__(self):
        self.values = {}

    def add_or_update(self, connector, pair):
        self.values.setdefault(connector, set()).add(pair)
        return self


class ControllerConfigBase(BaseModel):
    id: str = "test"


class ControllerBase:
    def __init__(self, config, *args, **kwargs):
        self.config = config
        self.market_data_provider = kwargs.get("market_data_provider") or (args[0] if args else None)
        self.executors_info = []


class ExecutionStrategy(Enum):
    LIMIT_MAKER = "LIMIT_MAKER"


class OrderExecutorConfig:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class CreateExecutorAction:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class StopExecutorAction(CreateExecutorAction):
    pass


def _module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


for package in (
    "hummingbot",
    "hummingbot.core",
    "hummingbot.core.data_type",
    "hummingbot.strategy_v2",
    "hummingbot.strategy_v2.controllers",
    "hummingbot.strategy_v2.executors",
    "hummingbot.strategy_v2.executors.order_executor",
    "hummingbot.strategy_v2.models",
):
    _module(package)
_module(
    "hummingbot.core.data_type.common",
    MarketDict=MarketDict,
    PositionAction=PositionAction,
    PositionMode=PositionMode,
    TradeType=TradeType,
)
_module(
    "hummingbot.strategy_v2.controllers.controller_base",
    ControllerBase=ControllerBase,
    ControllerConfigBase=ControllerConfigBase,
)
_module(
    "hummingbot.strategy_v2.executors.order_executor.data_types",
    ExecutionStrategy=ExecutionStrategy,
    OrderExecutorConfig=OrderExecutorConfig,
)
_module(
    "hummingbot.strategy_v2.models.executor_actions",
    CreateExecutorAction=CreateExecutorAction,
    ExecutorAction=object,
    StopExecutorAction=StopExecutorAction,
)

CONTROLLER_PATH = Path(__file__).parents[1] / "controllers/market_making/derive_binance_adaptive_mm.py"
spec = importlib.util.spec_from_file_location("final_controller", CONTROLLER_PATH)
controller = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = controller
assert spec.loader is not None
spec.loader.exec_module(controller)


def config(asset="XRP", **overrides):
    values = {
        "asset": asset,
        "trading_pair": f"{asset}-USDC",
        "reference_trading_pair": f"{asset}-USDT",
    }
    values.update(overrides)
    return controller.DeriveBinanceAdaptiveMMConfig(**values)


class Row:
    def __init__(self, price, amount="100"):
        self.price = Decimal(price)
        self.amount = Decimal(amount)


class Book:
    def __init__(self, bid, ask):
        self.bid = Row(bid)
        self.ask = Row(ask)
        self.last_diff_uid = 1

    def bid_entries(self):
        return iter([self.bid])

    def ask_entries(self):
        return iter([self.ask])


class Provider:
    def __init__(self):
        self.now = 100.0
        self.books = {
            "derive_perpetual": Book("0.5000", "0.5010"),
            "binance_perpetual": Book("0.4995", "0.5005"),
        }

    def time(self):
        return self.now

    def get_order_book(self, connector, pair):
        return self.books[connector]

    def get_connector(self, connector):
        return types.SimpleNamespace(account_positions={})

    def get_trading_rules(self, connector, pair):
        return types.SimpleNamespace(
            min_price_increment=Decimal("0.0001"),
            min_order_size=Decimal("1"),
            min_notional_size=Decimal("0"),
            min_order_value=Decimal("0"),
        )

    def quantize_order_price(self, connector, pair, price):
        return price.quantize(Decimal("0.0001"))

    def quantize_order_amount(self, connector, pair, amount):
        return amount.quantize(Decimal("1"))


def native_controller(provider=None, **overrides):
    provider = provider or Provider()
    overrides.setdefault("binance_recovery_seconds", 0)
    cfg = config(**overrides)
    return controller.DeriveBinanceAdaptiveMM(cfg, provider, asyncio.Queue()), provider


def test_xrp_and_link_are_the_only_valid_configs():
    assert config("XRP").asset == "XRP"
    assert config("LINK").asset == "LINK"
    with pytest.raises(ValueError, match="XRP or LINK"):
        config("SOL")


def test_execution_and_reference_venues_are_immutable():
    with pytest.raises(ValueError, match="derive_perpetual"):
        config(connector_name="binance_perpetual")
    with pytest.raises(ValueError, match="binance_perpetual"):
        config(reference_connector_name="bybit_perpetual")
    markets = config().update_markets(MarketDict())
    assert markets.values == {"derive_perpetual": {"XRP-USDC"}, "binance_perpetual": {"XRP-USDT"}}


def test_shadow_and_arming_are_separate_and_fail_closed():
    assert config().shadow_mode is True
    assert config().mainnet_armed is False
    with pytest.raises(ValueError, match="shadow_mode=false"):
        config(mainnet_armed=True)


def test_shadow_computes_quotes_but_emits_zero_actions():
    instance, _ = native_controller()
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["operational_state"] == "SHADOW"
    assert instance.processed_data["desired_bid"] is not None
    assert instance.processed_data["desired_ask"] is not None
    assert instance.determine_executor_actions() == []


def test_binance_and_derive_staleness_pause_independently():
    instance, provider = native_controller(binance_stale_seconds=1, derive_stale_seconds=1)
    asyncio.run(instance.update_processed_data())
    provider.now += 2
    provider.books["derive_perpetual"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["operational_state"] == "REFERENCE_PAUSED"
    assert instance.processed_data["block_reason"] == "BINANCE_STALE"
    assert instance.processed_data["desired_bid"] is None
    assert instance.processed_data["desired_ask"] is None

    instance, provider = native_controller(binance_stale_seconds=1, derive_stale_seconds=1)
    asyncio.run(instance.update_processed_data())
    provider.now += 2
    provider.books["binance_perpetual"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["operational_state"] == "DERIVE_PAUSED"
    assert instance.processed_data["block_reason"] == "DERIVE_STALE"


def test_binance_recovery_requires_continuous_healthy_period():
    instance, provider = native_controller(binance_recovery_seconds=3, binance_stale_seconds=1, derive_stale_seconds=1)
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["block_reason"] == "BINANCE_RECOVERY"
    provider.now += 3
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["operational_state"] == "SHADOW"


def test_armed_create_actions_are_derive_only_and_one_per_side():
    instance, _ = native_controller(shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert len(actions) == 2
    assert {action.executor_config.level_id for action in actions} == {"bid", "ask"}
    assert {action.executor_config.connector_name for action in actions} == {"derive_perpetual"}
    assert {action.executor_config.execution_strategy for action in actions} == {ExecutionStrategy.LIMIT_MAKER}


def test_kill_switch_cancel_bypasses_exhausted_churn_budget():
    instance, provider = native_controller(shadow_mode=False, mainnet_armed=True, manual_kill_switch=True)
    instance.executors_info = [
        types.SimpleNamespace(
            id="bid-1",
            is_active=True,
            timestamp=1,
            config=types.SimpleNamespace(level_id="bid", price=Decimal("0.49"), amount=Decimal("10")),
        )
    ]
    instance._action_timestamps.extend([provider.now] * instance.config.max_quote_mutations_per_minute)
    actions = instance.determine_executor_actions()
    assert len(actions) == 1
    assert isinstance(actions[0], StopExecutorAction)


def test_duplicate_or_unknown_levels_are_cancelled_before_new_creates():
    instance, _ = native_controller(shadow_mode=False, mainnet_armed=True)
    instance.executors_info = [
        types.SimpleNamespace(
            id=executor_id,
            is_active=True,
            timestamp=1,
            config=types.SimpleNamespace(level_id=level, price=Decimal("0.49"), amount=Decimal("10")),
        )
        for executor_id, level in (("bid-1", "bid"), ("bid-2", "bid"), ("rogue-1", "grid_0"))
    ]
    actions = instance.determine_executor_actions()
    assert {action.executor_id for action in actions} == {"bid-2", "rogue-1"}
    assert all(isinstance(action, StopExecutorAction) for action in actions)


def test_native_fill_markouts_are_unavailable_until_horizon_then_measured():
    instance, provider = native_controller(binance_stale_seconds=120, derive_stale_seconds=120)
    instance.executors_info = [
        types.SimpleNamespace(
            id="fill-1",
            is_active=False,
            is_done=True,
            filled_amount_quote=Decimal("10"),
            net_pnl_quote=Decimal("0"),
            timestamp=provider.now,
            config=types.SimpleNamespace(level_id="bid", side=TradeType.BUY, price=Decimal("0.5"), amount=Decimal("20")),
            custom_info={
                "executed_amount_base": Decimal("20"),
                "average_executed_price": Decimal("0.5"),
                "order_last_update": provider.now,
            },
        )
    ]
    asyncio.run(instance.update_processed_data())
    assert instance.get_custom_info()["markout_30s_bps"] is None
    provider.now += 30
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual"].last_diff_uid += 1
    provider.books["derive_perpetual"].bid.price = Decimal("0.51")
    provider.books["derive_perpetual"].ask.price = Decimal("0.511")
    asyncio.run(instance.update_processed_data())
    assert instance.get_custom_info()["markout_30s_bps"] > 0
    assert instance.get_custom_info()["markout_60s_bps"] is None


@pytest.mark.parametrize(
    ("direction", "volatility", "spread", "expected"),
    [
        ("0", "1", "2", "NORMAL"),
        ("3", "1", "2", "UP_TREND"),
        ("-3", "1", "2", "DOWN_TREND"),
        ("0", "9", "2", "HIGH_VOL"),
        ("0", "21", "2", "EXTREME"),
    ],
)
def test_market_state_and_mode_map(direction, volatility, spread, expected):
    state = controller.classify_market_state(
        Decimal(direction),
        Decimal(volatility),
        Decimal(spread),
        trend_threshold_bps=Decimal("2"),
        high_volatility_bps=Decimal("8"),
        extreme_volatility_bps=Decimal("20"),
        extreme_spread_bps=Decimal("80"),
    )
    assert state.value == expected
    assert controller.STATE_TO_MODE[state].value == {
        "NORMAL": "NEUTRAL",
        "UP_TREND": "LONG_BIAS",
        "DOWN_TREND": "SHORT_BIAS",
        "HIGH_VOL": "DEFENSIVE",
        "EXTREME": "PAUSED",
    }[expected]


def test_state_hysteresis_and_extreme_bypass():
    current = controller.MarketState.NORMAL
    state, since = controller.apply_state_hysteresis(current, controller.MarketState.HIGH_VOL, None, 10, 5)
    assert state == current and since == 10
    state, since = controller.apply_state_hysteresis(current, controller.MarketState.HIGH_VOL, since, 14, 5)
    assert state == current
    state, _ = controller.apply_state_hysteresis(current, controller.MarketState.HIGH_VOL, since, 15, 5)
    assert state == controller.MarketState.HIGH_VOL
    state, _ = controller.apply_state_hysteresis(current, controller.MarketState.EXTREME, None, 10, 99)
    assert state == controller.MarketState.EXTREME


def test_inventory_override_modes():
    classify = controller.classify_inventory
    cap = Decimal("100")
    assert classify(Decimal("0"), cap, Decimal("0.7")).value == "FLAT"
    assert classify(Decimal("10"), cap, Decimal("0.7")).value == "LONG_SKEW"
    assert classify(Decimal("-10"), cap, Decimal("0.7")).value == "SHORT_SKEW"
    assert classify(Decimal("70"), cap, Decimal("0.7")).value == "ASK_ONLY"
    assert classify(Decimal("-70"), cap, Decimal("0.7")).value == "BID_ONLY"


def test_basis_fair_value_is_causal_and_deterministic():
    assert controller.calculate_fair_value(Decimal("100"), Decimal("10"), Decimal("0.5")) == Decimal("100.10500")


def test_refresh_deadband_residency_tick_hold_and_adverse_override():
    base = dict(
        side=TradeType.BUY,
        current_price=Decimal("100"),
        tick_size=Decimal("0.01"),
        minimum_residency_seconds=Decimal("10"),
        deadband_bps=Decimal("3"),
        fast_adverse=False,
    )
    assert controller.should_refresh(desired_price=Decimal("100.00"), age_seconds=Decimal("20"), **base)[1] == "TICK_AWARE_HOLD"
    assert controller.should_refresh(desired_price=Decimal("100.10"), age_seconds=Decimal("5"), **base)[1] == "MINIMUM_RESIDENCY"
    assert controller.should_refresh(desired_price=Decimal("100.02"), age_seconds=Decimal("20"), **base)[1] == "DEADBAND"
    assert controller.should_refresh(desired_price=Decimal("100.10"), age_seconds=Decimal("20"), **base)[0] is True
    assert controller.should_refresh(desired_price=Decimal("100.00"), age_seconds=Decimal("1"), **(base | {"fast_adverse": True}))[1] == "FAST_ADVERSE_MOVE"


def test_shared_800_capital_defaults_cover_two_assets():
    xrp, link = config("XRP"), config("LINK")
    assert xrp.portfolio_capital_quote == link.portfolio_capital_quote == Decimal("800")
    assert xrp.total_amount_quote == link.total_amount_quote == Decimal("800")
    assert xrp.asset_cap_quote + link.asset_cap_quote <= xrp.portfolio_capital_quote - xrp.reserve_quote
    assert xrp.order_amount_quote * 2 <= xrp.max_asset_open_order_quote


def test_shared_portfolio_rejects_inconsistent_terms_and_excess_asset_caps():
    portfolio_id = "test_inconsistent_terms"
    native_controller(portfolio_id=portfolio_id, asset_cap_quote=Decimal("300"))
    with pytest.raises(ValueError, match="identical capital and reserve"):
        native_controller(
            portfolio_id=portfolio_id,
            portfolio_capital_quote=Decimal("900"),
            total_amount_quote=Decimal("900"),
        )

    portfolio_id = "test_excess_caps"
    native_controller(portfolio_id=portfolio_id, asset_cap_quote=Decimal("350"))
    provider = Provider()
    link_config = config(
        "LINK",
        portfolio_id=portfolio_id,
        asset_cap_quote=Decimal("300"),
        max_asset_inventory_quote=Decimal("180"),
    )
    with pytest.raises(ValueError, match="shared asset caps"):
        controller.DeriveBinanceAdaptiveMM(link_config, provider, asyncio.Queue())


def test_committed_xrp_and_link_configs_are_shadow_only_and_share_one_portfolio():
    config_dir = Path(__file__).parents[1] / "configs"
    rows = [yaml.safe_load((config_dir / f"derive_binance_adaptive_mm_{asset}.yml").read_text()) for asset in ("xrp", "link")]
    assert {row["asset"] for row in rows} == {"XRP", "LINK"}
    assert {row["portfolio_id"] for row in rows} == {"derive_xrp_link_800"}
    assert all(row["shadow_mode"] is True and row["mainnet_armed"] is False for row in rows)
    assert all(row["connector_name"] == "derive_perpetual" for row in rows)
    assert all(row["reference_connector_name"] == "binance_perpetual" for row in rows)
    assert sum(row["order_amount_quote"] * 2 for row in rows) <= rows[0]["max_total_open_order_quote"]


def test_native_minimum_size_blocks_quote_instead_of_submitting_invalid_order():
    instance, provider = native_controller()
    provider.get_trading_rules = lambda connector, pair: types.SimpleNamespace(
        min_price_increment=Decimal("0.0001"),
        min_order_size=Decimal("1000000"),
        min_notional_size=Decimal("0"),
        min_order_value=Decimal("0"),
    )
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["block_reason"] == "ORDER_BELOW_NATIVE_MINIMUM"
    assert instance.processed_data["desired_bid"] is None
    assert instance.processed_data["desired_ask"] is None


def test_source_contains_no_binance_executor_target_and_one_bid_one_ask():
    source = CONTROLLER_PATH.read_text()
    assert 'connector_name="derive_perpetual"' in source
    assert 'connector_name="binance_perpetual"' not in source
    assert '"bid": (TradeType.BUY' in source
    assert '"ask": (TradeType.SELL' in source
    assert "grid" not in source.lower()
