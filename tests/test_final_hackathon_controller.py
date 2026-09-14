from __future__ import annotations

import asyncio
import importlib.util
import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor
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
    CLOSE = "CLOSE"


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
        self.update_id = 1


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
        self.positions = {}
        self.min_order_size = Decimal("1")
        derive_book = Book("0.5000", "0.5010")
        self.books = {
            "derive_perpetual": derive_book,
            "derive_perpetual_paper_trade": derive_book,
            "binance_perpetual_paper_trade": Book("0.4995", "0.5005"),
        }

    def time(self):
        return self.now

    def get_order_book(self, connector, pair):
        return self.books[connector]

    def get_connector(self, connector):
        return types.SimpleNamespace(account_positions=self.positions)

    def get_trading_rules(self, connector, pair):
        return types.SimpleNamespace(
            min_price_increment=Decimal("0.0001"),
            min_order_size=self.min_order_size,
            min_notional_size=Decimal("0"),
            min_order_value=Decimal("0"),
        )

    def quantize_order_price(self, connector, pair, price):
        return price.quantize(Decimal("0.0001"))

    def quantize_order_amount(self, connector, pair, amount):
        return amount.quantize(Decimal("1"))


def native_controller(provider=None, *, seed_peer=True, **overrides):
    provider = provider or Provider()
    overrides.setdefault("binance_recovery_seconds", 0)
    cfg = config(**overrides)
    instance = controller.DeriveBinanceAdaptiveMM(cfg, provider, asyncio.Queue())
    if seed_peer:
        peer = "LINK" if cfg.asset == "XRP" else "XRP"
        instance._portfolio.setdefault(cfg.portfolio_id, {})[peer] = controller.PortfolioSnapshot(
            position_amount=Decimal("0"),
            mark_price=Decimal("1"),
            updated_at=provider.now,
        )
    return instance, provider


@pytest.fixture(autouse=True)
def reset_shared_portfolio_state():
    controller.DeriveBinanceAdaptiveMM._reset_shared_state_for_tests()


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
    assert markets.values == {
        "derive_perpetual_paper_trade": {"XRP-USDC"},
        "binance_perpetual_paper_trade": {"XRP-USDT"},
    }
    assert config().reference_connector_name == "binance_perpetual"
    assert config().reference_market_connector_name == "binance_perpetual_paper_trade"
    assert config().execution_market_connector_name == "derive_perpetual_paper_trade"
    assert config(shadow_mode=False).execution_market_connector_name == "derive_perpetual"


def test_shadow_serialization_suppresses_live_account_initialization():
    shadow = config()
    live = config(shadow_mode=False)

    assert shadow.position_mode == PositionMode.ONEWAY
    assert shadow.leverage == 1
    assert "position_mode" not in shadow.model_dump()
    assert "leverage" not in shadow.model_dump()
    assert live.model_dump()["position_mode"] == PositionMode.ONEWAY
    assert live.model_dump()["leverage"] == 1


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
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["operational_state"] == "DERIVE_PAUSED"
    assert instance.processed_data["block_reason"] == "DERIVE_STALE"
    assert instance.processed_data["derive_bbo"] == [Decimal("0.5000"), Decimal("0.5010")]
    assert instance.processed_data["binance_bbo"] == [Decimal("0.4995"), Decimal("0.5005")]
    assert instance.processed_data["derive_age_seconds"] == Decimal("2.0")
    assert instance.processed_data["binance_age_seconds"] == Decimal("0.0")


def test_paper_wrapper_bbo_change_resets_freshness_when_book_uid_is_static():
    instance, provider = native_controller(binance_stale_seconds=1, derive_stale_seconds=1)
    asyncio.run(instance.update_processed_data())
    provider.now += 2
    provider.books["derive_perpetual"].bid.price = Decimal("0.5001")
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1

    asyncio.run(instance.update_processed_data())

    assert instance.processed_data["operational_state"] == "SHADOW"
    assert instance.processed_data["derive_age_seconds"] == Decimal("0.0")


def test_binance_recovery_requires_continuous_healthy_period():
    instance, provider = native_controller(binance_recovery_seconds=3, binance_stale_seconds=1, derive_stale_seconds=1)
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["block_reason"] == "BINANCE_RECOVERY"
    provider.now += 3
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["operational_state"] == "SHADOW"


def test_fixed_time_volatility_samples_fresh_unchanged_mid_as_zero_return():
    instance, provider = native_controller(binance_stale_seconds=3, derive_stale_seconds=3)
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["volatility_sample_count"] == 0

    provider.now += 0.5
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["volatility_sample_count"] == 0

    provider.now += 0.5
    asyncio.run(instance.update_processed_data())
    assert list(instance._returns) == [0.0]
    assert instance.processed_data["volatility_sampling_state"] == "FIXED_1S"

    provider.now += 1
    provider.books["binance_perpetual_paper_trade"].bid.price += Decimal("0.001")
    provider.books["binance_perpetual_paper_trade"].ask.price += Decimal("0.001")
    asyncio.run(instance.update_processed_data())
    assert len(instance._returns) == 2
    assert instance._returns[-1] > 0
    assert instance.processed_data["volatility_bps"] > 0


def test_fixed_time_volatility_does_not_fill_missed_one_second_buckets():
    instance, provider = native_controller(binance_stale_seconds=3, derive_stale_seconds=3)
    asyncio.run(instance.update_processed_data())

    provider.now += 2
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert list(instance._returns) == []

    provider.now += 1
    asyncio.run(instance.update_processed_data())
    assert list(instance._returns) == [0.0]


def test_stale_binance_pauses_sampling_without_synthesizing_gap_returns():
    instance, provider = native_controller(binance_stale_seconds=2, derive_stale_seconds=2)
    asyncio.run(instance.update_processed_data())
    provider.now += 1
    asyncio.run(instance.update_processed_data())
    assert len(instance._returns) == 1

    provider.now += 3
    provider.books["derive_perpetual"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["operational_state"] == "REFERENCE_PAUSED"
    assert instance.processed_data["volatility_sampling_state"] == "PAUSED_STALE"
    assert len(instance._returns) == 1

    provider.now += 1
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert len(instance._returns) == 1

    provider.now += 1
    asyncio.run(instance.update_processed_data())
    assert len(instance._returns) == 2
    assert instance._returns[-1] == 0.0


def test_armed_create_actions_are_derive_only_and_one_per_side():
    instance, _ = native_controller(shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert len(actions) == 2
    assert {action.executor_config.level_id for action in actions} == {"bid", "ask"}
    assert {action.executor_config.connector_name for action in actions} == {"derive_perpetual"}
    assert {action.executor_config.execution_strategy for action in actions} == {ExecutionStrategy.LIMIT_MAKER}
    assert {action.executor_config.position_action for action in actions} == {PositionAction.OPEN}


def test_missing_peer_risk_snapshot_blocks_new_creates():
    instance, _ = native_controller(seed_peer=False, shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())

    assert instance.determine_executor_actions() == []
    assert instance.processed_data["peer_risk_state"] == "MISSING"
    assert instance._last_action == {"bid": "PEER_RISK_MISSING", "ask": "PEER_RISK_MISSING"}


def test_stale_peer_risk_snapshot_blocks_then_recovers():
    instance, provider = native_controller(
        shadow_mode=False,
        mainnet_armed=True,
        peer_stale_seconds=2,
    )
    asyncio.run(instance.update_processed_data())
    peer = "LINK"
    instance._portfolio[instance.config.portfolio_id][peer] = controller.PortfolioSnapshot(
        position_amount=Decimal("0"),
        mark_price=Decimal("1"),
        updated_at=provider.now - 3,
    )

    assert instance.determine_executor_actions() == []
    assert instance.processed_data["peer_risk_state"] == "STALE"

    instance._portfolio[instance.config.portfolio_id][peer] = controller.PortfolioSnapshot(
        position_amount=Decimal("0"),
        mark_price=Decimal("1"),
        updated_at=provider.now,
    )
    assert len(instance.determine_executor_actions()) == 2
    assert instance.processed_data["peer_risk_state"] == "HEALTHY"


def test_shadow_controllers_publish_healthy_peer_risk_diagnostics():
    xrp, _ = native_controller(Provider(), seed_peer=False, id="xrp", asset="XRP")
    link, _ = native_controller(Provider(), seed_peer=False, id="link", asset="LINK")

    asyncio.run(xrp.update_processed_data())
    assert xrp.processed_data["peer_risk_state"] == "MISSING"
    asyncio.run(link.update_processed_data())
    asyncio.run(xrp.update_processed_data())

    assert xrp.processed_data["peer_risk_healthy"] is True
    assert link.processed_data["peer_risk_healthy"] is True
    assert xrp.processed_data["peer_risk_state"] == "HEALTHY"
    assert link.processed_data["peer_risk_state"] == "HEALTHY"


def test_safety_cancel_still_works_when_peer_risk_snapshot_is_missing():
    instance, provider = native_controller(
        seed_peer=False,
        shadow_mode=False,
        mainnet_armed=True,
        manual_kill_switch=True,
    )
    instance.executors_info = [
        types.SimpleNamespace(
            id="bid-1",
            is_active=True,
            timestamp=provider.now,
            config=types.SimpleNamespace(level_id="bid", price=Decimal("0.49"), amount=Decimal("10")),
        )
    ]

    actions = instance.determine_executor_actions()
    assert len(actions) == 1
    assert isinstance(actions[0], StopExecutorAction)
    assert actions[0].executor_id == "bid-1"


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
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
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


@pytest.mark.parametrize(
    ("current", "side", "amount", "projected", "effect"),
    [
        ("0", TradeType.BUY, "10", "10", "INCREASE_SAME_DIRECTION"),
        ("0", TradeType.SELL, "10", "-10", "INCREASE_SAME_DIRECTION"),
        ("10", TradeType.BUY, "4", "14", "INCREASE_SAME_DIRECTION"),
        ("10", TradeType.SELL, "4", "6", "REDUCE"),
        ("10", TradeType.SELL, "20", "-10", "FLIP_DIRECTION"),
        ("-10", TradeType.SELL, "4", "-14", "INCREASE_SAME_DIRECTION"),
        ("-10", TradeType.BUY, "4", "-6", "REDUCE"),
        ("-10", TradeType.BUY, "20", "10", "FLIP_DIRECTION"),
        ("10", TradeType.SELL, "10", "0", "FLATTEN"),
    ],
)
def test_signed_projected_position_and_fill_classification(current, side, amount, projected, effect):
    result, classification = controller.projected_position(Decimal(current), side, Decimal(amount))
    assert result == Decimal(projected)
    assert classification.value == effect


def _set_position(provider, pair, amount):
    provider.positions = {
        pair: types.SimpleNamespace(trading_pair=pair, amount=Decimal(str(amount)), unrealized_pnl=Decimal("0"))
    }


def test_inventory_increasing_bid_is_resized_to_asset_capacity():
    provider = Provider()
    mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "LINK-USDC", Decimal("120") / mid)
    instance, _ = native_controller(
        provider,
        asset="LINK",
        order_amount_quote=Decimal("125"),
        max_asset_open_order_quote=Decimal("260"),
        shadow_mode=False,
        mainnet_armed=True,
    )
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    bid = next(action for action in actions if action.executor_config.level_id == "bid")
    assert bid.executor_config.amount < instance._plan.amount
    assert instance.processed_data["projected_position_notional_if_bid_fills"] <= Decimal("180")


def test_inventory_residual_below_native_minimum_blocks_side():
    provider = Provider()
    provider.min_order_size = Decimal("10")
    mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", Decimal("179") / mid)
    instance, _ = native_controller(
        provider, one_sided_inventory_ratio=Decimal("1"), shadow_mode=False, mainnet_armed=True
    )
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert all(action.executor_config.level_id != "bid" for action in actions)
    assert instance.processed_data["bid_size_block_reason"] == "PROJECTED_ASSET_INVENTORY_LIMIT"


@pytest.mark.parametrize(
    ("position", "level"),
    [(Decimal("12"), "ask"), (Decimal("-12"), "bid")],
)
def test_default_reducing_quote_flattens_but_does_not_flip(position, level):
    provider = Provider()
    _set_position(provider, "XRP-USDC", position)
    instance, _ = native_controller(provider, shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    action = next(item for item in actions if item.executor_config.level_id == level)
    assert action.executor_config.amount == Decimal("12")
    assert action.executor_config.position_action == PositionAction.CLOSE
    assert instance.processed_data[f"{level}_fill_effect"] == "FLATTEN"
    assert instance.processed_data[f"projected_position_amount_if_{level}_fills"] == Decimal("0")
    assert instance.config.allow_position_flips is False


@pytest.mark.parametrize(
    ("position", "level"),
    [(Decimal("100"), "ask"), (Decimal("-100"), "bid")],
)
def test_long_and_short_reductions_use_close(position, level):
    provider = Provider()
    _set_position(provider, "XRP-USDC", position)
    instance, _ = native_controller(provider, shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())

    action = next(item for item in instance.determine_executor_actions() if item.executor_config.level_id == level)
    assert action.executor_config.position_action == PositionAction.CLOSE
    assert instance.processed_data[f"{level}_fill_effect"] == "REDUCE"


@pytest.mark.parametrize(
    ("position", "level"),
    [(Decimal("12"), "bid"), (Decimal("-12"), "ask")],
)
def test_same_direction_increases_use_open(position, level):
    provider = Provider()
    _set_position(provider, "XRP-USDC", position)
    instance, _ = native_controller(provider, shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())

    action = next(item for item in instance.determine_executor_actions() if item.executor_config.level_id == level)
    assert action.executor_config.position_action == PositionAction.OPEN
    assert instance.processed_data[f"{level}_fill_effect"] == "INCREASE_SAME_DIRECTION"


def test_explicit_normal_state_flip_remains_projected_and_capped():
    provider = Provider()
    _set_position(provider, "XRP-USDC", Decimal("12"))
    instance, _ = native_controller(
        provider, allow_position_flips=True, shadow_mode=False, mainnet_armed=True
    )
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    ask = next(action for action in actions if action.executor_config.level_id == "ask")
    assert ask.executor_config.amount > Decimal("12")
    assert ask.executor_config.position_action == PositionAction.OPEN
    assert instance.processed_data["ask_fill_effect"] == "FLIP_DIRECTION"
    assert abs(instance.processed_data["projected_position_notional_if_ask_fills"]) <= Decimal("180")


def test_asset_cap_includes_position_active_opposite_order_and_new_quote():
    provider = Provider()
    mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", Decimal("120") / mid)
    instance, _ = native_controller(
        provider,
        max_asset_open_order_quote=Decimal("260"),
        shadow_mode=False,
        mainnet_armed=True,
    )
    asyncio.run(instance.update_processed_data())
    instance.executors_info = [
        types.SimpleNamespace(
            id="ask-active",
            is_active=True,
            timestamp=provider.now,
            config=types.SimpleNamespace(
                level_id="ask", price=instance._plan.ask_price, amount=Decimal("160") / instance._plan.ask_price
            ),
        )
    ]
    actions = instance.determine_executor_actions()
    bid = next(action for action in actions if action.executor_config.level_id == "bid")
    assert bid.executor_config.amount * bid.executor_config.price <= Decimal("20")


def test_shared_controllers_reserve_open_order_cap_atomically():
    xrp, _ = native_controller(
        Provider(), id="xrp", asset="XRP", max_total_open_order_quote=Decimal("60"), shadow_mode=False, mainnet_armed=True
    )
    link, _ = native_controller(
        Provider(),
        id="link",
        asset="LINK",
        order_amount_quote=Decimal("25"),
        max_total_open_order_quote=Decimal("60"),
        shadow_mode=False,
        mainnet_armed=True,
    )
    asyncio.run(xrp.update_processed_data())
    asyncio.run(link.update_processed_data())
    barrier = threading.Barrier(2)

    def decide(instance):
        barrier.wait()
        return instance.determine_executor_actions()

    with ThreadPoolExecutor(max_workers=2) as pool:
        batches = list(pool.map(decide, (xrp, link)))
    actions = [action for batch in batches for action in batch]
    reserved = sum(action.executor_config.amount * action.executor_config.price for action in actions)
    assert reserved <= Decimal("60")
    assert len(actions) >= 2


def test_shared_reservations_cannot_exceed_capital_minus_reserve():
    xrp_provider, link_provider = Provider(), Provider()
    mid = (xrp_provider.books["derive_perpetual"].bid.price + xrp_provider.books["derive_perpetual"].ask.price) / 2
    _set_position(xrp_provider, "XRP-USDC", Decimal("280") / mid)
    _set_position(link_provider, "LINK-USDC", Decimal("280") / mid)
    common = {
        "asset_cap_quote": Decimal("300"),
        "max_asset_inventory_quote": Decimal("300"),
        "max_asset_open_order_quote": Decimal("260"),
        "max_total_inventory_quote": Decimal("600"),
        "max_total_open_order_quote": Decimal("700"),
        "shadow_mode": False,
        "mainnet_armed": True,
    }
    xrp, _ = native_controller(xrp_provider, id="xrp", asset="XRP", **common)
    link, _ = native_controller(link_provider, id="link", asset="LINK", **common)
    asyncio.run(xrp.update_processed_data())
    asyncio.run(link.update_processed_data())
    actions = xrp.determine_executor_actions() + link.determine_executor_actions()
    reserved = sum(action.executor_config.amount * action.executor_config.price for action in actions)
    assert Decimal("560") + reserved <= Decimal("600")


def test_pending_create_reservation_prevents_duplicate_create():
    instance, _ = native_controller(Provider(), id="xrp", shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    assert len(instance.determine_executor_actions()) == 2
    assert instance.determine_executor_actions() == []
    assert instance._last_action == {
        "bid": "PENDING_CREATE_RESERVATION",
        "ask": "PENDING_CREATE_RESERVATION",
    }
    instance.market_data_provider.now += instance._reservation_ttl_seconds + 1
    instance.market_data_provider.books["derive_perpetual"].last_diff_uid += 1
    instance.market_data_provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    instance._portfolio[instance.config.portfolio_id]["LINK"] = controller.PortfolioSnapshot(
        position_amount=Decimal("0"),
        mark_price=Decimal("1"),
        updated_at=instance.market_data_provider.now,
    )
    asyncio.run(instance.update_processed_data())
    assert len(instance.determine_executor_actions()) == 2


def test_peer_cannot_expire_a_stalled_controllers_reservation():
    xrp_provider, link_provider = Provider(), Provider()
    xrp, _ = native_controller(xrp_provider, id="xrp", asset="XRP", shadow_mode=False, mainnet_armed=True)
    link, _ = native_controller(link_provider, id="link", asset="LINK", shadow_mode=False, mainnet_armed=True)
    asyncio.run(xrp.update_processed_data())
    asyncio.run(link.update_processed_data())
    xrp.determine_executor_actions()
    link_provider.now += xrp._reservation_ttl_seconds + 1
    link.determine_executor_actions()
    reservations = link._reservations[link.config.portfolio_id]
    assert (xrp.config.id, "bid") in reservations
    assert (xrp.config.id, "ask") in reservations


def test_projected_portfolio_inventory_limit_resizes_second_asset():
    xrp_provider = Provider()
    mid = (xrp_provider.books["derive_perpetual"].bid.price + xrp_provider.books["derive_perpetual"].ask.price) / 2
    _set_position(xrp_provider, "XRP-USDC", Decimal("150") / mid)
    xrp, _ = native_controller(
        xrp_provider,
        asset="XRP",
        max_total_inventory_quote=Decimal("200"),
        shadow_mode=False,
        mainnet_armed=True,
    )
    link, _ = native_controller(
        Provider(),
        asset="LINK",
        order_amount_quote=Decimal("125"),
        max_asset_open_order_quote=Decimal("260"),
        max_total_inventory_quote=Decimal("200"),
        shadow_mode=False,
        mainnet_armed=True,
    )
    asyncio.run(xrp.update_processed_data())
    asyncio.run(link.update_processed_data())
    actions = link.determine_executor_actions()
    bid = next(action for action in actions if action.executor_config.level_id == "bid")
    assert bid.executor_config.amount * bid.executor_config.price <= Decimal("50")


def test_portfolio_projection_uses_conservative_active_and_pending_prices():
    instance, _ = native_controller(Provider(), id="xrp", asset="XRP")
    instance._portfolio[instance.config.portfolio_id] = {
        "XRP": controller.PortfolioSnapshot(position_amount=Decimal("0"), mark_price=Decimal("1")),
        "LINK": controller.PortfolioSnapshot(
            position_amount=Decimal("0"),
            mark_price=Decimal("1"),
            open_bid_amount=Decimal("100"),
            open_bid_price=Decimal("2"),
        ),
    }
    assert instance._portfolio_projected_gross(TradeType.BUY, Decimal("0"), Decimal("1")) == Decimal("200")


def test_projected_notional_diagnostic_uses_conservative_derive_mid():
    instance, provider = native_controller()
    asyncio.run(instance.update_processed_data())
    derive_mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    assert instance._plan.bid_price < derive_mid
    assert instance.processed_data["projected_position_notional_if_bid_fills"] == (
        instance.processed_data["risk_adjusted_bid_order_amount"] * derive_mid
    )


def test_snapshot_shim_guard_recognizes_only_audited_shape(monkeypatch):
    class Affected:
        def __init__(self):
            self._snapshot_messages = {}
            self._snapshot_messages_queue_key = "order_book_snapshot"

        async def _request_order_book_snapshot(self):
            cached = self._snapshot_messages
            return await message_queue.get(), cached  # noqa: F821

    class Unaffected:
        def __init__(self):
            self._snapshot_messages = {}
            self._snapshot_messages_queue_key = "order_book_snapshot"

        async def _request_order_book_snapshot(self):
            return self._snapshot_messages

    class Unknown:
        pass

    monkeypatch.setattr(controller, "_is_hummingbot_2160", lambda _: True)
    assert controller._snapshot_shape_is_affected(Affected) is True
    assert controller._snapshot_shape_is_affected(Unaffected) is False
    assert controller._snapshot_shape_is_affected(Unknown) is False
    monkeypatch.setattr(controller, "_is_hummingbot_2160", lambda _: False)
    assert controller._snapshot_shape_is_affected(Affected) is False


def test_native_message_freshness_survives_unchanged_bbo(monkeypatch):
    provider = Provider()
    provider.now = 100
    metrics = types.SimpleNamespace(
        per_pair_metrics={
            "XRP-USDC": types.SimpleNamespace(last_diff_timestamp=99.5, last_snapshot_timestamp=0),
            "XRP-USDT": types.SimpleNamespace(last_diff_timestamp=99.5, last_snapshot_timestamp=0),
        }
    )
    provider.get_connector = lambda _: types.SimpleNamespace(
        account_positions={}, order_book_tracker=types.SimpleNamespace(metrics=metrics)
    )
    monkeypatch.setattr(controller.time, "perf_counter", lambda: provider.now)
    instance, _ = native_controller(provider, binance_stale_seconds=1, derive_stale_seconds=1)
    asyncio.run(instance.update_processed_data())
    provider.now = 100.5
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["derive_freshness_source"] == "NATIVE_MESSAGE_TIMESTAMP"
    assert instance.processed_data["derive_feed_age_seconds"] == Decimal("1.0")
    assert instance.processed_data["derive_bbo_change_age_seconds"] == Decimal("0.5")


def test_fallback_freshness_is_explicitly_labeled():
    instance, _ = native_controller()
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["derive_freshness_source"] == "BBO_CHANGE_FALLBACK"
    assert instance.processed_data["binance_freshness_source"] == "BBO_CHANGE_FALLBACK"


def test_account_equity_and_strategy_executor_pnl_are_separate():
    class AccountConnector:
        def __init__(self):
            self.equity = Decimal("800")
            self.account_positions = {
                "xrp": types.SimpleNamespace(
                    trading_pair="XRP-USDC", amount=Decimal("10"), unrealized_pnl=Decimal("3")
                )
            }

        def get_balance(self, asset):
            return self.equity

        def get_available_balance(self, asset):
            return self.equity - Decimal("20")

        def get_mid_price(self, pair):
            return Decimal("0.5005")

    provider = Provider()
    account = AccountConnector()
    provider.get_connector = lambda _: account
    instance, _ = native_controller(provider, shadow_mode=False)
    asyncio.run(instance.update_processed_data())
    instance.executors_info = [types.SimpleNamespace(net_pnl_quote=Decimal("7"), filled_amount_quote=Decimal("0"))]
    assert instance.get_custom_info()["pnl"] == Decimal("7")
    assert instance.processed_data["account_equity"] is None
    assert instance.processed_data["account_collateral_balance"] == Decimal("800")
    assert instance.processed_data["account_unrealized_pnl"] == Decimal("3")
    assert instance.processed_data["account_realized_pnl"] is None
    account.equity = Decimal("795")
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["account_drawdown"] is None
    assert instance.processed_data["collateral_balance_drawdown"] == Decimal("5")


def test_native_account_equity_path_drives_account_drawdown_gate_when_supported():
    class EquityConnector:
        def __init__(self):
            self.account_equity = Decimal("800")
            self.account_positions = {}

        def get_balance(self, asset):
            return Decimal("780")

        def get_available_balance(self, asset):
            return Decimal("760")

    provider = Provider()
    connector = EquityConnector()
    provider.get_connector = lambda _: connector
    instance, _ = native_controller(provider, shadow_mode=False, max_account_drawdown_quote=Decimal("40"))
    asyncio.run(instance.update_processed_data())
    connector.account_equity = Decimal("750")
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["account_drawdown"] == Decimal("50")
    assert instance.processed_data["operational_state"] == "RISK_PAUSED"
    assert instance.processed_data["block_reason"] == "ACCOUNT_DRAWDOWN_LIMIT"
