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

    def update_config(self, new_config):
        """Mirror Hummingbot V2's updatable-field config reload behavior."""
        updatable = {
            name: getattr(new_config, name)
            for name, field_info in self.config.__class__.model_fields.items()
            if (field_info.json_schema_extra or {}).get("is_updatable", False)
        }
        if updatable:
            self.config = self.config.model_copy(update=updatable)


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


def native_controller(provider=None, **overrides):
    provider = provider or Provider()
    overrides.setdefault("binance_recovery_seconds", 0)
    cfg = config(**overrides)
    instance = controller.DeriveBinanceAdaptiveMM(cfg, provider, asyncio.Queue())
    return instance, provider


@pytest.fixture(autouse=True)
def reset_shared_portfolio_state():
    controller.DeriveBinanceAdaptiveMM._reset_shared_state_for_tests()


def test_xrp_is_the_only_valid_config():
    assert config("XRP").asset == "XRP"
    with pytest.raises(ValueError, match="asset must be XRP"):
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


@pytest.mark.parametrize("total_spread", ["3", "4", "5", "6", "8"])
def test_normal_spread_uses_explicit_total_bid_ask_convention(total_spread):
    total = Decimal(total_spread)
    assert controller.normal_quote_edge_bps(total) * 2 == total
    bid, ask = controller.calculate_normal_quote_prices(
        Decimal("100"), Decimal("101"), Decimal("100.5"), total, Decimal("0.01")
    )
    assert controller.calculate_total_spread_bps(bid, ask) is not None
    assert bid >= Decimal("100")
    assert ask <= Decimal("101")


def test_normal_quotes_improve_a_wide_bbo_but_never_take():
    bid, ask = controller.calculate_normal_quote_prices(
        Decimal("100"), Decimal("101"), Decimal("100.5"), Decimal("4"), Decimal("0.01")
    )
    assert bid > Decimal("100")
    assert ask < Decimal("101")
    assert bid < ask


def test_protected_market_state_overrides_tight_normal_spread():
    normal_edge = controller.effective_quote_edge_bps(
        controller.MarketState.NORMAL, Decimal("4"), Decimal("4")
    )
    high_vol_edge = controller.effective_quote_edge_bps(
        controller.MarketState.HIGH_VOL, Decimal("4"), Decimal("8")
    )
    toxic_edge = controller.effective_quote_edge_bps(
        controller.MarketState.NORMAL, Decimal("4"), Decimal("4"), True, Decimal("4")
    )
    assert normal_edge == Decimal("2")
    assert high_vol_edge == Decimal("8")
    assert toxic_edge == Decimal("4")
    assert controller.effective_quote_edge_bps(
        controller.MarketState.NORMAL, Decimal("4"), Decimal("4"), True
    ) == Decimal("2")


def test_toxicity_guard_is_temporary_and_counts_activations():
    instance, _ = native_controller(toxicity_guard_seconds=Decimal("60"))
    instance._markout_5s.append(Decimal("-6"))
    assert instance._refresh_toxicity_guard(100.0) is True
    assert instance._toxicity_guard_activations == 1
    assert instance._refresh_toxicity_guard(110.0) is True
    assert instance._toxicity_guard_activations == 1
    assert instance._refresh_toxicity_guard(161.0) is False


def test_toxicity_guard_widens_normal_quotes_without_pausing():
    instance, _ = native_controller(
        normal_total_spread_bps=Decimal("4"),
        toxicity_markout_threshold_bps=Decimal("5"),
        toxicity_widening_total_spread_bps=Decimal("4"),
    )
    asyncio.run(instance.update_processed_data())
    baseline = instance.processed_data["quote_total_spread_bps"]
    instance._markout_5s.append(Decimal("-6"))
    instance.market_data_provider.now += 1
    instance.market_data_provider.books["derive_perpetual"].last_diff_uid += 1
    instance.market_data_provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert baseline is not None
    assert instance.processed_data["toxicity_guard_active"] is True
    assert instance.processed_data["toxicity_guard_activations"] == 1
    assert instance.processed_data["quote_total_spread_bps"] > baseline


def test_tight_normal_spread_keeps_inventory_skew_and_capacity_limits():
    provider = Provider()
    mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", Decimal("120") / mid)
    instance, _ = native_controller(
        provider,
        normal_total_spread_bps=Decimal("4"),
        order_amount_quote=Decimal("125"),
        max_asset_open_order_quote=Decimal("260"),
        shadow_mode=False,
        mainnet_armed=True,
    )
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["normal_total_spread_bps"] == Decimal("4")
    assert instance.processed_data["inventory_mode"] == "LONG_SKEW"
    bid = next(action for action in instance.determine_executor_actions() if action.executor_config.level_id == "bid")
    assert bid.executor_config.amount * bid.executor_config.price <= Decimal("60")


def test_profitable_volume_efficiency_requires_positive_after_costs():
    instance, provider = native_controller()
    instance.executors_info = [
        types.SimpleNamespace(
            filled_amount_quote=Decimal("40"),
            net_pnl_quote=Decimal("0.20"),
            cum_fees_quote=Decimal("0.004"),
        )
    ]
    provider.now += 3600
    details = instance.get_custom_info()
    assert details["maker_volume_per_hour"] == Decimal("40")
    assert details["pnl_per_1000_volume"] == Decimal("5.00")
    assert details["profitable_volume_efficiency_valid"] is True
    assert details["profitable_volume_efficiency"] > Decimal("0")
    assert details["gross_spread_capture_quote"] is None
    assert details["inventory_pnl_quote"] is None


def test_missing_executor_pnl_and_fees_are_reported_as_unknown_not_zero():
    instance, provider = native_controller()
    instance.executors_info = [types.SimpleNamespace(filled_amount_quote=Decimal("40"))]
    provider.now += 60

    details = instance.get_custom_info()

    assert details["pnl"] is None
    assert details["strategy_executor_pnl"] is None
    assert details["drawdown"] is None
    assert details["maker_fees_quote"] is None
    assert details["pnl_per_1000_volume"] is None
    assert details["profitable_volume_efficiency_valid"] is False


def test_native_placeholder_pnl_and_fees_after_a_fill_are_unknown():
    instance, provider = native_controller()
    instance.executors_info = [
        types.SimpleNamespace(
            filled_amount_quote=Decimal("40"),
            net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            custom_info={},
        )
    ]
    provider.now += 60

    details = instance.get_custom_info()

    assert details["pnl"] is None
    assert details["maker_fees_quote"] is None


def test_armed_create_actions_are_derive_only_and_one_per_side():
    instance, _ = native_controller(shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert len(actions) == 2
    assert {action.executor_config.level_id for action in actions} == {"bid", "ask"}
    assert {action.executor_config.connector_name for action in actions} == {"derive_perpetual"}
    assert {action.executor_config.execution_strategy for action in actions} == {ExecutionStrategy.LIMIT_MAKER}
    assert {action.executor_config.position_action for action in actions} == {PositionAction.OPEN}


def test_xrp_can_create_without_a_peer_controller():
    instance, _ = native_controller(shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert len(actions) == 2
    assert all(isinstance(action, CreateExecutorAction) for action in actions)
    assert "peer" not in " ".join(instance.processed_data).lower()


def test_safety_cancel_still_works_without_a_peer_controller():
    instance, provider = native_controller(
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
    assert instance.get_custom_info()["markout_5s_bps"] is None
    assert instance.get_custom_info()["markout_30s_bps"] is None
    provider.now += 30
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    provider.books["derive_perpetual"].bid.price = Decimal("0.51")
    provider.books["derive_perpetual"].ask.price = Decimal("0.511")
    asyncio.run(instance.update_processed_data())
    assert instance.get_custom_info()["markout_5s_bps"] > 0
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


def test_derive_microprice_uses_derive_sizes_and_decimal_fallback():
    micro = controller.calculate_derive_microprice(
        Decimal("100"),
        Decimal("101"),
        Decimal("3"),
        Decimal("1"),
    )
    assert micro == Decimal("100.75")
    assert controller.calculate_derive_microprice(
        Decimal("100"),
        Decimal("101"),
        Decimal("0"),
        Decimal("1"),
    ) == Decimal("100.5")


def test_normal_quote_center_is_derive_anchored_not_binance_absolute():
    provider = Provider()
    provider.books["binance_perpetual_paper_trade"].bid.price = Decimal("10")
    provider.books["binance_perpetual_paper_trade"].ask.price = Decimal("10.01")
    instance, _ = native_controller(provider)
    asyncio.run(instance.update_processed_data())
    derive_mid = Decimal("0.5005")
    assert instance.processed_data["quote_center_source"] == "DERIVE_MIDPOINT"
    assert instance.processed_data["derive_fair_value"] == derive_mid
    assert instance._plan.fair_value == derive_mid
    assert instance._plan.fair_value < Decimal("1")


def test_normal_quote_center_prefers_derive_microprice_when_sizes_are_valid():
    provider = Provider()
    provider.books["derive_perpetual"].bid.amount = Decimal("3")
    provider.books["derive_perpetual"].ask.amount = Decimal("1")
    instance, _ = native_controller(provider)
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["quote_center_source"] == "DERIVE_MICROPRICE"
    assert instance.processed_data["derive_fair_value"] == Decimal("0.50075")
    assert instance._plan.fair_value == Decimal("0.50075")


def _activate_from_create_actions(instance, actions):
    instance.executors_info = [
        _executor(
            f"{action.executor_config.level_id}-active",
            action.executor_config.level_id,
            str(action.executor_config.price),
            instance.market_data_provider.now,
        )
        for action in actions
        if type(action) is controller.CreateExecutorAction
    ]


def test_binance_movement_alone_does_not_refresh_normal_derive_quotes():
    instance, provider = native_controller(shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    creates = instance.determine_executor_actions()
    _activate_from_create_actions(instance, creates)
    provider.now += 1
    provider.books["binance_perpetual_paper_trade"].bid.price += Decimal("0.00001")
    provider.books["binance_perpetual_paper_trade"].ask.price += Decimal("0.00001")
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["market_state"] == "NORMAL"
    assert instance.processed_data["derive_fair_value"] == Decimal("0.5005")
    assert instance.determine_executor_actions() == []
    assert instance.processed_data["last_actions"] == {"bid": "TICK_AWARE_HOLD", "ask": "TICK_AWARE_HOLD"}


def test_derive_fair_movement_beyond_deadband_refreshes_after_residency():
    instance, provider = native_controller(
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("0"),
    )
    asyncio.run(instance.update_processed_data())
    creates = instance.determine_executor_actions()
    _activate_from_create_actions(instance, creates)
    provider.now += 1
    provider.books["derive_perpetual"].bid.price += Decimal("0.0004")
    provider.books["derive_perpetual"].ask.price += Decimal("0.0004")
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert actions
    assert all(isinstance(action, controller.StopExecutorAction) for action in actions)
    assert instance.processed_data["last_actions"]["bid"] == "DERIVE_STALENESS_REFRESH"
    assert instance.processed_data["last_actions"]["ask"] == "DERIVE_STALENESS_REFRESH"


def test_derive_normal_refresh_honors_minimum_residency():
    instance, provider = native_controller(
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("10"),
    )
    asyncio.run(instance.update_processed_data())
    creates = instance.determine_executor_actions()
    _activate_from_create_actions(instance, creates)
    provider.now += 5
    provider.books["derive_perpetual"].bid.price += Decimal("0.0004")
    provider.books["derive_perpetual"].ask.price += Decimal("0.0004")
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    assert instance.determine_executor_actions() == []
    assert instance.processed_data["last_actions"] == {"bid": "MINIMUM_RESIDENCY", "ask": "MINIMUM_RESIDENCY"}


def test_optional_max_quote_age_has_explicit_lifecycle_reason():
    instance, provider = native_controller(
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("0"),
        max_normal_quote_age_seconds=Decimal("2"),
    )
    asyncio.run(instance.update_processed_data())
    creates = instance.determine_executor_actions()
    _activate_from_create_actions(instance, creates)
    provider.now += 2
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert actions
    assert instance.processed_data["last_actions"] == {
        "bid": "MAX_QUOTE_AGE_REFRESH",
        "ask": "MAX_QUOTE_AGE_REFRESH",
    }


def test_binance_bearish_toxicity_bypasses_residency_and_keeps_ask_safe():
    instance, provider = native_controller(
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("10"),
    )
    asyncio.run(instance.update_processed_data())
    creates = instance.determine_executor_actions()
    _activate_from_create_actions(instance, creates)
    provider.now += 1
    provider.books["binance_perpetual_paper_trade"].bid.price -= Decimal("0.003")
    provider.books["binance_perpetual_paper_trade"].ask.price -= Decimal("0.003")
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert [action.executor_id for action in actions] == ["bid-active"]
    assert instance.processed_data["last_actions"]["bid"] == (
        "BINANCE_TRUE_SHOCK_LARGE_BINANCE_MOVE_CROSS_VENUE_DISLOCATION_CANCEL_BID"
    )
    assert instance.processed_data["desired_bid"] is None
    assert instance.processed_data["desired_ask"] is not None
    assert instance.processed_data["binance_emergency_bid_cancels"] == 1
    assert instance.processed_data["binance_emergency_ask_cancels"] == 0


def test_binance_bullish_toxicity_bypasses_residency_and_keeps_bid_safe():
    instance, provider = native_controller(
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("10"),
    )
    asyncio.run(instance.update_processed_data())
    creates = instance.determine_executor_actions()
    _activate_from_create_actions(instance, creates)
    provider.now += 1
    provider.books["binance_perpetual_paper_trade"].bid.price += Decimal("0.003")
    provider.books["binance_perpetual_paper_trade"].ask.price += Decimal("0.003")
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert [action.executor_id for action in actions] == ["ask-active"]
    assert instance.processed_data["last_actions"]["ask"] == (
        "BINANCE_TRUE_SHOCK_LARGE_BINANCE_MOVE_CROSS_VENUE_DISLOCATION_CANCEL_ASK"
    )
    assert instance.processed_data["desired_ask"] is None
    assert instance.processed_data["desired_bid"] is not None
    assert instance.processed_data["binance_emergency_ask_cancels"] == 1
    assert instance.processed_data["binance_emergency_bid_cancels"] == 0


def test_binance_shock_requires_two_meaningful_conditions_for_emergency():
    state, side, conditions = controller.classify_binance_shock(
        Decimal("25"), Decimal("0"), Decimal("0")
    )
    assert (state, side, conditions) == ("ELEVATED", "ask", ("LARGE_BINANCE_MOVE",))
    state, side, conditions = controller.classify_binance_shock(
        Decimal("25"), Decimal("15"), Decimal("0")
    )
    assert state == "EMERGENCY"
    assert side == "ask"
    assert conditions == ("LARGE_BINANCE_MOVE", "CROSS_VENUE_DISLOCATION")


def test_binance_elevated_state_widens_without_cancelling_quotes():
    instance, provider = native_controller(shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    creates = instance.determine_executor_actions()
    _activate_from_create_actions(instance, creates)
    baseline = instance.processed_data["quote_total_spread_bps"]
    provider.now += 1
    # Move both venues together: one elevated Binance move, no cross-venue
    # dislocation, so the quotes remain active and simply widen.
    for book_name in ("derive_perpetual", "binance_perpetual_paper_trade"):
        book = provider.books[book_name]
        book.bid.price += Decimal("0.0005")
        book.ask.price += Decimal("0.0005")
        book.last_diff_uid += 1
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    assert actions == []
    assert instance.processed_data["binance_shock_state"] == "ELEVATED"
    assert instance.processed_data["binance_emergency_active"] is False
    assert instance.processed_data["desired_bid"] is not None
    assert instance.processed_data["desired_ask"] is not None
    assert instance.processed_data["quote_total_spread_bps"] > baseline


def test_binance_emergency_hysteresis_requires_normal_recovery():
    active, side, conditions, recovery_since, state = controller.update_binance_shock_hysteresis(
        active=False,
        latched_side=None,
        latched_conditions=(),
        recovery_since=None,
        raw_state="EMERGENCY",
        raw_side="ask",
        raw_conditions=("LARGE_BINANCE_MOVE", "CROSS_VENUE_DISLOCATION"),
        now=100.0,
        recovery_seconds=10.0,
    )
    assert (active, side, conditions, recovery_since, state) == (
        True,
        "ask",
        ("LARGE_BINANCE_MOVE", "CROSS_VENUE_DISLOCATION"),
        None,
        "EMERGENCY",
    )
    active, side, conditions, recovery_since, state = controller.update_binance_shock_hysteresis(
        active=active,
        latched_side=side,
        latched_conditions=conditions,
        recovery_since=recovery_since,
        raw_state="NORMAL",
        raw_side=None,
        raw_conditions=(),
        now=101.0,
        recovery_seconds=10.0,
    )
    assert active is True and state == "EMERGENCY_RECOVERY" and recovery_since == 101.0
    active, *_rest, state = controller.update_binance_shock_hysteresis(
        active=active,
        latched_side=side,
        latched_conditions=conditions,
        recovery_since=recovery_since,
        raw_state="NORMAL",
        raw_side=None,
        raw_conditions=(),
        now=111.0,
        recovery_seconds=10.0,
    )
    assert active is False and state == "NORMAL"


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
    assert controller.should_refresh(
        desired_price=Decimal("100.10"),
        age_seconds=Decimal("20"),
        derive_staleness_bps=Decimal("2"),
        refresh_reason="DERIVE_STALENESS_REFRESH",
        **base,
    )[1] == "DEADBAND"
    assert controller.should_refresh(desired_price=Decimal("100.00"), age_seconds=Decimal("1"), **(base | {"fast_adverse": True}))[1] == "FAST_ADVERSE_MOVE"


def test_xrp_portfolio_defaults_fit_single_asset_capital():
    xrp = config()
    assert xrp.portfolio_capital_quote == Decimal("800")
    assert xrp.total_amount_quote == Decimal("800")
    assert xrp.asset_cap_quote <= xrp.portfolio_capital_quote - xrp.reserve_quote
    assert xrp.order_amount_quote * 2 <= xrp.max_asset_open_order_quote


def test_shared_portfolio_rejects_inconsistent_terms():
    portfolio_id = "test_inconsistent_terms"
    native_controller(portfolio_id=portfolio_id, asset_cap_quote=Decimal("300"))
    with pytest.raises(ValueError, match="identical capital and reserve"):
        native_controller(
            portfolio_id=portfolio_id,
            portfolio_capital_quote=Decimal("900"),
            total_amount_quote=Decimal("900"),
        )

def test_committed_config_is_xrp_only_shadow_surface():
    config_dir = Path(__file__).parents[1] / "configs"
    row = yaml.safe_load((config_dir / "derive_binance_adaptive_mm_xrp.yml").read_text())
    assert row["asset"] == "XRP"
    assert row["portfolio_id"] == "derive_xrp_800"
    assert row["shadow_mode"] is True and row["mainnet_armed"] is False
    assert row["connector_name"] == "derive_perpetual"
    assert row["reference_connector_name"] == "binance_perpetual"
    assert "derive_binance_adaptive_mm_xrp.yml" in {
        path.name for path in config_dir.glob("derive_binance_adaptive_mm_*.yml")
    }
    assert row["order_amount_quote"] == 40
    assert row["normal_total_spread_bps"] == 8
    bot = yaml.safe_load((config_dir / "v2_with_controllers.yml").read_text())
    assert bot["controllers_config"] == ["derive_binance_adaptive_mm_xrp.yml"]


def test_live_canary_config_is_separate_shadow_disarmed_surface():
    config_dir = Path(__file__).parents[1] / "configs"
    row = yaml.safe_load((config_dir / "derive_binance_adaptive_mm_xrp_live_canary.yml").read_text())
    assert row["id"] == "derive_binance_adaptive_mm_xrp_live_canary"
    assert row["asset"] == "XRP"
    assert row["trading_pair"] == "XRP-USDC"
    assert row["reference_trading_pair"] == "XRP-USDT"
    assert row["connector_name"] == "derive_perpetual"
    assert row["reference_connector_name"] == "binance_perpetual"
    assert row["shadow_mode"] is True
    assert row["mainnet_armed"] is False
    assert row["order_amount_quote"] == 25
    assert row["one_sided_inventory_ratio"] == 0.50
    assert row["max_asset_inventory_quote"] == 50
    assert row["max_asset_open_order_quote"] == 50
    assert row["total_amount_quote"] == row["portfolio_capital_quote"] == 100
    assert row["reserve_quote"] == 0
    assert row["max_total_inventory_quote"] == row["max_total_open_order_quote"] == 50
    assert row["normal_total_spread_bps"] == 8


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


def canary_native_controller(provider=None, **overrides):
    defaults = {
        "total_amount_quote": Decimal("100"),
        "portfolio_capital_quote": Decimal("100"),
        "reserve_quote": Decimal("0"),
        "asset_cap_quote": Decimal("50"),
        "max_total_inventory_quote": Decimal("50"),
        "max_total_open_order_quote": Decimal("50"),
        "max_asset_inventory_quote": Decimal("50"),
        "max_asset_open_order_quote": Decimal("50"),
        "order_amount_quote": Decimal("25"),
        "one_sided_inventory_ratio": Decimal("0.50"),
        "shadow_mode": False,
        "mainnet_armed": True,
    }
    defaults.update(overrides)
    return native_controller(provider, **defaults)


def test_canary_flat_allows_one_bid_and_one_ask():
    instance, _ = canary_native_controller()
    asyncio.run(instance.update_processed_data())

    actions = instance.determine_executor_actions()
    assert {action.executor_config.level_id for action in actions} == {"bid", "ask"}
    assert all(action.executor_config.position_action == PositionAction.OPEN for action in actions)


def test_canary_soft_long_threshold_blocks_increasing_bid_allows_reducing_ask():
    provider = Provider()
    derive_mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", Decimal("25.01") / derive_mid)
    instance, _ = canary_native_controller(provider)
    asyncio.run(instance.update_processed_data())

    actions = instance.determine_executor_actions()
    assert instance.processed_data["inventory_mode"] == "ASK_ONLY"
    assert all(action.executor_config.level_id != "bid" for action in actions)
    ask = next(action for action in actions if action.executor_config.level_id == "ask")
    assert ask.executor_config.amount > Decimal("0")
    assert ask.executor_config.position_action == PositionAction.CLOSE
    projected = instance.processed_data["projected_position_amount_if_ask_fills"]
    assert projected >= Decimal("0")


def test_canary_soft_short_threshold_blocks_increasing_ask_allows_reducing_bid():
    provider = Provider()
    derive_mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", -Decimal("25.01") / derive_mid)
    instance, _ = canary_native_controller(provider)
    asyncio.run(instance.update_processed_data())

    actions = instance.determine_executor_actions()
    assert instance.processed_data["inventory_mode"] == "BID_ONLY"
    assert all(action.executor_config.level_id != "ask" for action in actions)
    bid = next(action for action in actions if action.executor_config.level_id == "bid")
    assert bid.executor_config.amount > Decimal("0")
    assert bid.executor_config.position_action == PositionAction.CLOSE
    projected = instance.processed_data["projected_position_amount_if_bid_fills"]
    assert projected <= Decimal("0")


def test_canary_below_hard_cap_allows_reducing_quote():
    provider = Provider()
    derive_mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", Decimal("40") / derive_mid)
    instance, _ = canary_native_controller(provider)
    asyncio.run(instance.update_processed_data())

    actions = instance.determine_executor_actions()
    ask = next(action for action in actions if action.executor_config.level_id == "ask")
    assert ask.executor_config.position_action == PositionAction.CLOSE
    assert ask.executor_config.amount > Decimal("0")
    assert abs(instance.processed_data["projected_position_notional_if_ask_fills"]) < Decimal("50")


def test_canary_hard_inventory_cap_blocks_further_increase_without_flip():
    provider = Provider()
    derive_mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", Decimal("50.01") / derive_mid)
    instance, _ = canary_native_controller(provider)
    asyncio.run(instance.update_processed_data())

    actions = instance.determine_executor_actions()
    assert instance.processed_data["inventory_mode"] == "ASK_ONLY"
    assert all(action.executor_config.level_id != "bid" for action in actions)
    bid_amount, reason, projected, _ = instance._risk_adjusted_amount(
        level="bid",
        side=TradeType.BUY,
        price=provider.books["derive_perpetual"].bid.price,
        desired_amount=Decimal("50"),
        now=provider.now,
        reserve=False,
    )
    assert bid_amount == Decimal("0")
    assert reason == "PROJECTED_ASSET_INVENTORY_LIMIT"
    assert projected == provider.positions["XRP-USDC"].amount


def test_inventory_increasing_bid_is_resized_to_asset_capacity():
    provider = Provider()
    mid = (provider.books["derive_perpetual"].bid.price + provider.books["derive_perpetual"].ask.price) / 2
    _set_position(provider, "XRP-USDC", Decimal("120") / mid)
    instance, _ = native_controller(
        provider,
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
    asyncio.run(instance.update_processed_data())
    assert len(instance.determine_executor_actions()) == 2


def test_projected_portfolio_inventory_limit_resizes_xrp_quote():
    xrp_provider = Provider()
    mid = (xrp_provider.books["derive_perpetual"].bid.price + xrp_provider.books["derive_perpetual"].ask.price) / 2
    _set_position(xrp_provider, "XRP-USDC", Decimal("150") / mid)
    instance, _ = native_controller(
        xrp_provider,
        max_total_inventory_quote=Decimal("200"),
        max_asset_inventory_quote=Decimal("300"),
        order_amount_quote=Decimal("125"),
        max_asset_open_order_quote=Decimal("260"),
        shadow_mode=False,
        mainnet_armed=True,
    )
    asyncio.run(instance.update_processed_data())
    actions = instance.determine_executor_actions()
    bid = next(action for action in actions if action.executor_config.level_id == "bid")
    assert bid.executor_config.amount * bid.executor_config.price <= Decimal("50")


def test_portfolio_projection_uses_conservative_active_and_pending_prices():
    instance, _ = native_controller(Provider(), id="xrp", asset="XRP")
    instance._portfolio[instance.config.portfolio_id] = {
        "XRP": controller.PortfolioSnapshot(
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


def _manual_plan(instance, bid: str | None, ask: str | None, amount: str = "18"):
    instance._plan = controller.QuotePlan(
        Decimal(bid) if bid is not None else None,
        Decimal(ask) if ask is not None else None,
        Decimal(amount),
        Decimal("0.5"),
        controller.MarketState.NORMAL,
        controller.MMMode.NEUTRAL,
        controller.InventoryMode.FLAT,
    )
    instance._price_tick = Decimal("0.0001")


def _executor(executor_id, level, price, now, *, active=True, status="RUNNING", custom_info=None):
    return types.SimpleNamespace(
        id=executor_id,
        is_active=active,
        status=status,
        timestamp=now,
        custom_info=custom_info or {},
        config=types.SimpleNamespace(level_id=level, price=Decimal(price), amount=Decimal("18")),
    )


def test_native_price_self_cross_guard_blocks_new_bid_against_active_ask():
    instance, provider = native_controller(Provider(), shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "1.38549", "1.3864")
    instance.executors_info = [_executor("ask-1", "ask", "1.3853", provider.now)]

    actions = instance.determine_executor_actions()

    assert actions == []
    assert instance.processed_data["ask_size_block_reason"] != "SELF_CROSS_GUARD"
    assert instance.processed_data["bid_size_block_reason"] == "SELF_CROSS_GUARD"


def test_native_price_self_cross_guard_blocks_new_ask_against_active_bid():
    instance, provider = native_controller(Provider(), shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "1.3840", "1.38549")
    instance.executors_info = [_executor("bid-1", "bid", "1.3853", provider.now)]

    actions = instance.determine_executor_actions()

    assert actions == []
    assert instance.processed_data["ask_size_block_reason"] == "SELF_CROSS_GUARD"


def test_pending_cancel_blocks_replacement_until_terminal_confirmation():
    instance, provider = native_controller(
        Provider(),
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("0"),
        normal_refresh_deadband_bps=Decimal("1"),
    )
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "0.4990", "0.5000")
    old_ask = _executor("ask-1", "ask", "0.5010", provider.now - 20)
    instance.executors_info = [old_ask]

    stop_actions = instance.determine_executor_actions()
    assert len(stop_actions) == 1
    assert isinstance(stop_actions[0], StopExecutorAction)

    old_ask.is_active = False
    old_ask.status = "SHUTTING_DOWN"
    assert instance.determine_executor_actions() == []
    assert instance.processed_data["pending_cancels"] == ["ask"]

    old_ask.status = "TERMINATED"
    actions = instance.determine_executor_actions()
    assert len(actions) == 2
    assert all(isinstance(action, CreateExecutorAction) for action in actions)
    assert instance.processed_data["pending_cancels"] == []


def test_native_runnable_status_name_releases_pending_cancel():
    class RunnableStatus(Enum):
        SHUTTING_DOWN = 3
        TERMINATED = 4

    instance, _ = native_controller(shadow_mode=False, mainnet_armed=True)
    shutting = _executor("ask-1", "ask", "0.5010", 100, active=False, status=RunnableStatus.SHUTTING_DOWN)
    terminated = _executor("ask-2", "ask", "0.5010", 100, active=False, status=RunnableStatus.TERMINATED)

    assert instance._is_terminal_executor(shutting) is False
    assert instance._is_terminal_executor(terminated) is True


def test_older_terminal_executor_does_not_release_fresh_reservation():
    instance, provider = native_controller(shadow_mode=False, mainnet_armed=True)
    key = (instance.config.id, "bid")
    instance._reservations[instance.config.portfolio_id] = {
        key: controller.PendingReservation(
            controller_id=instance.config.id,
            asset=instance.config.asset,
            level="bid",
            side=TradeType.BUY,
            amount=Decimal("10"),
            price=Decimal("0.5"),
            created_at=provider.now,
        )
    }
    instance.executors_info = [
        _executor("bid-old", "bid", "0.5", provider.now - 1, active=False, status="FILLED")
    ]

    instance._prune_reservations(provider.now, {})

    assert key in instance._reservations[instance.config.portfolio_id]


def _attach_created_executors(instance, provider, actions):
    """Materialize controller create actions as native-looking executors."""
    executors = []
    for action in actions:
        cfg = action.executor_config
        executor = _executor(
            f"{cfg.level_id}-1",
            cfg.level_id,
            str(cfg.price),
            provider.now,
        )
        executor.config.amount = cfg.amount
        executors.append(executor)
    instance.executors_info = executors
    return executors


def _freshen_books(provider):
    provider.books["derive_perpetual"].last_diff_uid += 1
    provider.books["binance_perpetual_paper_trade"].last_diff_uid += 1


def test_post_fill_short_releases_cancel_and_filled_reservations_for_reducing_bid():
    instance, provider = canary_native_controller(
        minimum_normal_quote_residency_seconds=Decimal("0"),
        normal_refresh_deadband_bps=Decimal("1"),
    )
    asyncio.run(instance.update_processed_data())
    initial_actions = instance.determine_executor_actions()
    executors = _attach_created_executors(instance, provider, initial_actions)
    old_bid, old_ask = executors

    # The ask fill leaves a short position at the one-sided threshold.  The
    # filled executor is terminal/non-active; the existing bid must be
    # cancelled before a reducing bid can be recreated.
    _set_position(provider, "XRP-USDC", "-50")
    old_ask.is_active = False
    old_ask.status = "FILLED"
    provider.now += 1
    _freshen_books(provider)
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["inventory_mode"] == "BID_ONLY"
    assert instance.processed_data["desired_ask"] is None
    assert instance.processed_data["desired_bid"] is not None
    assert (instance.config.id, "ask") not in instance._reservations[instance.config.portfolio_id]

    _manual_plan(instance, "0.5010", None)
    cycle_one = instance.determine_executor_actions()
    assert [type(action) for action in cycle_one] == [StopExecutorAction]
    assert cycle_one[0].executor_id == old_bid.id
    assert not any(type(action) is CreateExecutorAction for action in cycle_one)

    old_bid.is_active = False
    old_bid.status = "SHUTTING_DOWN"
    assert instance.determine_executor_actions() == []
    assert instance.processed_data["pending_cancels"] == ["bid"]
    assert not any(type(action) is CreateExecutorAction for action in instance.determine_executor_actions())

    old_bid.status = "TERMINATED"
    replacement = instance.determine_executor_actions()
    bid_creates = [action for action in replacement if type(action) is CreateExecutorAction]
    assert len(bid_creates) == 1
    assert bid_creates[0].executor_config.side == TradeType.BUY
    assert bid_creates[0].executor_config.position_action == PositionAction.CLOSE
    assert bid_creates[0].executor_config.amount <= Decimal("50")
    assert not any(
        action.executor_config.side == TradeType.SELL
        for action in bid_creates
    )
    assert instance.processed_data["bid_fill_effect"] in {"REDUCE", "FLATTEN"}


def test_post_fill_long_releases_cancel_and_filled_reservations_for_reducing_ask():
    instance, provider = canary_native_controller(
        minimum_normal_quote_residency_seconds=Decimal("0"),
        normal_refresh_deadband_bps=Decimal("1"),
    )
    asyncio.run(instance.update_processed_data())
    initial_actions = instance.determine_executor_actions()
    executors = _attach_created_executors(instance, provider, initial_actions)
    old_bid, old_ask = executors

    _set_position(provider, "XRP-USDC", "50")
    old_bid.is_active = False
    old_bid.status = "FILLED"
    provider.now += 1
    _freshen_books(provider)
    asyncio.run(instance.update_processed_data())
    assert instance.processed_data["inventory_mode"] == "ASK_ONLY"
    assert instance.processed_data["desired_bid"] is None
    assert instance.processed_data["desired_ask"] is not None
    assert (instance.config.id, "bid") not in instance._reservations[instance.config.portfolio_id]

    _manual_plan(instance, None, "0.4990")
    cycle_one = instance.determine_executor_actions()
    assert [type(action) for action in cycle_one] == [StopExecutorAction]
    assert cycle_one[0].executor_id == old_ask.id
    assert not any(type(action) is CreateExecutorAction for action in cycle_one)

    old_ask.is_active = False
    old_ask.status = "SHUTTING_DOWN"
    assert instance.determine_executor_actions() == []
    assert instance.processed_data["pending_cancels"] == ["ask"]
    assert not any(type(action) is CreateExecutorAction for action in instance.determine_executor_actions())

    old_ask.status = "TERMINATED"
    replacement = instance.determine_executor_actions()
    ask_creates = [action for action in replacement if type(action) is CreateExecutorAction]
    assert len(ask_creates) == 1
    assert ask_creates[0].executor_config.side == TradeType.SELL
    assert ask_creates[0].executor_config.position_action == PositionAction.CLOSE
    assert ask_creates[0].executor_config.amount <= Decimal("50")
    assert not any(
        action.executor_config.side == TradeType.BUY
        for action in ask_creates
    )
    assert instance.processed_data["ask_fill_effect"] in {"REDUCE", "FLATTEN"}


def _reload_manual_kill_switch(instance, enabled=True):
    """Apply the flag through the same updatable-field path as Hummingbot."""
    reloaded = instance.config.model_copy(update={"manual_kill_switch": enabled})
    instance.update_config(reloaded)


def test_manual_kill_switch_is_applied_by_hummingbot_config_reload():
    field = config().model_fields["manual_kill_switch"]
    assert field.json_schema_extra == {"is_updatable": True}

    instance, _ = native_controller(shadow_mode=False, mainnet_armed=True)
    assert instance.config.manual_kill_switch is False
    _reload_manual_kill_switch(instance)
    assert instance.config.manual_kill_switch is True


def test_stop_requested_cancels_active_quotes_without_new_creates():
    instance, provider = native_controller(
        Provider(),
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("0"),
        normal_refresh_deadband_bps=Decimal("1"),
    )
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "0.4990", "0.5000")
    instance.executors_info = [
        _executor("bid-1", "bid", "0.4990", provider.now - 20),
        _executor("ask-1", "ask", "0.5010", provider.now - 20),
    ]

    _reload_manual_kill_switch(instance)
    actions = instance.determine_executor_actions()

    assert {action.executor_id for action in actions} == {"bid-1", "ask-1"}
    assert all(type(action) is StopExecutorAction for action in actions)
    assert not any(type(action) is CreateExecutorAction for action in actions)


def test_stop_requested_while_cancellation_pending_blocks_replacements():
    instance, provider = native_controller(
        Provider(),
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("0"),
        normal_refresh_deadband_bps=Decimal("1"),
    )
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "0.4990", "0.5000")
    instance.executors_info = [
        _executor("bid-1", "bid", "0.4990", provider.now - 20),
        _executor("ask-1", "ask", "0.5010", provider.now - 20),
    ]

    _reload_manual_kill_switch(instance)
    first_actions = instance.determine_executor_actions()
    assert len(first_actions) == 2
    for executor in instance.executors_info:
        executor.is_active = False
        executor.status = "SHUTTING_DOWN"

    replacement_actions = instance.determine_executor_actions()

    assert replacement_actions == []
    assert instance.processed_data["pending_cancels"] == ["ask", "bid"]
    assert not any(type(action) is CreateExecutorAction for action in replacement_actions)


def test_asserted_stop_remains_create_free_across_subsequent_cycles():
    instance, provider = native_controller(
        Provider(),
        shadow_mode=False,
        mainnet_armed=True,
        minimum_normal_quote_residency_seconds=Decimal("0"),
        normal_refresh_deadband_bps=Decimal("1"),
    )
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "0.4990", "0.5000")
    instance.executors_info = [
        _executor("bid-1", "bid", "0.4990", provider.now - 20),
        _executor("ask-1", "ask", "0.5010", provider.now - 20),
    ]

    _reload_manual_kill_switch(instance)
    assert len(instance.determine_executor_actions()) == 2
    for executor in instance.executors_info:
        executor.is_active = False
        executor.status = "TERMINATED"

    for _ in range(3):
        actions = instance.determine_executor_actions()
        assert actions == []
        assert not any(type(action) is CreateExecutorAction for action in actions)


def test_shadow_normal_stop_cancels_active_quotes_without_real_creates():
    instance, provider = native_controller(Provider())
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "0.4990", "0.5000")
    instance.executors_info = [
        _executor("bid-1", "bid", "0.4990", provider.now - 20),
        _executor("ask-1", "ask", "0.5010", provider.now - 20),
    ]

    _reload_manual_kill_switch(instance)
    actions = instance.determine_executor_actions()

    assert instance.config.shadow_mode is True
    assert instance.config.mainnet_armed is False
    assert {action.executor_id for action in actions} == {"bid-1", "ask-1"}
    assert all(type(action) is StopExecutorAction for action in actions)
    assert not any(type(action) is CreateExecutorAction for action in actions)


def test_simultaneous_quotes_that_collapse_to_same_native_price_are_blocked():
    instance, _ = native_controller(Provider(), shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    _manual_plan(instance, "1.3853", "1.38549")

    actions = instance.determine_executor_actions()

    assert actions == []
    assert instance.processed_data["bid_size_block_reason"] == "SELF_CROSS_GUARD"
    assert instance.processed_data["ask_size_block_reason"] == "SELF_CROSS_GUARD"


def test_derive_reject_latches_fail_closed_and_preserves_diagnostics():
    instance, provider = native_controller(Provider(), shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    instance.executors_info = [
        _executor(
            "bid-1",
            "bid",
            "0.4990",
            provider.now,
            custom_info={
                "current_retries": 1,
                "last_error": "Order was rejected because it crossed with another order placed by the same user",
            },
        )
    ]

    stop_actions = instance.determine_executor_actions()
    assert len(stop_actions) == 1
    assert isinstance(stop_actions[0], StopExecutorAction)
    assert instance.processed_data["execution_fail_closed"] is True
    assert "crossed" in instance.processed_data["last_error"]

    instance.executors_info[0].is_active = False
    instance.executors_info[0].status = "TERMINATED"
    assert instance.determine_executor_actions() == []
    details = instance.get_custom_info()
    assert details["last_error"]
    assert details["pending_cancels"] == []
    assert "peer_health" not in details


def test_nonce_reject_latches_fail_closed_without_new_creates():
    instance, provider = native_controller(Provider(), shadow_mode=False, mainnet_armed=True)
    asyncio.run(instance.update_processed_data())
    instance.executors_info = [
        _executor(
            "ask-1",
            "ask",
            "0.5010",
            provider.now,
            custom_info={
                "current_retries": 1,
                "last_error": "This nonce has already been used, please use a new nonce",
            },
        )
    ]

    actions = instance.determine_executor_actions()
    assert len(actions) == 1
    assert isinstance(actions[0], StopExecutorAction)
    assert instance.processed_data["operational_state"] == "ERROR"

    instance.executors_info[0].is_active = False
    instance.executors_info[0].status = "TERMINATED"
    assert instance.determine_executor_actions() == []
    assert "nonce" in instance.get_custom_info()["last_error"]


def test_nonce_compatibility_counter_is_strictly_monotonic():
    controller._last_derive_action_nonce = 0
    assert controller._next_unique_derive_nonce(100) == 100
    assert controller._next_unique_derive_nonce(100) == 101
    assert controller._next_unique_derive_nonce(99) == 102


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
