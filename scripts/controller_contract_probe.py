"""Probe the current Hummingbot image with fake books; never connects or places orders."""

from __future__ import annotations

import asyncio
import inspect
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_ROOT / "controllers")]

from market_making.derive_binance_adaptive_mm import (  # noqa: E402
    DERIVE_SNAPSHOT_RACE_COMPATIBILITY_ACTIVE,
    DeriveBinanceAdaptiveMM,
    DeriveBinanceAdaptiveMMConfig,
)


class _Row:
    def __init__(self, price: str, amount: str) -> None:
        self.price = Decimal(price)
        self.amount = Decimal(amount)


class _Book:
    def __init__(self, bid: str, ask: str) -> None:
        self._bid = _Row(bid, "10")
        self._ask = _Row(ask, "10")
        self.last_diff_uid = 1

    def bid_entries(self):
        return iter([self._bid])

    def ask_entries(self):
        return iter([self._ask])


class _Provider:
    def time(self) -> float:
        return 1000.0

    def get_order_book(self, connector: str, pair: str) -> _Book:
        return _Book("0.499", "0.501") if connector.startswith("derive_perpetual") else _Book("0.4995", "0.5005")

    def get_trading_rules(self, connector: str, pair: str):
        return SimpleNamespace(
            min_price_increment=Decimal("0.0001"),
            min_base_amount_increment=Decimal("1"),
            min_order_size=Decimal("1"),
            max_order_size=Decimal("100"),
            min_notional_size=Decimal("0"),
            min_order_value=Decimal("0"),
        )

    def get_connector(self, name: str):
        return SimpleNamespace(account_positions={})

    def quantize_order_price(self, connector: str, pair: str, price: Decimal) -> Decimal:
        return price.quantize(Decimal("0.0001"))

    def quantize_order_amount(self, connector: str, pair: str, amount: Decimal) -> Decimal:
        return amount.quantize(Decimal("1"))


async def main() -> None:
    import hummingbot
    from hummingbot.connector.derivative.derive_perpetual.derive_perpetual_derivative import (
        DerivePerpetualDerivative,
    )

    version = (Path(hummingbot.__file__).resolve().parent / "VERSION").read_text(encoding="utf-8").strip()
    if version != "2.16.0":
        raise AssertionError(f"expected Hummingbot 2.16.0, got {version}")
    place_order_source = inspect.getsource(DerivePerpetualDerivative._place_order)
    if '"reduce_only": False' not in place_order_source:
        raise AssertionError("Derive payload semantics changed: reduce_only=False not found")
    if '"reduce_only": position_action' in place_order_source:
        raise AssertionError("unexpected PositionAction-derived reduce_only behavior")
    limit_maker_gtc = (
        "order_type is OrderType.LIMIT_MAKER" in place_order_source
        and 'param_order_type = "gtc"' in place_order_source
    )
    if not limit_maker_gtc:
        raise AssertionError("Derive LIMIT_MAKER gtc contract changed")
    if not DERIVE_SNAPSHOT_RACE_COMPATIBILITY_ACTIVE:
        raise AssertionError("guarded 2.16.0 Derive snapshot shim did not activate")
    config = DeriveBinanceAdaptiveMMConfig(
        id="contract_probe",
        asset="XRP",
        trading_pair="XRP-USDC",
        reference_trading_pair="XRP-USDT",
        binance_recovery_seconds=0,
    )
    controller = DeriveBinanceAdaptiveMM(config, _Provider(), asyncio.Queue())
    await controller.update_processed_data()
    actions = controller.determine_executor_actions()
    if actions:
        raise AssertionError(f"shadow contract emitted actions: {actions}")
    if controller.processed_data.get("mm_mode") == "PAUSED":
        raise AssertionError(f"controller did not calculate quotes: {controller.processed_data}")
    print(f"shadow={config.shadow_mode} asset={config.asset} reference={config.reference_connector_name} actions={len(actions)}")
    print("derive_position_action=open_or_close reduce_only=false limit_maker_tif=gtc")
    print(f"hummingbot={version} snapshot_shim={DERIVE_SNAPSHOT_RACE_COMPATIBILITY_ACTIVE}")
    print(controller.to_format_status()[0])


if __name__ == "__main__":
    asyncio.run(main())
