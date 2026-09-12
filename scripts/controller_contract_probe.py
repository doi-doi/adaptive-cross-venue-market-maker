"""Probe the current Hummingbot image with fake books; never connects or places orders."""

from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_ROOT / "src"), str(_ROOT / "controllers")]

from market_making.derive_multi_asset_binance_reference_mm import (  # noqa: E402
    DeriveMultiAssetBinanceMMConfig,
    DeriveMultiAssetBinanceMMController,
)


class _Row:
    def __init__(self, price: str, amount: str) -> None:
        self.price = Decimal(price)
        self.amount = Decimal(amount)


class _Book:
    def __init__(self, bid: str, ask: str) -> None:
        self._bid = _Row(bid, "10")
        self._ask = _Row(ask, "10")

    def bid_entries(self):
        return iter([self._bid])

    def ask_entries(self):
        return iter([self._ask])


class _Provider:
    def time(self) -> float:
        return 1000.0

    def get_order_book(self, connector: str, pair: str) -> _Book:
        return _Book("100", "101") if connector == "derive_perpetual" else _Book("100.2", "100.8")

    def get_trading_rules(self, connector: str, pair: str):
        return SimpleNamespace(
            min_price_increment=Decimal("0.1"),
            min_base_amount_increment=Decimal("0.1"),
            min_order_size=Decimal("1"),
            max_order_size=Decimal("100"),
            min_notional_size=Decimal("0"),
            min_order_value=Decimal("0"),
        )

    def get_connector(self, name: str):
        return SimpleNamespace(account_positions={})


async def main() -> None:
    config = DeriveMultiAssetBinanceMMConfig(
        id="contract_probe",
        controller_name="derive_multi_asset_binance_reference_mm",
        assets=["DOGE", "ADA", "XRP"],
        max_active_assets=3,
    )
    controller = DeriveMultiAssetBinanceMMController(config, _Provider(), asyncio.Queue())
    await controller.update_processed_data()
    actions = controller.determine_executor_actions()
    if actions:
        raise AssertionError(f"shadow contract emitted actions: {actions}")
    controls = {row.get("reference_control") for row in controller.processed_data.values()}
    if controls != {"PRIORITY_FAILOVER"}:
        raise AssertionError(f"priority control not active: {controls}")
    print(f"mode={config.mode} assets={sorted(controller.processed_data)} priority={config.reference_priority} actions={len(actions)}")
    print(controller.to_format_status()[0])


if __name__ == "__main__":
    asyncio.run(main())
