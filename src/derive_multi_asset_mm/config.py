"""Strict, mainnet-only configuration loading."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from .models import AssetSpec, Mode, QuotePlacement


def _decimal(value: Any, default: str = "0") -> Decimal:
    return Decimal(str(default if value is None else value))


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class RuntimeConfig:
    mode: Mode = Mode.MAINNET_SHADOW
    dry_run: bool = True
    mainnet_armed: bool = False
    derive_connector: str = "derive_perpetual"
    binance_connector: str = "binance_perpetual"
    derive_public_url: str = "https://api.lyra.finance"
    derive_websocket_url: str = "wss://api.lyra.finance/ws"
    binance_exchange_info_url: str = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    binance_websocket_url: str = "wss://fstream.binance.com/stream"
    capital_usdc: Decimal = Decimal("800")
    assets: tuple[AssetSpec, ...] = ()
    max_active_assets: int = 4
    order_size_multiplier: Decimal = Decimal("1")
    maker_fee_bps: Decimal = Decimal("1")
    fair_value_mid_weight: Decimal = Decimal("0.50")
    fair_value_microprice_weight: Decimal = Decimal("0.50")
    basis_window: int = 120
    basis_max_deviation_bps: Decimal = Decimal("50")
    min_edge_bps: Decimal = Decimal("4")
    volatility_buffer_bps: Decimal = Decimal("2")
    latency_buffer_bps: Decimal = Decimal("1")
    toxicity_buffer_bps: Decimal = Decimal("1")
    minimum_profit_bps: Decimal = Decimal("1")
    directional_skew_max_bps: Decimal = Decimal("2")
    inventory_skew_max_bps: Decimal = Decimal("12")
    portfolio_skew_max_bps: Decimal = Decimal("4")
    bbo_stale_seconds: Decimal = Decimal("5")
    reference_stale_seconds: Decimal = Decimal("5")
    refresh_tolerance_bps: Decimal = Decimal("3")
    quote_max_age_seconds: Decimal = Decimal("30")
    max_single_order_notional: Decimal = Decimal("120")
    max_open_order_notional: Decimal = Decimal("400")
    max_inventory_per_asset: Decimal = Decimal("200")
    max_portfolio_inventory: Decimal = Decimal("400")
    max_drawdown: Decimal = Decimal("40")
    max_actions_per_minute: int = 30
    quote_placement: QuotePlacement = QuotePlacement.AT_TOUCH
    max_book_levels: int = 5
    direction_threshold_bps: Decimal = Decimal("1")
    high_vol_threshold_bps: Decimal = Decimal("8")
    extreme_vol_threshold_bps: Decimal = Decimal("20")
    aggressive_spread_max_bps: Decimal = Decimal("15")
    defensive_spread_min_bps: Decimal = Decimal("3")
    fast_move_threshold_bps: Decimal = Decimal("8")
    report_dir: Path = Path("reports/mainnet_shadow")
    log_dir: Path = Path("logs/mainnet_shadow")
    database_path: Path = Path("logs/mainnet_shadow/telemetry.sqlite")

    @classmethod
    def from_yaml(cls, path: str | Path) -> RuntimeConfig:
        config_path = Path(path)
        with config_path.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        if not isinstance(raw, dict):
            raise ValueError("configuration root must be a mapping")
        config = cls.from_mapping(raw)
        return config

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> RuntimeConfig:
        mode = Mode(str(raw.get("mode", Mode.MAINNET_SHADOW.value)).strip().upper())
        assets_raw = raw.get("assets", {})
        if isinstance(assets_raw, list):
            assets_raw = {str(asset): {} for asset in assets_raw}
        if not isinstance(assets_raw, dict):
            raise ValueError("assets must be a mapping or list")
        assets: list[AssetSpec] = []
        for symbol, value in assets_raw.items():
            if value is None:
                value = {}
            if not isinstance(value, dict):
                raise ValueError(f"asset override for {symbol} must be a mapping")
            assets.append(
                AssetSpec(
                    symbol=str(symbol).strip().upper(),
                    enabled=_bool(value.get("enabled"), True),
                    order_size_multiplier=_decimal(
                        value.get("order_size_multiplier", raw.get("order_size_multiplier", "1")), "1"
                    ),
                    max_inventory_per_asset=(
                        _decimal(value["max_inventory_per_asset"])
                        if value.get("max_inventory_per_asset") is not None
                        else None
                    ),
                )
            )
        config = cls(
            mode=mode,
            dry_run=_bool(raw.get("dry_run"), mode == Mode.MAINNET_SHADOW),
            mainnet_armed=_bool(raw.get("mainnet_armed"), False),
            derive_connector=str(raw.get("derive_connector", "derive_perpetual")),
            binance_connector=str(raw.get("binance_connector", "binance_perpetual")),
            derive_public_url=str(raw.get("derive_public_url", cls.derive_public_url)),
            derive_websocket_url=str(raw.get("derive_websocket_url", cls.derive_websocket_url)),
            binance_exchange_info_url=str(raw.get("binance_exchange_info_url", cls.binance_exchange_info_url)),
            binance_websocket_url=str(raw.get("binance_websocket_url", cls.binance_websocket_url)),
            capital_usdc=_decimal(raw.get("capital_usdc"), "800"),
            assets=tuple(assets),
            max_active_assets=int(raw.get("max_active_assets", 4)),
            order_size_multiplier=_decimal(raw.get("order_size_multiplier"), "1"),
            maker_fee_bps=_decimal(raw.get("maker_fee_bps"), "1"),
            fair_value_mid_weight=_decimal(raw.get("fair_value_mid_weight"), "0.5"),
            fair_value_microprice_weight=_decimal(raw.get("fair_value_microprice_weight"), "0.5"),
            basis_window=int(raw.get("basis_window", 120)),
            basis_max_deviation_bps=_decimal(raw.get("basis_max_deviation_bps"), "50"),
            min_edge_bps=_decimal(raw.get("min_edge_bps"), "4"),
            volatility_buffer_bps=_decimal(raw.get("volatility_buffer"), "2"),
            latency_buffer_bps=_decimal(raw.get("latency_buffer"), "1"),
            toxicity_buffer_bps=_decimal(raw.get("toxicity_buffer"), "1"),
            minimum_profit_bps=_decimal(raw.get("minimum_profit_buffer", raw.get("minimum_profit_bps", "1")), "1"),
            directional_skew_max_bps=_decimal(raw.get("directional_skew_max_bps"), "2"),
            inventory_skew_max_bps=_decimal(raw.get("inventory_skew_max_bps"), "12"),
            portfolio_skew_max_bps=_decimal(raw.get("portfolio_skew_max_bps"), "4"),
            bbo_stale_seconds=_decimal(raw.get("bbo_stale_seconds"), "5"),
            reference_stale_seconds=_decimal(raw.get("reference_stale_seconds"), "5"),
            refresh_tolerance_bps=_decimal(raw.get("refresh_tolerance_bps"), "3"),
            quote_max_age_seconds=_decimal(raw.get("quote_max_age_seconds"), "30"),
            max_single_order_notional=_decimal(raw.get("max_single_order_notional"), "120"),
            max_open_order_notional=_decimal(raw.get("max_open_order_notional"), "400"),
            max_inventory_per_asset=_decimal(raw.get("max_inventory_per_asset"), "200"),
            max_portfolio_inventory=_decimal(raw.get("max_portfolio_inventory"), "400"),
            max_drawdown=_decimal(raw.get("max_drawdown"), "40"),
            max_actions_per_minute=int(raw.get("max_actions_per_minute", 30)),
            quote_placement=QuotePlacement(str(raw.get("quote_placement", "AT_TOUCH")).upper()),
            max_book_levels=int(raw.get("max_book_levels", 5)),
            direction_threshold_bps=_decimal(raw.get("direction_threshold_bps"), "1"),
            high_vol_threshold_bps=_decimal(raw.get("high_vol_threshold_bps"), "8"),
            extreme_vol_threshold_bps=_decimal(raw.get("extreme_vol_threshold_bps"), "20"),
            aggressive_spread_max_bps=_decimal(raw.get("aggressive_spread_max_bps"), "15"),
            defensive_spread_min_bps=_decimal(raw.get("defensive_spread_min_bps"), "3"),
            fast_move_threshold_bps=_decimal(raw.get("fast_move_threshold_bps"), "8"),
            report_dir=Path(str(raw.get("report_dir", "reports/mainnet_shadow"))),
            log_dir=Path(str(raw.get("log_dir", "logs/mainnet_shadow"))),
            database_path=Path(str(raw.get("database_path", "logs/mainnet_shadow/telemetry.sqlite"))),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.mode == Mode.MAINNET_SHADOW and (not self.dry_run or self.mainnet_armed):
            raise ValueError("MAINNET_SHADOW requires dry_run=true and mainnet_armed=false")
        if self.mode == Mode.MAINNET_LIVE and (self.dry_run or not self.mainnet_armed):
            raise ValueError("MAINNET_LIVE requires dry_run=false and mainnet_armed=true")
        if self.derive_connector != "derive_perpetual":
            raise ValueError("this project is mainnet-only and requires derive_perpetual")
        if "testnet" in self.derive_connector.lower() or "testnet" in self.derive_public_url.lower():
            raise ValueError("testnet configuration is not supported")
        if not self.derive_public_url.startswith("https://") or not self.derive_websocket_url.startswith("wss://"):
            raise ValueError("Derive endpoints must be secure public endpoints")
        if not self.binance_exchange_info_url.startswith("https://") or not self.binance_websocket_url.startswith("wss://"):
            raise ValueError("Binance endpoints must be secure public endpoints")
        if self.capital_usdc <= 0 or self.max_active_assets < 1:
            raise ValueError("capital_usdc and max_active_assets must be positive")
        if not self.assets or not any(asset.enabled for asset in self.assets):
            raise ValueError("at least one enabled asset is required")
        if self.max_active_assets > len([asset for asset in self.assets if asset.enabled]):
            raise ValueError("max_active_assets cannot exceed enabled asset count")
        if self.order_size_multiplier <= 0 or any(asset.order_size_multiplier <= 0 for asset in self.assets):
            raise ValueError("order size multipliers must be positive")
        if self.fair_value_mid_weight < 0 or self.fair_value_microprice_weight < 0:
            raise ValueError("fair value weights cannot be negative")
        if self.fair_value_mid_weight + self.fair_value_microprice_weight <= 0:
            raise ValueError("at least one fair value weight must be positive")
        if self.basis_window < 1 or self.max_book_levels < 1:
            raise ValueError("basis_window and max_book_levels must be positive")
        if self.max_single_order_notional <= 0 or self.max_open_order_notional < self.max_single_order_notional:
            raise ValueError("open-order notional must cover one single order")
        if self.max_inventory_per_asset <= 0 or self.max_portfolio_inventory < self.max_inventory_per_asset:
            raise ValueError("portfolio inventory must cover one asset limit")
        if self.max_actions_per_minute < 1:
            raise ValueError("max_actions_per_minute must be positive")
        if self.high_vol_threshold_bps >= self.extreme_vol_threshold_bps:
            raise ValueError("high_vol_threshold_bps must be below extreme_vol_threshold_bps")

    @property
    def enabled_assets(self) -> tuple[AssetSpec, ...]:
        return tuple(asset for asset in self.assets if asset.enabled)

    def asset(self, symbol: str) -> AssetSpec:
        symbol = symbol.upper()
        for asset in self.assets:
            if asset.symbol == symbol:
                return asset
        raise KeyError(symbol)

    def public_safety(self) -> dict[str, Any]:
        return {
            "environment": "mainnet",
            "mode": self.mode.value,
            "mainnet_armed": self.mainnet_armed,
            "dry_run": self.dry_run,
            "real_orders": 0,
            "real_positions": 0,
            "binance_execution": False,
            "credentials_loaded": False,
            "private_api_used": False,
        }

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["mode"] = self.mode.value
        result["quote_placement"] = self.quote_placement.value
        result["assets"] = {
            asset.symbol: {
                "enabled": asset.enabled,
                "order_size_multiplier": str(asset.order_size_multiplier),
                "max_inventory_per_asset": (
                    str(asset.max_inventory_per_asset) if asset.max_inventory_per_asset is not None else None
                ),
            }
            for asset in self.assets
        }
        for key in (
            "capital_usdc",
            "order_size_multiplier",
            "maker_fee_bps",
            "fair_value_mid_weight",
            "fair_value_microprice_weight",
            "basis_max_deviation_bps",
            "min_edge_bps",
            "volatility_buffer_bps",
            "latency_buffer_bps",
            "toxicity_buffer_bps",
            "minimum_profit_bps",
            "directional_skew_max_bps",
            "inventory_skew_max_bps",
            "portfolio_skew_max_bps",
            "bbo_stale_seconds",
            "reference_stale_seconds",
            "refresh_tolerance_bps",
            "quote_max_age_seconds",
            "max_single_order_notional",
            "max_open_order_notional",
            "max_inventory_per_asset",
            "max_portfolio_inventory",
            "max_drawdown",
            "direction_threshold_bps",
            "high_vol_threshold_bps",
            "extreme_vol_threshold_bps",
            "aggressive_spread_max_bps",
            "defensive_spread_min_bps",
            "fast_move_threshold_bps",
        ):
            result[key] = str(result[key])
        for key in ("report_dir", "log_dir", "database_path"):
            result[key] = str(result[key])
        return result
