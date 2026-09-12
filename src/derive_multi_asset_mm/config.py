"""Strict, mainnet-only configuration loading."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
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
    multi_reference: bool = False
    reference_venues: tuple[str, ...] = ("binance", "bybit", "okx", "bitget")
    reference_selection_mode: str = "LEGACY"
    reference_priority: tuple[str, ...] = ("binance", "bybit", "okx")
    # Kept true by default for backwards compatibility with historical
    # profiles. New profiles should set this explicitly and omit Bitget from
    # reference_venues so the venue is not even scheduled at runtime.
    bitget_enabled: bool = True
    bitget_primary_enabled: bool = False
    recovery_min_healthy_seconds: float = 3.0
    reference_healthy_seconds: float = 2.0
    reference_stale_overrides: dict = field(default_factory=dict)
    reference_outlier_bps: Decimal = Decimal("50")
    reference_disagreement_pause_bps: Decimal = Decimal("25")
    minimum_reference_sources: int = 1
    mode: Mode = Mode.MAINNET_SHADOW
    dry_run: bool = True
    mainnet_armed: bool = False
    derive_connector: str = "derive_perpetual"
    binance_connector: str = "binance_perpetual"
    derive_public_url: str = "https://api.lyra.finance"
    derive_websocket_url: str = "wss://api.lyra.finance/ws"
    derive_trade_history_poll_seconds: float = 15.0
    storage_warning_free_gb: float = 15.0
    storage_critical_free_gb: float = 10.0
    storage_emergency_free_gb: float = 5.0
    raw_retention_seconds: int = 180
    feature_persist_interval_seconds: float = 1.0
    aggregate_interval_seconds: int = 60
    chunk_rotation_minutes: int = 10
    governor_check_interval_seconds: float = 5.0
    # Persistence-only budgets. They never alter the strategy loop, quote
    # inputs, sizing, lifecycle, or fill/markout rules.
    max_run_storage_gb: float = 3.0
    warning_run_storage_gb: float = 2.0
    critical_run_storage_gb: float = 2.5
    max_project_generated_data_gb: float = 10.0
    binance_exchange_info_url: str = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    binance_websocket_url: str = "wss://fstream.binance.com/stream"
    bybit_instruments_url: str = "https://api.bybit.com/v5/market/instruments-info?category=linear"
    bybit_websocket_url: str = "wss://stream.bybit.com/v5/public/linear"
    okx_instruments_url: str = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    okx_websocket_url: str = "wss://ws.okx.com:8443/ws/v5/public"
    bitget_contracts_url: str = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
    bitget_websocket_url: str = "wss://ws.bitget.com/v2/ws/public"
    capital_usdc: Decimal = Decimal("800")
    assets: tuple[AssetSpec, ...] = ()
    max_active_assets: int = 4
    order_size_multiplier: Decimal = Decimal("1")
    maker_fee_bps: Decimal = Decimal("1")
    fair_value_mid_weight: Decimal = Decimal("0.50")
    fair_value_microprice_weight: Decimal = Decimal("0.50")
    basis_window: int = 120
    basis_ewma_alpha: Decimal = Decimal("0.20")
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
    # Active quotes refresh only when their distance from the causal Derive mid
    # exceeds this threshold. The current policy is 2% = 200 bps.
    refresh_tolerance_bps: Decimal = Decimal("200")
    # The refresh-research phase uses a separate deadband/residency policy. A
    # null value preserves the historical lifecycle behavior for old profiles.
    refresh_deadband_bps: Decimal | None = None
    minimum_normal_quote_residency_seconds: Decimal = Decimal("0")
    fast_adverse_move_override_enabled: bool = False
    fast_adverse_move_threshold_bps: Decimal = Decimal("8")
    quote_max_age_seconds: Decimal = Decimal("30")
    max_single_order_notional: Decimal = Decimal("120")
    max_open_order_notional: Decimal = Decimal("400")
    max_inventory_per_asset: Decimal = Decimal("200")
    max_portfolio_inventory: Decimal = Decimal("400")
    max_drawdown: Decimal = Decimal("40")
    max_actions_per_minute: int = 30
    max_order_actions_per_second: Decimal = Decimal("1")
    max_order_actions_per_instrument_per_second: Decimal = Decimal("1")
    target_action_utilization: Decimal = Decimal("0.50")
    emergency_cancel_budget_per_minute: int = 6
    rate_limit_status: str = "DERIVE_RATE_LIMIT_NOT_FULLY_VERIFIED"
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
        reference_venues_raw = raw.get("reference_venues") or ("binance", "bybit", "okx", "bitget")
        if isinstance(reference_venues_raw, str):
            reference_venues_raw = [reference_venues_raw]
        reference_priority_raw = raw.get("reference_priority") or ("binance", "bybit", "okx")
        if isinstance(reference_priority_raw, str):
            reference_priority_raw = [reference_priority_raw]
        config = cls(
            multi_reference=_bool(raw.get("multi_reference"), False),
            reference_venues=tuple(
                str(venue).strip().lower()
                for venue in reference_venues_raw
            ),
            reference_selection_mode=str(raw.get("reference_selection_mode", "LEGACY")).strip().upper(),
            reference_priority=tuple(
                str(venue).strip().lower()
                for venue in reference_priority_raw
            ),
            bitget_enabled=_bool(raw.get("bitget_enabled"), True),
            bitget_primary_enabled=_bool(raw.get("bitget_primary_enabled"), False),
            recovery_min_healthy_seconds=float(raw.get("recovery_min_healthy_seconds", 3)),
            reference_healthy_seconds=float(raw.get("reference_healthy_seconds", 2)),
            reference_stale_overrides=dict(raw.get("reference_stale_overrides", {})),
            reference_outlier_bps=_decimal(raw.get("reference_outlier_bps"), "50"),
            reference_disagreement_pause_bps=_decimal(raw.get("reference_disagreement_pause_bps"), "25"),
            minimum_reference_sources=int(raw.get("minimum_reference_sources", 1)),
            mode=mode,
            dry_run=_bool(raw.get("dry_run"), mode == Mode.MAINNET_SHADOW),
            mainnet_armed=_bool(raw.get("mainnet_armed"), False),
            derive_connector=str(raw.get("derive_connector", "derive_perpetual")),
            binance_connector=str(raw.get("binance_connector", "binance_perpetual")),
            derive_public_url=str(raw.get("derive_public_url", cls.derive_public_url)),
            derive_websocket_url=str(raw.get("derive_websocket_url", cls.derive_websocket_url)),
            derive_trade_history_poll_seconds=float(raw.get("derive_trade_history_poll_seconds", 15.0)),
            storage_warning_free_gb=float(raw.get("storage_warning_free_gb", 15.0)),
            storage_critical_free_gb=float(raw.get("storage_critical_free_gb", 10.0)),
            storage_emergency_free_gb=float(raw.get("storage_emergency_free_gb", 5.0)),
            raw_retention_seconds=int(raw.get("raw_retention_seconds", 180)),
            feature_persist_interval_seconds=float(raw.get("feature_persist_interval_seconds", 1.0)),
            aggregate_interval_seconds=int(raw.get("aggregate_interval_seconds", 60)),
            chunk_rotation_minutes=int(raw.get("chunk_rotation_minutes", 10)),
            governor_check_interval_seconds=float(raw.get("governor_check_interval_seconds", 5.0)),
            max_run_storage_gb=float(raw.get("max_run_storage_gb", 3.0)),
            warning_run_storage_gb=float(raw.get("warning_run_storage_gb", 2.0)),
            critical_run_storage_gb=float(raw.get("critical_run_storage_gb", 2.5)),
            max_project_generated_data_gb=float(raw.get("max_project_generated_data_gb", 10.0)),
            binance_exchange_info_url=str(raw.get("binance_exchange_info_url", cls.binance_exchange_info_url)),
            binance_websocket_url=str(raw.get("binance_websocket_url", cls.binance_websocket_url)),
            bybit_instruments_url=str(raw.get("bybit_instruments_url", cls.bybit_instruments_url)),
            bybit_websocket_url=str(raw.get("bybit_websocket_url", cls.bybit_websocket_url)),
            okx_instruments_url=str(raw.get("okx_instruments_url", cls.okx_instruments_url)),
            okx_websocket_url=str(raw.get("okx_websocket_url", cls.okx_websocket_url)),
            bitget_contracts_url=str(raw.get("bitget_contracts_url", cls.bitget_contracts_url)),
            bitget_websocket_url=str(raw.get("bitget_websocket_url", cls.bitget_websocket_url)),
            capital_usdc=_decimal(raw.get("capital_usdc"), "800"),
            assets=tuple(assets),
            max_active_assets=int(raw.get("max_active_assets", 4)),
            order_size_multiplier=_decimal(raw.get("order_size_multiplier"), "1"),
            maker_fee_bps=_decimal(raw.get("maker_fee_bps"), "1"),
            fair_value_mid_weight=_decimal(raw.get("fair_value_mid_weight"), "0.5"),
            fair_value_microprice_weight=_decimal(raw.get("fair_value_microprice_weight"), "0.5"),
            basis_window=int(raw.get("basis_window", 120)),
            basis_ewma_alpha=_decimal(raw.get("basis_ewma_alpha"), "0.2"),
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
            refresh_tolerance_bps=_decimal(raw.get("refresh_tolerance_bps"), "200"),
            refresh_deadband_bps=(
                _decimal(raw.get("refresh_deadband_bps"))
                if raw.get("refresh_deadband_bps") is not None
                else None
            ),
            minimum_normal_quote_residency_seconds=_decimal(
                raw.get("minimum_normal_quote_residency_seconds"), "0"
            ),
            fast_adverse_move_override_enabled=_bool(
                raw.get("fast_adverse_move_override_enabled"), False
            ),
            fast_adverse_move_threshold_bps=_decimal(
                raw.get("fast_adverse_move_threshold_bps"), "8"
            ),
            quote_max_age_seconds=_decimal(raw.get("quote_max_age_seconds"), "30"),
            max_single_order_notional=_decimal(raw.get("max_single_order_notional"), "120"),
            max_open_order_notional=_decimal(raw.get("max_open_order_notional"), "400"),
            max_inventory_per_asset=_decimal(raw.get("max_inventory_per_asset"), "200"),
            max_portfolio_inventory=_decimal(raw.get("max_portfolio_inventory"), "400"),
            max_drawdown=_decimal(raw.get("max_drawdown"), "40"),
            max_actions_per_minute=int(raw.get("max_actions_per_minute", 30)),
            max_order_actions_per_second=_decimal(
                raw.get("max_order_actions_per_second"), "1"
            ),
            max_order_actions_per_instrument_per_second=_decimal(
                raw.get("max_order_actions_per_instrument_per_second"), "1"
            ),
            target_action_utilization=_decimal(
                raw.get("target_action_utilization"), "0.50"
            ),
            emergency_cancel_budget_per_minute=int(
                raw.get("emergency_cancel_budget_per_minute", 6)
            ),
            rate_limit_status=str(
                raw.get("rate_limit_status", "DERIVE_RATE_LIMIT_NOT_FULLY_VERIFIED")
            ),
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
        allowed_venues = {"binance", "bybit", "okx", "bitget"}
        if not self.reference_venues or any(venue not in allowed_venues for venue in self.reference_venues):
            raise ValueError("reference_venues must contain only binance, bybit, okx, and bitget")
        if len(set(self.reference_venues)) != len(self.reference_venues):
            raise ValueError("reference_venues cannot contain duplicates")
        if not self.bitget_enabled and "bitget" in self.reference_venues:
            raise ValueError("bitget_enabled=false requires bitget to be absent from reference_venues")
        if not self.bitget_enabled and self.bitget_primary_enabled:
            raise ValueError("bitget_enabled=false cannot enable Bitget as primary")
        if self.reference_selection_mode not in {"LEGACY", "PRIORITY_FAILOVER"}:
            raise ValueError("reference_selection_mode must be LEGACY or PRIORITY_FAILOVER")
        if not self.reference_priority or any(venue not in self.reference_venues for venue in self.reference_priority):
            raise ValueError("reference_priority must contain configured reference venues")
        if len(set(self.reference_priority)) != len(self.reference_priority):
            raise ValueError("reference_priority cannot contain duplicates")
        if self.reference_selection_mode == "PRIORITY_FAILOVER":
            if tuple(self.reference_priority) != ("binance", "bybit", "okx"):
                raise ValueError("PRIORITY_FAILOVER requires reference_priority binance, bybit, okx")
            if self.bitget_primary_enabled:
                raise ValueError("PRIORITY_FAILOVER does not allow Bitget as a primary reference")
        if self.recovery_min_healthy_seconds <= 0:
            raise ValueError("recovery_min_healthy_seconds must be positive")
        if self.minimum_reference_sources not in range(1, 5):
            raise ValueError("minimum_reference_sources must be between one and four")
        if self.minimum_reference_sources > len(self.reference_venues):
            raise ValueError("minimum_reference_sources cannot exceed configured reference venues")
        if self.reference_healthy_seconds <= 0 or self.reference_stale_seconds <= 0:
            raise ValueError("source freshness thresholds must be positive")
        if any(float(v) <= 0 for v in self.reference_stale_overrides.values()):
            raise ValueError("source stale overrides must be positive")
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
        if self.derive_trade_history_poll_seconds <= 0:
            raise ValueError("derive_trade_history_poll_seconds must be positive")
        if (
            self.storage_warning_free_gb <= self.storage_critical_free_gb
            or self.storage_critical_free_gb <= self.storage_emergency_free_gb
            or self.storage_emergency_free_gb <= 0
        ):
            raise ValueError("storage free-space thresholds must be warning > critical > emergency > 0")
        if self.raw_retention_seconds < 180:
            raise ValueError("raw_retention_seconds must be at least 180 seconds")
        if self.feature_persist_interval_seconds <= 0:
            raise ValueError("feature_persist_interval_seconds must be positive")
        if self.aggregate_interval_seconds <= 0 or self.chunk_rotation_minutes <= 0:
            raise ValueError("aggregate_interval_seconds and chunk_rotation_minutes must be positive")
        if self.governor_check_interval_seconds <= 0:
            raise ValueError("governor_check_interval_seconds must be positive")
        if (
            self.warning_run_storage_gb <= 0
            or self.warning_run_storage_gb > self.critical_run_storage_gb
            or self.critical_run_storage_gb > self.max_run_storage_gb
            or self.max_project_generated_data_gb <= 0
        ):
            raise ValueError(
                "storage budgets must satisfy 0 < warning_run_storage_gb <= critical_run_storage_gb <= max_run_storage_gb and max_project_generated_data_gb > 0"
            )
        if not self.binance_exchange_info_url.startswith("https://") or not self.binance_websocket_url.startswith("wss://"):
            raise ValueError("Binance endpoints must be secure public endpoints")
        for endpoint in (
            self.bybit_instruments_url,
            self.okx_instruments_url,
            self.bitget_contracts_url,
        ):
            if not endpoint.startswith("https://"):
                raise ValueError("reference REST endpoints must be secure public endpoints")
        for endpoint in (self.bybit_websocket_url, self.okx_websocket_url, self.bitget_websocket_url):
            if not endpoint.startswith("wss://"):
                raise ValueError("reference websocket endpoints must be secure public endpoints")
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
        if self.basis_ewma_alpha <= 0 or self.basis_ewma_alpha > 1:
            raise ValueError("basis_ewma_alpha must be in (0, 1]")
        if self.max_single_order_notional <= 0 or self.max_open_order_notional < self.max_single_order_notional:
            raise ValueError("open-order notional must cover one single order")
        if self.max_inventory_per_asset <= 0 or self.max_portfolio_inventory < self.max_inventory_per_asset:
            raise ValueError("portfolio inventory must cover one asset limit")
        if self.max_actions_per_minute < 1:
            raise ValueError("max_actions_per_minute must be positive")
        if self.refresh_tolerance_bps <= 0 or self.quote_max_age_seconds <= 0:
            raise ValueError("quote refresh thresholds must be positive")
        if self.refresh_deadband_bps is not None and self.refresh_deadband_bps < 0:
            raise ValueError("refresh_deadband_bps cannot be negative")
        if self.minimum_normal_quote_residency_seconds < 0:
            raise ValueError("minimum_normal_quote_residency_seconds cannot be negative")
        if self.fast_adverse_move_threshold_bps <= 0:
            raise ValueError("fast_adverse_move_threshold_bps must be positive")
        if self.max_order_actions_per_second <= 0:
            raise ValueError("max_order_actions_per_second must be positive")
        if self.max_order_actions_per_instrument_per_second <= 0:
            raise ValueError("max_order_actions_per_instrument_per_second must be positive")
        if self.target_action_utilization <= 0 or self.target_action_utilization > 1:
            raise ValueError("target_action_utilization must be in (0, 1]")
        if self.emergency_cancel_budget_per_minute < 1:
            raise ValueError("emergency_cancel_budget_per_minute must be positive")
        if self.high_vol_threshold_bps >= self.extreme_vol_threshold_bps:
            raise ValueError("high_vol_threshold_bps must be below extreme_vol_threshold_bps")

    @property
    def enabled_assets(self) -> tuple[AssetSpec, ...]:
        return tuple(asset for asset in self.assets if asset.enabled)

    @property
    def is_priority_failover(self) -> bool:
        return self.reference_selection_mode == "PRIORITY_FAILOVER"

    @property
    def primary_reference_control(self) -> str:
        if self.is_priority_failover:
            return "PRIORITY_FAILOVER"
        return "MULTI_SOURCE_CONSENSUS" if self.multi_reference else "BINANCE_ONLY_REFERENCE"

    @property
    def control_models(self) -> tuple[str, ...]:
        if self.is_priority_failover:
            return ("DERIVE_ONLY", "BINANCE_ONLY_NO_FAILOVER", "PRIORITY_FAILOVER")
        return ("DERIVE_ONLY", "BINANCE_ONLY_REFERENCE", "MULTI_SOURCE_CONSENSUS")

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
            "reference_execution": False,
            "reference_venues": list(self.reference_venues),
            "reference_selection_mode": self.reference_selection_mode,
            "reference_priority": list(self.reference_priority),
            "bitget_enabled": self.bitget_enabled,
            "bitget_primary_enabled": self.bitget_primary_enabled,
            "refresh_deadband_bps": self.refresh_deadband_bps,
            "minimum_normal_quote_residency_seconds": self.minimum_normal_quote_residency_seconds,
            "fast_adverse_move_override_enabled": self.fast_adverse_move_override_enabled,
            "fast_adverse_move_threshold_bps": self.fast_adverse_move_threshold_bps,
            "max_order_actions_per_second": self.max_order_actions_per_second,
            "max_order_actions_per_instrument_per_second": self.max_order_actions_per_instrument_per_second,
            "max_actions_per_minute": self.max_actions_per_minute,
            "target_action_utilization": self.target_action_utilization,
            "emergency_cancel_budget_per_minute": self.emergency_cancel_budget_per_minute,
            "max_run_storage_gb": self.max_run_storage_gb,
            "warning_run_storage_gb": self.warning_run_storage_gb,
            "critical_run_storage_gb": self.critical_run_storage_gb,
            "max_project_generated_data_gb": self.max_project_generated_data_gb,
            "rate_limit_status": self.rate_limit_status,
            "recovery_min_healthy_seconds": self.recovery_min_healthy_seconds,
            "credentials_loaded": False,
            "private_api_used": False,
        }

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["mode"] = self.mode.value
        result["quote_placement"] = self.quote_placement.value
        result["reference_venues"] = list(self.reference_venues)
        result["reference_priority"] = list(self.reference_priority)
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
            "refresh_deadband_bps",
            "minimum_normal_quote_residency_seconds",
            "fast_adverse_move_threshold_bps",
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
            "reference_outlier_bps",
            "reference_disagreement_pause_bps",
            "max_order_actions_per_second",
            "max_order_actions_per_instrument_per_second",
            "target_action_utilization",
        ):
            if result[key] is not None:
                result[key] = str(result[key])
        for key in ("report_dir", "log_dir", "database_path"):
            result[key] = str(result[key])
        return result
