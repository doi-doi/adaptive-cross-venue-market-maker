"""SQLite telemetry with bounded decision persistence and minute rollups."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from pathlib import Path
from typing import Any

from .models import FillRecord, MarkoutRecord, json_safe
from .storage import StorageGovernor, StorageMaintenanceWorker, StoragePolicy

_TELEMETRY_TABLES = {
    "decisions",
    "decision_rollups",
    "actions",
    "fills",
    "markouts",
    "state",
    "reference_health",
    "reference_values",
    "trades",
    "minute_aggregates",
}

_AGGREGATE_COLUMNS = (
    "timestamp_minute",
    "asset",
    "first_timestamp",
    "last_timestamp",
    "observation_count",
    "feature_snapshot_count",
    "derive_mid_min",
    "derive_mid_max",
    "derive_mid_sum",
    "derive_mid_median",
    "derive_spread_bps_min",
    "derive_spread_bps_max",
    "derive_spread_bps_sum",
    "derive_spread_bps_p90",
    "derive_spread_bps_median",
    "reference_fair_value_min",
    "reference_fair_value_max",
    "reference_fair_value_sum",
    "reference_fair_value_median",
    "basis_bps_min",
    "basis_bps_max",
    "basis_bps_sum",
    "basis_bps_median",
    "selected_reference_occupancy_json",
    "reference_observations",
    "reference_healthy_observations",
    "reference_health_counts_json",
    "reference_value_stats_json",
    "market_mode_occupancy_json",
    "direction_occupancy_json",
    "volatility_occupancy_json",
    "inventory_mode_occupancy_json",
    "action_counts_json",
    "model_metrics_json",
    "derive_trade_count",
    "derive_trade_notional",
    "conservative_fill_count",
    "touch_fill_count",
    "maker_volume",
    "creates",
    "replaces",
    "cancels",
    "holds",
    "blocks",
    "quote_observations",
    "quote_uptime_observations",
    "inventory_notional_sum",
    "inventory_notional_max",
    "pnl_proxy_first",
    "pnl_proxy_last",
    "pnl_proxy_delta",
    "error_count",
    "decision_detail_count",
    "decision_compressed_count",
    "raw_window_seconds",
)


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _json_text(value: Any) -> str:
    return json.dumps(json_safe(value), sort_keys=True, separators=(",", ":"))


def _increment(mapping: dict[str, int], key: Any, amount: int = 1) -> None:
    name = str(key or "UNSPECIFIED")
    mapping[name] = int(mapping.get(name, 0)) + amount


def _add_number(aggregate: dict[str, Any], key: str, value: Any) -> None:
    number = _number(value)
    if number is None:
        return
    aggregate[key + "_sum"] = float(aggregate.get(key + "_sum", 0.0)) + number
    minimum = aggregate.get(key + "_min")
    maximum = aggregate.get(key + "_max")
    aggregate[key + "_min"] = number if minimum is None else min(float(minimum), number)
    aggregate[key + "_max"] = number if maximum is None else max(float(maximum), number)


def _p90(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * 0.90
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _add_sample(aggregate: dict[str, Any], key: str, value: Any) -> None:
    number = _number(value)
    if number is None:
        return
    samples = aggregate[key]
    if len(samples) < 512:
        samples.append(number)
    else:
        samples[aggregate["observation_count"] % len(samples)] = number


class TelemetryStore:
    """Persist critical events and compact high-frequency observations.

    ``storage_config`` is optional for backwards compatibility with isolated
    tests and historical readers. A configured runner enables the governor;
    omitting it preserves the old full-detail behavior for raw-store callers.
    """

    def __init__(self, path: str | Path, *, storage_config: Any | None = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        policy = StoragePolicy.from_config(storage_config) if storage_config is not None else StoragePolicy()
        self.governor = StorageGovernor(self.path, policy if storage_config is not None else None)
        self._governor_enabled = storage_config is not None
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self._last_decision: dict[str, tuple[float, str]] = {}
        self._last_reference_health: dict[tuple[str, str], tuple[float, str]] = {}
        self._last_reference_value: dict[tuple[str, str], tuple[float, str]] = {}
        self._aggregate_cache: dict[tuple[int, str], dict[str, Any]] = {}
        self._last_aggregate_flush = float("-inf")
        self._last_governor_state_write = float("-inf")
        self._maintenance_worker = (
            StorageMaintenanceWorker(self.path, self.governor) if self._governor_enabled else None
        )
        self._create_schema()
        if self._maintenance_worker is not None:
            self._maintenance_worker.start()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                asset TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS decisions_asset_ts ON decisions(asset, timestamp);
            CREATE TABLE IF NOT EXISTS decision_rollups (
                timestamp_minute INTEGER NOT NULL,
                asset TEXT NOT NULL,
                decision_signature TEXT NOT NULL,
                first_timestamp REAL NOT NULL,
                last_timestamp REAL NOT NULL,
                count INTEGER NOT NULL,
                summary_json TEXT NOT NULL,
                PRIMARY KEY(timestamp_minute, asset, decision_signature)
            );
            CREATE INDEX IF NOT EXISTS decision_rollups_asset_ts ON decision_rollups(asset, timestamp_minute);
            CREATE TABLE IF NOT EXISTS actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL,
                order_id TEXT,
                price TEXT,
                amount TEXT,
                model TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                amount TEXT NOT NULL,
                fill_price TEXT NOT NULL,
                binance_fair_value TEXT NOT NULL,
                derive_mid TEXT NOT NULL,
                inventory_before TEXT NOT NULL,
                inventory_after TEXT NOT NULL,
                maker_fee_bps TEXT NOT NULL,
                market_mode TEXT NOT NULL,
                direction TEXT NOT NULL,
                basis_bps TEXT NOT NULL,
                quoted_edge_bps TEXT NOT NULL,
                model TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS markouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fill_timestamp REAL NOT NULL,
                horizon_seconds INTEGER NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                reference_price TEXT NOT NULL,
                derive_mid TEXT NOT NULL,
                binance_markout_bps TEXT NOT NULL,
                derive_markout_bps TEXT NOT NULL,
                model TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS reference_health (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                asset TEXT NOT NULL,
                venue TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS reference_health_asset_ts ON reference_health(asset, timestamp);
            CREATE TABLE IF NOT EXISTS reference_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                asset TEXT NOT NULL,
                venue TEXT NOT NULL,
                fair_value TEXT,
                mid TEXT,
                microprice TEXT,
                health TEXT NOT NULL,
                bbo_age TEXT,
                deviation_bps TEXT,
                valid INTEGER NOT NULL,
                excluded_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS reference_values_asset_ts ON reference_values(asset, timestamp);
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                asset TEXT NOT NULL,
                source TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                side TEXT NOT NULL,
                amount TEXT NOT NULL,
                price TEXT NOT NULL,
                exchange_timestamp REAL,
                UNIQUE(asset, source, trade_id)
            );
            CREATE INDEX IF NOT EXISTS trades_asset_ts ON trades(asset, timestamp);
            CREATE INDEX IF NOT EXISTS trades_asset_source_ts ON trades(asset, source, timestamp);
            CREATE TABLE IF NOT EXISTS minute_aggregates (
                timestamp_minute INTEGER NOT NULL,
                asset TEXT NOT NULL,
                first_timestamp REAL NOT NULL,
                last_timestamp REAL NOT NULL,
                observation_count INTEGER NOT NULL DEFAULT 0,
                feature_snapshot_count INTEGER NOT NULL DEFAULT 0,
                derive_mid_min REAL,
                derive_mid_max REAL,
                derive_mid_sum REAL NOT NULL DEFAULT 0,
                derive_mid_median REAL,
                derive_spread_bps_min REAL,
                derive_spread_bps_max REAL,
                derive_spread_bps_sum REAL NOT NULL DEFAULT 0,
                derive_spread_bps_p90 REAL,
                derive_spread_bps_median REAL,
                reference_fair_value_min REAL,
                reference_fair_value_max REAL,
                reference_fair_value_sum REAL NOT NULL DEFAULT 0,
                reference_fair_value_median REAL,
                basis_bps_min REAL,
                basis_bps_max REAL,
                basis_bps_sum REAL NOT NULL DEFAULT 0,
                basis_bps_median REAL,
                selected_reference_occupancy_json TEXT NOT NULL DEFAULT '{}',
                reference_observations INTEGER NOT NULL DEFAULT 0,
                reference_healthy_observations INTEGER NOT NULL DEFAULT 0,
                reference_health_counts_json TEXT NOT NULL DEFAULT '{}',
                reference_value_stats_json TEXT NOT NULL DEFAULT '{}',
                market_mode_occupancy_json TEXT NOT NULL DEFAULT '{}',
                direction_occupancy_json TEXT NOT NULL DEFAULT '{}',
                volatility_occupancy_json TEXT NOT NULL DEFAULT '{}',
                inventory_mode_occupancy_json TEXT NOT NULL DEFAULT '{}',
                action_counts_json TEXT NOT NULL DEFAULT '{}',
                model_metrics_json TEXT NOT NULL DEFAULT '{}',
                derive_trade_count INTEGER NOT NULL DEFAULT 0,
                derive_trade_notional REAL NOT NULL DEFAULT 0,
                conservative_fill_count INTEGER NOT NULL DEFAULT 0,
                touch_fill_count INTEGER NOT NULL DEFAULT 0,
                maker_volume REAL NOT NULL DEFAULT 0,
                creates INTEGER NOT NULL DEFAULT 0,
                replaces INTEGER NOT NULL DEFAULT 0,
                cancels INTEGER NOT NULL DEFAULT 0,
                holds INTEGER NOT NULL DEFAULT 0,
                blocks INTEGER NOT NULL DEFAULT 0,
                quote_observations INTEGER NOT NULL DEFAULT 0,
                quote_uptime_observations INTEGER NOT NULL DEFAULT 0,
                inventory_notional_sum REAL NOT NULL DEFAULT 0,
                inventory_notional_max REAL,
                pnl_proxy_first REAL,
                pnl_proxy_last REAL,
                pnl_proxy_delta REAL NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                decision_detail_count INTEGER NOT NULL DEFAULT 0,
                decision_compressed_count INTEGER NOT NULL DEFAULT 0,
                raw_window_seconds INTEGER NOT NULL DEFAULT 180,
                PRIMARY KEY(timestamp_minute, asset)
            );
            CREATE INDEX IF NOT EXISTS minute_aggregates_asset_ts ON minute_aggregates(asset, timestamp_minute);
            CREATE INDEX IF NOT EXISTS actions_asset_model_ts ON actions(asset, model, timestamp);
            CREATE INDEX IF NOT EXISTS fills_asset_model_ts ON fills(asset, model, timestamp);
            """
        )
        self._ensure_column("fills", "reference_control", "TEXT NOT NULL DEFAULT 'BINANCE_ONLY_REFERENCE'")
        self._ensure_column("markouts", "reference_control", "TEXT NOT NULL DEFAULT 'BINANCE_ONLY_REFERENCE'")
        self._ensure_column("actions", "model", "TEXT NOT NULL DEFAULT ''")
        for column, definition in (
            ("derive_mid_median", "REAL"),
            ("derive_spread_bps_median", "REAL"),
            ("reference_fair_value_median", "REAL"),
            ("basis_bps_median", "REAL"),
        ):
            self._ensure_column("minute_aggregates", column, definition)
        self.connection.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in self.connection.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _minute(self, timestamp: float) -> int:
        interval = max(1, self.governor.policy.aggregate_interval_seconds)
        return int(float(timestamp) // interval) * interval

    def _new_aggregate(self, timestamp_minute: int, asset: str, timestamp: float) -> dict[str, Any]:
        return {
            "timestamp_minute": timestamp_minute,
            "asset": asset,
            "first_timestamp": float(timestamp),
            "last_timestamp": float(timestamp),
            "observation_count": 0,
            "feature_snapshot_count": 0,
            "derive_mid_min": None,
            "derive_mid_max": None,
            "derive_mid_sum": 0.0,
            "derive_mid_median": None,
            "derive_mid_samples": [],
            "derive_spread_bps_min": None,
            "derive_spread_bps_max": None,
            "derive_spread_bps_sum": 0.0,
            "derive_spread_bps_p90": None,
            "derive_spread_bps_median": None,
            "derive_spread_samples": [],
            "derive_spread_bps_samples": [],
            "reference_fair_value_min": None,
            "reference_fair_value_max": None,
            "reference_fair_value_sum": 0.0,
            "reference_fair_value_median": None,
            "reference_fair_value_samples": [],
            "basis_bps_min": None,
            "basis_bps_max": None,
            "basis_bps_sum": 0.0,
            "basis_bps_median": None,
            "basis_bps_samples": [],
            "selected_reference_occupancy": {},
            "reference_observations": 0,
            "reference_healthy_observations": 0,
            "reference_health_counts": {},
            "reference_value_stats": {},
            "market_mode_occupancy": {},
            "direction_occupancy": {},
            "volatility_occupancy": {},
            "inventory_mode_occupancy": {},
            "action_counts": {},
            "model_metrics": {},
            "derive_trade_count": 0,
            "derive_trade_notional": 0.0,
            "conservative_fill_count": 0,
            "touch_fill_count": 0,
            "maker_volume": 0.0,
            "creates": 0,
            "replaces": 0,
            "cancels": 0,
            "holds": 0,
            "blocks": 0,
            "quote_observations": 0,
            "quote_uptime_observations": 0,
            "inventory_notional_sum": 0.0,
            "inventory_notional_max": None,
            "pnl_proxy_first": None,
            "pnl_proxy_last": None,
            "pnl_proxy_delta": 0.0,
            "error_count": 0,
            "decision_detail_count": 0,
            "decision_compressed_count": 0,
        }

    def _aggregate(self, timestamp: float, asset: str) -> dict[str, Any]:
        timestamp_minute = self._minute(timestamp)
        key = (timestamp_minute, asset)
        aggregate = self._aggregate_cache.get(key)
        if aggregate is None:
            row = self.connection.execute(
                "SELECT * FROM minute_aggregates WHERE timestamp_minute = ? AND asset = ?",
                (timestamp_minute, asset),
            ).fetchone()
            aggregate = self._new_aggregate(timestamp_minute, asset, timestamp)
            if row is not None:
                for column in _AGGREGATE_COLUMNS:
                    if column in aggregate:
                        aggregate[column] = row[column]
                for column, target in (
                    ("selected_reference_occupancy_json", "selected_reference_occupancy"),
                    ("reference_health_counts_json", "reference_health_counts"),
                    ("reference_value_stats_json", "reference_value_stats"),
                    ("market_mode_occupancy_json", "market_mode_occupancy"),
                    ("direction_occupancy_json", "direction_occupancy"),
                    ("volatility_occupancy_json", "volatility_occupancy"),
                    ("inventory_mode_occupancy_json", "inventory_mode_occupancy"),
                    ("action_counts_json", "action_counts"),
                    ("model_metrics_json", "model_metrics"),
                ):
                    try:
                        aggregate[target] = json.loads(row[column] or "{}")
                    except (TypeError, json.JSONDecodeError):
                        aggregate[target] = {}
                if row["derive_spread_bps_p90"] is not None:
                    aggregate["derive_spread_samples"] = [float(row["derive_spread_bps_p90"])]
                for column, sample_key in (
                    ("derive_mid_median", "derive_mid_samples"),
                    ("derive_spread_bps_median", "derive_spread_bps_samples"),
                    ("reference_fair_value_median", "reference_fair_value_samples"),
                    ("basis_bps_median", "basis_bps_samples"),
                ):
                    if column in row.keys() and row[column] is not None:
                        aggregate[sample_key] = [float(row[column])]
            self._aggregate_cache[key] = aggregate
        aggregate["first_timestamp"] = min(float(aggregate["first_timestamp"]), float(timestamp))
        aggregate["last_timestamp"] = max(float(aggregate["last_timestamp"]), float(timestamp))
        return aggregate

    def _decision_summary(self, payload: Any) -> dict[str, Any]:
        value = json_safe(payload)
        if not isinstance(value, dict):
            return {"value": value}
        summary: dict[str, Any] = {
            key: value.get(key)
            for key in (
                "reference_control",
                "selected_reference",
                "reference_selection_mode",
                "derive_bid",
                "derive_ask",
                "derive_mid",
                "derive_spread_bps",
                "reference_fair_value",
                "fair_value",
                "basis_bps",
                "reference_data_age_seconds",
                "derive_data_age_seconds",
                "market_mode",
                "direction",
                "volatility",
                "inventory_mode",
                "block_reason",
                "bid_reason",
                "ask_reason",
                "desired_bid",
                "desired_ask",
                "desired_bid_amount",
                "desired_ask_amount",
                "data_health",
                "divergence_protected",
                "fast_move_protected",
                "priority_event",
                "failover_event",
                "recovery_event",
                "reference_pause_reason",
                "selected_action",
            )
        }
        controls: dict[str, Any] = {}
        for control, control_value in _json_dict(value.get("controls")).items():
            if not isinstance(control_value, dict):
                continue
            plan = _json_dict(control_value.get("plan"))
            state = _json_dict(control_value.get("state"))
            consensus = _json_dict(control_value.get("consensus"))
            controls[control] = {
                "market_mode": state.get("market_mode"),
                "direction": state.get("direction"),
                "volatility": state.get("volatility"),
                "desired_bid": plan.get("bid_price"),
                "desired_ask": plan.get("ask_price"),
                "desired_bid_amount": plan.get("bid_amount"),
                "desired_ask_amount": plan.get("ask_amount"),
                "block_reason": plan.get("block_reason"),
                "pause_reason": consensus.get("pause_reason"),
            }
        summary["controls"] = controls
        return summary

    def _decision_signature(self, summary: dict[str, Any]) -> str:
        encoded = _json_text(summary).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _record_decision_rollup(
        self,
        timestamp: float,
        asset: str,
        signature: str,
        summary: dict[str, Any],
    ) -> None:
        minute = self._minute(timestamp)
        self.connection.execute(
            "INSERT INTO decision_rollups(timestamp_minute, asset, decision_signature, first_timestamp, last_timestamp, count, summary_json) "
            "VALUES (?, ?, ?, ?, ?, 1, ?) "
            "ON CONFLICT(timestamp_minute, asset, decision_signature) DO UPDATE SET "
            "last_timestamp=excluded.last_timestamp, count=decision_rollups.count + 1",
            (minute, asset, signature, timestamp, timestamp, _json_text(summary)),
        )

    def _record_decision_aggregate(self, timestamp: float, asset: str, payload: Any) -> tuple[str, bool]:
        value = json_safe(payload)
        value = value if isinstance(value, dict) else {}
        aggregate = self._aggregate(timestamp, asset)
        aggregate["observation_count"] += 1
        _add_number(aggregate, "derive_mid", value.get("derive_mid"))
        _add_sample(aggregate, "derive_mid_samples", value.get("derive_mid"))
        spread = _number(value.get("derive_spread_bps"))
        if spread is not None:
            _add_number(aggregate, "derive_spread_bps", spread)
            _add_sample(aggregate, "derive_spread_bps_samples", spread)
            samples = aggregate["derive_spread_samples"]
            if len(samples) < 512:
                samples.append(spread)
            else:
                index = aggregate["observation_count"] % len(samples)
                samples[index] = spread
            aggregate["derive_spread_bps_p90"] = _p90(samples)
        reference_fair_value = value.get("reference_fair_value", value.get("fair_value"))
        _add_number(aggregate, "reference_fair_value", reference_fair_value)
        _add_sample(aggregate, "reference_fair_value_samples", reference_fair_value)
        _add_number(aggregate, "basis_bps", value.get("basis_bps"))
        _add_sample(aggregate, "basis_bps_samples", value.get("basis_bps"))
        _increment(aggregate["selected_reference_occupancy"], value.get("selected_reference", "UNSELECTED"))
        _increment(aggregate["market_mode_occupancy"], value.get("market_mode", "UNSPECIFIED"))
        _increment(aggregate["direction_occupancy"], value.get("direction", "UNSPECIFIED"))
        _increment(aggregate["volatility_occupancy"], value.get("volatility", "UNSPECIFIED"))
        _increment(aggregate["inventory_mode_occupancy"], value.get("inventory_mode", "UNSPECIFIED"))
        inventory_notional = _number(value.get("position_notional"))
        if inventory_notional is not None:
            aggregate["inventory_notional_sum"] += inventory_notional
            current_max = aggregate["inventory_notional_max"]
            aggregate["inventory_notional_max"] = (
                abs(inventory_notional)
                if current_max is None
                else max(float(current_max), abs(inventory_notional))
            )
        if value.get("selected_reference"):
            aggregate["reference_observations"] += 1
        if value.get("data_health") in {"HEALTHY", "DEGRADED"}:
            aggregate["reference_healthy_observations"] += 1
        if value.get("block_reason") or value.get("reference_pause_reason"):
            aggregate["blocks"] += 1
        aggregate["quote_observations"] += 1
        quote_active = value.get("quote_active")
        if quote_active is None:
            quote_active = value.get("desired_bid") is not None or value.get("desired_ask") is not None
        if quote_active:
            aggregate["quote_uptime_observations"] += 1
        error_total = _number(value.get("error_count_total", value.get("error_count")))
        if error_total is not None:
            aggregate["error_count"] = max(int(aggregate["error_count"]), int(error_total))
        pnl_proxy = _number(value.get("pnl_proxy"))
        if pnl_proxy is not None:
            if aggregate["pnl_proxy_first"] is None:
                aggregate["pnl_proxy_first"] = pnl_proxy
            aggregate["pnl_proxy_last"] = pnl_proxy
            aggregate["pnl_proxy_delta"] = pnl_proxy - float(aggregate["pnl_proxy_first"])

        summary = self._decision_summary(value)
        signature = self._decision_signature(summary)
        previous = self._last_decision.get(asset)
        semantic_changed = previous is None or previous[1] != signature
        should_persist = (
            not self._governor_enabled
            or self.governor.should_persist_decision(
                timestamp,
                semantic_changed=semantic_changed,
                last_persisted=previous[0] if previous else None,
            )
        )
        self._record_decision_rollup(timestamp, asset, signature, summary)
        if should_persist:
            self.connection.execute(
                "INSERT INTO decisions(timestamp, asset, payload_json) VALUES (?, ?, ?)",
                (timestamp, asset, _json_text(value)),
            )
            aggregate["feature_snapshot_count"] += 1
            aggregate["decision_detail_count"] += 1
            self._last_decision[asset] = (timestamp, signature)
        else:
            aggregate["decision_compressed_count"] += 1
        return signature, semantic_changed

    def _model_metrics(self, aggregate: dict[str, Any], model: str) -> dict[str, Any]:
        return aggregate["model_metrics"].setdefault(
            model,
            {
                "action_rows": 0,
                "creates": 0,
                "replaces": 0,
                "cancels": 0,
                "holds": 0,
                "blocks": 0,
                "fills": 0,
                "maker_volume": 0.0,
            },
        )

    def _record_action_aggregate(self, timestamp: float, asset: str, action: Any, model: str) -> None:
        aggregate = self._aggregate(timestamp, asset)
        kind = str(action.kind)
        reason = str(action.reason)
        _increment(aggregate["action_counts"], kind)
        metrics = self._model_metrics(aggregate, model)
        metrics["action_rows"] = int(metrics.get("action_rows", 0)) + 1
        normalized = kind.lower() + "s"
        if normalized in {"creates", "cancels", "holds"}:
            metrics[normalized] += 1
        if kind == "CREATE":
            aggregate["creates"] += 1
        elif kind == "CANCEL":
            aggregate["cancels"] += 1
        elif kind == "HOLD":
            aggregate["holds"] += 1
        if kind == "REPLACE":
            aggregate["replaces"] += 1
            metrics["replaces"] += 1
        elif kind == "CANCEL" and reason == "REFRESH_NEEDED":
            aggregate["replaces"] += 1
            metrics["replaces"] += 1

    def _reference_stats(self, aggregate: dict[str, Any], venue: str) -> dict[str, Any]:
        return aggregate["reference_value_stats"].setdefault(
            venue,
            {
                "observations": 0,
                "valid": 0,
                "fair_sum": 0.0,
                "fair_min": None,
                "fair_max": None,
                "mid_sum": 0.0,
                "mid_min": None,
                "mid_max": None,
            },
        )

    def _record_reference_health_aggregate(self, timestamp: float, asset: str, venue: str, payload: Any) -> tuple[str, bool]:
        value = json_safe(payload)
        value = value if isinstance(value, dict) else {}
        health = str(value.get("health", value.get("status", "UNOBSERVED")))
        aggregate = self._aggregate(timestamp, asset)
        aggregate["reference_observations"] += 1
        if health in {"HEALTHY", "DEGRADED"}:
            aggregate["reference_healthy_observations"] += 1
        _increment(aggregate["reference_health_counts"], f"{venue}:{health}")
        previous = self._last_reference_health.get((asset, venue))
        signature = _json_text({"health": health, "connected": value.get("connected")})
        changed = previous is None or previous[1] != signature
        return signature, changed

    def _record_reference_value_aggregate(
        self,
        timestamp: float,
        asset: str,
        venue: str,
        fair_value: Any,
        mid: Any,
        valid: bool,
    ) -> tuple[str, bool]:
        aggregate = self._aggregate(timestamp, asset)
        stats = self._reference_stats(aggregate, venue)
        stats["observations"] += 1
        stats["valid"] += int(valid)
        for field, value in (("fair", fair_value), ("mid", mid)):
            number = _number(value)
            if number is None:
                continue
            stats[field + "_sum"] += number
            minimum = stats[field + "_min"]
            maximum = stats[field + "_max"]
            stats[field + "_min"] = number if minimum is None else min(float(minimum), number)
            stats[field + "_max"] = number if maximum is None else max(float(maximum), number)
        signature = _json_text({"fair_value": fair_value, "mid": mid, "valid": bool(valid)})
        previous = self._last_reference_value.get((asset, venue))
        return signature, previous is None or previous[1] != signature

    def set_state(self, key: str, value: Any, timestamp: float) -> None:
        self.connection.execute(
            "INSERT INTO state(key, value_json, updated_at) VALUES(?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
            (key, _json_text(value), timestamp),
        )
        self.commit()

    def get_state(self, key: str) -> Any | None:
        row = self.connection.execute("SELECT value_json FROM state WHERE key = ?", (key,)).fetchone()
        return json.loads(row["value_json"]) if row else None

    def insert_decision(self, timestamp: float, asset: str, payload: Any) -> None:
        self._record_decision_aggregate(timestamp, asset, payload)

    def insert_action(self, timestamp: float, asset: str, side: str, action: Any, model: str = "") -> None:
        self._record_action_aggregate(timestamp, asset, action, model)
        if str(action.kind) == "HOLD" and self._governor_enabled:
            return
        self.connection.execute(
            "INSERT INTO actions(timestamp, asset, side, action, reason, order_id, price, amount, model) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                timestamp,
                asset,
                side,
                action.kind,
                action.reason,
                action.order_id,
                str(action.price) if action.price is not None else None,
                str(action.amount),
                model,
            ),
        )

    def insert_fill(self, fill: FillRecord) -> None:
        self.connection.execute(
            "INSERT INTO fills(timestamp, asset, side, amount, fill_price, binance_fair_value, derive_mid, inventory_before, inventory_after, maker_fee_bps, market_mode, direction, basis_bps, quoted_edge_bps, model, reference_control) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                fill.timestamp,
                fill.asset,
                fill.side.value,
                str(fill.amount),
                str(fill.fill_price),
                str(fill.binance_fair_value),
                str(fill.derive_mid),
                str(fill.inventory_before),
                str(fill.inventory_after),
                str(fill.maker_fee_bps),
                fill.market_mode.value,
                fill.direction.value,
                str(fill.basis_bps),
                str(fill.quoted_edge_bps),
                fill.model,
                fill.reference_control,
            ),
        )
        aggregate = self._aggregate(fill.timestamp, fill.asset)
        notional = (_number(fill.amount) or 0.0) * (_number(fill.fill_price) or 0.0)
        if str(fill.model).endswith(":CONSERVATIVE"):
            aggregate["conservative_fill_count"] += 1
        elif str(fill.model).endswith(":TOUCH_SENSITIVITY"):
            aggregate["touch_fill_count"] += 1
        aggregate["maker_volume"] += notional
        metrics = self._model_metrics(aggregate, fill.model)
        metrics["fills"] += 1
        metrics["maker_volume"] += notional

    def insert_markout(self, markout: MarkoutRecord) -> None:
        self.connection.execute(
            "INSERT INTO markouts(fill_timestamp, horizon_seconds, asset, side, reference_price, derive_mid, binance_markout_bps, derive_markout_bps, model, reference_control) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                markout.fill_timestamp,
                markout.horizon_seconds,
                markout.asset,
                markout.side.value,
                str(markout.reference_price),
                str(markout.derive_mid),
                str(markout.binance_markout_bps),
                str(markout.derive_markout_bps),
                markout.model,
                markout.reference_control,
            ),
        )

    def insert_reference_health(self, timestamp: float, asset: str, venue: str, payload: Any) -> None:
        signature, changed = self._record_reference_health_aggregate(timestamp, asset, venue, payload)
        previous = self._last_reference_health.get((asset, venue))
        should_persist = (
            not self._governor_enabled
            or self.governor.should_persist_reference_detail(
                timestamp,
                changed=changed,
                last_persisted=previous[0] if previous else None,
            )
        )
        if should_persist:
            self.connection.execute(
                "INSERT INTO reference_health(timestamp, asset, venue, payload_json) VALUES (?, ?, ?, ?)",
                (timestamp, asset, venue, _json_text(payload)),
            )
            self._last_reference_health[(asset, venue)] = (timestamp, signature)

    def insert_reference_value(
        self,
        *,
        timestamp: float,
        asset: str,
        venue: str,
        fair_value: Any,
        mid: Any,
        microprice: Any,
        health: str,
        bbo_age: Any,
        deviation_bps: Any,
        valid: bool,
        excluded_reason: str = "",
    ) -> None:
        signature, changed = self._record_reference_value_aggregate(
            timestamp, asset, venue, fair_value, mid, valid
        )
        previous = self._last_reference_value.get((asset, venue))
        should_persist = (
            not self._governor_enabled
            or self.governor.should_persist_reference_detail(
                timestamp,
                changed=changed,
                last_persisted=previous[0] if previous else None,
            )
        )
        if should_persist:
            self.connection.execute(
                "INSERT INTO reference_values(timestamp, asset, venue, fair_value, mid, microprice, health, bbo_age, deviation_bps, valid, excluded_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    timestamp,
                    asset,
                    venue,
                    None if fair_value is None else str(fair_value),
                    None if mid is None else str(mid),
                    None if microprice is None else str(microprice),
                    health,
                    None if bbo_age is None else str(bbo_age),
                    None if deviation_bps is None else str(deviation_bps),
                    int(valid),
                    excluded_reason,
                ),
            )
            self._last_reference_value[(asset, venue)] = (timestamp, signature)

    def insert_trade(self, trade: Any, asset: str) -> bool:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO trades(timestamp, asset, source, trade_id, side, amount, price, exchange_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                trade.timestamp,
                asset,
                trade.source,
                str(trade.trade_id),
                trade.side.value,
                str(trade.amount),
                str(trade.price),
                trade.exchange_timestamp,
            ),
        )
        inserted = cursor.rowcount == 1
        if inserted and str(trade.source) == "derive":
            aggregate = self._aggregate(trade.timestamp, asset)
            aggregate["derive_trade_count"] += 1
            aggregate["derive_trade_notional"] += (_number(trade.amount) or 0.0) * (_number(trade.price) or 0.0)
        return inserted

    def _aggregate_values(self, aggregate: dict[str, Any]) -> tuple[Any, ...]:
        values = dict(aggregate)
        values["derive_mid_median"] = _median(aggregate["derive_mid_samples"])
        values["derive_spread_bps_p90"] = _p90(aggregate["derive_spread_samples"])
        values["derive_spread_bps_median"] = _median(aggregate["derive_spread_bps_samples"])
        values["reference_fair_value_median"] = _median(aggregate["reference_fair_value_samples"])
        values["basis_bps_median"] = _median(aggregate["basis_bps_samples"])
        values["selected_reference_occupancy_json"] = _json_text(aggregate["selected_reference_occupancy"])
        values["reference_health_counts_json"] = _json_text(aggregate["reference_health_counts"])
        values["reference_value_stats_json"] = _json_text(aggregate["reference_value_stats"])
        values["market_mode_occupancy_json"] = _json_text(aggregate["market_mode_occupancy"])
        values["direction_occupancy_json"] = _json_text(aggregate["direction_occupancy"])
        values["volatility_occupancy_json"] = _json_text(aggregate["volatility_occupancy"])
        values["inventory_mode_occupancy_json"] = _json_text(aggregate["inventory_mode_occupancy"])
        values["action_counts_json"] = _json_text(aggregate["action_counts"])
        values["model_metrics_json"] = _json_text(aggregate["model_metrics"])
        values["raw_window_seconds"] = self.governor.policy.raw_retention_seconds
        return tuple(values[column] for column in _AGGREGATE_COLUMNS)

    def _flush_aggregates(self, *, force: bool = False) -> None:
        if not self._aggregate_cache:
            return
        now = time.time()
        if not force and now - self._last_aggregate_flush < self.governor.policy.aggregate_interval_seconds:
            return
        placeholders = ", ".join("?" for _ in _AGGREGATE_COLUMNS)
        columns = ", ".join(_AGGREGATE_COLUMNS)
        for aggregate in self._aggregate_cache.values():
            self.connection.execute(
                f"INSERT OR REPLACE INTO minute_aggregates({columns}) VALUES ({placeholders})",
                self._aggregate_values(aggregate),
            )
        self._last_aggregate_flush = now
        if not force:
            current_minute = self._minute(now)
            keep_after = current_minute - max(
                2,
                math.ceil(self.governor.policy.raw_retention_seconds / self.governor.policy.aggregate_interval_seconds),
            ) * self.governor.policy.aggregate_interval_seconds
            self._aggregate_cache = {
                key: value
                for key, value in self._aggregate_cache.items()
                if key[0] >= keep_after
            }
        else:
            self._aggregate_cache.clear()

    def _write_governor_state(self, now: float) -> None:
        if not self._governor_enabled or now - self._last_governor_state_write < 30.0:
            return
        self.connection.execute(
            "INSERT INTO state(key, value_json, updated_at) VALUES(?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
            ("storage_governor", _json_text(self.governor.snapshot(now)), now),
        )
        self._last_governor_state_write = now

    def commit(self) -> None:
        now = time.time()
        if self._governor_enabled:
            self.governor.refresh(now)
            self._write_governor_state(now)
        self._flush_aggregates()
        self.connection.commit()

    def checkpoint_passive(self) -> tuple[int, int, int]:
        """Run a non-blocking checkpoint; never truncates a live WAL."""

        row = self.connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        return tuple(int(value) for value in row) if row else (0, 0, 0)

    def storage_snapshot(self) -> dict[str, Any]:
        snapshot = self.governor.snapshot(time.time())
        snapshot.update(
            {
                "decision_detail_rows": self.count("decisions"),
                "decision_rollup_rows": self.count("decision_rollups"),
                "minute_aggregate_rows": self.count("minute_aggregates"),
            }
        )
        return snapshot

    def rows(self, table: str) -> list[dict[str, Any]]:
        if table not in _TELEMETRY_TABLES:
            raise ValueError("unsupported telemetry table")
        if table == "minute_aggregates":
            self._flush_aggregates(force=True)
        return [dict(row) for row in self.connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()]

    def count(self, table: str) -> int:
        if table not in _TELEMETRY_TABLES:
            raise ValueError("unsupported telemetry table")
        row = self.connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        return int(row["count"])

    def close(self) -> None:
        if self._maintenance_worker is not None:
            self._maintenance_worker.close()
        self._flush_aggregates(force=True)
        self.connection.commit()
        try:
            self.checkpoint_passive()
        except sqlite3.DatabaseError:
            pass
        self.connection.close()

    def __enter__(self) -> TelemetryStore:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
