"""Compact SQLite telemetry store for decisions, lifecycle, fills, and markouts."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import FillRecord, MarkoutRecord, json_safe


class TelemetryStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

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
            CREATE TABLE IF NOT EXISTS actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL,
                order_id TEXT,
                price TEXT,
                amount TEXT
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
            """
        )
        self.connection.commit()

    def set_state(self, key: str, value: Any, timestamp: float) -> None:
        self.connection.execute(
            "INSERT INTO state(key, value_json, updated_at) VALUES(?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
            (key, json.dumps(json_safe(value), sort_keys=True), timestamp),
        )
        self.connection.commit()

    def get_state(self, key: str) -> Any | None:
        row = self.connection.execute("SELECT value_json FROM state WHERE key = ?", (key,)).fetchone()
        return json.loads(row["value_json"]) if row else None

    def insert_decision(self, timestamp: float, asset: str, payload: Any) -> None:
        self.connection.execute(
            "INSERT INTO decisions(timestamp, asset, payload_json) VALUES (?, ?, ?)",
            (timestamp, asset, json.dumps(json_safe(payload), sort_keys=True)),
        )

    def insert_action(self, timestamp: float, asset: str, side: str, action: Any) -> None:
        self.connection.execute(
            "INSERT INTO actions(timestamp, asset, side, action, reason, order_id, price, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                timestamp,
                asset,
                side,
                action.kind,
                action.reason,
                action.order_id,
                str(action.price) if action.price is not None else None,
                str(action.amount),
            ),
        )

    def insert_fill(self, fill: FillRecord) -> None:
        self.connection.execute(
            "INSERT INTO fills VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            ),
        )

    def insert_markout(self, markout: MarkoutRecord) -> None:
        self.connection.execute(
            "INSERT INTO markouts VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            ),
        )

    def commit(self) -> None:
        self.connection.commit()

    def rows(self, table: str) -> list[dict[str, Any]]:
        if table not in {"decisions", "actions", "fills", "markouts", "state"}:
            raise ValueError("unsupported telemetry table")
        return [dict(row) for row in self.connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()]

    def count(self, table: str) -> int:
        if table not in {"decisions", "actions", "fills", "markouts"}:
            raise ValueError("unsupported telemetry table")
        row = self.connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        return int(row["count"])

    def close(self) -> None:
        self.connection.commit()
        self.connection.close()

    def __enter__(self) -> TelemetryStore:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
