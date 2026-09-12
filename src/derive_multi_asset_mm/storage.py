"""Storage policy and low-overhead disk governor for shadow telemetry.

The governor changes persistence volume only.  It never changes quote inputs,
market-state calculations, order sizing, or lifecycle decisions.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class StorageLevel(StrEnum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


@dataclass(frozen=True)
class StoragePolicy:
    """Persistence policy expressed in the same units as the YAML config."""

    warning_free_gb: float = 15.0
    critical_free_gb: float = 10.0
    emergency_free_gb: float = 5.0
    raw_retention_seconds: int = 180
    feature_persist_interval_seconds: float = 1.0
    aggregate_interval_seconds: int = 60
    chunk_rotation_minutes: int = 10
    governor_check_interval_seconds: float = 5.0
    warning_run_storage_gb: float = 2.0
    critical_run_storage_gb: float = 2.5
    max_run_storage_gb: float = 3.0
    max_project_generated_data_gb: float = 10.0

    def __post_init__(self) -> None:
        if (
            self.warning_run_storage_gb <= 0
            or self.warning_run_storage_gb > self.critical_run_storage_gb
            or self.critical_run_storage_gb > self.max_run_storage_gb
            or self.max_project_generated_data_gb <= 0
        ):
            raise ValueError(
                "storage budgets must satisfy 0 < warning_run_storage_gb <= "
                "critical_run_storage_gb <= max_run_storage_gb and "
                "max_project_generated_data_gb > 0"
            )

    @classmethod
    def from_config(cls, config: Any) -> StoragePolicy:
        return cls(
            warning_free_gb=float(getattr(config, "storage_warning_free_gb", 15.0)),
            critical_free_gb=float(getattr(config, "storage_critical_free_gb", 10.0)),
            emergency_free_gb=float(getattr(config, "storage_emergency_free_gb", 5.0)),
            raw_retention_seconds=int(getattr(config, "raw_retention_seconds", 180)),
            feature_persist_interval_seconds=float(
                getattr(config, "feature_persist_interval_seconds", 1.0)
            ),
            aggregate_interval_seconds=int(getattr(config, "aggregate_interval_seconds", 60)),
            chunk_rotation_minutes=int(getattr(config, "chunk_rotation_minutes", 10)),
            governor_check_interval_seconds=float(
                getattr(config, "governor_check_interval_seconds", 5.0)
            ),
            warning_run_storage_gb=float(getattr(config, "warning_run_storage_gb", 2.0)),
            critical_run_storage_gb=float(getattr(config, "critical_run_storage_gb", 2.5)),
            max_run_storage_gb=float(getattr(config, "max_run_storage_gb", 3.0)),
            max_project_generated_data_gb=float(
                getattr(config, "max_project_generated_data_gb", 10.0)
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "warning_free_gb": self.warning_free_gb,
            "critical_free_gb": self.critical_free_gb,
            "emergency_free_gb": self.emergency_free_gb,
            "raw_retention_seconds": self.raw_retention_seconds,
            "feature_persist_interval_seconds": self.feature_persist_interval_seconds,
            "aggregate_interval_seconds": self.aggregate_interval_seconds,
            "chunk_rotation_minutes": self.chunk_rotation_minutes,
            "governor_check_interval_seconds": self.governor_check_interval_seconds,
            "warning_run_storage_gb": self.warning_run_storage_gb,
            "critical_run_storage_gb": self.critical_run_storage_gb,
            "max_run_storage_gb": self.max_run_storage_gb,
            "max_project_generated_data_gb": self.max_project_generated_data_gb,
        }


_STORAGE_LEVEL_RANK = {
    StorageLevel.NORMAL: 0,
    StorageLevel.WARNING: 1,
    StorageLevel.CRITICAL: 2,
    StorageLevel.EMERGENCY: 3,
}


def _file_size(path: Path) -> int:
    """Return a regular file's logical size without following symlinks."""

    try:
        if path.is_symlink() or not path.is_file():
            return 0
        return int(path.stat().st_size)
    except OSError:
        return 0


def _tree_size(root: Path) -> int:
    """Sum regular files below ``root`` without crossing symlinked folders."""

    if root.is_symlink() or not root.is_dir():
        return 0
    total = 0
    try:
        for directory, directories, filenames in os.walk(root, followlinks=False):
            directory_path = Path(directory)
            directories[:] = [
                name
                for name in directories
                if not (directory_path / name).is_symlink()
            ]
            for name in filenames:
                total += _file_size(directory_path / name)
    except OSError:
        # A concurrently rotated/removed file is not a reason to interrupt the
        # strategy loop. The next throttled measurement will retry it.
        return total
    return total


class StorageGovernor:
    """Cache disk pressure and select persistence behavior.

    Disk checks are throttled so they do not run once per strategy cycle.  At
    CRITICAL/EMERGENCY levels, unchanged feature detail is suppressed while
    critical event tables and one-minute aggregates continue to be written.
    """

    def __init__(self, path: str | Path, policy: StoragePolicy | None = None) -> None:
        self.path = Path(path)
        self.policy = policy or StoragePolicy()
        self._last_check = float("-inf")
        self._free_gb = 0.0
        self._level = StorageLevel.NORMAL
        self._last_event: str | None = None
        self._last_pruned_rows = 0
        self._total_pruned_rows = 0
        self._run_storage_gb = 0.0
        self._project_generated_data_gb = 0.0
        self._last_project_size_check = float("-inf")

    @property
    def free_gb(self) -> float:
        return self._free_gb

    @property
    def level_name(self) -> str:
        return self._level.value

    @property
    def current_run_storage_gb(self) -> float:
        return self._run_storage_gb

    @property
    def project_generated_data_gb(self) -> float:
        return self._project_generated_data_gb

    def _run_storage_bytes(self) -> int:
        """Measure the SQLite file and sidecars owned by this run."""

        return sum(
            _file_size(candidate)
            for candidate in (
                self.path,
                Path(f"{self.path}-wal"),
                Path(f"{self.path}-shm"),
                Path(f"{self.path}-journal"),
            )
        )

    def _project_root(self) -> Path:
        """Find the project root from a conventional ``<project>/logs/...`` path."""

        for candidate in (self.path.parent, *self.path.parents):
            if candidate.name == "logs":
                return candidate.parent
        return self.path.parent

    def _project_generated_bytes(self) -> int:
        """Measure generated logs/reports only; source and configuration are excluded."""

        root = self._project_root()
        return _tree_size(root / "logs") + _tree_size(root / "reports")

    def _budget_level(self) -> tuple[StorageLevel, str | None]:
        if self._run_storage_gb >= self.policy.max_run_storage_gb:
            return StorageLevel.EMERGENCY, "STORAGE_RUN_MAX_BUDGET"
        if self._project_generated_data_gb >= self.policy.max_project_generated_data_gb:
            return StorageLevel.CRITICAL, "STORAGE_PROJECT_MAX_BUDGET"
        if self._run_storage_gb >= self.policy.critical_run_storage_gb:
            return StorageLevel.CRITICAL, "STORAGE_RUN_CRITICAL_BUDGET"
        if self._run_storage_gb >= self.policy.warning_run_storage_gb:
            return StorageLevel.WARNING, "STORAGE_RUN_WARNING_BUDGET"
        return StorageLevel.NORMAL, None

    def refresh(self, now: float | None = None, *, force: bool = False) -> StorageLevel:
        timestamp = time.time() if now is None else float(now)
        if not force and timestamp - self._last_check < self.policy.governor_check_interval_seconds:
            return self._level
        previous_level = self._level
        base_level = StorageLevel.NORMAL
        try:
            self._free_gb = shutil.disk_usage(self.path.parent).free / (1024**3)
        except OSError:
            # Unknown capacity is safer as NORMAL: no optional pruning is
            # triggered and the caller still keeps critical telemetry.
            self._free_gb = 0.0
            base_level = StorageLevel.NORMAL
        else:
            if self._free_gb < self.policy.emergency_free_gb:
                base_level = StorageLevel.EMERGENCY
            elif self._free_gb < self.policy.critical_free_gb:
                base_level = StorageLevel.CRITICAL
            elif self._free_gb < self.policy.warning_free_gb:
                base_level = StorageLevel.WARNING
            else:
                base_level = StorageLevel.NORMAL

        self._run_storage_gb = self._run_storage_bytes() / (1024**3)
        if force or timestamp - self._last_project_size_check >= max(
            30.0, self.policy.governor_check_interval_seconds
        ):
            self._project_generated_data_gb = self._project_generated_bytes() / (1024**3)
            self._last_project_size_check = timestamp

        budget_level, budget_event = self._budget_level()
        self._level = (
            budget_level
            if _STORAGE_LEVEL_RANK[budget_level] > _STORAGE_LEVEL_RANK[base_level]
            else base_level
        )
        if self._level != previous_level:
            self._last_event = (
                budget_event
                if self._level == budget_level and budget_event
                else f"STORAGE_{self._level.value}"
            )
        self._last_check = timestamp
        return self._level

    def record_pruned(self, rows: int) -> None:
        self._last_pruned_rows = int(rows)
        self._total_pruned_rows += int(rows)

    def snapshot(self, now: float | None = None) -> dict[str, Any]:
        self.refresh(now)
        return {
            "level": self.level_name,
            "free_gb": round(self.free_gb, 6),
            "current_run_storage_gb": round(self.current_run_storage_gb, 6),
            "project_generated_data_gb": round(self.project_generated_data_gb, 6),
            "policy": self.policy.as_dict(),
            "governor_enabled": True,
            "event": self._last_event,
            "last_pruned_rows": self._last_pruned_rows,
            "total_pruned_rows": self._total_pruned_rows,
        }

    def should_persist_decision(
        self,
        timestamp: float,
        *,
        semantic_changed: bool,
        last_persisted: float | None,
    ) -> bool:
        level = self.refresh(timestamp)
        if last_persisted is None or semantic_changed:
            return True
        if level in {StorageLevel.CRITICAL, StorageLevel.EMERGENCY}:
            return False
        return timestamp - last_persisted >= self.policy.feature_persist_interval_seconds

    def should_persist_reference_detail(
        self,
        timestamp: float,
        *,
        changed: bool,
        last_persisted: float | None,
    ) -> bool:
        level = self.refresh(timestamp)
        if last_persisted is None or changed:
            return True
        if level in {StorageLevel.CRITICAL, StorageLevel.EMERGENCY}:
            # Keep a sparse health continuity sample even under pressure;
            # minute aggregates retain the complete occupancy counters.
            return timestamp - last_persisted >= self.policy.aggregate_interval_seconds
        return timestamp - last_persisted >= self.policy.feature_persist_interval_seconds


def _delete_batch(
    connection: sqlite3.Connection,
    table: str,
    timestamp_column: str,
    cutoff: float,
    batch_size: int,
    predicate: str = "",
    parameters: tuple[Any, ...] = (),
) -> int:
    query = (
        f'DELETE FROM "{table}" WHERE rowid IN ('
        f'SELECT rowid FROM "{table}" WHERE "{timestamp_column}" < ? {predicate} '
        "ORDER BY rowid LIMIT ?"
        ")"
    )
    cursor = connection.execute(query, (cutoff, *parameters, batch_size))
    return int(cursor.rowcount if cursor.rowcount != -1 else 0)


def prune_expired_raw_rows(path: str | Path, cutoff: float, *, batch_size: int = 500) -> int:
    """Batch-prune expired noncritical raw detail without VACUUM or table locks.

    Permanent event/fill/markout/trade tables are intentionally excluded. Decision
    rows carrying protection or failover semantics are retained; their compact
    rollups and minute aggregates are permanent as well.
    """

    database = Path(path)
    if not database.exists():
        return 0
    connection = sqlite3.connect(database, timeout=0.25)
    try:
        connection.execute("PRAGMA busy_timeout=250")
        total = 0
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        # The permanent minute rollup is the safety gate for deleting raw
        # detail. Legacy/full-detail stores are never pruned by this worker.
        if "minute_aggregates" not in tables:
            return 0
        if "decisions" in tables:
            total += _delete_batch(
                connection,
                "decisions",
                "timestamp",
                cutoff,
                batch_size,
                predicate=(
                    "AND COALESCE(json_extract(payload_json, '$.failover_event'), '') = '' "
                    "AND COALESCE(json_extract(payload_json, '$.recovery_event'), '') = '' "
                    "AND COALESCE(json_extract(payload_json, '$.priority_event'), '') = '' "
                    "AND COALESCE(json_extract(payload_json, '$.reference_pause_reason'), '') = '' "
                    "AND COALESCE(json_extract(payload_json, '$.block_reason'), '') = '' "
                    "AND COALESCE(json_extract(payload_json, '$.fast_move_protected'), 0) = 0"
                ),
            )
        for table, timestamp_column in (
            ("reference_health", "timestamp"),
            ("reference_values", "timestamp"),
        ):
            if table in tables:
                total += _delete_batch(connection, table, timestamp_column, cutoff, batch_size)
        if total:
            connection.commit()
        return total
    finally:
        connection.close()


class StorageMaintenanceWorker:
    """Low-priority, bounded raw-detail pruning for a configured telemetry DB."""

    def __init__(self, path: str | Path, governor: StorageGovernor) -> None:
        self.path = Path(path)
        self.governor = governor
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="telemetry-storage-maintenance",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        interval = max(5.0, min(60.0, self.governor.policy.governor_check_interval_seconds))
        while not self._stop.wait(interval):
            now = time.time()
            self.governor.refresh(now)
            try:
                pruned = prune_expired_raw_rows(
                    self.path,
                    now - self.governor.policy.raw_retention_seconds,
                )
            except (OSError, sqlite3.DatabaseError, json.JSONDecodeError):
                # A busy or partially initialized DB is retried on the next tick;
                # critical telemetry is never deleted by an uncertain operation.
                continue
            self.governor.record_pruned(pruned)
