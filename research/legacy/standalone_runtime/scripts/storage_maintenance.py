#!/usr/bin/env python3
"""Audit and safely archive shadow-run storage.

The command is deliberately conservative. It creates a before-manifest before
any mutation, treats open/incomplete files as KEEP, checkpoints only closed
SQLite databases, verifies the source schema/counts and Zstandard frame, and
removes a source only after a verified archive exists. With ``--watch`` it can
wait for a named shadow run to finish and archive that run after final reports
are present.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

FINAL_REPORT_MARKERS = (
    "final_report.json",
    "final_6h_validation_report.json",
    "final_multi_asset_shadow_report.json",
    "final_multi_reference_shadow_report.json",
)
PROTECTED_SUFFIXES = (
    ".sqlite-wal",
    ".sqlite-shm",
    ".wal",
    ".journal",
    ".lock",
    ".tmp",
)
TELEMETRY_TABLES = (
    "state",
    "decisions",
    "decision_rollups",
    "actions",
    "fills",
    "markouts",
    "reference_health",
    "reference_values",
    "trades",
    "minute_aggregates",
)


def _utc(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(float(timestamp), UTC).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _run(command: list[str], *, timeout: float = 10.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)


def _processes_for_root(root: Path) -> list[dict[str, Any]]:
    result = _run(["ps", "-axo", "pid=,command="], timeout=5.0)
    processes: list[dict[str, Any]] = []
    root_text = str(root.resolve())
    for line in result.stdout.splitlines():
        text = line.strip()
        if not text or root_text not in text:
            continue
        try:
            pid_text, command = text.split(None, 1)
            pid = int(pid_text)
        except (ValueError, IndexError):
            continue
        run_id = None
        if " --run-id " in f" {command} ":
            run_id = command.split(" --run-id ", 1)[1].split()[0]
        processes.append({"pid": pid, "command": command, "run_id": run_id})
    return processes


def _active_processes(root: Path) -> list[dict[str, Any]]:
    return [
        process
        for process in _processes_for_root(root)
        if "derive_multi_asset_mm.shadow" in str(process["command"])
    ]


def _open_paths(processes: list[dict[str, Any]]) -> dict[str, list[int]]:
    if not processes:
        return {}
    pids = ",".join(str(process["pid"]) for process in processes)
    result = _run(["lsof", "-Fn", "-p", pids], timeout=10.0)
    current_pid: int | None = None
    paths: dict[str, list[int]] = {}
    for line in result.stdout.splitlines():
        if line.startswith("p"):
            try:
                current_pid = int(line[1:])
            except ValueError:
                current_pid = None
        elif line.startswith("n") and current_pid is not None:
            value = line[1:]
            if value.endswith(" (deleted)"):
                value = value.removesuffix(" (deleted)")
            paths.setdefault(str(Path(value).resolve()), []).append(current_pid)
    return paths


def _state_for_log_dir(log_dir: Path) -> dict[str, Any]:
    return _read_json(log_dir / "state.json")


def _report_dir(root: Path, log_dir: Path) -> Path | None:
    logs_root = root / "logs"
    try:
        relative = log_dir.relative_to(logs_root)
    except ValueError:
        return None
    return root / "reports" / relative


def _has_final_report(report_dir: Path | None) -> bool:
    return bool(report_dir and any((report_dir / marker).is_file() for marker in FINAL_REPORT_MARKERS))


def _run_context(root: Path, path: Path) -> tuple[Path | None, Path | None, dict[str, Any]]:
    logs_root = root / "logs"
    reports_root = root / "reports"
    if path.is_relative_to(logs_root):
        log_dir = path.parent
        return log_dir, _report_dir(root, log_dir), _state_for_log_dir(log_dir)
    if path.is_relative_to(reports_root):
        report_dir = path.parent
        try:
            log_dir = logs_root / report_dir.relative_to(reports_root)
        except ValueError:
            log_dir = None
        return log_dir, report_dir, _state_for_log_dir(log_dir) if log_dir else {}
    return None, None, {}


def _run_id(root: Path, report_dir: Path | None, state: dict[str, Any]) -> str | None:
    if report_dir:
        metadata = _read_json(report_dir / "run_metadata.json")
        if metadata.get("run_id"):
            return str(metadata["run_id"])
    for key in ("run_id", "id"):
        if state.get(key):
            return str(state[key])
    return None


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _project_size(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def _largest_files(root: Path, limit: int = 20) -> list[dict[str, Any]]:
    entries: list[tuple[int, Path]] = []
    for path in root.rglob("*"):
        try:
            if path.is_file() and not path.is_symlink():
                entries.append((path.stat().st_size, path))
        except OSError:
            continue
    return [
        {"path": str(path.relative_to(root)), "size_bytes": size}
        for size, path in sorted(entries, reverse=True)[:limit]
    ]


def _directory_sizes(root: Path) -> dict[str, int]:
    names = ("logs", "reports", ".venv", ".git", ".ruff_cache", ".pytest_cache", "cache")
    return {name: _project_size(root / name) if (root / name).exists() else 0 for name in names}


def _audit(root: Path, target_state: Path | None = None) -> dict[str, Any]:
    usage = shutil.disk_usage(root)
    processes = _processes_for_root(root)
    target = _read_json(target_state) if target_state else {}
    return {
        "observed_at_utc": _utc(time.time()),
        "disk": {
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
            "free_gb": round(usage.free / (1024**3), 6),
        },
        "project_size_bytes": _project_size(root),
        "directory_sizes_bytes": _directory_sizes(root),
        "active_shadow_processes": _active_processes(root),
        "project_processes": processes,
        "target_state": {
            "path": str(target_state) if target_state else None,
            "status": target.get("status"),
            "pid": target.get("pid"),
            "started_at": target.get("started_at"),
            "ended_at": target.get("ended_at"),
        },
        "largest_files": _largest_files(root),
    }


def _candidate_files(root: Path) -> list[Path]:
    result: list[Path] = []
    for base in (root / "logs", root / "reports"):
        if not base.exists():
            continue
        for path in base.rglob("*"):
            try:
                if path.is_file() and not path.is_symlink():
                    result.append(path)
            except OSError:
                continue
    return sorted(result)


def _classify(
    root: Path,
    path: Path,
    active_paths: dict[str, list[int]],
) -> dict[str, Any]:
    log_dir, report_dir, state = _run_context(root, path)
    status = str(state.get("status", "UNKNOWN"))
    final_report = _has_final_report(report_dir)
    resolved = str(path.resolve())
    open_by = active_paths.get(resolved, [])
    relative = str(path.relative_to(root))
    suffix_protected = path.name.endswith(PROTECTED_SUFFIXES)
    if open_by:
        classification = "TIER_1_ACTIVE_OPEN"
        planned_action = "KEEP"
        reason = f"open by active runner/dashboard pid(s): {open_by}"
    elif status == "RUNNING":
        classification = "TIER_1_ACTIVE_RUN"
        planned_action = "KEEP"
        reason = "run state is RUNNING; PID/open-handle ambiguity is KEEP"
    elif status != "COMPLETE" and log_dir is not None:
        classification = "TIER_1_INCOMPLETE_OR_STALE"
        planned_action = "KEEP"
        reason = "run is incomplete, stale, or lacks a trustworthy terminal report"
    elif relative.startswith("reports/"):
        classification = "TIER_1_FINAL_OR_REPORT_ARTIFACT"
        planned_action = "KEEP"
        reason = "reports and audit artifacts are retained permanently"
    elif suffix_protected:
        classification = "TIER_1_RUNTIME_SIDECAR"
        planned_action = "KEEP_UNLESS_ZERO_AFTER_CLOSED_CHECKPOINT"
        reason = "runtime journal/WAL/SHM/temp file; never remove while active"
    elif path.name == "telemetry.sqlite" and status == "COMPLETE" and final_report:
        classification = "TIER_2_COMPLETED_RAW_TELEMETRY"
        planned_action = "ARCHIVE_ZSTD_VERIFY_THEN_REMOVE_SOURCE"
        reason = "closed completed DB with final report; archive preserves recoverability"
    elif path.suffix == ".log" and status == "COMPLETE":
        classification = "TIER_2_COMPLETED_DIAGNOSTIC_LOG"
        planned_action = "KEEP"
        reason = "diagnostic log retained; compress only in a separately approved pass"
    else:
        classification = "KEEP_UNCLASSIFIED"
        planned_action = "KEEP"
        reason = "no safe completed-run cleanup rule matched"
    try:
        stat = path.stat()
        size_bytes = stat.st_size
        age_seconds = max(0.0, time.time() - stat.st_mtime)
        modified_at = _utc(stat.st_mtime)
    except OSError:
        size_bytes = 0
        age_seconds = None
        modified_at = None
    return {
        "path": relative,
        "size_bytes": size_bytes,
        "age_seconds": age_seconds,
        "modified_at_utc": modified_at,
        "run_id": _run_id(root, report_dir, state),
        "run_status": status,
        "classification": classification,
        "planned_action": planned_action,
        "reason": reason,
        "open_by_pids": open_by,
        "report_dir": str(report_dir.relative_to(root)) if report_dir and report_dir.is_relative_to(root) else None,
    }


def build_manifest(root: Path, target_state: Path | None = None) -> dict[str, Any]:
    processes = _processes_for_root(root)
    active_paths = _open_paths(processes)
    entries = [_classify(root, path, active_paths) for path in _candidate_files(root)]
    planned = [entry for entry in entries if entry["planned_action"].startswith("ARCHIVE")]
    categories: dict[str, dict[str, int]] = {}
    for entry in entries:
        category = str(entry["classification"])
        bucket = categories.setdefault(category, {"files": 0, "bytes": 0})
        bucket["files"] += 1
        bucket["bytes"] += int(entry["size_bytes"])
    return {
        "manifest_version": 1,
        "generated_at_utc": _utc(time.time()),
        "project_root": str(root),
        "target_state": str(target_state) if target_state else None,
        "active_processes": processes,
        "active_open_path_count": len(active_paths),
        "entries": entries,
        "category_summary": categories,
        "planned_archive_count": len(planned),
        "planned_archive_bytes": sum(int(entry["size_bytes"]) for entry in planned),
        "safety_rule": "KEEP whenever active, open, incomplete, stale, or uncertain",
    }


def _database_summary(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.5)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        quick_check = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0])
        tables = [row["name"] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        counts = {}
        for table in TELEMETRY_TABLES:
            if table in tables:
                counts[table] = int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        schema = [
            {"name": row["name"], "sql": row["sql"]}
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name"
            )
        ]
        return {"quick_check": quick_check, "tables": tables, "row_counts": counts, "schema": schema}
    finally:
        connection.close()


def _checkpoint_closed_database(path: Path) -> tuple[bool, str]:
    try:
        connection = sqlite3.connect(path, timeout=5.0)
        connection.execute("PRAGMA busy_timeout=5000")
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        connection.commit()
        connection.close()
    except (OSError, sqlite3.DatabaseError) as exc:
        return False, f"closed checkpoint failed: {type(exc).__name__}: {exc}"
    if row and any(int(value) != 0 for value in row[1:]):
        return False, f"checkpoint left WAL frames: {tuple(row)}"
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists() and _size(sidecar) != 0:
            return False, f"non-empty sidecar remains: {sidecar.name}"
    return True, "closed WAL checkpoint completed"


def _checkpoint_active_database(path: Path) -> dict[str, Any]:
    """Attempt a bounded PASSIVE checkpoint without touching rows or WAL shape."""

    result: dict[str, Any] = {"path": str(path), "status": "SKIPPED"}
    if not path.exists():
        result["reason"] = "database is absent"
        return result
    try:
        connection = sqlite3.connect(path, timeout=0.5)
        connection.execute("PRAGMA busy_timeout=500")
        row = connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        connection.close()
        result.update({"status": "PASSIVE_CHECKPOINT", "result": tuple(int(value) for value in row) if row else None})
    except (OSError, sqlite3.DatabaseError) as exc:
        result["reason"] = f"non-blocking checkpoint unavailable: {type(exc).__name__}: {exc}"
    return result


def _zstd_archive(path: Path) -> tuple[bool, str, Path | None]:
    zstd = shutil.which("zstd")
    if not zstd:
        return False, "zstd executable is unavailable", None
    archive = Path(str(path) + ".zst")
    if archive.exists():
        test = _run([zstd, "-t", str(archive)], timeout=30.0)
        if test.returncode == 0:
            return False, "verified archive already exists; source left in place", archive
        return False, "existing archive failed zstd test; source left in place", archive
    temporary = Path(str(archive) + f".tmp.{os.getpid()}")
    try:
        result = _run([zstd, "-T0", "-19", "--no-progress", "-f", str(path), "-o", str(temporary)], timeout=300.0)
        if result.returncode != 0:
            return False, f"zstd compression failed: {result.stderr.strip()}", None
        test = _run([zstd, "-t", str(temporary)], timeout=30.0)
        if test.returncode != 0:
            return False, "compressed archive failed zstd test", None
        os.replace(temporary, archive)
        return True, "zstd archive created and tested", archive
    finally:
        if temporary.exists():
            temporary.unlink()


def _archive_database(root: Path, path: Path, active_paths: dict[str, list[int]]) -> dict[str, Any]:
    result: dict[str, Any] = {"path": str(path.relative_to(root)), "status": "SKIPPED"}
    if not path.exists():
        result["reason"] = "source disappeared"
        return result
    if active_paths.get(str(path.resolve())):
        result["reason"] = f"source is open by {active_paths[str(path.resolve())]}"
        return result
    try:
        before = _database_summary(path)
    except (OSError, sqlite3.DatabaseError) as exc:
        result["reason"] = f"source readability check failed: {type(exc).__name__}: {exc}"
        return result
    if before.get("quick_check") != "ok":
        result["reason"] = f"source quick_check={before.get('quick_check')}"
        return result
    checkpointed, checkpoint_reason = _checkpoint_closed_database(path)
    if not checkpointed:
        result["reason"] = checkpoint_reason
        return result
    try:
        after_checkpoint = _database_summary(path)
    except (OSError, sqlite3.DatabaseError) as exc:
        result["reason"] = f"post-checkpoint readability check failed: {type(exc).__name__}: {exc}"
        return result
    if before["row_counts"] != after_checkpoint["row_counts"] or before["tables"] != after_checkpoint["tables"]:
        result["reason"] = "checkpoint changed schema or row counts"
        return result
    success, archive_reason, archive = _zstd_archive(path)
    result.update(
        {
            "source_size_bytes": _size(path),
            "row_counts": before["row_counts"],
            "schema_tables": before["tables"],
            "checkpoint": checkpoint_reason,
            "archive": str(archive.relative_to(root)) if archive and archive.is_relative_to(root) else None,
            "archive_size_bytes": _size(archive) if archive else 0,
            "archive_verification": archive_reason,
        }
    )
    if not success or archive is None:
        result["reason"] = archive_reason
        return result
    if active_paths.get(str(path.resolve())):
        result["reason"] = "source became open after archive; source retained"
        return result
    path.unlink()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists() and _size(sidecar) == 0:
            sidecar.unlink()
    result["status"] = "ARCHIVED_AND_SOURCE_REMOVED"
    result["bytes_recovered"] = int(result["source_size_bytes"])
    result["reason"] = "source removed only after schema/count/readability and zstd verification"
    return result


def execute_closed_cleanup(root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    processes = _processes_for_root(root)
    active_paths = _open_paths(processes)
    results = []
    for entry in manifest.get("entries", []):
        if entry.get("planned_action") != "ARCHIVE_ZSTD_VERIFY_THEN_REMOVE_SOURCE":
            continue
        path = root / str(entry["path"])
        results.append(_archive_database(root, path, active_paths))
    recovered = sum(int(result.get("bytes_recovered", 0)) for result in results)
    usage = shutil.disk_usage(root)
    return {
        "completed_at_utc": _utc(time.time()),
        "results": results,
        "recovered_bytes": recovered,
        "recovered_gb": round(recovered / (1024**3), 6),
        "disk_after": {
            "free_bytes": usage.free,
            "free_gb": round(usage.free / (1024**3), 6),
        },
    }


def _retention_summary() -> dict[str, Any]:
    return {
        "warning_free_gb": 15,
        "critical_free_gb": 10,
        "emergency_free_gb": 5,
        "raw_retention_seconds": 180,
        "feature_persist_interval_seconds": 1,
        "aggregate_interval_seconds": 60,
        "chunk_rotation_minutes": 10,
        "warning_run_storage_gb": 2,
        "critical_run_storage_gb": 2.5,
        "max_run_storage_gb": 3,
        "max_project_generated_data_gb": 10,
        "project_generated_data_scope": "logs and reports only",
        "ordinary_full_raw_retention_after_success_hours": 24,
        "failed_infrastructure_raw_retained_until_root_cause": True,
        "governor_enabled": True,
        "archive_format": "SQLite + Zstandard",
        "parquet_available": False,
    }


def _cleanup_summary(
    root: Path,
    *,
    before_manifest: Path,
    after_manifest: Path,
    before_audit: dict[str, Any],
    cleanup: dict[str, Any],
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": "COMPLETE",
        "completed_at_utc": _utc(time.time()),
        "manifest_before": str(before_manifest.relative_to(root)),
        "manifest_after": str(after_manifest.relative_to(root)),
        "disk_before": before_audit.get("disk", {}),
        "disk_after": cleanup.get("disk_after", {}),
        "category_before": before.get("category_summary", {}),
        "category_after": after.get("category_summary", {}),
        "saved_by_cleanup_results": cleanup.get("results", []),
        "total_recovered_bytes": cleanup.get("recovered_bytes", 0),
        "total_recovered_gb": cleanup.get("recovered_gb", 0),
        "retention_policy": _retention_summary(),
        "active_processes_after": after.get("active_processes", []),
        "safety_rule": "No open, active, incomplete, stale, or uncertain file was removed",
    }


def _target_complete(root: Path, state_path: Path, target_pid: int | None) -> tuple[bool, str]:
    state = _read_json(state_path)
    status = str(state.get("status", "MISSING"))
    if status == "FAILED":
        return False, "target run failed; worker will not archive it"
    if status != "COMPLETE":
        return False, f"target state is {status}"
    if target_pid:
        for process in _active_processes(root):
            if int(process["pid"]) == target_pid:
                return False, "target PID still running"
    return True, "target state COMPLETE and target PID is no longer running"


def run_worker(
    root: Path,
    *,
    state_path: Path,
    target_pid: int | None,
    poll_seconds: float,
    before_manifest_path: Path,
) -> int:
    manifest = build_manifest(root, state_path)
    _write_json(before_manifest_path, manifest)
    print(json.dumps({"before_manifest": str(before_manifest_path), "audit": _audit(root, state_path)}, indent=2))
    initial_cleanup = execute_closed_cleanup(root, manifest)
    print(json.dumps({"initial_cleanup": initial_cleanup}, indent=2))
    active_databases = [
        root / str(entry["path"])
        for entry in manifest.get("entries", [])
        if entry.get("classification") == "TIER_1_ACTIVE_OPEN"
        and str(entry.get("path", "")).endswith("telemetry.sqlite")
    ]
    last_checkpoint = 0.0
    last_closed_cleanup = time.monotonic()
    deadline = time.monotonic() + 7 * 24 * 3600
    while time.monotonic() < deadline:
        if time.monotonic() - last_closed_cleanup >= 300.0:
            rolling_manifest = build_manifest(root, state_path)
            rolling_cleanup = execute_closed_cleanup(root, rolling_manifest)
            if rolling_cleanup.get("results"):
                print(json.dumps({"rolling_closed_cleanup": rolling_cleanup}, indent=2), flush=True)
            last_closed_cleanup = time.monotonic()
        if time.monotonic() - last_checkpoint >= 300.0:
            checkpoint_results = [_checkpoint_active_database(path) for path in active_databases]
            print(json.dumps({"passive_checkpoint": checkpoint_results}, indent=2), flush=True)
            last_checkpoint = time.monotonic()
        complete, reason = _target_complete(root, state_path, target_pid)
        if complete:
            report_dir = root / "reports" / state_path.relative_to(root / "logs").parent
            if not _has_final_report(report_dir):
                time.sleep(poll_seconds)
                continue
            processes = _processes_for_root(root)
            active_paths = _open_paths(processes)
            database = root / "logs" / state_path.relative_to(root / "logs").parent / "telemetry.sqlite"
            result = _archive_database(root, database, active_paths)
            after_manifest = build_manifest(root, state_path)
            after_path = before_manifest_path.with_name("cleanup_manifest_after.json")
            summary_path = before_manifest_path.with_name("cleanup_summary.json")
            _write_json(after_path, after_manifest)
            summary = {
                "status": "COMPLETE" if result.get("status") == "ARCHIVED_AND_SOURCE_REMOVED" else "COMPLETE_REPORT_RETAINED_DB_NOT_ARCHIVED",
                "worker_message": reason,
                "target_state": str(state_path.relative_to(root)),
                "archive_result": result,
                "initial_cleanup": initial_cleanup,
                "audit_after": _audit(root, state_path),
                "manifest_before": str(before_manifest_path.relative_to(root)),
                "manifest_after": str(after_path.relative_to(root)),
                "retention_policy": _retention_summary(),
            }
            _write_json(summary_path, summary)
            print(json.dumps({"final_cleanup": summary}, indent=2))
            return 0
        if "failed" in reason:
            print(reason, file=sys.stderr)
            return 2
        time.sleep(poll_seconds)
    print("storage worker deadline expired without a terminal target state", file=sys.stderr)
    return 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="safe shadow storage audit and closed-run archive")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--state", type=Path, default=None, help="target state.json for --watch")
    parser.add_argument("--pid", type=int, default=None, help="target runner PID for --watch")
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--execute", action="store_true", help="archive only manifest-approved closed DBs")
    parser.add_argument("--watch", action="store_true", help="wait for target COMPLETE and archive after final report")
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    root = args.project_root.resolve()
    before_path = (args.manifest or root / "reports" / "storage_cleanup" / "cleanup_manifest_before.json").resolve()
    state_path = args.state.resolve() if args.state else None
    if args.watch and state_path is None:
        parser.error("--watch requires --state")
    if args.watch:
        if args.poll_seconds <= 0:
            parser.error("--poll-seconds must be positive")
        return run_worker(
            root,
            state_path=state_path,
            target_pid=args.pid,
            poll_seconds=args.poll_seconds,
            before_manifest_path=before_path,
        )
    manifest = build_manifest(root, state_path)
    _write_json(before_path, manifest)
    before_audit = _audit(root, state_path)
    payload: dict[str, Any] = {"audit": before_audit, "manifest": str(before_path), "planned_cleanup": manifest}
    if args.execute:
        payload["cleanup"] = execute_closed_cleanup(root, manifest)
        after_path = before_path.with_name("cleanup_manifest_after.json")
        after = build_manifest(root, state_path)
        _write_json(after_path, after)
        payload["manifest_after"] = str(after_path)
        summary_path = before_path.with_name("cleanup_summary.json")
        summary = _cleanup_summary(
            root,
            before_manifest=before_path,
            after_manifest=after_path,
            before_audit=before_audit,
            cleanup=payload["cleanup"],
            before=manifest,
            after=after,
        )
        _write_json(summary_path, summary)
        payload["summary"] = summary
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
