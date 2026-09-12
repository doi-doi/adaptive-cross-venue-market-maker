#!/usr/bin/env python3
"""Phase 0 project-local storage audit and recoverable raw-data cleanup.

This command is intentionally narrower than a general disk-cleaner.  It only
operates below the configured project root, inventories the complete project
before mutation, treats active/open/ambiguous state as protected, and archives
closed SQLite bundles (database plus WAL/SHM sidecars) before removing the
verified originals.  It never starts a strategy, dashboard, collector, or
watcher.

Usage:
    python scripts/phase0_storage_cleanup.py --project-root . audit
    python scripts/phase0_storage_cleanup.py --project-root . execute

The execute command consumes the generated cleanup_manifest_before.csv from
the dated report directory.  It is deliberately fail-closed if that manifest
is missing or its project root does not match the requested root.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OUTPUT_NAME = "storage_cleanup_20260912"
OUTPUT_RELATIVE = Path("reports") / OUTPUT_NAME
TERMINAL_STATUSES = {"COMPLETED", "FAILED", "ABORTED", "INCOMPLETE"}
ACTIVE_STATUSES = {"RUNNING"}
REPORT_FILE_MARKERS = (
    "final",
    "summary",
    "fill",
    "markout",
    "trade",
    "pnl",
    "net_capture",
    "ranking",
    "recommendation",
    "comparison",
    "rule",
    "repair",
    "root_cause",
    "migration",
    "metadata",
    "accounting",
    "inventory",
)
PROTECTED_TOP_LEVEL = {
    ".git",
    ".venv",
    "src",
    "scripts",
    "tests",
    "conf",
    "controllers",
    "dashboard",
    "docs",
}
SOURCE_SUFFIXES = {
    ".py",
    ".pyi",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".cfg",
    ".md",
    ".rst",
    ".txt",
    ".gitignore",
}
DB_SUFFIXES = {".sqlite", ".sqlite3", ".db"}
SIDECAR_SUFFIXES = ("-wal", "-shm", ".wal", ".journal")
CACHE_DIR_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    ".hypothesis",
    ".tox",
}
HIGH_FREQUENCY_MARKERS = (
    "bbo",
    "tick",
    "book",
    "orderbook",
    "quote",
    "feature",
    "decision",
    "reference_value",
    "reference_health",
    "bitget",
)
ERROR_MARKERS = (
    "traceback",
    "operationalerror",
    "database is locked",
    "database locked",
    "exception",
    "error",
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def utc_timestamp(value: float | int | None) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(float(value), UTC).isoformat().replace("+00:00", "Z")


def size_gb(value: int | float) -> float:
    return round(float(value) / (1024**3), 6)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def write_json(path: Path, value: Any) -> None:
    write_text_atomic(path, json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    os.replace(temporary, path)


def run_command(command: list[str], *, timeout: float = 20.0) -> subprocess.CompletedProcess[str]:
    try:
        # Some native utilities emit non-UTF-8 diagnostics. Capture bytes and
        # decode with replacement so a diagnostic cannot interrupt cleanup
        # between archive creation and its verification/finally cleanup.
        completed = subprocess.run(command, capture_output=True, text=False, timeout=timeout, check=False)
        stdout = completed.stdout.decode("utf-8", errors="replace") if isinstance(completed.stdout, bytes) else str(completed.stdout or "")
        stderr = completed.stderr.decode("utf-8", errors="replace") if isinstance(completed.stderr, bytes) else str(completed.stderr or "")
        return subprocess.CompletedProcess(command, completed.returncode, stdout, stderr)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return subprocess.CompletedProcess(command, 1, "", f"{type(exc).__name__}: {exc}")


def decode_output(value: bytes | str | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value or "")


def create_tar_zstd(root: Path, source_paths: list[Path], temporary_archive: Path, zstd_path: str) -> tuple[int, str, str]:
    """Stream tar output into zstd without BSD-tar compressor flags."""

    relative_sources = [str(path.relative_to(root)) for path in source_paths]
    tar_process = subprocess.Popen(
        ["tar", "-cf", "-", "-C", str(root), *relative_sources],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert tar_process.stdout is not None
    zstd_process = subprocess.Popen(
        [zstd_path, "-T0", "-19", "--no-progress", "-f", "-o", str(temporary_archive)],
        stdin=tar_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tar_process.stdout.close()
    try:
        zstd_stdout, zstd_stderr = zstd_process.communicate(timeout=900.0)
        tar_stderr = tar_process.stderr.read() if tar_process.stderr is not None else b""
        tar_returncode = tar_process.wait(timeout=30.0)
    except (OSError, subprocess.TimeoutExpired) as exc:
        zstd_process.kill()
        tar_process.kill()
        zstd_process.wait()
        tar_process.wait()
        return 1, "", f"{type(exc).__name__}: {exc}"
    stderr = "\n".join(filter(None, [decode_output(tar_stderr), decode_output(zstd_stderr)]))
    stdout = decode_output(zstd_stdout)
    return max(int(tar_returncode), int(zstd_process.returncode or 0)), stdout, stderr


def read_tar_zstd_listing(archive: Path, zstd_path: str) -> tuple[int, list[str], str]:
    zstd_process = subprocess.Popen(
        [zstd_path, "-d", "-c", str(archive)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert zstd_process.stdout is not None
    tar_process = subprocess.Popen(
        ["tar", "-tf", "-"],
        stdin=zstd_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    zstd_process.stdout.close()
    try:
        tar_stdout, tar_stderr = tar_process.communicate(timeout=180.0)
        zstd_stderr = zstd_process.stderr.read() if zstd_process.stderr is not None else b""
        zstd_returncode = zstd_process.wait(timeout=30.0)
    except (OSError, subprocess.TimeoutExpired) as exc:
        tar_process.kill()
        zstd_process.kill()
        tar_process.wait()
        zstd_process.wait()
        return 1, [], f"{type(exc).__name__}: {exc}"
    listing = decode_output(tar_stdout).splitlines()
    stderr = "\n".join(filter(None, [decode_output(tar_stderr), decode_output(zstd_stderr)]))
    return max(int(tar_process.returncode or 0), int(zstd_returncode)), listing, stderr


def canonical_root(root: Path) -> Path:
    resolved = root.expanduser().resolve(strict=True)
    if resolved.name != "derive-multi-asset-binance-mm-v2":
        raise ValueError(f"unexpected project root: {resolved}")
    return resolved


def safe_path(root: Path, path: Path) -> Path:
    """Resolve a target and reject traversal, symlinks, and root deletion."""

    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=False)
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError(f"unsafe project target rejected: {candidate}")
    if candidate.is_symlink():
        raise ValueError(f"symlink target rejected: {candidate}")
    return resolved


def allocated_size(path: Path) -> int:
    try:
        stat = path.lstat()
    except OSError:
        return 0
    blocks = int(getattr(stat, "st_blocks", 0))
    return blocks * 512 if blocks else int(stat.st_size)


def file_mtime(path: Path) -> float | None:
    try:
        return path.lstat().st_mtime
    except OSError:
        return None


def walk_files(root: Path, *, exclude_output: bool) -> list[Path]:
    files: list[Path] = []
    output = root / OUTPUT_RELATIVE
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        dirnames[:] = [
            name
            for name in dirnames
            if not (exclude_output and (directory_path / name).resolve(strict=False) == output)
        ]
        for name in filenames:
            path = directory_path / name
            if exclude_output and path.resolve(strict=False).is_relative_to(output):
                continue
            try:
                if path.is_file() or path.is_symlink():
                    files.append(path)
            except OSError:
                continue
    return sorted(files, key=lambda item: str(item))


def walk_directories(root: Path, *, exclude_output: bool) -> list[Path]:
    directories: list[Path] = [root]
    output = root / OUTPUT_RELATIVE
    for directory, dirnames, _ in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        for name in list(dirnames):
            path = directory_path / name
            if exclude_output and path.resolve(strict=False).is_relative_to(output):
                continue
            try:
                if path.is_dir() and not path.is_symlink():
                    directories.append(path)
            except OSError:
                continue
    return sorted(set(directories), key=lambda item: str(item))


def path_is_under(path: Path, parent: Path | None) -> bool:
    return parent is not None and path == parent or (parent is not None and path.is_relative_to(parent))


def discover_processes(root: Path) -> tuple[list[dict[str, Any]], set[int]]:
    result = run_command(["ps", "-axo", "pid=,ppid=,etime=,command="], timeout=10.0)
    project_processes: list[dict[str, Any]] = []
    active_pids: set[int] = set()
    root_text = str(root)
    own_pid = os.getpid()
    for line in result.stdout.splitlines():
        text = line.strip()
        if not text or root_text not in text:
            continue
        try:
            pid_text, ppid_text, elapsed, command = text.split(None, 3)
            pid = int(pid_text)
            ppid = int(ppid_text)
        except (ValueError, IndexError):
            continue
        if pid == own_pid or "phase0_storage_cleanup.py" in command:
            continue
        run_id = None
        match = re.search(r"(?:--run-id|--run_id)\s+([^\s]+)", command)
        if match:
            run_id = match.group(1)
        entry = {"pid": pid, "ppid": ppid, "elapsed": elapsed, "command": command, "run_id": run_id}
        project_processes.append(entry)
        lowered = command.lower()
        if any(
            marker in lowered
            for marker in (
                "derive_multi_asset_mm",
                "hummingbot",
                "launch_six_hour_shadow",
                "run_refresh_research",
                "watcher",
                "dashboard",
                "collector",
            )
        ) and not any(marker in lowered for marker in ("rg ", "grep ", "lsof ", "find ")):
            active_pids.add(pid)
    return project_processes, active_pids


def discover_open_paths(pids: set[int]) -> dict[str, list[int]]:
    if not pids:
        return {}
    result = run_command(["lsof", "-Fn", "-p", ",".join(str(pid) for pid in sorted(pids))], timeout=30.0)
    paths: dict[str, list[int]] = defaultdict(list)
    current_pid: int | None = None
    for line in result.stdout.splitlines():
        if line.startswith("p"):
            try:
                current_pid = int(line[1:])
            except ValueError:
                current_pid = None
        elif line.startswith("n") and current_pid is not None:
            value = line[1:].removesuffix(" (deleted)")
            try:
                resolved = str(Path(value).resolve(strict=False))
            except OSError:
                continue
            paths[resolved].append(current_pid)
    return dict(paths)


def discover_run_dirs(root: Path) -> list[Path]:
    logs = root / "logs"
    if not logs.exists():
        return []
    found: list[Path] = []
    markers = {"state.json", "run_metadata.json", "telemetry.sqlite", "telemetry.sqlite3", "telemetry.db"}
    for directory, _, filenames in os.walk(logs, followlinks=False):
        if markers.intersection(filenames):
            found.append(Path(directory))
    return sorted(set(found), key=lambda item: str(item))


def discover_report_dirs(root: Path) -> list[Path]:
    reports = root / "reports"
    if not reports.exists():
        return []
    found: list[Path] = []
    markers = re.compile(r"^(final|run_metadata|state|summary|asset_|trade_|markout|fill|net_capture|pnl)", re.I)
    for directory, _, filenames in os.walk(reports, followlinks=False):
        if any(markers.search(name) for name in filenames):
            found.append(Path(directory))
    return sorted(set(found), key=lambda item: str(item))


def first_json(paths: Iterable[Path]) -> dict[str, Any]:
    for path in paths:
        if path.is_file():
            value = read_json(path)
            if value:
                return value
    return {}


def local_report_dir(root: Path, log_dir: Path) -> Path | None:
    try:
        relative = log_dir.relative_to(root / "logs")
    except ValueError:
        return None
    candidate = root / "reports" / relative
    return candidate if candidate.is_dir() else None


def ancestor_report_dirs(root: Path, report_dir: Path | None) -> list[Path]:
    if report_dir is None:
        return []
    reports = root / "reports"
    result: list[Path] = []
    current = report_dir
    while current.is_relative_to(reports) and current != reports:
        result.append(current)
        current = current.parent
    return result


def json_run_id(value: dict[str, Any]) -> str | None:
    for key in ("run_id", "runId", "id"):
        if value.get(key):
            return str(value[key])
    for container_key in ("run_metadata", "metadata", "run"):
        nested = value.get(container_key)
        if isinstance(nested, dict):
            found = json_run_id(nested)
            if found:
                return found
    return None


def build_run_contexts(root: Path) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for log_dir in discover_run_dirs(root):
        exact_report = local_report_dir(root, log_dir)
        report_candidates: list[Path] = []
        if exact_report:
            report_candidates.extend([exact_report / "run_metadata.json", exact_report / "state.json"])
            report_candidates.extend(sorted(exact_report.glob("final*.json"), key=lambda item: str(item)))
        report_candidates.extend([log_dir / "run_metadata.json", log_dir / "state.json"])
        metadata = first_json(report_candidates)
        run_id = json_run_id(metadata)
        state = first_json([log_dir / "state.json", exact_report / "state.json"] if exact_report else [log_dir / "state.json"])
        if run_id is None:
            run_id = json_run_id(state)
        if run_id is None:
            # Some early runners never persisted a run_id. Keep a stable,
            # explicit path-derived identifier rather than merging those
            # runs into one UNKNOWN context.
            run_id = f"legacy:{log_dir.relative_to(root / 'logs')}"
        contexts.append(
            {
                "key": str(log_dir.relative_to(root)),
                "log_dir": log_dir,
                "report_dir": exact_report,
                "run_id": run_id,
                "state": state,
                "metadata": metadata,
            }
        )

    known_run_ids = {item["run_id"] for item in contexts if item.get("run_id")}
    for report_dir in discover_report_dirs(root):
        if any(path_is_under(report_dir, context["report_dir"]) for context in contexts if context["report_dir"]):
            continue
        metadata = first_json([report_dir / "run_metadata.json", report_dir / "state.json"])
        run_id = json_run_id(metadata)
        if run_id is None and report_dir in [item["report_dir"] for item in contexts]:
            continue
        if run_id in known_run_ids:
            continue
        if run_id is None:
            # Generic report collections (for example an audit folder) are
            # evidence, not independent historical runs.
            continue
        contexts.append(
            {
                "key": str(report_dir.relative_to(root)),
                "log_dir": None,
                "report_dir": report_dir,
                "run_id": run_id,
                "state": read_json(report_dir / "state.json"),
                "metadata": metadata,
            }
        )

    for context in contexts:
        report_dir = context["report_dir"]
        evidence_dirs = ancestor_report_dirs(root, report_dir)
        if context["run_id"]:
            for candidate in discover_report_dirs(root):
                if candidate == report_dir:
                    continue
                candidate_metadata = first_json([candidate / "run_metadata.json", candidate / "state.json"])
                if json_run_id(candidate_metadata) == context["run_id"]:
                    evidence_dirs.append(candidate)
        context["evidence_dirs"] = sorted(set(evidence_dirs), key=lambda item: str(item))
    return contexts


def context_for_path(path: Path, contexts: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for context in contexts:
        for key in ("log_dir", "report_dir"):
            parent = context.get(key)
            if parent is not None and path_is_under(path, parent):
                candidates.append((len(str(parent)), context))
        for parent in context.get("evidence_dirs", []):
            if path_is_under(path, parent):
                candidates.append((len(str(parent)), context))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def text_excerpt(path: Path, limit: int = 24000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:limit]
    except OSError:
        return ""


def error_text_for_context(context: dict[str, Any]) -> str:
    log_dir = context.get("log_dir")
    if log_dir is None or not log_dir.exists():
        return ""
    pieces: list[str] = []
    for path in sorted(log_dir.rglob("*.stderr.log")) + sorted(log_dir.glob("*.log")):
        text = text_excerpt(path, 12000)
        if text and any(marker in text.lower() for marker in ERROR_MARKERS):
            pieces.append(f"--- {path.name} ---\n{text}")
    return "\n".join(pieces)[:60000]


def status_for_context(context: dict[str, Any], processes: list[dict[str, Any]], open_paths: dict[str, list[int]]) -> tuple[str, str]:
    run_id = context.get("run_id")
    log_dir = context.get("log_dir")
    active_processes = [item for item in processes if run_id and item.get("run_id") == run_id]
    open_for_run = False
    if log_dir and log_dir.exists():
        prefix = str(log_dir.resolve())
        open_for_run = any(path == prefix or path.startswith(prefix + os.sep) for path in open_paths)
    if active_processes or open_for_run:
        return "RUNNING", "active project process or open file handle observed"

    stored = str((context.get("state") or {}).get("status", "")).strip().upper()
    if stored in {"COMPLETE", "COMPLETED", "SUCCESS", "SUCCEEDED"}:
        return "COMPLETED", "terminal COMPLETE state observed"
    if stored in {"FAILED", "ERROR"}:
        return "FAILED", "terminal failure state observed"
    if stored in {"ABORTED", "ABORTED_BY_USER", "CANCELLED", "CANCELED", "STOPPED"}:
        return "ABORTED", "terminal abort/stop state observed"

    error_text = error_text_for_context(context)
    if error_text:
        return "FAILED", "runner or watcher error evidence observed after process exit"

    for report_dir in context.get("evidence_dirs", []):
        for path in report_dir.glob("final*.json"):
            report = read_json(path)
            report_status = str(report.get("status", report.get("run_status", ""))).upper()
            if report_status in {"FAILED", "ERROR"}:
                return "FAILED", f"final report records {report_status}"
            if report_status in {"COMPLETE", "COMPLETED", "SUCCESS", "SUCCEEDED"}:
                return "COMPLETED", "final report records terminal success"

    if stored == "RUNNING":
        return "INCOMPLETE", "stale RUNNING state without active process/open handle"
    return "INCOMPLETE", "no trustworthy terminal status was found"


def report_files(context: dict[str, Any]) -> list[Path]:
    result: set[Path] = set()
    for directory in context.get("evidence_dirs", []):
        if directory.exists():
            for path in directory.rglob("*"):
                if path.is_file() and not path.is_symlink():
                    result.add(path)
    return sorted(result, key=lambda item: str(item))


def final_report_exists(context: dict[str, Any]) -> bool:
    return any(path.name.lower().startswith("final") and path.suffix.lower() in {".json", ".md", ".txt"} for path in report_files(context))


def contains_run_id(path: Path, run_id: str | None) -> bool:
    if not run_id:
        return False
    if run_id in str(path):
        return True
    if path.suffix.lower() not in {".json", ".md", ".csv", ".jsonl", ".txt"}:
        return False
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return run_id in handle.read(65536)
    except OSError:
        return False


def canonical_evidence_exists(root: Path, context: dict[str, Any], kind: str) -> bool:
    marker = "fill" if kind == "fills" else "markout"
    for path in report_files(context):
        if marker in path.name.lower() and path.stat().st_size > 0:
            return True
    for path in walk_files(root, exclude_output=True):
        if not path.is_relative_to(root / "reports"):
            continue
        if marker in path.name.lower() and path.stat().st_size > 0 and contains_run_id(path, context.get("run_id")):
            return True
    return False


def parse_time(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        return utc_timestamp(value)
    text = str(value)
    if text.replace(".", "", 1).isdigit():
        try:
            return utc_timestamp(float(text))
        except ValueError:
            pass
    return text


def find_time(context: dict[str, Any], keys: tuple[str, ...]) -> str:
    for source in (context.get("metadata") or {}, context.get("state") or {}):
        for key in keys:
            if source.get(key) not in (None, ""):
                return parse_time(source[key])
    for path in report_files(context):
        if path.suffix.lower() != ".json":
            continue
        value = read_json(path)
        for key in keys:
            if value.get(key) not in (None, ""):
                return parse_time(value[key])
    return ""


def context_size(context: dict[str, Any], *, kind: str, contexts: list[dict[str, Any]] | None = None) -> int:
    paths: set[Path] = set()
    if kind in {"raw", "database", "logs"} and context.get("log_dir"):
        log_dir = context["log_dir"]
        nested_run_dirs = [
            item["log_dir"]
            for item in contexts or []
            if item.get("log_dir") is not None
            and item["log_dir"] != log_dir
            and item["log_dir"].is_relative_to(log_dir)
        ]
        for path in log_dir.rglob("*"):
            if path.is_file() and not path.is_symlink():
                if any(path_is_under(path, nested) for nested in nested_run_dirs):
                    continue
                if kind == "database" and not (path.suffix.lower() in DB_SUFFIXES or any(path.name.endswith(suffix) for suffix in SIDECAR_SUFFIXES)):
                    continue
                if kind == "logs" and not path.name.lower().endswith((".log", ".out", ".err")):
                    continue
                paths.add(path)
    if kind == "raw" and context.get("log_dir"):
        log_dir = context["log_dir"]
        nested_run_dirs = [
            item["log_dir"]
            for item in contexts or []
            if item.get("log_dir") is not None
            and item["log_dir"] != log_dir
            and item["log_dir"].is_relative_to(log_dir)
        ]
        paths = {
            path
            for path in log_dir.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and not any(path_is_under(path, nested) for nested in nested_run_dirs)
        }
    if kind == "reports":
        paths.update(report_files(context))
    return sum(allocated_size(path) for path in paths)


def database_paths(context: dict[str, Any], contexts: list[dict[str, Any]] | None = None) -> list[Path]:
    log_dir = context.get("log_dir")
    if log_dir is None or not log_dir.exists():
        return []
    nested_run_dirs = [
        item["log_dir"]
        for item in contexts or []
        if item.get("log_dir") is not None
        and item["log_dir"] != log_dir
        and item["log_dir"].is_relative_to(log_dir)
    ]
    return sorted(
        [
            path
            for path in log_dir.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and (path.suffix.lower() in DB_SUFFIXES)
            and not any(path_is_under(path, nested) for nested in nested_run_dirs)
        ],
        key=lambda item: str(item),
    )


def inventory_rows(root: Path, contexts: list[dict[str, Any]], processes: list[dict[str, Any]], open_paths: dict[str, list[int]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for context in contexts:
        status, status_reason = status_for_context(context, processes, open_paths)
        context["status"] = status
        context["status_reason"] = status_reason
        log_dir = context.get("log_dir")
        report_dir = context.get("report_dir")
        universe: Any = None
        for source in (context.get("metadata") or {}, context.get("state") or {}):
            for key in ("asset_universe", "active_universe", "assets", "active_assets", "universe"):
                if source.get(key):
                    universe = source[key]
                    break
            if universe:
                break
        if isinstance(universe, dict):
            universe = list(universe.keys())
        if isinstance(universe, (list, tuple, set)):
            universe_text = ",".join(str(item).upper() for item in universe)
        else:
            universe_text = str(universe or "")
        rows.append(
            {
                "run_id": context.get("run_id") or "UNKNOWN",
                "asset_universe": universe_text,
                "start_time": find_time(context, ("start_time", "started_at", "analysis_start_utc", "analysis_start")),
                "end_time": find_time(context, ("end_time", "ended_at", "analysis_end_utc", "analysis_end")),
                "status": status,
                "status_reason": status_reason,
                "log_path": str(log_dir.relative_to(root)) if log_dir else "",
                "report_path": str(report_dir.relative_to(root)) if report_dir else "",
                "raw_telemetry_size_bytes": context_size(context, kind="raw", contexts=contexts),
                "raw_telemetry_size_gb": size_gb(context_size(context, kind="raw", contexts=contexts)),
                "database_size_bytes": context_size(context, kind="database", contexts=contexts),
                "database_size_gb": size_gb(context_size(context, kind="database", contexts=contexts)),
                "log_size_bytes": context_size(context, kind="logs", contexts=contexts),
                "log_size_gb": size_gb(context_size(context, kind="logs", contexts=contexts)),
                "report_size_bytes": context_size(context, kind="reports"),
                "report_size_gb": size_gb(context_size(context, kind="reports")),
                "final_report_exists": "YES" if final_report_exists(context) else "NO",
                "canonical_fills_exists": "YES" if canonical_evidence_exists(root, context, "fills") else "NO",
                "canonical_markouts_exists": "YES" if canonical_evidence_exists(root, context, "markouts") else "NO",
                "database_paths": ";".join(str(path.relative_to(root)) for path in database_paths(context, contexts)),
            }
        )
    return sorted(rows, key=lambda row: (str(row["run_id"]), str(row["log_path"])))


def run_id_for_file(path: Path, contexts: list[dict[str, Any]]) -> str:
    context = context_for_path(path, contexts)
    return str(context.get("run_id") or "UNKNOWN") if context else "UNKNOWN"


def file_type(path: Path) -> str:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if name.endswith("-wal") or name.endswith(".wal"):
        return "sqlite_wal"
    if name.endswith("-shm"):
        return "sqlite_shm"
    if name.endswith(".journal"):
        return "sqlite_journal"
    if suffix in DB_SUFFIXES:
        return "sqlite_database"
    if suffix == ".zst" or name.endswith(".tar.zst"):
        return "compressed_archive"
    if suffix in {".log", ".out", ".err"}:
        return "log"
    if suffix in {".csv", ".json", ".jsonl", ".parquet"}:
        return "generated_dataset"
    if suffix in SOURCE_SUFFIXES or name.startswith(".env"):
        return "source_or_config"
    if name.endswith(".pid"):
        return "pid"
    if name.endswith(".lock") or name.endswith(".tmp") or ".tmp." in name:
        return "runtime_temp"
    if suffix in {".pyc", ".pyo"}:
        return "python_cache"
    return "other"


def is_report_path(root: Path, path: Path) -> bool:
    return path_is_under(path, root / "reports")


def is_source_or_config_path(root: Path, path: Path) -> bool:
    relative = path.relative_to(root)
    top = relative.parts[0] if relative.parts else ""
    return top in PROTECTED_TOP_LEVEL or path.name.startswith(".env") or path.suffix.lower() in SOURCE_SUFFIXES


def cache_reason(path: Path) -> str | None:
    parts = set(path.parts)
    if parts.intersection(CACHE_DIR_NAMES):
        return "project-local regenerable cache"
    if path.suffix.lower() in {".pyc", ".pyo"}:
        return "project-local Python bytecode cache"
    if path.name in {".coverage", ".DS_Store"}:
        return "regenerable local cache/metadata"
    return None


def failed_contexts(contexts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [context for context in contexts if context.get("run_id") == "zec_xrp_link_refresh_20260909T142815Z"]


def capture_failed_run_evidence(root: Path, contexts: list[dict[str, Any]], output: Path) -> list[dict[str, Any]]:
    captures: list[dict[str, Any]] = []
    for context in failed_contexts(contexts):
        log_dir = context.get("log_dir")
        error_text = error_text_for_context(context)
        db_summaries: list[dict[str, Any]] = []
        for database in database_paths(context, contexts):
            summary: dict[str, Any] = {"path": str(database.relative_to(root)), "size_bytes": allocated_size(database)}
            try:
                connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1.0)
                connection.execute("PRAGMA query_only=ON")
                summary["quick_check"] = connection.execute("PRAGMA quick_check(1)").fetchone()[0]
                tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
                summary["tables"] = tables
                summary["row_counts"] = {
                    table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
                    for table in tables
                    if table in {"state", "decisions", "decision_rollups", "actions", "fills", "markouts", "reference_health", "reference_values", "trades", "minute_aggregates"}
                }
                connection.close()
            except (OSError, sqlite3.DatabaseError) as exc:
                summary["read_error"] = f"{type(exc).__name__}: {exc}"
            db_summaries.append(summary)
        payload = {
            "captured_at_utc": utc_now(),
            "run_id": context.get("run_id"),
            "status_at_capture": context.get("status"),
            "status_reason": context.get("status_reason"),
            "log_path": str(log_dir.relative_to(root)) if log_dir else "",
            "report_paths": [str(path.relative_to(root)) for path in context.get("evidence_dirs", [])],
            "state": context.get("state", {}),
            "metadata": context.get("metadata", {}),
            "database_summaries": db_summaries,
            "runner_error_evidence": error_text,
            "root_cause_scope": "Phase 0 evidence capture only; SQLite root-cause and repair remain the next work order phases.",
            "preservation_rule": "Keep the failed-run error, metadata, canonical reports, fills/markouts, and a verified raw SQLite archive until Phase 1 confirms the database-lock root cause is no longer dependent on raw detail.",
        }
        write_json(output / "failed_run_root_cause_capture.json", payload)
        markdown = [
            "# Failed run root-cause evidence capture",
            "",
            f"- Captured UTC: `{payload['captured_at_utc']}`",
            f"- Run ID: `{payload['run_id']}`",
            f"- Observed status: `{payload['status_at_capture']}` ({payload['status_reason']})",
            "- Scope: Phase 0 evidence capture only; no SQLite repair or strategy run was started.",
            "",
            "## Preserved evidence",
            "",
            f"- Runtime log: `{payload['log_path']}`",
            *[f"- Report/evidence directory: `{item}`" for item in payload["report_paths"]],
            *[f"- SQLite evidence: `{item['path']}`; quick_check=`{item.get('quick_check', item.get('read_error', 'UNKNOWN'))}`; rows={item.get('row_counts', {})}" for item in db_summaries],
            "",
            "## Runner error excerpt",
            "",
            "```text",
            error_text or "No error text was readable at capture time.",
            "```",
            "",
            "The raw database bundle may be compressed only after this capture and archive verification. The Phase 1 SQLite audit remains responsible for confirming the lock root cause.",
            "",
        ]
        write_text_atomic(output / "failed_run_root_cause_capture.md", "\n".join(markdown))
        captures.append({"run_id": context.get("run_id"), "database_count": len(db_summaries), "error_bytes": len(error_text.encode())})
    return captures


def status_for_path(path: Path, contexts: list[dict[str, Any]], processes: list[dict[str, Any]], open_paths: dict[str, list[int]]) -> tuple[str, dict[str, Any] | None]:
    context = context_for_path(path, contexts)
    if str(path.resolve(strict=False)) in open_paths:
        return "RUNNING", context
    if context and context.get("status") == "RUNNING":
        return "RUNNING", context
    return (str(context.get("status")), context) if context else ("UNKNOWN", None)


def eligible_closed_context(context: dict[str, Any] | None, path: Path | None = None, open_paths: dict[str, list[int]] | None = None) -> bool:
    """Return true only for a filesystem-closed historical context.

    A stale RUNNING state is not treated as active after process/open-handle
    checks have classified it as INCOMPLETE.  The compact-report requirement
    keeps the cleanup from deleting ambiguous raw data that has no durable
    research record.
    """

    if not context or context.get("status") not in {"COMPLETED", "FAILED", "ABORTED", "INCOMPLETE"}:
        return False
    if path is not None and open_paths and str(path.resolve(strict=False)) in open_paths:
        return False
    if context.get("status") == "INCOMPLETE" and not final_report_exists(context):
        return False
    return True


def db_for_sidecar(path: Path) -> Path | None:
    name = path.name
    for suffix in SIDECAR_SUFFIXES:
        if name.endswith(suffix):
            return path.with_name(name[: -len(suffix)])
    return None


def classify_file(root: Path, path: Path, contexts: list[dict[str, Any]], processes: list[dict[str, Any]], open_paths: dict[str, list[int]], planned_db_paths: set[Path]) -> dict[str, Any]:
    context = context_for_path(path, contexts)
    run_id = str(context.get("run_id") or "UNKNOWN") if context else "UNKNOWN"
    status = str(context.get("status") or "UNKNOWN") if context else "UNKNOWN"
    resolved = str(path.resolve(strict=False))
    open_by = open_paths.get(resolved, [])
    if open_by:
        return classification_entry(path, run_id, "TIER_D_ACTIVE_DO_NOT_TOUCH", "ACTIVE_SKIP", f"open by project PID(s) {open_by}", context)
    if context and status == "RUNNING":
        return classification_entry(path, run_id, "TIER_D_ACTIVE_DO_NOT_TOUCH", "ACTIVE_SKIP", "run has an active process or open handle", context)
    cache = cache_reason(path)
    if cache:
        return classification_entry(path, run_id, "TIER_C_SAFE_TO_DELETE", "DELETE", cache, context)
    if is_source_or_config_path(root, path):
        return classification_entry(path, run_id, "TIER_A_KEEP_PERMANENTLY", "KEEP", "source, configuration, environment reference, documentation, or project control file", context)
    if is_report_path(root, path):
        return classification_entry(path, run_id, "TIER_A_KEEP_PERMANENTLY", "KEEP", "report/evidence tree retained as the compact research record", context)
    name = path.name.lower()
    ftype = file_type(path)
    if name.endswith(".pid") or name.endswith(".lock") or name.endswith(".tmp") or ".tmp." in name:
        return classification_entry(path, run_id, "TIER_C_SAFE_TO_DELETE", "DELETE", "stale project-local runtime marker with no active owner", context)
    if ftype in {"sqlite_wal", "sqlite_shm", "sqlite_journal"}:
        parent_db = db_for_sidecar(path)
        if parent_db and parent_db.resolve(strict=False) in planned_db_paths:
            return classification_entry(path, run_id, "TIER_B_KEEP_COMPRESSED", "COMPRESS", "closed SQLite sidecar archived with its database bundle", context)
        if allocated_size(path) == 0 and not (context and status not in TERMINAL_STATUSES):
            return classification_entry(path, run_id, "TIER_C_SAFE_TO_DELETE", "DELETE", "zero-byte closed SQLite sidecar", context)
        if context and status in TERMINAL_STATUSES:
            return classification_entry(path, run_id, "TIER_B_KEEP_COMPRESSED", "COMPRESS", "closed historical SQLite sidecar retained in an archive", context)
        return classification_entry(path, run_id, "TIER_D_ACTIVE_DO_NOT_TOUCH", "ACTIVE_SKIP", "non-empty SQLite sidecar is ambiguous without a closed database owner", context)
    if ftype == "sqlite_database":
        if eligible_closed_context(context, path, open_paths):
            if status == "FAILED" and context.get("run_id") == "zec_xrp_link_refresh_20260909T142815Z":
                reason = "failed-run raw SQLite retained as a verified archive after Phase 0 lock-evidence capture"
            elif final_report_exists(context):
                reason = "closed historical SQLite raw telemetry; compact report/evidence exists"
            else:
                reason = "closed historical SQLite raw telemetry; no active owner"
            return classification_entry(path, run_id, "TIER_B_KEEP_COMPRESSED", "COMPRESS", reason, context)
        return classification_entry(path, run_id, "TIER_D_ACTIVE_DO_NOT_TOUCH", "ACTIVE_SKIP", "SQLite database has no trustworthy closed status", context)
    if context and status == "FAILED" and ("stderr" in name or "error" in name or "trace" in name):
        return classification_entry(path, run_id, "TIER_A_KEEP_PERMANENTLY", "KEEP", "failed-run error evidence", context)
    if context and status in TERMINAL_STATUSES and ftype == "log":
        if any(marker in name for marker in HIGH_FREQUENCY_MARKERS) and final_report_exists(context):
            return classification_entry(path, run_id, "TIER_B_KEEP_COMPRESSED", "COMPRESS", "closed detailed historical telemetry log with compact reports", context)
        return classification_entry(path, run_id, "TIER_A_KEEP_PERMANENTLY", "KEEP", "run log retained for reproducibility and failure review", context)
    if context and status in TERMINAL_STATUSES and ftype == "generated_dataset":
        if any(marker in name for marker in REPORT_FILE_MARKERS):
            return classification_entry(path, run_id, "TIER_A_KEEP_PERMANENTLY", "KEEP", "compact summary/evidence dataset", context)
        if any(marker in name for marker in HIGH_FREQUENCY_MARKERS) and final_report_exists(context):
            return classification_entry(path, run_id, "TIER_C_SAFE_TO_DELETE", "DELETE", "regenerable historical raw dataset represented by compact reports", context)
        return classification_entry(path, run_id, "TIER_A_KEEP_PERMANENTLY", "KEEP", "generated evidence not safely identified as duplicate/raw", context)
    return classification_entry(path, run_id, "TIER_A_KEEP_PERMANENTLY", "KEEP", "unclassified or ambiguous project file; fail closed", context)


def classification_entry(path: Path, run_id: str, classification: str, action: str, reason: str, context: dict[str, Any] | None) -> dict[str, Any]:
    modified = file_mtime(path)
    return {
        "path": str(path),
        "size_bytes": allocated_size(path),
        "size_gb": size_gb(allocated_size(path)),
        "modified_time": utc_timestamp(modified),
        "run_id": run_id,
        "classification": classification,
        "reason": reason,
        "planned_action": action,
        "file_type": file_type(path),
        "run_status": str(context.get("status") if context else "UNKNOWN"),
    }


def build_manifest(root: Path, contexts: list[dict[str, Any]], processes: list[dict[str, Any]], open_paths: dict[str, list[int]], *, exclude_output: bool) -> list[dict[str, Any]]:
    files = walk_files(root, exclude_output=exclude_output)
    db_paths = {
        path.resolve(strict=False)
        for path in files
        if file_type(path) == "sqlite_database"
        and eligible_closed_context(context_for_path(path, contexts), path, open_paths)
    }
    entries = [classify_file(root, path, contexts, processes, open_paths, db_paths) for path in files]
    return entries


def directory_rows(root: Path, contexts: list[dict[str, Any]], entries: list[dict[str, Any]], *, exclude_output: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for directory in walk_directories(root, exclude_output=exclude_output):
        files = [path for path in walk_files(directory, exclude_output=False) if path.is_relative_to(directory)]
        if directory != root and not files:
            continue
        value = sum(allocated_size(path) for path in files)
        sample = entries_for_directory(entries, directory)
        classifications = sorted({str(item["classification"]) for item in sample})
        rows.append(
            {
                "path": str(directory),
                "size_bytes": value,
                "size_gb": size_gb(value),
                "modified_time": utc_timestamp(file_mtime(directory)),
                "run_id": run_id_for_file(directory, contexts),
                "file_type": "directory",
                "classification": ";".join(classifications),
            }
        )
    return sorted(rows, key=lambda row: (-int(row["size_bytes"]), str(row["path"])))[:50]


def entries_for_directory(entries: list[dict[str, Any]], directory: Path) -> list[dict[str, Any]]:
    prefix = str(directory) + os.sep
    return [entry for entry in entries if str(entry["path"]).startswith(prefix)]


def largest_file_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(entries, key=lambda row: (-int(row["size_bytes"]), str(row["path"])))[:50]


def project_category_totals(root: Path, entries: list[dict[str, Any]]) -> dict[str, int]:
    categories = {
        "raw_bbo": 0,
        "old_telemetry": 0,
        "duplicate_databases": 0,
        "bitget_data": 0,
        "caches": 0,
        "temp_debug": 0,
        "other": 0,
    }
    for entry in entries:
        if entry.get("planned_action") != "DELETE":
            continue
        name = str(entry["path"]).lower()
        reason = str(entry["reason"]).lower()
        value = int(entry["size_bytes"])
        if "bitget" in name or "bitget" in reason:
            categories["bitget_data"] += value
        elif any(marker in name or marker in reason for marker in ("bbo", "tick", "orderbook")):
            categories["raw_bbo"] += value
        elif "cache" in reason or "pyc" in reason:
            categories["caches"] += value
        elif any(marker in name or marker in reason for marker in ("tmp", "debug", "pid", "lock")):
            categories["temp_debug"] += value
        elif "database" in reason or "duplicate" in reason:
            categories["duplicate_databases"] += value
        elif "telemetry" in reason or "historical raw" in reason:
            categories["old_telemetry"] += value
        else:
            categories["other"] += value
    return categories


def disk_snapshot(root: Path, entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    usage = shutil.disk_usage(root)
    files = entries if entries is not None else []
    by_top: dict[str, int] = defaultdict(int)
    for entry in files:
        try:
            relative = Path(str(entry["path"])).relative_to(root)
            if relative.parts:
                by_top[relative.parts[0]] += int(entry["size_bytes"])
        except ValueError:
            continue
    logs_size = sum(value for key, value in by_top.items() if key == "logs")
    reports_size = sum(value for key, value in by_top.items() if key == "reports")
    database_size = sum(int(entry["size_bytes"]) for entry in files if entry.get("file_type") == "sqlite_database")
    raw_size = sum(int(entry["size_bytes"]) for entry in files if entry.get("file_type") in {"sqlite_database", "sqlite_wal", "sqlite_shm", "sqlite_journal"})
    project_size = sum(int(entry["size_bytes"]) for entry in files)
    return {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "total_gb": size_gb(usage.total),
        "used_gb": size_gb(usage.used),
        "free_gb": size_gb(usage.free),
        "project_size_bytes": project_size,
        "project_size_gb": size_gb(project_size),
        "logs_size_bytes": logs_size,
        "logs_size_gb": size_gb(logs_size),
        "reports_size_bytes": reports_size,
        "reports_size_gb": size_gb(reports_size),
        "database_size_bytes": database_size,
        "database_size_gb": size_gb(database_size),
        "raw_telemetry_size_bytes": raw_size,
        "raw_telemetry_size_gb": size_gb(raw_size),
        "observed_at_utc": utc_now(),
    }


def write_disk_usage(root: Path, path: Path, snapshot: dict[str, Any], *, processes: list[dict[str, Any]], open_paths: dict[str, list[int]], entries: list[dict[str, Any]], label: str) -> None:
    target_gb = max(20.0, snapshot["total_gb"] * 0.10)
    lines = [
        f"PHASE 0 STORAGE AUDIT — {label}",
        f"observed_at_utc: {snapshot['observed_at_utc']}",
        f"project_root: {root}",
        "units: GiB = bytes / 1024^3; file and directory rankings use allocated disk bytes where available",
        "",
        f"FILESYSTEM TOTAL SIZE: {snapshot['total_gb']:.6f} GiB ({snapshot['total_bytes']} bytes)",
        f"FILESYSTEM USED: {snapshot['used_gb']:.6f} GiB ({snapshot['used_bytes']} bytes)",
        f"FILESYSTEM FREE: {snapshot['free_gb']:.6f} GiB ({snapshot['free_bytes']} bytes)",
        f"PROJECT TOTAL SIZE: {snapshot['project_size_gb']:.6f} GiB ({snapshot['project_size_bytes']} bytes)",
        f"PROJECT LOGS SIZE: {snapshot['logs_size_gb']:.6f} GiB ({snapshot['logs_size_bytes']} bytes)",
        f"PROJECT REPORTS SIZE: {snapshot['reports_size_gb']:.6f} GiB ({snapshot['reports_size_bytes']} bytes)",
        f"PROJECT DATABASE SIZE: {snapshot['database_size_gb']:.6f} GiB ({snapshot['database_size_bytes']} bytes)",
        f"PROJECT RAW TELEMETRY SIZE: {snapshot['raw_telemetry_size_gb']:.6f} GiB ({snapshot['raw_telemetry_size_bytes']} bytes)",
        "",
        f"FREE-SPACE TARGET (max 20 GiB or 10% filesystem capacity): {target_gb:.6f} GiB",
        "LONG-RUN MINIMUM: 15 GiB; preferred: 20 GiB",
        f"SAFE_FOR_NEW_LONG_RUN_NOW: {'YES' if snapshot['free_gb'] >= 15 else 'NO'}",
        f"INSUFFICIENT_FREE_DISK_FOR_LONG_RUN: {'YES' if snapshot['free_gb'] < 15 else 'NO'}",
        "",
        f"PROJECT PROCESSES OBSERVED: {len(processes)}",
        *[f"PID {item['pid']} PPID {item['ppid']} elapsed {item['elapsed']} run_id {item.get('run_id') or ''} command {item['command']}" for item in processes],
        f"OPEN PROJECT PATHS OBSERVED: {len(open_paths)}",
        *[f"{path} <- PIDs {pids}" for path, pids in sorted(open_paths.items())],
        "",
        "CURRENT RUNTIME / DATABASE DIRECTORIES:",
    ]
    runtime_dirs = sorted({str(Path(str(entry["path"])).parent) for entry in entries if entry.get("file_type", "").startswith("sqlite") or Path(str(entry["path"])).name in {"state.json", "run_metadata.json"}})
    lines.extend(f"- {item}" for item in runtime_dirs)
    write_text_atomic(path, "\n".join(lines) + "\n")


def report_before(root: Path, output: Path, contexts: list[dict[str, Any]], processes: list[dict[str, Any]], open_paths: dict[str, list[int]]) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    # Resolve statuses before classifying databases and their sidecars.
    rows = inventory_rows(root, contexts, processes, open_paths)
    capture_failed_run_evidence(root, contexts, output)
    entries = build_manifest(root, contexts, processes, open_paths, exclude_output=True)
    snapshot = disk_snapshot(root, entries)
    write_disk_usage(root, output / "disk_usage_before.txt", snapshot, processes=processes, open_paths=open_paths, entries=entries, label="BEFORE MUTATION")
    write_csv(output / "largest_files_before.csv", largest_file_rows(entries), ["path", "size_bytes", "size_gb", "modified_time", "run_id", "file_type", "classification"])
    write_csv(output / "largest_directories_before.csv", directory_rows(root, contexts, entries, exclude_output=True), ["path", "size_bytes", "size_gb", "modified_time", "run_id", "file_type", "classification"])
    write_csv(output / "historical_run_inventory.csv", rows, list(rows[0].keys()) if rows else ["run_id"])
    manifest_fields = ["path", "size_bytes", "size_gb", "modified_time", "run_id", "classification", "reason", "planned_action"]
    write_csv(output / "cleanup_manifest_before.csv", entries, manifest_fields)
    summary = defaultdict(lambda: {"files": 0, "bytes": 0})
    for entry in entries:
        bucket = summary[str(entry["classification"])]
        bucket["files"] += 1
        bucket["bytes"] += int(entry["size_bytes"])
    write_json(output / "audit_before.json", {"generated_at_utc": utc_now(), "project_root": str(root), "disk": snapshot, "active_processes": processes, "open_paths": open_paths, "classification_summary": summary, "planned_compress_bytes": sum(int(item["size_bytes"]) for item in entries if item["planned_action"] == "COMPRESS"), "planned_delete_bytes": sum(int(item["size_bytes"]) for item in entries if item["planned_action"] == "DELETE")})
    return {"entries": entries, "contexts": contexts, "processes": processes, "open_paths": open_paths, "snapshot": snapshot, "inventory": rows}


def archive_bundle(root: Path, database: Path, *, open_paths: dict[str, list[int]]) -> dict[str, Any]:
    database = safe_path(root, database)
    if not database.exists() or database.suffix.lower() not in DB_SUFFIXES:
        return {"path": str(database), "status": "SKIP", "reason": "database source missing or suffix is not a supported SQLite database"}
    source_paths = [database]
    for suffix in SIDECAR_SUFFIXES:
        sidecar = database.with_name(database.name + suffix) if suffix.startswith("-") else database.with_name(database.name + suffix)
        if sidecar.exists() and sidecar.is_file():
            source_paths.append(sidecar)
    if any(str(path.resolve(strict=False)) in open_paths for path in source_paths):
        return {"path": str(database), "status": "ACTIVE_SKIP", "reason": "database bundle became open before archive"}
    try:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1.0)
        connection.execute("PRAGMA query_only=ON")
        source_tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        connection.close()
    except (OSError, sqlite3.DatabaseError) as exc:
        return {"path": str(database), "status": "SKIP", "reason": f"closed SQLite source readability failed: {type(exc).__name__}: {exc}"}
    expected_files = [str(path.relative_to(root)) for path in source_paths]
    archive = database.with_name(database.name + ".tar.zst")
    archive = safe_path(root, archive)
    zstd_path = shutil.which("zstd") or "/opt/homebrew/bin/zstd"
    if not Path(zstd_path).is_file():
        return {"path": str(database), "status": "SKIP", "reason": "zstd executable is unavailable"}
    if archive.exists():
        test = run_command([zstd_path, "-t", str(archive)], timeout=60.0)
        if test.returncode != 0:
            return {"path": str(database), "status": "SKIP", "reason": "existing archive failed zstd verification"}
    else:
        temporary = archive.with_name(f".{archive.name}.tmp.{os.getpid()}")
        returncode, _, stderr = create_tar_zstd(root, source_paths, temporary, zstd_path)
        if returncode != 0 or not temporary.exists() or allocated_size(temporary) == 0:
            if temporary.exists():
                temporary.unlink()
            return {"path": str(database), "status": "SKIP", "reason": f"archive command failed: {stderr.strip()}"}
        os.replace(temporary, archive)
    test = run_command([zstd_path, "-t", str(archive)], timeout=60.0)
    listing_returncode, listing_lines, listing_stderr = read_tar_zstd_listing(archive, zstd_path)
    listed_names = {line.strip().lstrip("./") for line in listing_lines if line.strip()}
    expected_names = {name.lstrip("./") for name in expected_files}
    if test.returncode != 0 or listing_returncode != 0 or not archive.exists() or allocated_size(archive) == 0 or not expected_names.issubset(listed_names):
        return {"path": str(database), "status": "SKIP", "reason": f"archive verification failed: {listing_stderr.strip()}", "archive": str(archive), "expected_files": expected_files, "listed_files": sorted(listed_names)}
    # The closed source connection above proves that SQLite can read the
    # database without allocating a second full-size copy. zstd frame and
    # tar-member verification proves the exact source bundle can be read back
    # from the archive. A full extracted quick_check is intentionally omitted
    # because it would temporarily double the largest database on this nearly
    # full filesystem.
    for path in source_paths:
        current = safe_path(root, path)
        if str(current.resolve(strict=False)) in open_paths:
            return {"path": str(database), "status": "ARCHIVE_KEEP_SOURCE", "reason": "bundle became open after archive verification", "archive": str(archive)}
    source_sizes = {str(path): allocated_size(path) for path in source_paths if path.exists()}
    source_bytes = sum(source_sizes.values())
    for path in source_paths:
        safe_path(root, path).unlink()
    return {"path": str(database), "status": "ARCHIVED_AND_SOURCE_REMOVED", "archive": str(archive), "expected_file_count": len(expected_files), "archive_size_bytes": allocated_size(archive), "source_tables": source_tables, "source_bytes_removed": source_bytes, "source_sizes": source_sizes, "source_files_removed": expected_files}


def execute_cleanup(root: Path, output: Path, before: dict[str, Any]) -> dict[str, Any]:
    entries = before["entries"]
    processes, active_pids = discover_processes(root)
    open_paths = discover_open_paths(active_pids)
    if processes or open_paths:
        print("ACTIVE_PROJECT_PROCESS_OR_OPEN_HANDLE_OBSERVED", file=sys.stderr)
    deleted: list[dict[str, Any]] = []
    existing_deleted: list[dict[str, Any]] = []
    deleted_log = output / "deleted_files.csv"
    if deleted_log.is_file():
        with deleted_log.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                row["bytes_reclaimed"] = int(row.get("bytes_reclaimed") or 0)
                row["category"] = category_for_entry(row)
                existing_deleted.append(row)
    for entry in entries:
        if entry["planned_action"] != "DELETE":
            continue
        path = safe_path(root, Path(entry["path"]))
        if not path.exists() or path.is_symlink():
            continue
        if str(path.resolve(strict=False)) in open_paths:
            continue
        if file_type(path) in {"sqlite_wal", "sqlite_shm", "sqlite_journal"} and allocated_size(path) != 0:
            continue
        bytes_reclaimed = allocated_size(path)
        path.unlink()
        deleted.append({"absolute_path": str(path), "bytes_reclaimed": bytes_reclaimed, "reason": entry["reason"], "timestamp": utc_now(), "category": category_for_entry(entry)})
    db_entries = [entry for entry in entries if entry["planned_action"] == "COMPRESS" and entry["file_type"] == "sqlite_database"]
    archive_results: list[dict[str, Any]] = []
    entry_by_path = {str(Path(entry["path"])): entry for entry in entries}
    for entry in sorted(db_entries, key=lambda item: int(item["size_bytes"]), reverse=True):
        path = Path(entry["path"])
        result = archive_bundle(root, path, open_paths=open_paths)
        archive_results.append(result)
        if result.get("status") == "ARCHIVED_AND_SOURCE_REMOVED":
            for source_path, bytes_reclaimed in (result.get("source_sizes") or {}).items():
                source_entry = entry_by_path.get(source_path)
                deleted.append(
                    {
                        "absolute_path": source_path,
                        "bytes_reclaimed": int(bytes_reclaimed),
                        "reason": (source_entry or entry)["reason"],
                        "timestamp": utc_now(),
                        "category": "old_telemetry",
                    }
                )
    all_deleted = existing_deleted + deleted
    write_csv(deleted_log, all_deleted, ["absolute_path", "bytes_reclaimed", "reason", "timestamp"])
    return {"archive_results": archive_results, "deleted": all_deleted, "recovered_by_archives": sum(int(item.get("source_bytes_removed", 0)) for item in archive_results if item.get("status") == "ARCHIVED_AND_SOURCE_REMOVED"), "deleted_bytes": sum(int(item["bytes_reclaimed"]) for item in all_deleted)}


def category_for_entry(entry: dict[str, Any]) -> str:
    name = str(entry.get("path", entry.get("absolute_path", ""))).lower()
    reason = str(entry["reason"]).lower()
    if "bitget" in name or "bitget" in reason:
        return "bitget_data"
    if any(marker in name or marker in reason for marker in ("bbo", "tick", "orderbook")):
        return "raw_bbo"
    if "cache" in reason or "pyc" in reason:
        return "caches"
    if any(marker in name or marker in reason for marker in ("tmp", "debug", "pid", "lock")):
        return "temp_debug"
    if entry.get("file_type") == "sqlite_database" or "database" in reason:
        return "duplicate_databases"
    if "telemetry" in reason or "historical raw" in reason:
        return "old_telemetry"
    return "other"


def after_reports(root: Path, output: Path, before_snapshot: dict[str, Any], cleanup: dict[str, Any]) -> dict[str, Any]:
    contexts = build_run_contexts(root)
    processes, active_pids = discover_processes(root)
    open_paths = discover_open_paths(active_pids)
    entries = build_manifest(root, contexts, processes, open_paths, exclude_output=False)
    snapshot = disk_snapshot(root, entries)
    write_disk_usage(root, output / "disk_usage_after.txt", snapshot, processes=processes, open_paths=open_paths, entries=entries, label="AFTER MUTATION")
    write_csv(output / "largest_files_after.csv", largest_file_rows(entries), ["path", "size_bytes", "size_gb", "modified_time", "run_id", "file_type", "classification"])
    write_csv(output / "largest_directories_after.csv", directory_rows(root, contexts, entries, exclude_output=False), ["path", "size_bytes", "size_gb", "modified_time", "run_id", "file_type", "classification"])
    manifest_fields = ["path", "size_bytes", "size_gb", "modified_time", "run_id", "classification", "reason", "planned_action"]
    write_csv(output / "cleanup_manifest_after.csv", entries, manifest_fields)
    target_gb = max(20.0, snapshot["total_gb"] * 0.10)
    summary = {
        "status": "EMERGENCY PROJECT STORAGE CLEANUP COMPLETE",
        "observed_at_utc": utc_now(),
        "filesystem_free_before_gb": before_snapshot["free_gb"],
        "filesystem_free_after_gb": snapshot["free_gb"],
        "total_space_recovered_gb": round(before_snapshot["free_gb"] and (snapshot["free_bytes"] - before_snapshot["free_bytes"]) / (1024**3), 6),
        "project_size_before_gb": before_snapshot["project_size_gb"],
        "project_size_after_gb": snapshot["project_size_gb"],
        "deleted_categories_gb": {category: size_gb(sum(int(item["bytes_reclaimed"]) for item in cleanup["deleted"] if item["category"] == category)) for category in ("raw_bbo", "old_telemetry", "duplicate_databases", "bitget_data", "caches", "temp_debug", "other")},
        "compressed_source_gb": size_gb(cleanup["recovered_by_archives"]),
        "compressed_archive_gb": size_gb(sum(int(item.get("archive_size_bytes", 0)) for item in cleanup["archive_results"])),
        "archive_results": cleanup["archive_results"],
        "deleted_file_count": len(cleanup["deleted"]),
        "target_free_gb": target_gb,
        "project_cleanup_insufficient": snapshot["free_gb"] < target_gb,
        "insufficient_free_disk_for_long_run": snapshot["free_gb"] < 15.0,
        "safe_for_new_long_run": snapshot["free_gb"] >= 15.0,
        "preferred_free_disk_met": snapshot["free_gb"] >= 20.0,
        "preservation_checks": {
            "source_code": (root / "src").is_dir(),
            "config": (root / "conf").is_dir(),
            "final_reports": (root / "reports").is_dir(),
            "canonical_fills": any("fill" in path.name.lower() for path in walk_files(root / "reports", exclude_output=False)) if (root / "reports").exists() else False,
            "canonical_markouts": any("markout" in path.name.lower() for path in walk_files(root / "reports", exclude_output=False)) if (root / "reports").exists() else False,
            "failed_run_root_cause_evidence": (output / "failed_run_root_cause_capture.md").is_file() and (output / "failed_run_root_cause_capture.json").is_file(),
        },
        "policy": {
            "raw_retention_seconds": 180,
            "feature_persist_interval_seconds": 1,
            "summary_interval_seconds": 60,
            "hold_events": "aggregate_only",
            "unchanged_decisions": "aggregate_only",
            "raw_market_data": "store_once_not_once_per_model",
            "closed_chunks": "compress_or_prune",
            "bitget_active_telemetry": False,
        },
        "work_order": ["PHASE 0 CLEANUP", "PHASE 1 SQLITE ROOT-CAUSE AUDIT", "PHASE 2 SINGLE-WRITER REPAIR", "PHASE 3 CONCURRENCY STRESS", "PHASE 4 15-MINUTE PREFLIGHT", "PHASE 5 NEW 6-HOUR RUN ONLY IF ALL PRIOR PHASES PASS"],
        "new_long_run_started": False,
    }
    write_json(output / "cleanup_summary.json", summary)
    lines = [
        "EMERGENCY PROJECT STORAGE CLEANUP COMPLETE",
        "",
        f"FREE SPACE BEFORE: {before_snapshot['free_gb']:.6f} GiB",
        f"FREE SPACE AFTER: {snapshot['free_gb']:.6f} GiB",
        f"TOTAL SPACE RECOVERED: {summary['total_space_recovered_gb']:.6f} GiB",
        f"PROJECT SIZE BEFORE: {before_snapshot['project_size_gb']:.6f} GiB",
        f"PROJECT SIZE AFTER: {snapshot['project_size_gb']:.6f} GiB",
        "",
        "DELETED",
    ]
    for category, value in summary["deleted_categories_gb"].items():
        lines.append(f"{category.upper()}: {value:.6f} GiB")
    lines.extend(
        [
            "",
            f"COMPRESSED SOURCE: {summary['compressed_source_gb']:.6f} GiB",
            f"COMPRESSED ARCHIVES: {summary['compressed_archive_gb']:.6f} GiB",
            "",
            f"PROJECT_CLEANUP_INSUFFICIENT: {'YES' if summary['project_cleanup_insufficient'] else 'NO'}",
            f"INSUFFICIENT_FREE_DISK_FOR_LONG_RUN: {'YES' if summary['insufficient_free_disk_for_long_run'] else 'NO'}",
            f"SAFE FOR NEW LONG RUN: {'YES' if summary['safe_for_new_long_run'] else 'NO'}",
            f"PREFERRED 20 GiB FREE TARGET MET: {'YES' if summary['preferred_free_disk_met'] else 'NO'}",
            "",
            "PRESERVED:",
            f"SOURCE CODE: {'YES' if summary['preservation_checks']['source_code'] else 'NO'}",
            f"CONFIG: {'YES' if summary['preservation_checks']['config'] else 'NO'}",
            f"FINAL REPORTS: {'YES' if summary['preservation_checks']['final_reports'] else 'NO'}",
            f"CANONICAL FILLS: {'YES' if summary['preservation_checks']['canonical_fills'] else 'NO'}",
            f"CANONICAL MARKOUTS: {'YES' if summary['preservation_checks']['canonical_markouts'] else 'NO'}",
            f"FAILED-RUN ROOT-CAUSE EVIDENCE: {'YES' if summary['preservation_checks']['failed_run_root_cause_evidence'] else 'NO'}",
            "",
            "NEW LONG RUN STARTED: NO",
            "NEXT ORDER: Phase 1 SQLite root-cause audit.",
            "",
        ]
    )
    write_text_atomic(output / "cleanup_summary.txt", "\n".join(lines))
    return {"summary": summary, "snapshot": snapshot, "entries": entries}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Phase 0 project-local storage recovery")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("command", choices=("audit", "execute"))
    args = parser.parse_args(argv)
    root = canonical_root(args.project_root)
    output = safe_path(root, root / OUTPUT_RELATIVE)
    contexts = build_run_contexts(root)
    processes, active_pids = discover_processes(root)
    open_paths = discover_open_paths(active_pids)
    if args.command == "audit":
        result = report_before(root, output, contexts, processes, open_paths)
        print(json.dumps({"output": str(output), "disk": result["snapshot"], "active_processes": processes, "open_path_count": len(open_paths), "run_count": len(result["inventory"]), "planned_compress_bytes": sum(int(item["size_bytes"]) for item in result["entries"] if item["planned_action"] == "COMPRESS"), "planned_delete_bytes": sum(int(item["size_bytes"]) for item in result["entries"] if item["planned_action"] == "DELETE")}, indent=2))
        return 0
    before_path = output / "cleanup_manifest_before.csv"
    if not before_path.is_file():
        print(f"missing required before manifest: {before_path}", file=sys.stderr)
        return 2
    before_rows: list[dict[str, Any]] = []
    with before_path.open(encoding="utf-8", newline="") as handle:
        before_rows = list(csv.DictReader(handle))
    try:
        for row in before_rows:
            if row.get("planned_action") in {"COMPRESS", "DELETE"}:
                target = Path(row["path"])
                if target.resolve(strict=False) != safe_path(root, target):
                    print(f"unsafe or non-canonical mutation target present: {target}", file=sys.stderr)
                    return 2
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    entries = []
    for row in before_rows:
        row["size_bytes"] = int(row.get("size_bytes") or 0)
        row["planned_action"] = row.get("planned_action", "KEEP")
        row["file_type"] = file_type(Path(row["path"]))
        entries.append(row)
    before_entries = {"entries": entries}
    snapshot_before = disk_snapshot(root, entries)
    cleanup = execute_cleanup(root, output, before_entries)
    result = after_reports(root, output, snapshot_before, cleanup)
    print(json.dumps(result["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
