#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
python_bin="${PYTHON_BIN:-python3.11}"
if [[ -x "$project_root/.venv/bin/python" && -z "${PYTHON_BIN:-}" ]]; then
  python_bin="$project_root/.venv/bin/python"
fi
exec "$python_bin" -m derive_multi_asset_mm.dashboard --host "${DASHBOARD_HOST:-127.0.0.1}" --port "${DASHBOARD_PORT:-8770}"
