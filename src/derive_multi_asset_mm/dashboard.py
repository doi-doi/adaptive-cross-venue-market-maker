"""Read-only local dashboard server."""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


def _safe_json(path: Path, fallback: dict) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def make_handler(root: Path):
    state_path = root / "logs/mainnet_shadow/state.json"
    report_path = root / "reports/mainnet_shadow/final_multi_asset_shadow_report.json"
    index_path = root / "dashboard/index.html"

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/api/state":
                payload = _safe_json(state_path, {"mode": "MAINNET_SHADOW", "mainnet_armed": False, "real_orders": 0, "real_positions": 0, "mappings": {}})
                self._send_json(payload)
            elif path == "/api/report":
                self._send_json(_safe_json(report_path, {"classification": "NOT_READY_FOR_SMALL_MAINNET_CANARY"}))
            elif path in {"/", "/index.html"}:
                body = index_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

        def _send_json(self, payload: dict) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the read-only Derive MM dashboard")
    parser.add_argument("--port", type=int, default=8770)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args(argv)
    root = Path.cwd()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(root))
    print(f"dashboard listening at http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
