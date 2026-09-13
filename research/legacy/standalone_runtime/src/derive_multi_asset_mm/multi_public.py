"""Public multi-venue reference discovery and websocket normalization.

The installed Hummingbot connector symbol maps are preferred as the canonical
symbol source. If that optional public connector probe is unavailable, a
venue-specific exact symbol is admitted only when public exchange REST
metadata proves that the market is active and is the expected USDT perpetual.
No connector credentials or private exchange methods are used here.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .config import RuntimeConfig
from .models import BookSnapshot

VENUES = ("binance", "bybit", "okx", "bitget")
CONNECTORS = {venue: f"{venue}_perpetual" for venue in VENUES}
URLS = {
    "binance": "wss://fstream.binance.com/stream",
    "bybit": "wss://stream.bybit.com/v5/public/linear",
    "okx": "wss://ws.okx.com:8443/ws/v5/public",
    "bitget": "wss://ws.bitget.com/v2/ws/public",
}


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def public_get(url: str, timeout: float = 20.0) -> Any:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "derive-mm-v2-public"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _connector_maps() -> tuple[list[dict[str, Any]], list[str]]:
    """Load exact symbol maps from the installed Hummingbot API image."""

    script = Path(__file__).resolve().parents[2] / "scripts/public_connector_discovery.py"
    try:
        result = subprocess.run(
            ["docker", "exec", "-i", "hummingbot-api", "python", "-"],
            input=script.read_text(encoding="utf-8"),
            capture_output=True,
            text=True,
            timeout=65,
            check=True,
        )
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        payload = json.loads(lines[-1]) if lines else []
        return (payload if isinstance(payload, list) else []), []
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, IndexError) as exc:
        return [], [f"connector_discovery:{type(exc).__name__}"]


def _symbols_by_connector(installed: list[dict[str, Any]]) -> dict[str, dict[str, list[str]]]:
    result: dict[str, dict[str, list[str]]] = {}
    for venue in installed:
        connector = str(venue.get("connector", ""))
        symbols = venue.get("symbols", {})
        if not connector or not isinstance(symbols, dict):
            continue
        result[connector] = {
            str(asset).upper(): sorted({str(symbol) for symbol in values if symbol})
            for asset, values in symbols.items()
            if isinstance(values, list)
        }
    return result


def _rows_from_payload(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    current: Any = payload
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
    if isinstance(current, list):
        return [row for row in current if isinstance(row, dict)]
    return []


def _query(url: str, params: dict[str, str], timeout: float) -> Any:
    separator = "&" if "?" in url else "?"
    return public_get(f"{url}{separator}{urllib.parse.urlencode(params)}", timeout=timeout)


def _metadata_by_venue(config: RuntimeConfig) -> tuple[dict[str, dict[str, dict[str, Any]]], list[str]]:
    """Fetch public contract metadata once per configured venue."""

    errors: list[str] = []
    metadata: dict[str, dict[str, dict[str, Any]]] = {venue: {} for venue in config.reference_venues}
    for venue in config.reference_venues:
        try:
            if venue == "binance":
                payload = public_get(config.binance_exchange_info_url)
                rows = _rows_from_payload(payload, "symbols")
            elif venue == "bybit":
                payload = _query(config.bybit_instruments_url, {"limit": "1000"}, 20.0)
                rows = _rows_from_payload(payload, "result", "list")
            elif venue == "okx":
                payload = public_get(config.okx_instruments_url)
                rows = _rows_from_payload(payload, "data")
            elif venue == "bitget":
                payload = public_get(config.bitget_contracts_url)
                rows = _rows_from_payload(payload, "data")
            else:  # pragma: no cover - RuntimeConfig validates this
                rows = []
            for row in rows:
                symbol = str(row.get("symbol") or row.get("instId") or row.get("inst_id") or "").upper()
                if symbol:
                    metadata[venue][symbol] = row
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{venue}_metadata:{type(exc).__name__}")
    return metadata, errors


def _metadata_ready(venue: str, row: dict[str, Any] | None, asset: str) -> tuple[bool, str]:
    if row is None:
        return False, "REFERENCE_METADATA_UNAVAILABLE"
    if venue == "binance":
        ready = (
            str(row.get("status", "")).upper() == "TRADING"
            and str(row.get("contractType", "")).upper() == "PERPETUAL"
            and str(row.get("quoteAsset", "")).upper() == "USDT"
            and str(row.get("baseAsset", "")).upper() == asset
        )
    elif venue == "bybit":
        ready = (
            str(row.get("status", "")).lower() == "trading"
            and str(row.get("quoteCoin", "")).upper() == "USDT"
            and str(row.get("settleCoin", "")).upper() == "USDT"
            and str(row.get("baseCoin", "")).upper() == asset
        )
    elif venue == "okx":
        ready = (
            str(row.get("state", "")).lower() == "live"
            and str(row.get("ctValCcy", "")).upper() == asset
            and str(row.get("settleCcy", "")).upper() == "USDT"
            and str(row.get("instType", "")).upper() == "SWAP"
        )
    else:
        ready = (
            str(row.get("symbolStatus", row.get("symbol_status", ""))).lower() in {"normal", "trading"}
            and str(row.get("baseCoin", row.get("base_coin", ""))).upper() == asset
            and str(row.get("quoteCoin", row.get("quote_coin", ""))).upper() == "USDT"
        )
    return (True, "") if ready else (False, "CONTRACT_CONVENTION_UNVERIFIED")


def _public_metadata_fallback_symbol(venue: str, asset: str, metadata: dict[str, dict[str, dict[str, Any]]]) -> str | None:
    """Resolve only the standard linear USDT symbol already present in public metadata."""

    symbol = {
        "binance": f"{asset}USDT",
        "bybit": f"{asset}USDT",
        "okx": f"{asset}-USDT-SWAP",
        "bitget": f"{asset}USDT",
    }.get(venue)
    if symbol is None or symbol.upper() not in metadata.get(venue, {}):
        return None
    return symbol.upper()


def _metadata_fields(venue: str, row: dict[str, Any] | None) -> dict[str, str]:
    row = row or {}
    if venue == "binance":
        filters = {str(item.get("filterType")): item for item in row.get("filters", []) if isinstance(item, dict)}
        price = filters.get("PRICE_FILTER", {}).get("tickSize")
        amount = filters.get("LOT_SIZE", {}).get("stepSize")
        minimum = filters.get("LOT_SIZE", {}).get("minQty")
        return {
            "tick_size": str(price or ""),
            "amount_step": str(amount or ""),
            "minimum_amount": str(minimum or ""),
            "minimum_notional": str(filters.get("MIN_NOTIONAL", {}).get("notional") or ""),
            "amount_multiplier": "1",
        }
    if venue == "bybit":
        lot = row.get("lotSizeFilter", {}) or {}
        price = row.get("priceFilter", {}) or {}
        return {
            "tick_size": str(price.get("tickSize") or ""),
            "amount_step": str(lot.get("qtyStep") or ""),
            "minimum_amount": str(lot.get("minOrderQty") or ""),
            "minimum_notional": str(lot.get("minNotionalValue") or ""),
            "amount_multiplier": "1",
        }
    if venue == "okx":
        return {
            "tick_size": str(row.get("tickSz") or ""),
            "amount_step": str(row.get("lotSz") or ""),
            "minimum_amount": str(row.get("minSz") or ""),
            "minimum_notional": "",
            "amount_multiplier": str(_decimal(row.get("ctVal"), Decimal("1")) * _decimal(row.get("ctMult"), Decimal("1"))),
        }
    price_place = row.get("pricePlace")
    try:
        tick_size = format(Decimal("1").scaleb(-int(price_place)), "f") if price_place is not None else ""
    except (TypeError, ValueError):
        tick_size = ""
    return {
        "tick_size": tick_size,
        "amount_step": str(row.get("sizeMultiplier") or "1"),
        "minimum_amount": str(row.get("minTradeNum") or row.get("minTradeAmount") or ""),
        "minimum_notional": "",
        "amount_multiplier": "1",
    }


def discover_references_with_report(
    assets: list[str] | tuple[str, ...], config: RuntimeConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Discover exact installed-connector markets and validate public status."""

    installed, errors = _connector_maps()
    by_connector = _symbols_by_connector(installed)
    metadata, metadata_errors = _metadata_by_venue(config)
    errors.extend(metadata_errors)
    rows: list[dict[str, Any]] = []
    for asset_value in assets:
        asset = str(asset_value).strip().upper()
        for venue in config.reference_venues:
            connector = CONNECTORS[venue]
            candidates = by_connector.get(connector, {}).get(asset, [])
            symbol = candidates[0] if len(candidates) == 1 else None
            symbol_source = "INSTALLED_HUMMINGBOT_CONNECTOR" if symbol else ""
            if symbol is None:
                symbol = _public_metadata_fallback_symbol(venue, asset, metadata)
                if symbol is not None:
                    symbol_source = "PUBLIC_METADATA_EXACT_FALLBACK"
            reason = "" if symbol else "REFERENCE_SYMBOL_UNAVAILABLE"
            status = "READY" if symbol else "REFERENCE_UNAVAILABLE"
            raw = metadata.get(venue, {}).get(str(symbol).upper()) if symbol else None
            if symbol:
                ready, reason = _metadata_ready(venue, raw, asset)
                status = "READY" if ready else "REFERENCE_UNAVAILABLE"
            fields = _metadata_fields(venue, raw)
            rows.append(
                {
                    "asset": asset,
                    "venue": venue,
                    "connector": connector,
                    "symbol": symbol,
                    "status": status,
                    "reason": reason,
                    "symbol_source": symbol_source or "UNRESOLVED",
                    "contract_type": "perpetual",
                    "underlying": asset,
                    "quote": "USDT",
                    **fields,
                }
            )
    report = {
        "observed_at": time.time(),
        "configured_venues": list(config.reference_venues),
        "installed_connectors": [str(row.get("connector")) for row in installed],
        "connector_errors": errors,
        "rows": rows,
        "credentials_loaded": False,
        "private_api_used": False,
        "real_orders_created": 0,
        "real_orders_cancelled": 0,
        "real_positions": 0,
    }
    return rows, report


def discover_references(assets: list[str] | tuple[str, ...], config: RuntimeConfig | None = None) -> list[dict[str, Any]]:
    """Compatibility wrapper returning only mapping rows."""

    if config is None:
        config = RuntimeConfig.from_mapping({"assets": {asset: {} for asset in assets}, "max_active_assets": len(assets)})
    rows, _ = discover_references_with_report(assets, config)
    return rows


def subscription(venue: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = [
        str(row["symbol"])
        for row in rows
        if row.get("venue") == venue and row.get("status") == "READY" and row.get("symbol")
    ]
    if venue == "binance":
        return {"method": "SUBSCRIBE", "params": [f"{symbol.lower()}@depth5@100ms" for symbol in symbols], "id": 1}
    if venue == "bybit":
        return {"op": "subscribe", "args": [f"orderbook.1.{symbol}" for symbol in symbols]}
    if venue == "okx":
        return {"op": "subscribe", "args": [{"channel": "books5", "instId": symbol} for symbol in symbols]}
    return {
        "op": "subscribe",
        # Bitget's public v2 endpoint reliably exposes the complete top level
        # on books1; books5 is not enabled for every USDT-FUTURES symbol.
        "args": [{"instType": "USDT-FUTURES", "channel": "books1", "instId": symbol} for symbol in symbols],
    }


def symbol_from_payload(venue: str, payload: dict[str, Any]) -> str | None:
    if venue == "binance":
        data = payload.get("data", payload)
        return str(data.get("s") or data.get("symbol") or "").upper() or None
    if venue == "bybit":
        data = payload.get("data", {})
        return str(data.get("s") or "").upper() or None
    if venue in {"okx", "bitget"}:
        arg = payload.get("arg", {}) if isinstance(payload.get("arg"), dict) else {}
        return str(arg.get("instId") or arg.get("inst_id") or "").upper() or None
    return None


def _levels(value: Any, multiplier: Decimal, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(value, list):
        return ()
    rows: list[tuple[Decimal, Decimal]] = []
    for raw in value:
        if not isinstance(raw, (list, tuple)) or len(raw) < 2:
            continue
        price, amount = _decimal(raw[0]), _decimal(raw[1]) * multiplier
        if not price.is_finite() or not amount.is_finite() or price <= 0 or amount < 0:
            raise ValueError("INVALID_DEPTH")
        if amount > 0:
            rows.append((price, amount))
    return tuple(sorted(rows, key=lambda item: item[0], reverse=reverse))


def _event_seconds(value: Any) -> float | None:
    if value is None:
        return None
    event = _decimal(value)
    if event <= 0:
        return None
    return float(event / Decimal("1000")) if event > Decimal("10000000000") else float(event)


def parse_snapshot(
    venue: str,
    payload: dict[str, Any],
    received: float,
    mapping: dict[str, dict[str, Any]],
) -> tuple[str, BookSnapshot, int | None, int | None, bool] | None:
    """Normalize one complete public snapshot.

    The final boolean marks Bybit's repeated complete snapshot. A repeated
    snapshot may refresh the causal receipt time while preserving the prior
    sequence number and BBO.
    """

    if venue == "binance":
        data = payload.get("data", payload)
        symbol = str(data.get("s") or data.get("symbol") or "").upper()
        bids_raw = data.get("bids", data.get("b"))
        asks_raw = data.get("asks", data.get("a"))
        sequence = data.get("lastUpdateId", data.get("u"))
        previous = data.get("pu")
        exchange_timestamp = data.get("E")
        repeat = False
    elif venue == "bybit":
        if not str(payload.get("topic", "")).startswith("orderbook."):
            return None
        if payload.get("type") not in {"snapshot", "delta"}:
            return None
        data = payload.get("data", {})
        symbol = str(data.get("s") or "").upper()
        bids_raw = data.get("b", data.get("bids"))
        asks_raw = data.get("a", data.get("asks"))
        sequence = data.get("u", data.get("seq"))
        previous = data.get("pu")
        exchange_timestamp = payload.get("ts")
        repeat = payload.get("type") == "snapshot"
    else:
        arg = payload.get("arg", {}) if isinstance(payload.get("arg"), dict) else {}
        if str(arg.get("channel", "")) not in {"books1", "books5"}:
            return None
        data_rows = payload.get("data")
        if not isinstance(data_rows, list) or not data_rows:
            return None
        data = data_rows[0]
        symbol = str(arg.get("instId") or arg.get("inst_id") or "").upper()
        bids_raw = data.get("bids")
        asks_raw = data.get("asks")
        sequence = data.get("seqId", data.get("seq"))
        previous = data.get("prevSeqId", data.get("prev_seq"))
        if venue == "bitget" and previous is None:
            # Bitget v2 names the link pseq and may emit zero on the first
            # update; zero is not evidence of a gap after a fresh snapshot.
            candidate = data.get("pseq")
            previous = candidate if _decimal(candidate) > 0 else None
        exchange_timestamp = data.get("ts")
        repeat = False
    if not symbol:
        return None
    row = mapping.get(symbol)
    if row is None or row.get("status") != "READY":
        raise ValueError("UNVALIDATED_SYMBOL")
    multiplier = _decimal(row.get("amount_multiplier"), Decimal("1"))
    bids = _levels(bids_raw, multiplier, True)
    asks = _levels(asks_raw, multiplier, False)
    if not bids or not asks:
        raise ValueError("EMPTY_BOOK")
    if asks[0][0] < bids[0][0]:
        raise ValueError("CROSSED_BOOK")
    book = BookSnapshot(
        timestamp=received,
        best_bid=bids[0][0],
        best_ask=asks[0][0],
        bid_size=bids[0][1],
        ask_size=asks[0][1],
        bids=bids,
        asks=asks,
        exchange_timestamp=_event_seconds(exchange_timestamp),
        source=venue,
    )
    return (
        row["asset"],
        book,
        int(sequence) if sequence is not None else None,
        int(previous) if previous is not None else None,
        repeat,
    )
