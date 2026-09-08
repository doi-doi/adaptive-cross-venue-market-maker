"""Public Derive/Binance discovery and websocket message normalization.

This module has no credential, private-method, order, cancel, or account-state
path. It is intentionally separate from the Hummingbot connector adapter.
"""

from __future__ import annotations

import json
import time
import urllib.request
from decimal import Decimal, InvalidOperation
from typing import Any

from .config import RuntimeConfig
from .models import AssetMapping, BookSnapshot, DeriveRules, Side, TradePrint


class PublicDataError(RuntimeError):
    """A public endpoint failed or returned an unusable schema."""


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        result = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation):
        return default
    return result if result.is_finite() else default


class DerivePublicClient:
    ALLOWED_METHODS = {"public/get_all_instruments", "public/get_tickers"}

    def __init__(self, base_url: str, timeout_seconds: float = 20.0) -> None:
        if not base_url.startswith("https://"):
            raise ValueError("Derive public client requires https")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def post(self, method: str, params: dict[str, Any]) -> Any:
        if method not in self.ALLOWED_METHODS:
            raise PublicDataError(f"method is outside public allowlist: {method}")
        request = urllib.request.Request(
            f"{self.base_url}/{method}",
            data=json.dumps(params, separators=(",", ":")).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "derive-mm-v2-public"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.load(response)
        except (OSError, TimeoutError, json.JSONDecodeError) as exc:
            raise PublicDataError(f"{method} failed: {type(exc).__name__}") from exc
        if not isinstance(payload, dict) or payload.get("error") is not None or "result" not in payload:
            raise PublicDataError(f"{method} returned an unusable public response")
        return payload["result"]

    def instruments(self) -> list[dict[str, Any]]:
        page = 1
        rows: dict[str, dict[str, Any]] = {}
        while True:
            result = self.post(
                "public/get_all_instruments",
                {"instrument_type": "perp", "expired": False, "page": page, "page_size": 1000},
            )
            page_rows = result if isinstance(result, list) else result.get("instruments", result.get("data", []))
            if not isinstance(page_rows, list):
                raise PublicDataError("public/get_all_instruments has no instrument list")
            for row in page_rows:
                if isinstance(row, dict) and row.get("instrument_name"):
                    rows[str(row["instrument_name"]).upper()] = row
            pagination = result.get("pagination") if isinstance(result, dict) else None
            pages = int(pagination.get("num_pages", page)) if isinstance(pagination, dict) else page
            if page >= pages or len(page_rows) < 1000:
                break
            page += 1
        return [rows[key] for key in sorted(rows)]

    def ticker(self, currency: str) -> dict[str, dict[str, Any]]:
        result = self.post("public/get_tickers", {"instrument_type": "perp", "currency": currency})
        if isinstance(result, list):
            rows = result
        elif isinstance(result, dict):
            rows = result.get("tickers", result.get("data", []))
            if isinstance(rows, dict):
                rows = list(rows.values())
        else:
            rows = []
        normalized: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            inner = row.get("instrument_ticker", row)
            if isinstance(inner, dict):
                name = str(inner.get("instrument_name", row.get("instrument_name", ""))).upper()
                if name:
                    normalized[name] = inner
        return normalized


class BinancePublicClient:
    def __init__(self, exchange_info_url: str, timeout_seconds: float = 20.0) -> None:
        if not exchange_info_url.startswith("https://"):
            raise ValueError("Binance public client requires https")
        self.exchange_info_url = exchange_info_url
        self.timeout_seconds = timeout_seconds

    def exchange_info(self) -> list[dict[str, Any]]:
        request = urllib.request.Request(
            self.exchange_info_url,
            headers={"Accept": "application/json", "User-Agent": "derive-mm-v2-public"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.load(response)
        except (OSError, TimeoutError, json.JSONDecodeError) as exc:
            raise PublicDataError(f"Binance exchange info failed: {type(exc).__name__}") from exc
        symbols = payload.get("symbols") if isinstance(payload, dict) else None
        return [row for row in symbols if isinstance(row, dict)] if isinstance(symbols, list) else []


def _active_derive(row: dict[str, Any]) -> bool:
    return (
        str(row.get("instrument_type", "")).lower() == "perp"
        and bool(row.get("is_active", True))
        and str(row.get("instrument_name", "")).upper().endswith("-PERP")
    )


def _rules_from_row(row: dict[str, Any], ticker: dict[str, Any] | None) -> DeriveRules:
    ticker = ticker or {}
    return DeriveRules(
        instrument_name=str(row.get("instrument_name", "")).upper(),
        base_asset=str(row.get("base_currency") or row.get("base_asset") or "").upper(),
        quote_asset=str(row.get("quote_currency") or row.get("quote_asset") or "USD").upper(),
        tick_size=_decimal(row.get("tick_size")),
        amount_step=_decimal(row.get("amount_step")),
        minimum_amount=_decimal(row.get("minimum_amount")),
        maximum_amount=(_decimal(row["maximum_amount"]) if row.get("maximum_amount") is not None else None),
        minimum_notional=_decimal(
            row.get("minimum_order_notional", row.get("min_order_notional", row.get("minimum_notional")))
        ),
        maker_fee_bps=(
            _decimal(row["maker_fee_rate"]) * Decimal("10000") if row.get("maker_fee_rate") is not None else None
        ),
        taker_fee_bps=(
            _decimal(row["taker_fee_rate"]) * Decimal("10000") if row.get("taker_fee_rate") is not None else None
        ),
    )


def discover_mappings(config: RuntimeConfig) -> tuple[dict[str, AssetMapping], dict[str, Any]]:
    """Validate only exact active Derive instruments and exact Binance USD-M perps."""

    derive_rows: list[dict[str, Any]] = []
    binance_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        derive_rows = DerivePublicClient(config.derive_public_url).instruments()
    except PublicDataError as exc:
        errors.append(str(exc))
    try:
        binance_rows = BinancePublicClient(config.binance_exchange_info_url).exchange_info()
    except PublicDataError as exc:
        errors.append(str(exc))

    derive_by_asset: dict[str, dict[str, Any]] = {}
    for row in derive_rows:
        if not _active_derive(row):
            continue
        asset = str(row.get("base_currency") or row.get("base_asset") or "").upper()
        instrument = str(row.get("instrument_name", "")).upper()
        if asset and instrument:
            derive_by_asset[asset] = row
    binance_by_symbol = {
        str(row.get("symbol", "")).upper(): row
        for row in binance_rows
        if str(row.get("status", "")).upper() == "TRADING"
        and str(row.get("contractType", "")).upper() == "PERPETUAL"
        and str(row.get("quoteAsset", "")).upper() == "USDT"
    }
    mappings: dict[str, AssetMapping] = {}
    ticker_cache: dict[str, dict[str, Any]] = {}
    for asset in config.enabled_assets:
        symbol = asset.symbol
        derive_row = derive_by_asset.get(symbol)
        derive_instrument = str(derive_row.get("instrument_name")).upper() if derive_row else None
        derive_pair = f"{symbol}-USDC" if derive_instrument else None
        binance_symbol = f"{symbol}USDT"
        if not derive_row:
            mappings[symbol] = AssetMapping(symbol, None, None, binance_symbol, valid=False, reason="DERIVE_INSTRUMENT_UNAVAILABLE")
            continue
        try:
            ticker_cache.setdefault(symbol, DerivePublicClient(config.derive_public_url).ticker(symbol))
        except PublicDataError as exc:
            errors.append(f"{symbol}:{exc}")
        rules = _rules_from_row(derive_row, ticker_cache.get(symbol, {}).get(derive_instrument))
        if not rules.valid():
            mappings[symbol] = AssetMapping(symbol, derive_instrument, derive_pair, binance_symbol, valid=False, reason="DERIVE_RULES_UNAVAILABLE", rules=rules)
            continue
        if binance_symbol not in binance_by_symbol:
            mappings[symbol] = AssetMapping(symbol, derive_instrument, derive_pair, binance_symbol, valid=False, reason="REFERENCE_MARKET_UNAVAILABLE", rules=rules)
            continue
        mappings[symbol] = AssetMapping(symbol, derive_instrument, derive_pair, binance_symbol, reference_available=True, valid=True, reason="READY", rules=rules)
    report = {
        "observed_at": time.time(),
        "derive_instrument_count": len(derive_rows),
        "binance_perpetual_count": len(binance_by_symbol),
        "errors": errors,
        "mappings": mappings,
        "credentials_loaded": False,
        "private_api_used": False,
        "real_orders_created": 0,
        "real_orders_cancelled": 0,
        "real_positions": 0,
    }
    return mappings, report


def _levels(value: Any, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(value, list):
        return ()
    rows = []
    for row in value:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price, amount = _decimal(row[0]), _decimal(row[1])
        if price > 0 and amount >= 0:
            rows.append((price, amount))
    return tuple(sorted(rows, key=lambda item: item[0], reverse=reverse))


def parse_book_message(payload: dict[str, Any], *, source: str, receipt_timestamp: float | None = None) -> tuple[str, BookSnapshot] | None:
    receipt = time.time() if receipt_timestamp is None else receipt_timestamp
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if source == "binance":
        symbol = str(data.get("s") or data.get("symbol") or "").upper()
        # USD-M depth uses ``b``/``a`` arrays; bookTicker uses scalar strings.
        bids = _levels(data.get("bids") or data.get("b"), True)
        asks = _levels(data.get("asks") or data.get("a"), False)
        if not bids and data.get("b") is not None and data.get("a") is not None:
            bids = ((_decimal(data.get("b")), _decimal(data.get("B"), Decimal("0"))),)
            asks = ((_decimal(data.get("a")), _decimal(data.get("A"), Decimal("0"))),)
        if not symbol or not bids or not asks:
            return None
        key = symbol.removesuffix("USDT")
    else:
        params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        data = params.get("data") if isinstance(params.get("data"), dict) else data
        channel = str(params.get("channel", ""))
        key = str(data.get("instrument_name") or data.get("instrument") or (channel.split(".")[1] if "." in channel else "")).upper()
        bids = _levels(data.get("bids"), True)
        asks = _levels(data.get("asks"), False)
        if not key or not bids or not asks:
            return None
    if asks[0][0] < bids[0][0]:
        return None
    event = _decimal(data.get("E", data.get("timestamp")), Decimal(str(receipt)))
    event_timestamp = float(event / Decimal("1000")) if event > Decimal("10000000000") else float(event)
    return key, BookSnapshot(
        timestamp=receipt,
        best_bid=bids[0][0],
        best_ask=asks[0][0],
        bid_size=bids[0][1],
        ask_size=asks[0][1],
        bids=bids,
        asks=asks,
        exchange_timestamp=event_timestamp,
        source=source,
    )


def parse_trade_message(payload: dict[str, Any], *, source: str, receipt_timestamp: float | None = None) -> tuple[str, TradePrint] | None:
    receipt = time.time() if receipt_timestamp is None else receipt_timestamp
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if source == "binance":
        symbol = str(data.get("s") or "").upper()
        price = _decimal(data.get("p"))
        amount = _decimal(data.get("q"))
        # Binance buyer_is_maker=true means the aggressor was a seller.
        side = Side.SELL if bool(data.get("m")) else Side.BUY
        key = symbol.removesuffix("USDT")
        event = _decimal(data.get("T", data.get("E")), Decimal(str(receipt)))
        trade_id = str(data.get("a") or data.get("t") or "")
    else:
        params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        rows = params.get("data") if isinstance(params.get("data"), list) else []
        if not rows:
            return None
        data = rows[0]
        key = str(data.get("instrument_name") or data.get("instrument") or "").upper()
        price = _decimal(data.get("trade_price", data.get("price")))
        amount = _decimal(data.get("trade_amount", data.get("amount", data.get("size"))))
        direction = str(data.get("direction", data.get("aggressor_side", data.get("side", "")))).lower()
        if direction in {"sell", "s", "ask", "short"}:
            side = Side.SELL
        elif direction in {"buy", "b", "bid", "long"}:
            side = Side.BUY
        else:
            return None
        event = _decimal(data.get("timestamp", data.get("trade_timestamp")), Decimal(str(receipt)))
        trade_id = str(data.get("trade_id", data.get("id", "")))
    if not key or price <= 0 or amount <= 0 or not trade_id:
        return None
    event_timestamp = float(event / Decimal("1000")) if event > Decimal("10000000000") else float(event)
    return key, TradePrint(receipt, price, amount, side, trade_id, event_timestamp, source)


def subscriptions(mappings: dict[str, AssetMapping]) -> tuple[list[str], list[str]]:
    ready = [mapping for mapping in mappings.values() if mapping.valid and mapping.derive_instrument and mapping.binance_symbol]
    binance = sorted({f"{mapping.binance_symbol.lower()}@depth5@100ms" for mapping in ready} | {f"{mapping.binance_symbol.lower()}@bookTicker" for mapping in ready} | {f"{mapping.binance_symbol.lower()}@trade" for mapping in ready})
    derive = sorted({f"orderbook.{mapping.derive_instrument}.1.20" for mapping in ready} | {f"trades.{mapping.derive_instrument}" for mapping in ready})
    return binance, derive
