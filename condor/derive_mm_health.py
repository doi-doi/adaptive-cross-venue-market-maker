"""Read-only Condor health routine for the XRP/LINK adaptive MM bot."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, Field, model_validator

CATEGORY = "Monitoring"
CONTINUOUS = True
ASSETS = ("XRP", "LINK")


class Config(BaseModel):
    bot_name: str = Field(default="derive-binance-adaptive-mm-shadow")
    poll_interval_seconds: float = Field(default=3.0, ge=1.0, le=30.0)
    report_auto_refresh_seconds: int = Field(default=3, ge=1, le=30)
    max_controller_drawdown_quote: float = Field(default=25.0, gt=0)
    max_account_drawdown_quote: float = Field(default=40.0, gt=0)
    execution_enabled: bool = Field(default=False)

    @model_validator(mode="after")
    def read_only(self):
        if self.execution_enabled:
            raise ValueError("derive_mm_health is read-only")
        return self


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _find_asset(data: Any, asset: str) -> dict[str, Any]:
    """Find controller custom_info without depending on one API response shape."""

    target = asset.upper()
    if isinstance(data, Mapping):
        if str(data.get("asset", "")).upper() == target:
            return dict(data)
        for value in data.values():
            found = _find_asset(value, target)
            if found:
                return found
    elif isinstance(data, (list, tuple)):
        for value in data:
            found = _find_asset(value, target)
            if found:
                return found
    return {}


def health_snapshot(
    bot_payload: Any,
    now: float | None = None,
    max_controller_drawdown_quote: float = 25.0,
    max_account_drawdown_quote: float = 40.0,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    payload = _mapping(bot_payload)
    if str(payload.get("status", "")).lower() == "success" and _mapping(payload.get("data")):
        payload = _mapping(payload["data"])
    bot_status = str(payload.get("status", "STOPPED")).upper()
    assets = {asset: _find_asset(payload, asset) for asset in ASSETS}
    alerts: list[str] = []
    for asset, row in assets.items():
        operational = str(row.get("operational_state", "ERROR"))
        reason = str(row.get("block_reason", ""))
        if not row:
            alerts.append(f"CONTROLLER_ERROR:{asset}:missing diagnostics")
        if operational == "REFERENCE_PAUSED":
            alerts.append(f"BINANCE_STALE:{asset}:{reason}")
        if operational == "DERIVE_PAUSED":
            alerts.append(f"DERIVE_STALE:{asset}:{reason}")
        if operational == "RISK_PAUSED":
            alerts.append(f"PORTFOLIO_LIMIT:{asset}:{reason}")
        if operational == "ERROR":
            alerts.append(f"CONTROLLER_ERROR:{asset}:{reason}")
        if str(row.get("market_state", "")) == "EXTREME":
            alerts.append(f"EXTREME_VOL:{asset}")
        if str(row.get("inventory_mode", "")) in {"ASK_ONLY", "BID_ONLY"}:
            alerts.append(f"INVENTORY_LIMIT:{asset}")
        mutation_limit = int(row.get("max_quote_mutations_per_minute", 30) or 30)
        if int(row.get("mutations_per_minute", 0) or 0) >= mutation_limit:
            alerts.append(f"EXCESSIVE_CHURN:{asset}")
        if float(row.get("strategy_executor_drawdown", row.get("drawdown", 0)) or 0) >= max_controller_drawdown_quote:
            alerts.append(f"STRATEGY_DRAWDOWN:{asset}")
        if bool(row.get("shadow_mode", False)) and abs(float(row.get("position_notional", 0) or 0)) > 0:
            alerts.append(f"UNEXPECTED_POSITION:{asset}")
        updated = float(row.get("updated_at", 0) or 0)
        if updated and now - updated > 10:
            alerts.append(f"CONTROLLER_ERROR:{asset}:diagnostics stale")
    errors = payload.get("error_logs", []) or []
    for entry in errors[-20:]:
        message = str(_mapping(entry).get("msg", ""))
        upper = message.upper()
        if "REJECT" in upper:
            alerts.append(f"ORDER_REJECT:{message[:120]}")
        elif "ERROR" in upper or "EXCEPTION" in upper:
            alerts.append(f"CONTROLLER_ERROR:{message[:120]}")
    account = next(
        (
            row
            for row in assets.values()
            if any(
                row.get(key) is not None
                for key in (
                    "account_equity",
                    "account_collateral_balance",
                    "account_unrealized_pnl",
                    "account_gross_position_exposure",
                )
            )
        ),
        {},
    )
    account_drawdown = account.get("account_drawdown")
    if account_drawdown is not None and float(account_drawdown) >= max_account_drawdown_quote:
        alerts.append("ACCOUNT_DRAWDOWN_LIMIT")
    if bot_status in {"STOPPED", "NOT_FOUND", "ERROR", "STOPPING"}:
        alerts.append("PROCESS_DOWN")
    elif bot_status == "IDLE":
        alerts.append("CONTROLLER_ERROR:bot heartbeat idle")
    overall = "HEALTHY"
    if any(item.startswith(("PROCESS_DOWN", "UNEXPECTED_POSITION", "ACCOUNT_DRAWDOWN_LIMIT")) for item in alerts):
        overall = "CRITICAL"
    elif any(item.startswith(("BINANCE_STALE", "DERIVE_STALE", "PORTFOLIO_LIMIT")) for item in alerts):
        overall = "PAUSED"
    elif alerts:
        overall = "DEGRADED"
    rows = list(assets.values())
    execution_modes = {"SHADOW" if row.get("shadow_mode") else "LIVE_ARMED" if row.get("mainnet_armed") else "LIVE_DISARMED" for row in rows if row}
    overview = {
        "execution_mode": ", ".join(sorted(execution_modes)) or "UNKNOWN",
        "uptime_seconds": min((float(row.get("uptime_seconds", 0) or 0) for row in rows if row), default=0),
        "last_update": max((float(row.get("updated_at", 0) or 0) for row in rows if row), default=0),
        "strategy_executor_pnl": sum((float(row.get("strategy_executor_pnl", row.get("pnl", 0)) or 0) for row in rows), 0.0),
        "total_pnl": sum((float(row.get("strategy_executor_pnl", row.get("pnl", 0)) or 0) for row in rows), 0.0),
        "total_volume": sum((float(row.get("volume", 0) or 0) for row in rows), 0.0),
        "total_exposure": sum((abs(float(row.get("position_notional", 0) or 0)) for row in rows), 0.0),
        "strategy_executor_drawdown": sum(
            (float(row.get("strategy_executor_drawdown", row.get("drawdown", 0)) or 0) for row in rows), 0.0
        ),
        "drawdown": sum((float(row.get("strategy_executor_drawdown", row.get("drawdown", 0)) or 0) for row in rows), 0.0),
        # Account fields are copied from one controller only. Both controllers
        # observe the same Derive subaccount, so summing would double count it.
        "account_realized_pnl": account.get("account_realized_pnl"),
        "account_unrealized_pnl": account.get("account_unrealized_pnl"),
        "account_equity": account.get("account_equity"),
        "account_collateral_balance": account.get("account_collateral_balance"),
        "available_collateral": account.get("available_collateral"),
        "account_gross_position_exposure": account.get("account_gross_position_exposure"),
        "account_net_position_exposure": account.get("account_net_position_exposure"),
        "account_drawdown": account_drawdown,
        "collateral_balance_drawdown": account.get("collateral_balance_drawdown"),
        "errors": len(payload.get("error_logs", []) or []),
    }
    return {"overall": overall, "bot_status": bot_status, "assets": assets, "alerts": alerts, "overview": overview}


def _asset_row(asset: str, row: Mapping[str, Any]) -> dict[str, Any]:
    position_amount = float(row.get("position_amount", 0) or 0)
    position_side = "LONG" if position_amount > 0 else "SHORT" if position_amount < 0 else "FLAT"
    return {
        "Asset": asset,
        "Derive BBO": str(row.get("derive_bbo", "—")),
        "Derive feed / BBO age": (
            f"{row.get('derive_feed_age_seconds', '—')} / {row.get('derive_bbo_change_age_seconds', '—')}"
        ),
        "Binance BBO": str(row.get("binance_bbo", "—")),
        "Binance feed / BBO age": (
            f"{row.get('binance_feed_age_seconds', '—')} / {row.get('binance_bbo_change_age_seconds', '—')}"
        ),
        "Fair": str(row.get("binance_fair_value", "—")),
        "Basis bps": str(row.get("basis_bps", "—")),
        "State / mode": f"{row.get('market_state', '—')} / {row.get('mm_mode', '—')}",
        "Inventory": f"{row.get('inventory_mode', '—')} {position_side} {row.get('position_notional', '—')}",
        "Projected bid / ask": (
            f"{row.get('projected_position_notional_if_bid_fills', '—')} / "
            f"{row.get('projected_position_notional_if_ask_fills', '—')}"
        ),
        "Risk size bid / ask": (
            f"{row.get('risk_adjusted_bid_order_amount', '—')} / {row.get('risk_adjusted_ask_order_amount', '—')}"
        ),
        "Desired": f"{row.get('desired_bid', '—')} / {row.get('desired_ask', '—')}",
        "Active": f"{row.get('active_bid', '—')} / {row.get('active_ask', '—')}",
        "Quote age": f"{row.get('bid_age', '—')} / {row.get('ask_age', '—')}",
        "Mutations/min": str(row.get("mutations_per_minute", "—")),
        "Creates / replaces / cancels": (
            f"{row.get('creates_per_minute', '—')} / {row.get('replaces_per_minute', '—')} / "
            f"{row.get('cancels_per_minute', '—')}"
        ),
        "Fills / volume": f"{row.get('fills', '—')} / {row.get('volume', '—')}",
        "Strategy PnL": str(row.get("strategy_executor_pnl", row.get("pnl", "—"))),
        "30s / 60s markout": f"{row.get('markout_30s_bps', 'N/A')} / {row.get('markout_60s_bps', 'N/A')}",
    }


async def run(config: Config, context: Any) -> str:
    """Poll existing Hummingbot state; never polls an exchange or mutates a bot."""

    if config.execution_enabled:
        raise RuntimeError("derive_mm_health refused execution_enabled=true")
    from condor.reports import LiveReport
    from config_manager import get_client

    chat_id = getattr(context, "_chat_id", None)
    report = LiveReport(
        "Derive XRP/LINK Adaptive MM Health",
        source_name="derive_mm_health",
        tags=["derive", "xrp", "link", "read-only"],
        auto_refresh_seconds=config.report_auto_refresh_seconds,
    )
    ticks = 0
    prior_alerts: set[str] = set()
    try:
        while True:
            client = await get_client(chat_id, context=context)
            payload: Any = {"status": "STOPPED", "error_logs": []}
            if client is not None:
                try:
                    payload = await client.bot_orchestration.get_bot_status(config.bot_name)
                except Exception as exc:  # fail closed and keep the board alive
                    payload = {"status": "STOPPED", "error_logs": [{"msg": f"{type(exc).__name__}: {exc}"}]}
            snapshot = health_snapshot(
                payload,
                max_controller_drawdown_quote=config.max_controller_drawdown_quote,
                max_account_drawdown_quote=config.max_account_drawdown_quote,
            )
            current_alerts = set(snapshot["alerts"])
            new_alerts = sorted(current_alerts - prior_alerts)
            if new_alerts and chat_id is not None and getattr(context, "bot", None) is not None:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="Derive XRP/LINK MM alert\n" + "\n".join(new_alerts),
                    )
                except Exception:
                    pass
            prior_alerts = current_alerts
            report.clear()
            report.builder.manual_order()
            report.builder.kpi("Overall", snapshot["overall"])
            report.builder.kpi("Bot", snapshot["bot_status"])
            report.builder.kpi("Hummingbot", "HEALTHY" if snapshot["bot_status"] == "RUNNING" else "DEGRADED")
            report.builder.kpi(
                "Derive",
                "STALE" if any(a.startswith("DERIVE_STALE") for a in snapshot["alerts"]) else "HEALTHY",
            )
            report.builder.kpi(
                "Binance",
                "STALE" if any(a.startswith("BINANCE_STALE") for a in snapshot["alerts"]) else "HEALTHY",
            )
            overview = snapshot["overview"]
            report.builder.kpi("Execution mode", overview["execution_mode"])
            report.builder.kpi("Uptime", f"{overview['uptime_seconds']:.0f}s")
            report.builder.kpi("Strategy executor PnL", f"{overview['strategy_executor_pnl']:.4f}")
            report.builder.kpi("Account equity", str(overview["account_equity"] if overview["account_equity"] is not None else "N/A"))
            report.builder.kpi(
                "Collateral balance",
                str(
                    overview["account_collateral_balance"]
                    if overview["account_collateral_balance"] is not None
                    else "N/A"
                ),
            )
            report.builder.kpi(
                "Account unrealized PnL",
                str(overview["account_unrealized_pnl"] if overview["account_unrealized_pnl"] is not None else "N/A"),
            )
            report.builder.kpi(
                "Account drawdown",
                str(overview["account_drawdown"] if overview["account_drawdown"] is not None else "N/A"),
            )
            report.builder.kpi(
                "Account gross / net exposure",
                f"{overview['account_gross_position_exposure'] if overview['account_gross_position_exposure'] is not None else 'N/A'} / "
                f"{overview['account_net_position_exposure'] if overview['account_net_position_exposure'] is not None else 'N/A'}",
            )
            report.builder.kpi(
                "Account realized PnL",
                str(overview["account_realized_pnl"] if overview["account_realized_pnl"] is not None else "N/A"),
            )
            report.builder.kpi(
                "Available collateral",
                str(overview["available_collateral"] if overview["available_collateral"] is not None else "N/A"),
            )
            report.builder.kpi("Total volume", f"{overview['total_volume']:.4f}")
            report.builder.kpi("Total exposure", f"{overview['total_exposure']:.4f}")
            report.builder.kpi("Strategy drawdown", f"{overview['strategy_executor_drawdown']:.4f}")
            report.builder.kpi("Errors", str(overview["errors"]))
            report.builder.kpi("Last update", str(overview["last_update"] or "—"))
            report.builder.table([_asset_row(asset, snapshot["assets"][asset]) for asset in ASSETS])
            report.builder.markdown("## Alerts\n" + ("\n".join(f"- {item}" for item in snapshot["alerts"]) or "- None"))
            await report.update()
            ticks += 1
            await asyncio.sleep(config.poll_interval_seconds)
    except asyncio.CancelledError:
        return f"derive_mm_health stopped after {ticks} read-only updates"
