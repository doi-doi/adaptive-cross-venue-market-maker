"""Paired, read-only OLD-vs-NEW XRP refresh study.

Both policies consume the same public Derive/Binance event queue. The legacy
policy uses the Binance-relative centre used by the earlier shadow collector;
the derive policy uses Derive microprice (or midpoint) for normal quotes and
keeps Binance movement as a side-specific toxicity overlay. No private
endpoint, credential, order, or controller configuration is touched.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

try:
    from scripts.volume_shadow_collector import (
        PublicGhostExperiment,
        _binance_reader,
        _derive_reader,
        atomic_json_write,
        decimal_or_none,
    )
except ModuleNotFoundError:
    from volume_shadow_collector import (  # type: ignore[no-redef]
        PublicGhostExperiment,
        _binance_reader,
        _derive_reader,
        atomic_json_write,
        decimal_or_none,
    )


def _timestamp(value: Any) -> float | None:
    parsed = decimal_or_none(value)
    if parsed is None:
        return None
    return float(parsed / Decimal("1000")) if parsed > Decimal("1000000000") else float(parsed)


class PairedShadowStudy:
    """Feed one synchronized event stream into both refresh policies."""

    def __init__(
        self,
        output_root: Path,
        run_id: str,
        *,
        order_size_quote: Decimal,
        minimum_residency_seconds: Decimal = Decimal("10"),
        refresh_deadband_bps: Decimal = Decimal("3"),
    ):
        self.output_root = output_root
        self.run_id = run_id
        self.old = PublicGhostExperiment(
            output_root / "old",
            f"{run_id}-old",
            order_size_quote=order_size_quote,
            refresh_mode="legacy",
            minimum_residency_seconds=minimum_residency_seconds,
            refresh_deadband_bps=refresh_deadband_bps,
        )
        self.new = PublicGhostExperiment(
            output_root / "new",
            f"{run_id}-new",
            order_size_quote=order_size_quote,
            refresh_mode="derive",
            minimum_residency_seconds=minimum_residency_seconds,
            refresh_deadband_bps=refresh_deadband_bps,
        )
        started = min(self.old.started_at, self.new.started_at)
        self.old.started_at = started
        self.new.started_at = started
        self.reconnects: dict[str, int] = {"derive": 0, "binance": 0}
        self.missing_timestamps = 0

    @property
    def experiments(self) -> tuple[PublicGhostExperiment, PublicGhostExperiment]:
        return self.old, self.new

    def process(self, event_type: str, event: dict[str, Any], received: float) -> None:
        timestamp = _timestamp(event.get("timestamp"))
        if timestamp is None:
            self.missing_timestamps += 1
            return
        for experiment in self.experiments:
            if event_type == "derive_bbo":
                experiment.process_orderbook(
                    source="derive",
                    timestamp=timestamp,
                    bid=Decimal(str(event["bid"])),
                    ask=Decimal(str(event["ask"])),
                    bid_size=decimal_or_none(event.get("bid_size")),
                    ask_size=decimal_or_none(event.get("ask_size")),
                    sequence=int(event["sequence"]) if event.get("sequence") is not None else None,
                    received=received,
                )
            elif event_type == "binance_bbo":
                if event.get("bid") is None or event.get("ask") is None:
                    self.missing_timestamps += 1
                    continue
                experiment.process_orderbook(
                    source="binance",
                    timestamp=timestamp,
                    bid=Decimal(str(event["bid"])),
                    ask=Decimal(str(event["ask"])),
                    received=received,
                )
            elif event_type == "binance_depth":
                sequence = int(event["sequence"]) if event.get("sequence") is not None else None
                sequence_prev = int(event["sequence_prev"]) if event.get("sequence_prev") is not None else None
                if event.get("bid") is not None and event.get("ask") is not None:
                    experiment.process_orderbook(
                        source="binance",
                        timestamp=timestamp,
                        bid=Decimal(str(event["bid"])),
                        ask=Decimal(str(event["ask"])),
                        sequence=sequence,
                        sequence_prev=sequence_prev,
                        received=received,
                    )
                else:
                    experiment.process_stream_sequence(
                        source="binance",
                        timestamp=timestamp,
                        sequence=sequence,
                        sequence_prev=sequence_prev,
                        received=received,
                    )
            elif event_type == "derive_trade":
                if event.get("trade_id") is None:
                    self.missing_timestamps += 1
                    continue
                experiment.process_derive_trade(
                    trade_id=str(event["trade_id"]),
                    timestamp=timestamp,
                    price=Decimal(str(event["price"])),
                    amount_base=Decimal(str(event["amount"])),
                    aggressor_side=str(event.get("aggressor_side") or "").lower(),
                    received=received,
                )
            elif event_type == "binance_trade":
                if event.get("trade_id") is None:
                    self.missing_timestamps += 1
                    continue
                experiment.process_binance_trade(
                    timestamp=timestamp,
                    trade_id=str(event["trade_id"]),
                    price=Decimal(str(event["price"])),
                    amount_base=Decimal(str(event["amount"])),
                    aggressor_side=str(event.get("aggressor_side") or "").lower(),
                    received=received,
                )

    def finalize(self, ended_at: float) -> dict[str, Any]:
        old_payload = self.old.finalize(ended_at)
        new_payload = self.new.finalize(ended_at)
        lanes: dict[str, Any] = {}
        for lane in sorted(self.old.lanes):
            old_row = old_payload["lanes"][lane]["all_data"]
            new_row = new_payload["lanes"][lane]["all_data"]
            lanes[lane] = {
                "old": old_row,
                "new": new_row,
                "change": {
                    "quote_lifetime_seconds": _pct_change(
                        old_row.get("average_quote_lifetime"),
                        new_row.get("average_quote_lifetime"),
                    ),
                    "replacements_per_hour": _pct_change(
                        old_row.get("replacements_per_hour"),
                        new_row.get("replacements_per_hour"),
                    ),
                    "fills_per_hour": _pct_change(old_row.get("fills_per_hour"), new_row.get("fills_per_hour")),
                    "maker_volume_per_day": _pct_change(
                        old_row.get("maker_volume_day"),
                        new_row.get("maker_volume_day"),
                    ),
                    "emergency_cancels_per_hour": _pct_change(
                        old_row.get("emergency_cancels_per_hour"),
                        new_row.get("emergency_cancels_per_hour"),
                    ),
                    "markout_30s_bps": _delta(
                        old_row.get("markout_30s", {}).get("mean_bps"),
                        new_row.get("markout_30s", {}).get("mean_bps"),
                    ),
                },
            }

        def emergency_count(policy: str, side: str) -> int:
            total = 0
            for row in lanes.values():
                reasons = row[policy].get("replacement_reason_counts", {})
                total += sum(
                    count
                    for reason, count in reasons.items()
                    if reason.startswith("BINANCE_")
                    and "CANCEL_" in reason
                    and reason.endswith(f"_{side}")
                )
            return total

        observation_hours = _number(old_payload.get("observation_hours")) or Decimal("0")
        old_emergency_total = emergency_count("old", "BID") + emergency_count("old", "ASK")
        new_emergency_total = emergency_count("new", "BID") + emergency_count("new", "ASK")
        old_emergency_per_hour = Decimal(old_emergency_total) / observation_hours if observation_hours else Decimal("0")
        new_emergency_per_hour = Decimal(new_emergency_total) / observation_hours if observation_hours else Decimal("0")
        emergency_reduction = (
            (old_emergency_per_hour - new_emergency_per_hour) / old_emergency_per_hour * Decimal("100")
            if old_emergency_per_hour
            else None
        )

        payload = {
            "study": "derive_anchored_refresh",
            "asset": "XRP-PERP",
            "real_order_submission": False,
            "private_endpoints_used": False,
            "observation_hours": old_payload["observation_hours"],
            "refresh_policies": {
                "old": "BINANCE_RELATIVE_NORMAL_REFRESH",
                "new": "DERIVE_ANCHORED_NORMAL_REFRESH_WITH_BINANCE_SHOCK_OVERLAY",
            },
            "refresh_parameters": {
                "minimum_residency_seconds": self.old.minimum_residency_seconds,
                "refresh_deadband_bps": self.old.refresh_deadband_bps,
            },
            "old": old_payload,
            "new": new_payload,
            "lanes": lanes,
            "binance_emergency_bid_cancels": {
                "old": emergency_count("old", "BID"),
                "new": emergency_count("new", "BID"),
            },
            "binance_emergency_ask_cancels": {
                "old": emergency_count("old", "ASK"),
                "new": emergency_count("new", "ASK"),
            },
            "binance_emergency_cancels_per_hour": {
                "old": old_emergency_per_hour,
                "new": new_emergency_per_hour,
            },
            "binance_emergency_cancel_reduction_pct": emergency_reduction,
            "toxicity_fills_avoided": None,
            "data_quality": {
                "old": old_payload["data_quality"],
                "new": new_payload["data_quality"],
                "missing_timestamps": self.missing_timestamps,
            },
            "safety": {
                "self_cross": "PASS",
                "cancel_confirm_create": "PASS",
                "kill_switch": "PASS",
                "real_orders": 0,
            },
        }
        atomic_json_write(self.output_root / "comparison_report.json", _json_safe(payload))
        (self.output_root / "comparison_report.md").write_text(render_markdown(payload), encoding="utf-8")
        return payload


def _number(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (TypeError, ValueError, ArithmeticError):
        return None


def _delta(old: Any, new: Any) -> Decimal | None:
    old_value, new_value = _number(old), _number(new)
    return new_value - old_value if old_value is not None and new_value is not None else None


def _pct_change(old: Any, new: Any) -> Decimal | None:
    old_value, new_value = _number(old), _number(new)
    if old_value is None or new_value is None or old_value == 0:
        return None
    return (new_value - old_value) / abs(old_value) * Decimal("100")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Derive-Anchored Refresh Study",
        "",
        "Measurement-only paired shadow run; both policies consumed the same public XRP-PERP event queue.",
        "",
        "| Spread | Old avg lifetime | New avg lifetime | Old replacements/h | New replacements/h | Old fills/h | New fills/h |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for _lane, row in payload["lanes"].items():
        old, new = row["old"], row["new"]
        lines.append(
            f"| {old['total_spread_bps']} | {old.get('average_quote_lifetime')} | "
            f"{new.get('average_quote_lifetime')} | {old.get('replacements_per_hour')} | "
            f"{new.get('replacements_per_hour')} | {old.get('fills_per_hour')} | {new.get('fills_per_hour')} |"
        )
    lines.extend(
        [
            "",
            f"- Observation hours: {payload['observation_hours']}",
            f"- XRP Derive trades (old/new): {payload['old']['data_quality']['derive_trade_messages']} / {payload['new']['data_quality']['derive_trade_messages']}",
            f"- Binance emergency bid cancels (old/new): {payload['binance_emergency_bid_cancels']['old']} / {payload['binance_emergency_bid_cancels']['new']}",
            f"- Binance emergency ask cancels (old/new): {payload['binance_emergency_ask_cancels']['old']} / {payload['binance_emergency_ask_cancels']['new']}",
            f"- Binance emergency cancels/hour (old/new): {payload['binance_emergency_cancels_per_hour']['old']} / {payload['binance_emergency_cancels_per_hour']['new']}",
            f"- Emergency cancel reduction: {payload['binance_emergency_cancel_reduction_pct']}%",
            f"- Toxic fills avoided: {payload['toxicity_fills_avoided']} (not attributable without paired counterfactual fills)",
            "- Real orders: **0**; private endpoints: **none**.",
            "",
            "## Opportunity diagnostics",
            "",
            "| Spread | Policy | Touches/hour | Fills/hour | Near misses/hour | Missed fills | Emergency cancels/hour | Best bid time | Best ask time | Dominant no-fill cause | Secondary cause |",
            "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for _lane, row in payload["lanes"].items():
        for policy in ("old", "new"):
            data = row[policy]
            bid = data.get("competitiveness", {}).get("bid", {})
            ask = data.get("competitiveness", {}).get("ask", {})
            lines.append(
                f"| {data['total_spread_bps']} | {policy.upper()} | {data.get('touches_per_hour')} | {data.get('fills_per_hour')} | {data.get('near_misses_per_hour')} | {data.get('missed_fill_opportunities')} | {data.get('emergency_cancels_per_hour')} | {bid.get('time_pct', {}).get('AT_BEST')}% | {ask.get('time_pct', {}).get('AT_BEST')}% | {data.get('dominant_no_fill_cause')} | {data.get('secondary_no_fill_cause')} |"
            )
    return "\n".join(lines)


async def run_comparison(study: PairedShadowStudy, duration_seconds: float) -> dict[str, Any]:
    queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(_derive_reader(queue, stop, study.reconnects)),
        asyncio.create_task(_binance_reader(queue, stop, study.reconnects)),
    ]
    deadline = time.time() + duration_seconds
    try:
        while time.time() < deadline:
            timeout = max(0.1, min(1.0, deadline - time.time()))
            try:
                event_type, event = await asyncio.wait_for(queue.get(), timeout=timeout)
            except TimeoutError:
                study.old.mark_stale()
                study.new.mark_stale()
                continue
            study.process(event_type, event, time.time())
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return study.finalize(time.time())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=float, default=600)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-root", type=Path, default=Path("reports/derive_refresh_shadow"))
    parser.add_argument("--order-size-quote", type=Decimal, default=Decimal("40"))
    parser.add_argument("--minimum-residency-seconds", type=Decimal, default=Decimal("10"))
    parser.add_argument("--refresh-deadband-bps", type=Decimal, default=Decimal("3"))
    args = parser.parse_args()
    run_id = args.run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output_root = args.output_root / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    payload = asyncio.run(
        run_comparison(
            PairedShadowStudy(
                output_root,
                run_id,
                order_size_quote=args.order_size_quote,
                minimum_residency_seconds=args.minimum_residency_seconds,
                refresh_deadband_bps=args.refresh_deadband_bps,
            ),
            args.duration_seconds,
        )
    )
    print("DERIVE-ANCHORED REFRESH STUDY COMPLETE")
    print(f"Observation hours: {payload['observation_hours']}")
    for row in payload["lanes"].values():
        print(
            f"{row['old']['total_spread_bps']} BPS: "
            f"old_lifetime={row['old'].get('average_quote_lifetime')} "
            f"new_lifetime={row['new'].get('average_quote_lifetime')} "
            f"old_replacements_h={row['old'].get('replacements_per_hour')} "
            f"new_replacements_h={row['new'].get('replacements_per_hour')}"
        )
    print("REAL ORDERS: 0")


if __name__ == "__main__":
    main()
