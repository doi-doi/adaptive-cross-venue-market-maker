"""Derive-only control model for the same timestamps and fill evidence."""

from __future__ import annotations

from decimal import Decimal

from .models import BookSnapshot, FairValue, ReferenceControl
from .priority import PrioritySelection
from .reference import aggregate_reference_book, fair_value_from_reference
from .source_health import consensus


def derive_only_fair_value(derive_book: BookSnapshot) -> FairValue:
    mid = derive_book.mid
    return FairValue(
        binance_mid=mid,
        binance_microprice=mid,
        top_n_imbalance=Decimal("0"),
        fair_value_raw=mid,
        baseline_basis_bps=Decimal("0"),
        basis_bps=Decimal("0"),
        derive_fair_value=mid,
        source_fair_values={"derive": mid},
        source_mids={"derive": mid},
        valid_sources=("derive",),
        confidence="DERIVE_ONLY_CONTROL",
        reference_control=ReferenceControl.DERIVE_ONLY.value,
    )


def build_control_fair_value(
    control: str,
    *,
    derive_book: BookSnapshot,
    source_books: dict[str, BookSnapshot],
    source_health: dict[str, object],
    basis_tracker: object,
    now: float,
    healthy_seconds: float,
    stale_seconds: float,
    stale_overrides: dict[str, float],
    outlier_bps: Decimal,
    disagreement_bps: Decimal,
    minimum_sources: int,
    mid_weight: Decimal,
    microprice_weight: Decimal,
    max_levels: int,
    priority_selection: PrioritySelection | None = None,
) -> tuple[FairValue | None, dict[str, object]]:
    """Build one control value from the same causal source state."""

    control = str(control)
    if control == ReferenceControl.DERIVE_ONLY.value:
        return derive_only_fair_value(derive_book), {
            "source_count": 1,
            "valid_sources": ["derive"],
            "confidence": "DERIVE_ONLY_CONTROL",
            "pause_reason": "",
            "dispersion_bps": Decimal("0"),
            "outliers": [],
            "source_fair_values": {"derive": derive_book.mid},
        }

    if control == ReferenceControl.PRIORITY_FAILOVER.value:
        if priority_selection is None:
            raise ValueError("PRIORITY_FAILOVER requires a priority selection")
        result = priority_selection.as_result()
        if result["dispersion_bps"] is not None and result["dispersion_bps"] > disagreement_bps:
            result["pause_reason"] = "REFERENCE_DISAGREEMENT_PAUSE"
            result["fair_value"] = None
            result["confidence"] = "PAUSED"
        if result["fair_value"] is None or priority_selection.selected_book is None:
            return None, result
        fair = fair_value_from_reference(
            derive_book,
            priority_selection.selected_book,
            basis_tracker,
            raw_fair_value=result["fair_value"],
            source_fair_values=result["source_fair_values"],
            source_mids=result["source_mids"],
            valid_sources=tuple(result["valid_sources"]),
            outliers=(),
            dispersion_bps=result["dispersion_bps"],
            confidence=str(result["confidence"]),
            pause_reason=str(result["pause_reason"]),
            reference_control=control,
            max_levels=max_levels,
        )
        return fair, result

    eligible_health = {venue: health for venue, health in source_health.items() if venue in source_books}
    binance_only = control in {
        ReferenceControl.BINANCE_ONLY_REFERENCE.value,
        ReferenceControl.BINANCE_ONLY_NO_FAILOVER.value,
    }
    required = 1 if binance_only else minimum_sources
    selected = {"binance": source_health["binance"]} if binance_only and "binance" in source_health else eligible_health
    result = consensus(
        selected,
        now,
        stale=stale_seconds,
        healthy=healthy_seconds,
        outlier_bps=outlier_bps,
        disagreement_bps=disagreement_bps,
        minimum_sources=required,
        mid_weight=mid_weight,
        micro_weight=microprice_weight,
        overrides=stale_overrides,
    )
    if result["fair_value"] is None:
        return None, result
    valid_sources = tuple(str(venue) for venue in result["valid_sources"])
    eligible_books = {venue: source_books[venue] for venue in valid_sources if venue in source_books}
    reference_book = aggregate_reference_book(eligible_books)
    if reference_book is None:
        result["pause_reason"] = "REFERENCE_BOOK_UNAVAILABLE"
        return None, result
    source_mids = {venue: source_books[venue].mid for venue in valid_sources if venue in source_books}
    fair = fair_value_from_reference(
        derive_book,
        reference_book,
        basis_tracker,
        raw_fair_value=result["fair_value"],
        source_fair_values=result["source_fair_values"],
        source_mids=source_mids,
        valid_sources=valid_sources,
        outliers=tuple(str(venue) for venue in result["outliers"]),
        dispersion_bps=result["dispersion_bps"],
        confidence=str(result["confidence"]),
        pause_reason=str(result["pause_reason"]),
        reference_control=control,
        max_levels=max_levels,
    )
    return fair, result


def control_contract() -> dict[str, str]:
    return {
        "MODEL_A": "DERIVE_ONLY",
        "MODEL_B": "BINANCE_ONLY_NO_FAILOVER",
        "MODEL_C": "PRIORITY_FAILOVER",
        "HISTORICAL_MODEL": "MULTI_SOURCE_CONSENSUS",
        "reference_venues": "BINANCE, BYBIT, OKX",
        "portfolio_policy": "separate research portfolios; no second live portfolio",
        "timestamp_policy": "same causal snapshots and no forward fill",
    }
