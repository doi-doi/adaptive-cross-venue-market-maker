"""Derive-only control model for the same timestamps and fill evidence."""

from __future__ import annotations

from decimal import Decimal

from .models import BookSnapshot, FairValue


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
    )


def control_contract() -> dict[str, str]:
    return {
        "MODEL_A": "DERIVE_ONLY",
        "MODEL_B": "BINANCE_REFERENCE",
        "portfolio_policy": "separate research comparison; no second live portfolio",
        "timestamp_policy": "same causal snapshots and no forward fill",
    }
