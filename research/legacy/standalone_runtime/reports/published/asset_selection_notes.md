# Asset selection notes

## Active

- **XRP** — retained in the active universe. The short retained snapshot
  reached internal quoting with healthy source data, but produced no direct
  Derive trades or conservative shadow fills. This is operational evidence,
  not a profitability result.
- **LINK** — retained in the active universe for a clean successor test. The
  short snapshot had healthy data but was blocked before quoting by
  `MAX_SINGLE_ORDER_NOTIONAL`; this is a pre-quote gate observation, not proof
  of poor LINK liquidity or a decimal-conversion failure.

## Retired

DOGE, ADA, and ZEC are retired from new active runs. CC, SOL, BNB, and HYPE are
also retired from the default scope. Their historical configurations and
evidence remain local for reproducibility and audit. Retirement is a scope
decision; it is not a claim that every historical observation is invalid or
that an asset can never be re-entered.

Any future re-entry requires a new exact Derive/reference mapping check,
runtime sizing/rule check, causal trade-feed check, markout denominator check,
and independent safety review.
