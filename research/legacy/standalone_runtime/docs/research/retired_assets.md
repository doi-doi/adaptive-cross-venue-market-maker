# Retired assets

The following assets are excluded from the default active configuration. Their
historical configurations, reports, and locally retained evidence are not
deleted solely because the assets were retired.

| Asset | Status | Boundary |
|---|---|---|
| DOGE | Retired | Historical decimal/order-book and shadow evidence only |
| ADA | Retired | Historical decimal/order-book and shadow evidence only |
| ZEC | Retired | Historical refresh-research evidence only |
| CC | Retired | Historical eight-asset comparison only |
| SOL | Retired | Historical eight-asset comparison only |
| BNB | Retired | Historical public-feed screen only |
| HYPE | Retired | Historical shadow/screen evidence only |

New runs must use `conf/mainnet_shadow.yml` or the XRP/LINK-only refresh
profile. Older files whose names describe three-asset or eight-asset runs are
reproducibility records; they are not current recommendations and should not
be selected for a new run.

Retirement is a scope decision, not a claim that every historical observation
was invalid or that the assets can never be researched again. A future asset
re-entry would require a new, separately reviewed mapping, sizing, data-health,
trade-evidence, and safety gate.
