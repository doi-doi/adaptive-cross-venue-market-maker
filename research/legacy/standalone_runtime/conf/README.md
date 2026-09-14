# Configuration profiles

Use `mainnet_shadow.yml` for new runs. It is the canonical active profile and
enables XRP and LINK only, with Binance -> Bybit -> OKX -> Pause references,
Bitget disabled, Derive-only execution, and live-disarmed safety defaults.

`mainnet_shadow_refresh_research.yml` is the current XRP/LINK-only offline
refresh/deadband profile. It is also shadow-only and disarmed.

The remaining `mainnet_shadow_3asset_*.yml`,
`mainnet_shadow_3asset_xrp_link_zec.yml`, and
`mainnet_shadow_8asset_consensus.yml` files are historical/reproducibility
profiles. They preserve prior research scope and should not be used for a new
run. `mainnet_live.example.yml` is a safe, disarmed live-shaped example and is
not authorization to trade.
