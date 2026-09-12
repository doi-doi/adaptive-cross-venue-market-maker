# Legacy research boundary

The `src/`, `conf/`, `scripts/`, `dashboard/`, and historical reports at the
repository root are retained research evidence from the standalone public-data
shadow system. They are not the final competition runtime and must not be used
to place orders.

The production-shaped runtime is only:

- `controllers/market_making/derive_binance_adaptive_mm.py`
- `configs/derive_binance_adaptive_mm_{xrp,link}.yml`
- Hummingbot's installed `v2_with_controllers.py`
- `condor/derive_mm_health.py`

Legacy profiles mentioning ZEC, DOGE, ADA, CC, SOL, BNB, HYPE, Bybit, OKX, or
Bitget are historical and disabled. They are not loaded by the final configs.
