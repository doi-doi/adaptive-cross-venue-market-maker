# Legacy research boundary

The `src/`, `conf/`, `scripts/`, `dashboard/`, and historical reports at the
repository root are retained research evidence from the standalone public-data
shadow system. They are not the final competition runtime and must not be used
to place orders.

The production-shaped runtime described by this archive was retired. The active
submission runtime is now the XRP-only surface in the repository root:

- `controllers/market_making/derive_binance_adaptive_mm.py`
- `configs/derive_binance_adaptive_mm_xrp.yml`
- Hummingbot's installed `v2_with_controllers.py`
- `condor/derive_mm_health.py`

Any older multi-asset or LINK references in this directory are historical and
must not be loaded for competition or execution.

Legacy profiles mentioning ZEC, DOGE, ADA, CC, SOL, BNB, HYPE, Bybit, OKX, or
Bitget are historical and disabled. They are not loaded by the final configs.
