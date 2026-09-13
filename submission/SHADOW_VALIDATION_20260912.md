# Native Hummingbot and Condor shadow validation

> Historical pre-hardening evidence only. Its floating `latest` image identity
> is superseded by `LIVE_SAFETY_HARDENING.md` and must not be used for the
> competition runtime pin.

## Scope

- Instance: `derive-binance-adaptive-mm-shadow-final-20260912-224929`
- Image: `hummingbot/hummingbot:latest`, version `2.16.0`
- Image ID: `sha256:632d2b07aa156b761310f2f7258a78c9660a1c28b6df4b33874e09a0c7d06c85`
- Account profile: existing `master_account`
- Controllers: `derive_binance_adaptive_mm_xrp`, `derive_binance_adaptive_mm_link`
- Condor routine instance: `cc716c3a`, `derive_mm_health`, read-only
- Deployed: 2026-09-12 22:49:29 UTC
- Stopped: 2026-09-12 23:06:22 UTC
- Wall-clock duration: 16m53s
- Final controller uptime: 961.48s

## Observed runtime evidence

Hummingbot created native `derive_perpetual_paper_trade` and
`binance_perpetual_paper_trade` public wrappers. The native trackers initialized
XRP-USDC, LINK-USDC, XRP-USDT and LINK-USDT. Both controller diagnostics stayed
current with zero error logs. XRP and LINK reached `SHADOW / READY`, calculated
fair value, basis, state, inventory, quantized desired bid/ask and order amount,
and emitted no executor actions. LINK also transitioned through `SHORT_BIAS`
during the sample window, proving live state/mode updates rather than a frozen
status payload.

The final capture reported:

| Metric | XRP | LINK |
|---|---:|---:|
| Operational state | SHADOW | SHADOW |
| Market state / mode | NORMAL / NEUTRAL | NORMAL / NEUTRAL |
| Derive age | 0.0s | 0.0s |
| Binance age | 0.0s | 0.0s |
| Position notional | 0 | 0 |
| Portfolio open orders | 0 | 0 |
| Mutations/min | 0 | 0 |
| Fills / volume / PnL | 0 / 0 / 0 | 0 / 0 / 0 |

Independent API filters for `master_account`, `derive_perpetual`, XRP-USDC and
LINK-USDC returned zero active orders and zero positions. The native executor
performance endpoint returned zero executors. The SQLite database contained two
controller records and zero order, executor, position and fill records.

Storage remained stable at 204800 bytes. SQLite `pragma integrity_check`
returned `ok`; there was no lock crash. Container usage at final capture was
1.43% CPU and 218.4 MiB memory. Condor's final report was `HEALTHY`, `SHADOW`,
with both feeds healthy and no alerts.

## Safety and shutdown

The committed configs were `shadow_mode=true` and `mainnet_armed=false`.
No live trading was started. The Condor routine was stopped, then Hummingbot API
performed graceful stop-and-archive. The bot run ended `STOPPED / ARCHIVED`
with no error message, and the container was removed. Artifacts remain under the
local Hummingbot archive; credentials, logs, databases and raw telemetry are not
committed.

Result: PASS.
