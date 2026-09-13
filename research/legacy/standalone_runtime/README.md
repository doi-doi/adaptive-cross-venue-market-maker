# Retired standalone runtime

This directory preserves the pre-Hummingbot standalone runtime, multi-reference
configurations, dashboard, reports, scripts, and tests for research history.
Nothing here is part of the competition runtime and no file here is a live
configuration example.

Run historical tools from this archive root so their relative `conf/`,
`reports/`, and `dashboard/` paths retain their original meaning. For example:

```bash
cd research/legacy/standalone_runtime
PYTHONPATH=src python scripts/quote_fill_diagnostic.py --help
```

The current runtime surface is limited to `controllers/`, `configs/`, `condor/`,
and the current scripts and tests at repository root.
