# Phase 3B — Historical data expansion report

**Status: blocked on source data.** The authorized source is NSE's own CSV export pages. This workspace had no exported files, and the official NSE Bhavcopy MCP request timed out. The project now has an importer and audit path, but no NSE-sourced 2020–2026 dataset was available to analyze.

## NSE source and import status

Use NSE's [security-wise history CSV page](https://www.nseindia.com/historical/price-and-volume-data-per-security) for each equity, plus its separate [historical index data](https://www.nseindia.com/reports-indices-historical-index-data) and [India VIX](https://www.nseindia.com/reports-indices-historical-vix) CSV pages. The importer expects those exports in `data/nse_exports/`. For wider bulk history or other data-use rights, NSE lists a separate [EOD/historical data subscription](https://www.nseindia.com/static/market-data/eod-historical-data-subscription); the public [Bhavcopy MCP](https://www.nseindia.com/nse-mcp) is informational and non-commercial.

The import attempt found no `data/nse_exports/` directory and imported **0 of 502** requested equities and index series. Its manifest is [historical_download_manifest.json](../data/historical_cache/historical_download_manifest.json). The official-source data-only audit is [results/data_expansion_20260926_121901](../results/data_expansion_20260926_121901/): status `NO_DATA`, zero bars, and no baseline run.

## Existing local-cache diagnostic

The separate [local-cache audit](../results/data_expansion_20260926_121712/) is diagnostic only; it is **Yahoo-derived**, not NSE data.

- Requested range: 2020-01-01 to 2026-09-26. Cached range: **2025-07-21 to 2026-09-25**, totaling 148,070 normalized daily bars.
- Retrieved: 501 of 503 requested rows (500 current-universe equities plus NIFTY). One placeholder is intentionally excluded; India VIX has no bars.
- The 2021 minimum requires NIFTY and at least 400 of 500 non-placeholder current equities across the complete window. **Zero** equities cover that window. The 2020 preferred target is also unmet.
- NIFTY has 293 cached sessions and zero gaps against the observed NIFTY cache dates. Its independent exchange-calendar completeness is unknown. EMA200 first becomes valid on 2026-05-14. VIX is unavailable; the regime code can fall back to 20-session annualized NIFTY realized volatility.
- Quality checks found zero duplicate rows, missing OHLCV rows, invalid prices, or invalid OHLC relationships. There are 2,409 zero-volume equity rows and 10 moves above 20% without a same-date action field; these are review candidates, not confirmed bad data. Yahoo cache action columns report 872 dividend rows and 26 split rows, which does not establish complete corporate-action coverage.
- Six equities have fewer than the 220-row warm-up requirement: EMMVEE (217), ICICIAMC (194), MEESHO (201), PINELABS (219), PWL (217), and TENNIND (216). One DUMMY placeholder and India VIX are also excluded for their stated reasons.
- The common cache start on 2025-07-21 follows the original two-month evaluation range plus 370 calendar days of warm-up. It is a request-window boundary, not evidence that the provider has no earlier records.

No verified ticker identity mapping or point-in-time constituent history is available; survivorship and corporate-action risks remain. The per-symbol audit writes an empty mapping template instead of inferring corporate identities.

## Research status

The 24-month train / 6-month validation / 6-month test walk-forward cannot run without 36 months of history plus indicator warm-up. The fixed 11-strategy baseline was skipped because the minimum window failed and no official NSE source provenance passed. The prior Phase 3 results were not modified. No strategy selection, parameter search, or optimization ran in Phase 3B.

After exporting NSE CSVs into `data/nse_exports/`, run:

```bash
python scripts/download_data.py --start 2020-01-01 --end 2026-09-26 --source-dir data/nse_exports
python scripts/run_data_expansion.py --start 2020-01-01 --end 2026-09-26 --cache-dir data/historical_cache --run-baseline
```

The second command only runs the fixed baselines when both the coverage threshold and official NSE import provenance pass.
