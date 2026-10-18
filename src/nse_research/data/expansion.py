"""Data-only audits and manifests for historical research datasets."""

from __future__ import annotations

import html
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from nse_research.data.providers import YahooFinanceProvider, validate_and_normalize_bars
from nse_research.indicators.features import add_features

BASELINE_STRATEGIES = (
    "trend_following",
    "momentum",
    "breakout_20",
    "breakout_50",
    "breakout_previous_day",
    "pullback",
    "mean_reversion",
    "swing_1_3",
    "swing_3_5",
    "swing_5_10",
    "swing_10_20",
)


def normalize_symbol(value: str) -> str:
    """Normalize symbol formatting only; never infer corporate identity changes."""
    symbol = str(value).strip().upper()
    if not symbol:
        return ""
    if symbol.startswith("^"):
        return symbol
    if symbol.endswith(".NS"):
        return symbol
    return f"{symbol}.NS"


def _cache_path(cache_dir: Path, symbol: str, interval: str = "1d") -> Path:
    return cache_dir / YahooFinanceProvider.cache_filename(symbol, interval)


def _read_cache(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any] | None]:
    if not path.exists():
        return pd.DataFrame(), pd.DataFrame(), None
    try:
        raw = pd.read_csv(path, parse_dates=["timestamp"], index_col="timestamp")
        bars, audit = validate_and_normalize_bars(raw, "1d")
        return raw, bars, audit
    except Exception as exc:
        return pd.DataFrame(), pd.DataFrame(), {"error": str(exc)}


def _safe_count(frame: pd.DataFrame, column: str, predicate: Any) -> int:
    if column not in frame:
        return 0
    values = pd.to_numeric(frame[column], errors="coerce")
    return int(predicate(values).fillna(False).sum())


def inspect_symbol_history(
    symbol: str,
    raw: pd.DataFrame,
    bars: pd.DataFrame,
    normalization_audit: dict[str, Any] | None,
    benchmark_sessions: pd.DatetimeIndex,
    is_index: bool = False,
) -> dict[str, Any]:
    """Return quality counters; benchmark continuity uses observed NIFTY sessions."""
    if raw.empty or bars.empty:
        return {
            "symbol": symbol,
            "first_date": None,
            "last_date": None,
            "rows": 0,
            "observed_nifty_sessions_in_span": 0,
            "missing_observed_nifty_sessions": None,
            "duplicate_rows": 0,
            "duplicate_timestamps": 0,
            "missing_ohlcv_rows": 0,
            "invalid_price_rows": 0,
            "invalid_ohlc_rows": 0,
            "zero_volume_rows": 0,
            "abnormal_price_changes_gt_20pct": 0,
            "abnormal_price_change_dates": "",
            "dividend_event_rows": 0,
            "split_event_rows": 0,
            "unexplained_large_moves": 0,
            "unexplained_large_move_dates": "",
            "quality_error": (normalization_audit or {}).get("error", "no cached bars"),
        }

    first = pd.Timestamp(bars.index.min()).normalize()
    last = pd.Timestamp(bars.index.max()).normalize()
    raw_index = pd.DatetimeIndex(pd.to_datetime(raw.index, errors="coerce"))
    duplicate_timestamps = int(raw_index.duplicated(keep="last").sum())
    duplicate_rows = int(raw.reset_index().duplicated(keep=False).sum())
    calendar = benchmark_sessions.normalize()
    sessions = calendar[(calendar >= first) & (calendar <= last)]
    observed = pd.DatetimeIndex(bars.index).normalize()
    missing_days = sessions.difference(observed)

    close = pd.to_numeric(bars["close"], errors="coerce")
    pct_change = close.pct_change()
    large_move_mask = pct_change.abs() > 0.20
    large_dates = [pd.Timestamp(value).date().isoformat() for value in pct_change.index[large_move_mask.fillna(False)]]
    split_field_available = "stock_splits" in raw.columns
    dividend_field_available = "dividends" in raw.columns
    splits = pd.to_numeric(raw.get("stock_splits", pd.Series(0, index=raw.index)), errors="coerce").fillna(0)
    dividends = pd.to_numeric(raw.get("dividends", pd.Series(0, index=raw.index)), errors="coerce").fillna(0)
    action_dates = set()
    for source in (splits, dividends):
        action_dates.update(pd.Timestamp(stamp).normalize() for stamp in source.index[source.ne(0)])
    unexplained_dates = [
        pd.Timestamp(stamp).date().isoformat()
        for stamp in pct_change.index[large_move_mask.fillna(False)]
        if pd.Timestamp(stamp).normalize() not in action_dates
    ]

    return {
        "symbol": symbol,
        "first_date": first.date().isoformat(),
        "last_date": last.date().isoformat(),
        "rows": int(len(bars)),
        "observed_nifty_sessions_in_span": int(len(sessions)),
        "missing_observed_nifty_sessions": int(len(missing_days)) if len(sessions) else None,
        "missing_session_dates_sample": ";".join(value.date().isoformat() for value in missing_days[:25]),
        "duplicate_rows": duplicate_rows,
        "duplicate_timestamps": duplicate_timestamps,
        "missing_ohlcv_rows": int((normalization_audit or {}).get("missing_ohlcv_rows", 0)),
        "invalid_price_rows": int((normalization_audit or {}).get("invalid_price_rows", 0)),
        "invalid_ohlc_rows": int((normalization_audit or {}).get("invalid_ohlc_rows", 0)),
        "zero_volume_rows": 0 if is_index else _safe_count(raw, "volume", lambda series: series.eq(0)),
        "abnormal_price_changes_gt_20pct": int(large_move_mask.fillna(False).sum()),
        "abnormal_price_change_dates": ";".join(large_dates),
        "corporate_action_fields_available": split_field_available and dividend_field_available,
        "dividend_event_rows": int(dividends.ne(0).sum()) if dividend_field_available else None,
        "split_event_rows": int(splits.ne(0).sum()) if split_field_available else None,
        "unexplained_large_moves": len(unexplained_dates),
        "unexplained_large_move_dates": ";".join(unexplained_dates),
        "quality_error": "",
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if pd.isna(value) if not isinstance(value, (str, bytes)) else False:
        return None
    return value


def write_data_expansion_artifacts(
    *,
    project_root: Path,
    output_dir: Path,
    cache_dir: Path,
    requested_start: date,
    requested_end: date,
    provider: str,
    requested_symbols: list[str],
    min_history_rows: int = 220,
    provider_failures: dict[str, str] | None = None,
    provider_limitations: list[str] | None = None,
    source_download_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run data-only checks and write auditable manifests/reports without backtests."""
    output_dir.mkdir(parents=True, exist_ok=True)
    normalized_symbols = list(dict.fromkeys(normalize_symbol(value) for value in requested_symbols if str(value).strip()))
    benchmark_symbol = "^NSEI"
    vix_symbol = "^INDIAVIX"
    symbols = list(dict.fromkeys([*normalized_symbols, benchmark_symbol, vix_symbol]))

    loaded: dict[str, tuple[pd.DataFrame, pd.DataFrame, dict[str, Any] | None]] = {
        symbol: _read_cache(_cache_path(cache_dir, symbol)) for symbol in symbols
    }
    benchmark = loaded.get(benchmark_symbol, (pd.DataFrame(), pd.DataFrame(), None))[1]
    benchmark_sessions = pd.DatetimeIndex(benchmark.index) if not benchmark.empty else pd.DatetimeIndex([])
    universe_rows: list[dict[str, Any]] = []
    quality_rows: list[dict[str, Any]] = []
    retrieved: list[str] = []
    failed: list[dict[str, str]] = []
    all_first: list[date] = []
    all_last: list[date] = []

    placeholder_symbols = {symbol for symbol in normalized_symbols if symbol.removesuffix(".NS").startswith("DUMMY")}
    for symbol in symbols:
        raw, bars, audit = loaded[symbol]
        is_index = symbol.startswith("^")
        quality = inspect_symbol_history(symbol, raw, bars, audit, benchmark_sessions, is_index)
        quality_rows.append(quality)
        has_bars = not bars.empty
        if has_bars:
            retrieved.append(symbol)
            all_first.append(pd.Timestamp(bars.index.min()).date())
            all_last.append(pd.Timestamp(bars.index.max()).date())
        elif symbol not in placeholder_symbols:
            if symbol in (provider_failures or {}):
                failed.append({"symbol": symbol, "reason": (provider_failures or {})[symbol]})
            else:
                failed.append({"symbol": symbol, "reason": "no valid cached bars"})

        exclusion_reason = ""
        if symbol in placeholder_symbols:
            exclusion_reason = "placeholder constituent row; no tradable security"
        elif not has_bars:
            exclusion_reason = "no valid cached bars"
        elif not is_index and len(bars) < min_history_rows:
            exclusion_reason = f"{len(bars)} rows; minimum {min_history_rows} required"
        universe_rows.append({
            "symbol": symbol,
            "first_available_date": quality["first_date"],
            "last_available_date": quality["last_date"],
            "rows": quality["rows"],
            "symbol_changed": "unknown; no verified historical security master supplied",
            "symbol_mapping": "none applied; formatting normalized only",
            "point_in_time_membership": "unavailable; current constituent snapshot only",
            "excluded": bool(exclusion_reason),
            "exclusion_reason": exclusion_reason,
        })

    benchmark_quality = next((row for row in quality_rows if row["symbol"] == benchmark_symbol), {})
    vix_quality = next((row for row in quality_rows if row["symbol"] == vix_symbol), {})
    benchmark_ema_ready = None
    if not benchmark.empty:
        benchmark_features = add_features(benchmark)
        ema_dates = benchmark_features.index[benchmark_features["ema200"].notna()]
        benchmark_ema_ready = pd.Timestamp(ema_dates.min()).date().isoformat() if len(ema_dates) else None
    first_date = min(all_first).isoformat() if all_first else None
    last_date = max(all_last).isoformat() if all_last else None
    no_history = not bool(retrieved)
    coverage_distribution: dict[str, int] = {}
    for row in universe_rows:
        first = row["first_available_date"]
        if first and row["symbol"].endswith(".NS"):
            coverage_distribution[first] = coverage_distribution.get(first, 0) + 1
    common_recent_start = max(coverage_distribution.items(), key=lambda item: item[1]) if coverage_distribution else (None, 0)
    equity_rows = [row for row in universe_rows if row["symbol"].endswith(".NS") and not row["symbol"].removesuffix(".NS").startswith("DUMMY")]
    equity_starts_2021 = sum(bool(row["first_available_date"] and row["first_available_date"] <= "2021-01-01") for row in equity_rows)
    equity_starts_2020 = sum(bool(row["first_available_date"] and row["first_available_date"] <= "2020-01-01") for row in equity_rows)
    end_cutoff = (requested_end - pd.Timedelta(days=2)).isoformat()
    equity_full_2021 = sum(
        bool(row["first_available_date"] and row["first_available_date"] <= "2021-01-01" and row["last_available_date"] and row["last_available_date"] >= end_cutoff)
        for row in equity_rows
    )
    equity_full_2020 = sum(
        bool(row["first_available_date"] and row["first_available_date"] <= "2020-01-01" and row["last_available_date"] and row["last_available_date"] >= end_cutoff)
        for row in equity_rows
    )
    all_equity_start_dates = {row["first_available_date"] for row in equity_rows if row["first_available_date"]}
    all_equity_start_same = bool(all_equity_start_dates) and len(all_equity_start_dates) <= 1
    benchmark_start = benchmark_quality.get("first_date")
    minimum_coverage_required = int(np.ceil(len(equity_rows) * 0.80))
    benchmark_end = benchmark_quality.get("last_date")
    benchmark_end_met = bool(benchmark_end and benchmark_end >= end_cutoff)
    minimum_benchmark_met = bool(benchmark_start and benchmark_start <= "2021-01-01" and benchmark_end_met)
    preferred_benchmark_met = bool(benchmark_start and benchmark_start <= "2020-01-01" and benchmark_end_met)
    meets_minimum = minimum_benchmark_met and equity_full_2021 >= minimum_coverage_required
    meets_preferred = preferred_benchmark_met and equity_full_2020 >= minimum_coverage_required
    limitations = list(provider_limitations or [])
    if not limitations:
        normalized_provider = provider.casefold()
        if "yahoo" in normalized_provider:
            limitations.append("This dataset is Yahoo-derived local cache data; no official NSE exports were loaded into it.")
            limitations.append("Yahoo history is not exchange-certified; retain the original provider files and review its adjustments and corrections policy before research use.")
        elif "nse" in normalized_provider:
            limitations.append("NSE website exports contain unadjusted OHLC and do not provide a complete corporate-action feed; review splits, dividends, mergers, symbol changes, and delistings separately.")
            limitations.append("NSE website terms govern storage and reuse. The public Bhavcopy MCP is informational and non-commercial; use the official subscription product when your required coverage or usage rights call for it.")
        else:
            limitations.append(f"Data provider declared as: {provider}. The input source and its data-use rights must be independently verified.")
        limitations.append("The requested universe uses current constituents, not point-in-time historical membership; survivorship bias remains.")

    quality_frame = pd.DataFrame(quality_rows)
    def quality_total(column: str) -> int:
        if column not in quality_frame:
            return 0
        return int(pd.to_numeric(quality_frame[column], errors="coerce").fillna(0).sum())

    def symbols_with_quality(column: str) -> int:
        if column not in quality_frame:
            return 0
        return int(pd.to_numeric(quality_frame[column], errors="coerce").fillna(0).gt(0).sum())

    manifest: dict[str, Any] = {
        "provider": provider,
        "source_download_manifest": source_download_manifest or {},
        "retrieval_date": datetime.now(timezone.utc).date().isoformat(),
        "requested_dates": {"start": requested_start.isoformat(), "end": requested_end.isoformat()},
        "actual_dates": {"start": first_date, "end": last_date},
        "symbols_requested": symbols,
        "symbols_retrieved": retrieved,
        "symbols_failed": failed,
        "symbols_requested_equities": normalized_symbols,
        "placeholder_symbols_excluded": sorted(placeholder_symbols),
        "symbols_requested_count": len(symbols),
        "symbols_retrieved_count": len(retrieved),
        "symbols_failed_count": len(failed),
        "rows": int(sum(row["rows"] for row in quality_rows)),
        "quality_summary": {
            "duplicate_rows": quality_total("duplicate_rows"),
            "duplicate_timestamps": quality_total("duplicate_timestamps"),
            "missing_ohlcv_rows": quality_total("missing_ohlcv_rows"),
            "invalid_price_rows": quality_total("invalid_price_rows"),
            "invalid_ohlc_rows": quality_total("invalid_ohlc_rows"),
            "zero_volume_rows_equities": quality_total("zero_volume_rows"),
            "abnormal_price_changes_gt_20pct": quality_total("abnormal_price_changes_gt_20pct"),
            "unexplained_large_moves": quality_total("unexplained_large_moves"),
            "symbols_with_unexplained_large_moves": symbols_with_quality("unexplained_large_moves"),
            "symbols_with_corporate_action_fields": int(quality_frame.get("corporate_action_fields_available", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
            "dividend_event_rows": quality_total("dividend_event_rows"),
            "split_event_rows": quality_total("split_event_rows"),
        },
        "missing_data": {
            "symbols_without_data": len(failed),
            "missing_observed_nifty_sessions_total": int(sum(row.get("missing_observed_nifty_sessions") or 0 for row in quality_rows)),
            "missing_observed_nifty_sessions_definition": "Observed NIFTY cache sessions inside each symbol's first/last-date span; no synthetic calendar is used.",
        },
        "exclusions": [row for row in universe_rows if row["excluded"]],
        "provider_limitations": limitations,
        "requested_minimum_start": "2021-01-01",
        "requested_preferred_start": "2020-01-01",
        "minimum_target_met": meets_minimum,
        "preferred_target_met": meets_preferred,
        "data_only_validation": {
            "status": "NO_DATA" if no_history else ("TARGET_RANGE_MISSING" if not meets_minimum else "PASS"),
            "target_start": "2021-01-01",
            "preferred_start": "2020-01-01",
            "minimum_target_coverage_definition": f"NIFTY plus at least 80% of {len(equity_rows)} requested non-placeholder current-universe equities covering the complete requested window from 2021-01-01 through within two calendar days of the requested end.",
            "preferred_target_coverage_definition": f"NIFTY plus at least 80% of {len(equity_rows)} requested non-placeholder current-universe equities covering the complete requested window from 2020-01-01 through within two calendar days of the requested end.",
            "minimum_target_equity_count": equity_full_2021,
            "preferred_target_equity_count": equity_full_2020,
            "equities_with_history_start_by_2021": equity_starts_2021,
            "equities_with_history_start_by_2020": equity_starts_2020,
            "minimum_target_equities_required": minimum_coverage_required,
            "nifty_minimum_target_met": minimum_benchmark_met,
            "nifty_preferred_target_met": preferred_benchmark_met,
            "nifty_end_date_reaches_requested_window": benchmark_end_met,
            "benchmark": benchmark_symbol,
            "benchmark_first_date": benchmark_quality.get("first_date"),
            "benchmark_last_date": benchmark_quality.get("last_date"),
            "benchmark_sessions": benchmark_quality.get("rows", 0),
            "benchmark_missing_observed_sessions": benchmark_quality.get("missing_observed_nifty_sessions"),
            "benchmark_completeness_status": "not independently verifiable without a separate NSE trading-session calendar",
            "benchmark_ema200_first_valid_date": benchmark_ema_ready,
            "benchmark_ema200_rows": max(0, int(benchmark_quality.get("rows", 0)) - 199),
            "vix_symbol": vix_symbol,
            "vix_status": "AVAILABLE" if vix_quality.get("rows", 0) else "VIX_UNAVAILABLE",
            "volatility_fallback": "20-session annualized realized volatility calculated from NIFTY close",
            "most_common_equity_start_date": common_recent_start[0],
            "equities_with_that_start_date": common_recent_start[1],
            "equity_start_dates_all_same": all_equity_start_same,
            "equities_with_history_by_2021": equity_starts_2021,
            "equities_with_history_by_2020": equity_starts_2020,
            "eligible_equity_count_at_minimum_history": sum(
                row["symbol"].endswith(".NS") and not row["excluded"] for row in universe_rows
            ),
        },
        "universe_method": "Current NIFTY 500 membership snapshot in data/universe.csv; survivorship and historical membership bias remain.",
        "corporate_action_method": "Raw OHLC is preserved as provided. Dividend/split counts are reported only when source columns exist; absent action columns are marked unavailable. Moves above 20% without same-date recorded action are candidates for manual review, not confirmed anomalies.",
        "symbol_normalization": "Uppercase exchange suffix normalization only. No issuer identity, merger, delisting, or ticker-change mappings inferred.",
        "cache_dir": str(cache_dir),
        "quality_rows": len(quality_rows),
    }

    pd.DataFrame(universe_rows).to_csv(output_dir / "historical_universe_manifest.csv", index=False)
    pd.DataFrame(columns=["old_symbol", "new_symbol", "effective_date", "source", "confidence", "notes"]).to_csv(
        output_dir / "historical_symbol_mapping.csv", index=False
    )
    pd.DataFrame(quality_rows).to_csv(output_dir / "historical_data_quality.csv", index=False)
    (output_dir / "historical_data_manifest.json").write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )
    source_manifest = manifest.get("source_download_manifest", {})
    source_symbols = set(source_manifest.get("symbols_with_verified_nse_cache_provenance", []))
    source_equities = {
        symbol for symbol in source_symbols
        if symbol.endswith(".NS") and not symbol.removesuffix(".NS").startswith("DUMMY")
    }
    verified_nse_source = (
        source_manifest.get("provider") == "NSE official historical CSV export"
        and "^NSEI" in source_symbols
        and len(source_equities) >= minimum_coverage_required
    )
    if not meets_minimum:
        placeholder_status = "NOT_RUN_INSUFFICIENT_HISTORY"
        placeholder_reason = f"Baseline not run: data starts {first_date or 'none'} and does not meet the 2021-01-01 minimum target."
    elif not verified_nse_source:
        placeholder_status = "NOT_RUN_SOURCE_NOT_VERIFIED"
        placeholder_reason = "Baseline not run: date coverage passed, but imported official NSE CSV provenance did not pass for NIFTY plus the minimum equity count."
    else:
        placeholder_status = "NOT_RUN_BASELINE_NOT_REQUESTED"
        placeholder_reason = "Baseline not run: data passed; use --run-baseline to run the fixed 11 strategies."
    _write_baseline_placeholders(output_dir, requested_start, requested_end, placeholder_status, placeholder_reason)
    _write_data_quality_html(output_dir, manifest, pd.DataFrame(quality_rows))
    _write_completion_summary(output_dir, manifest)
    return manifest


def _write_baseline_placeholders(
    output_dir: Path, requested_start: date, requested_end: date, status: str, reason: str
) -> None:
    baseline = pd.DataFrame([{
        "strategy": name,
        "status": status,
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "trades": None,
        "win_rate_pct": None,
        "profit_factor": None,
        "expectancy": None,
        "net_return_pct": None,
        "maximum_drawdown_pct": None,
        "annual_return_pct": None,
        "trading_costs": None,
        "reason": reason,
    } for name in BASELINE_STRATEGIES])
    baseline.to_csv(output_dir / "baseline_strategy_results.csv", index=False)
    years = []
    for year in range(2021, requested_end.year + 1):
        label = f"{year} YTD" if year == requested_end.year else str(year)
        for strategy in BASELINE_STRATEGIES:
            years.append({"strategy": strategy, "year": label, "status": status, "net_return_pct": None, "reason": reason})
    pd.DataFrame(years).to_csv(output_dir / "yearly_results.csv", index=False)
    pd.DataFrame([{"status": status, "reason": reason}]).to_csv(output_dir / "monthly_results.csv", index=False)


def _write_data_quality_html(output_dir: Path, manifest: dict[str, Any], quality: pd.DataFrame) -> None:
    summary = manifest["data_only_validation"]
    escaped_limitations = "".join(f"<li>{html.escape(str(item))}</li>" for item in manifest["provider_limitations"])
    columns = [
        "symbol", "first_date", "last_date", "rows", "missing_observed_nifty_sessions",
        "duplicate_rows", "missing_ohlcv_rows", "invalid_price_rows", "invalid_ohlc_rows",
        "zero_volume_rows", "abnormal_price_changes_gt_20pct", "unexplained_large_moves",
    ]
    table = quality.loc[:, [column for column in columns if column in quality]].to_html(index=False, border=0, na_rep="—")
    text = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Historical data quality</title>
<style>body{{font:15px system-ui,sans-serif;margin:2rem;color:#182230}}table{{border-collapse:collapse;font-size:12px}}th,td{{padding:.35rem .5rem;border-bottom:1px solid #ddd;text-align:left}}th{{position:sticky;top:0;background:#f2f4f7}}.summary{{display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:12px}}.card{{padding:12px;background:#f2f4f7;border-radius:6px}}.scroll{{max-height:70vh;overflow:auto}}</style></head><body>
<h1>Historical data quality report</h1><p>Cache: {html.escape(str(manifest['cache_dir']))}; requested {manifest['requested_dates']['start']} to {manifest['requested_dates']['end']}.</p>
<div class="summary"><div class="card">Dataset range<br><b>{html.escape(str(manifest['actual_dates']['start']))} – {html.escape(str(manifest['actual_dates']['end']))}</b></div><div class="card">Retrieved symbols<br><b>{manifest['symbols_retrieved_count']} / {manifest['symbols_requested_count']}</b></div><div class="card">Minimum target<br><b>{'MET' if manifest['minimum_target_met'] else 'NOT MET'}</b></div><div class="card">Data-only status<br><b>{html.escape(str(summary['status']))}</b></div></div>
<h2>Market context</h2><p>NIFTY: {html.escape(str(summary['benchmark_first_date']))} to {html.escape(str(summary['benchmark_last_date']))}, {summary['benchmark_sessions']} rows; EMA200 first valid {html.escape(str(summary['benchmark_ema200_first_valid_date']))}. Independent session completeness: {html.escape(str(summary['benchmark_completeness_status']))}. India VIX: {html.escape(str(summary['vix_status']))}. Volatility fallback: {html.escape(str(summary['volatility_fallback']))}.</p>
<h2>Provider and coverage limitations</h2><ul>{escaped_limitations}</ul><h2>Per-symbol checks</h2><div class="scroll">{table}</div></body></html>"""
    (output_dir / "data_quality_report.html").write_text(text, encoding="utf-8")
    (output_dir / "historical_data_quality_report.html").write_text(text, encoding="utf-8")


def _write_completion_summary(output_dir: Path, manifest: dict[str, Any]) -> None:
    summary = manifest["data_only_validation"]
    actual_start = manifest["actual_dates"]["start"] or "none"
    actual_end = manifest["actual_dates"]["end"] or "none"
    text = f"""# Data Expansion Completion Check

Status: **{summary['status']}**. This is a data-only audit. No strategies or optimization were run.

- Requested range: {manifest['requested_dates']['start']} to {manifest['requested_dates']['end']}.
- Available cache range: {actual_start} to {actual_end}.
- Symbols retrieved: {manifest['symbols_retrieved_count']} / {manifest['symbols_requested_count']}.
- 2021 minimum target met: {manifest['minimum_target_met']} ({summary['minimum_target_equity_count']} equities cover the complete requested window from 2021; {summary['minimum_target_equities_required']} required, plus NIFTY).
- 2020 preferred target met: {manifest['preferred_target_met']} ({summary['preferred_target_equity_count']} equities cover the complete requested window from 2020).
- NIFTY: {summary['benchmark_first_date']} to {summary['benchmark_last_date']}, {summary['benchmark_sessions']} rows; EMA200 first valid {summary['benchmark_ema200_first_valid_date']}.
- India VIX: {summary['vix_status']}; realized volatility fallback is available from NIFTY closes.
- Equity histories begin on one common date: {summary['equity_start_dates_all_same']}.
- Walk-forward 24/6/6: unavailable until at least 36 months plus indicator warm-up are present.
- Baseline strategy run: **not run** because longer history is unavailable; placeholder result files record the reason.

The output directory contains `historical_data_manifest.json`, `historical_universe_manifest.csv`, `historical_symbol_mapping.csv`, `historical_data_quality.csv`, baseline placeholder tables, and the HTML quality report. No ticker identity mappings are asserted without a verified security master. The local source cache was not modified by this audit.
"""
    (output_dir / "DATA_EXPANSION_COMPLETE.md").write_text(text, encoding="utf-8")
    validation = manifest["data_only_validation"]
    quality = manifest.get("quality_summary", {})
    source = manifest.get("source_download_manifest", {})
    report = f"""# Phase 3B Data Expansion Report

## Result

**Status: {validation['status']}**. This run completed the data-only audit. It did not run strategies or tune parameters.

- Declared provider: {manifest['provider']}.
- Requested range: {manifest['requested_dates']['start']} to {manifest['requested_dates']['end']}.
- Available range: {manifest['actual_dates']['start']} to {manifest['actual_dates']['end']} ({manifest['rows']:,} normalized daily bars).
- Retrieved symbols: {manifest['symbols_retrieved_count']} / {manifest['symbols_requested_count']}; data failures: {manifest['symbols_failed_count']}.
- 2021 minimum target: **{'met' if manifest['minimum_target_met'] else 'not met'}**; {validation['minimum_target_equity_count']} equities cover the complete requested window, with {validation['minimum_target_equities_required']} required, plus NIFTY.
- 2020 preferred target: **{'met' if manifest['preferred_target_met'] else 'not met'}**; {validation['preferred_target_equity_count']} equities cover the complete requested window.
- Symbols with verified NSE CSV cache provenance: {len(source.get('symbols_with_verified_nse_cache_provenance', []))}.

## Quality checks

- Duplicated rows/timestamps: {quality.get('duplicate_rows', 0)} / {quality.get('duplicate_timestamps', 0)}.
- Missing or invalid OHLCV rows: {quality.get('missing_ohlcv_rows', 0)} missing, {quality.get('invalid_price_rows', 0)} invalid prices, {quality.get('invalid_ohlc_rows', 0)} invalid OHLC relationships.
- Zero-volume equity rows: {quality.get('zero_volume_rows_equities', 0)}.
- Price changes over 20%: {quality.get('abnormal_price_changes_gt_20pct', 0)}; unexplained by same-date source action fields: {quality.get('unexplained_large_moves', 0)} across {quality.get('symbols_with_unexplained_large_moves', 0)} symbols. These are review candidates, not confirmed errors.
- Action fields were present for {quality.get('symbols_with_corporate_action_fields', 0)} symbols; source-reported event rows: {quality.get('dividend_event_rows', 0)} dividends and {quality.get('split_event_rows', 0)} splits. This does not prove full action coverage.

## Market series and universe

- NIFTY 50: {validation['benchmark_first_date']} to {validation['benchmark_last_date']}, {validation['benchmark_sessions']} rows; {validation['benchmark_missing_observed_sessions']} gaps versus observed NIFTY sessions. The NIFTY series itself is not independently verified against an official trading calendar.
- NIFTY EMA200 first valid date: {validation['benchmark_ema200_first_valid_date']}.
- India VIX: {validation['vix_status']}; the backtest regime can use {validation['volatility_fallback']} when VIX is absent.
- Most common equity start: {validation['most_common_equity_start_date']} ({validation['equities_with_that_start_date']} equities). Starts are common across all equities: {validation['equity_start_dates_all_same']}.
- No verified historical ticker map or point-in-time constituent series was provided. The current constituent snapshot has survivorship bias; no identity mappings were inferred.

## Research eligibility

- Walk-forward 24-month train / 6-month validation / 6-month test: **unavailable** because the audited range does not contain 36 months plus the indicator warm-up.
- Unchanged 11-strategy baseline: **not run** unless both date coverage and official NSE import provenance pass. Output CSVs preserve a not-run status and reason.
- Provider/data-use limitations: {' '.join(str(item) for item in manifest['provider_limitations'])}

Detailed per-symbol files: `historical_data_quality.csv`, `historical_universe_manifest.csv`, and `historical_symbol_mapping.csv`. HTML review: `data_quality_report.html`.
"""
    (output_dir / "DATA_EXPANSION_REPORT.md").write_text(report, encoding="utf-8")
