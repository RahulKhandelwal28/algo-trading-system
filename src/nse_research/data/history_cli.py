"""Date-ranged historical downloads and data-only expansion reporting."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from nse_research.config import AppConfig
from nse_research.data.baseline import run_fixed_baseline_11
from nse_research.data.expansion import normalize_symbol, write_data_expansion_artifacts
from nse_research.data.providers import NseOfficialHistoryCsvProvider, YahooFinanceProvider, validate_and_normalize_bars

LOGGER = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_symbols(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "Symbol" not in frame:
        raise ValueError(f"symbol file must contain a Symbol column: {path}")
    return list(dict.fromkeys(normalize_symbol(value) for value in frame["Symbol"].dropna() if str(value).strip()))


def download_history_main(argv: list[str] | None = None) -> int:
    root = _project_root()
    parser = argparse.ArgumentParser(description="Import and resume date-ranged NSE CSV exports in a local research cache")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--start", default="2020-01-01", help="Inclusive ISO date; default 2020-01-01")
    parser.add_argument("--end", help="Inclusive ISO date; defaults to today in the configured timezone")
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--symbols", help="Comma-separated symbols; overrides --symbols-file")
    parser.add_argument("--symbols-file", default="data/universe.csv")
    parser.add_argument("--source-dir", default="data/nse_exports", help="Official NSE CSV exports downloaded from the NSE historical-data pages")
    parser.add_argument("--cache-dir", default="data/historical_cache", help="Separate cache; existing Phase 2/3 cache is left intact")
    parser.add_argument("--force-refresh", action="store_true", help="Re-import source exports even when the cache already covers the requested dates")
    parser.add_argument("--manifest", help="Download manifest path; defaults inside --cache-dir")
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = root / config_path
    config = AppConfig.load(config_path)
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else datetime.now(ZoneInfo(config.project.timezone)).date()
    if start > end:
        parser.error("--start must be on or before --end")
    if args.symbols:
        symbols = list(dict.fromkeys(normalize_symbol(value) for value in args.symbols.split(",") if value.strip()))
    else:
        symbols_file = Path(args.symbols_file)
        if not symbols_file.is_absolute():
            symbols_file = root / symbols_file
        symbols = [symbol for symbol in _read_symbols(symbols_file) if not symbol.removesuffix(".NS").startswith("DUMMY")]
    symbols = list(dict.fromkeys([*symbols, config.universe.benchmark, config.regime.india_vix_symbol]))
    cache_dir = Path(args.cache_dir)
    if not cache_dir.is_absolute():
        cache_dir = root / cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.source_dir)
    if not source_dir.is_absolute():
        source_dir = root / source_dir
    provider = NseOfficialHistoryCsvProvider(source_dir)
    prior_manifest_path = cache_dir / "historical_download_manifest.json"
    try:
        prior_download_manifest = json.loads(prior_manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        prior_download_manifest = {}
    previously_verified = set(
        prior_download_manifest.get(
            "symbols_with_verified_nse_cache_provenance",
            prior_download_manifest.get("symbols_loaded_from_official_csv", []),
        )
    )
    prior_source_files = dict(prior_download_manifest.get("source_files_by_symbol", {}))
    bars: dict[str, pd.DataFrame] = {}
    source_missing: list[dict[str, str]] = []
    official_symbols: list[str] = []
    reused_cache_symbols: list[str] = []
    source_files_by_symbol: dict[str, str] = {}
    for symbol in symbols:
        cache_path = cache_dir / YahooFinanceProvider.cache_filename(symbol, args.interval)
        cached = pd.DataFrame()
        if cache_path.exists():
            try:
                raw_cached = pd.read_csv(cache_path, parse_dates=["timestamp"], index_col="timestamp")
                existing, _ = validate_and_normalize_bars(raw_cached, args.interval)
                if args.force_refresh:
                    cached = existing.loc[(existing.index.date < start) | (existing.index.date > end)]
                else:
                    cached = existing
            except Exception as exc:
                LOGGER.warning("Ignoring malformed cache %s: %s", cache_path, exc)
        source_file = provider.find_export(symbol, args.interval)
        exported = provider.history(symbol, start, end, args.interval)
        if not exported.empty:
            official_symbols.append(symbol)
            if source_file is not None:
                source_files_by_symbol[symbol] = str(source_file.relative_to(source_dir))
            merged = pd.concat([cached, exported]).sort_index()
            merged = merged.loc[~merged.index.duplicated(keep="last")]
            merged, _ = validate_and_normalize_bars(merged, args.interval)
            temporary = cache_path.with_name(f".{cache_path.name}.tmp")
            merged.to_csv(temporary, index_label="timestamp")
            temporary.replace(cache_path)
            cached = merged
        if not cached.empty:
            window = cached.loc[(cached.index.date >= start) & (cached.index.date <= end)]
            if not window.empty:
                bars[symbol] = window
                if exported.empty:
                    reused_cache_symbols.append(symbol)
            else:
                source_missing.append({"symbol": symbol, "reason": "cache contains no bars inside the requested date range"})
        else:
            source_missing.append({"symbol": symbol, "reason": "no official NSE export or reusable cache file found"})
    failures = [row for row in source_missing if row["symbol"] not in bars]
    verified_symbols = set(official_symbols) | (set(reused_cache_symbols) & previously_verified)
    source_files_by_symbol = {**prior_source_files, **source_files_by_symbol}
    manifest = {
        "provider": "NSE official historical CSV export" if verified_symbols else "Unverified local cache; no official NSE CSV rows loaded",
        "requested_dates": {"start": start.isoformat(), "end": end.isoformat()},
        "interval": args.interval,
        "retrieval_date": datetime.now(ZoneInfo(config.project.timezone)).date().isoformat(),
        "symbols_requested": symbols,
        "symbols_retrieved": sorted(bars),
        "symbols_imported_from_official_csv_this_run": sorted(official_symbols),
        "symbols_with_verified_nse_cache_provenance": sorted(verified_symbols),
        "symbols_reused_from_cache_without_official_csv": sorted(reused_cache_symbols),
        "source_files_by_symbol": source_files_by_symbol,
        "symbols_failed": failures,
        "source_files_missing": source_missing,
        "rows_by_symbol": {symbol: len(frame) for symbol, frame in sorted(bars.items())},
        "actual_dates_by_symbol": {
            symbol: {"start": frame.index.min().date().isoformat(), "end": frame.index.max().date().isoformat()}
            for symbol, frame in sorted(bars.items()) if not frame.empty
        },
        "cache_dir": str(cache_dir),
        "resume": not args.force_refresh,
        "source_dir": str(source_dir),
        "provider_failures": {row["symbol"]: row["reason"] for row in failures},
        "source_limitations": [
            "NSE historical exports are provided as per-security CSVs and must first be downloaded from the official website.",
            "The exported historical price-volume columns contain unadjusted OHLC and traded quantity; corporate-action adjustment data is not part of these exports.",
            "NSE website terms and applicable data-use rights govern storage, reuse, and redistribution.",
        ],
    }
    manifest_path = Path(args.manifest) if args.manifest else cache_dir / "historical_download_manifest.json"
    if not manifest_path.is_absolute():
        manifest_path = root / manifest_path
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Imported {len(bars)} of {len(symbols)} symbols from NSE exports/cache in {cache_dir}")
    print(f"Download manifest: {manifest_path}")
    if failures:
        LOGGER.warning("%d symbols are unavailable; see the download manifest", len(failures))
        return 1
    return 0


def data_expansion_main(argv: list[str] | None = None) -> int:
    root = _project_root()
    parser = argparse.ArgumentParser(description="Validate historical market data without running strategies")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", help="Inclusive ISO date; defaults to today in the configured timezone")
    parser.add_argument("--symbols-file", default="data/universe.csv")
    parser.add_argument("--cache-dir", default="data/historical_cache")
    parser.add_argument("--output-dir", help="Defaults to results/data_expansion_<timestamp>")
    parser.add_argument("--provider", help="Override the provider label; normally inferred from the download manifest")
    parser.add_argument("--provider-limitation", action="append", default=[], help="Document an input-source limitation; may be repeated")
    parser.add_argument("--run-baseline", action="store_true", help="Run only the unchanged 11-strategy baseline after official NSE coverage passes")
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = root / config_path
    config = AppConfig.load(config_path)
    symbols_file = Path(args.symbols_file)
    if not symbols_file.is_absolute():
        symbols_file = root / symbols_file
    symbols = _read_symbols(symbols_file)
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else datetime.now(ZoneInfo(config.project.timezone)).date()
    if start > end:
        parser.error("--start must be on or before --end")
    cache_dir = Path(args.cache_dir)
    if not cache_dir.is_absolute():
        cache_dir = root / cache_dir
    if args.output_dir:
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = root / output_dir
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = root / config.project.result_root / f"data_expansion_{stamp}"
    download_manifest_path = cache_dir / "historical_download_manifest.json"
    provider_failures: dict[str, str] = {}
    source_download_manifest: dict[str, object] = {}
    if download_manifest_path.exists():
        try:
            source_download_manifest = json.loads(download_manifest_path.read_text(encoding="utf-8"))
            provider_failures = source_download_manifest.get("provider_failures", {})
        except (json.JSONDecodeError, OSError):
            provider_failures = {}
            source_download_manifest = {}
    provider_name = args.provider or str(source_download_manifest.get("provider") or "Unverified local CSV cache")
    manifest = write_data_expansion_artifacts(
        project_root=root,
        output_dir=output_dir,
        cache_dir=cache_dir,
        requested_start=start,
        requested_end=end,
        provider=provider_name,
        requested_symbols=symbols,
        min_history_rows=config.universe.min_history_rows,
        provider_failures=provider_failures,
        provider_limitations=args.provider_limitation or None,
        source_download_manifest=source_download_manifest,
    )
    print(f"Data-only validation: {manifest['data_only_validation']['status']}")
    print(f"Range: {manifest['actual_dates']['start']} to {manifest['actual_dates']['end']}; output: {output_dir}")
    if args.run_baseline:
        baseline_result = run_fixed_baseline_11(
            root=root,
            cache_dir=cache_dir,
            output_dir=output_dir,
            manifest=manifest,
            requested_start=start,
            requested_end=end,
        )
        print(f"Baseline-only run: {baseline_result['status']} ({baseline_result['reason']})")
    return 0 if manifest["data_only_validation"]["status"] == "PASS" else 2
