#!/usr/bin/env python3
"""Build a deterministic realtime subset and its official Upstox instrument map."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nse_research.realtime.upstox import (  # noqa: E402
    DEFAULT_REALTIME_UNIVERSE_SIZE,
    UpstoxConfigurationError,
    build_mapping_rows,
    download_official_bod,
    mapping_artifact_path,
    read_instrument_master,
    read_project_universe,
    realtime_universe_artifact_path,
    search_universe_with_rate_limit,
    select_realtime_universe,
    unverified_mapping_rows,
    write_mapping_csv,
    write_realtime_universe_csv,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create explicit ISIN-verified Upstox NSE EQ mappings")
    parser.add_argument("--universe", default="data/paper_trading/phase3d_personal_universe_v2.csv")
    parser.add_argument("--universe-size", type=int,
                        default=int(os.environ.get("REALTIME_UNIVERSE_SIZE", str(DEFAULT_REALTIME_UNIVERSE_SIZE))),
                        choices=(100, 200, 486))
    parser.add_argument("--output", help="Mapping CSV (defaults to the Phase 4A.8A artifact for the selected size)")
    parser.add_argument("--bod-json", help="Previously downloaded official NSE JSON or JSON.GZ")
    parser.add_argument("--download-bod", action="store_true", help="Make one request to the official Upstox NSE BOD URL")
    parser.add_argument("--search-api", action="store_true", help="Use serial, read-only ISIN searches; requires environment token")
    parser.add_argument("--request-interval-seconds", type=float,
                        default=float(os.environ.get("UPSTOX_MAPPING_REQUEST_DELAY_SECONDS", "0.25")))
    parser.add_argument("--max-retries", type=int,
                        default=int(os.environ.get("UPSTOX_MAPPING_MAX_RETRIES", "3")))
    args = parser.parse_args(argv)
    if args.download_bod and args.search_api:
        parser.error("choose --download-bod or --search-api for one source pass")
    if args.request_interval_seconds < 0 or not 0 <= args.max_retries <= 5:
        parser.error("mapping delay must be non-negative and retries must be between zero and five")

    try:
        frozen_universe = read_project_universe(ROOT / args.universe)
        universe = select_realtime_universe(frozen_universe, args.universe_size)
    except (OSError, ValueError) as error:
        print(f"MAPPING_BLOCKED reason={type(error).__name__}:{error}")
        return 2
    selected_path = write_realtime_universe_csv(
        [{**row, "selection_method": "REQUIRED_SMOKE_FIRST_THEN_FROZEN_FILE_ORDER"} for row in universe],
        realtime_universe_artifact_path(args.universe_size, project_root=ROOT),
    )
    output_path = Path(args.output) if args.output else mapping_artifact_path(args.universe_size, project_root=ROOT)
    if not output_path.is_absolute():
        output_path = ROOT / output_path

    method = "UPSTOX_BOD_JSON"
    if args.search_api:
        token = os.environ.get("UPSTOX_ACCESS_TOKEN", "")
        if not token:
            rows = unverified_mapping_rows(
                universe,
                "UPSTOX_ACCESS_TOKEN is not configured; no Search API request was made",
                method="INSTRUMENT_SEARCH_ISIN",
            )
            output = write_mapping_csv(rows, output_path)
            print(f"UPSTOX_PROVIDER_NOT_CONFIGURED selected={len(universe)} verified=0 output={output.relative_to(ROOT)} universe={selected_path.relative_to(ROOT)}")
            return 2
        rows = search_universe_with_rate_limit(
            universe, token, interval_seconds=args.request_interval_seconds,
            max_retries=args.max_retries,
        )
        method = "INSTRUMENT_SEARCH_ISIN"
    else:
        source = Path(args.bod_json) if args.bod_json else ROOT / "data/upstox/instruments/NSE.json.gz"
        records = None
        if args.download_bod:
            try:
                records = download_official_bod(source)
            except Exception as error:
                safe_reason = str(error) if isinstance(error, UpstoxConfigurationError) else type(error).__name__
                rows = unverified_mapping_rows(
                    universe,
                    f"Official Upstox NSE BOD unavailable or invalid ({safe_reason}); no match conclusion made",
                )
                output = write_mapping_csv(rows, output_path)
                print(f"MAPPING_UNVERIFIED rows={len(rows)} verified=0 source_status={safe_reason} output={output.relative_to(ROOT)}")
                return 3
        elif source.is_file():
            try:
                records = read_instrument_master(source)
            except Exception as error:
                rows = unverified_mapping_rows(
                    universe,
                    f"Official Upstox NSE BOD could not be parsed ({type(error).__name__}); no match conclusion made",
                )
                output = write_mapping_csv(rows, output_path)
                print(f"MAPPING_UNVERIFIED rows={len(rows)} verified=0 source_status={type(error).__name__} output={output.relative_to(ROOT)}")
                return 3
        if records is None:
            rows = unverified_mapping_rows(
                universe,
                "Official Upstox NSE BOD JSON is not present locally; no match conclusion made",
            )
        else:
            rows = build_mapping_rows(universe, records, method=method)

    output = write_mapping_csv(rows, output_path)
    counts = {status: sum(row["mapping_status"] == status for row in rows)
              for status in ("VERIFIED", "MISSING", "DUPLICATE", "INVALID", "UNVERIFIED")}
    status = "MAPPING_VERIFIED" if counts["VERIFIED"] == args.universe_size else "MAPPING_INCOMPLETE"
    print(f"{status} rows={len(rows)} selected={args.universe_size} method={method} verified={counts['VERIFIED']} missing={counts['MISSING']} duplicate={counts['DUPLICATE']} invalid={counts['INVALID']} unverified={counts['UNVERIFIED']} output={output.relative_to(ROOT)} universe={selected_path.relative_to(ROOT)}")
    return 0 if counts["VERIFIED"] == args.universe_size else 3


if __name__ == "__main__":
    raise SystemExit(main())
