"""Import an explicitly authorized local NSE CM EOD delivery into Phase 3D."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from nse_research.data.licensed_nse_cm_eod import LicensedNseCmEodProvider, NseCmEodSchema
from nse_research.data.licensed_nse_cm_eod_setup import import_authorized_nse_cm_eod
from nse_research.data.providers import NseOfficialHistoryCsvProvider
from nse_research.forward.engine import load_local_universe


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import an authorized, locally supplied NSE CM EOD CSV using the existing normalizer"
    )
    parser.add_argument("--source-dir", default="data/licensed_nse_cm_eod/incoming")
    parser.add_argument("--dataset", default="data/licensed_nse_cm_eod/nse_cm_eod_dataset.csv")
    parser.add_argument("--start", required=True, help="first requested date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="last requested date, YYYY-MM-DD")
    parser.add_argument("--authorization-reference", required=True,
                        help="explicit NSE license/authorization reference for this delivery")
    parser.add_argument("--source-provider", help="provider name only when supplied by the authorized source")
    parser.add_argument("--delivery-date", help="source delivery date, YYYY-MM-DD, if supplied")
    parser.add_argument("--source-identifier", help="source batch/order identifier, if supplied")
    parser.add_argument("--adjustment-status",
                        help="adjusted/unadjusted status only when the source documentation states it")
    parser.add_argument("--symbols-file", default="data/universe.csv")
    parser.add_argument("--index-data-dir", default="data/nse_exports")
    parser.add_argument("--date-column", required=True)
    parser.add_argument("--symbol-column", required=True)
    parser.add_argument("--series-column", required=True)
    parser.add_argument("--open-column", required=True)
    parser.add_argument("--high-column", required=True)
    parser.add_argument("--low-column", required=True)
    parser.add_argument("--close-column", required=True)
    parser.add_argument("--volume-column", required=True)
    return parser


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _official_sessions(path: Path) -> list[date] | None:
    if not path.is_file():
        return None
    parser = NseOfficialHistoryCsvProvider(path.parent)
    try:
        frames = parser._read_export(path, "^NSEI", preserve_invalid=True)
        if not frames:
            return None
        index = pd.concat(frames).index
        return sorted({pd.Timestamp(stamp).date() for stamp in index if pd.notna(stamp)})
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    try:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        if start > end:
            raise ValueError("--start must be on or before --end")
        if args.delivery_date:
            date.fromisoformat(args.delivery_date)
        schema = NseCmEodSchema(
            date_column=args.date_column,
            symbol_column=args.symbol_column,
            series_column=args.series_column,
            open_column=args.open_column,
            high_column=args.high_column,
            low_column=args.low_column,
            close_column=args.close_column,
            volume_column=args.volume_column,
        )
        source_dir = _resolve(root, args.source_dir)
        dataset_path = _resolve(root, args.dataset)
        symbols_path = _resolve(root, args.symbols_file)
        index_dir = _resolve(root, args.index_data_dir)
        expected_symbols = load_local_universe(symbols_path)
        session_dates = _official_sessions(index_dir / "index_NSEI.csv")
        provider = LicensedNseCmEodProvider(
            source_dir,
            schema,
            dataset_path=dataset_path,
            manifest_path=dataset_path.parent / "nse_cm_eod_import_manifest.json",
            report_path=dataset_path.parent / "nse_cm_eod_validation_report.json",
            exchange_sessions=session_dates,
        )
        outcome = import_authorized_nse_cm_eod(
            provider=provider,
            start=start,
            end=end,
            authorization_reference=args.authorization_reference,
            expected_symbols=expected_symbols,
            source_provider=args.source_provider,
            delivery_date=args.delivery_date,
            source_identifier=args.source_identifier,
            adjustment_status=args.adjustment_status,
        )
    except Exception as error:
        print(f"Licensed NSE CM EOD import failed closed: {type(error).__name__}: {error}", file=sys.stderr)
        return 2

    print(f"Import: {outcome['import'].get('validation_status', 'FAIL')}")
    print(f"Dataset: {outcome['import'].get('dataset_path') or 'not written'}")
    print(f"Coverage: {dataset_path.parent / 'coverage_report.json'} ({outcome['coverage']['validation_status']})")
    print(f"Provenance: {dataset_path.parent / 'provenance.json'}")
    return 0 if outcome["coverage"]["validation_status"] in {"PASS", "PASS_WITH_WARNINGS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())

