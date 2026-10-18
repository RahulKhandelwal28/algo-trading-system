# NSE CM EOD Sample Audit

## 1. Executive Summary

**Status: `AUTHORIZATION_NOT_PROVIDED`.** No authorized NSE CM EOD sample was found in `data/licensed_nse_cm_eod/incoming/`, and no permission evidence was supplied. Technical sample validation is therefore **NOT_ASSESSABLE**. This is an intake-preparation report, not a successful data audit.

No normalized sample was created. The existing Phase 3C Kaggle experiment, TejHQ audit, all research results, the licensed Phase 3D provider path, strategies, risk, and costs were left unchanged. Phase 3C and Phase 3D were not run.

## 2. Authorization Status

- **OBSERVED FACT:** `manifest/source_authorization.json` has `authorization_status: NOT_PROVIDED`, a null authorization reference, and no terms file.
- **OBSERVED FACT:** No authorization agreement, invoice/terms package, NSE confirmation, or project-specific authorization reference was supplied with this task.
- **UNKNOWN:** Whether any NSE CM EOD product acquired under a future agreement permits the intended use. A public NSE-hosted report by itself does not establish permission for the project's retained historical research dataset.
- **Gate:** `UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED`.

The manifest records the target provider/product named by the task, not proof that a source delivery was received or licensed.

## 3. Source Provenance

There is no sample filename, file hash, byte size, acquisition date, source identifier, delivery coverage, or authorization reference to record. Corresponding provenance fields remain `null` in `manifest/sample_provenance.json`.

The official CM ZIP files retained under `data/tejhq/audit/official_cm/` were downloaded for the separate TejHQ missing-session investigation. They are not represented as licensed NSE EOD product deliveries and were not used as sample input.

## 4. Source Format

No sample was supplied, so its container, encoding, headers, record layout, and product version are unknown. The prepared local audit accepts CSV, CSV.GZ, and CSV.ZIP. It rejects binary formats with `BINARY_FORMAT_REQUIRES_SPECIFICATION` until NSE supplies a format specification. The project documentation records that NSE's public All Reports archive shows a 2024 transition from legacy CM bhavcopy to UDiFF, but those public report formats must not be assumed to match a separately licensed delivery.

## 5. Column Mapping

`manifest/source_column_mapping.json` lists the 13 target fields and required/optional field groups. Every source-header mapping is `null`, and `mapping_status` is `UNVERIFIED_NO_SAMPLE`. No example header names have been copied into the mapping. Exact headers, date formats, numeric separators, source format, and version must be recorded from an authorized sample and its technical documentation before audit.

The series mapping is required. The audit never creates `EQ` when a source series field is absent.

## 6. EQ-Series Coverage

**NOT_ASSESSABLE:** there are no source rows. No total row, EQ row, non-EQ row, unique EQ symbol, unique EQ date, or observed series count is claimed. Once provided, all source series values will be reported, and only source rows whose explicitly mapped series is exactly `EQ` are eligible for EQ validation.

## 7. Date Coverage

**NOT_ASSESSABLE:** no source dates exist. The audit is prepared to compare source EQ dates with the local official references `data/nse_exports/index_NSEI.csv` and `data/nse_exports/index_INDIAVIX.csv`. A NIFTY-only date is reported as `POSSIBLE_EQ_DATA_GAP`; it is not automatically called a non-trading date or a confirmed data gap. A CM sample's explicit EQ row is direct evidence of an EQ date. The calendar comparison does not infer sessions from weekdays.

## 8. OHLCV Validation

**NOT_ASSESSABLE:** no OHLCV rows exist. The sample auditor requires positive open/high/low/close, non-negative volume, finite explicitly parsed numeric values, and `low <= open/close <= high` with tolerance `1e-9`. Invalid rows are to be recorded in `audit/invalid_ohlcv.csv`; they are not repaired.

Raw prices are not adjusted. There is no corporate-action adjustment in the sample audit.

## 9. Duplicate Validation

**NOT_ASSESSABLE:** no sample keys exist. The prepared audit key is `(date, symbol, series)`. It reports duplicate key groups, excess duplicate rows, and conflicting duplicate groups in `audit/duplicate_records.csv`. A duplicate causes the technical gate to fail; source rows are never silently deduplicated.

## 10. Symbol/ISIN Validation

**NOT_ASSESSABLE:** no sample symbol or ISIN values exist. The audit will report rows with and without ISIN, unique symbols/ISINs, symbol-to-multiple-ISIN and ISIN-to-multiple-symbol diagnostics. It will not infer ticker continuity or merge securities. Any identity relationship requires explicit source documentation.

## 11. Historical Security Coverage

**UNKNOWN:** the absent sample gives no evidence of current, historical, or delisted-security coverage, and no security-reference history was supplied. The intended full period is 2020-01-01 through the latest available date, but full-period availability and delisting completeness require NSE confirmation and actual delivery evidence.

## 12. Corporate Actions

**OBSERVED FACT:** no corporate-action file or adjustment specification was supplied. **UNKNOWN:** whether a future NSE delivery includes corporate actions or whether its prices are adjusted. The audit preserves delivered prices and does not create adjusted prices.

## 13. Extreme Return Diagnostics

**NOT_ASSESSABLE:** no closes exist. When sample rows exist, `audit/extreme_returns.csv` will report observed-bar close-to-close moves above 10%, 20%, and 50%, including the prior observed date and elapsed calendar-day gap. These are diagnostic flags only; no observation is removed.

## 14. Known Limitations

- No actual authorized sample, technical specification, license terms, or source delivery metadata were available.
- Public bhavcopies from the prior TejHQ audit are not substituted for a licensed product sample.
- Source mappings remain blank by design; the future source may use different headers/formats on either side of the 2024 UDiFF transition.
- No audit conclusion about data completeness, issuer identity history, corporate actions, or licensing can be drawn until evidence is supplied.
- The sample audit tool is local-only and is separate from Phase 3D. A passing sample does not create the full Phase 3D dataset.

## 15. Acceptance Gates

Current `data/licensed_nse_cm_eod/audit/sample_validation.json`:

| Gate | Status | Basis |
|---|---|---|
| Authorization | FAIL | `NOT_PROVIDED`; no explicit reference or permitted-use scope |
| Schema | NOT_ASSESSABLE | no sample |
| EQ series | NOT_ASSESSABLE | no source rows |
| OHLCV | NOT_ASSESSABLE | no source rows |
| Duplicates | NOT_ASSESSABLE | no source keys |
| Dates | NOT_ASSESSABLE | no source dates |
| Coverage | NOT_ASSESSABLE | no source dates to compare |
| Identity | NOT_ASSESSABLE | no source symbols/ISINs |
| Overall technical validation | NOT_ASSESSABLE | no sample |
| Overall acceptance | `UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED` | authorization absent |

No `normalized/nse_cm_eod_sample.csv` was written. The normalized sample is created only when authorization is confirmed for all intended uses and technical validation passes.

## 16. Recommendation for Full Acquisition

**Do not begin full acquisition yet.** Obtain written NSE authorization and applicable terms that explicitly cover local retention, quantitative research, historical backtesting, walk-forward research, and paper-trading simulation. Request one real sample, source schema/version documentation, date/number conventions, symbol and ISIN history, corporate-action policy, and written coverage/de-listing details. Then populate the manifests from those supplied facts and audit the sample before planning a full delivery.

Phase 3D remains `PHASE3D_FORWARD_BLOCKED` because `data/licensed_nse_cm_eod/nse_cm_eod_dataset.csv` is absent. This workflow does not alter that guard or run Phase 3D.
