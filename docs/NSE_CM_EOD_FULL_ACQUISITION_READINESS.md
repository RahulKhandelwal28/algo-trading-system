# NSE CM EOD Full-Acquisition Readiness

**Decision: NOT READY — `UNKNOWN — REQUIRES NSE CONFIRMATION`.** No authorized NSE CM EOD sample or license documentation is present. Do not infer product rights or delivery coverage from NSE's public historical reports.

| Question | Current answer |
|---|---|
| Can we acquire complete 2020–2026 data? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** No product delivery, availability schedule, retention plan, or history coverage commitment was supplied. |
| Does the sample demonstrate explicit EQ coverage? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** No sample exists. The future mapping requires an explicit source series field and does not fabricate EQ. |
| Does the source contain historical securities? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** No sample or source catalog establishes current, historical, or delisted securities. |
| Does it provide security identity history? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** ISIN/symbol history documentation was not supplied. |
| Does it provide corporate actions? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** No corporate-action source or adjustment methodology was supplied. |
| Backtesting permitted? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** |
| Quantitative research permitted? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** |
| Paper-trading simulation permitted? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** |
| Local storage permitted? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** |
| Internal use permitted? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** |
| Commercial use permitted? | **UNKNOWN — REQUIRES NSE CONFIRMATION.** |

## Evidence required to change this decision

1. An NSE agreement/reference, confirmation, or subscription documentation with applicable terms.
2. Written permission for the specific research, backtesting, walk-forward, simulation, and local-storage uses intended by this project.
3. A real sample obtained under that authorization and its delivery/schema specification.
4. A documented source version and coverage period, including whether delisted securities, historical symbols/ISINs, and corporate-action data are included.
5. A passing sample audit with no unresolved required-field, EQ-series, OHLCV, duplicate, or date-parsing failures.

The blank source mapping and `NOT_PROVIDED` authorization manifest are in `data/licensed_nse_cm_eod/manifest/`. A technically passing sample remains ineligible for Phase 3D until authorization is confirmed and the required full dataset is delivered. No strategy or trading path is activated by the sample audit.
