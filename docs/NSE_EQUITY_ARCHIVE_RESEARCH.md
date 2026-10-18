# Official NSE Equity Archive Research

**Reviewed:** 2026-09-26
**Scope:** Official NSE sources for daily cash-market equity OHLCV; no strategy or Phase 3 work.

## Finding

NSE publishes a date-based, all-securities **CM-UDiFF Common Bhavcopy Final** ZIP through [All Reports](https://www.nseindia.com/all-reports). The page showed the 2026-09-25 file as `BhavCopy_NSE_CM_0_0_0_20260925_F_0000.csv.zip`. NSE's [UDiFF bhavcopy specification](https://www.nseindia.com/static/resources/forms-formats-members) defines the required trade date, symbol, series, OHLC, and total traded quantity fields. This is the best technical fit for producing per-symbol files without symbol-level history calls.

The public archive is **not a suitable free source for this project's automated backtest and paper-trading use under the terms currently published by NSE**. NSE's website terms prohibit systematic or automated data collection and prohibit using website data for virtual trading or simulation. The disclaimer's free research-data provision is limited to research use, a 2 GB aggregate ceiling, and individuals from accredited academic institutions, recognized research organizations, and think tanks. No such eligibility or separate license is established for this project. Therefore no market-data files were fetched, no five-symbol sample was tested, and no downloader was implemented.

The next source to pursue is a written **NSE Data & Analytics EOD/historical Capital Market license** that explicitly covers this project's automated ingestion, local retention, research/backtesting, and paper-trading simulation. NSE does not state the public archive's earliest retrievable date or the paid product's historical start date on the pages reviewed, so 2020-01-01 coverage must be confirmed directly before planning a full import.

## Official-source comparison

| NSE source | Data and field fit | Coverage and frequency | Corporate actions and historical symbols | Access, limits, and suitability |
|---|---|---|---|---|
| [CM-UDiFF Common Bhavcopy Final (ZIP)](https://www.nseindia.com/all-reports) | One CM-market file per trade date containing all securities/series. The official schema includes `TradDt`, `TckrSymb`, `SctySrs`, `OpnPric`, `HghPric`, `LwPric`, `ClsPric`, and `TtlTradgVol`, which map directly to the project's output columns. | Daily. The live reports page showed 2026-09-25. NSE says to use this report from 2024-07-08, when the older CM CSV reports were discontinued. The earliest retained date and any multi-date download limits are not stated on the page. | Bhavcopy OHLC is the exchange's actual, unadjusted price data; NSE directs users to bhavcopy for actual prices rather than corporate-action-adjusted 52-week values. It does not carry a corporate-action adjustment series. The UDiFF specification says the file contains symbols eligible to trade on that date, so delisted names can appear in historical daily files if those files are available. Historical archive retention and old/new symbol lineage are not confirmed. | The report is available to download from NSE's page. No public API quota was found. NSE's website terms prohibit automated collection and simulation use, so this route is blocked for the current project without explicit written authorization. |
| Legacy [CM - Bhavcopy (CSV)](https://www.nseindia.com/all-reports) and CM - Common Bhavcopy (CSV) | Legacy daily CM bhavcopy reports; potentially the pre-UDiFF portion of a stitched daily history. The old files were not sampled, so their exact headers and row-validation behavior are not confirmed here. | NSE marks both reports discontinued effective 2024-07-08 and points to UDiFF. Whether legacy daily files remain downloadable back to 2020 is not stated. | Same market-price caveat applies if these are bhavcopy files. No corporate-action map or symbol-lineage file is established by the report listing. | Same website terms and access restrictions as above. Not suitable for an automated downloader without NSE permission. |
| [Full Bhavcopy and Security Deliverable data](https://www.nseindia.com/all-reports) | Current daily CSV report listed by NSE. The page does not publish a field specification there; the required columns, series handling, and equivalence to the target output were not verified from a sample. | A current daily report is listed, but historical start/end availability is not published on the page reviewed. | The report name includes delivery data; corporate-action adjustments and symbol-history guarantees were not established. | Public report download, but governed by the same website terms. Candidate only; not selected over the documented UDiFF bhavcopy. |
| [Paid EOD / Historical Data](https://www.nseindia.com/static/market-data/eod-historical-data-subscription) | NSE describes end-of-day files as binary and says they contain bhavcopy information plus security and trade details for the Capital Market and other segments. NSE publishes a CM EOD technical specification. This is the official subscription path to request the needed records and delivery format. | Daily EOD product. Public page does not state earliest historical date, latest historical date, retention window, or per-request limits; obtain them from NSE Data & Analytics. | Request unadjusted OHLC, trade-date symbol/series, and any separate corporate-action or old/new-symbol records needed. Inclusion of delisted names depends on the licensed historical coverage and delivered security masters. | Subscription/order request and an executed agreement are required. NSE says agreements specify intended use and handling. The published domestic EOD display tariff lists ₹1,00,000 per segment per display medium, effective 2026-04-01; that is not a quote for internal research/non-display or paper-trading use. Request a quote and written scope for this project. |
| [Historical Security-wise Price Volume Data](https://www.nseindia.com/resources/historical-reports-capital-market-daily-monthly-archives) | The previously investigated single-security route returned non-EQ `SLBMH_*`/other series for EQ autocomplete symbols and is already documented as unusable. This research did not revisit or reverse-engineer that endpoint. | Not a date-based broad-universe archive. | No validated EQ history was obtained. | Excluded from acquisition recommendation. See [NSE equity data report](NSE_EQUITY_DATA_REPORT.md). |

NSE's [Data List Under the NSE Data Sharing Policy](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/Data%20list%20under%20NSE%20Data%20Sharing%20Policy%20for%20Research%20and%20Analysis_20250728.pdf) classifies daily historical security-wise OHLC and bhavcopy archives as research-data items on its website. That listing does not remove the website's usage terms or establish permission for this project's automated collection and simulation use.

## UDiFF field mapping

The official UDiFF workbook labels these CM bhavcopy fields:

| Project field | NSE UDiFF field | UDiFF tag |
|---|---|---|
| `Symbol` | Instrument Symbol or Scrip Code | `TckrSymb` |
| `Series` | Instrument Series | `SctySrs` |
| `Date` | Trade Date | `TradDt` |
| `Open Price` | Open | `OpnPric` |
| `High Price` | High | `HghPric` |
| `Low Price` | Low | `LwPric` |
| `Close Price` | Close | `ClsPric` |
| `Total Traded Quantity` | Total Traded Qty | `TtlTradgVol` |

NSE's UDiFF specification says the symbol field should be populated for all symbols/scrips eligible to trade on that trade date. The series field is present for CM. Keep only rows whose series is explicitly `EQ`; do not infer EQ from the ticker. No corporate-action adjustment should be applied.

The UDiFF workbook's nomenclature describes the Bhavcopy file with a `.csv.gz` suffix, while the current All Reports page lists the downloadable report as `.csv.zip`. No archive was fetched, so the website archive's inner filename, compression nesting, and exact row headers still need verification from an authorized sample.

## Availability, limits, and licensing

- The public All Reports page supports date-based report downloads and lists a current final CM-UDiFF file. It does not document the earliest date retained, a 2020–2026 bulk-download interface, a download quota, or a web API rate limit. Availability of a current daily file does not prove all historical dates are retained.
- The page marks the legacy CM Bhavcopy CSV and CM Common Bhavcopy CSV as discontinued effective 2024-07-08. A history spanning 2020 would require a confirmed legacy format before the cutover and UDiFF after it.
- NSE's [Terms of Use](https://www.nseindia.com/static/nse-terms-of-use) prohibit systematic or automated data collection on or in relation to its website and prohibit using website content for gaming, virtual trading, or simulation. The terms also restrict reproduction/storage except where the content is available for download; the download control alone does not grant the prohibited uses.
- NSE's [Disclaimer](https://www.nseindia.com/static/nse-disclaimer) says no-cost research-oriented downloads are capped at 2 GB in aggregate and restricted to people from accredited academic institutions, recognized research organizations, and think tanks. It excludes commercial, speculative, and investigative purposes.
- The [NSE Research Initiative](https://www.nseindia.com/static/research/research-initiatives) accepts data requests for academic, non-commercial research through its form and `nseri@nse.co.in`. This route does not establish permission for the project's paper-trading simulation use.
- NSE's [Data Sharing & Usage Policy](https://www.nseindia.com/static/market-data/nse-data-policy) requires subscribers to submit their requirements and execute an agreement that specifies intended use and handling. Research requests are routed through NSE's Economic Policy Research Department. Commercial market-data access is priced by NSE Data & Analytics.
- NSE's [EOD tariff](https://nsearchives.nseindia.com//web/mediaattachment/2026-04/Download_End_of_the_Day_Data_Tariff_20260424120810.pdf) lists ₹1,00,000 for domestic Capital Market EOD per medium of data display. It should not be treated as the fee for a non-display research/paper-trading license; obtain a use-specific written quote.

## Acquisition route if NSE authorizes the project

Request an NSE Data & Analytics quote for **Capital Market EOD bhavcopy/history covering 2020-01-01 through 2026-09-25** (or the available start date), and ask for written confirmation that the license permits:

1. Automated ingestion and local retention of the licensed daily files.
2. Internal algorithmic research, backtesting, and paper-trading simulation.
3. Creation of per-symbol files with `Symbol,Series,Date,Open Price,High Price,Low Price,Close Price,Total Traded Quantity`.
4. The exact covered dates, archive delivery method, file formats before and after 2024-07-08, and any download/storage limits.
5. Whether historical files include delisted/renamed securities, and whether NSE can provide a date-effective security master and corporate-action reference separately.

The EOD page provides `marketdata@nse.co.in` and `+91-22-2659 8385` for subscription enquiries. If NSE grants the required rights and supplies the archive, implement a local-file `OfficialNSEEquityArchiveProvider` over those licensed daily files. That provider should retain the existing EQ validation, OHLC/volume checks, resumable processing, atomic outputs, and provenance. Do not implement an automated downloader for the public website under the current published terms.

## Sample and project status

- Five-symbol sample for 2025-01-01 through 2025-01-10: **not run**. No licensed sample was obtained, so there are no RELIANCE/TCS/INFY/HDFCBANK/SBIN row counts or OHLC results to report.
- EQ integrity: **not tested** against actual archive records. The official schema has a series field, but schema documentation alone does not validate sample rows.
- Comparison with `index_NSEI.csv`: **not run**; do this after an authorized sample passes.
- Full 2020–2026 acquisition: **BLOCKED** until NSE confirms access rights and date coverage.
- No project code, strategy, Phase 3 output, manifest, or market-data file was changed. Existing 109 passing tests are the previously reported baseline; not rerun because this update changes documentation only.

## Official references

- [NSE All Reports](https://www.nseindia.com/all-reports)
- [NSE Historical Reports — Capital Market Daily/Monthly Archives](https://www.nseindia.com/resources/historical-reports-capital-market-daily-monthly-archives)
- [NSE Forms & Formats — UDiFF file formats and sample-report references](https://www.nseindia.com/static/resources/forms-formats-members)
- [NSE EOD / Historical Data subscription](https://www.nseindia.com/static/market-data/eod-historical-data-subscription)
- [NSE Data Sharing & Usage Policy](https://www.nseindia.com/static/market-data/nse-data-policy)
- [NSE Disclaimer](https://www.nseindia.com/static/nse-disclaimer)
- [NSE Terms of Use](https://www.nseindia.com/static/nse-terms-of-use)
- [NSE Data List under the Data Sharing Policy for Research and Analysis (PDF)](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/Data%20list%20under%20NSE%20Data%20Sharing%20Policy%20for%20Research%20and%20Analysis_20250728.pdf)
- [NSE UDiFF trade and bhavcopy formats (XLSX)](https://nsearchives.nseindia.com//web/mediaattachment/2026-06/UDiFF_trade_and_Bhavcopy_file_formats_20260630115803.xlsx)
- [NSE EOD data tariff (PDF)](https://nsearchives.nseindia.com//web/mediaattachment/2026-04/Download_End_of_the_Day_Data_Tariff_20260424120810.pdf)
