# Phase 3D — Forward Paper Trading

## Separate personal one-month experiment

The one-month personal paper experiment uses a separately frozen configuration and ledger so the existing licensed-data Phase 3D baseline remains intact:

```text
config/phase3d_personal.yaml
data/paper_trading/PHASE3D_PERSONAL_BASELINE_V2.json
data/paper_trading/phase3d_personal_universe_v2.csv
results/phase3d_forward_20260926_225042/
```

Its equity source is the previously audited Yahoo-derived local OHLCV cache in `data/cache/`. This is third-party market data, not official NSE or licensed NSE equity data. The separate experiment does not use Kaggle or TejHQ rows. NIFTY 50 and India VIX continue to come from the official NSE CSVs under `data/nse_exports/`. The cache provides equity history through 2026-09-25; its raw OHLC is retained as supplied, while exchange-equivalent corporate-action treatment is not independently certified.

The corrected V2 experiment freezes 486 current universe identities with explicit `Series=EQ`, at least 220 valid warm-up bars, and no records rejected by the existing 40% close-change review threshold. It excludes the DUMMY placeholder, current non-EQ HFCL, six symbols below warm-up before the official-session audit, three symbols that fail that existing data-quality threshold, and four symbols that fall below the warm-up minimum after the official-session filter. The experiment is configured for 2026-09-25 through 2026-10-27, with 21 successfully processed NSE sessions; weekends and the listed 2026-10-02 and 2026-10-20 holidays do not count.

The NSE calendar audit identified 2,351 off-session cache rows in the original 490-symbol scan. V2 excludes 2,331 off-session rows from its selected 486-symbol input, on five dates, and records their dates/counts in `data_manifest.json`; it does not edit cache files. One V2 session, 2026-09-25, passed preflight and was processed from this filtered local cache. It generated 28 signals, accepted 5, and created 5 simulated entry orders; there were no fills because the execution model waits for the next valid session open. The V2 experiment is **IN_PROGRESS**, not a one-month result. The earlier 490-symbol run at `results/phase3d_forward_20260926_222615/` remains unchanged; V2's manifest identifies it as superseded, and V2 does not include it in its session count or report.

### Daily HTTP EOD acquisition

`src/nse_research/forward/acquisition.py` adds a bounded, provider-neutral HTTPS batch adapter. It retains each response body under `data/paper_trading/raw/`, hashes the raw bytes, records source URL/status/retries and coverage in `raw/daily_manifest.jsonl`, maps supported CSV/JSON source fields into the existing intake schema, and invokes the existing exact-date EQ validator. A passing response is written to `data/paper_trading/normalized/YYYY-MM-DD.csv`. Raw files are immutable; repeated successful acquisition uses checksum-verified cached files. The wrapper sequence is **acquire → validate/normalize → preflight → paper run**. Acquisition errors and preflight failures do not update the paper ledger or processed-session count.

Before requesting a source, the adapter checks the requested date against dates present in the official NSE NIFTY export. If that export does not yet contain a requested future session, it reports `WAITING_FOR_YYYY_MM_DD_EOD_DATA` and makes no HTTP request. Requests use HTTPS, bounded timeouts/retries/backoff, and a conservative rate limit. Redirects, credentials, broker access, and access-control bypasses are not supported; HTTP 401/403/429 and block pages fail closed.

The existing equity cache is Yahoo-derived third-party data. Yahoo-style chart responses do not provide NSE `Series`, so they fail with `DATA_SOURCE_NOT_SUITABLE`; the adapter never derives `EQ` from the frozen identity file. The project's controlled sample of the NSE public security-history endpoint also returned no EQ rows, so that endpoint is not selected as a daily source. No source is silently chosen or switched. A configured HTTPS provider must return a batch CSV or JSON with explicit date, symbol, series, OHLC, and volume.

For a provider that meets that data contract, acquire and process a date with:

```bash
PYTHONPATH=src PAPER_TRADING_ENABLED=true LIVE_TRADING_ENABLED=false \
  python scripts/run_phase3d_personal_daily.py \
    --market-date YYYY-MM-DD \
    --source-name 'provider name' \
    --source-url 'https://provider.example/eod?date={date}&symbols={symbols}'
```

The URL must be HTTPS and contain `{date}`; `{symbols}` is optional. The command makes a single batch request, retains the raw body, validates/normalizes it, then runs preflight and only then invokes the paper trader. No API credentials are accepted or stored. Official NIFTY and India VIX files remain separate and are never overwritten.

### Dated local EOD intake

For a later session, place one exact-date file at:

```text
data/paper_trading/incoming/YYYY-MM-DD.csv
```

It must contain only the frozen experiment's required identities and use the normalized columns below. Every required symbol needs exactly one record for that date with its source `Series=EQ`; series values are never created or inferred.

```text
date,symbol,series,open,high,low,close,volume
2026-09-28,ABC,EQ,100,105,98,103,100000
```

The importer leaves the source untouched, rejects malformed values, missing/unknown symbols, date mismatches, duplicate symbol/date keys, invalid OHLC, and non-EQ identities, then writes a manifest-backed file under `data/paper_trading/normalized/`. It does not infer provider or adjustment provenance; pass optional provenance flags only when the source explicitly supplies those facts.

Run exactly one date with the local-only wrapper. It imports a dated file if present, runs preflight, records a blocked status without changing the paper ledger if preflight fails, and processes that one date only when every check passes:

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
source .venv/bin/activate
export PYTHONPATH=src
export PAPER_TRADING_ENABLED=true
export LIVE_TRADING_ENABLED=false
python scripts/run_phase3d_personal_daily.py --market-date YYYY-MM-DD
```

To inspect readiness without running a session:

```bash
python -m nse_research.daily_paper_trader \
  --config config/phase3d_personal.yaml \
  --provider personal_local_eod \
  --preflight --market-date YYYY-MM-DD
```

Missing exact-date equity input returns `FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA`. The paper provider has no broker execution interface. It does not use Kaggle, TejHQ, or licensed data as an automatic fallback. The daily wrapper contacts only the explicitly supplied HTTPS EOD data URL and applies the raw-retention and schema gates above; it never contacts a broker. India VIX can use the existing causal NIFTY realized-volatility fallback only when the official VIX row is unavailable or stale; malformed VIX data blocks the session.

The isolated result directory contains `config_snapshot.yaml`, `data_manifest.json`, `strategy_manifest.json`, `experiment_manifest.json`, `daily_snapshots.csv`, `signals.csv`, `orders.csv`, `fills.csv`, `positions.csv`, `trades.csv`, `costs.csv`, `events.jsonl`, `summary.json`, and `REPORT.md`. Dated Markdown/JSON reports are also copied into `daily/YYYY-MM-DD/`. `PHASE3D_FORWARD_REPORT.md` is generated only after 21 valid processed sessions. Each of the 11 daily strategies is reported independently in baseline order, with no ranking or selection.

The existing dashboard remains read-only. To display this separate experiment ledger, start it with:

```bash
python -m nse_research.dashboard \
  --config config/phase3d_personal.yaml \
  --database results/phase3d_forward_20260926_225042/paper_ledger.sqlite \
  --host 127.0.0.1 --port 8765
```

## Scope and readiness

Phase 3D runs the frozen daily equity strategy set against newly supplied NSE cash EOD data. It is a local paper simulator and research recorder. It does not place broker orders, contact a broker, or change strategy, risk, cost, or historical backtest code.

The immutable run baseline is `data/paper_trading/PHASE3C_BASELINE.json` (`baseline_sha256: 2f2b3d054cdd8e86865141bc08bb30ffcc2517af224143ac3d673811b2b33cd9`). It records the Phase 3C daily strategy names, strategy settings, risk and cost settings, slippage, the configured universe snapshot, the licensed NSE CM EOD provider, source hashes, and Phase 3C source-manifest metadata. The runner validates that manifest before each workflow. A mismatch fails closed; make a separately named/versioned experiment for any future change.

The daily baseline has these 11 strategies: trend, momentum, breakout 20, breakout 50, previous-day breakout, pullback, mean reversion, swing 1–3, 3–5, 5–10, and 10–20. Intraday VWAP is excluded from daily runs. Existing Phase 3C risk and cost values remain frozen: 1% risk per trade, 2% daily loss, 10% portfolio drawdown, five open positions, 25% sector exposure, and the configured NSE cost/slippage model.

The baseline freezes the current `data/universe.csv` for Phase 3D. That configured 500-symbol universe differs from the Phase 3C Kaggle run universe by three symbols. It is recorded as configured and is not represented as a constituent-for-constituent match to Phase 3C.

**Current data gate:** `data/licensed_nse_cm_eod/nse_cm_eod_dataset.csv` is not present in this project. Real forward trading evidence therefore has not started. The runner will not fall back to Yahoo, Kaggle, old caches, or another provider. Supply the canonical file under the applicable NSE data authorization, then start the workflow below.

## Paper safety

Every run requires `PAPER_TRADING_ENABLED=true` and `LIVE_TRADING_ENABLED=false`. The CLI prints:

```text
MODE: PAPER TRADING
LIVE TRADING: DISABLED
BROKER ORDERS: DISABLED
```

The forward engine does not import or call an execution broker. Its orders and fills are internal SQLite records. It also refuses a data provider exposing common live-order methods. `live_trading.enabled` must stay false. The dashboard is local-only and GET-only; state-changing HTTP methods return 405.

## Approved data inputs

The configured provider is `licensed_nse_cm_eod`, backed by the canonical normalized CSV from the existing licensed NSE CM importer:

```text
data/licensed_nse_cm_eod/nse_cm_eod_dataset.csv
Symbol,Series,Date,Open Price,High Price,Low Price,Close Price,Total Traded Quantity
```

Only `Series=EQ` rows are read. The importer and forward runner make no automatic provider switch. NIFTY and India VIX are read from the official NSE index exports already held under `data/nse_exports/`:

```text
data/nse_exports/index_NSEI.csv
data/nse_exports/index_INDIAVIX.csv
```

### Import and audit an authorized delivery

Place the locally supplied delivery under `data/licensed_nse_cm_eod/incoming/`. Do not rename or map its headers by guesswork: obtain the eight exact source column names from the authorized delivery documentation, and pass them to the existing importer wrapper. The wrapper invokes `LicensedNseCmEodProvider`, publishes the canonical dataset only when that importer validates the source rows, and writes `coverage_report.json` and `provenance.json` beside the dataset. The coverage report compares the imported dates with the official local NSE NIFTY sessions and the configured project universe. The provenance file omits provider, delivery date, source identifier, and adjustment status unless those facts are explicitly supplied.

Example command shape (replace every `<...>` value with the licensed delivery's documented fact):

```bash
export PYTHONPATH=src
python scripts/import_licensed_nse_cm_eod.py \
  --source-dir data/licensed_nse_cm_eod/incoming \
  --start 2020-01-01 --end 2026-09-26 \
  --authorization-reference '<NSE authorization reference>' \
  --source-provider '<provider, if stated>' \
  --delivery-date '<YYYY-MM-DD, if supplied>' \
  --source-identifier '<batch identifier, if supplied>' \
  --adjustment-status '<only if documented>' \
  --date-column '<exact source header>' \
  --symbol-column '<exact source header>' \
  --series-column '<exact source header>' \
  --open-column '<exact source header>' \
  --high-column '<exact source header>' \
  --low-column '<exact source header>' \
  --close-column '<exact source header>' \
  --volume-column '<exact source header>'
```

Never change or synthesize a source `Series`; only explicit `EQ` records enter the canonical dataset. The importer does not adjust prices. An absent adjustment statement remains absent in provenance.

Before a first forward run, check readiness with:

```bash
python -m nse_research.daily_paper_trader --preflight
```

This command is read-only: it creates no orders, does not write the paper ledger, and does not create run reports. It checks the canonical equity file, both official index files, configured symbols, the required warm-up and market-date rows, data quality, the paper/live switches, provider selection, the frozen baseline, and existing runs for that date. It prints `PHASE3D_READY` only when every check passes; otherwise it prints `PHASE3D_FORWARD_BLOCKED` with the reasons. A ready result is a separate step from starting the normal daily command below.

Each requested equity, NIFTY, and India VIX series must contain a valid row for the requested market date. Duplicate session dates, invalid OHLCV, nonpositive prices, malformed values, stale dates, missing required symbols, or configured abnormal close moves block signal generation. Index observations are not invented or substituted. Every run records provider, dataset, paths, market date, data timestamps, requested/received/missing symbols, per-symbol checks, and NSE index values/dates where available.

For local fixture/replay work only, the explicit `local_eod_csv` provider accepts one normalized file per equity in `forward/market_data/SYMBOL_1d.csv` with `timestamp,open,high,low,close,volume`. It is not an automatic fallback; selecting a different provider requires a separately reviewed baseline version.

## Session detection and idempotency

`data/paper_trading/nse_cash_holidays_2026.csv` contains the local 2026 NSE cash-market holiday/special-session calendar from NSE circular [NSE/CMTR/71775](https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf). The holiday list must be maintained from official NSE circulars as new calendar years are published. Observed approved market data identifies a trading session, including a special weekend session. A listed holiday or ordinary weekend without market data is `NON_TRADING_DAY`; a special session without data or an unknown weekday without data is `DATA_UNAVAILABLE`. Unknown weekdays are not assumed holidays.

A successful date is committed once to the daily journal and portfolio snapshot. Re-running it appends `RUN_STARTED` and `RUN_SKIPPED_DUPLICATE` audit events and does not add signals, orders, positions, snapshots, or daily-journal rows. A failed/stale-data date has no successful daily-journal row and can be retried. The JSONL event journal is append-only, so failed-attempt records remain available after recovery.

## Daily order, position, and accounting lifecycle

A close-of-day signal creates a `CREATED` simulated entry order. On a later valid session it fills at that session's open with the existing adverse slippage assumption and the shared `CostModel`; if the opening gap exceeds the saved risk budget, the order is rejected. No partial fills are simulated. Filled orders remain `FILLED` after the related position later closes; order status and position status are separate records.

Order records keep the requested/raw signal price, simulated execution price, slippage, creation/fill timestamps, quantity, side, status, and fee components separately: brokerage, STT, exchange charges, GST, SEBI charges, stamp duty, DP charge, and total costs. Position records retain entry/exit dates and prices, quantity, current mark, realized/unrealized P&L, holding sessions, status, and total recorded costs. Costs are not folded into execution prices.

Open positions are marked to the latest close using the configured adverse liquidation slippage and estimated exit costs. Stop/target exits use the daily OHLC bar. When both thresholds are touched, the existing conservative `stop_first` rule applies. A daily candle cannot establish intraday event order beyond this assumption.

The account records cash, reserved cash, market value, gross/net exposure, realized and unrealized P&L, transaction costs, equity, daily/cumulative returns, peak, drawdown, and max drawdown. Every valid trading-day run reconciles cash plus net liquidation market value to equity, the starting-capital/P&L bridge, linked orders and positions, unique orders/snapshots, and valid quantities/prices. A failed reconciliation is recorded and raises an error; the ledger is not silently repaired.

Strategy-account metrics are a separate descriptive view with the configured starting capital as each strategy's own denominator. They are not combined into a strategy portfolio and are not ranked. Minimum sample labels are `INSUFFICIENT_FORWARD_SAMPLE`, `EARLY_FORWARD_SAMPLE`, and `MEANINGFUL_FORWARD_SAMPLE`; small samples are not statistically meaningful.

## Run the daily forward workflow

Once the licensed canonical equity file and both current official index exports are in place:

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
source .venv/bin/activate
export PYTHONPATH=src
export PAPER_TRADING_ENABLED=true
export PAPER_STARTING_CAPITAL=100000
export LIVE_TRADING_ENABLED=false
python -m nse_research.daily_paper_trader
```

The no-argument command chooses the latest completed candidate in `Asia/Kolkata`, classifies the exchange session, and validates exact-date EOD availability before signals. To specify a completed production market date:

```bash
python -m nse_research.daily_paper_trader --market-date YYYY-MM-DD
```

A historical replay is isolated from production storage and reports:

```bash
python -m nse_research.daily_paper_trader --date YYYY-MM-DD
```

`--date` uses `data/paper_trading/replay/forward_paper.sqlite` and `results/phase3d_forward/replay/`. It cannot accept `--database`. Keep `LIVE_TRADING_ENABLED=false` for all commands. If the data gate fails, inspect the event journal and data-quality report; do not substitute another source.

## Persistent records and reports

Production ledger:

```text
data/paper_trading/forward_paper.sqlite
data/paper_trading/events/YYYY-MM-DD.jsonl
```

Append-only compatibility exports and cumulative production reports live in `results/phase3d_forward/`. Each market date has:

```text
results/phase3d_forward/YYYY-MM-DD/
  daily_report.json
  daily_report.md
  signals.csv
  orders.csv
  trades.csv
  positions.csv
  portfolio.csv
  risk_events.csv
  data_quality.json
  run_manifest.json
```

`run_manifest.json` includes baseline hash, provider/dataset, timestamps, universe, symbol coverage, reconciliation, and explicit `live_trading_enabled: false` / `broker_contacted: false` fields. The cumulative research note is `docs/PHASE3D_FORWARD_RESEARCH.md`; it separates forward paper results from historical Phase 3C. Monthly snapshots are written to `results/phase3d_forward/monthly/YYYY-MM/`. Prior finalized months are not rewritten. A Phase 3C comparison is written only after at least 30 closed forward trades across at least three calendar months.

The forward event stream includes run/session/data states, generated and rejected signals, orders, fills, positions, risk triggers, marks, reconciliation, reports, and failures. Do not delete or edit journal files or daily reports as a way to alter experimental results.

## Scheduler example (macOS cron)

Schedule after the licensed distributor's confirmed EOD delivery time. The example below uses 18:30 Asia/Kolkata; adjust it if the licensed feed is delivered later. The same exact-date data checks run before signal generation, so a missing file/date results in a recorded failure and no signals.

```bash
mkdir -p /Users/rahul.khandelwal/Documents/personal/algo_trading_system/logs
crontab -e
```

Add one line:

```cron
30 18 * * 1-5 /bin/zsh -lc 'cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system && source .venv/bin/activate && export PYTHONPATH=src PAPER_TRADING_ENABLED=true PAPER_STARTING_CAPITAL=100000 LIVE_TRADING_ENABLED=false && python -m nse_research.daily_paper_trader' >> /Users/rahul.khandelwal/Documents/personal/algo_trading_system/logs/phase3d_forward.log 2>&1
```

The runner handles exchange holidays and weekends using the local official calendar and available data. Update the calendar from the official NSE holiday circular each year; an unlisted weekday with no exact-date data remains `DATA_UNAVAILABLE` and will not generate signals.

## Read-only dashboard

In a second terminal:

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
source .venv/bin/activate
export PYTHONPATH=src
python -m nse_research.dashboard --host 127.0.0.1 --port 8765
```

Open <http://127.0.0.1:8765/>. The **Forward Paper Trading** view shows account metrics, all frozen strategy accounts, recent signals/orders/trades/risk events, and data health. The API binds to loopback and is read-only. Its static React runtime is loaded from `unpkg.com`; market and portfolio APIs remain local.

## Tests and evidence boundary

Run deterministic local/synthetic tests with:

```bash
source .venv/bin/activate
export PYTHONPATH=src
pytest -q
```

Tests do not contact NSE, Kaggle, or brokers. They include a five-day lifecycle (signal, delayed fill/mark, exit and costs, sector-risk rejection, duplicate-date rerun), reconciliation, append-only journal checks, failure/retry, data staleness, baseline drift, calendar handling, and read-only dashboard behavior.

The default licensed-provider workflow has not been run because the licensed NSE CM EOD dataset is missing. The separate personal experiment above has one processed session from a Yahoo-derived local cache and is still in progress; one session does not establish monthly performance, live fill quality, or strategy profitability.
