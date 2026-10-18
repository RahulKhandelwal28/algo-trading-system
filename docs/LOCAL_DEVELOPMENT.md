# Local development dashboard and Phase 3D / 4A safety

## Architecture

The dashboard uses the existing Python `ThreadingHTTPServer` in `src/nse_research/dashboard.py`. This one service serves both:

- the static page at `/` and its local `/styles.css` and `/app.js` assets;
- read-only JSON endpoints under `/api/dashboard/`.

The page and API share one origin and port. There is no separate Flask, FastAPI, Django, Node, or paper-trading service to start. The dashboard reads the V2 Phase 3D SQLite ledger using SQLite read-only mode. It also reads the configured manifests, market-data CSVs, frozen-universe identity file, Phase 3C/validation results, and existing report files. It does not call the daily runner, acquire data, or modify the experiment. The frontend requests use `GET`; the backend rejects `POST`, `PUT`, `DELETE`, and `PATCH` with HTTP 405. `HEAD` is supported for local page/assets and health checks.

The Python service uses port `8765` and binds to `127.0.0.1`. The dashboard HTML/CSS/JavaScript are served locally. React and ReactDOM are currently loaded by `index.html` from `unpkg.com`, so the browser needs access to that CDN to render the React UI; the dashboard API and paper-trading data remain local.

## Browser views and read-only routes

The default **System** view shows the paper-only mode, live/broker flags, experiment progress, latest processed date, next required session, and any missing-data blocker. Other views expose experiment details, the existing forward-paper account/signals/orders/positions/trades, configured strategy parameters, risk and cost settings, daily data health, source coverage, the explicit-EQ frozen universe, historical research/report status, Phase 4A realtime feed health/candles, and Phase 4G-YF first-session status. The Phase 4G-YF view reads fixed session artifacts only; it never starts data acquisition or paper execution.

The local JSON routes are:

| Route | Read-only contents |
| --- | --- |
| `GET /health` | Service status, mode, broker/live flags, configured provider, and Phase 3D progress/blocker |
| `GET /api/dashboard/system` | System mode, safety flags, ledger path, experiment summary |
| `GET /api/dashboard/experiment` | Manifest progress, next expected date, and exact missing-file reasons |
| `GET /api/dashboard/overview` | Latest forward-paper portfolio snapshot and that session's signals |
| `GET /api/dashboard/signals` | Recorded signals; supports date, symbol, strategy, side, and accepted/rejected filters |
| `GET /api/dashboard/positions` | Open and closed paper positions |
| `GET /api/dashboard/performance` | Forward-paper equity, P&L, and trade metrics |
| `GET /api/dashboard/strategies` | Recorded strategy-account metrics and configured strategy parameters |
| `GET /api/dashboard/activity` | Recent recorded signals, orders, closed trades, and risk events |
| `GET /api/dashboard/risk` | Configured risk/cost settings and read-only ledger risk observations |
| `GET /api/dashboard/data-health` | Last run's data-health and reconciliation details |
| `GET /api/dashboard/data-sources` | Equity/index coverage, source notes, and explicit frozen-universe series counts |
| `GET /api/dashboard/research` | Phase 3C, walk-forward/Monte Carlo, Phase 3D summary, and available report links |
| `GET /api/dashboard/realtime` | Read-only provider configuration, feed health snapshot, validated quotes, market session, and real intraday candles |
| `GET /api/dashboard/phase4g-yf` | Read-only Phase 4G-YF 2026-09-28 preparation/session artifacts, candidate rankings, data quality, and paper results |
| `GET /api/dashboard/daily-report?date=YYYY-MM-DD` | Existing daily report JSON for a recorded session |
| `GET /research-report/<id>` | A report selected from the dashboard's fixed report catalog |

The server only serves the three known UI assets (`/`, `/styles.css`, and `/app.js`) plus cataloged reports. It does not expose arbitrary project files. The report catalog is read-only and only contains reports already present under `results/`.

## Prerequisites

- Python 3.11 or later.
- The project's `.venv` with `requirements.txt` installed.
- Existing Phase 3D V2 config, experiment manifest, ledger, and dashboard assets.

If setting up the environment for the first time:

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Start the local dashboard

From the repository root, run:

```bash
./scripts/start_local.sh
```

Open <http://127.0.0.1:8765/>. The startup script checks the virtual environment, imports the dashboard dependency chain, validates the local UI files and Phase 3D paper-only state, then starts the existing Python dashboard service. Its output reports the current experiment session count and whether the next expected session is waiting for validated EOD files.

The same URL is the backend/API URL because the Python service serves both the page and API. The dashboard reads `config/phase3d_personal.yaml` and its configured V2 ledger in `results/phase3d_forward_20260926_225042/`.

The script pins `LIVE_TRADING_ENABLED=false`, clears the paper-run opt-in variable, and starts no acquisition, realtime worker, or trading process. It prints the configured realtime mode, live-trading status, and broker-execution status. Press **Ctrl+C** to stop the dashboard. It runs as the foreground process, so stopping it closes the local server.

## Read-only behavior and safety

Opening or refreshing the UI only makes local `GET` requests to the dashboard API. The Python service opens the SQLite ledger with `mode=ro`; it has no order-execution API. Its `POST`, `PUT`, and `DELETE` handlers return HTTP 405. The startup command does not run `scripts/run_phase3d_personal_daily.py`, advance the session count, fill pending paper orders, create market data, or connect to a broker.

The personal profile selects the user-authorized `jugaad_data` source and five NSE EQ symbols; the shared defaults remain disabled and the dashboard starts no provider. For the separate paper-signal session, run `PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --preflight`, then use `--max-polls 1` at market open before continuous monitoring. It passes validated closed sampled bars to the existing scanner and writes paper candidate levels separately; it cannot create orders, fills, or paper-ledger changes. See [PHASE4G_JUGAAD_PAPER_QUICKSTART.md](PHASE4G_JUGAAD_PAPER_QUICKSTART.md) and [PHASE4A_REALTIME_DATA.md](PHASE4A_REALTIME_DATA.md).

For the optional Groww market-data feed, use [PHASE4G_GROWW_PAPER_QUICKSTART.md](PHASE4G_GROWW_PAPER_QUICKSTART.md). That path requires a Groww Trading API subscription, a locally provisioned short-lived access token, and the official instrument CSV. It does not use OpenAlgo and does not call order APIs. It writes recommendation snapshots separately and leaves the current Jugaad profile and Phase 3D ledger untouched.

The current manifest records 1 of 21 sessions, latest processed 2026-09-25. The expected 2026-09-28 session remains blocked until exact-date validated equity data and required official NIFTY and India VIX rows are available. Starting or refreshing the dashboard does not change this status or the ledger.

## Run a paper session separately

Only run the daily workflow when actual EOD data for the requested NSE session has been supplied and validated. The daily runner performs its preflight before the normal paper session; it fails closed on missing or invalid data. It does not use a dashboard startup as a trigger.

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
source .venv/bin/activate
export PYTHONPATH=src
export PAPER_TRADING_ENABLED=true
export LIVE_TRADING_ENABLED=false
python scripts/run_phase3d_personal_daily.py --market-date YYYY-MM-DD
```

If using an explicitly configured HTTPS EOD source, supply `--source-name` and `--source-url` as documented in [PHASE3D_DAILY_PAPER_TRADING.md](PHASE3D_DAILY_PAPER_TRADING.md). That command can acquire and, after validation and preflight, process the requested session; do not use it merely to open the dashboard.

## Tests and checks

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
source .venv/bin/activate
export PYTHONPATH=src
pytest -q
python -m compileall src scripts
node --check forward/dashboard/app.js
```

## Troubleshooting

### Port 8765 is already in use

Stop the other local service or choose another loopback port:

```bash
DASHBOARD_PORT=8766 ./scripts/start_local.sh
```

The server remains bound to `127.0.0.1` on either port.

### A local page or asset returns 404

Run `./scripts/start_local.sh` and open the printed root URL. The existing Python dashboard maps `/`, `/styles.css`, and `/app.js` directly. Do not serve the repository's `forward/` directory with `python -m http.server`; that bypasses the dashboard API and puts the HTML and its root-relative assets at different paths.

Check local routes with:

```bash
curl -i http://127.0.0.1:8765/
curl -i http://127.0.0.1:8765/styles.css
curl -i http://127.0.0.1:8765/app.js
curl -i http://127.0.0.1:8765/api/dashboard/overview
```

If the HTML, CSS, and JavaScript return 200 but the React UI stays on “Loading”, check browser access to the `unpkg.com` React and ReactDOM scripts. If an API route fails, inspect the startup config and configured read-only SQLite ledger path.
