# OpenAlgo Read-Only Data Smoke Test

This setup checks that the project can reach a running OpenAlgo instance and
receive timestamped NSE one-minute candles for five explicit NSE/EQ symbols.
It only calls OpenAlgo's symbol-search and ticker-history data APIs. It does
not call quote, account, position, or order APIs; it does not generate signals
or write to either paper ledger.

## What you need

1. A running OpenAlgo deployment with the broker session already connected.
2. The base address of that deployment. `https://openalgo.in/` is the public
   product site, not the user's connected API. For local deployments, confirm
   that the OpenAlgo server, rather than another application, owns the port.
3. An OpenAlgo API key created by that deployment. Keep it in a local secret
   manager or shell environment; do not put it in project files or chat.

The project accepts a remote `https://` base URL. Plain `http://` is allowed
only for loopback (`localhost`, `127.0.0.1`, or `::1`).

## Configure without saving the key in shell history

From the project directory in zsh:

```zsh
export OPENALGO_API_BASE_URL='https://your-running-openalgo-host'
read -s 'OPENALGO_API_KEY?OpenAlgo API key: '
export OPENALGO_API_KEY
```

Use the origin of the logged-in OpenAlgo deployment shown in Chrome's address
bar, without a dashboard page path. Do not use the public marketing domain.

## Check the previous completed session

The bounded smoke test defaults to RELIANCE, TCS, INFY, HDFCBANK, and SBIN and
refuses more than five symbols. It validates explicit `NSE` and `EQ` identity,
then downloads the requested date's one-minute ticker history and checks its
date, session timestamps, OHLC, volume, and duplicate timestamps.

```zsh
PYTHONPATH=src .venv/bin/python scripts/check_openalgo_readonly.py --date 2026-10-01
```

The result must say `OPENALGO_READONLY_PREFLIGHT_READY` and show five symbols
passed before proceeding. A future date is rejected. For the next NSE session,
run the same command with that session's date only after bars are available:

```zsh
PYTHONPATH=src .venv/bin/python scripts/check_openalgo_readonly.py --date 2026-10-05
```

To clear the secret from the current shell after the check:

```zsh
unset OPENALGO_API_KEY
```

## Failure meanings

- `HTTP 403`: the base host was reached but the OpenAlgo API key was rejected.
- `HTTP 404`, HTML, or non-JSON: the host/path is not an OpenAlgo API deployment.
- No exact `NSE`/`EQ` result: the symbol mapping is missing or ambiguous; it is
  not eligible for a paper signal.
- No bars or invalid timestamps/OHLC: data is not ready or fails validation.
- Connection failure: the configured host is unreachable from this machine.

Errors are sanitized; API keys are not printed. Do not turn a failed check into
a Yahoo or synthetic fallback.

## Current project boundary

The existing Phase 4G-YF runner is explicitly bound to its 2026-09-28
experiment and does not use OpenAlgo. This smoke test does not change that
runner or connect OpenAlgo bars to the signal engine. A passing check confirms
only that the selected five symbols have usable OpenAlgo data for that date;
the wider universe and paper scanner still need a separate read-only
integration and preflight before use. Live trading remains disabled.
