# Jugaad NSE Paper Signal Session

This is the local, no-broker Phase 4G experiment for the five EQ symbols in
`config/phase3d_personal.yaml`. It uses the installed `jugaad-data` package to
poll NSE quote snapshots, validates source time and explicit EQ identity, builds
sampled 1m/5m bars, and passes immutable closed bars to the existing long-only
intraday scanner.

## Before the session

From the project root:

```bash
source .venv/bin/activate
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --preflight
```

Preflight checks local configuration, the frozen EQ roster, package import,
output-path writability, and paper-only settings. It does not contact NSE and
does not test that NSE will return valid quotes during the next session.

## Run one check, then monitor

At the next NSE cash session:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --max-polls 1
```

If the accepted-symbol list is non-empty, start the continuous monitor:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --interval-seconds 15
```

The default config requests RELIANCE, TCS, INFY, HDFCBANK, and SBIN. The
provider serializes requests with at least one second between request starts
and applies a five-second HTTP timeout to the Jugaad session. The runner stops
when the configured NSE session closes; Ctrl+C stops it earlier. Use
`--symbols RELIANCE,TCS` for a smaller subset or `--recommendations-out PATH`
to choose the snapshot path.

## Output and boundaries

The runner writes a replace-atomically JSON snapshot to
`results/jugaad_paper/<local-date>/paper_recommendations.json` and continues to
write the realtime health snapshot configured in YAML. Candidates carry a
closed-bar timestamp, strategy, entry reference, stop, target, reasons, and
expiry. The entry is a paper reference price; the output has no quantity, order,
fill, broker call, or ledger mutation. A stale or currently unvalidated symbol
is not labeled an active candidate.

This is deterministic rule-based analysis, not an AI model response. The source
is HTTP polling of last-traded-price snapshots, not a WebSocket or an
exchange-native intraday-candle stream. Resulting OHLC is explicitly labeled
approximate, bars with fewer than two accepted snapshots are skipped, and
cumulative quote volume is never fabricated into bar volume. As a result,
VWAP and volume-breakout strategies remain unavailable; only scanner rules that
do not require those data fields can produce candidates. Quote freshness,
explicit NSE EQ, OHLC, closed-bar, and session-calendar gates remain fail-closed.

The first live result can only be established during an open NSE session. NSE
may reject or limit requests, return incomplete data, or change its website
response. If quotes fail validation, no active paper candidate is produced.
This experiment supports NSE cash equities only; it does not provide option
contracts, option premiums, or option-buy recommendations.
