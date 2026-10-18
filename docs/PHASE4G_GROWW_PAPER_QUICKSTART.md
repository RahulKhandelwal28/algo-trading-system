# Groww market-data paper recommendation quickstart

This runs the existing closed-bar NSE cash-equity scanner on Groww's read-only LTP feed. It does not run the Phase 3D ledger or call Groww's order APIs. The Groww Trading API requires an active subscription; current pricing and access are described on the [official Groww API page](https://groww.in/trade-api).

## One-time setup

From the project root:

```bash
source .venv/bin/activate
python -m pip install -r requirements-realtime.txt
mkdir -p data/realtime
curl --fail --location --output data/realtime/groww_instruments.csv \
  https://growwapi-assets.groww.in/instruments/instrument.csv
export GROWW_INSTRUMENTS_CSV="$PWD/data/realtime/groww_instruments.csv"
```

Generate a Groww API access token from the Groww Trading APIs page in your account. Keep it out of YAML, source files, screenshots, and chat. In zsh, enter it without echoing or adding it to shell history:

```zsh
read -s "GROWW_ACCESS_TOKEN?Paste the Groww access token: "
export GROWW_ACCESS_TOKEN
echo
```

The access-token flow expires daily at 6:00 AM according to Groww's documentation, so refresh it as required. Do not commit instrument or credential files.

## Local preflight

```bash
PYTHONPATH=src .venv/bin/python scripts/run_groww_paper.py \
  --preflight \
  --symbols RELIANCE,TCS,INFY,HDFCBANK,SBIN \
  --paper-capital 15000
```

Preflight checks the local SDK installation, access-token presence, instrument CSV schema/mappings, project universe, and paper-only settings. It makes **no Groww request**. A successful preflight does not prove that the feed will connect or that the market is open.

## First live-data check and monitoring

Run only during an open NSE cash session:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_groww_paper.py \
  --max-polls 1 \
  --symbols RELIANCE,TCS,INFY,HDFCBANK,SBIN \
  --paper-capital 15000
```

If the output accepts fresh quotes and you want to continue the bounded paper monitor:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_groww_paper.py \
  --interval-seconds 15 \
  --symbols RELIANCE,TCS,INFY,HDFCBANK,SBIN \
  --paper-capital 15000
```

Stop with Ctrl+C. Results are written under `results/groww_paper/<local-date>/paper_recommendations.json`; feed-health details are written to the configured local realtime health file. Candidate levels are deterministic closed-bar references, and sizing is only an estimate. No active signal is guaranteed, and an empty result is valid.

The monitor only uses cash equities. It does not make options recommendations, create orders, simulate fills, or change the Phase 3D ledger. See [PHASE4G_GROWW_AI_RECOMMENDATION_ARCHITECTURE.md](PHASE4G_GROWW_AI_RECOMMENDATION_ARCHITECTURE.md) for the architecture, constraints, and AI boundary.
