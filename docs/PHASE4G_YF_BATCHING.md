# Phase 4G-YF — Bounded Batching and Receipt-Time Protection

## Scope and status

This change reduces acquisition latency for the frozen Phase 4G-YF universe while keeping the existing scanner, ranking factors, paper risk rules, fill behavior, and Phase 4F registration boundary intact. Data workers return observations and metadata; one main-thread processor validates, ranks, handles strategies, simulates execution, and writes the paper ledger.

**DATA SOURCE = Yahoo Finance**  
**DATA STATUS = THIRD_PARTY_RESEARCH DATA**  
**LIVE TRADING = DISABLED**  
**BROKER = NONE**

No broker path was added. Automated network-facing tests use mocks. After the user explicitly requested a real fetch, a single `--once` collection was run on 2026-09-28; it passed the final data checks described below. No continuous collection was started.

## Previous and new acquisition paths

The earlier forward runner sent the complete 486-symbol universe through one `yf.download()` call with `threads=False`. This avoided yfinance's per-symbol internal thread fan-out, but left acquisition work serialized.

The new path slices the existing insertion-ordered frozen mapping and launches a bounded number of independent batch tasks:

| Setting | Default |
|---|---:|
| Frozen universe | 486 symbols |
| Batch size | 50 symbols |
| Concurrent batches | 2 |
| Batch count | 10 |
| Batch sizes | 50, 50, 50, 50, 50, 50, 50, 50, 50, 36 |

The partitioner preserves the existing frozen order and rejects duplicate mappings. It does not select or drop symbols. `--batch-size 100 --max-concurrent-batches 1` and `--batch-size 50 --max-concurrent-batches 1` are supported. Non-default combinations receive separate output directories by default, such as `results/phase4g_yf_20260928_batch100_c1/`.

### yfinance isolation

The installed yfinance 1.7.0 implementation creates per-call download scratch state, but its `YfData` object is a process-wide singleton containing a shared session and cookie state. For that reason, concurrent production batches use a `ProcessPoolExecutor` with an explicit `spawn` context and `max_workers=max_concurrent_batches`. Each batch still calls `yf.download(..., threads=False)`. This prevents concurrent workers from sharing yfinance's process-global session state and caps the worker count at the configured limit.

Workers return frozen result records and copied data frames. They do not import or write the paper ledger. Batch completion order is retained in response timestamps, and results are passed to the main process in response-receipt order. Decision, scanner, risk, order, fill, P&L, and ledger operations remain serial.

## Timestamp and information-availability rules

Every accepted 1-minute observation stores:

- `market_timestamp`: the provider bar's timestamp, normalized to Asia/Kolkata. The original source time remains in `source_timestamp` and `source_timezone`.
- `received_timestamp`: the local Asia/Kolkata time when the successful response containing that bar returned.
- `request_started_at`, `response_received_at`, and `batch_id` for provenance.

All application timestamps must be timezone-aware. A response timestamp is shared by every bar in that response; the system does not claim that a historical bar arrived at its market timestamp.

Multi-symbol responses can contain a union of timestamps across tickers. If a ticker has no OHLC values at one of those timestamps and its volume is zero or absent, that cell is omitted as an absent bar. It is counted in `empty_no_trade_cells_ignored`; no price or bar is synthesized. Partially missing OHLC, or a missing OHLC cell with positive volume, remains invalid. The absent interval remains visible to the existing coverage checks.

A bar can influence a decision only when its response had arrived by the decision cutoff and its market bar was already closed before that cutoff:

```text
received_timestamp <= decision_timestamp
market_timestamp < decision_timestamp
market_timestamp + bar_interval <= decision_timestamp
status == CLOSED
```

The receipt-time rule is enforced both in the central `rank_candidates()` path and in the forward processor. Missing or naive receipt timestamps fail closed. Future and forming market bars continue to trigger the existing future-data protection.

## Decision snapshots and late data

Before each current-time scanner processing cycle and each scheduled ranking cutoff, the runner records a decision snapshot under `reports/decision_snapshots.jsonl`. Snapshots report available and expected symbols, symbols missing because of late receipt or data quality, eligible bars, bars excluded by receipt time, and bars excluded because they had not closed. Late-bar records include the market and receipt timestamps.

A scheduled ranking snapshot is one-shot. If its grace window expires, the decision is marked skipped; later responses cannot re-run or backfill that timestamp. A response received after a decision may be considered at a later decision if its bars satisfy the ordinary close-time and chronology checks.

Manifest exclusion totals count decision-time exclusion events. The same observation can be counted again at a later cutoff if it remains late for that decision; these fields are not unique-bar totals.

The existing 80% minimum snapshot-coverage gate is unchanged. If a failed batch drops usable coverage below that threshold, the processor fails closed. A failed batch is written to `batch_manifest.json` with its symbols, timestamps, attempts, error type/message, and zero returned bars. Successful batches remain recorded and are not replaced with fabricated, daily, synthetic, or stale data.

## Retries and rate control

The downloader performs at most two retries per batch (three attempts total), with short exponential backoff of 0.25 and 0.5 seconds. Every attempt records its own start, finish, response receipt when successful, duration, and failure details. Failed-attempt timestamps are never attached to accepted bars. Batch workers are capped at two by default, and the collector keeps its existing 60-second refresh interval.

## Ledger and same-response fill behavior

Only the main-thread processor reaches the Phase 4B/4C path. It processes bars chronologically and persists state through the existing isolated Phase 4G ledger. Signals are submitted after bars in the acquired snapshot have been processed, so a bar from the same acquired response cannot retroactively fill an order submitted from that response. The Phase 4C fill model and risk defaults are unchanged.

## Output separation and manifests

The default batched run writes to:

```text
results/phase4g_yf_20260928_batched/
```

The earlier `results/phase4g_yf_20260928/` directory is retained as the legacy serial preparation/run namespace and is not reused by the default batched commands. Each batch configuration can use its own automatically named namespace; `--output-dir` can override it explicitly.

`manifest.json` records the frozen/session universe size, batch size, concurrency, batch count and execution summary, request counts, successes/failures/retries, request and receipt times, receipt latency summaries, decision-time eligibility/exclusion counts, and collection performance timings. `batch_manifest.json` contains the deterministic plan and one execution record per real batch attempt group. A preflight-only manifest contains a batch plan but no fabricated execution records.

Performance fields remain `null` until collection has occurred. The current result states `PERFORMANCE_COMPARISON = NOT_AVAILABLE`: no trustworthy measured serial baseline exists, and no speed improvement is claimed. Future comparisons should use identical date range, symbol ordering, timeout, refresh cadence, and yfinance version; report end-to-end wall time separately from the sum of overlapping worker request durations.

## Tests and limitations

Mocked tests cover the 486-symbol partition, exact ordering and counts, bounded concurrency, out-of-order completion, batch failures, bounded retries, response receipt timestamps, late-data and in-progress-bar exclusion, later-decision availability, CLI defaults/overrides/rejection, and no acquisition-ledger access. Existing Phase 4G tests cover the same-snapshot order/fill guard and ledger idempotency.

Yahoo Finance remains a third-party research source rather than an authorized NSE feed. Batching reduces client-side serial waiting but does not guarantee provider completeness, stable response latency, or availability. No historical research artifact, Phase 4B strategy definition, Phase 4C risk/fill setting, or prior Phase 4G output was changed.

## One-shot live-fetch validation (2026-09-28)

The user-requested real Yahoo fetch used 50-symbol batches and a two-worker maximum. Earlier attempts were retained in the batch manifest and failure reports. The final one-shot refresh, received at about 11:51 IST, returned 73,798 closed 1-minute bars for all 486 symbols. The interval covered 09:15–11:49 IST. The latest acquisition snapshot and an independent Phase 4E validation both passed: 486 symbols present, zero invalid rows, zero duplicate rows, and no missing symbols. Empty per-ticker cells were omitted without filling them. The pipeline reported `PHASE4G_YF_WARMED_UP_NO_RETRO_SIGNALS`; signals, orders, fills, and ledger mutations were all zero.

This was one refresh, not a complete session. The manifest therefore remains `data_acquisition = IN_PROGRESS`. The cumulative `acquisition_report.json` retains failures from earlier attempts and can show an overall `FAIL` even though its `latest_snapshot` is `PASS`; `data_quality_failure.json` and the batch execution history preserve those earlier attempts. The successful cycle measured about 79 seconds end-to-end, including overlapping network requests and processing. `PERFORMANCE_COMPARISON = NOT_AVAILABLE` because there is no measured serial baseline; no speedup is claimed.

## How to run

From the project root:

```bash
source .venv/bin/activate
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py --mode preflight
```

The preflight imports/checks local dependencies and session configuration only; it does not download Yahoo data or touch the ledger. A forward collection, when independently initiated during the configured session and after preflight passes, uses:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode collect --batch-size 50 --max-concurrent-batches 2
```

Alternative isolated configurations:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode collect --batch-size 100 --max-concurrent-batches 1

PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode collect --batch-size 50 --max-concurrent-batches 1
```

`--mode collect` remains session-date/time gated. To request one fetch cycle rather than continuous polling, add `--once`; that option was used for the live-fetch validation above. The default multi-refresh session was not started.
