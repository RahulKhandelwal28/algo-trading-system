# Phase 4G-YF.1 — Yahoo Revision Reconciliation

Review date: 2026-09-28 (Asia/Kolkata)  
Result: **BLOCKED — post-immutability revisions are retained; Yahoo refresh returned no rows**

## Executive result

```text
PHASE4G_YF_1_STATUS=BLOCKED
BAR_IDENTITY=(SYMBOL,MARKET_TIMESTAMP)
BAR_SETTLING_SECONDS=5
IMMUTABLE_BAR_READINESS=BLOCKED
BLOCKING_REASON=POST_IMMUTABILITY_REVISION
```

The five-second closed-bar gate was exercised against the protected Phase 4G monitor history. Every one of the 12,762 retained revision records was received after that bar's one-minute interval plus the five-second settling period. Those revisions affect 11,487 unique bar identities. Their frozen first/latest snapshots were not replaced; affected bars are marked conflicting and excluded from strategy use.

The one authorized Yahoo refresh requested the frozen 486-symbol universe in ten batches. All ten batches failed after DNS resolution errors for `query2.finance.yahoo.com`; 30 attempts (20 retries) returned zero bars. No HTTP status or response body was obtained. The request did not produce fresh rows with which to update the reconciliation.

The readiness gate stopped processing before scanner, strategy, or execution calls. This run generated zero signals, orders, fills, broker calls, live orders, and paper-ledger mutations. A single-session paper test was not run because the controlled-refresh gate did not pass.

## Revision findings

| Evidence set | Conflicts / records | OHLC changes | Volume changes | Zero to positive | Receipt-time classification |
|---|---:|---:|---:|---:|---|
| Previous controlled-refresh audit | 1,027 conflicts | 643 | 1,004 | 1,004 | All 1,027 second observations were post-immutability; 327 first observations were in settling and 700 first observations were already post-cutoff. |
| Protected monitor revision history | 12,762 revision records across 11,487 unique bar keys | 1,608 | 12,125 | 1,430 | 12,762 later observations were post-immutability; 777 first observations were in settling, 11,985 first observations were post-cutoff, and none arrived before interval close. |
| Phase 4G-YF.1 one-shot refresh | 0 rows | 0 | 0 | 0 | No Yahoo bars received; all batches failed at DNS resolution. |

The first two evidence sets are different audit windows and must not be added as if they were disjoint. The original controlled-refresh record remains in `results/phase4g_yf_20260928_correctness_audit/`; the older monitor's raw fetches and compact revisions remain in `results/phase4g_yf_20260928_monitor/`.

For the retained monitor history, all 12,762 payload changes classify as `POST_IMMUTABILITY_REVISION`; none classify as `PRE_CLOSE_REVISION`, `PRE_IMMUTABILITY_REVISION`, or `UNKNOWN`. The 12,762 records affect 11,487 unique keys, with 1,275 additional revision records beyond one per affected key. Volume changed in 12,125 records (1,430 zero-to-positive and 10,695 positive-to-zero). OHLC changed in 1,608 records; 971 records changed both OHLC and volume. All replayed observations passed structural timestamp and OHLCV validation, but that does not establish which conflicting provider payload is economically correct.

## Cause assessment

- **Forming/incomplete bars:** Not supported as the primary explanation. None of the retained monitor's first observations arrived before the minute ended. Some first observations arrived during the five-second settling interval, but every later conflicting observation arrived after the freeze cutoff.
- **Delayed volume updates:** The revisions are predominantly volume changes. However, the retained receipt times put the changes after the configured freeze, so the gate cannot safely treat them as normal settling updates.
- **Yahoo historical corrections:** Possible, but the available Yahoo payloads contain no revision identifier or correction reason to distinguish corrections from other provider-side changes.
- **Repeated overlapping fetches:** Confirmed as the mechanism producing repeated observations of the same `(symbol, market_timestamp)` keys. Re-fetching explains the duplicates, but does not explain why values changed.
- **Timezone/resampling issues:** No evidence was found in the inspected rows: timestamps are timezone-aware and minute-aligned, and the path uses native one-minute bars. This does not rule out an upstream Yahoo error.
- **OHLC inconsistencies:** Conflicting OHLC values are observed (643 in the prior controlled refresh, 1,608 records in retained monitor history). The values passed structural OHLC checks; the cause and authoritative value remain unknown.
- **Current refresh:** It supplied no observations, so it cannot distinguish provider correction, delayed volume, or another upstream cause.

## Immutable-bar gate

```text
FORMING_BARS=0
SETTLING_BARS=0
FROZEN_BAR_SNAPSHOTS=105328
CLEAN_IMMUTABLE_BARS=93841
CONFLICTING_BARS=11487
REJECTED_OR_BLOCKED_BARS=11487
PRE_IMMUTABILITY_REVISIONS=0
POST_IMMUTABILITY_REVISIONS=12762
UNKNOWN_CONFLICTS=0
PHASE4G_YF_IMMUTABLE_BAR_READINESS=BLOCKED
```

A frozen snapshot remains retained when a later revision conflicts; it is not silently replaced. Conflicting bars do not enter the canonical strategy store.

## Controlled refresh

```text
REQUESTED_SYMBOLS=486
BATCH_SIZE=50
MAX_CONCURRENT_BATCHES=2
BATCHES=10
SUCCESSFUL_BATCHES=0
FAILED_BATCHES=10
ATTEMPTS=30
RETRIES=20
ROWS_SEEN=0
NEW_MARKET_BARS=0
DUPLICATE_IDENTICAL=0
HTTP_STATUS=NOT_OBTAINED
NETWORK_ERROR=DNS resolution failure for query2.finance.yahoo.com
USED_FOR_FORWARD_SIGNALS=NO
```

No retry or second refresh was started. The refreshed acquisition report records `used_for_forward_signals=false`. The failed snapshot has no returned market rows to preserve; the pre-existing raw monitor snapshots were left unchanged.

## Data correctness and safety

```text
BAR_IDENTITY=PASS
TIMESTAMP_PROTECTION=PASS
LOOKAHEAD_PROTECTION=PASS
REVISION_HANDLING=PASS
IMMUTABILITY_GATE=BLOCKED
STRATEGY_INPUT_GATE=BLOCKED
SIGNAL_DEDUPLICATION=PASS
PAPER_ORDER_PROTECTION=PASS

PAPER_ONLY=YES
BROKER_CALLS=0
LIVE_ORDERS=0
SIGNALS=0
ORDERS=0
FILLS=0
LEDGER_MUTATIONS=0
```

The executable signal timestamp now begins at the actual decision time after the immutable bar becomes available; `signal_bar_timestamp` and receipt time remain separate fields. This preserves the existing signal-age duration and paper fill mechanics while preventing the paper engine from aging a signal before it could have been observed. The configured target remains `TARGET_R=1.5`; risk parameters were not changed.

The read-only dashboard exposes readiness, forming/settling/immutable/conflicting counts, revision counts, and blocking reasons. It does not start acquisition.

## Tests and checks

Final results are recorded below after the full regression run:

```text
TESTS_PASSED=618
TESTS_FAILED=0
COMPILEALL=PASS
DASHBOARD_JS_SYNTAX=PASS
```

## Artifacts

- `results/phase4g_yf_20260928_revision_reconciliation/reports/revision_summary.json`
- `results/phase4g_yf_20260928_revision_reconciliation/reports/revision_records.jsonl`
- `results/phase4g_yf_20260928_revision_reconciliation/reports/immutable_bar_audit.jsonl`
- `results/phase4g_yf_20260928_revision_reconciliation/reports/readiness_report.json`
- `results/phase4g_yf_20260928_revision_reconciliation/reports/reconciliation_findings.json`
- `results/phase4g_yf_20260928_revision_reconciliation/reports/acquisition_report.json`
- `results/phase4g_yf_20260928_revision_reconciliation/batch_manifest.json`

## Next status

```text
NEXT_STATUS=BLOCKED
```

The existing post-immutability conflicts prevent a readiness pass, and the single controlled refresh received no data. Do not run the single-session paper test or continuous monitor from this result. Preserve this reconciliation output and the earlier audits; any future qualification must use a successful, separately authorized refresh and resolve the post-freeze conflicts without rewriting prior observations.
