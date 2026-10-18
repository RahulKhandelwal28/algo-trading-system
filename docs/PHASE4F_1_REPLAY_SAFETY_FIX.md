# Phase 4F.1 — Phase 4D Replay Safety Fix

Date: 2026-09-27  
Repository: `/Users/rahul.khandelwal/Documents/personal/algo_trading_system`  
Final status: **PASS**

## Problem

The standalone Phase 4D `REPLAY` command accepted an arbitrary `--data` CSV and could use the persistent Phase 4C paper ledger by default. That bypassed Phase 4F registration and Phase 4E research-readiness checks.

## Root Cause

`scripts/run_phase4d_intraday_paper.py --mode REPLAY` instantiated `Phase4DOrchestrator` directly. The orchestrator's lower-level CSV replay interface validates bars for execution but does not establish Phase 4F registry membership or research readiness.

## Minimal Fix

- The legacy `run_phase4d_intraday_paper.py --mode REPLAY` path now exits before constructing the orchestrator. It returns `REPLAY_DATA_MUST_BE_PHASE4F_REGISTERED` and directs callers to the Phase 4F replay command. Its `--data` argument is retained for read-only `PRECHECK` only.
- `Phase4DReplayAdapter` now distinguishes an unknown dataset (`PHASE4F_DATASET_NOT_REGISTERED`) from a registered dataset that fails readiness (`PHASE4F_DATA_NOT_RESEARCH_READY`). It reuses the existing Phase 4E readiness checks and canonical provider.
- Replay now requires a date with observed bars in the provider's registered session coverage; otherwise it returns `PHASE4F_SESSION_NOT_AVAILABLE` before importing or invoking the execution engine.
- The explicit ledger argument is checked before replay. Missing ledger returns `PHASE4D_TEMPORARY_LEDGER_REQUIRED`; the configured persistent Phase 4C ledger returns `PHASE4D_PERSISTENT_LEDGER_REJECTED_FOR_REPLAY`. Other ledger paths must remain under the system temporary directory.
- The Phase 4D execution engine, state machine, Phase 4C risk/fill rules, costs, accounting, and session lifecycle were not modified.

## CLI Behavior Before

`python scripts/run_phase4d_intraday_paper.py --mode REPLAY --data <path>` could send a canonical-shaped CSV directly to Phase 4D, and an omitted `--ledger` selected the persistent Phase 4C ledger.

## CLI Behavior After

Historical replay uses only:

```bash
python scripts/replay_intraday_session.py \
  --dataset <REGISTERED_PHASE4F_DATASET_ID> \
  --date YYYY-MM-DD \
  --ledger /tmp/phase4f_replay_ledger.json
```

The route requires a registered dataset, Phase 4E research readiness, the verified canonical Phase 4F output, and an observed requested session. The legacy direct-CSV replay route is blocked. There is no daily, Yahoo, TejHQ, Phase 3D, or synthetic fallback.

## Tests

- Full suite: **539 collected, 539 passed; 0 failed** (`PYTHONPATH=src .venv/bin/pytest -q`, exit 0).
- `python -m compileall src scripts tests`: passed.
- Six relevant CLI `--help` checks: passed.
- Actual CLI blocked cases: legacy arbitrary `--data`, missing ledger, unregistered dataset, and persistent ledger each returned exit code 2 with the expected status. No ledger was created for blocked cases.
- A test-only `SYNTHETIC_TEST_FIXTURE` was registered, validated by Phase 4E, normalized, and replayed through the canonical provider, Phase 4B scanner, Phase 4C paper engine, and Phase 4D using two separate temporary ledgers. Both runs completed and produced identical signals, orders, fills, and trades. The fixture is not registered in production. The source-label restriction was overridden only inside this isolated test process so the rest of the path could be exercised; production code still blocks synthetic sources.
- The Phase 4F readiness command still returns `PHASE4F_NO_RESEARCH_READY_DATA`, exit 2, because no production intraday dataset is registered.

## Ledger Safety

- Missing ledger: rejected before dataset lookup or execution.
- Persistent Phase 4C ledger: rejected before dataset lookup or execution.
- Unknown/non-ready datasets and unavailable sessions: rejected before any ledger is opened or created.
- Hash of `data/paper_trading/phase4c/ledger.json` was unchanged across the real CLI checks and test-only end-to-end replay.
- No broker calls occurred; live trading remains disabled.

## Protected Artifact Integrity

SHA-256 comparison covered **363 files** immediately before and after this change, including all existing result files, Phase 4B/4C/4D implementation modules, configuration, Phase 3C/3D universe inputs, and the persistent paper ledger. Changed: **0**; missing: **0**; unexpected additions in the protected set: **0**. An independent comparison to the saved **368-file pre-Phase-4F protected baseline** also found 0 changed and 0 missing files.

Manifests:

- Before: `/private/tmp/phase4f_1_protected_before.json`
- After: `/private/tmp/phase4f_1_protected_after.json`
- Earlier protected baseline: `/private/tmp/phase4f_audit_before.json`

No production intraday data was acquired. The Phase 4F registry has zero datasets, so actual-data replay remains unavailable.

## Final Status

**PASS** — the supported historical replay boundary now requires Phase 4F readiness and a temporary ledger, while the legacy arbitrary-CSV replay command is blocked. This closes the Phase 4D replay safety gate; it does not assert that production intraday data is available.
