# Synthetic intraday software fixtures

Every CSV in this directory is labeled `SYNTHETIC_TEST_FIXTURE`. These rows
are deterministic test inputs only. They are not NSE observations, cannot pass
the Phase 4E rights/provenance gate, are never copied into `data/intraday/`, and
are never used as a production fallback.
