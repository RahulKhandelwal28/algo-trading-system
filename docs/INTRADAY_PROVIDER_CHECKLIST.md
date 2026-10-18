# Intraday Data Provider Checklist

Complete this checklist from written provider documentation and the actual delivery. Mark unresolved facts UNKNOWN; do not rely on a marketing claim or filename as row-level evidence.

## Data identity and interval

- [ ] Is the data genuinely intraday, rather than daily data relabeled as bars?
- [ ] Is the exchange explicitly NSE for every row?
- [ ] Is the series explicitly NSE cash-equity EQ for every row?
- [ ] What is the bar timeframe (1m or 5m)? Are all delivered files consistent?
- [ ] Are symbols canonical NSE symbols? If aliases are used, is a versioned source-to-canonical mapping supplied?
- [ ] Does the identity source include delisted symbols and point-in-time membership where relevant?

## Timestamp and sessions

- [ ] What does each timestamp mean: bar start, bar end, exchange timestamp, or vendor timestamp?
- [ ] What is the source timezone? Are timestamps explicitly offset-aware?
- [ ] Does the source document finalized/closed-bar behavior and corrections/revisions?
- [ ] What date range and session dates are actually covered?
- [ ] Are regular and special sessions documented, including holidays and shortened windows?
- [ ] Are missing bars documented as no-trade, exchange closure, or source gaps?

## Price and volume meaning

- [ ] Are OHLC prices raw, adjusted, or unknown? Which corporate actions and dates are adjusted?
- [ ] Are all symbols on the same adjustment basis?
- [ ] Does volume mean incremental bar shares, cumulative shares, trade count, or another quantity?
- [ ] If cumulative, how and when does it reset? Is conversion to bar volume documented?
- [ ] How are zero-volume bars represented? Are bars emitted only when trades occur?
- [ ] Are corrections, cancellations, auction trades, and outliers described?

## Coverage and quality

- [ ] What are the first and last actual dates, row counts, symbol counts, and session counts?
- [ ] Is a file-level manifest supplied with filenames, sizes, checksums, and immutable revision ID?
- [ ] Are duplicate bars possible, and what source rule governs them?
- [ ] Are expected symbols and expected NSE sessions supplied?
- [ ] Are delisted securities, symbol changes, mergers, splits, suspensions, and renamed companies included or identified?
- [ ] Is historical universe membership point-in-time, or is the dataset subject to survivorship bias?

## Provenance, authorization and retention

- [ ] What is the legal provider/publisher and source URL or delivery channel?
- [ ] What is the immutable dataset/revision identifier and acquisition/delivery timestamp?
- [ ] What license or authorization/reference ID applies to this account and dataset?
- [ ] Is local retention allowed, and for how long?
- [ ] Is personal/noncommercial research and backtesting explicitly allowed?
- [ ] Is commercial use allowed or prohibited?
- [ ] Is redistribution prohibited, restricted, or explicitly permitted?
- [ ] Are underlying market-data rights distinct from the repository's code license?
- [ ] Has the provider supplied a sample, schema, correction policy, and source documentation?

No row may be promoted to research use while required identity, quality, data-rights, or Phase 4E gates remain unresolved. A successful CSV parse is not evidence of authorization or accuracy.
