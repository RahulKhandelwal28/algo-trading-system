# Groww-fed NSE paper recommendations: architecture and limits

## Recommendation

Start with NSE cash-equity paper recommendations. Do not begin with F&O options on a ₹15,000 account. This project does not yet have an options scanner, option-premium candle pipeline, or option-specific risk model. The existing scanner is a stock-equity engine and cannot safely turn an underlying-stock signal into an option contract recommendation.

Groww's official Trading API currently requires an active subscription. Its product page lists ₹499 per month plus taxes, and its API documentation describes the access token/API-key setup. This is market-data access through a broker API, even though the new project adapter never calls an order endpoint. Check the current Groww price and API terms before subscribing.

## Data and signal flow

```text
Groww LTP feed (NSE/CASH exchange tokens)
  -> official instrument CSV mapping (NSE + CASH + EQ)
  -> source timestamp, symbol, positive-price and freshness checks
  -> sampled 1-minute / 5-minute LTP candles
  -> existing deterministic closed-bar stock scanner
  -> paper-only entry reference, ATR stop, fixed-R target and expiry
  -> optional ₹15,000 share-quantity estimate
  -> atomic JSON recommendation snapshot
```

The Groww feed returns last-traded-price snapshots with source timestamps; it does not supply trade-by-trade history or volume in this adapter. Bars are therefore approximate sampled-LTP OHLC, not exchange-native candles. Volume-based and VWAP-dependent criteria cannot be validated from this feed and remain unavailable. Fewer than two accepted samples in a bar are rejected.

The current entry is the close of a completed sampled candle. The output separately includes current LTP, source timestamp, and its distance from the entry reference. The configured scanner calculates long-only candidate levels from closed bars; default reference stop distance is 1 ATR and target is 1.5 times the risk distance. They are analytical levels, not guaranteed fills or predicted prices.

With `--paper-capital 15000`, quantity is an advisory whole-share estimate bounded by the configured 1% risk-per-trade budget and 25% single-position notional cap. That is ₹150 planned risk and ₹3,750 maximum notional before fees under the current personal YAML settings. Quantity may be zero when one share exceeds either bound. It excludes brokerage, taxes, slippage, gaps, and execution risk. It does not place orders or change the ledger.

## AI boundary

The working pipeline is deterministic rule-based analysis, not an AI model. No model is configured in this project. If a model is added later, it should receive already validated market facts and deterministic signals to produce a plain-language explanation or flag missing/conflicting evidence. It must not invent an instrument mapping, quote, entry, stop, target, or quantity, and it must not call order APIs. Every recommendation should retain source time, data quality, strategy reason codes, and the rule-calculated levels.

## Options boundary

Options need a separate F&O data and validation path: current contract/expiry/strike/lot-size mapping, timestamped option LTP and liquidity/spread checks, option-premium candles, Greeks/volatility context, expiry and gap controls, lot-based premium affordability, and option-specific costs/risk. A stock-equity stop cannot be copied to a call or put premium. No such options recommendation or order path is implemented here.

SEBI's August 2026 study reports that 87.7% of individual traders lost money in equity derivatives in FY26 and that options accounted for 92% of aggregate individual losses. Those population results do not predict an individual outcome, but they support validating a cash-equity paper workflow first and avoiding claims that options are suitable for ₹15,000.

## Safety properties

- Groww feed calls subscribe/read/unsubscribe for market data only.
- Groww instrument identity must explicitly map each selected symbol to NSE/CASH/EQ.
- Quotes without valid `tsInMillis`, with stale data, or with invalid prices are rejected.
- Paper recommendations are written separately from the Phase 3D ledger.
- The runner refuses to start when `live_trading.enabled` is true.
- No order, position, portfolio, or broker execution method is called.
- A valid local preflight proves package/token/mapping presence only; it does not prove live-feed connectivity. The first market check must be run during an open NSE session.

## Official references

- [Groww Python SDK feed](https://groww.in/trade-api/docs/python-sdk/feed)
- [Groww instruments CSV schema](https://groww.in/trade-api/docs/python-sdk/instruments)
- [Groww API subscription and pricing](https://groww.in/trade-api)
- [SEBI FY25–FY26 profitability study](https://www.sebi.gov.in/sebi_data/attachdocs/aug-2026/1787233506209.pdf)
