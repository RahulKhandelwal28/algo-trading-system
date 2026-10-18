# Reference repository review

Reviewed on 2026-09-25. No source code was copied into this project.

Repository sources: [KoshIndia/kite-algo-bot](https://github.com/KoshIndia/kite-algo-bot), [hardikjh/swing_trade](https://github.com/hardikjh/swing_trade), and [zerodha/pykiteconnect](https://github.com/zerodha/pykiteconnect).

| Reference | Architecture and dependencies | License | Relevant limits and use here |
|---|---|---|---|
| KoshIndia/kite-algo-bot | Modular config/data/strategy/risk/engine/backtest/broker/reporting layout; pandas, NumPy, PyYAML, dotenv, optional Kite SDK, FastAPI, pytest. Shared decision pipeline and risk gates are useful patterns. | MIT, as identified by the repository. | One commit and broad feature claims warrant independent verification. Its quick start includes synthetic data; synthetic runs must never be presented as market results. We reuse only the architectural ideas, not code. |
| hardikjh/swing_trade | Nifty 500 screen, cached yfinance prices, indicator-driven signals and portfolio/backtest scripts. | No LICENSE file was listed in the repository file view; code reuse is not authorized by a license we could verify. | Its own LIMITATIONS.md calls out absent delivery data, yfinance quality gaps, no transaction costs, a single concurrent position, and survivorship bias. We use these as design warnings only, not code. |
| zerodha/pykiteconnect | Official client for Kite REST APIs and WebSocket streaming; separate Python package with examples and tests. | MIT. | It is a broker/API client, not a backtesting engine or data license. It is not vendored. The current project is paper-only; any future adapter must be optional and must not bypass the live kill switch. |

The project uses a separate provider interface and its own implementations. Any performance result requires real input data and records its source. A current Nifty 500 constituent list is not point-in-time membership and therefore cannot eliminate survivorship bias in historical tests.

The [swing_trade limitations file](https://github.com/hardikjh/swing_trade/blob/main/LIMITATIONS.md) specifically notes missing delivery-percent data, possible yfinance data errors, no modeled transaction costs, a single open position assumption, and survivorship bias. The [Nifty 500 page](https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-500) describes the index's current constituent universe; its current constituents alone do not reconstruct historical membership.
