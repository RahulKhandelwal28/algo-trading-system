/* Read-only React dashboard. Every request is GET-only. */
(() => {
  const h = React.createElement;
  const tabs = ["System", "Experiment", "Forward Paper Trading", "Overview", "Signals", "Positions", "Performance", "Strategies", "Risk & Costs", "Data Health", "Research", "Realtime Data", "Intraday Scanner", "Paper Execution", "Intraday Paper Session", "Phase 4G-YF"];
  const api = {
    Overview: "/api/dashboard/overview", Signals: "/api/dashboard/signals",
    Positions: "/api/dashboard/positions", Performance: "/api/dashboard/performance",
    Strategies: "/api/dashboard/strategies", "Data Health": "/api/dashboard/data-health",
    System: "/api/dashboard/system", Experiment: "/api/dashboard/experiment",
    "Risk & Costs": "/api/dashboard/risk", Research: "/api/dashboard/research",
    "Realtime Data": "/api/dashboard/realtime",
    "Intraday Scanner": "/api/dashboard/intraday-scanner",
    "Paper Execution": "/api/dashboard/phase4c-paper",
    "Intraday Paper Session": "/api/dashboard/phase4d-session",
    "Phase 4G-YF": "/api/dashboard/phase4g-yf",
  };
  const money = value => value == null ? "—" : `₹${Number(value).toLocaleString("en-IN", {maximumFractionDigits: 2})}`;
  const percent = value => value == null ? "—" : `${(Number(value) * 100).toFixed(2)}%`;

  function Metric(props) {
    return h("div", {className: "metric"}, h("span", null, props.label), h("strong", null, props.value ?? "—"));
  }
  function Table({columns, rows, empty = "No forward-paper records yet."}) {
    if (!rows || !rows.length) return h("p", {className: "empty"}, empty);
    return h("div", {className: "table-wrap"}, h("table", null,
      h("thead", null, h("tr", null, columns.map(col => h("th", {key: col.key}, col.label)))),
      h("tbody", null, rows.map((row, i) => h("tr", {key: row.order_id || row.trade_id || row.position_id || row.signal_id || `${row.date || row.trading_date || "row"}-${i}`},
        columns.map(col => h("td", {key: col.key}, col.render ? col.render(row[col.key], row) : (row[col.key] ?? "—"))))))));
  }
  function EquityCurve({points}) {
    if (!points || points.length < 2) return h("p", {className: "empty"}, "Equity curve appears after multiple daily reconciliations.");
    const values = points.map(p => Number(p.equity));
    const min = Math.min(...values), max = Math.max(...values), spread = max - min || 1;
    const coords = values.map((v, i) => `${(i / (values.length - 1)) * 700},${150 - ((v - min) / spread) * 130}`).join(" ");
    return h("svg", {viewBox: "0 0 700 160", role: "img", "aria-label": "Forward paper equity curve", className: "chart"},
      h("polyline", {points: coords, fill: "none", stroke: "#38bdf8", strokeWidth: "3", vectorEffect: "non-scaling-stroke"}));
  }
  function App() {
    const [tab, setTab] = React.useState("System");
    const [data, setData] = React.useState(null);
    const [serviceHealth, setServiceHealth] = React.useState(null);
    const [error, setError] = React.useState("");
    const [filters, setFilters] = React.useState({date: "", symbol: "", strategy: "", side: "", status: ""});
    const load = React.useCallback(async () => {
      try {
        if (tab === "Forward Paper Trading") {
          const endpoints = ["overview", "strategies", "activity", "data-health", "experiment"];
          const payloads = await Promise.all(endpoints.map(async name => {
            const response = await fetch(`/api/dashboard/${name}`, {method: "GET", cache: "no-store"});
            if (!response.ok) throw new Error(`Dashboard API returned ${response.status}`);
            return response.json();
          }));
          setData({overview: payloads[0], strategies: payloads[1], activity: payloads[2], health: payloads[3], experiment: payloads[4]});
          setError("");
          return;
        }
        if (tab === "Data Health") {
          const [healthResponse, sourcesResponse] = await Promise.all([
            fetch(api[tab], {method: "GET", cache: "no-store"}),
            fetch("/api/dashboard/data-sources", {method: "GET", cache: "no-store"}),
          ]);
          if (!healthResponse.ok || !sourcesResponse.ok) throw new Error(`Dashboard API returned ${healthResponse.status}/${sourcesResponse.status}`);
          setData({health: await healthResponse.json(), sources: await sourcesResponse.json()});
          setError("");
          return;
        }
        let url = api[tab];
        if (tab === "Signals") {
          const params = new URLSearchParams(Object.fromEntries(Object.entries(filters).filter(([, value]) => value)));
          if (params.toString()) url += `?${params}`;
        }
        const response = await fetch(url, {method: "GET", cache: "no-store"});
        if (!response.ok) throw new Error(`Dashboard API returned ${response.status}`);
        setData(await response.json()); setError("");
      } catch (e) { setError(String(e)); }
    }, [tab, filters]);
    React.useEffect(() => { load(); const timer = setInterval(load, 15000); return () => clearInterval(timer); }, [load]);
    React.useEffect(() => {
      let active = true;
      const refresh = async () => {
        try {
          const response = await fetch("/health", {method: "GET", cache: "no-store"});
          if (response.ok && active) setServiceHealth(await response.json());
        } catch (_) { if (active) setServiceHealth(null); }
      };
      refresh();
      const timer = setInterval(refresh, 15000);
      return () => { active = false; clearInterval(timer); };
    }, []);
    const field = (key, label, type = "text") => h("label", {className: "filter", key}, label,
      type === "select" ? h("select", {value: filters[key], onChange: e => setFilters({...filters, [key]: e.target.value})},
        [h("option", {value: "", key: "all"}, "All"), ...(key === "side" ? ["BUY", "SELL"] : ["accepted", "rejected"]).map(x => h("option", {key: x, value: x}, x))]
      ) : h("input", {type, value: filters[key], onChange: e => setFilters({...filters, [key]: e.target.value}), placeholder: label}));
    let content;
    if (tab === "System") {
      const d = data || {}, experiment = d.experiment || {};
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, "Local read-only application · PAPER / RESEARCH ONLY · no broker client · server start does not run the paper engine."),
        h("div", {className: "grid"},
          h(Metric, {label: "System status", value: d.system_status || "Loading"}),
          h(Metric, {label: "Mode", value: d.mode}),
          h(Metric, {label: "Paper mode", value: d.paper_mode_enabled ? "ENABLED" : "DISABLED"}),
          h(Metric, {label: "Live trading", value: d.live_trading_enabled ? "ENABLED" : "DISABLED"}),
          h(Metric, {label: "Broker", value: d.broker_status || "—"}),
          h(Metric, {label: "Equity provider", value: d.data_provider}),
          h(Metric, {label: "Dashboard access", value: d.dashboard_read_only ? "READ ONLY" : "—"}),
          h(Metric, {label: "Phase 3D", value: `${experiment.progress || "—"} sessions`})),
        h("h2", null, "Phase 3D forward experiment"),
        h("div", {className: "grid"},
          h(Metric, {label: "Experiment status", value: experiment.status}),
          h(Metric, {label: "Latest processed", value: experiment.last_processed_date}),
          h(Metric, {label: "Next required session", value: experiment.next_required_session}),
          h(Metric, {label: "Next session", value: experiment.next_session_status}),
          h(Metric, {label: "Experiment window", value: `${experiment.start_date || "—"} → ${experiment.end_date || "—"}`})),
        h("h2", null, "Current data blocker"),
        (experiment.block_reasons || []).length
          ? h("ul", {className: "detail-list"}, experiment.block_reasons.map((reason, i) => h("li", {key: i}, reason)))
          : h("p", {className: "empty"}, "No missing-file blocker is recorded; the normal paper preflight is still required before any separate run."),
        h("p", {className: "scope"}, `Ledger: ${d.ledger_path || "—"}. Refreshing this page reads state only.`));
    } else if (tab === "Experiment") {
      const d = data || {};
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, "Manifest-derived status. This view does not execute the runner or advance a session."),
        h("div", {className: "grid"},
          h(Metric, {label: "Progress", value: d.progress}), h(Metric, {label: "Status", value: d.status}),
          h(Metric, {label: "Latest processed", value: d.last_processed_date}),
          h(Metric, {label: "Next required", value: d.next_required_session}),
          h(Metric, {label: "Provider", value: d.provider}),
          h(Metric, {label: "Experiment window", value: `${d.start_date || "—"} → ${d.end_date || "—"}`})),
        h("h2", null, d.next_session_status || "Next-session status"),
        (d.block_reasons || []).length
          ? h("ul", {className: "detail-list"}, d.block_reasons.map((reason, i) => h("li", {key: i}, reason)))
          : h("p", {className: "empty"}, "No current missing-data reason is recorded."),
        h("h2", null, "Processed sessions"),
        h("p", null, (d.processed_session_dates || []).join(", ") || "No processed sessions recorded."),
        h("p", {className: "scope"}, `Manifest: ${d.manifest_path || "—"} · Broker contacted: ${d.broker_contacted ? "yes" : "no"} · Live trading: ${d.live_trading_enabled ? "enabled" : "disabled"}`));
    } else if (tab === "Forward Paper Trading") {
      const d = data || {}, account = d.overview || {}, health = d.health || {}, activity = d.activity || {};
      const experiment = d.experiment || {};
      const strategyRows = (d.strategies?.strategies || []).map(row => ({
        strategy: row.strategy, signals: row.signals, trades: row.closed_trades,
        starting_capital: row.starting_capital, equity: row.equity, cash: row.cash,
        win_rate: row.win_rate, net_pnl: row.net_pnl, return: row.return,
        profit_factor: row.profit_factor, expectancy: row.expectancy,
        max_drawdown: row.max_drawdown, open_positions: row.open_positions,
      }));
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, `FORWARD PAPER only · ${experiment.progress || "—"} sessions · next ${experiment.next_required_session || "—"}: ${experiment.next_session_status || "—"} · LIVE TRADING: DISABLED · no broker connection`),
        h("h2", null, "Account"),
        h("div", {className: "grid"},
          h(Metric, {label: "Starting capital", value: money(account.starting_capital)}),
          h(Metric, {label: "Current equity", value: money(account.current_equity)}),
          h(Metric, {label: "Cash", value: money(account.cash)}),
          h(Metric, {label: "Gross exposure", value: money(account.gross_exposure)}),
          h(Metric, {label: "Cumulative P&L", value: money(account.cumulative_pnl)}),
          h(Metric, {label: "Cumulative return", value: percent(account.cumulative_return)}),
          h(Metric, {label: "Maximum drawdown", value: percent(account.maximum_drawdown)})),
        h("h2", null, "Strategy accounts"),
        h(Table, {columns: [
          {key: "strategy", label: "Strategy"}, {key: "starting_capital", label: "Capital", render: money},
          {key: "equity", label: "Equity", render: money}, {key: "cash", label: "Cash", render: money},
          {key: "signals", label: "Signals"},
          {key: "trades", label: "Trades"}, {key: "win_rate", label: "Win rate", render: percent},
          {key: "net_pnl", label: "Net P&L", render: money}, {key: "return", label: "Return", render: percent},
          {key: "profit_factor", label: "Profit factor"}, {key: "expectancy", label: "Expectancy", render: money},
          {key: "max_drawdown", label: "Max DD", render: percent}, {key: "open_positions", label: "Open"}], rows: strategyRows}),
        h("h2", null, "Recent signals"), h(Table, {columns: [
          {key: "timestamp", label: "Time"}, {key: "symbol", label: "Symbol"},
          {key: "strategy", label: "Strategy"}, {key: "direction", label: "Side"},
          {key: "signal_price", label: "Signal price", render: money},
          {key: "status", label: "Status"}, {key: "rejection_reason", label: "Reason"}], rows: activity.signals || []}),
        h("h2", null, "Recent orders"), h(Table, {columns: [
          {key: "created_at", label: "Created"}, {key: "filled_at", label: "Filled"}, {key: "order_id", label: "Paper order"},
          {key: "symbol", label: "Symbol"}, {key: "side", label: "Side"},
          {key: "quantity", label: "Quantity"}, {key: "intended_price", label: "Intended", render: money},
          {key: "requested_price", label: "Requested", render: money}, {key: "simulated_fill_price", label: "Paper fill", render: money},
          {key: "stop_loss", label: "Stop", render: money}, {key: "target", label: "Target", render: money},
          {key: "costs", label: "Costs", render: money}, {key: "status", label: "Status"}], rows: activity.orders || []}),
        h("h2", null, "Recent closed trades"), h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"},
          {key: "entry_date", label: "Entry"}, {key: "exit_date", label: "Exit"},
          {key: "entry_price", label: "Entry price", render: money}, {key: "exit_price", label: "Exit price", render: money},
          {key: "realized_pnl", label: "Net P&L", render: money}, {key: "total_costs", label: "Costs", render: money},
          {key: "holding_sessions", label: "Sessions"}], rows: activity.closed_trades || []}),
        h("h2", null, "Recent risk events"), h(Table, {columns: [
          {key: "timestamp", label: "Time"}, {key: "symbol", label: "Symbol"},
          {key: "strategy", label: "Strategy"}, {key: "reason", label: "Reason"}], rows: activity.risk_events || []}),
        h("h2", null, "Data health"),
        h("div", {className: "grid"},
          h(Metric, {label: "Last successful run", value: account.last_successful_run}),
          h(Metric, {label: "Last market date", value: account.last_market_date}),
          h(Metric, {label: "Provider", value: health.provider}),
          h(Metric, {label: "Symbols loaded", value: health.symbols_received}),
          h(Metric, {label: "Missing symbols", value: (health.symbols_missing || []).length}),
          h(Metric, {label: "Data freshness", value: health.system_data_status}),
          h(Metric, {label: "Reconciliation", value: account.reconciliation_status || "UNKNOWN"})),
        h("p", {className: "empty"}, `Missing symbols: ${(health.symbols_missing || []).join(", ") || "none"}`));
    } else if (tab === "Overview") {
      const d = data || {};
      content = h(React.Fragment, null,
        h("div", {className: "grid"},
          h(Metric, {label: "Paper capital", value: money(d.starting_capital)}),
          h(Metric, {label: "Current equity", value: money(d.current_equity)}),
          h(Metric, {label: "Today's P&L", value: money(d.today_pnl)}),
          h(Metric, {label: "Cumulative P&L", value: money(d.cumulative_pnl)}),
          h(Metric, {label: "Drawdown", value: percent(d.drawdown)}),
          h(Metric, {label: "Open positions", value: d.open_positions?.length ?? 0}),
          h(Metric, {label: "Today's signals", value: d.today_signals?.length ?? 0}),
          h(Metric, {label: "Market regime", value: d.market_regime || "UNKNOWN"}),
          h(Metric, {label: "Data freshness", value: d.data_freshness || "UNKNOWN"}),
          h(Metric, {label: "System health", value: d.status || "UNKNOWN"})),
        h("h2", null, "Today's signals"), h(Table, {columns: [
          {key: "timestamp", label: "Time"}, {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"},
          {key: "direction", label: "Signal"}, {key: "signal_price", label: "Price", render: money},
          {key: "status", label: "Status"}, {key: "rejection_reason", label: "Reason"}], rows: d.today_signals || []}),
        h("p", {className: "scope"}, "FORWARD PAPER data only. Historical backtests are reported separately."));
    } else if (tab === "Signals") {
      content = h(React.Fragment, null,
        h("div", {className: "filters"}, field("date", "Date", "date"), field("strategy", "Strategy"), field("symbol", "Symbol"), field("side", "BUY/SELL", "select"), field("status", "Accepted/rejected", "select")),
        h(Table, {columns: [
          {key: "timestamp", label: "Time"}, {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"},
          {key: "direction", label: "Signal"}, {key: "signal_price", label: "Entry", render: money},
          {key: "stop_loss", label: "Stop", render: money}, {key: "target", label: "Target", render: money},
          {key: "risk_amount", label: "Risk", render: money}, {key: "status", label: "Status"}, {key: "rejection_reason", label: "Reason"}], rows: data?.signals || []}));
    } else if (tab === "Positions") {
      content = h(React.Fragment, null, h("h2", null, "Open positions"),
        h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"}, {key: "entry_price", label: "Entry", render: money},
          {key: "mark_price", label: "Current", render: money}, {key: "quantity", label: "Quantity"},
          {key: "stop_loss", label: "Stop", render: money}, {key: "target", label: "Target", render: money},
          {key: "unrealized_pnl", label: "Unrealized P&L", render: money}, {key: "holding_sessions", label: "Sessions"}], rows: data?.open || []}),
        h("h2", null, "Closed positions"), h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"}, {key: "entry_date", label: "Entry date"},
          {key: "exit_date", label: "Exit date"}, {key: "realized_pnl", label: "Realized P&L", render: money}], rows: data?.closed || []}));
    } else if (tab === "Performance") {
      const d = data || {}, m = d.metrics || {};
      content = h(React.Fragment, null, h("p", {className: "scope"}, "FORWARD PAPER — separate from BACKTEST results."),
        h("div", {className: "grid"}, h(Metric, {label: "Trades", value: m.number_of_trades ?? 0}),
          h(Metric, {label: "Win rate", value: percent(m.win_rate)}), h(Metric, {label: "Profit factor", value: m.profit_factor ?? "—"}),
          h(Metric, {label: "Expectancy", value: money(m.expectancy)}), h(Metric, {label: "Max drawdown", value: percent(m.maximum_drawdown)}),
          h(Metric, {label: "Average win", value: money(m.average_win)}), h(Metric, {label: "Average loss", value: money(m.average_loss)})),
        h("h2", null, "Cumulative equity"), h(EquityCurve, {points: d.equity_curve}),
        h("h2", null, "Daily P&L"), h(Table, {columns: [{key: "date", label: "Date"}, {key: "pnl", label: "P&L", render: money}], rows: d.daily_pnl || []}),
        h("h2", null, "Monthly P&L"), h(Table, {columns: [{key: "month", label: "Month"}, {key: "pnl", label: "P&L", render: money}], rows: d.monthly_pnl || []}));
    } else if (tab === "Strategies") {
      const configuredRows = Object.entries(data?.strategy_parameters || {}).map(([parameter, value]) => ({
        parameter, value: typeof value === "object" ? JSON.stringify(value) : String(value),
      }));
      content = h(React.Fragment, null, h("p", {className: "scope"}, "Forward-paper descriptive statistics; no strategy ranking."),
        h("h2", null, `Configured strategies (${(data?.configured_strategies || []).length})`),
        h("p", null, (data?.configured_strategies || []).join(", ") || "Configuration unavailable."),
        h("h2", null, "Current configured parameters"),
        h(Table, {columns: [{key: "parameter", label: "Parameter"}, {key: "value", label: "Configured value"}], rows: configuredRows}),
        h("h2", null, "Recorded forward strategy accounts"),
        h(Table, {columns: [
          {key: "strategy", label: "Strategy"}, {key: "signals", label: "Signals"},
          {key: "closed_trades", label: "Trades"}, {key: "win_rate", label: "Win rate", render: percent},
          {key: "net_pnl", label: "Net P&L", render: money}, {key: "return", label: "Return", render: percent},
          {key: "profit_factor", label: "Profit factor"}, {key: "expectancy", label: "Expectancy", render: money},
          {key: "max_drawdown", label: "Drawdown", render: percent}, {key: "open_positions", label: "Open"}], rows: data?.strategies || []}));
    } else if (tab === "Risk & Costs") {
      const d = data || {}, state = d.observed_state || {}, limits = d.configured_limits || {};
      const riskRows = Object.entries(limits).map(([limit, value]) => ({
        limit, value: typeof value === "boolean" ? (value ? "enabled" : "disabled") :
          (["risk_per_trade", "max_daily_loss", "max_portfolio_drawdown", "max_single_position_fraction", "max_gross_exposure_fraction", "max_sector_exposure_fraction"].includes(limit) ? percent(value) : value),
      }));
      const costRows = Object.entries(d.cost_configuration || {}).map(([name, value]) => ({name, value: typeof value === "number" ? String(value) : value}));
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, "Current config and ledger observations are displayed as stored. No risk limits are recalculated or changed here."),
        h("div", {className: "grid"}, h(Metric, {label: "Observed date", value: state.trading_date}),
          h(Metric, {label: "Equity", value: money(state.equity)}), h(Metric, {label: "Daily P&L", value: money(state.daily_pnl)}),
          h(Metric, {label: "Drawdown", value: percent(state.drawdown)}), h(Metric, {label: "Gross exposure", value: money(state.gross_exposure)}),
          h(Metric, {label: "Open positions", value: state.open_positions ?? "—"}),
          h(Metric, {label: "Risk events", value: d.risk_event_status || "—"})),
        h("h2", null, "Configured risk limits"),
        h(Table, {columns: [{key: "limit", label: "Limit"}, {key: "value", label: "Configured value"}], rows: riskRows}),
        h("h2", null, "Recorded order costs"),
        h(Table, {columns: [{key: "name", label: "Cost setting"}, {key: "value", label: "Configured value"}], rows: costRows}),
        h("h2", null, "Recorded risk events"),
        h(Table, {columns: [{key: "timestamp", label: "Time"}, {key: "market_date", label: "Date"}, {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"}, {key: "reason", label: "Reason"}], rows: d.risk_events || []}),
        h("h2", null, "Paper order status counts"),
        h(Table, {columns: [{key: "status", label: "Status"}, {key: "count", label: "Orders"}], rows: state.orders_by_status || []}));
    } else if (tab === "Research") {
      const d = data || {}, kaggle = d.phase3c || {}, validation = d.phase3_validation || {}, phase3d = d.phase3d || {};
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, "Historical research records and reports are read from existing result directories. This view does not rerun experiments or change results."),
        h("h2", null, "Phase 3C — Kaggle NIFTY 500"),
        h("div", {className: "grid"}, h(Metric, {label: "Status", value: kaggle.status}), h(Metric, {label: "Provider", value: kaggle.provider}),
          h(Metric, {label: "Symbols", value: kaggle.symbols}), h(Metric, {label: "Strategies", value: kaggle.strategies}),
          h(Metric, {label: "Walk-forward windows", value: kaggle.walk_forward_windows}),
          h(Metric, {label: "Monte Carlo", value: kaggle.monte_carlo?.status}),
          h(Metric, {label: "MC simulations run", value: kaggle.monte_carlo?.simulations_run})),
        h("p", null, kaggle.status_reason || "Phase 3C result manifest unavailable."),
        h("p", null, `Dataset: ${kaggle.dataset_name || "—"} · ${kaggle.date_range?.start || "—"} → ${kaggle.date_range?.end || "—"}`),
        h("p", null, `Monte Carlo statuses: ${JSON.stringify(kaggle.monte_carlo?.status_counts || {})}`),
        kaggle.report_href ? h("p", null, h("a", {href: kaggle.report_href, target: "_blank", rel: "noreferrer"}, "Open Phase 3C report")) : null,
        h("h2", null, "Phase 3 walk-forward validation"),
        h("div", {className: "grid"}, h(Metric, {label: "Run status", value: validation.status}),
          h(Metric, {label: "Walk-forward", value: validation.walk_forward_status}),
          h(Metric, {label: "Walk-forward windows", value: validation.walk_forward_windows}),
          h(Metric, {label: "Monte Carlo", value: validation.monte_carlo_status})),
        h("p", null, validation.walk_forward_reason || "Validation report unavailable."),
        h("p", null, validation.monte_carlo_reason || ""),
        validation.report_href ? h("p", null, h("a", {href: validation.report_href, target: "_blank", rel: "noreferrer"}, "Open walk-forward report")) : null,
        h("h2", null, "Phase 3D current recorded results"),
        h("div", {className: "grid"}, h(Metric, {label: "Status", value: phase3d.status}),
          h(Metric, {label: "Sessions", value: phase3d.sessions_processed}), h(Metric, {label: "Signals", value: phase3d.signals}),
          h(Metric, {label: "Rejected signals", value: phase3d.signals_rejected}), h(Metric, {label: "Orders", value: phase3d.orders_created}),
          h(Metric, {label: "Fills", value: phase3d.fills}), h(Metric, {label: "Trades", value: phase3d.trades}),
          h(Metric, {label: "Net P&L", value: money(phase3d.net_pnl)}), h(Metric, {label: "Costs", value: money(phase3d.total_costs)})),
        h("h2", null, "Available result reports"),
        h(Table, {columns: [{key: "title", label: "Report"}, {key: "run_id", label: "Run"}, {key: "path", label: "File"},
          {key: "href", label: "Open", render: href => h("a", {href, target: "_blank", rel: "noreferrer"}, "View")}], rows: d.reports || []}));
    } else if (tab === "Phase 4G-YF") {
      const d = data || {}, p = d.preflight || {};
      const m = d.monitor || {};
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, "Read-only session view · PAPER / RESEARCH ONLY · opening the dashboard never starts collection or paper execution."),
        h("h2", null, "PHASE 4G-YF — INTRADAY FORWARD RESEARCH"),
        h("div", {className: "grid"},
          h(Metric, {label: "Session", value: d.session_date || "2026-09-28"}),
          h(Metric, {label: "Status", value: d.status || "—"}),
          h(Metric, {label: "Execution state", value: d.execution_status || "—"}),
          h(Metric, {label: "Data", value: `${d.provider || "Yahoo Finance"} ${d.interval || "1m"}`}),
          h(Metric, {label: "Source classification", value: d.data_classification || "—"}),
          h(Metric, {label: "Universe", value: d.universe_expected ?? 486}),
          h(Metric, {label: "Symbols received / valid", value: `${d.symbols_received ?? 0} / ${d.valid_symbols ?? "—"}`}),
          h(Metric, {label: "Bars", value: d.bars ?? 0}),
          h(Metric, {label: "Data quality", value: d.data_quality || "NOT_ACQUIRED"}),
          h(Metric, {label: "Immutable-bar readiness", value: d.immutable_bar_readiness || "NOT_RUN"}),
          h(Metric, {label: "Session hours", value: `${d.session_open || "09:15"}–${d.session_close || "15:30"} ${d.timezone || "Asia/Kolkata"}`})),
        h("h2", null, "Immutable closed-bar reconciliation"),
        h("div", {className: "grid"},
          h(Metric, {label: "Readiness gate", value: d.immutable_bar_readiness || "NOT_RUN"}),
          h(Metric, {label: "Settling period", value: d.bar_settling_seconds == null ? "—" : `${d.bar_settling_seconds}s`}),
          h(Metric, {label: "Post-freeze revisions", value: d.revision_summary?.post_immutability_revisions ?? "—"}),
          h(Metric, {label: "Pre-freeze revisions", value: d.revision_summary?.pre_immutability_revisions ?? "—"}),
          h(Metric, {label: "Immutable bars", value: d.revision_summary?.immutable_bars ?? "—"}),
          h(Metric, {label: "Rejected bars", value: d.revision_summary?.rejected_bars ?? "—"})),
        (d.revision_summary?.blocking_reasons || []).length
          ? h("ul", {className: "detail-list"}, d.revision_summary.blocking_reasons.map((reason, i) => h("li", {key: i}, reason)))
          : null,
        h("h2", null, "Forward paper results"),
        h("div", {className: "grid"},
          h(Metric, {label: "Signals", value: d.signals == null ? "NOT RUN" : d.signals}),
          h(Metric, {label: "Paper orders", value: d.paper_orders == null ? "NOT RUN" : d.paper_orders}),
          h(Metric, {label: "Fills", value: d.fills == null ? "NOT RUN" : d.fills}),
          h(Metric, {label: "Open positions", value: d.open_positions == null ? "NOT RUN" : d.open_positions}),
          h(Metric, {label: "Closed trades", value: d.closed_trades == null ? "NOT RUN" : d.closed_trades}),
          h(Metric, {label: "Gross P&L", value: money(d.gross_pnl)}),
          h(Metric, {label: "Costs", value: money(d.costs)}),
          h(Metric, {label: "Net P&L", value: money(d.net_pnl)}),
          h(Metric, {label: "Max drawdown", value: percent(d.max_drawdown)}),
          h(Metric, {label: "Risk events", value: d.risk_events == null ? "NOT RUN" : d.risk_events})),
        h("p", {className: "scope"}, `Calendar: ${p.calendar_check || "—"} · Data acquired: ${d.data_acquired ? "YES" : "NO"} · Broker calls: ${d.broker_calls ?? 0} · Live orders: ${d.live_orders ?? 0} · ${d.paper_only ? "PAPER_ONLY=YES" : "PAPER STATUS UNKNOWN"}`),
        !d.data_acquired ? h("p", {className: "empty"}, "PREPARED only. Yahoo data has not been acquired; signals, orders, fills, and P&L have not been run.") : null,
        (d.block_reasons || []).length ? h("ul", {className: "detail-list"}, d.block_reasons.map((reason, i) => h("li", {key: i}, reason))) : null,
        h("h2", null, "Latest chronological top candidates"),
        h(Table, {columns: [
          {key: "decision_timestamp", label: "Decision time"}, {key: "rank", label: "Rank"},
          {key: "symbol", label: "Symbol"}, {key: "score", label: "Score"}],
          rows: d.top_candidates || [], empty: "No candidate ranking has been observed."}),
        h("h2", null, "Continuous PAPER MONITORING"),
        h("p", {className: "scope"}, "Read-only monitor display · Yahoo Finance is a third-party research source; NSE authorization is not claimed. The dashboard never starts the monitor."),
        h("h3", null, "DATA"),
        h("div", {className: "grid"},
          h(Metric, {label: "Monitor status", value: m.status || "NOT STARTED"}),
          h(Metric, {label: "Market", value: m.market_status || "—"}),
          h(Metric, {label: "Date", value: m.session_date || "2026-09-28"}),
          h(Metric, {label: "Last refresh", value: m.last_refresh || "—"}),
          h(Metric, {label: "Universe symbols", value: m.universe ?? 486}),
          h(Metric, {label: "Symbols with session data", value: m.symbols_with_valid_session_data ?? "—"}),
          h(Metric, {label: "Symbols with current data", value: m.symbols_with_current_response_data ?? 0}),
          h(Metric, {label: "Symbols with new bars", value: m.symbols_with_new_bars ?? 0}),
          h(Metric, {label: "Symbols with fetch failure", value: m.symbols_with_refresh_failure ?? 0}),
          h(Metric, {label: "Rows returned this refresh", value: m.current_rows_seen ?? "—"}),
          h(Metric, {label: "Unique market bars this refresh", value: m.unique_market_bars ?? "—"}),
          h(Metric, {label: "New market bars this refresh", value: m.new_market_bars ?? "—"}),
          h(Metric, {label: "Duplicate market bars", value: m.duplicate_market_bars ?? "—"}),
          h(Metric, {label: "Conflicting duplicates", value: m.duplicate_conflict ?? "—"}),
          h(Metric, {label: "Eligible bars", value: m.eligible_bars ?? "—"}),
          h(Metric, {label: "Immutable-bar readiness", value: m.immutable_bar_readiness || "NOT_RUN"}),
          h(Metric, {label: "Bar settling period", value: m.bar_settling_seconds == null ? "—" : `${m.bar_settling_seconds}s`}),
          h(Metric, {label: "Forming bars", value: m.forming_bars ?? "—"}),
          h(Metric, {label: "Settling bars", value: m.settling_bars ?? "—"}),
          h(Metric, {label: "Immutable closed bars", value: m.immutable_bars ?? "—"}),
          h(Metric, {label: "Conflicting bars", value: m.conflicting_bars ?? "—"}),
          h(Metric, {label: "Pre-freeze revisions", value: m.pre_immutability_revisions ?? "—"}),
          h(Metric, {label: "Post-freeze revisions", value: m.post_immutability_revisions ?? "—"}),
          h(Metric, {label: "Last market bar", value: m.last_market_bar || "—"}),
          h(Metric, {label: "Last response received", value: m.last_response_received || "—"}),
          h(Metric, {label: "Late bars excluded", value: m.late_bars_excluded ?? 0}),
          h(Metric, {label: "Warmup bars", value: m.warmup_bars ?? "—"}),
          h(Metric, {label: "Current-session new bars", value: m.current_session_new_bars ?? "—"})),
        h("h3", null, "SIGNALS"),
        h("div", {className: "grid"},
          h(Metric, {label: "WATCH", value: m.watch_signals ?? m.watchlist_candidates ?? 0}),
          h(Metric, {label: "BUY signals", value: m.buy_signals_today ?? 0}),
          h(Metric, {label: "REJECTED", value: m.rejected_signals ?? 0}),
          h(Metric, {label: "Open paper positions", value: m.open_paper_positions ?? 0}),
          h(Metric, {label: "Receipt-time protection", value: m.receipt_time_protection || "PENDING"})),
        (m.revision_gate_reasons || []).length
          ? h("ul", {className: "detail-list"}, m.revision_gate_reasons.map((reason, i) => h("li", {key: i}, reason)))
          : null,
        h("h3", null, "PAPER POSITIONS"),
        h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"},
          {key: "average_entry_price", label: "Entry", render: money},
          {key: "current_price", label: "Current", render: money},
          {key: "stop_price", label: "SL", render: money}, {key: "target_price", label: "Target", render: money},
          {key: "quantity", label: "Qty"}, {key: "unrealized_pnl", label: "Unrealized P&L", render: money}],
          rows: m.open_positions_detail || [], empty: "No open paper positions."}),
        h("h2", null, "PAPER BUY SIGNALS"),
        h(Table, {columns: [
          {key: "timestamp", label: "Signal time"}, {key: "decision_timestamp", label: "Decision time"},
          {key: "market_timestamp", label: "Market bar"}, {key: "received_timestamp", label: "Received at"},
          {key: "symbol", label: "Symbol"},
          {key: "strategy", label: "Strategy"}, {key: "entry", label: "Entry", render: money},
          {key: "stop_loss", label: "SL", render: money}, {key: "target", label: "Target", render: money},
          {key: "quantity", label: "Qty"},
          {key: "risk_reward", label: "R:R", render: value => value == null ? "—" : `1:${Number(value).toFixed(2)}`},
          {key: "status", label: "Paper status"}],
          rows: m.buy_signals || [], empty: "NO QUALIFIED BUY SIGNALS"}),
        h("h2", null, "Monitor watchlist"),
        h(Table, {columns: [
          {key: "rank", label: "Rank"}, {key: "symbol", label: "Symbol"},
          {key: "strategy", label: "Strategy"}, {key: "signal", label: "State"},
          {key: "score", label: "Score"}],
          rows: m.watchlist || [], empty: "No current watchlist candidates."}),
        h("h2", null, "Monitor data quality"),
        h("div", {className: "grid"},
          h(Metric, {label: "Source", value: m.data_provider || "Yahoo Finance"}),
          h(Metric, {label: "Classification", value: m.data_classification || "THIRD_PARTY_RESEARCH_SOURCE"}),
          h(Metric, {label: "NSE authorization", value: m.nse_authorization || "NOT_CLAIMED"}),
          h(Metric, {label: "Phase 4E data quality", value: m.data_quality || "NOT_ACQUIRED"}),
          h(Metric, {label: "Valid / invalid rows", value: `${m.valid_rows ?? 0} / ${m.invalid_rows ?? 0}`}),
          h(Metric, {label: "Duplicates / missing symbols", value: `${m.duplicate_rows ?? 0} / ${m.missing_symbols ?? 0}`})),
        h("p", {className: "scope"}, `Monitor artifacts: ${m.output_directory || "—"} · Paper ledger: ${m.ledger_path || "—"} · PAPER_ONLY=${m.paper_only ? "YES" : "UNKNOWN"} · BROKER_CALLS=${m.broker_calls ?? 0} · LIVE_ORDERS=${m.live_orders ?? 0}`),
        h("p", {className: "scope"}, `Session artifacts: ${d.output_directory || "—"} · Temporary ledger: ${d.temporary_ledger || "—"} · No Phase 3D ledger use.`));
    } else if (tab === "Intraday Scanner") {
      const d = data || {}, health = d.health || {};
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, d.reason || "Read-only intraday scanner status. This view never starts a provider or replay."),
        h("div", {className: "grid"},
          h(Metric, {label: "Data source", value: d.data_source || "—"}),
          h(Metric, {label: "Data classification", value: d.data_classification || "UNKNOWN"}),
          h(Metric, {label: "Scanner", value: d.scanner_status || "UNKNOWN"}),
          h(Metric, {label: "Universe", value: `${health.symbols_expected ?? d.universe_size ?? 0} · ${d.universe_status || "UNKNOWN"}`}),
          h(Metric, {label: "Universe file", value: d.universe_path || "—"}),
          h(Metric, {label: "Last bar", value: d.last_bar || "—"}),
          h(Metric, {label: "1m / 5m bars", value: `${d.bars_1m ?? 0} / ${d.bars_5m ?? 0}`}),
          h(Metric, {label: "Indicators ready", value: d.indicators_ready ?? 0}),
          h(Metric, {label: "Signals today", value: d.signals_today ?? 0})),
        h("h2", null, "Data health"),
        h("div", {className: "grid"},
          h(Metric, {label: "Symbols expected / with data", value: `${health.symbols_expected ?? 0} / ${health.symbols_with_data ?? 0}`}),
          h(Metric, {label: "Bars received / rejected", value: `${health.bars_received ?? 0} / ${health.bars_rejected ?? 0}`}),
          h(Metric, {label: "Missing intervals", value: health.missing_intervals ?? 0}),
          h(Metric, {label: "Duplicates", value: health.duplicate_bars ?? 0}),
          h(Metric, {label: "Out of order", value: health.out_of_order_bars ?? 0}),
          h(Metric, {label: "Timestamp errors", value: health.timestamp_errors ?? 0}),
          h(Metric, {label: "Volume unavailable", value: health.volume_unavailable ?? 0})),
        h("h2", null, "Intraday signals"),
        h(Table, {columns: [
          {key: "timestamp", label: "Time"}, {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"},
          {key: "timeframe", label: "Timeframe"}, {key: "side", label: "Direction"},
          {key: "reference_price", label: "Reference", render: money},
          {key: "stop_reference", label: "Stop reference", render: money},
          {key: "target_reference", label: "Target reference", render: money},
          {key: "reason_codes", label: "Reasons", render: value => Array.isArray(value) ? value.join(", ") : value}],
          rows: d.signals || [], empty: "No intraday signals. No genuine intraday data is currently available."}),
        h("p", {className: "scope"}, `Orders: ${d.orders ?? 0} · Scanner configured: ${d.configured_enabled ? "yes" : "no"} · Scanner started: ${d.scanner_started ? "yes" : "no"} · Live trading disabled.`));
    } else if (tab === "Paper Execution") {
      const d = data || {}, limits = d.risk_limits || {};
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, d.status === "OK"
          ? `Read-only Phase 4C paper ledger · ${d.ledger_path || ""} · no broker connection or live orders.`
          : `Paper execution state: ${d.status || "DATA_UNAVAILABLE"}. ${d.reason || "No paper ledger has been recorded."}`),
        h("div", {className: "grid"},
          h(Metric, {label: "Execution mode", value: d.execution_mode || "PAPER"}),
          h(Metric, {label: "Paper trading", value: d.paper_trading_enabled ? "ENABLED" : "DISABLED"}),
          h(Metric, {label: "Live trading", value: d.live_trading_enabled ? "ENABLED — BLOCKED" : "DISABLED"}),
          h(Metric, {label: "Initial capital", value: money(d.initial_capital)}),
          h(Metric, {label: "Current equity", value: money(d.current_equity)}),
          h(Metric, {label: "Available cash", value: money(d.available_cash)}),
          h(Metric, {label: "Used capital", value: money(d.used_capital)}),
          h(Metric, {label: "Gross exposure", value: money(d.gross_exposure)}),
          h(Metric, {label: "Open positions", value: `${d.open_positions ?? 0} / ${limits.max_positions ?? 5}`}),
          h(Metric, {label: "Today's P&L", value: money(d.today_pnl)}),
          h(Metric, {label: "Total P&L", value: money(d.total_pnl)}),
          h(Metric, {label: "Drawdown", value: percent(d.drawdown)}),
          h(Metric, {label: "Risk state", value: d.risk_state || d.status || "DATA_UNAVAILABLE"}),
          h(Metric, {label: "Risk status", value: d.risk_status || "DATA_UNAVAILABLE"}),
          h(Metric, {label: "Current daily loss", value: money(d.current_daily_loss)})),
        h("h2", null, "Risk limits"),
        h("div", {className: "grid"},
          h(Metric, {label: "Risk per trade", value: percent(limits.risk_per_trade)}),
          h(Metric, {label: "Daily loss limit", value: `${percent(limits.daily_loss_limit)} · ${money(limits.daily_loss_limit_amount)}`}),
          h(Metric, {label: "Portfolio drawdown limit", value: percent(limits.portfolio_drawdown_limit)}),
          h(Metric, {label: "Symbol exposure limit", value: percent(limits.max_symbol_exposure)}),
          h(Metric, {label: "Sector exposure limit", value: percent(limits.max_sector_exposure)}),
          h(Metric, {label: "Pending orders", value: d.pending_orders ?? 0})),
        h("h2", null, "Current symbol exposure"),
        h(Table, {columns: [{key: "name", label: "Symbol"}, {key: "exposure", label: "Exposure", render: money}],
          rows: Object.entries(d.symbol_exposures || {}).map(([name, exposure]) => ({name, exposure})),
          empty: "No current symbol exposure."}),
        h("h2", null, "Current sector exposure"),
        h(Table, {columns: [{key: "name", label: "Sector"}, {key: "exposure", label: "Exposure", render: money}],
          rows: Object.entries(d.sector_exposures || {}).map(([name, exposure]) => ({name, exposure})),
          empty: "No sector exposure has been recorded."}),
        Object.keys(d.halt_reasons || {}).length
          ? h("p", {className: "scope"}, `Halt details: ${Object.entries(d.halt_reasons).map(([code, detail]) => `${code}: ${detail}`).join(" · ")}`)
          : null,
        h("h2", null, "Orders"),
        h(Table, {columns: [
          {key: "timestamp", label: "Time"}, {key: "symbol", label: "Symbol"}, {key: "strategy", label: "Strategy"},
          {key: "side", label: "Side"}, {key: "quantity", label: "Quantity"},
          {key: "requested_price", label: "Requested", render: money}, {key: "filled_price", label: "Fill", render: money},
          {key: "status", label: "Status"}, {key: "rejection_reason", label: "Reason"}], rows: d.orders || [],
          empty: "No paper orders recorded."}),
        h("h2", null, "Positions"),
        h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "quantity", label: "Quantity"},
          {key: "average_entry_price", label: "Entry", render: money}, {key: "current_price", label: "Current", render: money},
          {key: "stop_price", label: "Stop", render: money}, {key: "target_price", label: "Target", render: money},
          {key: "unrealized_pnl", label: "Unrealized P&L", render: money}, {key: "strategy", label: "Strategy"},
          {key: "timeframe", label: "Timeframe"}], rows: d.positions || [], empty: "No open paper positions."}),
        h("h2", null, "Closed paper trades"),
        h(Table, {columns: [
          {key: "entry_time", label: "Entry"}, {key: "exit_time", label: "Exit"},
          {key: "symbol", label: "Symbol"}, {key: "quantity", label: "Quantity"},
          {key: "gross_pnl", label: "Gross P&L", render: money}, {key: "total_cost", label: "Costs", render: money},
          {key: "net_pnl", label: "Net P&L", render: money}, {key: "exit_reason", label: "Exit reason"}],
          rows: d.trades || [], empty: "No closed paper trades recorded."}),
        h("p", {className: "scope"}, "A software test or synthetic replay is not market performance. This view does not start execution or offer trading controls."));
    } else if (tab === "Intraday Paper Session") {
      const d = data || {}, s = d.session || {}, p = d.portfolio || {}, health = d.data_health || {}, checkpoint = d.checkpoint || {};
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, "Phase 4D replay/paper state · read only · no provider worker · no broker connection · live trading disabled."),
        h("h2", null, "Current session"),
        h("div", {className: "grid"},
          h(Metric, {label: "Session ID", value: s.session_id || "—"}),
          h(Metric, {label: "Date", value: s.trading_date || "—"}),
          h(Metric, {label: "Status", value: s.status || d.status}),
          h(Metric, {label: "Mode", value: s.mode || "PRECHECK"}),
          h(Metric, {label: "Data source", value: Array.isArray(s.data_source) ? s.data_source.join(", ") : (s.data_source || "—")}),
          h(Metric, {label: "Universe", value: s.universe_size || health.expected_symbols || "—"}),
          h(Metric, {label: "Last event", value: s.last_event_at || checkpoint.last_processed_timestamp || "—"}),
          h(Metric, {label: "Last bar", value: s.last_bar_at || checkpoint.last_processed_timestamp || "—"}),
          h(Metric, {label: "Signals", value: s.signal_count ?? "—"}),
          h(Metric, {label: "Orders", value: s.order_count ?? "—"}),
          h(Metric, {label: "Fills", value: s.fill_count ?? "—"}),
          h(Metric, {label: "Closed trades", value: s.closed_trade_count ?? "—"})),
        h("h2", null, "Paper portfolio"),
        h("div", {className: "grid"},
          h(Metric, {label: "Equity", value: money(p.equity)}), h(Metric, {label: "Cash", value: money(p.cash)}),
          h(Metric, {label: "Exposure", value: money(p.exposure)}), h(Metric, {label: "Open positions", value: p.open_positions ?? 0}),
          h(Metric, {label: "Daily P&L", value: money(p.daily_pnl)}), h(Metric, {label: "Total P&L", value: money(p.total_pnl)}),
          h(Metric, {label: "Drawdown", value: percent(p.drawdown)}), h(Metric, {label: "Risk state", value: p.risk_state || "—"})),
        h("h2", null, "Data health"),
        h("div", {className: "grid"},
          h(Metric, {label: "Dataset", value: d.dataset_available ? "AVAILABLE" : "UNAVAILABLE"}),
          h(Metric, {label: "Expected symbols", value: health.expected_symbols ?? "—"}),
          h(Metric, {label: "Symbols with data", value: health.symbols_with_data ?? "—"}),
          h(Metric, {label: "Coverage", value: percent(health.coverage_percent)}),
          h(Metric, {label: "Bars", value: health.bars_received ?? "—"}),
          h(Metric, {label: "Missing intervals", value: health.missing_intervals ?? "—"}),
          h(Metric, {label: "Duplicates", value: health.duplicate_intervals ?? "—"}),
          h(Metric, {label: "Rejected bars", value: health.rejected_rows ?? "—"})),
        h("p", {className: "scope"}, `Dataset: ${d.data_path || "—"} · Ledger events: ${d.ledger_event_count ?? 0} · Refresh reads state only.`));
    } else if (tab === "Realtime Data") {
      const d = data || {}, quotes = d.quotes || [], candles = d.candles || [];
      const provider = d.provider_details || {};
      const discovery = d.source_discovery || {};
      const universeSize = Number(d.universe_size ?? provider.universe_size ?? 100);
      const connected = provider.connection_status || (d.status_code === "REALTIME_PROVIDER_NOT_CONFIGURED"
        ? "NOT_CONFIGURED" : (d.feed_status || "DISCONNECTED"));
      const rawMarketStatus = provider.market_segment_status;
      const marketStatus = ["NORMAL_OPEN", "PRE_OPEN_END", "CLOSING_START", "CLOSING_END", "CAS_LM_START", "CAS_M_STOP", "CAS_STOP"].includes(rawMarketStatus)
        ? (rawMarketStatus === "NORMAL_OPEN" ? "OPEN" : (["PRE_OPEN_END", "PRE_OPEN_START"].includes(rawMarketStatus) ? "PREOPEN" : "CLOSED"))
        : (d.market_status || "DATA_UNAVAILABLE");
      const latency = (key) => provider[key] == null ? "—" : `${Number(provider[key]).toFixed(1)} ms`;
      const candleRows = candles.map(candle => ({
        symbol: candle.symbol, interval: candle.interval,
        window: `${candle.start || "—"} → ${candle.end || "—"}`,
        open: candle.open, high: candle.high, low: candle.low, close: candle.close,
        volume: candle.volume, status: candle.status, ticks: candle.tick_count,
      }));
      content = h(React.Fragment, null,
        h("p", {className: "scope"}, d.status_label || "REALTIME FEED: NOT CONFIGURED"),
        h("p", {className: "scope"}, d.reason || "Only validated market data is shown. This section does not generate signals or orders."),
        h("h2", null, "Free no-broker source discovery"),
        h("div", {className: "grid"},
          h(Metric, {label: "Provider", value: discovery.provider || "No candidate recorded"}),
          h(Metric, {label: "Source classification", value: discovery.source_classification || "UNKNOWN"}),
          h(Metric, {label: "Access status", value: discovery.access_status || "UNKNOWN"}),
          h(Metric, {label: "Universe capability", value: discovery.universe_capability || "UNKNOWN"}),
          h(Metric, {label: "Timestamp status", value: discovery.timestamp_status || "UNKNOWN"}),
          h(Metric, {label: "Intraday capability", value: discovery.intraday_capability || "UNKNOWN"}),
          h(Metric, {label: "Last validation", value: discovery.last_validation || "UNKNOWN"})),
        h("div", {className: "grid"},
          h(Metric, {label: "Feed status", value: d.feed_status || "DISCONNECTED"}),
          h(Metric, {label: "Provider", value: d.provider === "upstox" ? "Upstox V3" : (d.provider || "—")}),
          h(Metric, {label: "Mode", value: provider.mode || d.mode || "LTPC"}),
          h(Metric, {label: "Universe", value: universeSize}),
          h(Metric, {label: "Connection", value: connected}),
          h(Metric, {label: "Market status", value: marketStatus}),
          h(Metric, {label: "NSE_EQ feed status", value: rawMarketStatus || "UNKNOWN"}),
          h(Metric, {label: "Last successful update", value: d.last_successful_update || "—"}),
          h(Metric, {label: "Last source quote time", value: d.last_quote_timestamp || "—"}),
          h(Metric, {label: "Last tick", value: provider.last_tick_timestamp || d.last_quote_timestamp || "—"}),
          h(Metric, {label: "Latency p50 / p95", value: `${latency("latency_p50_ms")} / ${latency("latency_p95_ms")}`}),
          h(Metric, {label: "Mapped", value: `${provider.mapped_symbols ?? 0} / ${universeSize}`}),
          h(Metric, {label: "Subscribed", value: `${provider.subscribed_symbols ?? 0} / ${universeSize}`}),
          h(Metric, {label: "Receiving", value: `${provider.receiving_symbols ?? (d.accepted_symbols || []).length} / ${universeSize}`}),
          h(Metric, {label: "Requested symbols", value: d.requested_symbols ?? 0}),
          h(Metric, {label: "Receiving symbols", value: (d.accepted_symbols || []).length}),
          h(Metric, {label: "Missing symbols", value: (d.missing_symbols || []).length}),
          h(Metric, {label: "Stale symbols", value: (d.stale_symbols || []).length}),
          h(Metric, {label: "Received / accepted", value: `${d.received_quote_count ?? 0} / ${d.accepted_quote_count ?? 0}`}),
          h(Metric, {label: "Rejected / stale quotes", value: `${d.rejected_quote_count ?? 0} / ${d.stale_quote_count ?? 0}`}),
          h(Metric, {label: "Rejected", value: provider.invalid_count ?? d.rejected_quote_count ?? 0}),
          h(Metric, {label: "Stale", value: provider.stale_count ?? d.stale_quote_count ?? 0}),
          h(Metric, {label: "Duplicates", value: provider.duplicate_count ?? 0}),
          h(Metric, {label: "Volume anomalies", value: provider.volume_anomaly_count ?? 0}),
          h(Metric, {label: "Quotes", value: provider.quote_count ?? d.accepted_quote_count ?? 0}),
          h(Metric, {label: "1m / 5m candles", value: `${candles.filter(c => c.interval === "1m").length} / ${candles.filter(c => c.interval === "5m").length}`}),
          h(Metric, {label: "Reconnects / errors", value: `${provider.reconnect_count ?? d.reconnect_count ?? 0} / ${d.error_count ?? 0}`})),
        h("p", {className: "scope"}, `Market session: ${d.market_status_reason || "—"} · Candle intervals: ${(d.candle_intervals || []).join(", ") || "—"}`),
        h("h2", null, "Universe coverage"),
        h("p", null, `Requested: ${(d.requested_symbol_list || []).join(", ") || "none"}`),
        h("p", null, `Received: ${(d.received_symbols || []).join(", ") || "none"}`),
        h("p", null, `Missing: ${(d.missing_symbols || []).join(", ") || "none"}`),
        h("p", null, `Invalid: ${(d.invalid_symbols || []).join(", ") || "none"}`),
        h("p", null, `Stale: ${(d.stale_symbols || []).join(", ") || "none"}`),
        h("h2", null, "Latest validated quotes"),
        h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "exchange", label: "Exchange"}, {key: "series", label: "Series"},
          {key: "instrument_key", label: "Instrument key"},
          {key: "last_price", label: "Last price", render: money}, {key: "source_timestamp", label: "Source time"},
          {key: "source_timestamp_raw", label: "Raw LTT"}, {key: "source_timestamp_unit", label: "LTT unit"},
          {key: "source_timestamp_origin", label: "Timestamp source"},
          {key: "received_at", label: "Received at"}, {key: "quote_age_seconds", label: "Age (s)"},
          {key: "last_trade_quantity", label: "Last trade qty"}, {key: "latency_ms", label: "Latency (ms)"},
          {key: "freshness_status", label: "Freshness"}, {key: "source", label: "Source"}],
          rows: quotes, empty: "No validated live quotes received. Values are not simulated."}),
        h("h2", null, "Recent quote rejections"),
        h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "code", label: "Failure code"},
          {key: "reason", label: "Reason"}, {key: "quote_age_seconds", label: "Age (s)"}],
          rows: d.recent_rejections || [], empty: "No rejected quotes in the current feed snapshot."}),
        h("h2", null, "Latest intraday candles"),
        h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "interval", label: "Interval"}, {key: "window", label: "Window"},
          {key: "open", label: "Open", render: money}, {key: "high", label: "High", render: money},
          {key: "low", label: "Low", render: money}, {key: "close", label: "Close", render: money},
          {key: "volume", label: "Volume"}, {key: "status", label: "FORMING / CLOSED"}, {key: "ticks", label: "Quotes"}],
          rows: candleRows, empty: "No intraday candles. The engine creates bars only from validated quotes during an NSE session."}),
        h("p", {className: "scope"}, "Safety: data-only · no strategy evaluation · no signals · no orders · paper ledger unchanged · live trading disabled."));
    } else {
      const d = data?.health || {}, sources = data?.sources || {};
      const indexRows = Object.entries(sources.indexes || {}).map(([index, row]) => ({
        index, source: row.source, first_date: row.first_date, last_date: row.last_date,
        rows: row.rows, duplicates: row.duplicate_dates, invalid_dates: row.invalid_date_rows,
        next_session: row.next_required_session || "—",
        required_session_present: row.required_session_present == null ? "—" : (row.required_session_present ? "PRESENT" : "MISSING"),
      }));
      const equity = sources.equity || {}, universe = sources.frozen_universe || {};
      content = h(React.Fragment, null, h("div", {className: "grid"},
        h(Metric, {label: "Provider", value: d.provider}), h(Metric, {label: "Last update", value: d.last_update}),
        h(Metric, {label: "Market date", value: d.market_date}), h(Metric, {label: "Symbols received", value: d.symbols_received}),
        h(Metric, {label: "Symbols scanned", value: d.symbols_scanned}), h(Metric, {label: "Invalid records", value: d.invalid_records}),
        h(Metric, {label: "Duplicate records", value: d.duplicate_records}), h(Metric, {label: "Abnormal records", value: d.abnormal_records}),
        h(Metric, {label: "Status", value: d.system_data_status})),
        h("h2", null, "Historical equity coverage and source"),
        h("div", {className: "grid"}, h(Metric, {label: "Configured provider", value: equity.configured_provider}),
          h(Metric, {label: "Recorded history source", value: equity.historical_source}),
          h(Metric, {label: "Equity first date", value: equity.first_date}), h(Metric, {label: "Equity last date", value: equity.last_date}),
          h(Metric, {label: "Eligible symbols", value: equity.eligible_symbols}),
          h(Metric, {label: `Equity file for ${sources.next_required_session || "next session"}`,
            value: equity.next_date_file_present == null ? "—" : (equity.next_date_file_present ? "PRESENT" : "MISSING")})),
        h("p", {className: "scope"}, `Adjustment record: ${equity.adjustment_status || "not supplied"}`),
        h("h2", null, "Official NSE index coverage"),
        h(Table, {columns: [{key: "index", label: "Index"}, {key: "source", label: "Source"}, {key: "first_date", label: "First date"},
          {key: "last_date", label: "Last date"}, {key: "rows", label: "Rows"}, {key: "duplicates", label: "Duplicates"},
          {key: "invalid_dates", label: "Invalid dates"}, {key: "next_session", label: "Required session"},
          {key: "required_session_present", label: "Exact row"}], rows: indexRows}),
        h("p", {className: "scope"}, `NIFTY source: ${sources.indexes?.NIFTY?.source_url || "—"} · India VIX source: ${sources.indexes?.INDIA_VIX?.source_url || "—"}`),
        h("h2", null, "Frozen Phase 3D universe"),
        h("div", {className: "grid"}, h(Metric, {label: "Status", value: universe.status}), h(Metric, {label: "Symbols", value: universe.symbols}),
          h(Metric, {label: "Explicit EQ", value: universe.eq_symbols}), h(Metric, {label: "Non-EQ", value: universe.non_eq_rows}),
          h(Metric, {label: "Missing series", value: universe.missing_series_rows}), h(Metric, {label: "Duplicate symbols", value: universe.duplicate_symbols})),
        h("p", {className: "scope"}, `Coverage policy: ${sources.data_gaps_policy || "—"}`),
        h("h2", null, "Missing symbols"), h("p", null, (d.missing_symbols || []).join(", ") || "None"),
        h("h2", null, "Stale symbols"), h("p", null, (d.stale_symbols || []).join(", ") || "None"),
        h("h2", null, "Invalid symbols"), h("p", null, (d.invalid_symbols || []).join(", ") || "None"),
        h("h2", null, "Per-symbol checks"), h(Table, {columns: [
          {key: "symbol", label: "Symbol"}, {key: "provider", label: "Provider"}, {key: "market_date", label: "Market date"},
          {key: "data_timestamp", label: "Data timestamp"}, {key: "freshness_status", label: "Freshness"},
          {key: "valid", label: "Valid"}, {key: "reason", label: "Reason"}], rows: Object.entries(d.checks || {}).map(([symbol, check]) => ({symbol, ...check}))}));
    }
    const currentExperiment = serviceHealth?.experiment || {};
    return h("div", {className: "app"},
      h("header", null, h("div", null, h("p", {className: "eyebrow"}, "NSE EQUITY RESEARCH"), h("h1", null, "Research & paper system")),
        h("span", {className: "live-badge"}, serviceHealth?.live_trading_enabled === false ? "LIVE TRADING: DISABLED" : "LIVE STATUS: UNKNOWN")),
      h("section", {className: "status-strip", "aria-label": "Current Phase 3D status"},
        h("strong", null, serviceHealth?.system_status || "Reading local status…"),
        h("span", null, `Realtime: ${serviceHealth?.realtime_feed_status || "UNKNOWN"}`),
        h("span", null, `Phase 3D: ${currentExperiment.progress || "—"}`),
        h("span", null, `Latest: ${currentExperiment.last_processed_date || "—"}`),
        h("span", null, `Next: ${currentExperiment.next_required_session || "—"}`),
        h("span", null, `Broker: ${serviceHealth?.broker_connected === false ? "NOT CONNECTED" : "UNKNOWN"}`)),
      h("nav", null, tabs.map(name => h("button", {key: name, className: tab === name ? "active" : "", onClick: () => setTab(name)}, name))),
      h("main", null, h("h2", {className: "page-title"}, tab), error ? h("p", {className: "error"}, error) : null,
        (data?.status === "NO_FORWARD_DATA" || data?.overview?.status === "NO_FORWARD_DATA") ? h("p", {className: "empty"}, "No forward-paper run exists yet. Configure local EOD data and run the daily workflow.") : null,
        content), h("footer", null, "Read-only local dashboard · refreshed every 15 seconds · no broker connection"));
  }
  ReactDOM.createRoot(document.getElementById("root")).render(h(App));
})();
