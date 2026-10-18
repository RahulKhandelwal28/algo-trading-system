"""CSV, chart, manifest, and HTML report outputs."""

from __future__ import annotations

import html
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def write_json(path: str | Path, payload: Any) -> None:
    Path(path).write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True), encoding="utf-8")


def write_config_snapshot(path: str | Path, config: dict[str, Any]) -> None:
    Path(path).write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def make_comparison(metrics: list[dict[str, Any]], reference_capital: float = 100000) -> tuple[pd.DataFrame, str | None]:
    frame = pd.DataFrame(metrics)
    if frame.empty:
        return frame, None
    frame = frame.loc[frame["initial_capital"].astype(float) == float(reference_capital)].copy()
    columns = [
        "strategy", "trades", "winning_trades", "losing_trades", "win_rate_pct",
        "average_win", "average_loss", "payoff_ratio", "profit_factor",
        "expectancy_per_trade", "gross_pnl", "net_pnl", "net_return_pct",
        "maximum_drawdown_pct", "sharpe", "sortino", "trading_costs", "turnover",
    ]
    visible = [column for column in columns if column in frame]
    return frame[visible].sort_values("strategy").reset_index(drop=True), None


def write_charts(
    chart_dir: str | Path,
    equity_rows: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
    capital_growth_rows: list[dict[str, Any]],
) -> list[str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    path = Path(chart_dir)
    path.mkdir(parents=True, exist_ok=True)
    created: list[str] = []
    equity = pd.DataFrame(equity_rows)
    if not equity.empty:
        fig, ax = plt.subplots(figsize=(11, 5))
        for strategy, group in equity.groupby("strategy"):
            group = group.sort_values("timestamp")
            ax.plot(pd.to_datetime(group["timestamp"]), group["equity"], label=strategy)
        ax.set_title("Equity curves (₹100,000 initial capital)")
        ax.set_ylabel("Equity (₹)")
        ax.legend(fontsize=7, ncol=2)
        fig.tight_layout()
        fig.savefig(path / "equity_curve.png", dpi=150)
        plt.close(fig)
        created.append("equity_curve.png")

        fig, ax = plt.subplots(figsize=(11, 4))
        for strategy, group in equity.groupby("strategy"):
            group = group.sort_values("timestamp")
            ax.plot(pd.to_datetime(group["timestamp"]), group["drawdown"], label=strategy)
        ax.set_title("Marked equity drawdown")
        ax.set_ylabel("Drawdown (fraction)")
        ax.legend(fontsize=7, ncol=2)
        fig.tight_layout()
        fig.savefig(path / "drawdown_curve.png", dpi=150)
        plt.close(fig)
        created.append("drawdown_curve.png")

    comparisons = pd.DataFrame([row for row in metric_rows if row.get("initial_capital") == 100000])
    if not comparisons.empty:
        fig, ax = plt.subplots(figsize=(11, 5))
        ax.bar(comparisons["strategy"], comparisons["net_return_pct"], color="#507dbc")
        ax.set_title("Strategy net return after modeled costs")
        ax.set_ylabel("Net return (%)")
        ax.tick_params(axis="x", labelrotation=35)
        fig.tight_layout()
        fig.savefig(path / "strategy_comparison.png", dpi=150)
        plt.close(fig)
        created.append("strategy_comparison.png")

    growth = pd.DataFrame(capital_growth_rows)
    if not growth.empty:
        fig, ax = plt.subplots(figsize=(8, 5))
        for strategy, group in growth.groupby("strategy"):
            group = group.sort_values("capital")
            ax.plot(pd.to_numeric(group["capital"]), group["final_capital"], marker="o", label=strategy)
        ax.set_title("Capital growth by strategy and initial capital")
        ax.set_xlabel("Initial capital (₹)")
        ax.set_ylabel("Final capital (₹)")
        ax.legend(fontsize=7, ncol=2)
        fig.tight_layout()
        fig.savefig(path / "capital_growth.png", dpi=150)
        plt.close(fig)
        created.append("capital_growth.png")
    return created


def write_html_report(
    path: str | Path,
    period: dict[str, Any],
    comparison: pd.DataFrame,
    metrics: list[dict[str, Any]],
    best_strategy: str | None,
    monte_carlo: dict[str, Any],
    walk_forward: dict[str, Any],
    warnings: list[str],
    data_status: str = "AVAILABLE",
    details: dict[str, Any] | None = None,
) -> None:
    comparison_html = comparison.to_html(index=False, border=0, classes="metrics") if not comparison.empty else "<p>No strategy metrics available.</p>"
    warning_html = "".join(f"<li>{html.escape(item)}</li>" for item in warnings) or "<li>None recorded.</li>"
    metric_rows = []
    for metric in metrics:
        reference_capital = float(period.get("reference_capital", 100000))
        if metric.get("initial_capital") != reference_capital:
            continue
        pf = metric.get("profit_factor")
        pf_text = "∞" if isinstance(pf, (int, float)) and math.isinf(pf) else f"{float(pf or 0):.2f}"
        metric_rows.append(
            "<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in [
                metric.get("strategy", ""), metric.get("trades", 0),
                "—" if metric.get("win_rate_pct") is None else f"{metric['win_rate_pct']:.1f}%",
                pf_text, f"₹{metric.get('expectancy_per_trade', 0):.2f}",
                f"{metric.get('net_return_pct', 0):.2f}%", f"{metric.get('maximum_drawdown_pct', 0):.2f}%",
                f"₹{metric.get('trading_costs', 0):.2f}",
            ]) + "</tr>"
        )
    reference_capital = float(period.get("reference_capital", 100000))
    reference = [metric for metric in metrics if metric.get("initial_capital") == reference_capital]
    observed_90 = [metric for metric in reference if metric.get("win_rate_90pct_plus")]
    high_win_text = "None reached 90% observed net win rate." if not observed_90 else "Observed 90%+ win rate: " + ", ".join(
        f"{metric['strategy']} ({metric.get('win_rate_pct', 0):.1f}%, {metric.get('trades', 0)} trades; " +
        ("at least 30 trades" if metric.get("win_rate_sample_sufficient") else "LOW SAMPLE") + ")" for metric in observed_90
    )
    milestone_rows = []
    for metric in reference:
        multiple = float(metric.get("capital_multiple", 0))
        statuses = [f"{threshold:.2f}x: {'ACHIEVED' if multiple >= threshold else 'NOT ACHIEVED'}" for threshold in [1.25, 1.5, 1.75, 2.0]]
        milestone_rows.append(f"<li>{html.escape(str(metric['strategy']))}: {html.escape(', '.join(statuses))}</li>")
    milestone_html = "<ul>" + "".join(milestone_rows) + "</ul>" if milestone_rows else "<p>No capital scenarios available.</p>"
    body = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>NSE Equity Backtest</title>
<style>body{{font:15px system-ui,sans-serif;max-width:1200px;margin:36px auto;padding:0 20px;color:#202b38}}h1{{color:#153d63}}table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{padding:8px;border-bottom:1px solid #dce2e8;text-align:right}}th:first-child,td:first-child{{text-align:left}}th{{background:#edf3f8}}.warn{{background:#fff5df;padding:12px 18px}}img{{max-width:100%}}code{{word-break:break-all}}</style></head><body>
<h1>NSE equity research backtest</h1><p>Data status: <b>{html.escape(data_status)}</b></p>
<p>Period: {html.escape(str(period.get('start', 'unavailable')))} to {html.escape(str(period.get('end', 'unavailable')))}; completed sessions: {html.escape(str(period.get('sessions', 'unavailable')))}; eligible symbols: {html.escape(str(period.get('symbols', 'unavailable')))}; provider: {html.escape(str(period.get('provider', 'unavailable')))}.</p>
<p>Reference capital: ₹{reference_capital:,.0f}. Signals use the prior completed candle and execute at the next candle open. Portfolio sizing uses cash and mark-to-market holdings; the bar high, low, and close are not used before the open fill. Stop is assumed first if both stop and target trade in the same candle. The drawdown tripwire halts new entries and liquidates at the observed open or close when breached. Gaps can push realized drawdown beyond the configured cap. Results are research diagnostics and do not imply future performance.</p>
<h2>Strategy comparison</h2>{comparison_html}
<h2>Full metrics by strategy and capital</h2>{pd.DataFrame((details or {}).get('all_scenario_metrics', [])).to_html(index=False, border=0, classes='metrics') if (details or {}).get('all_scenario_metrics') else '<p>No scenario metrics available.</p>'}
<h2>90% win-rate investigation</h2><p>{html.escape(high_win_text)} Expectancy = win rate × average win − loss rate × absolute average loss. Payoff ratio = average win ÷ absolute average loss; profit factor = total gross winning P&amp;L ÷ absolute total gross losing P&amp;L. A high win rate can still lose money when losses are sufficiently larger than wins. All results are labeled with trade counts; two months cannot establish durable performance.</p>
<h2>Observed capital milestones at ₹{reference_capital:,.0f}</h2>{milestone_html}
<h2>Metrics at ₹{reference_capital:,.0f}</h2><table><thead><tr><th>Strategy</th><th>Trades</th><th>Win rate</th><th>Profit factor</th><th>Expectancy/trade</th><th>Net return</th><th>Max drawdown</th><th>Costs</th></tr></thead><tbody>{''.join(metric_rows)}</tbody></table>
<h2>Exit-mode comparison</h2>{pd.DataFrame((details or {}).get('exit_comparison', [])).to_html(index=False, border=0, classes='metrics') if (details or {}).get('exit_comparison') else '<p>No exit comparison results.</p>'}
<h2>Trend target sensitivity</h2>{pd.DataFrame((details or {}).get('reward_risk_sensitivity', [])).to_html(index=False, border=0, classes='metrics') if (details or {}).get('reward_risk_sensitivity') else '<p>No target sensitivity results.</p>'}
<h2>Monthly P&amp;L by strategy and capital</h2>{pd.DataFrame((details or {}).get('monthly_pnl', [])).to_html(index=False, border=0, classes='metrics') if (details or {}).get('monthly_pnl') else '<p>No monthly P&amp;L results.</p>'}
<h2>Data, risk, and execution details</h2><pre>{html.escape(json.dumps(json_safe(details or {}), indent=2))}</pre>
<h2>Monte Carlo trade-order analysis</h2><pre>{html.escape(json.dumps(json_safe(monte_carlo), indent=2))}</pre>
<h2>Walk-forward slices</h2><pre>{html.escape(json.dumps(json_safe(walk_forward), indent=2))}</pre>
<h2>Limitations and warnings</h2><div class="warn"><ul>{warning_html}</ul></div>
<h2>Charts</h2><p>Charts are in the sibling charts directory.</p>
<p>Options were not run in this equity-first phase. No historical options data or results are included.</p>
</body></html>"""
    Path(path).write_text(body, encoding="utf-8")
