from nse_research.backtest.costs import CostModel
from nse_research.config import AppConfig
from nse_research.risk.controls import portfolio_drawdown_tripwire, position_size


def load_config() -> AppConfig:
    return AppConfig.load("config.yaml")


def test_delivery_costs_include_both_taxed_legs_and_dp_charge() -> None:
    model = CostModel(load_config().costs)
    total, entry, exit_cost = model.round_trip(100.0, 110.0, 10, 1, intraday=False)
    assert entry.stt > 0
    assert exit_cost.stt > 0
    assert exit_cost.dp == 15.34
    assert total > entry.total + exit_cost.total - 1e-9


def test_position_size_respects_one_percent_risk_and_position_cap() -> None:
    config = load_config()
    result = position_size(100000, 100, 10, config.risk, 25000)
    assert result.quantity > 0
    assert result.quantity * 10 <= 1000
    assert result.quantity * 100 <= 25000


def test_position_size_rejects_zero_quantity() -> None:
    config = load_config()
    result = position_size(10000, 1000, 500, config.risk, 100)
    assert result.quantity == 0
    assert result.reason


def test_cost_inclusive_position_risk_stays_within_one_percent_budget() -> None:
    config = load_config()
    result = position_size(100000, 100, 5, config.risk, 25000, estimated_cost_per_share=0.5, fixed_costs=15.34)
    assert result.quantity > 0
    assert result.estimated_risk <= 1000


def test_sector_risk_limit_is_configured_at_twenty_five_percent() -> None:
    assert load_config().risk.max_sector_exposure_fraction == 0.25


def test_drawdown_tripwire_reserves_one_risk_unit() -> None:
    config = load_config()
    assert abs(portfolio_drawdown_tripwire(config.risk) - 0.09) < 1e-12
