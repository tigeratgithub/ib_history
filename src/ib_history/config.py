from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List


@dataclass(frozen=True)
class RollRule:
    """主力切换近似规则。"""

    name: str
    description: str


MNQ_DEFAULT_RULE = RollRule(
    name="mnq_third_friday_monday",
    description="到期月第三个周五前的周一滚动（约提前4-5个交易日）",
)
MGC_DEFAULT_RULE = RollRule(
    name="mgc_prev_month_end_minus_2bd",
    description="到期月前一月月底前2个交易日滚动",
)


@dataclass
class Config:
    ib_host: str = "127.0.0.1"
    ib_port: int = 4002
    ib_client_id: int = 1
    ib_timeout: float = 60.0
    timezone_data: str = "UTC"
    timezone_display: str = "America/New_York"
    pacing_sleep_seconds: float = 1.5
    retry_rounds: int = 3
    what_to_show: str = "TRADES"
    use_rth: bool = False
    use_continuous_futures: bool = False
    max_days_per_bar: Dict[str, int] = field(
        default_factory=lambda: {
            "1m": 1,
            "3m": 3,
            "5m": 5,
            "15m": 10,
            "30m": 20,
            "1h": 30,
            "1d": 365,
        }
    )
    bar_size_map: Dict[str, str] = field(
        default_factory=lambda: {
            "1m": "1 min",
            "3m": "3 mins",
            "5m": "5 mins",
            "15m": "15 mins",
            "30m": "30 mins",
            "1h": "1 hour",
            "1d": "1 day",
        }
    )
    roll_table_path: str = "data/roll_schedule.csv"
    contract_months: Dict[str, List[int]] = field(
        default_factory=lambda: {
            "MNQ": [3, 6, 9, 12],
            "MGC": [2, 4, 6, 8, 10, 12],
        }
    )
    contract_exchange: Dict[str, str] = field(
        default_factory=lambda: {
            "MNQ": "CME",
            "MGC": "COMEX",
        }
    )
    contract_currency: Dict[str, str] = field(
        default_factory=lambda: {
            "MNQ": "USD",
            "MGC": "USD",
        }
    )
    roll_rule_mnq: RollRule = MNQ_DEFAULT_RULE
    roll_rule_mgc: RollRule = MGC_DEFAULT_RULE


def default_config() -> Config:
    return Config()


def merge_config(base: Config, **overrides) -> Config:
    data = base.__dict__.copy()
    data.update({k: v for k, v in overrides.items() if v is not None})
    return Config(**data)


def third_friday(year: int, month: int) -> date:
    first_day = date(year, month, 1)
    first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
    return first_friday + timedelta(weeks=2)


def previous_business_day(day: date, offset: int = 1) -> date:
    current = day
    steps = 0
    while steps < offset:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            steps += 1
    return current


def mnq_roll_date(year: int, month: int) -> date:
    return previous_business_day(third_friday(year, month), offset=4)


def mgc_roll_date(year: int, month: int) -> date:
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    last_day = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return previous_business_day(last_day, offset=1)
