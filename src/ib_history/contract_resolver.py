from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List

from .config import Config, mgc_roll_date, mnq_roll_date
from .roll_table import load_roll_schedule, resolve_from_table


@dataclass(frozen=True)
class ResolvedContract:
    symbol: str
    contract_month: str
    exchange: str
    currency: str


def _roll_date(symbol: str, year: int, month: int) -> date:
    if symbol.upper() == "MNQ":
        return mnq_roll_date(year, month)
    if symbol.upper() == "MGC":
        return mgc_roll_date(year, month)
    raise ValueError(f"未配置主力滚动规则: {symbol}")


def resolve_contract(symbol: str, as_of: datetime, config: Config) -> ResolvedContract:
    symbol = symbol.upper()
    roll_table = load_roll_schedule(config.roll_table_path)
    table_record = resolve_from_table(roll_table, symbol, as_of)
    if table_record:
        return ResolvedContract(
            symbol=symbol,
            contract_month=table_record.contract_month,
            exchange=config.contract_exchange.get(symbol, ""),
            currency=config.contract_currency.get(symbol, ""),
        )
    months = config.contract_months.get(symbol)
    if not months:
        raise ValueError(f"未配置合约月份: {symbol}")

    year = as_of.year
    for offset_year in (0, 1):
        for month in months:
            candidate_year = year + offset_year
            roll = _roll_date(symbol, candidate_year, month)
            if as_of.date() < roll:
                contract_month = f"{candidate_year}{month:02d}"
                return ResolvedContract(
                    symbol=symbol,
                    contract_month=contract_month,
                    exchange=config.contract_exchange.get(symbol, ""),
                    currency=config.contract_currency.get(symbol, ""),
                )

    # fallback to last month of next year
    candidate_year = year + 1
    month = months[-1]
    contract_month = f"{candidate_year}{month:02d}"
    return ResolvedContract(
        symbol=symbol,
        contract_month=contract_month,
        exchange=config.contract_exchange.get(symbol, ""),
        currency=config.contract_currency.get(symbol, ""),
    )
