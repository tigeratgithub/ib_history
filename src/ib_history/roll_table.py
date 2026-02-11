from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .config import Config, mgc_roll_date, mnq_roll_date


@dataclass(frozen=True)
class RollRecord:
    symbol: str
    contract_month: str
    start_date: date
    end_date: date


def _roll_date(symbol: str, year: int, month: int) -> date:
    if symbol.upper() == "MNQ":
        return mnq_roll_date(year, month)
    if symbol.upper() == "MGC":
        return mgc_roll_date(year, month)
    raise ValueError(f"未配置主力滚动规则: {symbol}")


def build_roll_schedule(symbol: str, years: Iterable[int], config: Config) -> List[RollRecord]:
    symbol = symbol.upper()
    months = config.contract_months.get(symbol, [])
    years = list(years)
    if not years:
        return []
    records: List[RollRecord] = []
    for year in years:
        for month in months:
            roll = _roll_date(symbol, year, month)
            contract_month = f"{year}{month:02d}"
            if records:
                last = records[-1]
                records[-1] = RollRecord(
                    symbol=last.symbol,
                    contract_month=last.contract_month,
                    start_date=last.start_date,
                    end_date=roll,
                )
            records.append(
                RollRecord(
                    symbol=symbol,
                    contract_month=contract_month,
                    start_date=roll,
                    end_date=roll,
                )
            )
    # shift start for first record to year start
    if records:
        first = records[0]
        records[0] = RollRecord(
            symbol=first.symbol,
            contract_month=first.contract_month,
            start_date=date(min(years), 1, 1),
            end_date=first.end_date,
        )
        # ensure last record covers through end_year
        last = records[-1]
        records[-1] = RollRecord(
            symbol=last.symbol,
            contract_month=last.contract_month,
            start_date=last.start_date,
            end_date=date(max(years), 12, 31),
        )
    return records


def export_roll_schedule(path: str, config: Config, start_year: int = 2018, end_year: int = 2035) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    years = range(start_year, end_year + 1)
    records: List[RollRecord] = []
    for symbol in ("MNQ", "MGC"):
        records.extend(build_roll_schedule(symbol, years, config))
    with target.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "contract_month", "start_date", "end_date"])
        for rec in records:
            writer.writerow(
                [
                    rec.symbol,
                    rec.contract_month,
                    rec.start_date.isoformat(),
                    rec.end_date.isoformat(),
                ]
            )
    return target


def load_roll_schedule(path: str) -> Dict[str, List[RollRecord]]:
    table: Dict[str, List[RollRecord]] = {}
    file = Path(path)
    if not file.exists():
        return table
    with file.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            record = RollRecord(
                symbol=row["symbol"],
                contract_month=row["contract_month"],
                start_date=date.fromisoformat(row["start_date"]),
                end_date=date.fromisoformat(row["end_date"]),
            )
            table.setdefault(record.symbol, []).append(record)
    return table


def resolve_from_table(
    table: Dict[str, List[RollRecord]], symbol: str, as_of: datetime
) -> Optional[RollRecord]:
    records = table.get(symbol.upper())
    if not records:
        return None
    for record in records:
        if record.start_date <= as_of.date() < record.end_date:
            return record
    return None
