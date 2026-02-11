from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Sequence

from .config import Config, default_config, merge_config
import time

from .contract_resolver import resolve_contract
from .ib_client import DataClient, IBAsyncClient
from .report import FailureRecord, FetchReport
from .slicer import slice_by_bar
from .storage import ensure_db, insert_bars, log_failure


def parse_lookback(lookback: str) -> timedelta:
    unit = lookback[-1].lower()
    value = int(lookback[:-1])
    if unit == "d":
        return timedelta(days=value)
    if unit == "w":
        return timedelta(weeks=value)
    if unit == "m":
        return timedelta(days=value * 30)
    if unit == "y":
        return timedelta(days=value * 365)
    raise ValueError(f"不支持的 lookback 格式: {lookback}")


def fetch_history(
    symbols: Sequence[str],
    bars: Sequence[str],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    lookback: Optional[str] = None,
    config: Optional[Config] = None,
    db_path: str = "data/ib_history.sqlite",
    client: Optional[DataClient] = None,
) -> FetchReport:
    cfg = merge_config(config or default_config())
    if end is None:
        end = datetime.utcnow()
    if start is None:
        if lookback is None:
            raise ValueError("start 或 lookback 至少提供一个")
        start = end - parse_lookback(lookback)

    report = FetchReport(
        symbols=list(symbols),
        bars=list(bars),
        ranges=[{"start": start.isoformat(), "end": end.isoformat()}],
    )

    owns_client = client is None
    client = client or IBAsyncClient.from_config(cfg, resolve_contract)
    conn = ensure_db(db_path)

    try:
        for symbol in symbols:
            contract_ranges = _build_contract_ranges(client, symbol, start, end, cfg)
            for bar in bars:
                for contract, range_start, range_end in contract_ranges:
                    slices = slice_by_bar(range_start, range_end, bar, cfg.max_days_per_bar)
                    for time_slice in slices:
                        success = False
                        for attempt in range(1, cfg.retry_rounds + 1):
                            try:
                                rows = _fetch_with_contract(
                                    client,
                                    symbol,
                                    bar,
                                    time_slice.start,
                                    time_slice.end,
                                    cfg,
                                    contract,
                                )
                                if not rows:
                                    record = FailureRecord(
                                        symbol=symbol,
                                        bar=bar,
                                        start_utc=time_slice.start.isoformat(),
                                        end_utc=time_slice.end.isoformat(),
                                        attempt=attempt,
                                        reason="no_data",
                                        is_no_data=True,
                                    )
                                    report.no_data.append(record)
                                    log_failure(
                                        conn,
                                        symbol,
                                        bar,
                                        record.start_utc,
                                        record.end_utc,
                                        attempt,
                                        record.reason,
                                        True,
                                        datetime.utcnow().isoformat(),
                                    )
                                    success = True
                                else:
                                    report.success_count += insert_bars(conn, symbol, bar, rows)
                                    success = True
                                break
                            except Exception as exc:  # noqa: BLE001
                                record = FailureRecord(
                                    symbol=symbol,
                                    bar=bar,
                                    start_utc=time_slice.start.isoformat(),
                                    end_utc=time_slice.end.isoformat(),
                                    attempt=attempt,
                                    reason=str(exc),
                                    is_no_data=False,
                                )
                                report.failures.append(record)
                                log_failure(
                                    conn,
                                    symbol,
                                    bar,
                                    record.start_utc,
                                    record.end_utc,
                                    attempt,
                                    record.reason,
                                    False,
                                    datetime.utcnow().isoformat(),
                                )
                                time.sleep(cfg.pacing_sleep_seconds)
                        if not success:
                            continue
                        time.sleep(cfg.pacing_sleep_seconds)
        conn.commit()
        return report
    finally:
        conn.close()
        if owns_client:
            client.close()


def _fetch_with_contract(client, symbol, bar, start, end, config, contract):
    if hasattr(client, "fetch_bars_for_contract"):
        return client.fetch_bars_for_contract(contract, bar, start, end, config)
    return client.fetch_bars(symbol, bar, start, end, config=config)


def _build_contract_ranges(client, symbol: str, start: datetime, end: datetime, config):
    if not hasattr(client, "list_fut_contracts"):
        return [(None, start, end)]
    contracts = client.list_fut_contracts(symbol, config=config)
    contracts = sorted(contracts, key=lambda c: c.lastTradeDateOrContractMonth)
    ranges = []
    prev_end = start - timedelta(days=1)
    for contract in contracts:
        last = contract.lastTradeDateOrContractMonth
        if len(last) >= 8:
            last = last[:8]
        try:
            last_dt = datetime.strptime(last, "%Y%m%d")
        except ValueError:
            continue
        if last_dt < start:
            prev_end = last_dt
            continue
        range_start = max(start, prev_end + timedelta(days=1))
        range_end = min(end, last_dt)
        if range_start <= range_end:
            ranges.append((contract, range_start, range_end))
        prev_end = last_dt
        if prev_end >= end:
            break
    if not ranges:
        ranges.append((contracts[-1] if contracts else None, start, end))
    return ranges
