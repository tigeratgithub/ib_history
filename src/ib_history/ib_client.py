from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Mapping, Optional, Protocol


class DataClient(Protocol):
    def fetch_bars(
        self, symbol: str, bar: str, start: datetime, end: datetime, config=None
    ) -> List[Mapping]:
        ...

    def close(self) -> None:
        ...

    def list_fut_contracts(self, symbol: str, config=None) -> List[object]:
        ...


@dataclass
class IBAsyncClient:
    """占位实现：请用 ib_async 连接 IB Gateway 并实现 fetch_bars。"""

    host: str = "127.0.0.1"
    port: int = 4002
    client_id: int = 1
    timeout: float = 60.0
    what_to_show: str = "TRADES"
    use_rth: bool = False
    _ib: Optional[object] = None
    _contract_resolver: Optional[object] = None
    _contfut_cache: dict = None
    _fut_cache: dict = None

    def _load_ib(self):
        from ib_async import IB, Future, Contract, util  # type: ignore

        return IB, Future, Contract, util

    def _ensure_connected(self) -> None:
        if self._ib is not None:
            return
        IB, _, _, _ = self._load_ib()
        ib = IB()
        ib.connect(self.host, self.port, clientId=self.client_id, timeout=self.timeout)
        self._ib = ib
        if self._contfut_cache is None:
            self._contfut_cache = {}
        if self._fut_cache is None:
            self._fut_cache = {}

    def fetch_bars(
        self, symbol: str, bar: str, start: datetime, end: datetime, config=None
    ) -> List[Mapping]:
        if config is None:
            raise ValueError("IBAsyncClient.fetch_bars 需要传入 config")
        return self.fetch_bars_with_config(symbol, bar, start, end, config)

    @classmethod
    def from_config(cls, config, contract_resolver):
        client = cls(
            host=config.ib_host,
            port=config.ib_port,
            client_id=config.ib_client_id,
            timeout=config.ib_timeout,
            what_to_show=config.what_to_show,
            use_rth=config.use_rth,
        )
        client._contract_resolver = contract_resolver
        return client

    def _duration_str(self, start: datetime, end: datetime) -> str:
        delta = end - start
        if delta < timedelta(days=1):
            seconds = max(30, int(delta.total_seconds()))
            return f"{seconds} S"
        days = max(1, int(delta.total_seconds() // 86400))
        return f"{days} D"

    def _bar_size(self, bar: str, config) -> str:
        return config.bar_size_map.get(bar, bar)

    def fetch_bars_with_config(
        self, symbol: str, bar: str, start: datetime, end: datetime, config
    ) -> List[Mapping]:
        self._ensure_connected()
        IB, Future, Contract, util = self._load_ib()
        if config.use_continuous_futures:
            contract = self._get_contfut_contract(symbol, config, Contract)
        else:
            contract = self._get_fut_contract_by_date(symbol, start, config, Contract)
        duration = self._duration_str(start, end)
        bars = self._ib.reqHistoricalData(  # type: ignore[attr-defined]
            contract,
            endDateTime=end,
            durationStr=duration,
            barSizeSetting=self._bar_size(bar, config),
            whatToShow=self.what_to_show,
            useRTH=self.use_rth,
            formatDate=1,
            timeout=self.timeout,
        )
        rows = []
        for bar_data in bars:
            ts = bar_data.date
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                ts_utc = ts.astimezone(timezone.utc).isoformat()
            else:
                ts_utc = datetime.combine(ts, datetime.min.time(), tzinfo=timezone.utc).isoformat()
            rows.append(
                {
                    "ts_utc": ts_utc,
                    "open": bar_data.open,
                    "high": bar_data.high,
                    "low": bar_data.low,
                    "close": bar_data.close,
                    "volume": int(bar_data.volume),
                    "vwap": bar_data.average,
                    "trade_count": bar_data.barCount,
                }
            )
        return rows

    def fetch_bars_for_contract(
        self, contract, bar: str, start: datetime, end: datetime, config
    ) -> List[Mapping]:
        self._ensure_connected()
        IB, Future, Contract, util = self._load_ib()
        duration = self._duration_str(start, end)
        bars = self._ib.reqHistoricalData(  # type: ignore[attr-defined]
            contract,
            endDateTime=end,
            durationStr=duration,
            barSizeSetting=self._bar_size(bar, config),
            whatToShow=self.what_to_show,
            useRTH=self.use_rth,
            formatDate=1,
            timeout=self.timeout,
        )
        rows = []
        for bar_data in bars:
            ts = bar_data.date
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                ts_utc = ts.astimezone(timezone.utc).isoformat()
            else:
                ts_utc = datetime.combine(ts, datetime.min.time(), tzinfo=timezone.utc).isoformat()
            rows.append(
                {
                    "ts_utc": ts_utc,
                    "open": bar_data.open,
                    "high": bar_data.high,
                    "low": bar_data.low,
                    "close": bar_data.close,
                    "volume": int(bar_data.volume),
                    "vwap": bar_data.average,
                    "trade_count": bar_data.barCount,
                }
            )
        return rows

    def _get_contfut_contract(self, symbol: str, config, Contract):
        symbol = symbol.upper()
        if symbol in self._contfut_cache:
            return self._contfut_cache[symbol]
        cont = Contract()
        cont.symbol = symbol
        cont.secType = "CONTFUT"
        cont.exchange = config.contract_exchange.get(symbol, "")
        cont.currency = config.contract_currency.get(symbol, "")
        details = self._ib.reqContractDetails(cont)  # type: ignore[attr-defined]
        if not details:
            raise ValueError(f"无法获取连续合约信息: {symbol}")
        contract = details[0].contract
        self._contfut_cache[symbol] = contract
        return contract

    def _get_fut_contract_by_date(self, symbol: str, as_of: datetime, config, Contract):
        symbol = symbol.upper()
        cache = self._fut_cache.get(symbol) if self._fut_cache else None
        if cache is None:
            base = Contract()
            base.symbol = symbol
            base.secType = "FUT"
            base.exchange = config.contract_exchange.get(symbol, "")
            base.currency = config.contract_currency.get(symbol, "")
            base.includeExpired = False
            details = self._ib.reqContractDetails(base)  # type: ignore[attr-defined]
            cache = []
            for d in details:
                con = d.contract
                cache.append(con)
            self._fut_cache[symbol] = cache
        target = None
        for con in sorted(
            cache,
            key=lambda c: c.lastTradeDateOrContractMonth,
        ):
            last_date = con.lastTradeDateOrContractMonth
            if len(last_date) >= 8:
                last_date = last_date[:8]
            try:
                last_dt = datetime.strptime(last_date, "%Y%m%d")
            except ValueError:
                continue
            if last_dt.date() >= as_of.date():
                target = con
                break
        if target is None and cache:
            target = sorted(cache, key=lambda c: c.lastTradeDateOrContractMonth)[-1]
        if target is None:
            raise ValueError(f"未找到合约: {symbol} {as_of.date().isoformat()}")
        return target

    def list_fut_contracts(self, symbol: str, config=None) -> List[object]:
        self._ensure_connected()
        IB, Future, Contract, util = self._load_ib()
        symbol = symbol.upper()
        base = Contract()
        base.symbol = symbol
        base.secType = "FUT"
        base.exchange = config.contract_exchange.get(symbol, "")
        base.currency = config.contract_currency.get(symbol, "")
        base.includeExpired = True
        details = self._ib.reqContractDetails(base)  # type: ignore[attr-defined]
        return [d.contract for d in details]

    def close(self) -> None:
        if self._ib is not None:
            self._ib.disconnect()
            self._ib = None
