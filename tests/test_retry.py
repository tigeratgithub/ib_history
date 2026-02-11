from datetime import datetime

from ib_history.fetcher import fetch_history


class DummyClient:
    def __init__(self):
        self.calls = 0

    def fetch_bars(self, symbol, bar, start, end, config=None):
        self.calls += 1
        if self.calls < 2:
            raise RuntimeError("fail")
        return [
            {
                "ts_utc": start.isoformat(),
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
                "vwap": 1,
                "trade_count": 1,
            }
        ]

    def close(self):
        return None


def test_retry_success(tmp_path):
    report = fetch_history(
        symbols=["MNQ"],
        bars=["1m"],
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 2),
        db_path=str(tmp_path / "test.sqlite"),
        client=DummyClient(),
    )
    assert report.success_count == 1
