import sqlite3
from tempfile import NamedTemporaryFile

from ib_history.storage import create_failure_table, insert_bars, ensure_db


def test_insert_bars_and_failure_table():
    with NamedTemporaryFile(suffix=".sqlite") as tmp:
        conn = ensure_db(tmp.name)
        rows = [
            {
                "ts_utc": "2024-01-01T00:00:00",
                "open": 1,
                "high": 2,
                "low": 1,
                "close": 2,
                "volume": 10,
                "vwap": 1.5,
                "trade_count": 1,
            }
        ]
        inserted = insert_bars(conn, "MNQ", "1m", rows)
        assert inserted == 1
        create_failure_table(conn)
        conn.close()
