from __future__ import annotations

import os
import sqlite3
from typing import Iterable, Mapping


def ensure_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


def bars_table(symbol: str, bar: str) -> str:
    safe_symbol = symbol.upper()
    safe_bar = bar.replace("/", "_")
    return f"bars_{safe_symbol}_{safe_bar}"


def create_bars_table(conn: sqlite3.Connection, symbol: str, bar: str) -> None:
    table = bars_table(symbol, bar)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            ts_utc TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            vwap REAL,
            trade_count INTEGER
        )
        """
    )


def create_failure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fetch_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            bar TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            reason TEXT,
            is_no_data INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )


def insert_bars(conn: sqlite3.Connection, symbol: str, bar: str, rows: Iterable[Mapping]) -> int:
    create_bars_table(conn, symbol, bar)
    payload = [
        (
            row["ts_utc"],
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("volume"),
            row.get("vwap"),
            row.get("trade_count"),
        )
        for row in rows
    ]
    if not payload:
        return 0
    table = bars_table(symbol, bar)
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {table}
        (ts_utc, open, high, low, close, volume, vwap, trade_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return len(payload)


def log_failure(
    conn: sqlite3.Connection,
    symbol: str,
    bar: str,
    start_utc: str,
    end_utc: str,
    attempt: int,
    reason: str,
    is_no_data: bool,
    created_at: str,
) -> None:
    create_failure_table(conn)
    conn.execute(
        """
        INSERT INTO fetch_failures
        (symbol, bar, start_utc, end_utc, attempt, reason, is_no_data, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (symbol, bar, start_utc, end_utc, attempt, reason, int(is_no_data), created_at),
    )
