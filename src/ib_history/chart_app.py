from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

from .storage import bars_table


@dataclass
class ChartContext:
    db_path: str
    symbol: str
    bar: str


def _load_bars(db_path: str, symbol: str, bar: str, display_tz: str):
    table = bars_table(symbol, bar)
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            f"SELECT ts_utc, open, high, low, close, volume FROM {table} ORDER BY ts_utc"
        )
        rows = cursor.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    if pd is None:
        return [
            {
                "time": datetime.fromisoformat(row[0]).timestamp(),
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5],
            }
            for row in rows
        ]
    df = pd.DataFrame(
        rows, columns=["time", "open", "high", "low", "close", "volume"]
    )
    # pandas 3.x 可能产生 datetime64[us, UTC]，lightweight-charts 内部按 ns 计算时间戳。
    # 这里统一转换为 datetime64[ns]（无时区）以确保K线正常显示。
    df["time"] = pd.to_datetime(df["time"], utc=True)
    if display_tz:
        df["time"] = df["time"].dt.tz_convert(ZoneInfo(display_tz))
    df["time"] = df["time"].dt.tz_convert(None).astype("datetime64[ns]")
    return df


def show_chart(
    db_path: str,
    symbol: str = "MNQ",
    bar: str = "3m",
    display_tz: str = "America/New_York",
) -> None:
    import sys

    print(f"[chart_app] loaded from: {__file__}")
    print(f"[chart_app] sys.path[0:5]: {sys.path[:5]}")
    print("hello tiger")
    if pd is None:
        raise RuntimeError("请先安装 pandas 以使用图表功能。")
    from lightweight_charts import Chart

    chart = Chart(toolbox=True)
    chart.legend(visible=True, ohlc=True, percent=True, lines=False, color_based_on_candle=True)
    chart.crosshair()

    context = ChartContext(db_path=db_path, symbol=symbol, bar=bar)

    def refresh():
        data = _load_bars(context.db_path, context.symbol, context.bar, display_tz)
        if data is None:
            return
        chart.set(data, True)

    def on_symbol_search(chart_obj, searched):
        context.symbol = searched.upper()
        refresh()

    def on_timeframe_selection(chart_obj):
        context.bar = chart_obj.topbar["timeframe"].value
        refresh()

    chart.events.search += on_symbol_search
    chart.topbar.textbox("symbol", context.symbol)
    chart.topbar.switcher("timeframe", ("3m", "15m"), default=context.bar, func=on_timeframe_selection)
    chart.topbar.textbox("info", "Hover to see OHLC", align="right")

    # 自定义 crosshair 回调，显示 O/H/L/C 及 C-O 绝对值
    def on_crosshair(chart_obj, *args):
        if not args:
            return
        payload = args[0]
        try:
            import json

            data = json.loads(payload)
            time_val = data.get("t")
            open_v = data.get("o")
            high_v = data.get("h")
            low_v = data.get("l")
            close_v = data.get("c")
        except Exception:
            if len(args) < 5:
                return
            time_val, open_v, high_v, low_v, close_v = args[0:5]
        print(f"[crosshair] t={time_val} o={open_v} h={high_v} l={low_v} c={close_v}")
        try:
            ts = datetime.fromtimestamp(float(time_val), tz=ZoneInfo(display_tz))
            interval_seconds = {"3m": 180, "15m": 900}.get(context.bar, 0)
            end_ts = ts if interval_seconds == 0 else ts + timedelta(seconds=interval_seconds)
            time_str = end_ts.strftime("%Y-%m-%d %H:%M")
        except Exception:
            time_str = str(time_val)
        try:
            diff = float(close_v) - float(open_v)
            diff_str = f"{diff:+.2f}"
            pct = (diff / float(open_v)) * 100 if float(open_v) != 0 else 0.0
            pct_str = f"{pct:+.2f}%"
        except Exception:
            diff_str = "n/a"
            pct_str = "n/a"
        tz_str = display_tz
        info = (
            f"{time_str} {tz_str} | O {open_v} H {high_v} L {low_v} C {close_v} "
            f"| C-O {diff_str} ({pct_str})"
        )
        chart_obj.topbar["info"].set(info)
        print(f"[kline_info] {info}")

    salt = chart.id[chart.id.index(".") + 1 :]
    chart.win.handlers[f"crosshair{salt}"] = on_crosshair
    chart.run_script(
        f"""
        let crossHandler{salt} = (param) => {{
            if (!param || !param.time) return;
            const seriesData = param.seriesData.get({chart.id}.series);
            if (!seriesData) return;
            let t = seriesData.time ?? param.time;
            if (t && typeof t === 'object') {{
                if (t.timestamp) {{
                    t = t.timestamp;
                }} else if (t.year) {{
                    t = Date.UTC(t.year, t.month - 1, t.day) / 1000;
                }}
            }}
            const payload = JSON.stringify({{
                t: t,
                o: seriesData.open,
                h: seriesData.high,
                l: seriesData.low,
                c: seriesData.close
            }});
            window.callbackFunction(`crosshair{salt}_~_${{payload}}`);
        }};
        {chart.id}.chart.subscribeCrosshairMove(crossHandler{salt});
        """
    )
    # 启动后发送一次测试回调，确认 Python 回调链路是否正常
    chart.run_script(
        f"window.callbackFunction(`crosshair{salt}_~_{{\\\"t\\\":0,\\\"o\\\":1,\\\"h\\\":2,\\\"l\\\":0.5,\\\"c\\\":1.2}}`);",
        run_last=True,
    )

    refresh()
    chart.show(block=True)
