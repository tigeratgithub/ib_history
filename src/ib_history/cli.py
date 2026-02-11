from __future__ import annotations

import argparse
from datetime import datetime

from .config import default_config, merge_config
from .fetcher import fetch_history
from .report import FetchReport


def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ib-history")
    sub = parser.add_subparsers(dest="command", required=True)

    fetch = sub.add_parser("fetch", help="拉取历史K线")
    fetch.add_argument("--symbols", required=True, help="如 MNQ,MGC")
    fetch.add_argument("--bars", required=True, help="如 1m,5m,1h")
    fetch.add_argument("--start", help="ISO 格式，例如 2024-01-01T00:00:00")
    fetch.add_argument("--end", help="ISO 格式，例如 2024-06-01T00:00:00")
    fetch.add_argument("--lookback", help="如 6m, 2y")
    fetch.add_argument("--db", default="data/ib_history.sqlite")
    fetch.add_argument("--report", default="reports/fetch_latest.json")
    fetch.add_argument("--host", default=None)
    fetch.add_argument("--port", type=int, default=None)
    fetch.add_argument("--client-id", type=int, default=None)

    chart = sub.add_parser("chart", help="启动图表展示（待实现）")
    chart.add_argument("--db", default="data/ib_history.sqlite")
    chart.add_argument("--symbol", default="MNQ")
    chart.add_argument("--bar", default="3m")
    chart.add_argument("--display-tz", default="America/New_York")

    roll = sub.add_parser("roll-table", help="生成主力切换表")
    roll.add_argument("--path", default="data/roll_schedule.csv")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "fetch":
        start = parse_datetime(args.start) if args.start else None
        end = parse_datetime(args.end) if args.end else None
        cfg = merge_config(
            default_config(),
            ib_host=args.host,
            ib_port=args.port,
            ib_client_id=args.client_id,
        )
        report = fetch_history(
            symbols=[s.strip() for s in args.symbols.split(",")],
            bars=[b.strip() for b in args.bars.split(",")],
            start=start,
            end=end,
            lookback=args.lookback,
            db_path=args.db,
            config=cfg,
        )
        report.write_json(args.report)
        print(f"成功写入K线数量: {report.success_count}")
        print(f"失败片段数: {len(report.failures)} | 无数据片段数: {len(report.no_data)}")
    elif args.command == "chart":
        from .chart_app import show_chart

        show_chart(args.db, symbol=args.symbol, bar=args.bar, display_tz=args.display_tz)
    elif args.command == "roll-table":
        from .roll_table_cli import generate_roll_table

        generate_roll_table(args.path)
        print(f"已生成主力切换表: {args.path}")


if __name__ == "__main__":
    main()
