from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Tuple


@dataclass(frozen=True)
class TimeSlice:
    start: datetime
    end: datetime


def slice_range(start: datetime, end: datetime, max_days: int) -> List[TimeSlice]:
    if start >= end:
        return []
    slices: List[TimeSlice] = []
    cursor = start
    while cursor < end:
        next_end = min(cursor + timedelta(days=max_days), end)
        slices.append(TimeSlice(start=cursor, end=next_end))
        cursor = next_end
    return slices


def slice_by_bar(start: datetime, end: datetime, bar: str, max_days_per_bar: dict) -> List[TimeSlice]:
    max_days = max_days_per_bar.get(bar)
    if max_days is None:
        raise ValueError(f"未配置bar周期的最大分片天数: {bar}")
    return slice_range(start, end, max_days=max_days)
