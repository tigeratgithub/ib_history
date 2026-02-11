from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class FailureRecord:
    symbol: str
    bar: str
    start_utc: str
    end_utc: str
    attempt: int
    reason: str
    is_no_data: bool


@dataclass
class FetchReport:
    symbols: List[str]
    bars: List[str]
    ranges: List[Dict[str, str]]
    success_count: int = 0
    failures: List[FailureRecord] = field(default_factory=list)
    no_data: List[FailureRecord] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "symbols": self.symbols,
            "bars": self.bars,
            "ranges": self.ranges,
            "success_count": self.success_count,
            "failures": [record.__dict__ for record in self.failures],
            "no_data": [record.__dict__ for record in self.no_data],
        }

    def write_json(self, path: str) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.to_dict(), indent=2, ensure_ascii=False))
