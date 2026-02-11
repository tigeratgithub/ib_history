from __future__ import annotations

from .config import default_config
from .roll_table import export_roll_schedule


def generate_roll_table(path: str = "data/roll_schedule.csv") -> None:
    export_roll_schedule(path, default_config())
