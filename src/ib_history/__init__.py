"""IBKR MNQ/MGC 历史K线获取工具库。"""

from .config import Config, default_config
from .fetcher import fetch_history
__all__ = [
    "Config",
    "default_config",
    "fetch_history",
]
