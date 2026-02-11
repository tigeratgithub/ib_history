# ib_history
用于获取 MNQ / MGC 历史 K 线数据。由于 IBKR 不支持连续主力合约按时间段拉取，本项目通过**单月合约分段**获取并落库，供回测使用。

## 快速开始

### 1. 环境
- 使用 `uv` 管理虚拟环境，虚拟环境位于 `.venv`

### 2. 图表查看
使用脚本启动（已包含 `PYTHONPATH`）：

```powershell
.\run_chart.ps1
```

如需手动运行：

```powershell
$env:PYTHONPATH="$pwd\src;$pwd\lightweight-charts-python"
uv run --python .\.venv\Scripts\python.exe -m ib_history.cli chart --db data/ib_history.sqlite --symbol MNQ --bar 3m
```

## 说明
- 数据库存储：`bars_{symbol}_{bar}` 表
- 失败日志：`fetch_failures` 表
- 图表：基于 `lightweight-charts-python`，支持标的/周期切换与动态加载
