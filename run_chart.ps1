Param(
    [string]$Symbol = "MNQ",
    [string]$Bar = "3m",
    [string]$Db = "data/ib_history.sqlite"
)

$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot

$env:UV_CACHE_DIR = "$PSScriptRoot\.uv_cache"
$env:PYTHONPATH = "$PSScriptRoot\src;$PSScriptRoot\lightweight-charts-python"

uv run --python .\.venv\Scripts\python.exe -m ib_history.cli chart --db $Db --symbol $Symbol --bar $Bar
