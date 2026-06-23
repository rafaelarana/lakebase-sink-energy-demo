#!/usr/bin/env bash
# Monitor rows landing in Lakebase asset_live_state + pipeline latency, printing every interval.
# Reuses the producer venv (psycopg + databricks-sdk) and provisioning/setup.env.
#
#   scripts/monitor_lakebase.sh                       # 5s interval, until Ctrl-C
#   scripts/monitor_lakebase.sh --interval 5 --duration 120
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ING="$ROOT/src/ingest"
VENV="$ING/.venv-producer"

[ -f "$ROOT/provisioning/setup.env" ] || { echo "✗ run scripts/setup.sh --apply first (no provisioning/setup.env)"; exit 1; }

PYSPEC=3.12
for v in python3.12 python3.11 python3.10; do command -v "$v" >/dev/null 2>&1 && { PYSPEC="$($v -c 'import sys;print("%d.%d"%sys.version_info[:2])')"; break; }; done
[ -d "$VENV" ] || uv venv --python "$PYSPEC" "$VENV" >/dev/null
uv pip install --python "$VENV/bin/python" -q "psycopg[binary]>=3.1" "databricks-sdk>=0.89.0"

exec "$VENV/bin/python" "$ROOT/src/monitor/monitor_lakebase.py" "$@"
