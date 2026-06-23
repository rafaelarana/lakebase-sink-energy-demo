#!/usr/bin/env bash
# Injection simulator — drive a chosen load (signals/sec × seconds) into bronze via Zerobus and
# print ingestion latency live. Reuses the producer venv + .env + compiled proto.
#
#   scripts/simulate_injection.sh                          # prompts for rate + duration
#   scripts/simulate_injection.sh --rate 500 --duration 60
#   scripts/simulate_injection.sh --rate 2000 --duration 120 --seed 7
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ING="$ROOT/src/ingest"

set -a
[ -f "$ROOT/.env" ] && . "$ROOT/.env"
[ -f "$ING/.env" ] && . "$ING/.env"
set +a

export DEMO_CATALOG="${DEMO_CATALOG:-lakebase_sink_demo}"
export DEMO_OPS_SCHEMA="${DEMO_OPS_SCHEMA:-ops}"
export ZEROBUS_TABLE_NAME="${ZEROBUS_TABLE_NAME:-${DEMO_CATALOG}.${DEMO_OPS_SCHEMA}.bronze_sensor_reading}"

: "${ZEROBUS_SERVER_ENDPOINT:?set in src/ingest/.env (run scripts/setup_zerobus.sh)}"
: "${DATABRICKS_WORKSPACE_URL:?set DATABRICKS_WORKSPACE_URL}"
: "${DATABRICKS_CLIENT_ID:?set DATABRICKS_CLIENT_ID}"
: "${DATABRICKS_CLIENT_SECRET:?set DATABRICKS_CLIENT_SECRET}"

PYSPEC=3.12
for v in python3.12 python3.11 python3.10; do command -v "$v" >/dev/null 2>&1 && { PYSPEC="$($v -c 'import sys;print("%d.%d"%sys.version_info[:2])')"; break; }; done
VENV="$ING/.venv-producer"
[ -d "$VENV" ] || uv venv --python "$PYSPEC" "$VENV" >/dev/null
uv pip install --python "$VENV/bin/python" -q -r "$ING/requirements.txt"

"$VENV/bin/python" -m grpc_tools.protoc -I "$ING/schema" --python_out "$ING" "$ING/schema/sensor_reading.proto"

cd "$ING"
exec "$VENV/bin/python" simulate_injection.py "$@"
