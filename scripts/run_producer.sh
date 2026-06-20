#!/usr/bin/env bash
# Compile the proto and run the Zerobus producer (OFF-platform) into the bronze table.
# Loads .env (repo root) then src/ingest/.env, fills sensible defaults.
#
#   scripts/run_producer.sh                 # stream forever (Ctrl-C to stop)
#   scripts/run_producer.sh --max-batches 30 --batch-size 200
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

: "${ZEROBUS_SERVER_ENDPOINT:?set ZEROBUS_SERVER_ENDPOINT in src/ingest/.env (see .env.example)}"
: "${DATABRICKS_WORKSPACE_URL:?set DATABRICKS_WORKSPACE_URL}"
: "${DATABRICKS_CLIENT_ID:?set DATABRICKS_CLIENT_ID (ingestion service principal)}"
: "${DATABRICKS_CLIENT_SECRET:?set DATABRICKS_CLIENT_SECRET}"

# pick a Python 3.10-3.12 for the producer venv
PYSPEC=3.12
for v in python3.12 python3.11 python3.10; do command -v "$v" >/dev/null 2>&1 && { PYSPEC="$($v -c 'import sys;print("%d.%d"%sys.version_info[:2])')"; break; }; done
VENV="$ING/.venv-producer"
[ -d "$VENV" ] || uv venv --python "$PYSPEC" "$VENV" >/dev/null
uv pip install --python "$VENV/bin/python" -q -r "$ING/requirements.txt"

# (re)compile the proto → src/ingest/sensor_reading_pb2.py
"$VENV/bin/python" -m grpc_tools.protoc -I "$ING/schema" --python_out "$ING" "$ING/schema/sensor_reading.proto"

echo "→ streaming to $ZEROBUS_TABLE_NAME via $ZEROBUS_SERVER_ENDPOINT"
cd "$ING"
exec "$VENV/bin/python" zerobus_producer.py "$@"
