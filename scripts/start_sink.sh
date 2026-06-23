#!/usr/bin/env bash
# =============================================================================
# Start (or stop) the continuous Lakebase streaming sink — reusing the values
# scripts/setup.sh already captured (provisioning/setup.env). No arguments needed.
#
#   scripts/start_sink.sh           # start the sink (billable classic DBR 18.3+ cluster)
#   scripts/start_sink.sh --stop    # pause it (cluster stops)
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_FILE="$ROOT/provisioning/setup.env"

c_grn=$'\033[32m'; c_red=$'\033[31m'; c_rst=$'\033[0m'
die(){ printf "${c_red}✗ %s${c_rst}\n" "$*" >&2; exit 1; }

[[ -f "$STATE_FILE" ]] || die "No provisioning/setup.env — run ./run.sh or scripts/setup.sh --apply first."
# shellcheck disable=SC1090
. "$STATE_FILE"
: "${PROFILE:?missing in setup.env}" "${CATALOG:?}" "${OPS_SCHEMA:?}" "${LAKEBASE_PROJECT_ID:?}"
TARGET="${TARGET:-dev}"; DBR="${DBR:-18.3.x-scala2.13}"   # sink needs DBR 18.3+ classic

PAUSE=UNPAUSED; ACTION="Starting"
[[ "${1:-}" == "--stop" ]] && { PAUSE=PAUSED; ACTION="Stopping"; }

echo "$ACTION sink — project=$LAKEBASE_PROJECT_ID, catalog=$CATALOG, target=$TARGET"
databricks bundle deploy -t "$TARGET" -p "$PROFILE" \
  --var=catalog="$CATALOG" --var=ops_schema="$OPS_SCHEMA" \
  --var=lakebase_project_id="$LAKEBASE_PROJECT_ID" --var=dbr_version="$DBR" \
  --var=stream_pause="$PAUSE"

if [[ "$PAUSE" == "UNPAUSED" ]]; then
  printf "${c_grn}✓ Sink started${c_rst} (continuous, classic DBR 18.3+ cluster). Watch asset_live_state fill.\n"
else
  printf "${c_grn}✓ Sink paused${c_rst} (cluster stops).\n"
fi
