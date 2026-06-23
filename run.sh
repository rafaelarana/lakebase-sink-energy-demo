#!/usr/bin/env bash
# =============================================================================
# run.sh — one command to stand up the whole demo, end to end:
#   1. scripts/setup.sh --apply       → deploy all objects + create tables (DAB)
#   2. scripts/setup_zerobus.sh       → service principal + secret + grants + .env (endpoint auto-derived)
#   3. scripts/run_producer.sh        → feed the bronze history via Zerobus
#
# Each step reuses what the previous resolved (provisioning/setup.env), so you only
# pass the workspace bits once.
#
# Usage:
#   ./run.sh                              # interactive — prompts for profile + catalog (like setup.sh)
#   ./run.sh --profile <p> --catalog <cat> [options]
#
# Options (— provisioning flags pass through to setup.sh):
#   --profile NAME       Databricks CLI profile        (prompted if omitted)
#   --catalog NAME       existing Unity Catalog         (prompted if omitted)
#   --project-id SLUG    Lakebase project slug          (default lakebase-sink-demo; auto-retried if taken)
#   --schema NAME        schema name                    (default ops)
#   --dbr VERSION        runtime for the sink cluster   (default 18.3.x-scala2.13; sink needs DBR 18.3+ classic)
#   --start-stream       also launch the continuous Lakebase sink (billable classic cluster)
#   --batches N          producer batches to send       (default 10; use --forever to stream)
#   --forever            run the producer until Ctrl-C
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

c_blue=$'\033[34m'; c_grn=$'\033[32m'; c_rst=$'\033[0m'
say(){ printf "\n${c_blue}########  %s  ########${c_rst}\n" "$*"; }

BATCHES=10
SETUP_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile|--catalog|--project-id|--schema|--dbr|--target) SETUP_ARGS+=("$1" "$2"); shift 2 ;;
    --start-stream|--yes) SETUP_ARGS+=("$1"); shift ;;
    --batches) BATCHES="$2"; shift 2 ;;
    --forever) BATCHES=0; shift ;;
    -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $1 (try --help)" >&2; exit 1 ;;
  esac
done

say "1/3 · Provision (deploy + create tables)"
# ${arr[@]+...} guard: safe empty-array expansion under `set -u` (bash 3.2 on macOS).
"$ROOT/scripts/setup.sh" ${SETUP_ARGS[@]+"${SETUP_ARGS[@]}"} --apply

say "2/3 · Zerobus setup (service principal + .env)"
"$ROOT/scripts/setup_zerobus.sh"

say "3/3 · Feed bronze via Zerobus"
if [[ "$BATCHES" == "0" ]]; then
  "$ROOT/scripts/run_producer.sh"
else
  "$ROOT/scripts/run_producer.sh" --max-batches "$BATCHES"
fi

say "Done"
printf "${c_grn}The demo is up.${c_rst} Query live state in Lakebase:\n"
cat <<'EOF'
    SELECT site_name, sensor_asset_id, value, status, reading_ts
    FROM   asset_live_state ORDER BY updated_ts DESC LIMIT 20;

  Start the sink (if not already):  scripts/start_sink.sh        # reuses captured args
  Tear down:                        databricks bundle destroy -t dev -p <p> <same --var values>
EOF
