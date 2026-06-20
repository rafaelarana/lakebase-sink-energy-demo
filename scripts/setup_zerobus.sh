#!/usr/bin/env bash
# =============================================================================
# Automate the Zerobus producer setup — including the service principal.
#
# Reuses what scripts/setup.sh already resolved (provisioning/setup.env): profile,
# catalog, deployed schema, project. The Zerobus endpoint is DERIVED automatically
# (workspace-id from the x-databricks-org-id header + region from the Lakebase endpoint
# DNS). So after `setup.sh --apply` this is just:
#
#     scripts/setup_zerobus.sh        # no arguments
#     scripts/run_producer.sh
#
# It: creates/reuses an M2M service principal, mints its OAuth secret, grants it
# USE CATALOG / USE SCHEMA / SELECT / MODIFY on the bronze schema, and writes
# src/ingest/.env. Any value can be overridden with a flag (--zerobus-endpoint to pin it).
#
# Flags (all optional if provisioning/setup.env exists):
#   --profile --catalog --schema --zerobus-endpoint --table --sp-name --workspace-url
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_OUT="$ROOT/src/ingest/.env"
STATE_FILE="$ROOT/provisioning/setup.env"

c_blue=$'\033[34m'; c_grn=$'\033[32m'; c_red=$'\033[31m'; c_yel=$'\033[33m'; c_rst=$'\033[0m'
step(){ printf "\n${c_blue}==>${c_rst} %s\n" "$*"; }
ok(){ printf "    ${c_grn}✓${c_rst} %s\n" "$*"; }
warn(){ printf "    ${c_yel}!${c_rst} %s\n" "$*"; }
die(){ printf "\n${c_red}✗ %s${c_rst}\n" "$*" >&2; exit 1; }
jqr(){ python3 -c "import sys,json;d=json.load(sys.stdin);print($1)" 2>/dev/null; }

# Derive the Zerobus endpoint <workspace-id>.zerobus.<region>.cloud.databricks.com:
#   workspace-id  ← the x-databricks-org-id response header
#   region+domain ← the suffix of the Lakebase endpoint DNS (...database.<region>.cloud...)
auto_zerobus_endpoint(){
  local host token org ephost
  host=$(databricks auth profiles -o json 2>/dev/null | python3 -c "
import sys,json
for p in json.load(sys.stdin).get('profiles',[]):
    if p.get('name')=='$PROFILE': print(p.get('host','')); break" 2>/dev/null)
  token=$(databricks auth token -p "$PROFILE" 2>/dev/null | jqr "d['access_token']")
  [[ -n "$host" && -n "$token" ]] || return 1
  org=$(curl -fsSI -H "Authorization: Bearer $token" "$host/api/2.0/clusters/spark-versions" 2>/dev/null \
        | tr -d '\r' | awk -F': ' 'tolower($1)=="x-databricks-org-id"{print $2}')
  [[ -n "$org" ]] || return 1
  ephost=$(databricks api get "/api/2.0/postgres/projects/$LAKEBASE_PROJECT_ID/branches/production/endpoints/primary" \
           -p "$PROFILE" 2>/dev/null | jqr "d['status']['hosts']['host']")
  [[ "$ephost" == *.database.* ]] || return 1
  printf '%s.zerobus.%s' "$org" "${ephost#*.database.}"
}

# ---- defaults from setup.sh's state file ------------------------------------
PROFILE=""; CATALOG=""; SCHEMA=""; ZEROBUS_EP=""; WS_URL=""
TABLE="bronze_sensor_reading"; SP_NAME="lbsink-zerobus-producer"
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  . "$STATE_FILE"
  PROFILE="${PROFILE:-}"; CATALOG="${CATALOG:-}"; SCHEMA="${SCHEMA:-}"; ZEROBUS_EP="${ZEROBUS_ENDPOINT:-}"
fi

while [[ $# -gt 0 ]]; do case "$1" in
  --profile) PROFILE="$2"; shift 2;;
  --catalog) CATALOG="$2"; shift 2;;
  --schema) SCHEMA="$2"; shift 2;;
  --table) TABLE="$2"; shift 2;;
  --sp-name) SP_NAME="$2"; shift 2;;
  --zerobus-endpoint) ZEROBUS_EP="$2"; shift 2;;
  --workspace-url) WS_URL="$2"; shift 2;;
  -h|--help) sed -n '2,18p' "$0"; exit 0;;
  *) die "Unknown arg: $1 (try --help)";;
esac; done

[[ -f "$STATE_FILE" ]] && ok "Reusing provisioning/setup.env (profile=$PROFILE, catalog=$CATALOG, schema=$SCHEMA)" \
  || warn "No provisioning/setup.env — run scripts/setup.sh --apply first, or pass all flags"
[[ -n "$PROFILE" ]] || die "profile not set (run setup.sh --apply, or pass --profile)"
[[ -n "$CATALOG" ]] || die "catalog not set (pass --catalog)"
[[ -n "$SCHEMA"  ]] || die "schema not set (pass --schema = the DEPLOYED schema name)"
command -v databricks >/dev/null || die "databricks CLI not found"

# Auto-derive the Zerobus endpoint from the workspace if not given/saved.
if [[ -z "$ZEROBUS_EP" ]]; then
  step "0 · Deriving Zerobus endpoint from the workspace"
  ZEROBUS_EP=$(auto_zerobus_endpoint || true)
  [[ -n "$ZEROBUS_EP" ]] && ok "Zerobus endpoint: $ZEROBUS_EP" \
    || die "Could not auto-derive the Zerobus endpoint — pass --zerobus-endpoint"
fi

step "1 · Service principal '$SP_NAME'"
LIST=$(databricks service-principals list -p "$PROFILE" -o json 2>/dev/null || echo '[]')
SP_LINE=$(printf '%s' "$LIST" | python3 -c "
import sys,json
d=json.load(sys.stdin); items=d if isinstance(d,list) else d.get('Resources',d.get('resources',[]))
for s in items:
    if s.get('displayName')=='$SP_NAME': print(s.get('id',''), s.get('applicationId','')); break
" 2>/dev/null || true)
SP_ID=$(printf '%s' "$SP_LINE" | awk '{print $1}')
APP_ID=$(printf '%s' "$SP_LINE" | awk '{print $2}')
if [[ -n "$SP_ID" ]]; then
  ok "Reusing existing SP (id=$SP_ID, app=$APP_ID)"
else
  CREATE=$(databricks service-principals create --display-name "$SP_NAME" -p "$PROFILE" -o json) || die "Create SP failed (need admin)"
  SP_ID=$(printf '%s' "$CREATE" | jqr "d.get('id','')")
  APP_ID=$(printf '%s' "$CREATE" | jqr "d.get('applicationId','')")
  [[ -n "$SP_ID" && -n "$APP_ID" ]] || die "Could not parse SP id/applicationId"
  ok "Created SP (id=$SP_ID, app=$APP_ID)"
fi

step "2 · Mint OAuth secret"
SEC=$(databricks service-principal-secrets-proxy create "$SP_ID" -p "$PROFILE" -o json) || die "Mint secret failed"
CLIENT_SECRET=$(printf '%s' "$SEC" | jqr "d.get('secret','')")
[[ -n "$CLIENT_SECRET" ]] || die "No 'secret' in response"
ok "Secret minted (written to .env)"

step "3 · Grant UC permissions on $CATALOG.$SCHEMA"
grant(){ databricks grants update "$1" "$2" -p "$PROFILE" \
    --json "{\"changes\":[{\"principal\":\"$APP_ID\",\"add\":$3}]}" >/dev/null || die "GRANT failed on $1 $2"; ok "granted $3 on $1 $2"; }
grant CATALOG "$CATALOG"         '["USE_CATALOG"]'
grant SCHEMA  "$CATALOG.$SCHEMA" '["USE_SCHEMA","SELECT","MODIFY"]'

step "4 · Write $ENV_OUT"
if [[ -z "$WS_URL" ]]; then
  WS_URL=$(databricks auth profiles -o json 2>/dev/null | python3 -c "
import sys,json
for p in json.load(sys.stdin).get('profiles',[]):
    if p.get('name')=='$PROFILE': print(p.get('host','')); break" 2>/dev/null)
fi
cat > "$ENV_OUT" <<EOF
# Generated by scripts/setup_zerobus.sh (DO NOT COMMIT)
ZEROBUS_SERVER_ENDPOINT=$ZEROBUS_EP
DATABRICKS_WORKSPACE_URL=$WS_URL
ZEROBUS_TABLE_NAME=$CATALOG.$SCHEMA.$TABLE
DATABRICKS_CLIENT_ID=$APP_ID
DATABRICKS_CLIENT_SECRET=$CLIENT_SECRET
EMIT_INTERVAL_SECONDS=1.0
SPIKE_PROBABILITY=0.02
BAD_QUALITY_PROBABILITY=0.01
EOF
chmod 600 "$ENV_OUT"
ok "wrote .env (client_id=$APP_ID, table=$CATALOG.$SCHEMA.$TABLE)"

step "Done — run the producer:  scripts/run_producer.sh   (--max-batches N for a bounded run)"
