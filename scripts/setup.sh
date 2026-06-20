#!/usr/bin/env bash
# =============================================================================
# lakebase-streaming-sink-demo — full project setup for a fresh workspace.
#
# Runs the build in dependency order (all Databricks objects via DAB):
#   1. Verify local tools           (databricks CLI, python3)
#   2. Databricks CLI profile        (pick from existing / create new; verify auth)
#   3. Unity Catalog                 (must exist; prompt + create-if-missing)
#   4. Names                         (schema, Lakebase project slug, DBR version)
#   5. bundle validate               (always)
#   6. bundle deploy                 (only with --apply; creates schema/volume/
#                                      Lakebase project/jobs — billable infra)
#   7. run setup_demo                (bronze table + dim_asset + Lakebase DDL/grants)
#
# Safe by default: steps 1–5 only. With --apply it deploys + runs setup_demo.
# Add --start-stream to also launch the continuous streaming-sink job (needs a
# classic DBR-18 cluster — billable). The Zerobus producer is off-platform:
# run scripts/run_producer.sh afterwards (see src/ingest/.env.example).
#
# Usage:
#   scripts/setup.sh [--profile NAME] [--catalog NAME] [--schema NAME]
#                    [--project-id SLUG] [--dbr VERSION] [--target dev|prod]
#                    [--apply] [--start-stream] [--yes]
#
# Examples:
#   scripts/setup.sh                                   # pick profile, catalog, names → validate
#   scripts/setup.sh --profile <profile> --catalog <catalog> --apply
#   scripts/setup.sh --profile <profile> --catalog <catalog> --apply --start-stream
# =============================================================================
set -euo pipefail

# ---- defaults ---------------------------------------------------------------
PROFILE="";        PROFILE_EXPLICIT=false
CATALOG="main";    CATALOG_EXPLICIT=false
OPS_SCHEMA="ops"
PROJECT_ID="lakebase-sink-demo"
DBR="18.2.x-scala2.13"          # Lakebase sink needs DBR 18+ on CLASSIC compute (scala2.13)
TARGET="dev"
ZEROBUS_EP=""                   # optional; persisted so setup_zerobus.sh can reuse it
DO_APPLY=false
START_STREAM=false
AUTO_YES=false

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_FILE="$ROOT/provisioning/setup.env"   # resolved values, reused by setup_zerobus.sh

# ---- pretty output ----------------------------------------------------------
c_blue=$'\033[34m'; c_grn=$'\033[32m'; c_red=$'\033[31m'; c_yel=$'\033[33m'; c_rst=$'\033[0m'
step() { printf "\n${c_blue}==>${c_rst} %s\n" "$*"; }
ok()   { printf "    ${c_grn}✓${c_rst} %s\n" "$*"; }
warn() { printf "    ${c_yel}!${c_rst} %s\n" "$*"; }
die()  { printf "\n${c_red}✗ %s${c_rst}\n" "$*" >&2; exit 1; }

# ---- profile picker (same shape as the Lumen setup) -------------------------
profiles_tsv() {
  local json; json="$(databricks auth profiles -o json 2>/dev/null || true)"
  [[ -z "$json" ]] && return 0
  printf '%s' "$json" | python3 -c 'import sys,json
for p in json.load(sys.stdin).get("profiles",[]):
    print("\t".join([p.get("name",""), p.get("host",""), str(p.get("valid",False)).lower()]))' 2>/dev/null
}

select_profile() {
  local names=() hosts=() valids=() n h v i
  while IFS=$'\t' read -r n h v; do [[ -z "$n" ]] && continue; names+=("$n"); hosts+=("$h"); valids+=("$v"); done < <(profiles_tsv)
  step "2 · Choose a Databricks profile"
  if [[ ${#names[@]} -eq 0 ]]; then warn "No CLI profiles found."; else
    for i in "${!names[@]}"; do
      local mark="${c_red}✗ expired${c_rst}"; [[ "${valids[$i]}" == "true" ]] && mark="${c_grn}✓ valid${c_rst}"
      printf "      ${c_blue}%2d${c_rst}) %-22s %-50s [%b]\n" "$((i+1))" "${names[$i]}" "${hosts[$i]}" "$mark"
    done
  fi
  local newopt=$(( ${#names[@]} + 1 ))
  printf "      ${c_blue}%2d${c_rst}) %s\n" "$newopt" "Create a new profile (databricks auth login)"
  local choice; read -r -p "    Select [1-$newopt]: " choice
  [[ "$choice" =~ ^[0-9]+$ ]] || die "Invalid selection: $choice"
  if [[ "$choice" -ge 1 && "$choice" -le ${#names[@]} ]]; then
    PROFILE="${names[$((choice-1))]}"; ok "Selected profile: $PROFILE"
  elif [[ "$choice" -eq "$newopt" ]]; then
    local newname newhost
    read -r -p "    New profile name: " newname; [[ -n "$newname" ]] || die "Name required"
    read -r -p "    Workspace host URL (https://...): " newhost; [[ "$newhost" == https://* ]] || die "Host must start with https://"
    databricks auth login --host "$newhost" --profile "$newname" || die "Login failed"
    PROFILE="$newname"; ok "Created profile: $PROFILE"
  else die "Out of range: $choice"; fi
}

select_catalog() {
  step "3 · Unity Catalog (hosts the $OPS_SCHEMA schema + checkpoints volume)"
  local input; read -r -p "    Catalog name (Enter = '$CATALOG'): " input
  [[ -n "$input" ]] && CATALOG="$input"
  if databricks catalogs get "$CATALOG" --profile "$PROFILE" >/dev/null 2>&1; then
    ok "Catalog '$CATALOG' exists"
  else
    warn "Catalog '$CATALOG' not found."
    local reply; read -r -p "    Create it now? [Y/n]: " reply
    case "${reply:-Y}" in
      [Yy]*|"") databricks catalogs create "$CATALOG" --profile "$PROFILE" >/dev/null \
                  || die "Create failed (Default-Storage metastores need a MANAGED LOCATION — pick an existing catalog)"; ok "Created '$CATALOG'" ;;
      *) die "Catalog '$CATALOG' must exist (the bundle creates the schema/volume inside it)" ;;
    esac
  fi
}

# ---- args -------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; PROFILE_EXPLICIT=true; shift 2 ;;
    --catalog) CATALOG="$2"; CATALOG_EXPLICIT=true; shift 2 ;;
    --schema) OPS_SCHEMA="$2"; shift 2 ;;
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --dbr) DBR="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --zerobus-endpoint) ZEROBUS_EP="$2"; shift 2 ;;
    --apply) DO_APPLY=true; shift ;;
    --start-stream) START_STREAM=true; shift ;;
    --yes|-y) AUTO_YES=true; shift ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) die "Unknown argument: $1 (try --help)" ;;
  esac
done

# ---- 1. tools ---------------------------------------------------------------
step "1 · Checking local tools"
command -v databricks >/dev/null 2>&1 || die "Missing databricks CLI (https://docs.databricks.com/dev-tools/cli/install.html)"
ok "databricks — $(databricks --version 2>&1 | head -1)"
command -v python3 >/dev/null 2>&1 || die "Missing python3"
ok "python3 — $(python3 --version 2>&1)"

# ---- 2. profile -------------------------------------------------------------
if ! $PROFILE_EXPLICIT && ! $AUTO_YES && [[ -t 0 ]]; then select_profile; fi
[[ -n "$PROFILE" ]] || die "No profile (pass --profile or run interactively)"
if databricks auth token --profile "$PROFILE" >/dev/null 2>&1; then
  ok "Authenticated: $(databricks current-user me --profile "$PROFILE" 2>/dev/null | sed -n 's/.*"userName": *"\([^"]*\)".*/\1/p' | head -1)"
else
  warn "Profile '$PROFILE' has no valid token — launching login…"
  databricks auth login --profile "$PROFILE" || die "Login failed"
fi

# ---- 3. catalog -------------------------------------------------------------
if ! $CATALOG_EXPLICIT && ! $AUTO_YES && [[ -t 0 ]]; then select_catalog
else step "3 · Catalog: $CATALOG"; databricks catalogs get "$CATALOG" --profile "$PROFILE" >/dev/null 2>&1 || die "Catalog '$CATALOG' not found"; fi

# ---- 4. names ---------------------------------------------------------------
step "4 · Names"
ok "schema=$OPS_SCHEMA · lakebase project=$PROJECT_ID · runtime=$DBR · target=$TARGET"
warn "If the Lakebase slug was recently deleted it stays reserved — use a fresh --project-id."

# DAB variable overrides — passed as an ARRAY (never a single string: word-splitting
# a quoted string silently folds every flag into the first --var value). The Lakebase
# project slug is added per-attempt so deploy can retry on a reserved/taken slug.
VARSB=(
  "--var=catalog=$CATALOG"
  "--var=ops_schema=$OPS_SCHEMA"
  "--var=dbr_version=$DBR"
)
# Deploy the continuous sink PAUSED unless --start-stream — avoids auto-starting a
# billable classic cluster on every deploy.
STREAM_PAUSE=PAUSED; $START_STREAM && STREAM_PAUSE=UNPAUSED

# ---- 5. validate ------------------------------------------------------------
step "5 · bundle validate"
databricks bundle validate -t "$TARGET" -p "$PROFILE" "${VARSB[@]}" \
  --var=lakebase_project_id="$PROJECT_ID" --var=stream_pause="$STREAM_PAUSE" >/dev/null
ok "Bundle is valid"

if ! $DO_APPLY; then
  step "Stopping before deploy (no --apply)."
  cat <<EOF
    Review above. To provision (creates the schema, volume, a Lakebase Autoscale
    project, and the jobs — billable), re-run with:

        scripts/setup.sh --profile $PROFILE --catalog $CATALOG --apply
EOF
  exit 0
fi

# ---- 6. deploy (auto-retry on a reserved/taken Lakebase slug) ---------------
step "6 · bundle deploy (creating all objects via DAB)"
warn "Provisioning real infrastructure in profile '$PROFILE'."
deploy_try() {  # $1 = project slug
  databricks bundle deploy -t "$TARGET" -p "$PROFILE" "${VARSB[@]}" \
    --var=lakebase_project_id="$1" --var=stream_pause="$STREAM_PAUSE" 2>&1
}
SLUG="$PROJECT_ID"; DEPLOYED=false
for _try in 1 2 3 4; do
  OUT="$(deploy_try "$SLUG")" || true
  if printf '%s' "$OUT" | grep -q "Deployment complete"; then
    PROJECT_ID="$SLUG"; DEPLOYED=true
    ok "Deployed (schema, volume, Lakebase project '$SLUG', jobs)"
    break
  elif printf '%s' "$OUT" | grep -q "slug already exists"; then
    SLUG="${PROJECT_ID}-$(printf '%04x' "$RANDOM")"
    warn "Lakebase slug taken/reserved (deleted slugs stay reserved) — retrying with: $SLUG"
  else
    printf '%s\n' "$OUT" | tail -6; die "bundle deploy failed"
  fi
done
$DEPLOYED || die "Could not find a free Lakebase project slug after retries"

# Final var set (with the slug that actually deployed) for the run + teardown.
VARS=( "${VARSB[@]}" "--var=lakebase_project_id=$PROJECT_ID" "--var=stream_pause=$STREAM_PAUSE" )

# ---- 7. setup job (tables + Lakebase DDL) -----------------------------------
step "7 · run setup_demo (bronze + dim_asset + Lakebase asset_live_state)"
databricks bundle run setup_demo -t "$TARGET" -p "$PROFILE" "${VARS[@]}"
ok "Tables created and seeded"

$START_STREAM && ok "Sink deployed UNPAUSED — the continuous job is starting (classic DBR-18 cluster)."

# ---- persist resolved values for setup_zerobus.sh ---------------------------
# Resolve the ACTUAL deployed schema name (dev mode prefixes it) and save the
# whole config so the Zerobus setup reuses it (no need to retype anything).
RESOLVED_SCHEMA=$(databricks bundle validate -t "$TARGET" -p "$PROFILE" "${VARS[@]}" -o json 2>/dev/null \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['resources']['schemas']['ops']['name'])" 2>/dev/null)
RESOLVED_SCHEMA="${RESOLVED_SCHEMA:-$OPS_SCHEMA}"
mkdir -p "$(dirname "$STATE_FILE")"
cat > "$STATE_FILE" <<EOF
# Resolved by scripts/setup.sh — reused by setup_zerobus.sh / start_sink.sh (gitignored)
PROFILE=$PROFILE
CATALOG=$CATALOG
OPS_SCHEMA=$OPS_SCHEMA
SCHEMA=$RESOLVED_SCHEMA
LAKEBASE_PROJECT_ID=$PROJECT_ID
DBR=$DBR
TARGET=$TARGET
ZEROBUS_ENDPOINT=$ZEROBUS_EP
EOF
ok "Saved resolved config → provisioning/setup.env"

step "Setup complete"
cat <<EOF
    Lakebase project: ${c_grn}$PROJECT_ID${c_rst}   (catalog=$CATALOG, schema=$RESOLVED_SCHEMA)

    Feed bronze with the Zerobus producer — reuses the values above and auto-derives the
    Zerobus endpoint, so no arguments are needed:
        scripts/setup_zerobus.sh
        scripts/run_producer.sh

    Start the sink later:  scripts/start_sink.sh            # reuses everything above
    Tear down everything:  databricks bundle destroy -t $TARGET -p $PROFILE ${VARS[*]}
EOF
