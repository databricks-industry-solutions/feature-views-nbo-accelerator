#!/usr/bin/env bash
# Tear down everything this accelerator created in a workspace.
#
#   bash scripts/cleanup.sh -p <profile> --var warehouse_id=<id> [--var name=value ...]          # dry run
#   bash scripts/cleanup.sh -p <profile> --var warehouse_id=<id> [--var name=value ...] --yes    # delete
#
# Pass the same --var values you used for `databricks bundle deploy`, so the script resolves the same
# catalog, schema, online store, endpoints, and app. The Kafka topic and cluster are not touched.
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE=""
TARGET="dev"
APPLY=false
VARS=()
while (($#)); do
  case "$1" in
    -p | --profile) PROFILE="$2"; shift 2 ;;
    -t | --target) TARGET="$2"; shift 2 ;;
    --var) VARS+=(--var "$2"); shift 2 ;;
    --var=*) VARS+=("$1"); shift ;;
    --yes) APPLY=true; shift ;;
    -h | --help) sed -n '2,8p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$PROFILE" ]] || { echo "usage: bash scripts/cleanup.sh -p <profile> [--var name=value ...] [--yes]" >&2; exit 2; }
for required in databricks jq; do
  command -v "$required" >/dev/null || { echo "required command not found: $required" >&2; exit 1; }
done

db() { databricks "$@" -p "$PROFILE"; }

# ── resolve the deployed names from the bundle ───────────────────────────────
CONFIG="$(db bundle validate -t "$TARGET" ${VARS[@]+"${VARS[@]}"} -o json)"
var() { jq -r --arg k "$1" '.variables[$k].value // empty' <<<"$CONFIG"; }
CATALOG="$(var catalog)"
SCHEMA="$(var schema)"
FQ="$CATALOG.$SCHEMA"
STORE="$(var online_store_name)"
APP="$(var app_name)"
TOPIC="$(var kafka_topic)"
[[ -n "$CATALOG" && -n "$SCHEMA" && -n "$STORE" ]] || { echo "could not resolve catalog, schema, or online store from the bundle" >&2; exit 1; }

# ── find what exists ─────────────────────────────────────────────────────────
ENDPOINTS=()
for name in "$(var ranker_online_endpoint)" "$(var ranker_realtime_endpoint)" "$(var feature_serving_endpoint)"; do
  [[ -n "$name" ]] && db serving-endpoints get "$name" >/dev/null 2>&1 && ENDPOINTS+=("$name")
done
SCHEMA_EXISTS=false
db schemas get "$FQ" >/dev/null 2>&1 && SCHEMA_EXISTS=true
FEATURES=()
STREAMS=()
MATERIALIZED="" # id<TAB>feature<TAB>kind, offline first
if $SCHEMA_EXISTS; then
  FEATURES=($( (db feature-engineering list-features "$CATALOG" "$SCHEMA" -o json 2>/dev/null || echo '[]') | jq -r '.[]?.full_name'))
  STREAMS=($( (db feature-engineering list-streams --parent "$FQ" -o json 2>/dev/null || echo '[]') | jq -r '.[]?.name'))
  MATERIALIZED="$(for feature in ${FEATURES[@]+"${FEATURES[@]}"}; do
    (db api get "/api/2.0/feature-engineering/materialized-features?feature_name=$feature" 2>/dev/null || echo '{}') |
      jq -r '.materialized_features[]? | [.materialized_feature_id, .feature_name,
        (if .streaming_mode then "streaming" elif .is_online then "online" else "offline" end)] | @tsv'
  done | sort -t$'\t' -k3,3)" # offline sorts first: deleting it also removes its online copies
fi
STORE_EXISTS=false
db api get "/api/2.0/feature-store/online-stores/$STORE" >/dev/null 2>&1 && STORE_EXISTS=true
count() { [[ -z "$1" ]] && echo 0 || grep -c . <<<"$1"; }

echo "Profile $PROFILE, target $TARGET, schema $FQ"
echo
echo "$($APPLY && echo "Deleting" || echo "Would delete"):"
echo "  Bundle resources (Lakeflow Jobs, app ${APP:-?}, dashboard): databricks bundle destroy"
echo "  Serving endpoints: ${ENDPOINTS[*]:-none}"
echo "  Materialized features: $(count "$MATERIALIZED"), with their pipelines and jobs"
echo "  Feature definitions: ${#FEATURES[@]}"
echo "  Streams: ${STREAMS[*]:-none}, with their ingestion pipelines"
echo "  Schema: $($SCHEMA_EXISTS && echo "$FQ (all tables, models, and functions in it)" || echo "none")"
echo "  Lakebase online store: $($STORE_EXISTS && echo "$STORE (all online tables in it)" || echo "none")"
echo "Not touched: Kafka topic ${TOPIC:-?} and the Kafka cluster."
if ! $APPLY; then
  echo
  echo "Dry run. Re-run with --yes to delete."
  exit 0
fi

# ── delete ───────────────────────────────────────────────────────────────────
FAILED=0
step() {
  local label="$1" out
  shift
  if out="$(db "$@" 2>&1)"; then
    echo "  deleted $label"
  elif grep -qiE "not ?found|does not exist|RESOURCE_DOES_NOT_EXIST" <<<"$out"; then
    echo "  already gone: $label"
  else
    echo "  FAILED $label: ${out:0:300}"
    FAILED=$((FAILED + 1))
  fi
}

echo
echo "[1/6] Bundle resources"
db bundle destroy -t "$TARGET" ${VARS[@]+"${VARS[@]}"} --auto-approve || FAILED=$((FAILED + 1))

echo "[2/6] Serving endpoints"
for name in ${ENDPOINTS[@]+"${ENDPOINTS[@]}"}; do step "endpoint $name" serving-endpoints delete "$name"; done

echo "[3/6] Materialized features"
while IFS=$'\t' read -r id feature kind; do
  [[ -n "$id" ]] && step "$kind materialization of $feature" feature-engineering delete-materialized-feature "$id"
done <<<"$MATERIALIZED"

echo "[4/6] Feature definitions and Streams"
for feature in ${FEATURES[@]+"${FEATURES[@]}"}; do step "feature $feature" feature-engineering delete-feature "$feature"; done
for stream in ${STREAMS[@]+"${STREAMS[@]}"}; do step "stream $stream" feature-engineering delete-stream "$stream"; done

echo "[5/6] Schema"
$SCHEMA_EXISTS && step "schema $FQ" schemas delete "$FQ" --force

echo "[6/6] Lakebase online store"
$STORE_EXISTS && step "online store $STORE" api delete "/api/2.0/feature-store/online-stores/$STORE"

echo
if ((FAILED)); then
  echo "$FAILED step(s) failed. Fix the cause shown above and re-run; finished steps are skipped."
  exit 1
fi
echo "Cleanup complete."
