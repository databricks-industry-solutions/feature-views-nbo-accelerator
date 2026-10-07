#!/usr/bin/env bash
# Pause, resume, or inspect every cost-bearing component of the NBO demo.
#
#   scripts/demo_power.sh status
#   scripts/demo_power.sh pause
#   scripts/demo_power.sh resume
#
# Feature Engineering generates opaque pipeline names and may replace their UUIDs. Resolve them
# from the stream/materialization metadata every run instead of relying on `pipelines list` names.
set -euo pipefail

ACTION="${1:-status}"
PROFILE="${PROFILE:-fe-vm-ttan-vm}"
CATALOG="${CATALOG:-fins_industry_solutions}"
SCHEMA="${SCHEMA:-nbo_tian_tan}"
FQ="$CATALOG.$SCHEMA"
STREAM="${STREAM:-$FQ.session_events_stream}"
ONLINE_STORE="${ONLINE_STORE:-nbottan}"
APP="${APP:-nbo-recommender-ui-ttan}"
RANKER_ENDPOINT="${RANKER_ENDPOINT:-nbo-ranker-realtime}"
FEATURE_ENDPOINT="${FEATURE_ENDPOINT:-nbo-customer-features}"
PAUSE_LAKEBASE="${PAUSE_LAKEBASE:-0}"
PIPELINE_TIMEOUT_S="${PIPELINE_TIMEOUT_S:-900}"

FEATURES=(cust_clicks_10m cust_cat_views_10m cust_mobile_cat_views_10m cust_cat_views_30d)
db() { databricks "$@" --profile "$PROFILE"; }

AUTH_ENV="$(db auth env -o json 2>/dev/null)"
WORKSPACE_URL="$(jq -r '.env.DATABRICKS_HOST' <<<"$AUTH_ENV")"
WORKSPACE_ID="$(jq -r '.env.DATABRICKS_WORKSPACE_ID' <<<"$AUTH_ENV")"
pipeline_url() { printf '%s/?o=%s#joblist/pipelines/%s' "$WORKSPACE_URL" "$WORKSPACE_ID" "$1"; }

# feature<TAB>kind<TAB>pipeline_id<TAB>job_id<TAB>materialized_feature_id
resolve_materializations() {
  local feature id
  for feature in "${FEATURES[@]}"; do
    for id in $(db feature-engineering list-materialized-features --feature-name "$FQ.$feature" -o json |
                jq -r '.[].materialized_feature_id'); do
      db api get "/api/2.0/feature-engineering/materialized-features/$id" |
        jq -r --arg feature "$feature" '
          [$feature,
           (if .streaming_mode then "stream" else "batch" end),
           (.pipeline_id // ""),
           (.job_id // "" | tostring),
           .materialized_feature_id] | @tsv'
    done
  done
}

MAP="$(resolve_materializations)"
RTM_PIPES=($(awk -F'\t' '$2=="stream" && $3!="" {print $3}' <<<"$MAP" | sort -u))
BATCH_JOBS=($(awk -F'\t' '$2=="batch" && $4!="" {print $4}' <<<"$MAP" | sort -u))
INGEST_PIPE="$(db feature-engineering get-stream "$STREAM" -o json | jq -r '.ingestion_config.ingestion_pipeline_id')"
TRAFFIC_JOB="${TRAFFIC_JOB_ID:-$(db apps get "$APP" -o json | jq -r '.resources[] | select(.name=="traffic-job") | .job.id')}"
CONTROL_PATH="dbfs:/Volumes/$CATALOG/$SCHEMA/nbo_control/traffic.json"

pipe_state() { db pipelines get "$1" -o json | jq -r '.state'; }

wait_pipe() {
  local id="$1" wanted="$2" started state
  started=$(date +%s)
  while true; do
    state="$(pipe_state "$id")"
    [[ "$state" == "$wanted" ]] && return 0
    if (( $(date +%s) - started > PIPELINE_TIMEOUT_S )); then
      echo "Timed out waiting for pipeline $id to become $wanted (last state: $state)" >&2
      return 1
    fi
    sleep 10
  done
}

write_traffic_control() {
  local enabled="$1" tmp
  tmp="$(mktemp -t nbo-traffic-control.XXXXXX)"
  printf '{"enabled":%s,"updated_at":"%s"}\n' "$enabled" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$tmp"
  db fs cp "$tmp" "$CONTROL_PATH" --overwrite >/dev/null
  rm -f "$tmp"
}

set_job_pause() { # job id, PAUSED|UNPAUSED; no-op when the generated job has no trigger
  local job_id="$1" pause="$2" settings
  settings="$(db jobs get "$job_id" -o json |
    jq -c --arg pause "$pause" '.settings | {schedule,trigger} |
      with_entries(select(.value != null)) | map_values(.pause_status = $pause)')"
  [[ "$settings" == "{}" ]] || db jobs update --json "{\"job_id\":$job_id,\"new_settings\":$settings}" >/dev/null
}

set_endpoint_scale_to_zero() { # endpoint, true|false
  local endpoint="$1" enabled="$2" current payload
  current="$(db serving-endpoints get "$endpoint" -o json)"
  [[ "$(jq -r '.config.served_entities[0].scale_to_zero_enabled' <<<"$current")" == "$enabled" ]] && return 0
  payload="$(jq -c --argjson enabled "$enabled" '{
    served_entities: [.config.served_entities[] |
      {name, entity_name, workload_size, scale_to_zero_enabled:$enabled} +
      (if (.entity_version // "") != "" then {entity_version} else {} end)],
    traffic_config: {routes: [.config.traffic_config.routes[] |
      {served_entity_name, traffic_percentage}]}
  }' <<<"$current")"
  db serving-endpoints update-config "$endpoint" --json "$payload" >/dev/null
}

lakebase_state() {
  db database get-database-instance "$ONLINE_STORE" -o json 2>/dev/null | jq -r '.state' || echo UNKNOWN
}

status() {
  echo "app        $APP $(db apps get "$APP" -o json | jq -r '.compute_status.state')"
  echo "lakebase   $ONLINE_STORE $(lakebase_state)"
  echo "endpoint   $RANKER_ENDPOINT scale_to_zero=$(db serving-endpoints get "$RANKER_ENDPOINT" -o json | jq -r '.config.served_entities[0].scale_to_zero_enabled')"
  echo "endpoint   $FEATURE_ENDPOINT scale_to_zero=$(db serving-endpoints get "$FEATURE_ENDPOINT" -o json | jq -r '.config.served_entities[0].scale_to_zero_enabled')"
  echo "producer   job=$TRAFFIC_JOB active_runs=$(db jobs list-runs --job-id "$TRAFFIC_JOB" --active-only -o json | jq 'length')"
  echo "ingestion  $INGEST_PIPE $(pipe_state "$INGEST_PIPE")  $(pipeline_url "$INGEST_PIPE")"
  while IFS=$'\t' read -r feature kind pipe job materialized_id; do
    if [[ "$kind" == stream ]]; then
      echo "rtm         $pipe $(pipe_state "$pipe")  $feature"
      echo "            $(pipeline_url "$pipe")"
    fi
  done <<<"$MAP"
  for job in "${BATCH_JOBS[@]}"; do
    db jobs get "$job" -o json |
      jq -r '"batch job   \(.job_id) \(.settings.schedule.pause_status // .settings.trigger.pause_status // "NO_TRIGGER")"'
  done
}

case "$ACTION" in
  status)
    status
    ;;
  pause)
    echo "[1/6] disabling and canceling the Kafka traffic producer"
    write_traffic_control false
    for run in $(db jobs list-runs --job-id "$TRAFFIC_JOB" --active-only -o json | jq -r '.[].run_id'); do
      db jobs cancel-run "$run" >/dev/null &
    done
    wait

    echo "[2/6] stopping Databricks App $APP"
    [[ "$(db apps get "$APP" -o json | jq -r '.compute_status.state')" == STOPPED ]] || db apps stop "$APP" >/dev/null

    echo "[3/6] stopping ingestion + ${#RTM_PIPES[@]} RTM-to-Lakebase pipelines"
    for pipe in "$INGEST_PIPE" "${RTM_PIPES[@]}"; do
      [[ "$(pipe_state "$pipe")" == IDLE ]] || db pipelines stop "$pipe" --no-wait >/dev/null
    done
    for pipe in "$INGEST_PIPE" "${RTM_PIPES[@]}"; do wait_pipe "$pipe" IDLE; done

    echo "[4/6] pausing ${#BATCH_JOBS[@]} scheduled materialization jobs"
    for job in "${BATCH_JOBS[@]}"; do set_job_pause "$job" PAUSED; done

    echo "[5/6] enabling scale-to-zero on serving endpoints"
    set_endpoint_scale_to_zero "$RANKER_ENDPOINT" true &
    set_endpoint_scale_to_zero "$FEATURE_ENDPOINT" true &
    wait

    if [[ "$PAUSE_LAKEBASE" == 1 ]]; then
      echo "[6/6] stopping Lakebase $ONLINE_STORE"
      db database update-database-instance "$ONLINE_STORE" stopped --json '{"stopped":true}' >/dev/null
    else
      echo "[6/6] leaving Lakebase available (set PAUSE_LAKEBASE=1 to stop it)"
    fi
    status
    ;;
  resume)
    echo "[1/6] ensuring Lakebase $ONLINE_STORE is available"
    if [[ "$(lakebase_state)" != AVAILABLE ]]; then
      db database update-database-instance "$ONLINE_STORE" stopped --json '{"stopped":false}' >/dev/null
      until [[ "$(lakebase_state)" == AVAILABLE ]]; do sleep 15; done
    fi

    echo "[2/6] disabling scale-to-zero and warming serving endpoints"
    set_endpoint_scale_to_zero "$RANKER_ENDPOINT" false &
    set_endpoint_scale_to_zero "$FEATURE_ENDPOINT" false &
    wait

    echo "[3/6] starting ingestion, then ${#RTM_PIPES[@]} RTM-to-Lakebase pipelines"
    [[ "$(pipe_state "$INGEST_PIPE")" == RUNNING ]] || db pipelines start-update "$INGEST_PIPE" >/dev/null
    wait_pipe "$INGEST_PIPE" RUNNING
    for pipe in "${RTM_PIPES[@]}"; do
      [[ "$(pipe_state "$pipe")" == RUNNING ]] || db pipelines start-update "$pipe" >/dev/null
    done
    for pipe in "${RTM_PIPES[@]}"; do wait_pipe "$pipe" RUNNING; done

    echo "[4/6] unpausing ${#BATCH_JOBS[@]} scheduled materialization jobs"
    for job in "${BATCH_JOBS[@]}"; do set_job_pause "$job" UNPAUSED; done

    echo "[5/6] enabling and starting the Kafka traffic producer"
    write_traffic_control true
    if [[ "$(db jobs list-runs --job-id "$TRAFFIC_JOB" --active-only -o json | jq 'length')" == 0 ]]; then
      db jobs run-now "$TRAFFIC_JOB" --no-wait >/dev/null
    fi

    echo "[6/6] starting Databricks App $APP"
    [[ "$(db apps get "$APP" -o json | jq -r '.compute_status.state')" == ACTIVE ]] || db apps start "$APP" >/dev/null
    status
    ;;
  *)
    echo "usage: $0 status|pause|resume" >&2
    exit 2
    ;;
esac
