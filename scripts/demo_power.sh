#!/usr/bin/env bash
# Pause, resume, or inspect every cost-bearing component of the NBO demo.
#
#   scripts/demo_power.sh status
#   scripts/demo_power.sh pause
#   scripts/demo_power.sh resume
#
# This follows the proven Lime demo power sequence:
#   - Resolve generated Feature Engineering pipelines/jobs from FE metadata every run.
#   - Keep the Feature Store-managed Autoscaling Lakebase endpoint enabled.
#   - On resume, verify Lakebase first, wait for serving updates, start ingestion before RTM,
#     then restore traffic and deploy the last runnable App snapshot.
set -euo pipefail

ACTION="${1:-status}"
PROFILE="${PROFILE:-fe-vm-ttan-vm}"
CATALOG="${CATALOG:-fins_industry_solutions}"
SCHEMA="${SCHEMA:-nbo_tian_tan}"
FQ="$CATALOG.$SCHEMA"
STREAM="${STREAM:-$FQ.session_events_stream}"
ONLINE_STORE="${ONLINE_STORE:-nbottan}"
AUTOSCALING_PROJECT="${AUTOSCALING_PROJECT:-$ONLINE_STORE}"
AUTOSCALING_BRANCH="${AUTOSCALING_BRANCH:-production}"
AUTOSCALING_ENDPOINT="${AUTOSCALING_ENDPOINT:-primary}"
APP="${APP:-nbo-recommender-ui-ttan}"
ENDPOINTS=(
  "${RANKER_ENDPOINT:-nbo-ranker-realtime}"
  "${FEATURE_ENDPOINT:-nbo-customer-features}"
)
PIPELINE_TIMEOUT_S="${PIPELINE_TIMEOUT_S:-900}"
LAKEBASE_TIMEOUT_S="${LAKEBASE_TIMEOUT_S:-1200}"
ENDPOINT_TIMEOUT_S="${ENDPOINT_TIMEOUT_S:-1200}"
APP_TIMEOUT_S="${APP_TIMEOUT_S:-900}"
APP_DEPLOYMENT_ID=""
APP_PREVIOUS_DEPLOYMENT_ID=""
LB_PROJECT_PATH="projects/$AUTOSCALING_PROJECT"
LB_BRANCH_PATH="$LB_PROJECT_PATH/branches/$AUTOSCALING_BRANCH"
LB_ENDPOINT_PATH="$LB_BRANCH_PATH/endpoints/$AUTOSCALING_ENDPOINT"

FEATURES=(cust_clicks_10m cust_cat_views_10m cust_mobile_cat_views_10m cust_cat_views_30d)

db() { databricks "$@" --profile "$PROFILE"; }
die() { echo "ERROR: $*" >&2; exit 1; }

for required in databricks jq curl; do
  command -v "$required" >/dev/null || die "required command not found: $required"
done

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
STREAM_JSON="$(db feature-engineering get-stream "$STREAM" -o json)"
INGEST_PIPE="$(jq -r '.ingestion_config.ingestion_pipeline_id' <<<"$STREAM_JSON")"
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
      die "pipeline $id did not become $wanted after ${PIPELINE_TIMEOUT_S}s (last state: $state)"
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

set_job_pause() {
  local job_id="$1" pause="$2" settings
  settings="$(db jobs get "$job_id" -o json |
    jq -c --arg pause "$pause" '.settings | {schedule,trigger} |
      with_entries(select(.value != null)) | map_values(.pause_status = $pause)')"
  [[ "$settings" == "{}" ]] || db jobs update --json "{\"job_id\":$job_id,\"new_settings\":$settings}" >/dev/null
}

endpoint_json() { db serving-endpoints get "$1" -o json; }

set_endpoint_scale_to_zero() {
  local endpoint="$1" enabled="$2" current current_enabled ready update payload
  current="$(endpoint_json "$endpoint")"
  current_enabled="$(jq -r '.config.served_entities[0].scale_to_zero_enabled' <<<"$current")"
  ready="$(jq -r '.state.ready' <<<"$current")"
  update="$(jq -r '.state.config_update' <<<"$current")"
  if [[ "$current_enabled" == "$enabled" && "$ready" == READY && "$update" == NOT_UPDATING ]]; then
    return 0
  fi
  payload="$(jq -c --argjson enabled "$enabled" '{
    served_entities: [.config.served_entities[] |
      {name, entity_name, workload_size, scale_to_zero_enabled:$enabled} +
      (if (.entity_version // "") != "" then {entity_version} else {} end)],
    traffic_config: {routes: [.config.traffic_config.routes[] |
      {served_entity_name, traffic_percentage}]}
  }' <<<"$current")"
  db serving-endpoints update-config "$endpoint" --json "$payload" >/dev/null
}

set_all_endpoint_scale_to_zero() {
  local enabled="$1" endpoint i failed=0
  local pids=() names=()
  for endpoint in "${ENDPOINTS[@]}"; do
    set_endpoint_scale_to_zero "$endpoint" "$enabled" &
    pids+=("$!")
    names+=("$endpoint")
  done
  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
      echo "Endpoint update failed: ${names[$i]}" >&2
      failed=1
    fi
  done
  [[ "$failed" == 0 ]]
}

wait_endpoint_ready() {
  local endpoint="$1" started body ready update message
  started=$(date +%s)
  while true; do
    body="$(endpoint_json "$endpoint")"
    ready="$(jq -r '.state.ready' <<<"$body")"
    update="$(jq -r '.state.config_update' <<<"$body")"
    if [[ "$ready" == READY && "$update" == NOT_UPDATING ]]; then return 0; fi
    if [[ "$update" == UPDATE_FAILED ]]; then
      message="$(jq -r '.pending_config.served_entities[0].state.deployment_state_message // .config.served_entities[0].state.deployment_state_message // "unknown"' <<<"$body")"
      die "$endpoint update failed: $message"
    fi
    if (( $(date +%s) - started > ENDPOINT_TIMEOUT_S )); then
      die "timed out waiting for $endpoint (ready=$ready update=$update)"
    fi
    sleep 10
  done
}

autoscaling_project_exists() {
  db postgres get-project "$LB_PROJECT_PATH" -o json >/dev/null 2>&1
}

autoscaling_endpoint_json() {
  db postgres get-endpoint "$LB_ENDPOINT_PATH" -o json
}

feature_store_state() {
  db feature-store get-online-store "$ONLINE_STORE" -o json 2>/dev/null | jq -r '.state // "UNKNOWN"'
}

ensure_lakebase_ready() {
  local body disabled state fs_state started
  autoscaling_project_exists || die "Autoscaling Lakebase project not found: $LB_PROJECT_PATH"
  body="$(autoscaling_endpoint_json)"
  disabled="$(jq -r 'if .status.disabled == null then "true" else (.status.disabled | tostring) end' <<<"$body")"
  if [[ "$disabled" == true ]]; then
    echo "      enabling $LB_ENDPOINT_PATH"
    db postgres update-endpoint "$LB_ENDPOINT_PATH" spec.disabled --json '{"spec":{"disabled":false}}' >/dev/null
  fi

  started=$(date +%s)
  while true; do
    body="$(autoscaling_endpoint_json)"
    disabled="$(jq -r 'if .status.disabled == null then "true" else (.status.disabled | tostring) end' <<<"$body")"
    state="$(jq -r '.status.current_state // "UNKNOWN"' <<<"$body")"
    fs_state="$(feature_store_state)"
    if [[ "$disabled" == false && "$state" == ACTIVE && "$fs_state" == AVAILABLE ]]; then
      echo "      $LB_ENDPOINT_PATH ACTIVE; Feature Store AVAILABLE"
      return 0
    fi
    [[ "$fs_state" == FAILED ]] && die "Feature Store $ONLINE_STORE is FAILED"
    if (( $(date +%s) - started > LAKEBASE_TIMEOUT_S )); then
      die "Lakebase not ready after ${LAKEBASE_TIMEOUT_S}s (state=$state disabled=$disabled feature_store=$fs_state)"
    fi
    sleep 10
  done
}

app_json() { db apps get "$APP" -o json; }

stop_app() {
  local body compute
  body="$(app_json)"
  compute="$(jq -r '.compute_status.state' <<<"$body")"
  [[ "$compute" == STOPPED ]] || db apps stop "$APP" >/dev/null
}

deploy_app() {
  local body source deployment compute started pending_id pending_state
  body="$(app_json)"
  APP_PREVIOUS_DEPLOYMENT_ID="$(jq -r '.active_deployment.deployment_id // ""' <<<"$body")"
  source="$(jq -r '.default_source_code_path // ""' <<<"$body")"
  [[ -n "$source" ]] || die "App $APP has no default_source_code_path; deploy it once from apps/recommender-ui"
  compute="$(jq -r '.compute_status.state' <<<"$body")"
  if [[ "$compute" != ACTIVE ]]; then
    echo "      starting App compute"
    db apps start "$APP" >/dev/null
    started=$(date +%s)
    until [[ "$(app_json | jq -r '.compute_status.state')" == ACTIVE ]]; do
      (( $(date +%s) - started <= APP_TIMEOUT_S )) || die "App compute did not become ACTIVE"
      sleep 10
    done
  fi
  body="$(app_json)"
  pending_id="$(jq -r '.pending_deployment.deployment_id // ""' <<<"$body")"
  pending_state="$(jq -r '.pending_deployment.status.state // ""' <<<"$body")"
  if [[ -n "$pending_id" && "$pending_state" != FAILED ]]; then
    APP_DEPLOYMENT_ID="$pending_id"
    echo "      adopting pending deployment $APP_DEPLOYMENT_ID"
    return 0
  fi
  # Starting compute alone can leave app_status=UNAVAILABLE. Restore the current workspace snapshot.
  deployment="$(db apps deploy "$APP" --source-code-path "$source" --no-wait -o json)"
  APP_DEPLOYMENT_ID="$(jq -r '.deployment_id // ""' <<<"$deployment")"
  [[ -n "$APP_DEPLOYMENT_ID" ]] || die "App deployment did not return a deployment_id"
  echo "      submitted deployment $APP_DEPLOYMENT_ID"
}

wait_app_ready() {
  local started body compute app_state deployment_state active_id pending_id signature last_signature="" token url
  started=$(date +%s)
  body="$(app_json)"
  url="$(jq -r '.url' <<<"$body")"
  token="$(db auth token -o json | jq -r '.access_token')"
  while true; do
    body="$(app_json)"
    compute="$(jq -r '.compute_status.state' <<<"$body")"
    app_state="$(jq -r '.app_status.state' <<<"$body")"
    active_id="$(jq -r '.active_deployment.deployment_id // ""' <<<"$body")"
    pending_id="$(jq -r '.pending_deployment.deployment_id // ""' <<<"$body")"
    deployment_state="$(jq -r --arg id "$APP_DEPLOYMENT_ID" --arg old "$APP_PREVIOUS_DEPLOYMENT_ID" '
      if .active_deployment.deployment_id == $id then .active_deployment.status.state
      elif .pending_deployment.deployment_id == $id then .pending_deployment.status.state
      elif .pending_deployment == null and .active_deployment.deployment_id != $old then .active_deployment.status.state
      else "UNKNOWN" end' <<<"$body")"
    signature="$deployment_state/$compute/$app_state/$active_id/$pending_id"
    if [[ "$signature" != "$last_signature" ]]; then
      echo "      deployment=$deployment_state compute=$compute app=$app_state"
      last_signature="$signature"
    fi
    [[ "$deployment_state" == FAILED ]] && die "App deployment $APP_DEPLOYMENT_ID failed"
    if [[ "$deployment_state" == SUCCEEDED && "$compute" == ACTIVE && "$app_state" == RUNNING ]]; then
      if curl -fsS -H "Authorization: Bearer $token" "$url/api/config" >/dev/null 2>&1; then
        echo "      $url RUNNING; /api/config reachable"
        return 0
      fi
    elif [[ "$app_state" == CRASHED ]]; then
      die "App $APP crashed during resume; inspect: databricks apps logs $APP --profile $PROFILE"
    fi
    if (( $(date +%s) - started > APP_TIMEOUT_S )); then
      die "App did not become reachable after ${APP_TIMEOUT_S}s (deployment=$deployment_state compute=$compute app=$app_state)"
    fi
    sleep 10
  done
}

status() {
  local body compute app_state lb disabled lb_state fs endpoint ep_body
  body="$(app_json)"
  compute="$(jq -r '.compute_status.state' <<<"$body")"
  app_state="$(jq -r '.app_status.state' <<<"$body")"
  echo "app        $APP compute=$compute app=$app_state"

  if autoscaling_project_exists; then
    lb="$(autoscaling_endpoint_json)"
    disabled="$(jq -r 'if .status.disabled == null then "true" else (.status.disabled | tostring) end' <<<"$lb")"
    lb_state="$(jq -r '.status.current_state // "UNKNOWN"' <<<"$lb")"
    fs="$(feature_store_state)"
    echo "lakebase   $LB_ENDPOINT_PATH state=$lb_state disabled=$disabled feature_store=$fs"
  else
    echo "lakebase   $LB_PROJECT_PATH NOT_FOUND"
  fi

  for endpoint in "${ENDPOINTS[@]}"; do
    ep_body="$(endpoint_json "$endpoint")"
    echo "endpoint   $endpoint ready=$(jq -r '.state.ready' <<<"$ep_body") update=$(jq -r '.state.config_update' <<<"$ep_body") scale_to_zero=$(jq -r '.config.served_entities[0].scale_to_zero_enabled' <<<"$ep_body")"
  done
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
    stop_app

    echo "[3/6] stopping ingestion + ${#RTM_PIPES[@]} RTM-to-Lakebase pipelines"
    for pipe in "$INGEST_PIPE" "${RTM_PIPES[@]}"; do
      [[ "$(pipe_state "$pipe")" == IDLE ]] || db pipelines stop "$pipe" --no-wait >/dev/null
    done
    for pipe in "$INGEST_PIPE" "${RTM_PIPES[@]}"; do wait_pipe "$pipe" IDLE; done

    echo "[4/6] pausing ${#BATCH_JOBS[@]} generated materialization jobs"
    for job in "${BATCH_JOBS[@]}"; do set_job_pause "$job" PAUSED; done

    echo "[5/6] enabling scale-to-zero on all serving endpoints"
    set_all_endpoint_scale_to_zero true

    echo "[6/6] leaving Autoscaling Lakebase enabled"
    echo "      Feature Store-managed production endpoints stay enabled; disabling them can strand restart."
    status
    ;;
  resume)
    echo "[1/6] ensuring Autoscaling Lakebase + Feature Store are available"
    ensure_lakebase_ready

    echo "[2/6] disabling scale-to-zero and waiting for all serving endpoints"
    set_all_endpoint_scale_to_zero false
    for endpoint in "${ENDPOINTS[@]}"; do wait_endpoint_ready "$endpoint"; done

    echo "[3/6] starting ingestion, then ${#RTM_PIPES[@]} RTM-to-Lakebase pipelines"
    [[ "$(pipe_state "$INGEST_PIPE")" == RUNNING ]] || db pipelines start-update "$INGEST_PIPE" >/dev/null
    wait_pipe "$INGEST_PIPE" RUNNING
    for pipe in "${RTM_PIPES[@]}"; do
      [[ "$(pipe_state "$pipe")" == RUNNING ]] || db pipelines start-update "$pipe" >/dev/null
    done
    for pipe in "${RTM_PIPES[@]}"; do wait_pipe "$pipe" RUNNING; done

    echo "[4/6] unpausing ${#BATCH_JOBS[@]} generated materialization jobs"
    for job in "${BATCH_JOBS[@]}"; do set_job_pause "$job" UNPAUSED; done

    echo "[5/6] enabling and starting the Kafka traffic producer"
    write_traffic_control true
    if [[ "$(db jobs list-runs --job-id "$TRAFFIC_JOB" --active-only -o json | jq 'length')" == 0 ]]; then
      db jobs run-now "$TRAFFIC_JOB" --no-wait >/dev/null
    fi

    echo "[6/6] restoring the Databricks App snapshot and verifying reachability"
    deploy_app
    wait_app_ready
    status
    ;;
  *)
    echo "usage: $0 status|pause|resume" >&2
    exit 2
    ;;
esac
