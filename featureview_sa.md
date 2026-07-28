# Feature Views NBO Accelerator — SA Onboarding & Handoff

**Purpose:** everything you need to continue or transition this project without re-learning the hard way.
Read this first. It captures the environment, the working solution, and — most importantly — the
**pitfalls that cost real time and their exact fixes.**

**Project:** Real-Time Next-Best-Offer (FSI) recommendation accelerator built on Databricks
**Feature Views**. One declarative feature definition powers point-in-time-correct offline training
*and* low-latency online serving. Owners: **Sixuan He** + **Tian Tan**.

**Last updated:** 2026-07-28

---

## 1. Environment — the exact, verified facts

| Thing | Value |
|---|---|
| **Workspace** | `https://fe-vm-ttan-vm.cloud.databricks.com` (FEVM, **us-west-2**) |
| **CLI profile** | `fe-vm-ttan-vm` (OAuth). Auth expires after long sessions → re-run `databricks auth login --profile fe-vm-ttan-vm` |
| **Catalog / schema** | `fins_industry_solutions.nbo` — **hyphen-free**, standard S3 storage, Tian-created, Sixuan has ALL_PRIVILEGES |
| **Feature-engineering SDK** | `databricks-feature-engineering>=0.16.0` |
| **Compute** | **Serverless** (`environment_key` + `client:"4"`), latest env. FV APIs run on serverless — do NOT need classic clusters |
| **SQL warehouse** | `2a0b73493fe19b04` ([dev] Payment Analysis Warehouse, running) — use for SQL checks |
| **Kafka/MSK** | UC connection **`msk_kafka`** (AWS MSK, IAM auth, public-TLS **:9198**, us-west-2) |
| **Kafka auth** | UC **service credential `msk_kafka`** (IAM role `ttan-fv-databricks-msk-access`, Tian owns; Sixuan has ACCESS not MANAGE) |
| **Online store** | Lakebase `nbo-online-store` (CU_1, AVAILABLE) |
| **GitHub repo** | `github.com/tiantan32/feature-views-nbo-accelerator` (private). Branches `main` + `sixuan/feature-views-core`. Commit identity `Hehehe421` (Sixuan personal). This CLI session's `gh` is `sixuan-he_data` (has WRITE). |
| **Workspace notebook path** | `/Workspace/Users/sixuan.he@databricks.com/nbo_accelerator/` |

**MCP note:** the `execute_sql`/`run_python` MCP tools point at Sixuan's *other* workspace
`fe-vm-vm-summer` — NOT ttan. Always run on ttan via the **CLI** (`databricks ... -p fe-vm-ttan-vm`)
or the Statement Execution API against warehouse `2a0b73493fe19b04`.

---

## 2. Repo structure (two-part accelerator)

```
notebooks/
  part1_feature_views/          # Feature Views Fundamentals — the fast win (batch, no VS, no streaming)
    00_setup, 01_generate_data, 02_define_feature_views, 03_materialize_features,
    04_train_ranker, 05_deploy_ranking_endpoint (online-lookup, route-optimized), 06_latency_benchmark
  part2_realtime_two_stage/      # Real-Time Streaming Recommender (streaming feature feeds the ranker)
    07_kafka_topic_and_producer, 08_streaming_feature_views,
    09_realtime_serving, 10_latency_and_freshness
apps/recommender-app/            # Streamlit demo + live latency meter
dashboards/nbo_dashboard.json    # AI/BI dashboard
resources/                       # Asset Bundle: part1_job.yml, part2_job.yml, app_and_dashboard.yml
RESTRUCTURE_PLAN.md              # the Part 1 / Part 2 split rationale (reviewed)
```

**Design decision (reviewed):** Vector Search was **dropped**. For a ~40-offer catalog, retrieval adds
~90ms for no benefit and no accuracy gain (the ranker is the accuracy engine). Rank-all the catalog;
personalization lives entirely in the ranker. VS is a documented catalog-scale extension, out of scope.

---

## 3. What WORKS end-to-end (verified on the workspace)

- ✅ **Part 1 batch** — synthetic data, batch feature views (Sliding/Tumbling/ColumnSelection),
  materialize to offline Delta + online Lakebase, point-in-time training (LightGBM, **val AUC ~0.70**),
  online-lookup serving.
- ✅ **Route-optimized serving** — `nbo-ranker-online` / `nbo-ranker-realtime`. Feature-read + rank-all
  measured at **~38ms p50 in-region** (~118ms from laptop incl. ~80ms cross-region WAN).
- ✅ **True online feature lookup** — request carries only `{customer_id + offer fields}`; endpoint
  fetches the 5 (or 6) customer features from Lakebase by `customer_id`.
- ✅ **Streaming ingestion** — Kafka(MSK) → `session_events_ingest` Delta table works (250K+ rows,
  fresh events land within seconds).
- ✅ **Streaming feature in TRAINING** — `cust_clicks_10m` RollingWindow feature reads its ingest
  table for point-in-time joins; 0% null; ranker trains on it.

## 4. What does NOT work yet (the one open item)

- ❌ **Streaming feature ONLINE materialization** — `materialize_features(StreamingMode())` registers
  the feature (`is_online=True`, `ACTIVE`, `mode=STREAMING_MODE_TYPE_RTM`) but the **online serving
  table (`nbo_stream_serving_*`) stays at 0 rows**, `last_materialization_time=None`, and **no
  materialization compute pipeline is ever provisioned**. So the endpoint can't serve a *live*
  streaming feature value → **event→serving freshness (~200ms p99 story) can't be measured.**

  **Everything else was ruled out** (topic pollution, event format, offsets, stale window, hyphenated
  catalog, validation errors, online-store config, feature definition — all fixed/verified). The online
  store itself is proven working because **batch** features write to it (100K rows). This is isolated to
  the RTM online-materialization step not producing on this workspace.

  **Next action:** escalate to the Feature Store / Eng team with the crisp repro below, OR try MBM mode
  (see Pitfall #9). Do NOT keep brute-forcing — it's a preview-serving behavior, not a code bug on our side.

---

## 5. PITFALLS & SOLUTIONS (read this — each one cost hours)

### Pitfall 1 — Code hidden under `%md` cells (CRITICAL, silent no-op)
**Symptom:** a notebook run reports SUCCESS but does nothing; exported source shows all `# MAGIC %md`.
**Cause:** in a Databricks source notebook, a cell whose **first line is `# MAGIC %md`** is treated as
an *entirely markdown* cell. If you write a `%md` heading and then real code in the **same cell**, the
code never executes.
**Solution:** every `%md` heading must be its **own cell**, with `# COMMAND ----------` separating it
from the following code cell. Verify with: export the notebook and confirm no code lines follow `# MAGIC`
lines within one cell. (This bug affected all 11 notebooks; now fixed.)

### Pitfall 2 — `%pip install` + `dbutils.library.restartPython()` placement
**Symptom:** later cells lose imports/variables; flaky state.
**Solution:** `%pip install ...` + `restartPython()` must be the **first executable cell** (restart wipes
all prior Python state). It's fine to have a markdown title cell before it (markdown isn't Python state).

### Pitfall 3 — Producer writing to the WRONG Kafka topic (sticky widgets)
**Symptom:** producer job "succeeds," but the stream's ingest table never gets new data; topic
watermark for your target topic never grows.
**Cause:** `dbutils.widgets.text("topic", "old-default")` — a pre-existing widget value is **sticky** and
job `base_parameters` may not override it, so events go to the old topic while you check the new one.
**Solution:** verify the actual topic by checking Kafka **high-watermark offsets** (not just "job
succeeded"). For deterministic runs, hardcode/validate the topic or delete stale widgets. Confirm produce
by watermark delta, not job status.

### Pitfall 4 — Kafka `startingOffsets=latest`: produce AFTER the stream exists
**Symptom:** ingest table stays empty though topic has millions of messages.
**Cause:** the managed ingestion pipeline (created by `create_stream`) reads from the **latest offset** —
it only captures messages produced *after* it's RUNNING. Pre-existing events are skipped.
**Solution:** order is **create_stream (08) first → then produce (07)**. For historical coverage, use
`StreamBackfillSource` in `IngestionConfig`. When testing, produce a *fresh* batch while the pipeline is
confirmed RUNNING.

### Pitfall 5 — Streaming stream validation `OverflowError: date value out of range`
**Symptom:** the ingestion job's `validation` task fails INTERNAL_ERROR → `fs_kafka_ingestion` skipped →
nothing ingests.
**Cause:** the topic contained **mixed `event_time` formats** from earlier debug runs (epoch-millis
integers AND ISO strings). The stream's schema validation parses a 13-digit int as a date-time → overflow.
**Solution:** keep ONE consistent event format. `event_time` must be an **ISO-8601 string**
(`yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`) and the stream JSON Schema must declare it as
`{"type":"string","format":"date-time"}`. If a topic is polluted, **delete and recreate the topic clean.**

### Pitfall 6 — Stream schema is JSON Schema, NOT Spark StructType JSON
**Symptom:** `create_stream` → `Invalid payload_schema: object type schema must have a 'properties' field`.
**Cause:** passing Spark's `StructType.json()` format.
**Solution:** use **JSON Schema**: `{"type":"object","properties":{"field":{"type":"string"}, ...}}`.

### Pitfall 7 — Hyphenated catalog breaks the streaming training read
**Symptom:** `create_training_set` with a streaming feature → `INVALID_IDENTIFIER: fins-industry-solutions
must be back quoted`.
**Cause:** the FE library's `StreamSource.load_df()` passes the raw 3-level ingest table name to
`spark.read.table()` **without backticking** (`data_source.py:1540`) — the one read path that doesn't
sanitize. Batch `DeltaTableSource` uses `_quoted_full_name()`, which is why batch works.
**Solution:** use a **hyphen-free catalog** (`fins_industry_solutions`, not `fins-industry-solutions`).
This is why the whole project migrated to the underscore catalog. (Low-sev library bug; hyphen-free
sidesteps it via a supported config, no monkeypatch.)

### Pitfall 8 — `fe.log_model` streaming/online-lookup serving quirks
- `fe.log_model` with the sklearn flavor defaults to **skops** serialization, which rejects LightGBM
  types → `UntrustedTypesFoundException`. **Solution:** pass
  `skops_trusted_types=["collections.OrderedDict","lightgbm.basic.Booster",
  "lightgbm.sklearn.LGBMClassifier","sklearn.compose._column_transformer._RemainderColsList"]`
  (or use `mlflow.sklearn.log_model(..., serialization_format="cloudpickle")` for the plain-model path).
- The serving wrapper passes the raw model **all** training-set columns (online-looked-up features +
  request-time columns). To feed **request-time offer columns** alongside online features, declare them
  via **`RequestSource`** + a passthrough `ColumnSelection` feature per column — and the feature **name
  must equal the column name** (no prefix). Make the sklearn `ColumnTransformer` `remainder="drop"` so
  extra columns the FS layer appends (`customer_id`, `ts`, `record_id`) never reach the model.
- ColumnSelection PIT key: add `updated_at = ts` (and for streaming, `event_time = ts`) to the labels df
  or `create_training_set` errors on the missing timestamp key.

### Pitfall 9 — `StreamingMode()` mode is server-defaulted to RTM; online table stays empty
**Symptom:** `materialize_features(StreamingMode())` succeeds, feature is `ACTIVE`, but the online serving
table is 0 rows forever, `last_materialization_time=None`, no materialization pipeline provisioned.
**Findings:** the client `StreamingMode()` exposes only `pipeline_schedule_state` — **no way to pick
MBM vs RTM**; its `_to_sdk()` sends an empty spec, so the server defaults to **`STREAMING_MODE_TYPE_RTM`**.
The docs describe a continuous pipeline → Lakebase (~200ms), which matches **MBM**, not RTM.
**Status: UNRESOLVED.** Candidate next steps (in order):
  1. Escalate to Feature Store/Eng team with the repro (recommended — likely a preview enablement/behavior).
  2. Try forcing **MBM**: the SDK-level `StreamingMode(mode=StreamingModeStreamingModeType.STREAMING_MODE_TYPE_MBM)`
     accepts a mode arg even though the client wrapper doesn't wire it. (Reaching past the public wrapper —
     verify it's supported before relying on it.)
  3. Confirm with the team whether RTM online serving requires a workspace enablement flag.

### Pitfall 10 — Managed pipelines/jobs auto-start but can stall; inspect via FE API
- `create_stream` auto-starts a **managed ingestion job** (`ingestion_job_id`) that runs a pipeline
  (`ingestion_pipeline_id`). It usually shows RUNNING, but has been observed **stalled** (RUNNING yet not
  consuming new offsets). If ingest freezes, check the ingestion **job** runs (it can fail with
  `INTERNAL_ERROR` in a `validation` task — see Pitfall 5) and `start-update` the pipeline if IDLE.
- **For ANY Feature-View object (stream, feature, materialized feature), use the FE API**
  (`fe.get_stream`, `fe.list_materialized_features(feature_name=...)`) — the plain workspace
  jobs/pipelines APIs won't reliably surface FV-managed objects. `list_materialized_features` **requires**
  `feature_name=` (no bare call).

### Pitfall 11 — Route-optimized endpoints can't be queried the normal way
**Symptom:** `w.serving_endpoints.query(...)` → `400: This is a route-optimized endpoint, use the
route-optimized URL`; PAT → 401 "malformed"; plain OAuth → 401 "missing authorization details".
**Solution:** use `w.serving_endpoints_data_plane.query(...)` — it resolves the data-plane URL and mints
the downscoped OAuth token. **Requires OAuth creds**, which the **serverless job runtime does NOT have**
→ so benchmark from an OAuth-authed client (laptop with the CLI profile) or the app's service principal.
`route_optimized=True` is **immutable** at create → delete + recreate to change it. Set
`scale_to_zero_enabled=False` for the demo to avoid cold-start p99 tails.

### Pitfall 12 — Endpoint transient `No available connection in 2s`
Right after a config rollout the endpoint's Lakebase connection can 401/error once during warmup; it
succeeds on retry. Warm the endpoint before benchmarking.

### Pitfall 13 — CronSchedule aggregation materialization is ASYNC (don't misdiagnose)
Batch aggregation online/offline tables (`nbo_off_*`, `nbo_on_*`) provision via serverless pipelines that
take **5–10+ min** to first-backfill. `last_materialization_time=None` early on is normal — wait, don't
conclude it's broken. (I wrongly called this an "ES ticket" earlier — it was just async latency.)

### Pitfall 14 — Don't pollute the workspace folder with scratch notebooks
I created 60+ throwaway check notebooks in the accelerator folder — a mess. **Keep the accelerator folder
to canonical notebooks only.** For verification, prefer the notebook's own printed output or SQL via the
warehouse; if you must create throwaway notebooks, use a separate temp path and delete them.

### Pitfall 15 — Non-idempotent notebooks fail on re-run
`create_feature` / `materialize_features` throw `AlreadyExists` on a second run.
**Solution:** guard with get-or-create (`try: fe.get_feature(...) except: fe.create_feature(...)`) and
skip materialize if already materialized. (Fixed in 08; apply the same pattern elsewhere.)

### Pitfall 16 — GitHub / auth logistics
- Repo is under **`tiantan32`** (personal). Sixuan's personal GH is **`Hehehe421`**; this CLI session's
  `gh` is `sixuan-he_data`. Commits are authored as `Hehehe421`; the session has WRITE and can push.
- MSK topic **auto-create is disabled** → pre-create topics via temp UC service creds + Kafka
  `AdminClient` (Spark connector can't do admin ops). See notebook 07 cell 1.

---

## 6. Key measured results (honest, with scope)

| Metric | Value | Scope |
|---|---|---|
| Offline training AUC | ~0.70 | signal-injected synthetic labels; batch + streaming features |
| Serving: feature-read + rank-all (40 offers) | **~38ms p50 in-region** | route-optimized endpoint, laptop ~118ms incl. WAN |
| Batch online feature lookup | works | `nbo_on_*` populated (100K rows) |
| Streaming ingest freshness | seconds | Kafka→ingest table, verified |
| **Streaming feature ONLINE serving** | **NOT working** | online table 0 rows (Pitfall #9) |
| Serving p95 (earlier v1, non-route-optimized + query_text) | ~235ms | before optimization |

**Latency optimization levers that mattered:** (1) route-optimize the endpoint + scale-to-zero off;
(2) if using Vector Search, embed client-side + `query_vector` (drops ~50ms managed-embedding hop) —
though VS is now out of scope.

---

## 7. How to run (canonical, on ttan serverless)

Run notebooks in order via `databricks jobs submit -p fe-vm-ttan-vm` with a serverless environment
(`{"spec":{"client":"4","dependencies":["databricks-feature-engineering>=0.16.0", ...]}}`), or deploy the
Asset Bundle (`databricks bundle deploy`) and run the Part 1 / Part 2 jobs.

**Part 1 (works):** 00 → 01 → 02 → 03 → 04 → 05 → 06.
**Part 2 (streaming):** **08 first** (create stream — starts ingestion at latest offset) → **07** (produce
events, they land in ingest) → 09 (re-log ranker with `cust_clicks_10m`) → 10 (latency + freshness).
> Freshness in 10 is blocked until the RTM online-materialization issue (Pitfall #9) is resolved.

Auth: `databricks auth login --profile fe-vm-ttan-vm` (re-run if the session expired).

---

## 8. Immediate next steps for whoever continues

1. **Resolve streaming online materialization (Pitfall #9)** — escalate to Feature Store/Eng with the
   repro, or test MBM mode. This unblocks the freshness benchmark and the full Part 2 story.
2. **Run 09 + 10** once online serving works, capture real event→serving freshness (~200ms target).
3. **Wire the app** (`apps/recommender-app`) to `nbo-ranker-realtime` and verify the live demo + latency meter.
4. **Add inline verification prints** to canonical notebooks (topic count, ingest rows, online rows) so
   they're self-verifying when run in the UI.
5. **Polish** README/dashboards, then submit to the industry-solutions repo.
</content>
