# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Serving & Freshness Benchmark
# MAGIC Both latency numbers for the Feature Views path, measured live in one place:
# MAGIC
# MAGIC 1. **Serving latency** — request → ranked response. The request carries only
# MAGIC    `{customer_id, offer_id, offer attrs}`; the route-optimized endpoint fetches the customer
# MAGIC    features from the online store by `customer_id` and scores the full offer catalog. No
# MAGIC    retrieval stage.
# MAGIC 2. **Feature freshness** — event → online availability, measured **per event**: each probe
# MAGIC    writes **one Kafka record** for its own `customer_id`, then reads that key's commit from the
# MAGIC    online store. `commit_time − event_time` spans the whole path — Kafka write → ingestion →
# MAGIC    aggregation → online-store write.
# MAGIC
# MAGIC Section 1 always runs. Section 2 runs when `allow_streaming_online=true` (it needs the Part 2
# MAGIC streaming path: the MSK connection and a RUNNING ingestion pipeline).
# MAGIC
# MAGIC Results are written to the `results_table` widget so the AI/BI dashboard can read them:
# MAGIC Part 1 → `part1_latency_results`, Part 2 → `part2_latency_results`.

# COMMAND ----------
# MAGIC %pip install "databricks-sdk>=0.30" "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-online")
dbutils.widgets.text("results_table", "part1_latency_results")
dbutils.widgets.text("n_requests", "100")
# Freshness (section 2) — needs the Part 2 streaming path.
dbutils.widgets.dropdown("allow_streaming_online", "false", ["false", "true"])
dbutils.widgets.text("kafka_connection", "msk_kafka")
dbutils.widgets.text("service_credential", "msk_kafka")
dbutils.widgets.text("topic", "nbo-session-events")
dbutils.widgets.text("n_markers", "200")
dbutils.widgets.text("spacing_sec", "2")        # one event per micro-batch
dbutils.widgets.text("warmup_discard", "5")     # standard benchmark warm-up, excluded from stats
dbutils.widgets.text("settle_timeout_sec", "600")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")
results_table = dbutils.widgets.get("results_table").strip()
N = int(dbutils.widgets.get("n_requests"))
RUN_FRESHNESS = dbutils.widgets.get("allow_streaming_online") == "true"
conn_name = dbutils.widgets.get("kafka_connection")
service_credential = dbutils.widgets.get("service_credential")
topic = dbutils.widgets.get("topic")
N_MARKERS = int(dbutils.widgets.get("n_markers"))
SPACING_SEC = float(dbutils.widgets.get("spacing_sec"))
WARMUP_DISCARD = int(dbutils.widgets.get("warmup_discard"))
SETTLE_TIMEOUT_SEC = int(dbutils.widgets.get("settle_timeout_sec"))

import time
import json
import statistics
import requests
import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient

w = WorkspaceClient()
fe = FeatureEngineeringClient()

def pct(a, p):
    if not a:
        return None
    xs = sorted(a)
    # nearest-rank: the ceil(p/100 * n)-th value (1-based) → 0-based index below.
    idx = max(0, (p * len(xs) + 99) // 100 - 1)
    return round(xs[min(idx, len(xs) - 1)], 1)

print(f"catalog={catalog}  schema={schema}  endpoint={ranker_endpoint}  results_table={results_table}")
print(f"freshness section: {'ENABLED' if RUN_FRESHNESS else 'skipped (allow_streaming_online=false)'}")

# COMMAND ----------
# MAGIC %md ## 1 · Serving latency — online feature read + rank-all
# MAGIC Candidate set is the full offer catalog; customer features are looked up online by key.
# MAGIC
# MAGIC **Auth (route-optimized endpoints):** these require an OAuth token **downscoped to the
# MAGIC endpoint**. The notebook runtime identity cannot mint one (`serving_endpoints_data_plane.query()`
# MAGIC → `OAuth tokens are not available for runtime authentication`; PATs unsupported), so this queries
# MAGIC via a **service principal** using `client_credentials` + `authorization_details`
# MAGIC (`query_inference_endpoint`). The SP id/secret live in the `nbo` secret scope and the SP must have
# MAGIC CAN_QUERY on the endpoint. **Run on SERVERLESS compute** — the workspace enforces an IP access
# MAGIC list and only serverless egress is allowlisted; a classic cluster gets a 403 on the token request.
# MAGIC https://docs.databricks.com/aws/en/machine-learning/model-serving/query-route-optimization
# COMMAND ----------
offers = [r.asDict() for r in spark.table(f"`{catalog}`.{schema}.offers").collect()]
customers = [r.asDict() for r in
             spark.table(f"`{catalog}`.{schema}.customers").select("customer_id").limit(200).collect()]
print(f"{len(customers)} customers × {len(offers)} offers")

ep   = w.serving_endpoints.get(name=ranker_endpoint)
url  = ep.data_plane_info.query_info.endpoint_url   # full https://….../invocations
EPID = ep.id                                        # alphanumeric endpoint id
host = w.config.host
CID  = dbutils.secrets.get("nbo", "sp_client_id")
CSEC = dbutils.secrets.get("nbo", "sp_client_secret")

authz = json.dumps([{"type": "workspace_permission", "object_type": "serving-endpoints",
                     "object_path": f"/serving-endpoints/{EPID}",
                     "actions": ["query_inference_endpoint"]}])
resp = requests.post(f"{host}/oidc/v1/token", auth=(CID, CSEC),
                     data={"grant_type": "client_credentials", "scope": "all-apis",
                           "authorization_details": authz})
if resp.status_code != 200:
    raise RuntimeError(
        f"SP token request failed: {resp.status_code} — {resp.text[:300]}\n"
        "A 403 here means this compute's egress IP is not on the workspace IP access list. "
        "Run this notebook on SERVERLESS compute (its egress NAT is allowlisted), not a classic "
        "all-purpose cluster."
    )
tok = resp.json()["access_token"]

def rank(customer_id):
    # Request carries ONLY customer_id + offer fields; the endpoint fetches the customer features online.
    recs = [{
        "customer_id": customer_id,
        "offer_id": o["offer_id"],
        "product_category": o["product_category"],
        "base_reward": float(o["base_reward"]),
        "tier_requirement": int(o["tier_requirement"]),
    } for o in offers]
    r = requests.post(url, headers={"Authorization": f"Bearer {tok}"},
                      json={"dataframe_records": recs})
    r.raise_for_status()
    return r.json()["predictions"]

for i in range(12):  # warm
    rank(customers[i % len(customers)]["customer_id"])

lat = []
for i in range(N):
    t0 = time.perf_counter()
    rank(customers[i % len(customers)]["customer_id"])
    lat.append((time.perf_counter() - t0) * 1000)

serving = {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99),
           "mean": round(statistics.mean(lat), 1)}
print("MEASURED — serving latency (online feature read + rank-all)")
print(f"  endpoint={ranker_endpoint}  requests={N}  offers scored per request={len(offers)}")
print(f"  p50={serving['p50']}ms  p95={serving['p95']}ms  p99={serving['p99']}ms  "
      f"mean={serving['mean']}ms  min={round(min(lat),1)}ms  max={round(max(lat),1)}ms")
print("  measured from this driver; subtract cross-region WAN RTT for the in-region number.")

# COMMAND ----------
# MAGIC %md ## 2 · Feature freshness — event → online availability
# MAGIC One Kafka record per write, a fresh `customer_id` per event, so each measurement is a clean
# MAGIC single-event path. Needs the streaming materialization (notebook 07) RUNNING; endpoint-free.
# COMMAND ----------
probe = None
if not RUN_FRESHNESS:
    print("Freshness section skipped: allow_streaming_online=false. Set it to true (with the MSK "
          "connection configured and the notebook-07 ingestion pipeline RUNNING) to measure freshness.")
else:
    conn = w.connections.get(name=conn_name)
    BOOTSTRAP = dict(conn.options or {})["bootstrap_servers"]
    # Bootstrap + service credential only; adding kafka.security.protocol / kafka.sasl.mechanism
    # alongside a service credential is rejected as conflicting.
    KAFKA_OPTS = {"kafka.bootstrap.servers": BOOTSTRAP, "databricks.serviceCredential": service_credential}

    def emit_single_event(customer_id):
        """Write exactly ONE Kafka record for customer_id (single-row Spark Kafka write)."""
        spark.range(1).select(
            F.lit(customer_id).alias("key"),
            F.to_json(F.struct(
                F.concat(F.lit("mk_"), F.lit(customer_id)).alias("event_id"),
                F.lit(customer_id).alias("customer_id"),
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_time"),
                F.lit("marker").alias("event_type"),
                F.lit("credit_card").alias("product_category"),
                F.lit(1000).alias("dwell_ms"),
                F.lit("web").alias("device"),
            )).alias("value"),
        ).write.format("kafka").options(**KAFKA_OPTS).option("topic", topic).save()

    online_tbl = None
    for m in fe.list_materialized_features(feature_name=f"{catalog}.{schema}.cust_clicks_10m"):
        if m.is_online:
            online_tbl = m.table_name
            break

    if not online_tbl:
        print("Streaming online table for cust_clicks_10m not found — run notebook 07 first.")
    else:
        run_tag = f"probe{int(time.time())}"
        ids = []
        for i in range(N_MARKERS):
            cid = f"{run_tag}_{i}"          # fresh key: the producer only emits cust_<n>
            emit_single_event(cid)
            ids.append(cid)
            if SPACING_SEC:
                time.sleep(SPACING_SEC)
        print(f"emitted {len(ids)} single-event markers (tag={run_tag})")

        idlist = "','".join(ids)
        deadline = time.time() + SETTLE_TIMEOUT_SEC
        landed = 0
        while time.time() < deadline:
            landed = spark.sql(
                f"SELECT COUNT(DISTINCT customer_id) AS c FROM {online_tbl} "
                f"WHERE customer_id IN ('{idlist}')"
            ).first()[0]
            if landed >= len(ids):
                break
            time.sleep(10)
        print(f"landed {landed}/{len(ids)} markers in the online store")

        probe = spark.sql(f"""
            WITH per_key AS (
              SELECT customer_id,
                     CAST(split(customer_id, '_')[1] AS INT) AS emit_index,
                     MIN(unix_millis(to_timestamp(commit_time))
                         - unix_millis(to_timestamp(event_time))) AS lag_ms
              FROM {online_tbl}
              WHERE customer_id IN ('{idlist}')
                AND event_time IS NOT NULL AND is_timer = false
              GROUP BY customer_id
            )
            SELECT COUNT(*) AS n,
                   percentile_approx(lag_ms, 0.50) AS p50,
                   percentile_approx(lag_ms, 0.95) AS p95,
                   percentile_approx(lag_ms, 0.99) AS p99,
                   MIN(lag_ms) AS min_ms, MAX(lag_ms) AS max_ms,
                   SUM(CASE WHEN lag_ms < 200 THEN 1 ELSE 0 END) AS under_200ms
            FROM per_key
            WHERE emit_index >= {WARMUP_DISCARD}
        """).collect()[0].asDict()

        if probe["n"]:
            pctu = 100.0 * probe["under_200ms"] / probe["n"]
            print("MEASURED — feature freshness (event → online store), per single event")
            print(f"  events={probe['n']} (warm-up {WARMUP_DISCARD} excluded)")
            print(f"  p50={probe['p50']}ms  p95={probe['p95']}ms  p99={probe['p99']}ms  "
                  f"min={probe['min_ms']}ms  max={probe['max_ms']}ms")
            print(f"  under 200ms: {probe['under_200ms']}/{probe['n']} ({pctu:.1f}%)")
        else:
            probe = None
            print("No probe markers landed — check that the notebook-07 ingestion pipeline is RUNNING.")

# COMMAND ----------
# MAGIC %md ## Persist results to Delta for the dashboard
# COMMAND ----------
rows = [{"stage": "feature_read_and_rank",
         "p50": serving["p50"], "p95": serving["p95"], "p99": serving["p99"]}]
if probe:
    rows.append({"stage": "freshness_event_to_online",
                 "p50": float(probe["p50"]), "p95": float(probe["p95"]), "p99": float(probe["p99"])})

(spark.createDataFrame(pd.DataFrame(rows))
      .write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.{results_table}"))
print(f"Wrote {catalog}.{schema}.{results_table} ({len(rows)} rows)")

# Keep both dashboard tables resolvable even on a Part-1-only deploy, where the Part 2 table is
# never written. Creating it empty lets the dashboard's real-time section render.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.{schema}.part2_latency_results
    (stage STRING, p50 DOUBLE, p95 DOUBLE, p99 DOUBLE)
""")

for r in rows:
    print(f"  {r['stage']:28} p50={r['p50']}ms  p95={r['p95']}ms  p99={r['p99']}ms")
