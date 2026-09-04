# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Freshness Benchmark — Event→Online Freshness + Part 2 Serving Latency
# MAGIC The two numbers that prove the real-time streaming path, measured live and reported
# MAGIC separately:
# MAGIC 1. **Serving latency** — request → ranked response (online feature-read + rank-all,
# MAGIC    **no retrieval**). The clean "feature read + rank" story.
# MAGIC 2. **Freshness** — event → online-availability for the streaming `cust_clicks_10m`
# MAGIC    feature (the Feature Views launch reference is ~200ms p99). **Measured live** here.
# MAGIC
# MAGIC No Vector Search anywhere — personalization is entirely in the ranker, which now consumes
# MAGIC the live `cust_clicks_10m` streaming feature (notebook 09).

# COMMAND ----------
# MAGIC %pip install "databricks-sdk>=0.30" "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-realtime")
dbutils.widgets.text("kafka_connection", "msk_kafka")
dbutils.widgets.text("service_credential", "msk_kafka")
dbutils.widgets.text("topic", "nbo-session-events")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")
conn_name = dbutils.widgets.get("kafka_connection")
service_credential = dbutils.widgets.get("service_credential")
topic = dbutils.widgets.get("topic")

# --- Preflight gate (same switch as notebook 07) ---
# This benchmark queries the realtime endpoint and measures streaming freshness over MSK, both of
# which depend on Part 2's streaming path (notebooks 07/08/09). Part 2 is gated OFF by default, so
# gate here to skip cleanly instead of failing on a missing endpoint / feature. Opt in with
# allow_streaming_online=true once the MSK infra is configured.
dbutils.widgets.dropdown("allow_streaming_online", "false", ["false", "true"])
if dbutils.widgets.get("allow_streaming_online") != "true":
    msg = ("Streaming latency & freshness benchmark skipped: Part 2 streaming is gated "
           "(allow_streaming_online=false). Part 1's feature-serving benchmark covers the "
           "feature-read + rank story. Set allow_streaming_online=true (with working MSK infra) to "
           "run Part 2, then re-run.")
    print(msg); dbutils.notebook.exit(msg)

import time
import json
import statistics
import requests
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def pct(a, p):
    if not a:
        return None
    xs = sorted(a)
    # nearest-rank: the ceil(p/100 * n)-th value (1-based) → 0-based index below. int(n*p/100)
    # was off by one (e.g. p99 over N=100 returned the max, not the 99th value).
    idx = max(0, (p * len(xs) + 99) // 100 - 1)
    return round(xs[min(idx, len(xs) - 1)], 1)

# COMMAND ----------
# MAGIC %md ## 1 · Serving latency — online feature-read + rank-all (no retrieval)
# MAGIC Request carries only `customer_id` + offer fields; the endpoint looks up all 6 customer
# MAGIC features (incl. streaming `cust_clicks_10m`) online and scores the full offer catalog.
# MAGIC
# MAGIC **Auth (route-optimized endpoints):** these require an OAuth token **downscoped to the
# MAGIC endpoint**. The notebook runtime identity cannot mint one (`serving_endpoints_data_plane.query()`
# MAGIC → `OAuth tokens are not available for runtime authentication`; PATs unsupported), so this queries
# MAGIC via a **service principal** using `client_credentials` + `authorization_details`
# MAGIC (`query_inference_endpoint`). The SP id/secret live in the `nbo` secret scope and the SP must have
# MAGIC CAN_QUERY on the endpoint. **Run on SERVERLESS compute** — the workspace enforces an IP access
# MAGIC list and only serverless egress is allowlisted; a classic cluster gets a 403 on the token request.
# MAGIC https://docs.databricks.com/aws/en/machine-learning/model-serving/query-route-optimization
# MAGIC Measured (incl. cross-region WAN): **feature-read + rank p50 ≈ 33ms / p95 ≈ 41ms / p99 ≈ 49ms**.
# COMMAND ----------
offers = [r.asDict() for r in spark.table(f"`{catalog}`.{schema}.offers").collect()]
customers = [r.asDict() for r in
             spark.table(f"`{catalog}`.{schema}.customers").select("customer_id").limit(200).collect()]
print(f"{len(customers)} customers × {len(offers)} offers")

# Mint a SERVICE-PRINCIPAL token downscoped to this endpoint (client_credentials + authorization_details).
# The notebook runtime identity can't mint one; the SP can. SP id/secret come from the `nbo` secret scope.
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

serving = None
for i in range(12):  # warm
    rank(customers[i % len(customers)]["customer_id"])

N = 100
lat = []
for i in range(N):
    t0 = time.perf_counter()
    rank(customers[i % len(customers)]["customer_id"])
    lat.append((time.perf_counter() - t0) * 1000)

serving = {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99),
           "mean": round(statistics.mean(lat), 1)}
print(f"feature-read + rank ({len(offers)} offers): "
      f"p50={serving['p50']}  p95={serving['p95']}  p99={serving['p99']}ms")
print("NOTE: measured from this driver; subtract cross-region WAN RTT for the in-region number.")

# COMMAND ----------
# MAGIC %md ## 2 · Freshness — event → online availability (live-measured)
# MAGIC Emit a marker event to Kafka for a probe customer, then poll the online-store feature
# MAGIC value until `cust_clicks_10m` reflects it, timing the gap. Requires the streaming
# MAGIC materialization (notebook 07) to be running and continuously consuming the topic.
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()
conn = w.connections.get(name=conn_name)
BOOTSTRAP = dict(conn.options or {})["bootstrap_servers"]
# Match nb08's working producer options: bootstrap + service credential only. Adding
# kafka.security.protocol / kafka.sasl.mechanism alongside a service credential is rejected as
# conflicting (see nb08), so they are intentionally omitted here.
KAFKA_OPTS = {"kafka.bootstrap.servers": BOOTSTRAP, "databricks.serviceCredential": service_credential}

def emit_marker(customer_id, n=5):
    """Produce n marker events for customer_id to the topic (Spark Kafka write)."""
    rows = spark.range(n).select(
        F.lit(customer_id).alias("key"),
        F.to_json(F.struct(
            F.concat(F.lit("mark_"), (F.lit(int(time.time() * 1000)) + F.col("id")).cast("string")).alias("event_id"),
            F.lit(customer_id).alias("customer_id"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_time"),
            F.lit("marker").alias("event_type"),
            F.lit("credit_card").alias("product_category"),
            F.lit(1000).alias("dwell_ms"),
            F.lit("web").alias("device"),
        )).alias("value"),
    )
    rows.write.format("kafka").options(**KAFKA_OPTS).option("topic", topic).save()

# Freshness probe loop. Enable when the streaming pipeline (07) + producer (08 continuous) are live.
RUN_FRESHNESS = False  # set True once the streaming pipeline is running
freshness_ms = []
if RUN_FRESHNESS:
    probe_customers = [c["customer_id"] for c in customers[:20]]
    for cid in probe_customers:
        before = rank(cid)  # baseline (forces an online read for this customer)
        t0 = time.perf_counter()
        emit_marker(cid, n=5)
        # poll the ranker (which reads cust_clicks_10m online) until the score shifts
        for _ in range(60):  # up to ~6s
            time.sleep(0.1)
            if rank(cid) != before:
                freshness_ms.append((time.perf_counter() - t0) * 1000)
                break
    if freshness_ms:
        print(f"freshness (event→online): p50={pct(freshness_ms,50)}  "
              f"p95={pct(freshness_ms,95)}  p99={pct(freshness_ms,99)}ms")
else:
    print("Ranker-based freshness probe disabled (needs interactive OAuth for the route-optimized "
          "endpoint). The SQL-based method below measures freshness directly from the online table "
          "and does NOT depend on the endpoint — prefer it.")

# COMMAND ----------
# MAGIC %md ## 2b · Freshness — measured directly from the online table (endpoint-free, robust)
# MAGIC `commit_time − event_time` on the streaming online table is the true event→online latency and
# MAGIC needs no serving call. **Measure at a LOW, steady event rate (~20–25 ev/s) with the pipeline
# MAGIC caught up**, and exclude window-expiry rows (`event_time IS NULL`) and timer rows
# MAGIC (`is_timer = true`) — those carry no source event and inflate the tail. At high throughput
# MAGIC (e.g. 2000 ev/s) the RollingWindow pipeline runs a backlog and this lag balloons into seconds:
# MAGIC that is throughput-under-load, not steady-state freshness. Measured steady-state (dedup'd,
# MAGIC ~22k samples): **p50 ≈ 104ms / p95 ≈ 135ms / p99 ≈ 157ms** (min ≈ 61ms).
# COMMAND ----------
online_tbl = None
for m in fe.list_materialized_features(feature_name=f"{catalog}.{schema}.cust_clicks_10m"):
    if m.is_online:
        online_tbl = m.table_name
        break

sql_freshness = None
if online_tbl:
    df = spark.sql(f"""
        WITH d AS (
          SELECT DISTINCT customer_id, event_time,
                 unix_millis(to_timestamp(commit_time)) - unix_millis(to_timestamp(event_time)) AS lag_ms
          FROM {online_tbl}
          WHERE event_time IS NOT NULL AND is_timer = false
            AND commit_time > current_timestamp() - INTERVAL 5 MINUTES
        )
        SELECT count(*) AS n,
               percentile_approx(lag_ms, 0.50) AS p50,
               percentile_approx(lag_ms, 0.95) AS p95,
               percentile_approx(lag_ms, 0.99) AS p99,
               min(lag_ms) AS min_ms
        FROM d
    """).collect()[0].asDict()
    if df["n"]:
        sql_freshness = df
        print(f"event→online freshness (n={df['n']}): "
              f"p50={df['p50']}ms  p95={df['p95']}ms  p99={df['p99']}ms  min={df['min_ms']}ms")
    else:
        print("No fresh event-bearing rows in the last 5 min — start the 08 continuous producer "
              "(low rate) and let the 07 pipeline catch up, then re-run.")
else:
    print("Streaming online table for cust_clicks_10m not found — run notebook 07 first.")

# COMMAND ----------
# MAGIC %md ## Persist results to Delta for the dashboard
# COMMAND ----------
rows = []
if serving:
    rows.append({"stage": "feature_read_and_rank",
                 "p50": serving["p50"], "p95": serving["p95"], "p99": serving["p99"]})
if sql_freshness:
    rows.append({"stage": "freshness_event_to_online",
                 "p50": float(sql_freshness["p50"]), "p95": float(sql_freshness["p95"]),
                 "p99": float(sql_freshness["p99"])})
elif freshness_ms:
    rows.append({"stage": "freshness_event_to_online",
                 "p50": pct(freshness_ms, 50), "p95": pct(freshness_ms, 95), "p99": pct(freshness_ms, 99)})

if rows:
    (spark.createDataFrame(pd.DataFrame(rows))
          .write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.part2_latency_results"))
    print(f"Wrote {catalog}.{schema}.part2_latency_results ({len(rows)} rows)")
else:
    print("No results to persist (serving benchmark needs interactive auth; freshness needs a live "
          "stream). Existing part2_latency_results table left unchanged.")
