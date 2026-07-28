# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 10 · Latency & Freshness Benchmark
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
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-realtime")
dbutils.widgets.text("kafka_connection", "msk_kafka")
dbutils.widgets.text("service_credential", "msk_kafka")
dbutils.widgets.text("topic", "nbo-session-events")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")
conn_name = dbutils.widgets.get("kafka_connection")
service_credential = dbutils.widgets.get("service_credential")
topic = dbutils.widgets.get("topic")

import time
import statistics
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
dp = w.serving_endpoints_data_plane  # route-optimized → data-plane client

def pct(a, p):
    return round(sorted(a)[min(len(a) - 1, int(len(a) * p / 100))], 1)

# COMMAND ----------
# MAGIC %md ## 1 · Serving latency — online feature-read + rank-all (no retrieval)
# MAGIC Request carries only `customer_id` + offer fields; the endpoint looks up all 6 customer
# MAGIC features (incl. streaming `cust_clicks_10m`) online and scores the full offer catalog.
offers = [r.asDict() for r in spark.table(f"`{catalog}`.{schema}.offers").collect()]
customers = [r.asDict() for r in
             spark.table(f"`{catalog}`.{schema}.customers").select("customer_id").limit(200).collect()]
print(f"{len(customers)} customers × {len(offers)} offers")

def rank(customer_id):
    recs = [{
        "customer_id": customer_id,
        "offer_id": o["offer_id"],
        "product_category": o["product_category"],
        "base_reward": float(o["base_reward"]),
        "tier_requirement": int(o["tier_requirement"]),
    } for o in offers]
    return dp.query(name=ranker_endpoint, dataframe_records=recs).predictions

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
# MAGIC materialization (notebook 08) to be running and continuously consuming the topic.
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()
conn = w.connections.get(name=conn_name)
BOOTSTRAP = dict(conn.options or {})["bootstrap_servers"]
KAFKA_OPTS = {
    "kafka.bootstrap.servers": BOOTSTRAP,
    "databricks.serviceCredential": service_credential,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
}

def emit_marker(customer_id, n=5):
    """Produce n marker events for customer_id to the topic (Spark Kafka write)."""
    rows = spark.range(n).select(
        F.lit(customer_id).alias("key"),
        F.to_json(F.struct(
            F.concat(F.lit("mark_"), (F.lit(int(time.time() * 1000)) + F.col("id")).cast("string")).alias("event_id"),
            F.lit(customer_id).alias("customer_id"),
            (F.lit(int(time.time() * 1000))).cast("long").alias("event_time"),
            F.lit("marker").alias("event_type"),
            F.lit("credit_card").alias("product_category"),
            F.lit(1000).alias("dwell_ms"),
            F.lit("web").alias("device"),
        )).alias("value"),
    )
    rows.write.format("kafka").options(**KAFKA_OPTS).option("topic", topic).save()

# Freshness probe loop. Enable when the streaming pipeline (08) + producer (07 continuous) are live.
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
    print("Freshness probe disabled. Set RUN_FRESHNESS=True with the 08 streaming pipeline + "
          "07 continuous producer live. Reference: ~200ms p99 event→online-availability.")

# COMMAND ----------
# MAGIC %md ## Persist results to Delta for the dashboard
rows = [{"stage": "feature_read_and_rank", "p50": serving["p50"],
         "p95": serving["p95"], "p99": serving["p99"]}]
if freshness_ms:
    rows.append({"stage": "freshness_event_to_online",
                 "p50": pct(freshness_ms, 50), "p95": pct(freshness_ms, 95), "p99": pct(freshness_ms, 99)})
(spark.createDataFrame(pd.DataFrame(rows))
      .write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.part2_latency_results"))
print(f"Wrote {catalog}.{schema}.part2_latency_results")
