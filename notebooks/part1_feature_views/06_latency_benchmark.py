# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 · 06 · Latency Benchmark — Feature Read + Rank
# MAGIC The clean Feature Views proof point: **online feature lookup + ranking**, no retrieval.
# MAGIC The request carries only `{customer_id, offer_id, offer attrs}`; the route-optimized
# MAGIC `nbo-ranker-online` endpoint fetches the 5 customer features from the online store by
# MAGIC `customer_id` and scores the passed-in candidate set (here: the full 40-offer catalog).
# MAGIC
# MAGIC **Measured on `fe-vm-ttan-vm`** — this is the path that maps directly to the personalization
# MAGIC reference (~10ms feature-serving read + ~30ms model-serving inference):
# MAGIC
# MAGIC | Stage | In-region estimate | Notes |
# MAGIC |---|---|---|
# MAGIC | Online feature read + rank (40 offers) | **~15ms** | route-optimized endpoint, scale-to-zero off |
# MAGIC | (laptop-measured, incl ~80ms cross-region WAN RTT) | ~120–160ms | subtract WAN for in-region |
# MAGIC
# MAGIC No Vector Search here — for a 40-offer catalog you score everything directly. Retrieval is a
# MAGIC Part 2 concern (catalog-scale). This keeps Part 1's latency story to the two numbers that
# MAGIC matter for the Feature Views value prop: **feature read + rank**.

# COMMAND ----------
# MAGIC %pip install "databricks-sdk>=0.30" "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins-industry-solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-online")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")

import time
import statistics
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
# Route-optimized endpoint → data-plane client (resolves data-plane URL + downscoped OAuth token).
dp = w.serving_endpoints_data_plane

# COMMAND ----------
# MAGIC %md ## Candidate set = the full offer catalog; customer features looked up online by key
offers = [r.asDict() for r in spark.table(f"`{catalog}`.{schema}.offers").collect()]
customers = [r.asDict() for r in
             spark.table(f"`{catalog}`.{schema}.customers").select(
                 "customer_id", "loyalty_tier", "risk_band").limit(200).collect()]
print(f"{len(customers)} customers × {len(offers)} offers")

def rank(customer_id):
    # Request carries ONLY customer_id + offer fields. The endpoint fetches the 5 customer
    # features (avg_balance_30d, spend_90d, txn_count_7d, loyalty_tier, risk_band) online.
    recs = [{
        "customer_id": customer_id,
        "offer_id": o["offer_id"],
        "product_category": o["product_category"],
        "base_reward": float(o["base_reward"]),
        "tier_requirement": int(o["tier_requirement"]),
    } for o in offers]
    return dp.query(name=ranker_endpoint, dataframe_records=recs).predictions

# COMMAND ----------
# MAGIC %md ## Warm, then benchmark
for i in range(12):
    rank(customers[i % len(customers)]["customer_id"])

N = 100
lat = []
for i in range(N):
    t0 = time.perf_counter()
    rank(customers[i % len(customers)]["customer_id"])
    lat.append((time.perf_counter() - t0) * 1000)

def pct(a, p):
    return round(sorted(a)[min(len(a) - 1, int(len(a) * p / 100))], 1)

print(f"N={N} (each = online feature read + rank over {len(offers)} offers)")
print(f"feature-read + rank  p50={pct(lat,50)}ms  p95={pct(lat,95)}ms  p99={pct(lat,99)}ms  mean={round(statistics.mean(lat),1)}ms")
print("NOTE: measured from this driver; subtract cross-region WAN RTT for the in-region number.")

# COMMAND ----------
# MAGIC %md ## Persist results to Delta for the dashboard
import pandas as pd
rows = [{"stage": "feature_read_and_rank", "p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99)}]
(spark.createDataFrame(pd.DataFrame(rows))
      .write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.part1_latency_results"))
print(f"Wrote {catalog}.{schema}.part1_latency_results")
