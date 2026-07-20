# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Latency Benchmark — Prove <300ms
# MAGIC Load-tests the end-to-end online path (retrieval → ranking) and publishes percentiles.
# MAGIC The <300ms claim must be measured and defensible, not assumed.
# MAGIC
# MAGIC **Measured result (fins-industry-solutions.nbo, serverless, N=100 after warmup):**
# MAGIC
# MAGIC | Stage | p50 | p95 | p99 |
# MAGIC |---|---|---|---|
# MAGIC | Retrieval (Vector Search) | ~145ms | ~205ms | ~380ms |
# MAGIC | Ranking (Model Serving) | ~30ms | ~55ms | ~290ms |
# MAGIC | **End-to-end** | **~175ms** | **~235ms** | ~660ms |
# MAGIC
# MAGIC **p50 and p95 land under the 300ms budget.** p99 shows occasional tail spikes, driven by
# MAGIC the **managed-embedding FMAPI hop** in retrieval (the query string is embedded server-side
# MAGIC per request) plus rare serving cold-slots. To tighten the tail: embed queries client-side
# MAGIC and use `query_vector`, add a provisioned-throughput embedding endpoint, or disable
# MAGIC scale-to-zero on the ranker for the demo.
# MAGIC
# MAGIC **Two numbers, kept honest:** this measures *serving* latency (request → ranked response).
# MAGIC It is distinct from the Feature Views launch figure of ~200ms p99 *event→online-availability*
# MAGIC (freshness), which the streaming path (notebook 01b/02) would measure separately.

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins-industry-solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("vs_endpoint", "nbo-vs-endpoint")
dbutils.widgets.text("ranker_endpoint", "nbo-ranker")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")
idx = f"{catalog}.{schema}.offers_index"

import time
import statistics
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

w = WorkspaceClient()
vsc = VectorSearchClient(disable_notice=True)
index = vsc.get_index(endpoint_name=vs_endpoint, index_name=idx)

# COMMAND ----------
# MAGIC %md ## Simulated in-session contexts + a customer pool
feats_pd = (spark.table(f"`{catalog}`.{schema}.customers")
            .select("customer_id", "loyalty_tier", "risk_band").limit(200).toPandas())

CONTEXTS = [
    "customer comparing credit cards with cashback and travel rewards",
    "customer viewing savings account rates and high yield options",
    "customer using personal loan calculator for debt consolidation",
    "customer browsing mortgage refinance and home equity options",
    "customer exploring retirement investment and IRA products",
]

def retrieve(ctx, k=10):
    r = index.similarity_search(query_text=ctx,
        columns=["offer_id", "product_category", "offer_text"], num_results=k)
    return r.get("result", {}).get("data_array", [])

def rank(customer_row, candidates):
    recs = [{
        "offer_id": c[0],
        "cust_loyalty_tier": customer_row["loyalty_tier"],
        "cust_risk_band": customer_row["risk_band"],
        # agg features passed as request inputs (see nb 05 design note)
        "cust_avg_balance_30d": 25000.0,
        "cust_spend_90d": 5000.0,
        "cust_txn_count_7d": 5.0,
    } for c in candidates]
    return w.serving_endpoints.query(name=ranker_endpoint, dataframe_records=recs).predictions

# COMMAND ----------
# MAGIC %md ## Warm the endpoints, then benchmark
# MAGIC Under-warming badly skews the tail (scale-to-zero cold start + managed-embedding FMAPI warmup).
for i in range(12):
    rank(feats_pd.iloc[i % len(feats_pd)], retrieve(CONTEXTS[i % len(CONTEXTS)], 10))

N = 100
e2e, t_ret, t_rank = [], [], []
for i in range(N):
    cust = feats_pd.iloc[i % len(feats_pd)]
    t0 = time.perf_counter()
    cand = retrieve(CONTEXTS[i % len(CONTEXTS)], 10)
    t1 = time.perf_counter()
    rank(cust, cand)
    t2 = time.perf_counter()
    t_ret.append((t1 - t0) * 1000)
    t_rank.append((t2 - t1) * 1000)
    e2e.append((t2 - t0) * 1000)

def pct(a, p):
    return round(sorted(a)[min(len(a) - 1, int(len(a) * p / 100))], 1)

print(f"N={N}")
print(f"retrieval  p50={pct(t_ret,50)}  p95={pct(t_ret,95)}  p99={pct(t_ret,99)}")
print(f"ranking    p50={pct(t_rank,50)}  p95={pct(t_rank,95)}  p99={pct(t_rank,99)}")
print(f"e2e        p50={pct(e2e,50)}  p95={pct(e2e,95)}  p99={pct(e2e,99)}  mean={round(statistics.mean(e2e),1)}")
print(f"UNDER 300ms @ p95: {pct(e2e,95) < 300}")

# COMMAND ----------
# MAGIC %md ## Persist results to Delta for the dashboard
rows = [
    {"stage": "retrieval", "p50": pct(t_ret, 50), "p95": pct(t_ret, 95), "p99": pct(t_ret, 99)},
    {"stage": "ranking",   "p50": pct(t_rank, 50), "p95": pct(t_rank, 95), "p99": pct(t_rank, 99)},
    {"stage": "e2e",       "p50": pct(e2e, 50),   "p95": pct(e2e, 95),   "p99": pct(e2e, 99)},
]
(spark.createDataFrame(pd.DataFrame(rows))
      .write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.latency_results"))
print(f"Wrote {catalog}.{schema}.latency_results")
