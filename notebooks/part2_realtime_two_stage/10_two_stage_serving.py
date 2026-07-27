# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 10 · Two-Stage Serving (Retrieve → Rank)
# MAGIC Wires the full real-time recommender:
# MAGIC 1. **Stage 1 — retrieve:** embed the in-session context client-side, query the
# MAGIC    `offers_index` (notebook 09) with `query_vector` for the top-K candidate offers.
# MAGIC 2. **Stage 2 — rank:** score those candidates on the **Part 1 online-lookup endpoint**
# MAGIC    (`nbo-ranker-online`) — which auto-fetches the customer features (now including the
# MAGIC    streaming `cust_clicks_10m`, notebook 08) from the online store by `customer_id`.
# MAGIC
# MAGIC **The key design point:** Part 2 reuses Part 1's ranker unchanged. It only *adds* a retrieval
# MAGIC stage in front and (optionally) a streaming feature the endpoint looks up. If you registered
# MAGIC the streaming feature in 08, re-log the ranker with `cust_clicks_10m` in the training set so
# MAGIC the endpoint looks it up too — otherwise the batch-feature ranker from Part 1 is used as-is.

# COMMAND ----------
# MAGIC %pip install "databricks-sdk>=0.30" databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins-industry-solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("vs_endpoint", "nbo-vs-endpoint")
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-online")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")
idx = f"{catalog}.{schema}.offers_index"

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

w = WorkspaceClient()
dp = w.serving_endpoints_data_plane           # route-optimized ranker → data-plane client
vsc = VectorSearchClient(disable_notice=True)
index = vsc.get_index(endpoint_name=vs_endpoint, index_name=idx)

# COMMAND ----------
# MAGIC %md ## Two-stage recommend
COLS = ["offer_id", "product_category", "offer_text", "base_reward", "tier_requirement"]

def embed(text):
    e = w.serving_endpoints.query(name="databricks-gte-large-en", input=[text]).data[0]
    return e.embedding if hasattr(e, "embedding") else e["embedding"]

def recommend(customer_id, context, k=10):
    # Stage 1 — retrieve (client-side embed → query_vector avoids the server-side FMAPI hop)
    res = index.similarity_search(query_vector=embed(context), columns=COLS, num_results=k)
    cands = res.get("result", {}).get("data_array", [])
    ix = {c: i for i, c in enumerate(COLS)}

    # Stage 2 — rank on the online-lookup endpoint (customer features fetched by customer_id)
    recs = [{
        "customer_id": customer_id,
        "offer_id": c[ix["offer_id"]],
        "product_category": c[ix["product_category"]],
        "base_reward": float(c[ix["base_reward"]]),
        "tier_requirement": int(c[ix["tier_requirement"]]),
    } for c in cands]
    preds = dp.query(name=ranker_endpoint, dataframe_records=recs).predictions

    ranked = sorted(zip(cands, preds), key=lambda x: x[1], reverse=True)
    return [{"offer_id": c[ix["offer_id"]], "category": c[ix["product_category"]],
             "score": float(p)} for c, p in ranked]

# COMMAND ----------
# MAGIC %md ## Smoke-test
cust = spark.table(f"`{catalog}`.{schema}.customers").select("customer_id").limit(1).collect()[0][0]
for r in recommend(cust, "customer comparing credit cards with cashback and travel rewards")[:5]:
    print(r)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next → notebook 11
# MAGIC Benchmark the end-to-end two-stage serving path (retrieve + rank) against the <300ms budget,
# MAGIC and separately measure streaming feature **freshness** (event → online availability).
