# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 09 · Vector Search Index over Offers
# MAGIC Builds the candidate-retrieval index for the two-stage recommender. Embeds each offer's
# MAGIC `offer_text` with the FMAPI `databricks-gte-large-en` model (managed-embedding Delta-sync
# MAGIC index). Retrieval returns the top-K offers whose text best matches the customer's
# MAGIC **in-session intent** — the piece that lets ranking scale past a tiny catalog.
# MAGIC
# MAGIC **Why this is Part 2, not Part 1:** for the 40-offer demo catalog you can score every offer
# MAGIC directly (Part 1). Retrieval earns its place only at catalog scale (thousands of
# MAGIC products / eligibility-scoped offers), where ANN narrows N→K under the latency budget. We
# MAGIC show the pattern on the small catalog but frame it as "the architecture you use at scale."

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins-industry-solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("vs_endpoint", "nbo-vs-endpoint")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vs_endpoint = dbutils.widgets.get("vs_endpoint")

from databricks.vector_search.client import VectorSearchClient

def q(fqn):  # backtick each identifier part (catalog has a hyphen)
    return ".".join(f"`{p}`" for p in fqn.split("."))

# COMMAND ----------
# MAGIC %md ## Source table (Change Data Feed required for Delta-sync)
src = f"{catalog}.{schema}.offers_vs_src"
idx = f"{catalog}.{schema}.offers_index"

spark.sql(f"""CREATE OR REPLACE TABLE {q(src)}
              TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
              SELECT offer_id, product_category, offer_text, base_reward, tier_requirement
              FROM {q(catalog + '.' + schema + '.offers')}""")
print(f"Source rows: {spark.table(q(src)).count()}")

# COMMAND ----------
# MAGIC %md ## Endpoint + managed-embedding index
vsc = VectorSearchClient(disable_notice=True)
eps = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if vs_endpoint not in eps:
    vsc.create_endpoint_and_wait(name=vs_endpoint, endpoint_type="STANDARD")

existing = [i.get("name") for i in vsc.list_indexes(vs_endpoint).get("vector_indexes", [])]
if idx not in existing:
    vsc.create_delta_sync_index(
        endpoint_name=vs_endpoint, index_name=idx, source_table_name=src,
        pipeline_type="TRIGGERED", primary_key="offer_id",
        embedding_source_column="offer_text",
        embedding_model_endpoint_name="databricks-gte-large-en",
    )
    print(f"Created index {idx}.")
else:
    print(f"Index {idx} already exists.")

# COMMAND ----------
# MAGIC %md ## Wait for readiness, then smoke-test retrieval
import time
index = vsc.get_index(endpoint_name=vs_endpoint, index_name=idx)
for _ in range(30):
    st = index.describe().get("status", {})
    if st.get("ready"):
        break
    time.sleep(20)
print("index ready:", index.describe().get("status", {}).get("ready"))

res = index.similarity_search(
    query_text="customer using personal loan calculator for debt consolidation",
    columns=["offer_id", "product_category", "offer_text"], num_results=5)
for row in res.get("result", {}).get("data_array", [])[:5]:
    print(row[:3])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next → notebook 10
# MAGIC The two-stage serving notebook embeds the session context **client-side** and retrieves
# MAGIC with `query_vector` (avoids the per-request server-side embedding hop, ~50ms cheaper), then
# MAGIC ranks the retrieved candidates on the Part 1 online-lookup endpoint.
