# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Deploy the Real-Time Serving Path
# MAGIC Two-stage recommender:
# MAGIC 1. **Candidate retrieval** — Vector Search over offer embeddings (top-N candidates)
# MAGIC 2. **Ranking** — Model Serving endpoint scores customer × candidates using online features

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
dbutils.widgets.text("vector_search_endpoint", "nbo-vs-endpoint")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vs_endpoint = dbutils.widgets.get("vector_search_endpoint")

# COMMAND ----------
# MAGIC %md ## Stage 1 — Offer embeddings + Vector Search index
# MAGIC Embed offer descriptions (FMAPI embeddings), build a Delta-sync Vector Search index
# MAGIC keyed by offer_id. Retrieval returns the top-N candidate offers for a context vector.
# TODO: create_vs_endpoint / create_vs_index over offers table

# COMMAND ----------
# MAGIC %md ## Stage 2 — Ranking model serving endpoint
# MAGIC Deploy the UC-registered `nbo_ranker` (@prod). Because it was logged with fe.log_model,
# MAGIC the endpoint auto-fetches online features by entity key at request time.
# TODO: create Model Serving endpoint for {catalog}.{schema}.nbo_ranker@prod
#        workload_size="Small", scale_to_zero_enabled=True

# COMMAND ----------
# MAGIC %md ## Optional — Feature Serving endpoint
# MAGIC If the app needs raw features (e.g. to display "why this offer"), expose a
# MAGIC feature_spec via create_feature_serving_endpoint (serves from online store only).
print("TODO: wire two-stage serving + optional feature serving endpoint")
