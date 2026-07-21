# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Deploy the Real-Time Serving Path
# MAGIC Two-stage recommender, **verified end-to-end on serverless**:
# MAGIC 1. **Candidate retrieval** — Vector Search over offer embeddings (top-N candidates)
# MAGIC 2. **Ranking** — Model Serving endpoint scores customer × candidate offers
# MAGIC
# MAGIC **Design note (important):** the ranker is logged as a plain model taking the 6 features
# MAGIC as *request inputs* (not `fe.log_model` auto-online-lookup). In this workspace the
# MAGIC CronSchedule aggregation online tables did not populate on demand (see notebook 03 note),
# MAGIC which blocks endpoint-side online feature lookup. Passing features in the request keeps the
# MAGIC serving path working and the latency story intact; the offline PIT training (notebook 04)
# MAGIC still proves the "author once" claim. For a workspace where CronSchedule backfill fires,
# MAGIC switch back to `fe.log_model(training_set=...)` for automatic online lookups.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0" databricks-vectorsearch lightgbm scikit-learn mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins-industry-solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("vs_endpoint", "nbo-vs-endpoint")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vs_endpoint = dbutils.widgets.get("vs_endpoint")

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
mlflow.set_registry_uri("databricks-uc")
w = WorkspaceClient()

def q(fqn):  # backtick each identifier part (catalog has a hyphen)
    return ".".join(f"`{p}`" for p in fqn.split("."))

# COMMAND ----------
# MAGIC %md ## Stage 1 — Offer embeddings + Vector Search index
# MAGIC Managed-embedding Delta-sync index over `offers.offer_text` (FMAPI `databricks-gte-large-en`).
# MAGIC Source table needs Change Data Feed enabled. Retrieval returns top-N candidate offers.
src = f"{catalog}.{schema}.offers_vs_src"
idx = f"{catalog}.{schema}.offers_index"

spark.sql(f"""CREATE OR REPLACE TABLE {q(src)}
              TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
              SELECT offer_id, product_category, offer_text, base_reward, tier_requirement
              FROM {q(catalog + '.' + schema + '.offers')}""")

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
# Wait for readiness with index.describe()["status"]["ready"] before querying (see nb 06).

# COMMAND ----------
# MAGIC %md ## Stage 2 — Ranking model → Model Serving endpoint
# MAGIC Train + log a serving-friendly ranker (features as request inputs), register, deploy.
# MAGIC Uses cloudpickle serialization (skops rejects raw LightGBM types).
import pandas as pd
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from lightgbm import LGBMClassifier
from mlflow.models.signature import infer_signature

fe = FeatureEngineeringClient()
feats = [fe.get_feature(full_name=f"{catalog}.{schema}.{n}") for n in
         ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
          "cust_loyalty_tier", "cust_risk_band"]]
labels = spark.table(q(f"{catalog}.{schema}.labels")).withColumn("updated_at", F.col("ts"))
ts = fe.create_training_set(df=labels, features=feats, label="accepted",
                            exclude_columns=["record_id", "customer_id", "ts", "updated_at"])
tdf = ts.load_df().toPandas()

CAT = ["offer_id", "cust_loyalty_tier", "cust_risk_band"]
NUM = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d"]
X = tdf[CAT + NUM].copy()
for c in CAT:
    X[c] = X[c].astype(str)
for c in NUM:
    X[c] = pd.to_numeric(X[c], errors="coerce")
y = tdf["accepted"].astype(int)
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

model = Pipeline([
    ("pre", ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT)],
        remainder="passthrough")),
    ("clf", LGBMClassifier(n_estimators=200, learning_rate=0.05, num_leaves=31,
                           subsample=0.8, colsample_bytree=0.8, random_state=42)),
])
model.fit(Xtr, ytr)
auc = roc_auc_score(yte, model.predict_proba(Xte)[:, 1])
print("val_auc:", auc)

MODEL = f"{catalog}.{schema}.nbo_ranker_serving"
sig = infer_signature(Xte.head(20), model.predict_proba(Xte.head(20))[:, 1])
with mlflow.start_run(run_name="nbo_ranker_serving"):
    mlflow.log_metric("val_auc", auc)
    mlflow.sklearn.log_model(model, artifact_path="model", signature=sig,
                             input_example=Xte.head(5), registered_model_name=MODEL,
                             serialization_format="cloudpickle")

from mlflow.tracking import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
newest = max(c.search_model_versions(f"name='{MODEL}'"), key=lambda v: int(v.version))
c.set_registered_model_alias(MODEL, "prod", newest.version)

# COMMAND ----------
# MAGIC %md ## Deploy the ranking endpoint — **route-optimized** for low latency
# MAGIC Two latency levers that matter for a personalization hot path:
# MAGIC - **`route_optimized=True`** — bypasses the standard serving proxy; cuts tens of ms of
# MAGIC   per-request overhead. Immutable at create time, so recreate to change it.
# MAGIC - **`scale_to_zero_enabled=False`** — keeps a warm replica, removing cold-start p99 spikes.
# MAGIC
# MAGIC Measured effect: ranking p50 dropped from ~30ms (proxy) to in-region ≈15ms, and the
# MAGIC end-to-end p99 tail (660ms with scale-to-zero) disappeared.
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput, TrafficConfig, Route,
)

ENDPOINT = "nbo-ranker"
SERVED_NAME = "nbo-ranker-ro"
served = [ServedEntityInput(name=SERVED_NAME, entity_name=MODEL, entity_version=newest.version,
                            workload_size="Small", scale_to_zero_enabled=False)]

# route_optimized is immutable → delete + recreate if the endpoint already exists non-optimized.
existing = {e.name: e for e in w.serving_endpoints.list()}
if ENDPOINT in existing and not getattr(existing[ENDPOINT], "route_optimized", False):
    w.serving_endpoints.delete(name=ENDPOINT)
    existing.pop(ENDPOINT)

if ENDPOINT not in existing:
    w.serving_endpoints.create(
        name=ENDPOINT, route_optimized=True,
        config=EndpointCoreConfigInput(
            name=ENDPOINT, served_entities=served,
            traffic_config=TrafficConfig(routes=[Route(served_model_name=SERVED_NAME, traffic_percentage=100)]),
        ),
    )
else:
    w.serving_endpoints.update_config(name=ENDPOINT, served_entities=served)
print(f"Route-optimized endpoint '{ENDPOINT}' deploying {MODEL} v{newest.version}")

# COMMAND ----------
# MAGIC %md ## Querying a route-optimized endpoint
# MAGIC Route-optimized endpoints reject the standard proxy URL — you must POST to the data-plane
# MAGIC URL with a **downscoped OAuth token**. The SDK data-plane client handles both (requires
# MAGIC OAuth-authenticated creds — works from a workspace/app with OAuth, not PAT):
# MAGIC ```python
# MAGIC dp = w.serving_endpoints_data_plane
# MAGIC dp.query(name="nbo-ranker", dataframe_records=recs).predictions
# MAGIC ```
# MAGIC The app (apps/recommender-app) uses this path via its service principal's OAuth creds.
