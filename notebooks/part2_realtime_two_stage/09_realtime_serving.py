# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 09 · Real-Time Serving with Streaming Features
# MAGIC Extends Part 1's online-lookup ranker with the **streaming in-session feature**
# MAGIC `cust_clicks_10m` (notebook 08). No Vector Search, no retrieval stage — for the NBO
# MAGIC catalog we **rank all offers** and let personalization live entirely in the ranker.
# MAGIC The request carries only `{customer_id, offer_id + offer attrs}`; the endpoint fetches
# MAGIC **all customer features — batch + streaming — from the online store by `customer_id`**.
# MAGIC
# MAGIC This is the real-time proof point: the freshest in-session signal (`cust_clicks_10m`,
# MAGIC continuously maintained by the streaming pipeline) feeds the same ranker within the
# MAGIC sub-300ms serving path. Latency + freshness are benchmarked in notebook 10.
# MAGIC
# MAGIC **Why no Vector Search:** for a ~40-offer catalog, retrieval adds ~90ms for no benefit —
# MAGIC you can score every offer directly. Retrieval only earns its place at catalog scale
# MAGIC (thousands of products); that variant is deliberately out of scope here (see README).

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0" lightgbm scikit-learn mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "nbo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import mlflow
import pandas as pd
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    RequestSource, FieldDefinition, ScalarDataType, ColumnSelection, Feature,
)
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from lightgbm import LGBMClassifier

mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()

# COMMAND ----------
# MAGIC %md ## Re-log the ranker with the streaming feature added
# MAGIC Same Part 1 online-lookup pattern, now with **6 online-looked-up customer features**
# MAGIC (5 batch + `cust_clicks_10m` streaming) plus the 4 request-time offer columns. The
# MAGIC streaming feature is looked up by `customer_id` exactly like the batch features — the
# MAGIC ranker doesn't know or care that it came from a stream.
def gf(n):
    return fe.get_feature(full_name=f"{catalog}.{schema}.{n}")

cust_feats = [gf(n) for n in ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
                              "cust_loyalty_tier", "cust_risk_band", "cust_clicks_10m"]]

# Request-time offer columns (RequestSource + passthrough ColumnSelection; name == column name).
req = RequestSource(schema=[
    FieldDefinition(name="offer_id", data_type=ScalarDataType.STRING),
    FieldDefinition(name="product_category", data_type=ScalarDataType.STRING),
    FieldDefinition(name="base_reward", data_type=ScalarDataType.DOUBLE),
    FieldDefinition(name="tier_requirement", data_type=ScalarDataType.INTEGER),
])

def get_or_create_req(colname):
    try:
        return fe.get_feature(full_name=f"{catalog}.{schema}.{colname}")
    except Exception:
        return fe.create_feature(source=req, function=ColumnSelection(column=colname),
                                 catalog_name=catalog, schema_name=schema, name=colname)

offer_feats = [get_or_create_req(c) for c in
               ["offer_id", "product_category", "base_reward", "tier_requirement"]]

# COMMAND ----------
# MAGIC %md ## Point-in-time training set + train
# MAGIC `cust_clicks_10m` is a RollingWindow feature — point-in-time correct for the label `ts`.
labels = spark.table(f"`{catalog}`.{schema}.labels").withColumn("updated_at", F.col("ts"))
offers = spark.table(f"`{catalog}`.{schema}.offers").select(
    "offer_id", "product_category", "base_reward", "tier_requirement")
labels = labels.join(offers, on="offer_id", how="left")

ts = fe.create_training_set(
    df=labels, features=cust_feats + offer_feats, label="accepted",
    exclude_columns=["record_id", "customer_id", "ts", "updated_at"],
)
tdf = ts.load_df().toPandas()

CAT = ["offer_id", "product_category", "cust_loyalty_tier", "cust_risk_band"]
NUM = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
       "cust_clicks_10m", "base_reward", "tier_requirement"]
X = tdf[CAT + NUM].copy()
for c in CAT:
    X[c] = X[c].astype(str)
for c in NUM:
    X[c] = pd.to_numeric(X[c], errors="coerce")
y = tdf["accepted"].astype(int)
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# remainder="drop" is serve-safe: the FS wrapper appends customer_id/ts/etc.; drop them.
model = Pipeline([
    ("pre", ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT),
         ("num", "passthrough", NUM)], remainder="drop")),
    ("clf", LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                           subsample=0.8, colsample_bytree=0.8, random_state=42)),
])
model.fit(Xtr, ytr)
print("val_auc:", roc_auc_score(yte, model.predict_proba(Xte)[:, 1]))

# COMMAND ----------
# MAGIC %md ## Log with feature metadata → register → deploy route-optimized
MODEL = f"{catalog}.{schema}.nbo_ranker_realtime"
with mlflow.start_run(run_name="nbo_ranker_realtime"):
    fe.log_model(model=model, artifact_path="model", flavor=mlflow.sklearn,
                 training_set=ts, registered_model_name=MODEL,
                 skops_trusted_types=["collections.OrderedDict", "lightgbm.basic.Booster",
                     "lightgbm.sklearn.LGBMClassifier",
                     "sklearn.compose._column_transformer._RemainderColsList"])
from mlflow.tracking import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
newest = max(c.search_model_versions(f"name='{MODEL}'"), key=lambda v: int(v.version))
c.set_registered_model_alias(MODEL, "prod", newest.version)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput, TrafficConfig, Route,
)
w = WorkspaceClient()
ENDPOINT, SERVED = "nbo-ranker-realtime", "nbo-realtime-ro"
served = [ServedEntityInput(name=SERVED, entity_name=MODEL, entity_version=newest.version,
                            workload_size="Small", scale_to_zero_enabled=False)]
if ENDPOINT in [e.name for e in w.serving_endpoints.list()]:
    w.serving_endpoints.update_config(name=ENDPOINT, served_entities=served)
else:
    w.serving_endpoints.create(name=ENDPOINT, route_optimized=True,
        config=EndpointCoreConfigInput(name=ENDPOINT, served_entities=served,
            traffic_config=TrafficConfig(routes=[Route(served_model_name=SERVED, traffic_percentage=100)])))
print(f"Route-optimized endpoint '{ENDPOINT}' deploying {MODEL} v{newest.version}")

# COMMAND ----------
# MAGIC %md ## Rank-all serving — request carries only customer_id + offer fields
# MAGIC The endpoint looks up all 6 customer features (incl. the live `cust_clicks_10m`) online.
# MAGIC ```python
# MAGIC dp = w.serving_endpoints_data_plane   # route-optimized → data-plane client
# MAGIC offers = spark.table(f"`{catalog}`.{schema}.offers").collect()
# MAGIC recs = [{"customer_id": cid, "offer_id": o.offer_id, "product_category": o.product_category,
# MAGIC          "base_reward": o.base_reward, "tier_requirement": o.tier_requirement} for o in offers]
# MAGIC preds = dp.query(name="nbo-ranker-realtime", dataframe_records=recs).predictions
# MAGIC ```
