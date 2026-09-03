# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 · 05 · Deploy the Ranking Endpoint (true online feature lookup)
# MAGIC The purest Feature Views story, and Part 1's headline. **No Vector Search** — the endpoint
# MAGIC ranks a candidate set passed in the request (for the 40-offer catalog, all offers).
# MAGIC The ranking endpoint auto-fetches customer features **from the online store by
# MAGIC `customer_id`** at request time — the request body carries only `customer_id` + the
# MAGIC offer fields. This is the "author a feature once, serve it online" proof point.
# MAGIC
# MAGIC **Verified end-to-end** — request `{customer_id, offer_id,
# MAGIC product_category, base_reward, tier_requirement}` → endpoint looks up the 5 customer
# MAGIC features online → ranks. ~15ms in-region per 40-offer batch, per-customer offer spread up to 1.0.
# MAGIC
# MAGIC ### How request-time offer columns coexist with online-looked-up features
# MAGIC `fe.log_model(training_set=...)` records the training-set schema. The serving wrapper
# MAGIC forwards to the raw model **every training-set column** — both the online-looked-up
# MAGIC features AND request-time columns declared via **`RequestSource`**. The gotcha: a
# MAGIC `RequestSource` `ColumnSelection` feature's **name must equal its column name** (no prefix),
# MAGIC and each must be registered with `create_feature(catalog_name=, schema_name=)`.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0" lightgbm scikit-learn mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-online")  # override in a shared workspace
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")

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
# MAGIC %md ## Features: 5 customer features (online lookup) + 4 offer columns (RequestSource)
# COMMAND ----------
def gf(n):
    return fe.get_feature(full_name=f"{catalog}.{schema}.{n}")

cust_feats = [gf(n) for n in ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
                              "cust_loyalty_tier", "cust_risk_band"]]

# Declare the request-time offer columns as a RequestSource, then register a passthrough
# ColumnSelection feature per column (name MUST equal the column name).
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
# COMMAND ----------
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
NUM = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d", "base_reward", "tier_requirement"]
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
# COMMAND ----------
# Pin the serving env explicitly (see notebook 04 for the rationale). Exact-pin
# mlflow/sklearn/lightgbm; range-pin numpy/pandas below mlflow's caps. Do NOT add
# databricks-feature-engineering — fe.log_model injects databricks-feature-lookup and
# the two conflict.
import sklearn, lightgbm
pip_requirements = [
    f"mlflow=={mlflow.__version__}",
    f"scikit-learn=={sklearn.__version__}",
    f"lightgbm=={lightgbm.__version__}",
    "numpy>=1.26,<2",
    "pandas>=2.1,<3",
    "cloudpickle",
]

MODEL = f"{catalog}.{schema}.nbo_ranker_online"
with mlflow.start_run(run_name="nbo_ranker_online_lookup"):
    fe.log_model(model=model, artifact_path="model", flavor=mlflow.sklearn,
                 training_set=ts, registered_model_name=MODEL,
                 pip_requirements=pip_requirements,
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
ENDPOINT, SERVED = dbutils.widgets.get("ranker_endpoint"), "nbo-online-ro"
# scale_to_zero_enabled=True so the endpoint costs nothing while idle — the right default for an
# accelerator someone clones and forgets about. Trade-off: after ~30 min idle the endpoint scales to
# zero and the next request pays a cold start (tens of seconds), so the latency numbers in notebook
# 06 are WARM numbers. For a live demo or a latency benchmark, warm it with a few throwaway requests
# first, or set scale_to_zero_enabled=False for the duration of the demo (and remember it then bills
# continuously until you delete it).
served = [ServedEntityInput(name=SERVED, entity_name=MODEL, entity_version=newest.version,
                            workload_size="Small", scale_to_zero_enabled=True)]
if ENDPOINT in [e.name for e in w.serving_endpoints.list()]:
    w.serving_endpoints.update_config(name=ENDPOINT, served_entities=served)
else:
    w.serving_endpoints.create(name=ENDPOINT, route_optimized=True,
        config=EndpointCoreConfigInput(name=ENDPOINT, served_entities=served,
            traffic_config=TrafficConfig(routes=[Route(served_model_name=SERVED, traffic_percentage=100)])))

# Endpoint deploy is async. Block until the config update finishes and the endpoint is READY
# so the downstream latency benchmark (06) doesn't query a not-ready / cold endpoint.
print(f"Waiting for {ENDPOINT} to become READY (build + provision can take ~10-20 min on first deploy)...")
w.serving_endpoints.wait_get_serving_endpoint_not_updating(name=ENDPOINT)
print(f"{ENDPOINT} is READY.")

# COMMAND ----------
# MAGIC %md ## Grant the benchmark service principal CAN_QUERY (route-optimized query path)
# MAGIC Notebook 06 queries this **route-optimized** endpoint with an OAuth token *downscoped* to
# MAGIC `query_inference_endpoint`, minted from the SP creds in the `nbo` secret scope. Minting that
# MAGIC token requires the SP to hold `CAN_QUERY` on the endpoint. Since the endpoint is (re)created
# MAGIC here on every run, we (re)apply the grant now — otherwise 06 fails with
# MAGIC `invalid_authorization_details: User is not authorized to the requested authorizations`.
# COMMAND ----------
from databricks.sdk.service.serving import (
    ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel,
)
# The benchmark SP lives in the `nbo` secret scope, which only the route-optimized latency
# benchmark (06) needs. Guard the read so a Part-1-only run without that scope still finishes:
# the endpoint is already deployed above — don't hard-fail here just because 06's SP is absent.
try:
    sp_client_id = dbutils.secrets.get("nbo", "sp_client_id")
except Exception:
    sp_client_id = None
if sp_client_id:
    w.serving_endpoints.update_permissions(  # PATCH: adds the grant, preserves owner/admin ACLs
        serving_endpoint_id=w.serving_endpoints.get(name=ENDPOINT).id,
        access_control_list=[ServingEndpointAccessControlRequest(
            service_principal_name=sp_client_id,
            permission_level=ServingEndpointPermissionLevel.CAN_QUERY)],
    )
    print(f"Granted CAN_QUERY on {ENDPOINT} to benchmark SP {sp_client_id}.")
else:
    print("Skipped benchmark-SP grant: `nbo` secret scope / sp_client_id not found. "
          "The endpoint is deployed and usable; set up the `nbo` scope to run the "
          "route-optimized latency benchmark (06).")

# COMMAND ----------
# MAGIC %md ## Query — request carries only customer_id + offer fields; features fetched online
# MAGIC ```python
# MAGIC dp = w.serving_endpoints_data_plane   # route-optimized → data-plane client
# MAGIC recs = [{"customer_id": cid, "offer_id": o.offer_id, "product_category": o.product_category,
# MAGIC          "base_reward": o.base_reward, "tier_requirement": o.tier_requirement} for o in offers]
# MAGIC preds = dp.query(name="nbo-ranker-online", dataframe_records=recs).predictions
# MAGIC ```
