# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 09 · Real-Time Serving with Streaming Features
# MAGIC Extends Part 1's online-lookup ranker with the **streaming in-session feature**
# MAGIC `cust_clicks_10m` (notebook 07). No Vector Search, no retrieval stage — for the NBO
# MAGIC catalog we **rank all offers** and let personalization live entirely in the ranker.
# MAGIC The request carries only `{customer_id, offer_id + offer attrs}`; the endpoint fetches
# MAGIC **all customer features — batch + streaming — from the online store by `customer_id`**.
# MAGIC
# MAGIC This is the real-time proof point: the freshest in-session signal (`cust_clicks_10m`,
# MAGIC continuously maintained by the streaming pipeline) feeds the same ranker within the
# MAGIC sub-300ms serving path. Latency + freshness are benchmarked in the feature-freshness benchmark.
# MAGIC
# MAGIC **Why no Vector Search:** for a ~40-offer catalog, retrieval adds ~90ms for no benefit —
# MAGIC you can score every offer directly. Retrieval only earns its place at catalog scale
# MAGIC (thousands of products); that variant is deliberately out of scope here (see README).

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0" lightgbm scikit-learn mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-realtime")  # override in a shared workspace
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")

# --- Preflight gate (same switch as notebook 07) ---
# This notebook re-logs the ranker with the streaming feature `cust_clicks_10m` and deploys the
# realtime endpoint. Both require Part 2's streaming feature to be materialized (notebooks 07/08),
# which is gated OFF by default. Gate here so a gated run skips cleanly instead of failing on a
# missing streaming feature. Opt in with allow_streaming_online=true once the MSK infra works.
dbutils.widgets.dropdown("allow_streaming_online", "false", ["false", "true"])
if dbutils.widgets.get("allow_streaming_online") != "true":
    msg = ("Streaming ranker re-log + realtime endpoint deploy skipped: Part 2 streaming is gated "
           "(allow_streaming_online=false). Part 1 covers the full author-once / online-lookup / "
           "ranking story end-to-end. Set allow_streaming_online=true (with working MSK infra) to "
           "run Part 2, then re-run.")
    print(msg); dbutils.notebook.exit(msg)

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
# COMMAND ----------
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
# COMMAND ----------
# The batch attribute features (ColumnSelection) use timeseries column `updated_at`; the streaming
# RollingWindow feature `cust_clicks_10m` uses `event_time` (its leaf timeseries key). create_training_set
# requires BOTH timestamp keys present on the label df for point-in-time joins, so provide each from the
# label `ts` (they all mean "as of the label event time" here). Missing `event_time` is what raised
# "Training DataFrame is missing timestamp key required for join: event_time".
labels = (spark.table(f"`{catalog}`.{schema}.labels")
          .withColumn("updated_at", F.col("ts"))
          .withColumn("event_time", F.col("ts")))
offers = spark.table(f"`{catalog}`.{schema}.offers").select(
    "offer_id", "product_category", "base_reward", "tier_requirement")
labels = labels.join(offers, on="offer_id", how="left")

ts = fe.create_training_set(
    df=labels, features=cust_feats + offer_feats, label="accepted",
    exclude_columns=["record_id", "customer_id", "ts", "updated_at", "event_time"],
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

# Serve the acceptance PROBABILITY, not the 0/1 class (see nb05) — otherwise every offer ties at
# 0/1 and there's no ranking. Overriding predict to return predict_proba[:, 1] emits P(accept).
class ProbaLGBM(LGBMClassifier):
    def predict(self, X, **kwargs):
        return self.predict_proba(X, **kwargs)[:, 1]

# remainder="drop" is serve-safe: the FS wrapper appends customer_id/ts/etc.; drop them.
model = Pipeline([
    ("pre", ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT),
         ("num", "passthrough", NUM)], remainder="drop")),
    ("clf", ProbaLGBM(n_estimators=300, learning_rate=0.05, num_leaves=31,
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

MODEL = f"{catalog}.{schema}.nbo_ranker_realtime"
with mlflow.start_run(run_name="nbo_ranker_realtime"):
    fe.log_model(model=model, artifact_path="model", flavor=mlflow.sklearn,
                 training_set=ts, registered_model_name=MODEL,
                 pip_requirements=pip_requirements,
                 # cloudpickle (not skops) so the notebook-defined ProbaLGBM subclass serializes by
                 # value and loads at serving without an importable module.
                 serialization_format="cloudpickle")
from mlflow.tracking import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
newest = max(c.search_model_versions(f"name='{MODEL}'"), key=lambda v: int(v.version))
c.set_registered_model_alias(MODEL, "prod", newest.version)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput, TrafficConfig, Route,
)
w = WorkspaceClient()
ENDPOINT, SERVED = dbutils.widgets.get("ranker_endpoint"), "nbo-realtime-ro"
# scale_to_zero_enabled=True so the endpoint costs nothing while idle (see notebook 05 for the same
# choice). Trade-off: after idle it scales to zero and the next request pays a cold start, so warm
# the endpoint before measuring latency (the feature-freshness benchmark) or demoing.
served = [ServedEntityInput(name=SERVED, entity_name=MODEL, entity_version=newest.version,
                            workload_size="Small", scale_to_zero_enabled=True)]
if ENDPOINT in [e.name for e in w.serving_endpoints.list()]:
    w.serving_endpoints.update_config(name=ENDPOINT, served_entities=served)
else:
    w.serving_endpoints.create(name=ENDPOINT, route_optimized=True,
        config=EndpointCoreConfigInput(name=ENDPOINT, served_entities=served,
            traffic_config=TrafficConfig(routes=[Route(served_model_name=SERVED, traffic_percentage=100)])))
print(f"Route-optimized endpoint '{ENDPOINT}' deploying {MODEL} v{newest.version}")

# Endpoint deploy is async — block until READY so the feature-freshness benchmark doesn't hit a cold endpoint.
w.serving_endpoints.wait_get_serving_endpoint_not_updating(name=ENDPOINT)
print(f"{ENDPOINT} is READY.")

# COMMAND ----------
# MAGIC %md ## Grant the benchmark service principal CAN_QUERY (route-optimized query path)
# MAGIC The feature-freshness benchmark queries this **route-optimized** endpoint with an OAuth token *downscoped* to
# MAGIC `query_inference_endpoint`, minted from the SP creds in the `nbo` secret scope. Minting that
# MAGIC token requires the SP to hold `CAN_QUERY` on the endpoint. Since the endpoint is (re)created
# MAGIC here on every run, we (re)apply the grant now — otherwise 10 fails with
# MAGIC `invalid_authorization_details: User is not authorized to the requested authorizations`.
# COMMAND ----------
from databricks.sdk.service.serving import (
    ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel,
)
# Guard the read so a Part-2 run without the `nbo` benchmark scope still finishes: the endpoint is
# already deployed above — don't hard-fail here just because the feature-freshness benchmark's SP is absent (mirrors 05).
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
          "route-optimized latency feature-freshness benchmark.")

# COMMAND ----------
# MAGIC %md ## Rank-all serving — request carries only customer_id + offer fields
# MAGIC The endpoint looks up all 6 customer features (incl. the live `cust_clicks_10m`) online.
# MAGIC
# MAGIC **Auth note:** this is a *route-optimized* endpoint. It accepts **only** an OAuth token
# MAGIC downscoped to the endpoint (`authorization_details`) — **not** a PAT and **not** a job/notebook
# MAGIC runtime token. `serving_endpoints_data_plane.query()` performs that token exchange, but it needs
# MAGIC an interactive OAuth (U2M) client or a service-principal `client_credentials` flow; run as a
# MAGIC serverless **job** it raises `OAuth tokens are not available for runtime authentication`. So this
# MAGIC demo cell is best-effort: it runs when executed interactively and is skipped (not failed) under
# MAGIC job runtime auth. See `feature_freshness_benchmark` for the benchmarked numbers and
# MAGIC https://docs.databricks.com/aws/en/machine-learning/model-serving/query-route-optimization
# COMMAND ----------
def _rank_all(customer_id):
    offers = spark.table(f"`{catalog}`.{schema}.offers").collect()
    recs = [{"customer_id": customer_id, "offer_id": o.offer_id,
             "product_category": o.product_category, "base_reward": float(o.base_reward),
             "tier_requirement": int(o.tier_requirement)} for o in offers]
    return w.serving_endpoints_data_plane.query(
        name=ENDPOINT, dataframe_records=recs).predictions

# Demo with the first available customer. Under job runtime auth the route-optimized token
# exchange is unavailable, so we surface a clear skip instead of failing the notebook.
try:
    sample_customer = spark.table(f"`{catalog}`.{schema}.customers").select("customer_id").first()[0]
    print("rank-all predictions:", _rank_all(sample_customer))
except Exception as e:
    if "OAuth tokens are not available" in str(e):
        print("Skipping live query cell: route-optimized endpoints require an interactive OAuth "
              "(U2M) or service-principal client_credentials token; a serverless job runtime token "
              "cannot query them. Endpoint is deployed and READY — run this cell interactively to score.")
    else:
        raise
