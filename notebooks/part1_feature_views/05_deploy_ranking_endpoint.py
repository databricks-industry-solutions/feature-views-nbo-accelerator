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
# MAGIC %md ## Features: 7 customer features (online lookup) + offer and visitor columns (RequestSource)
# MAGIC - **Online lookup by `customer_id`:** balances, spend, txn count, tier, risk, income, tenure.
# MAGIC - **Request-time (`RequestSource`):** the 4 offer columns, plus the visitor's OfferMatch answers
# MAGIC   (`ctx_goal`, `ctx_credit`, `ctx_income`, `ctx_card_spend`) and the visitor's website clicks per
# MAGIC   category this session (`ctx_session_cat_views`). The app sends these with every request.
# COMMAND ----------
def gf(n):
    return fe.get_feature(full_name=f"{catalog}.{schema}.{n}")

CUST = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
        "cust_loyalty_tier", "cust_risk_band", "cust_annual_income", "cust_tenure_months"]
cust_feats = [gf(n) for n in CUST]

# Declare the request-time offer columns as a RequestSource, then register a passthrough
# ColumnSelection feature per column (name MUST equal the column name).
req = RequestSource(schema=[
    FieldDefinition(name="offer_id", data_type=ScalarDataType.STRING),
    FieldDefinition(name="product_category", data_type=ScalarDataType.STRING),
    FieldDefinition(name="base_reward", data_type=ScalarDataType.DOUBLE),
    FieldDefinition(name="tier_requirement", data_type=ScalarDataType.INTEGER),
    FieldDefinition(name="ctx_goal", data_type=ScalarDataType.STRING),
    FieldDefinition(name="ctx_credit", data_type=ScalarDataType.STRING),
    FieldDefinition(name="ctx_income", data_type=ScalarDataType.DOUBLE),
    FieldDefinition(name="ctx_card_spend", data_type=ScalarDataType.DOUBLE),
    # Website clicks in this offer's category during the visitor's session (last 10 min). The app counts
    # them and sends one value per offer row, so a click re-ranks on the very next request.
    FieldDefinition(name="ctx_session_cat_views", data_type=ScalarDataType.INTEGER),
])

def get_or_create_req(colname):
    try:
        return fe.get_feature(full_name=f"{catalog}.{schema}.{colname}")
    except Exception:
        return fe.create_feature(source=req, function=ColumnSelection(column=colname),
                                 catalog_name=catalog, schema_name=schema, name=colname)

REQ = ["offer_id", "product_category", "base_reward", "tier_requirement",
       "ctx_goal", "ctx_credit", "ctx_income", "ctx_card_spend", "ctx_session_cat_views"]
offer_feats = [get_or_create_req(c) for c in REQ]

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

# offer_id is an identifier, not a preference signal. Encoding it made one arbitrary offer dominate.
CAT = ["product_category", "cust_loyalty_tier", "cust_risk_band", "ctx_goal", "ctx_credit"]
NUM = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d", "cust_annual_income", "cust_tenure_months",
       "base_reward", "tier_requirement", "ctx_income", "ctx_card_spend", "ctx_session_cat_views"]
X = tdf[CAT + NUM].copy()
for c in CAT:
    X[c] = X[c].astype(str)
for c in NUM:
    X[c] = pd.to_numeric(X[c], errors="coerce")
y = tdf["accepted"].astype(int)
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Serve the acceptance PROBABILITY, not the 0/1 class. A plain LGBMClassifier.predict returns the
# hard class label, so the served ranker would return 0/1 and every offer would tie — no ranking.
# Overriding predict to return predict_proba[:, 1] makes the endpoint emit P(accept) so the catalog
# ranks with a real spread. (Logged with cloudpickle below so this notebook-defined subclass
# serializes by value and loads at serving without an import.)
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
val_auc = roc_auc_score(yte, model.predict_proba(Xte)[:, 1])
print("val_auc:", round(val_auc, 4))

# COMMAND ----------
# MAGIC %md ## Personalization check: the top offer must differ across customers
# MAGIC Rank all 40 offers for 2,000 customers (features as of now, neutral OfferMatch answers) and
# MAGIC check the top-1 mix. If one category wins for most customers, the demo can't show personalization.
# COMMAND ----------
cust_sample = (spark.table(f"`{catalog}`.{schema}.customers").select("customer_id", "risk_band", "annual_income")
               .orderBy(F.xxhash64("customer_id")).limit(2000))
grid = (cust_sample.crossJoin(offers)
        .withColumn("ts", F.current_timestamp())          # timestamp key for the transaction windows
        .withColumn("updated_at", F.col("ts"))            # timestamp key for the ColumnSelection features
        .withColumn("ctx_goal", F.lit("none"))
        .withColumn("ctx_credit", F.expr("CASE risk_band WHEN 'low' THEN 'excellent' WHEN 'medium' THEN 'good' ELSE 'fair' END"))
        .withColumn("ctx_income", F.col("annual_income").cast("double"))
        .withColumn("ctx_card_spend", (F.col("annual_income") / 12 * 0.1).cast("double"))
        .withColumn("ctx_session_cat_views", F.lit(0).cast("int"))
        .drop("risk_band", "annual_income"))
gdf = fe.create_training_set(df=grid, features=cust_feats + offer_feats, label=None,
                             exclude_columns=["ts", "updated_at"]).load_df().toPandas()
GX = gdf[CAT + NUM].copy()
for c in CAT:
    GX[c] = GX[c].astype(str)
for c in NUM:
    GX[c] = pd.to_numeric(GX[c], errors="coerce")
gdf["score"] = model.predict_proba(GX)[:, 1]
top1 = gdf.loc[gdf.groupby("customer_id")["score"].idxmax()]
mix = top1["product_category"].value_counts(normalize=True).round(3)
print("val_auc:", round(val_auc, 4), "| distinct top-1 offers:", top1["offer_id"].nunique())
print("top-1 category share:\n", mix.to_string())
assert val_auc >= 0.70, f"val AUC {val_auc:.3f} < 0.70: labels carry too little signal"
assert mix.max() <= 0.5, f"one category wins for {mix.max():.0%} of customers: not personalized"
if mix.max() > 0.4:
    print(f"WARNING: top category share {mix.max():.0%} is above the 40% target.")

# An explicit OfferMatch answer must visibly change the recommendation. Re-score the same customer ×
# offer grid with each selected goal; at least 60% of customers must get that category at rank #1.
# This catches the demo-breaking failure where Starter Secured Card wins for nearly every answer.
goal_hit_rate = {}
base_cols = gdf[["customer_id", "offer_id", "product_category"]].copy()
for goal in ["credit_card", "savings", "personal_loan", "mortgage", "investment"]:
    Q = gdf[CAT + NUM].copy()
    Q["ctx_goal"] = goal
    for c in CAT:
        Q[c] = Q[c].astype(str)
    for c in NUM:
        Q[c] = pd.to_numeric(Q[c], errors="coerce")
    ranked = base_cols.copy()
    ranked["score"] = model.predict_proba(Q)[:, 1]
    best = ranked.loc[ranked.groupby("customer_id")["score"].idxmax()]
    goal_hit_rate[goal] = round(float((best["product_category"] == goal).mean()), 3)
print("OfferMatch goal -> top-category hit rate:", goal_hit_rate)
assert min(goal_hit_rate.values()) >= 0.60, (
    f"OfferMatch answers do not drive ranking strongly enough: {goal_hit_rate}")

# The three guest controls must change WHICH product wins within the selected category, not merely
# change its probability. This is the customer-visible personalization contract for OfferMatch.
def top_offer_for(category, **overrides):
    mask = gdf["product_category"] == category
    Q = gdf.loc[mask, CAT + NUM].copy()
    for col, value in overrides.items():
        Q[col] = value
    for c in CAT:
        Q[c] = Q[c].astype(str)
    for c in NUM:
        Q[c] = pd.to_numeric(Q[c], errors="coerce")
    ranked = gdf.loc[mask, ["customer_id", "offer_id"]].copy()
    ranked["score"] = model.predict_proba(Q)[:, 1]
    return ranked.loc[ranked.groupby("customer_id")["score"].idxmax()].set_index("customer_id")["offer_id"]

def switch_rate(a, b):
    joined = pd.concat([a.rename("a"), b.rename("b")], axis=1).dropna()
    return round(float((joined["a"] != joined["b"]).mean()), 3)

control_switch = {
    "credit": switch_rate(
        top_offer_for("credit_card", ctx_goal="credit_card", ctx_credit="fair", ctx_card_spend=1500),
        top_offer_for("credit_card", ctx_goal="credit_card", ctx_credit="excellent", ctx_card_spend=1500)),
    "income": switch_rate(
        top_offer_for("mortgage", ctx_goal="mortgage", ctx_income=45000),
        top_offer_for("mortgage", ctx_goal="mortgage", ctx_income=250000)),
    "card_spend": switch_rate(
        top_offer_for("credit_card", ctx_goal="credit_card", ctx_credit="good", ctx_card_spend=250),
        top_offer_for("credit_card", ctx_goal="credit_card", ctx_credit="good", ctx_card_spend=7000)),
}
print("OfferMatch control -> top-product switch rate:", control_switch)
assert min(control_switch.values()) >= 0.20, (
    f"OfferMatch controls do not change the recommended product often enough: {control_switch}")

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
                 # cloudpickle (not skops) so the notebook-defined ProbaLGBM subclass serializes by
                 # value and loads at serving without needing an importable module. skops cannot
                 # serialize a custom notebook class; the FS signature still comes from the training set.
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
ENDPOINT, SERVED = dbutils.widgets.get("ranker_endpoint"), "nbo-online-ro"
# Demo endpoint stays provisioned so user actions never pay a cold start. `scripts/demo_power.sh pause`
# must restore scale-to-zero after the demo to avoid idle serving cost.
served = [ServedEntityInput(name=SERVED, entity_name=MODEL, entity_version=newest.version,
                            workload_size="Small", scale_to_zero_enabled=False)]
if ENDPOINT in [e.name for e in w.serving_endpoints.list()]:
    w.serving_endpoints.update_config(name=ENDPOINT, served_entities=served)
else:
    w.serving_endpoints.create(name=ENDPOINT, route_optimized=True,
        config=EndpointCoreConfigInput(name=ENDPOINT, served_entities=served,
            traffic_config=TrafficConfig(routes=[Route(served_model_name=SERVED, traffic_percentage=100)])))

# Endpoint deploy is async. Block until the config update finishes and the endpoint is READY
# so the downstream latency feature-serving benchmark doesn't query a not-ready / cold endpoint.
print(f"Waiting for {ENDPOINT} to become READY (build + provision can take ~10-20 min on first deploy)...")
w.serving_endpoints.wait_get_serving_endpoint_not_updating(name=ENDPOINT)
print(f"{ENDPOINT} is READY.")

# COMMAND ----------
# MAGIC %md ## Grant the benchmark service principal CAN_QUERY (route-optimized query path)
# MAGIC The feature-serving benchmark queries this **route-optimized** endpoint with an OAuth token *downscoped* to
# MAGIC `query_inference_endpoint`, minted from the SP creds in the `nbo` secret scope. Minting that
# MAGIC token requires the SP to hold `CAN_QUERY` on the endpoint. Since the endpoint is (re)created
# MAGIC here on every run, we (re)apply the grant now — otherwise 06 fails with
# MAGIC `invalid_authorization_details: User is not authorized to the requested authorizations`.
# COMMAND ----------
from databricks.sdk.service.serving import (
    ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel,
)
# The benchmark SP lives in the `nbo` secret scope, which only the route-optimized latency
# feature-serving benchmark needs. Guard the read so a Part-1-only run without that scope still finishes:
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
          "route-optimized latency feature-serving benchmark.")

# COMMAND ----------
# MAGIC %md ## Query — request carries only customer_id + offer fields; features fetched online
# MAGIC ```python
# MAGIC dp = w.serving_endpoints_data_plane   # route-optimized → data-plane client
# MAGIC recs = [{"customer_id": cid, "offer_id": o.offer_id, "product_category": o.product_category,
# MAGIC          "base_reward": o.base_reward, "tier_requirement": o.tier_requirement,
# MAGIC          "ctx_goal": "none", "ctx_credit": "good", "ctx_income": 85000.0, "ctx_card_spend": 1500.0,
# MAGIC          "ctx_session_cat_views": 0}
# MAGIC         for o in offers]
# MAGIC preds = dp.query(name="nbo-ranker-online", dataframe_records=recs).predictions
# MAGIC ```
