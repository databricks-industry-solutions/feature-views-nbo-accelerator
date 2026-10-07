# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 10 · Feature Serving Endpoint (profile signals for the app)
# MAGIC The ranker endpoint (notebook 09) returns one score per offer. The app also shows the visitor
# MAGIC *why*: their stored attributes, their in-session activity, and derived signals. Those come from a
# MAGIC **Feature Serving endpoint** over a FeatureSpec built from the same Feature Views, so the app reads
# MAGIC governed features through one API and never queries the online store directly.
# MAGIC
# MAGIC The spec mixes every kind of feature in this accelerator:
# MAGIC
# MAGIC | Kind | Feature | Computed |
# MAGIC |---|---|---|
# MAGIC | Batch ColumnSelection | tier, risk, income, tenure | materialized (Lakebase) |
# MAGIC | Batch window | `cust_spend_90d`, `cust_avg_balance_30d` | materialized (Lakebase) |
# MAGIC | Streaming RollingWindow | `cust_clicks_10m`, `cust_mobile_cat_views_10m` | materialized (StreamingMode) |
# MAGIC | Streaming **SawtoothWindow** (Beta) | `cust_cat_views_30d` | materialized (StreamingMode) |
# MAGIC | **CustomUDF** (Beta) | `cust_spend_to_income` | on demand, at request time |
# MAGIC
# MAGIC Request: one row per product category, `{customer_id, product_category}`. Category-keyed features
# MAGIC come back per row; customer-keyed ones repeat.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.18.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("feature_endpoint", "nbo-customer-features")
dbutils.widgets.dropdown("allow_streaming_online", "false", ["false", "true"])
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
ENDPOINT = dbutils.widgets.get("feature_endpoint")

if dbutils.widgets.get("allow_streaming_online") != "true":
    msg = "Feature Serving endpoint skipped: Part 2 streaming is gated (allow_streaming_online=false)."
    print(msg); dbutils.notebook.exit(msg)

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import CustomUDF, FeatureViewSource
fe = FeatureEngineeringClient()

def gf(n):
    return fe.get_feature(full_name=f"{catalog}.{schema}.{n}")

# COMMAND ----------
# MAGIC %md ## 1 · A derived feature with CustomUDF (no serving-side code)
# MAGIC `spend_to_income` annualizes 90-day card spend and divides by income. It is a Unity Catalog
# MAGIC function, bound to two upstream features through a `FeatureViewSource`, and evaluated per request
# MAGIC at serving time. Offline it gets `None` for missing inputs, online it gets `NaN`; it handles both.
# COMMAND ----------
UDF = f"{catalog}.{schema}.spend_to_income"
# IF NOT EXISTS, never OR REPLACE: a CustomUDF feature binds to the function it was created against, and
# replacing the function gives it a new identity, which breaks every feature built on it.
spark.sql(f"""
CREATE FUNCTION IF NOT EXISTS {UDF}(spend DOUBLE, income DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'Annualized 90-day spend divided by annual income (NBO accelerator)'
AS $$
import math
if spend is None or income is None or math.isnan(spend) or math.isnan(income) or income <= 0:
    return None
return float(spend) * 4.0 / float(income)
$$
""")

spend_90d, income = gf("cust_spend_90d"), gf("cust_annual_income")
try:
    spend_to_income = gf("cust_spend_to_income")
except Exception as get_err:
    print(f"get_feature(cust_spend_to_income): {type(get_err).__name__}: {str(get_err)[:300]}")
    spend_to_income = fe.create_feature(
        name="cust_spend_to_income",
        source=FeatureViewSource(features=[spend_90d, income]),
        # Registered upstreams bind by their full three-part name.
        function=CustomUDF(function_name=UDF, input_bindings={
            "spend": spend_90d.full_name, "income": income.full_name}),
        catalog_name=catalog, schema_name=schema)
    print("Created CustomUDF feature cust_spend_to_income.")

# COMMAND ----------
# MAGIC %md ## 2 · FeatureSpec
# COMMAND ----------
NAMES = ["cust_loyalty_tier", "cust_risk_band", "cust_annual_income", "cust_tenure_months",
         "cust_spend_90d", "cust_avg_balance_30d", "cust_clicks_10m", "cust_mobile_cat_views_10m"]
spec_features = [gf(n) for n in NAMES] + [spend_to_income]
# Sawtooth (Beta): include only if notebook 07 materialized it online (a spec over a feature with no
# online table cannot be served).
saw_online = any(m.is_online for m in fe.list_materialized_features(
    feature_name=f"{catalog}.{schema}.cust_cat_views_30d"))
if saw_online:
    spec_features.append(gf("cust_cat_views_30d"))
else:
    print("cust_cat_views_30d is not materialized online; serving the spec without the Sawtooth feature.")

SPEC = f"{catalog}.{schema}.nbo_customer_profile"
try:
    fe.create_feature_spec(name=SPEC, features=spec_features)
    print(f"Created FeatureSpec {SPEC} with {len(spec_features)} features.")
except Exception as e:
    if "already exists" not in str(e).lower():
        raise
    print(f"FeatureSpec {SPEC} already exists; reusing it.")

# COMMAND ----------
# MAGIC %md ## 3 · Route-optimized Feature Serving endpoint
# MAGIC Created with the SDK, not `fe.create_feature_serving_endpoint`: route optimization is a
# MAGIC creation-time property and the `fe.*` helper cannot set it.
# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput,
    ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel,
)
w = WorkspaceClient()
served = [ServedEntityInput(entity_name=SPEC, workload_size="Small", scale_to_zero_enabled=False)]
if ENDPOINT in [e.name for e in w.serving_endpoints.list()]:
    w.serving_endpoints.update_config(name=ENDPOINT, served_entities=served)
else:
    w.serving_endpoints.create(name=ENDPOINT, route_optimized=True,
                               config=EndpointCoreConfigInput(name=ENDPOINT, served_entities=served))
print(f"Waiting for {ENDPOINT} to become READY...")
w.serving_endpoints.wait_get_serving_endpoint_not_updating(name=ENDPOINT)
ep = w.serving_endpoints.get(ENDPOINT)
assert ep.route_optimized, f"{ENDPOINT} was created WITHOUT route optimization: delete and recreate"
print(f"{ENDPOINT} is READY and route-optimized.")

# Grant the query service principal CAN_QUERY (same SP the benchmarks and app use).
try:
    sp_client_id = dbutils.secrets.get("nbo", "sp_client_id")
except Exception:
    sp_client_id = None
if sp_client_id:
    w.serving_endpoints.update_permissions(
        serving_endpoint_id=ep.id,
        access_control_list=[ServingEndpointAccessControlRequest(
            service_principal_name=sp_client_id, permission_level=ServingEndpointPermissionLevel.CAN_QUERY)])
    print(f"Granted CAN_QUERY on {ENDPOINT} to {sp_client_id}.")

# COMMAND ----------
# MAGIC %md ## Query shape (route-optimized: OAuth token scoped to the endpoint)
# MAGIC ```python
# MAGIC rows = [{"customer_id": "cust_55310", "product_category": c}
# MAGIC         for c in ["credit_card", "savings", "personal_loan", "mortgage", "investment"]]
# MAGIC w.serving_endpoints_data_plane.query(name="nbo-customer-features", dataframe_records=rows)
# MAGIC ```
