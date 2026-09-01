# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Materialize Features
# MAGIC Provision serverless declarative pipelines to write features to **offline Delta**
# MAGIC (training) and **online Lakebase** (serving). Materialization strategy differs by
# MAGIC feature type. Features were registered in notebook 02, so here we **fetch** them with
# MAGIC `get_feature` (re-creating raises `AlreadyExists`).
# MAGIC
# MAGIC **Verified end-to-end on serverless (`databricks-feature-engineering>=0.16.0`).**
# MAGIC
# MAGIC Gotchas learned the hard way:
# MAGIC - Offline and online destinations **must differ** — use distinct `table_name_prefix`
# MAGIC   (here `nbo_off` vs `nbo_on`), even within the same catalog/schema.
# MAGIC - Aggregation features → `CronSchedule` (offline+online); `ColumnSelection` → `TableTrigger`
# MAGIC   (online-only). Mixing a ColumnSelection into a CronSchedule call is rejected.
# MAGIC - Backfill pipelines run async; materialized Delta tables (`nbo_off_*`, `nbo_on_*`)
# MAGIC   appear once the first backfill completes.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("online_store_name", "nbo")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
osn = dbutils.widgets.get("online_store_name")

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    OfflineStoreConfig, OnlineStoreConfig, CronSchedule, TableTrigger,
)
fe = FeatureEngineeringClient()

# COMMAND ----------
# MAGIC %md ## Fetch the features registered in notebook 02
# COMMAND ----------
def gf(name):
    return fe.get_feature(full_name=f"{catalog}.{schema}.{name}")

agg_features = [gf("cust_avg_balance_30d"), gf("cust_spend_90d"), gf("cust_txn_count_7d")]
attr_features = [gf("cust_loyalty_tier"), gf("cust_risk_band")]

def already_materialized(feature_name: str) -> bool:
    """Re-run guard: materialize_features is not idempotent, so skip features that already
    have a materialization pipeline provisioned."""
    return len(list(fe.list_materialized_features(
        feature_name=f"{catalog}.{schema}.{feature_name}"))) > 0

# COMMAND ----------
# MAGIC %md ## Aggregation features → offline Delta + online Lakebase (CronSchedule + backfill)
# COMMAND ----------
if all(already_materialized(f) for f in ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d"]):
    print("Aggregation features already materialized — skipping.")
else:
    fe.materialize_features(
        features=agg_features,
        offline_config=OfflineStoreConfig(catalog, schema, "nbo_off"),
        online_config=OnlineStoreConfig(catalog, schema, "nbo_on", osn),
        trigger=CronSchedule(quartz_cron_expression="0 0 0 * * ?", timezone_id="UTC"),
    )

# COMMAND ----------
# MAGIC %md ## ColumnSelection features → online-only (TableTrigger)
# COMMAND ----------
if all(already_materialized(f) for f in ["cust_loyalty_tier", "cust_risk_band"]):
    print("Attribute features already materialized — skipping.")
else:
    fe.materialize_features(
        features=attr_features,
        online_config=OnlineStoreConfig(catalog, schema, "nbo_on", osn),
        trigger=TableTrigger(),
    )

# COMMAND ----------
# MAGIC %md ## Inspect the provisioned pipelines, then WAIT for the first online backfill
# MAGIC `materialize_features` provisions the sync pipelines **async** — the online tables
# MAGIC (`nbo_on_*`) are created and populated only once the first backfill completes. The
# MAGIC downstream endpoint (notebook 05) looks these up by `customer_id`, so we **block here until
# MAGIC every online table is queryable**. Without this gate, 05 races ahead (training in 04 is not a
# MAGIC long enough buffer) and fails with
# MAGIC `RESOURCE_DOES_NOT_EXIST: Feature table '...nbo_on_*' does not exist`.
# COMMAND ----------
import time

FEATS = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
         "cust_loyalty_tier", "cust_risk_band"]
online_tables = set()
for f in FEATS:
    for m in fe.list_materialized_features(feature_name=f"{catalog}.{schema}.{f}"):
        print(f"{f:24s} online={m.is_online}  table={m.table_name}")
        if m.is_online:
            # normalize to a fully-qualified 3-level name for the readiness probe
            t = m.table_name
            online_tables.add(t if t.count(".") >= 2 else f"{catalog}.{schema}.{t.split('.')[-1]}")

print("\nWaiting for online tables to finish their first backfill:", sorted(online_tables))
deadline = time.time() + 30 * 60  # generous cap; first backfill is typically a few minutes
pending = set(online_tables)
while pending and time.time() < deadline:
    for t in list(pending):
        try:
            spark.sql(f"SELECT 1 FROM {t} LIMIT 1").collect()
            pending.discard(t)
            print(f"  ready: {t}")
        except Exception:
            pass  # table not created / not queryable yet — keep polling
    if pending:
        time.sleep(20)
if pending:
    raise TimeoutError(
        f"Online tables not queryable after 30 min: {sorted(pending)}. "
        "Inspect the 'Synced table: ...' pipelines before deploying the endpoint (05).")
print("All online tables ready — safe to train (04) and deploy the endpoint (05).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming features (RollingWindow over MSK) — online-only via `StreamingMode`
# MAGIC This is a Part 2 concern; see notebook 08 (currently gated — read its preflight cell):
# MAGIC ```python
# MAGIC from databricks.feature_engineering.entities import StreamingMode
# MAGIC fe.materialize_features(
# MAGIC     features=[clicks_10m],
# MAGIC     online_config=OnlineStoreConfig(catalog, schema, "nbo_stream", osn),
# MAGIC     trigger=StreamingMode(),
# MAGIC )
# MAGIC ```
