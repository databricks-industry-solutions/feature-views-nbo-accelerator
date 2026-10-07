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
ATTR = ["cust_loyalty_tier", "cust_risk_band", "cust_annual_income", "cust_tenure_months"]
attr_features = [gf(n) for n in ATTR]

def already_materialized(feature_name: str, online: bool = False) -> bool:
    """Re-run guard: materialize_features is not idempotent, so skip features that already have a
    materialization pipeline. With online=True, only an online materialization in THIS store (osn)
    counts: a model's features must all live in one online store, so one materialized into a
    different store does not satisfy it."""
    ms = list(fe.list_materialized_features(feature_name=f"{catalog}.{schema}.{feature_name}"))
    if not online:
        return len(ms) > 0
    return any(m.is_online and getattr(getattr(m, "online_store_config", None), "online_store_name", None) == osn
               for m in ms)

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
# Per-feature guard: an existing deployment already has tier/risk materialized, and the income/tenure
# features were added later, so only materialize the ones that are missing.
todo = [f for n, f in zip(ATTR, attr_features) if not already_materialized(n, online=True)]
if not todo:
    print(f"Attribute features already materialized in online store '{osn}' — skipping.")
else:
    print(f"Materializing into '{osn}':", [n for n in ATTR if not already_materialized(n, online=True)])
    fe.materialize_features(
        features=todo,
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

FEATS = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d"] + ATTR
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
# Ready = queryable AND has ≥1 row: a synced table is often created (so SELECT succeeds) before its
# first backfill lands rows, and gating on existence alone would declare "ready" over an empty store
# and let 05's endpoint look up nulls. Track queryable-vs-not so the deadline distinguishes a table
# that never came up (hard failure) from one that's queryable but still empty (warn, don't hard-fail
# — a feature could legitimately backfill zero rows).
pending = set(online_tables)          # not yet confirmed backfilled (queryable + ≥1 row)
never_queryable = set(online_tables)  # never returned from a SELECT (missing / not provisioned)
while pending and time.time() < deadline:
    for t in list(pending):
        try:
            n = spark.sql(f"SELECT 1 FROM {t} LIMIT 1").count()
            never_queryable.discard(t)  # the SELECT returned → table exists and is queryable
            if n > 0:
                pending.discard(t)
                print(f"  ready (backfilled): {t}")
        except Exception:
            pass  # table not created / not queryable yet — keep polling
    if pending:
        time.sleep(20)
if pending:
    unqueryable = pending & never_queryable
    empty = pending - never_queryable
    if unqueryable:
        raise TimeoutError(
            f"Online tables not queryable after 30 min: {sorted(unqueryable)}. "
            "Inspect the 'Synced table: ...' pipelines before deploying the endpoint (05).")
    print(f"WARNING: online tables queryable but still empty after 30 min: {sorted(empty)}. "
          "Proceeding — but 05's endpoint may look up nulls for these until their backfill lands.")
print("All online tables ready — safe to train (04) and deploy the endpoint (05).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming features (RollingWindow over MSK) — online-only via `StreamingMode`
# MAGIC This is a Part 2 concern; see notebook 07 (currently gated — read its preflight cell):
# MAGIC ```python
# MAGIC from databricks.feature_engineering.entities import StreamingMode
# MAGIC fe.materialize_features(
# MAGIC     features=[clicks_10m],
# MAGIC     online_config=OnlineStoreConfig(catalog, schema, "nbo_stream", osn),
# MAGIC     trigger=StreamingMode(),
# MAGIC )
# MAGIC ```
