# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Configure catalog/schema, create the Lakebase online store, and set permissions.
# MAGIC
# MAGIC **Prereqs:** DBR 17.0 ML+, `databricks-feature-engineering>=0.16.0`, a catalog on
# MAGIC standard storage (required for streaming Feature Views).

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
dbutils.widgets.text("online_store_name", "nbo-online-store")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
online_store_name = dbutils.widgets.get("online_store_name")

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# MAGIC %md ## Create the Lakebase online store
# MAGIC Name must be DNS-compliant: lowercase, alphanumeric + hyphens, **no underscores**.
# MAGIC NOTE: streaming online materialization (Part 2) is blocked until DBR 19 (~2026-08-11) by
# MAGIC an Eng-confirmed FS bug — the streaming JDBC sink emits the Postgres target with a *quoted*
# MAGIC schema identifier (`"nbo"`), which the pre-DBR-19 validator rejects. Batch (Part 1) is
# MAGIC unaffected. See featureview_sa.md §4 / Pitfall #9.
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Create the Lakebase online store (idempotent: skip if it already exists).
# Both batch (Part 1) and streaming (Part 2) materialization write here, so a clean
# deploy MUST create it — otherwise 08's materialize_features has no online store target.
try:
    existing = fe.get_online_store(name=online_store_name)
except Exception:
    existing = None
if existing is not None:
    print(f"Online store {online_store_name} already exists.")
else:
    fe.create_online_store(name=online_store_name, capacity="CU_1")
    print(f"Created online store {online_store_name} (CU_1). Allow a few min to reach AVAILABLE.")

print(f"Catalog={catalog}  Schema={schema}  OnlineStore={online_store_name}")
