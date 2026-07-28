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
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# TODO: create the online store (idempotent-guard in real notebook)
# fe.create_online_store(name=online_store_name, capacity="CU_1")

print(f"Catalog={catalog}  Schema={schema}  OnlineStore={online_store_name}")
