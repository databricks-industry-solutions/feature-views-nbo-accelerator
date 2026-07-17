# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Materialize Features
# MAGIC Provision serverless pipelines to write features to offline Delta (training) and
# MAGIC online Lakebase (serving). Materialization strategy differs by feature type.

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
dbutils.widgets.text("online_store_name", "nbo-online-store")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
online_store_name = dbutils.widgets.get("online_store_name")

from databricks.feature_engineering import (
    FeatureEngineeringClient, OfflineStoreConfig, OnlineStoreConfig,
    CronSchedule, TableTrigger, StreamingMode,
)
fe = FeatureEngineeringClient()
# from notebook 02: avg_balance_30d, spend_90d, loyalty_tier, risk_band, clicks_10m

# COMMAND ----------
# MAGIC %md ## Aggregation features → offline Delta + online (needs CronSchedule + backfill)
# fe.materialize_features(
#     features=[avg_balance_30d, spend_90d],
#     offline_config=OfflineStoreConfig(catalog_name=catalog, schema_name=schema,
#                                        table_name_prefix="features"),
#     online_config=OnlineStoreConfig(catalog_name=catalog, schema_name=schema,
#                                     online_store_name=online_store_name),
#     trigger=CronSchedule(quartz_cron_expression="0 0 0 * * ?", timezone_id="UTC"),
# )

# COMMAND ----------
# MAGIC %md ## ColumnSelection features → online-only (TableTrigger)
# fe.materialize_features(
#     features=[loyalty_tier, risk_band],
#     online_config=OnlineStoreConfig(catalog_name=catalog, schema_name=schema,
#                                     online_store_name=online_store_name),
#     trigger=TableTrigger(),
# )

# COMMAND ----------
# MAGIC %md ## Streaming features → online-only (StreamingMode)
# MAGIC Auto-maintains a Delta ingestion table (also feeds point-in-time training in nb 04).
# MAGIC Starts at latest Kafka offset; use StreamBackfillSource to replay history.
# fe.materialize_features(
#     features=[clicks_10m],
#     online_config=OnlineStoreConfig(catalog_name=catalog, schema_name=schema,
#                                     online_store_name=online_store_name),
#     trigger=StreamingMode(),
# )
print("TODO: uncomment materialization calls once features are registered")
