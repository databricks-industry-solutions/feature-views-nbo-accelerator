# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Define Feature Views
# MAGIC The heart of the accelerator. We declare **one set of Feature objects** that will power
# MAGIC both offline training (point-in-time) and online serving. Three modes are exercised:
# MAGIC
# MAGIC | Mode | API | Example feature |
# MAGIC |---|---|---|
# MAGIC | Batch, overlapping | `SlidingWindow` | 30-day avg balance |
# MAGIC | Batch, fixed bucket | `TumblingWindow` | 90-day total spend by category |
# MAGIC | Streaming, in-session | `RollingWindow` (Kafka) | clicks/dwell in last 10 min |
# MAGIC | Latest attribute | `ColumnSelection` | loyalty tier, risk band |

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from datetime import timedelta
from databricks.feature_engineering import FeatureEngineeringClient
# API names per databricks-feature-engineering>=0.16.0
from databricks.feature_engineering import (
    Feature, AggregationFunction, DeltaTableSource, StreamSource,
    SlidingWindow, TumblingWindow, RollingWindow, ColumnSelection,
)
from databricks.feature_engineering.aggregations import Avg, Sum, Count, Max

fe = FeatureEngineeringClient()

# COMMAND ----------
# MAGIC %md ## Batch source + windowed features
txn_source = DeltaTableSource(
    catalog_name=catalog, schema_name=schema, table_name="transactions",
    filter_condition="amount > 0",
)

avg_balance_30d = Feature(
    source=txn_source, entity=["customer_id"], timeseries_column="ts",
    function=AggregationFunction(
        operator=Avg(input="balance"),
        time_window=SlidingWindow(window_duration=timedelta(days=30)),
    ),
    name="cust_avg_balance_30d",
)

spend_90d = Feature(
    source=txn_source, entity=["customer_id"], timeseries_column="ts",
    function=AggregationFunction(
        operator=Sum(input="amount"),
        time_window=TumblingWindow(window_duration=timedelta(days=90)),
    ),
    name="cust_spend_90d",
)

# COMMAND ----------
# MAGIC %md ## Latest-attribute features (ColumnSelection)
cust_source = DeltaTableSource(catalog_name=catalog, schema_name=schema, table_name="customers")

loyalty_tier = Feature(
    source=cust_source, entity=["customer_id"], timeseries_column="updated_at",
    function=ColumnSelection("loyalty_tier"), name="cust_loyalty_tier",
)
risk_band = Feature(
    source=cust_source, entity=["customer_id"], timeseries_column="updated_at",
    function=ColumnSelection("risk_band"), name="cust_risk_band",
)

# COMMAND ----------
# MAGIC %md ## Streaming in-session features (RollingWindow over Kafka)
# MAGIC Requires a UC Kafka connection + the topic/producer from **notebook 01b**.
# MAGIC JSON payload exposed under `value.*`. Streaming supports a limited operator set:
# MAGIC Count, Avg, Sum, StddevPop, Max, Min, Last.
# MAGIC
# MAGIC First register the topic as a governed stream (uses the same UC connection as 01b):
# MAGIC ```python
# MAGIC fe.create_stream(
# MAGIC     name=f"{catalog}.{schema}.session_events_stream",
# MAGIC     source_config=KafkaStreamConfig(
# MAGIC         subscription_mode=KafkaSubscriptionMode(subscribe="nbo-session-events")),
# MAGIC     connection_config=StreamConnectionConfig(uc_connection_name="<kafka_connection>"),
# MAGIC     schema_config=DirectSchemas(payload_schema=SchemaConfig(json_schema="{...}")),
# MAGIC     ingestion_config=IngestionConfig(
# MAGIC         ingestion_destination=IngestionDestination(
# MAGIC             delta_table_name=f"{catalog}.{schema}.session_events_ingest"),
# MAGIC         deduplication_columns=["value.event_id"]),
# MAGIC )
# MAGIC ```
stream_source = StreamSource(full_name=f"{catalog}.{schema}.session_events_stream")

clicks_10m = Feature(
    source=stream_source, entity=["value.customer_id"],
    timeseries_column="value.event_time",
    function=AggregationFunction(
        operator=Count(input="value.event_id"),
        time_window=RollingWindow(window_duration=timedelta(minutes=10)),
    ),
    name="cust_clicks_10m",
)

# TODO: add dwell-time, product-page-views-per-category, calculator-usage features

# COMMAND ----------
# MAGIC %md ## Register features
# MAGIC In >=0.16.0 use `create_feature()` / `register_feature()` (Beta FVs must be
# MAGIC re-created by 2026-07-22). Persist the feature list for notebooks 03 & 04.
ALL_FEATURES = [avg_balance_30d, spend_90d, loyalty_tier, risk_band, clicks_10m]
# TODO: fe.create_feature(...) for each
print(f"Defined {len(ALL_FEATURES)} features")
