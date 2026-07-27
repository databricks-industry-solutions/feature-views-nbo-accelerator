# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 08 · Streaming Feature Views — Freshest In-Session Intent
# MAGIC Registers the MSK topic (notebook 07) as a governed **stream** and declares a
# MAGIC `RollingWindow` streaming feature (`cust_clicks_10m`) — the blog's headline differentiator:
# MAGIC *capture the customer's freshest in-session intent* and reuse the same definition offline.
# MAGIC
# MAGIC **Prereqs**
# MAGIC - Notebook 07 has produced events to the `nbo-session-events` topic on `msk_kafka`.
# MAGIC - Catalog on **standard storage** (`fins-industry-solutions` qualifies) — streaming FVs
# MAGIC   cannot use default storage.
# MAGIC - `databricks-feature-engineering>=0.16.0` on **serverless (latest env)**.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. `create_stream` — register the Kafka topic as a UC-governed stream (auto-maintains a Delta
# MAGIC    ingestion table that also feeds point-in-time training).
# MAGIC 2. `create_feature` — a `RollingWindow(10 min)` count of in-session events per customer.
# MAGIC 3. `materialize_features` with `StreamingMode()` — online-only, continuously maintained.
# MAGIC
# MAGIC The resulting `cust_clicks_10m` feature is looked up online by the **same** ranker endpoint
# MAGIC from Part 1 (05) — it just gains a fourth online-looked-up feature. No ranker code changes;
# MAGIC re-log with the streaming feature added to the training set to include it.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins-industry-solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("kafka_connection", "msk_kafka")
dbutils.widgets.text("topic", "nbo-session-events")
dbutils.widgets.text("online_store_name", "nbo-online-store")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
conn_name = dbutils.widgets.get("kafka_connection")
topic = dbutils.widgets.get("topic")
osn = dbutils.widgets.get("online_store_name")

from datetime import timedelta
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    StreamSource, KafkaStreamConfig, KafkaSubscriptionMode, StreamConnectionConfig,
    DirectSchemas, SchemaConfig, IngestionConfig, IngestionDestination,
    AggregationFunction, Count, RollingWindow, OnlineStoreConfig, StreamingMode,
)
fe = FeatureEngineeringClient()

# COMMAND ----------
# MAGIC %md ## 1 · Register the Kafka topic as a governed stream
# MAGIC The payload schema mirrors the producer in notebook 07, in **JSON Schema** format (not Spark
# MAGIC StructType JSON). `event_time` must be a **date-time string** — the streaming FV's timeseries
# MAGIC column must be TIMESTAMP, so epoch-millis integers are rejected. Fields are under `value.*`.
SESSION_EVENT_JSON_SCHEMA = """
{
  "type": "object",
  "properties": {
    "event_id":         {"type": "string"},
    "customer_id":      {"type": "string"},
    "event_time":       {"type": "string", "format": "date-time"},
    "event_type":       {"type": "string"},
    "product_category": {"type": "string"},
    "dwell_ms":         {"type": "integer"},
    "device":           {"type": "string"}
  }
}
"""

STREAM_NAME = f"{catalog}.{schema}.session_events_stream"
try:
    fe.get_stream(name=STREAM_NAME)
    print(f"Stream {STREAM_NAME} already exists.")
except Exception:
    fe.create_stream(
        name=STREAM_NAME,
        source_config=KafkaStreamConfig(
            subscription_mode=KafkaSubscriptionMode(subscribe=topic)),
        connection_config=StreamConnectionConfig(uc_connection_name=conn_name),
        schema_config=DirectSchemas(
            payload_schema=SchemaConfig(json_schema=SESSION_EVENT_JSON_SCHEMA)),
        ingestion_config=IngestionConfig(
            ingestion_destination=IngestionDestination(
                delta_table_name=f"{catalog}.{schema}.session_events_ingest"),
            deduplication_columns=["value.event_id"]),
    )
    print(f"Created stream {STREAM_NAME}.")

# COMMAND ----------
# MAGIC %md ## 2 · Declare the RollingWindow streaming feature
# MAGIC In-session click count over the last 10 minutes, keyed by `value.customer_id`.
# MAGIC Streaming supports a limited operator set (Count, Avg, Sum, StddevPop, Max, Min, Last).
stream_source = StreamSource(full_name=STREAM_NAME)

clicks_10m = fe.create_feature(
    source=stream_source,
    entity=["value.customer_id"],
    timeseries_column="value.event_time",
    function=AggregationFunction(
        operator=Count(input="value.event_id"),
        time_window=RollingWindow(window_duration=timedelta(minutes=10))),
    catalog_name=catalog, schema_name=schema, name="cust_clicks_10m",
)

# COMMAND ----------
# MAGIC %md ## 3 · Materialize online-only with StreamingMode
# MAGIC Continuously maintained in the online store; distinct table prefix from the batch features.
fe.materialize_features(
    features=[clicks_10m],
    online_config=OnlineStoreConfig(catalog, schema, "nbo_stream", osn),
    trigger=StreamingMode(),
)
print("Streaming materialization started for cust_clicks_10m.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next → notebook 09 (real-time serving)
# MAGIC Notebook 09 re-logs the ranker with `cust_clicks_10m` added to the online-lookup training
# MAGIC set (same pattern as Part 1 · 05, now with 6 looked-up features). The endpoint then fetches
# MAGIC all customer features — batch + streaming — by `customer_id` at request time, and ranks all
# MAGIC offers. Serving latency + event→online freshness are measured live in notebook 10.
