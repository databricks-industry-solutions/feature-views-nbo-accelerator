# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 08 · Streaming Feature View — Freshest In-Session Intent
# MAGIC Defines a **streaming** Feature View over Kafka/MSK, strictly following the Databricks docs:
# MAGIC - Streams: <https://docs.databricks.com/aws/en/machine-learning/feature-store/streams>
# MAGIC - Feature Views (streaming): <https://docs.databricks.com/aws/en/machine-learning/feature-store/feature-views#streaming-features>
# MAGIC
# MAGIC The streaming feature `cust_clicks_10m` (count of in-session events per customer over a
# MAGIC 10-minute rolling window) captures the customer's freshest in-session intent and, once
# MAGIC materialized, serves to the model endpoint at ~200ms p99 freshness.
# MAGIC
# MAGIC **Prereqs:** `databricks-feature-engineering>=0.16.0`, DBR 17.0 ML+, enterprise workspace
# MAGIC with Lakebase, and a **standard-storage UC catalog** (`fins_industry_solutions` qualifies).
# MAGIC The MSK topic exists (notebook 07).
# MAGIC
# MAGIC **Per the docs:**
# MAGIC - `create_stream` starts a **managed, continuous ingestion pipeline** that reads the topic
# MAGIC   into the ingestion Delta table, starting from the **latest offset** (so produce events
# MAGIC   *after* this notebook, or supply a `StreamBackfillSource` for history).
# MAGIC - Column refs are prefixed with **`value.`** (Kafka payload); the JSON Schema declares
# MAGIC   `event_time` as `{"type": "string", "format": "date-time"}` so the timeseries column is TIMESTAMP.
# MAGIC - Streaming features support **`RollingWindow` only** (no Sliding/Tumbling) and materialize
# MAGIC   **online-only** with `StreamingMode()`.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
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
    KafkaStreamConfig, KafkaSubscriptionMode, StreamConnectionConfig,
    DirectSchemas, SchemaConfig, IngestionConfig, IngestionDestination,
    StreamSource, Feature, AggregationFunction, Count, RollingWindow,
    OnlineStoreConfig, StreamingMode,
)
fe = FeatureEngineeringClient()

STREAM_NAME = f"{catalog}.{schema}.session_events_stream"
INGEST_TABLE = f"{catalog}.{schema}.session_events_ingest"

# COMMAND ----------
# MAGIC %md ## 1 · Create the Stream (per docs: this starts the managed ingestion pipeline)
# MAGIC Schema is **JSON Schema** format. `event_time` is a date-time string so the FV timeseries
# MAGIC column is TIMESTAMP.
# COMMAND ----------
PAYLOAD_JSON_SCHEMA = (
    '{'
    '  "type": "object",'
    '  "properties": {'
    '    "event_id":         {"type": "string"},'
    '    "customer_id":      {"type": "string"},'
    '    "event_time":       {"type": "string", "format": "date-time"},'
    '    "event_type":       {"type": "string"},'
    '    "product_category": {"type": "string"},'
    '    "dwell_ms":         {"type": "integer"},'
    '    "device":           {"type": "string"}'
    '  }'
    '}'
)

try:
    fe.get_stream(name=STREAM_NAME)
    print(f"Stream {STREAM_NAME} already exists.")
except Exception:
    fe.create_stream(
        name=STREAM_NAME,
        source_config=KafkaStreamConfig(
            subscription_mode=KafkaSubscriptionMode(subscribe=topic),
        ),
        connection_config=StreamConnectionConfig(uc_connection_name=conn_name),
        schema_config=DirectSchemas(
            payload_schema=SchemaConfig(json_schema=PAYLOAD_JSON_SCHEMA),
        ),
        ingestion_config=IngestionConfig(
            ingestion_destination=IngestionDestination(delta_table_name=INGEST_TABLE),
            deduplication_columns=["value.event_id"],
        ),
    )
    print(f"Created stream {STREAM_NAME} (ingestion -> {INGEST_TABLE}).")

# COMMAND ----------
# MAGIC %md ## 2 · Define the streaming feature (RollingWindow over the StreamSource)
# MAGIC Column refs use the `value.` prefix (Kafka payload). Only `RollingWindow` is supported for
# MAGIC streaming aggregations.
# COMMAND ----------
stream_source = StreamSource(full_name=STREAM_NAME)

# Idempotent: get the feature if it already exists, else create it (re-runnable notebook).
try:
    clicks_10m = fe.get_feature(full_name=f"{catalog}.{schema}.cust_clicks_10m")
    print("Streaming feature cust_clicks_10m already exists.")
except Exception:
    clicks_10m = fe.create_feature(
        name="cust_clicks_10m",
        source=stream_source,
        entity=["value.customer_id"],
        timeseries_column="value.event_time",
        function=AggregationFunction(
            operator=Count(input="value.event_id"),
            time_window=RollingWindow(window_duration=timedelta(minutes=10)),
        ),
        catalog_name=catalog,
        schema_name=schema,
    )
    print("Created streaming feature cust_clicks_10m.")

# COMMAND ----------
# MAGIC %md ## 3 · Materialize online-only with StreamingMode
# MAGIC Streaming features are online-only; `StreamingMode()` runs the continuous materialization
# MAGIC to the Lakebase online store.
# COMMAND ----------
# Idempotent: only materialize if not already materialized online (re-runnable notebook).
already = [m for m in fe.list_materialized_features(feature_name=f"{catalog}.{schema}.cust_clicks_10m") if m.is_online]
if already:
    print(f"cust_clicks_10m already materialized online -> {already[0].table_name}")
else:
    fe.materialize_features(
        features=[clicks_10m],
        online_config=OnlineStoreConfig(
            catalog_name=catalog, schema_name=schema,
            table_name_prefix="nbo_stream_serving", online_store_name=osn,
        ),
        trigger=StreamingMode(),
    )
    print("Materialized cust_clicks_10m online with StreamingMode.")

# COMMAND ----------
# MAGIC %md ## 4 · Verify the ingestion pipeline is RUNNING
# MAGIC Per docs, `create_stream` starts the ingestion pipeline automatically. If it's still IDLE,
# MAGIC start it explicitly. Then produce events (notebook 07) — the pipeline reads from the latest
# MAGIC offset, so only events produced *after* it is RUNNING are captured.
# COMMAND ----------
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
stream = w.feature_engineering.get_stream(name=STREAM_NAME)
pid = stream.ingestion_config.ingestion_pipeline_id
state = str(w.pipelines.get(pipeline_id=pid).state)
print("ingestion_pipeline_id:", pid, "| state:", state)
if "RUNNING" not in state:
    w.pipelines.start_update(pipeline_id=pid)
    print("Started ingestion pipeline (allow ~5-7 min to reach RUNNING).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next → notebook 09 (real-time serving)
# MAGIC After the ingestion pipeline is RUNNING and notebook 07 has produced events, notebook 09
# MAGIC re-logs the ranker with `cust_clicks_10m` added (online-lookup) and deploys the
# MAGIC route-optimized `nbo-ranker-realtime` endpoint. `create_training_set` reads the streaming
# MAGIC feature's ingestion table for point-in-time joins (leaf-node names: `customer_id`, `event_time`).
