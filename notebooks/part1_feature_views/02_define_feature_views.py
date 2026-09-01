# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Define Feature Views
# MAGIC The heart of the accelerator. We declare **one set of Feature objects**
# MAGIC (`databricks-feature-engineering>=0.16.0`) that power both offline training
# MAGIC (point-in-time) and online serving. Modes exercised here:
# MAGIC
# MAGIC | Mode | API | Example feature |
# MAGIC |---|---|---|
# MAGIC | Batch, overlapping | `SlidingWindow` | 30-day avg balance, 7-day txn count |
# MAGIC | Batch, fixed bucket | `TumblingWindow` | 90-day total spend |
# MAGIC | Latest attribute | `ColumnSelection` | loyalty tier, risk band |
# MAGIC
# MAGIC Streaming in-session features (`RollingWindow` over MSK) are a Part 2 concern, added once the
# MAGIC Kafka stream is registered — see notebook 08 (currently gated; read its preflight cell) and the
# MAGIC streaming-extension sketch at the bottom of this notebook.
# MAGIC
# MAGIC **Runs on serverless** (latest environment) with `databricks-feature-engineering>=0.16.0`.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")

from datetime import timedelta
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    DeltaTableSource, AggregationFunction, Avg, Sum, Count, ColumnSelection,
    SlidingWindow, TumblingWindow,
)
fe = FeatureEngineeringClient()

# Idempotent registration: get_feature if it already exists, else create_feature. create_feature
# is not idempotent (raises AlreadyExists on a re-run or a shared/pre-provisioned catalog), so
# this mirrors the get-or-create guards in notebooks 03 and 08 and keeps 02 safely re-runnable.
def get_or_create_feature(name, **create_kwargs):
    try:
        f = fe.get_feature(full_name=f"{catalog}.{schema}.{name}")
        print(f"Feature {name} already exists — reusing it.")
        return f
    except Exception:
        f = fe.create_feature(catalog_name=catalog, schema_name=schema, name=name, **create_kwargs)
        print(f"Created feature {name}.")
        return f

# COMMAND ----------
# MAGIC %md ## Sources — entity/timeseries live on the Feature, not the source
# COMMAND ----------
txn_source = DeltaTableSource(catalog_name=catalog, schema_name=schema,
                              table_name="transactions", filter_condition="amount > 0")
cust_source = DeltaTableSource(catalog_name=catalog, schema_name=schema, table_name="customers")

# COMMAND ----------
# MAGIC %md ## Windowed aggregation features
# MAGIC `AggregationFunction(operator=..., time_window=...)` — window lives *inside* the
# MAGIC aggregation. Sliding = overlapping (recomputed each slide); Tumbling = fixed buckets.
# COMMAND ----------
avg_balance_30d = get_or_create_feature(
    "cust_avg_balance_30d",
    source=txn_source, entity=["customer_id"], timeseries_column="ts",
    function=AggregationFunction(
        operator=Avg(input="balance"),
        time_window=SlidingWindow(window_duration=timedelta(days=30), slide_duration=timedelta(days=1))),
)
spend_90d = get_or_create_feature(
    "cust_spend_90d",
    source=txn_source, entity=["customer_id"], timeseries_column="ts",
    function=AggregationFunction(
        operator=Sum(input="amount"),
        time_window=TumblingWindow(window_duration=timedelta(days=90))),
)
txn_count_7d = get_or_create_feature(
    "cust_txn_count_7d",
    source=txn_source, entity=["customer_id"], timeseries_column="ts",
    function=AggregationFunction(
        operator=Count(input="txn_id"),
        time_window=SlidingWindow(window_duration=timedelta(days=7), slide_duration=timedelta(days=1))),
)

# COMMAND ----------
# MAGIC %md ## Latest-attribute features (ColumnSelection — no window)
# COMMAND ----------
loyalty_tier = get_or_create_feature(
    "cust_loyalty_tier",
    source=cust_source, entity=["customer_id"], timeseries_column="updated_at",
    function=ColumnSelection(column="loyalty_tier"),
)
risk_band = get_or_create_feature(
    "cust_risk_band",
    source=cust_source, entity=["customer_id"], timeseries_column="updated_at",
    function=ColumnSelection(column="risk_band"),
)

# COMMAND ----------
# MAGIC %md ## Validate — `compute_features` previews values (no persistence, no lineage)
# COMMAND ----------
agg_features = [avg_balance_30d, spend_90d, txn_count_7d]
attr_features = [loyalty_tier, risk_band]

preview = fe.compute_features(features=agg_features)
preview.show(5, truncate=False)
print("preview columns:", preview.columns)
print(f"Defined + registered {len(agg_features) + len(attr_features)} features in {catalog}.{schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming extension (RollingWindow over MSK) — Part 2, notebook 08
# MAGIC Register the Kafka topic as a governed stream, then declare RollingWindow features.
# MAGIC Uses the `msk_kafka` UC connection (MSK IAM). See notebook 07 for the topic + producer.
# MAGIC ```python
# MAGIC from databricks.feature_engineering.entities import (
# MAGIC     StreamSource, KafkaStreamConfig, KafkaSubscriptionMode, StreamConnectionConfig,
# MAGIC     DirectSchemas, SchemaConfig, IngestionConfig, IngestionDestination, RollingWindow,
# MAGIC )
# MAGIC fe.create_stream(
# MAGIC     name=f"{catalog}.{schema}.session_events_stream",
# MAGIC     source_config=KafkaStreamConfig(subscription_mode=KafkaSubscriptionMode(subscribe="nbo-session-events")),
# MAGIC     connection_config=StreamConnectionConfig(uc_connection_name="msk_kafka"),
# MAGIC     schema_config=DirectSchemas(payload_schema=SchemaConfig(json_schema=SESSION_EVENT_JSON_SCHEMA)),
# MAGIC     ingestion_config=IngestionConfig(
# MAGIC         ingestion_destination=IngestionDestination(delta_table_name=f"{catalog}.{schema}.session_events_ingest"),
# MAGIC         deduplication_columns=["value.event_id"]),
# MAGIC )
# MAGIC stream_source = StreamSource(full_name=f"{catalog}.{schema}.session_events_stream")
# MAGIC clicks_10m = fe.create_feature(
# MAGIC     source=stream_source, entity=["value.customer_id"], timeseries_column="value.event_time",
# MAGIC     function=AggregationFunction(operator=Count(input="value.event_id"),
# MAGIC         time_window=RollingWindow(window_duration=timedelta(minutes=10))),
# MAGIC     catalog_name=catalog, schema_name=schema, name="cust_clicks_10m")
# MAGIC ```
