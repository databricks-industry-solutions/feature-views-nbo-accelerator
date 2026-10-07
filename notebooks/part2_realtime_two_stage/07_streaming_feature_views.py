# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Part 2 · 07 · Streaming Feature View — Freshest In-Session Intent
# MAGIC Defines a **streaming** Feature View over Kafka/MSK, strictly following the Databricks docs:
# MAGIC - Streams: <https://docs.databricks.com/aws/en/machine-learning/feature-store/streams>
# MAGIC - Feature Views (streaming): <https://docs.databricks.com/aws/en/machine-learning/feature-store/feature-views#streaming-features>
# MAGIC
# MAGIC The streaming feature `cust_clicks_10m` (count of in-session events per customer over a
# MAGIC 10-minute rolling window) captures the customer's freshest in-session intent to serve to the
# MAGIC model endpoint. **Status:** the online materialization of this streaming feature is currently
# MAGIC gated (see the preflight cell below) — Part 1 is the fully-working path.
# MAGIC
# MAGIC **Prereqs:** `databricks-feature-engineering>=0.16.0`, DBR 17.0 ML+, enterprise workspace
# MAGIC with Lakebase, and a **standard-storage UC catalog** (`fins_industry_solutions` qualifies).
# MAGIC The MSK topic exists (notebook 08).
# MAGIC
# MAGIC **Per the docs:**
# MAGIC - `create_stream` starts a **managed, continuous ingestion pipeline** that reads the topic
# MAGIC   into the ingestion Delta table, starting from the **latest offset** (so produce events
# MAGIC   *after* this notebook, or supply a `StreamBackfillSource` for history).
# MAGIC - Column refs are prefixed with **`value.`** (Kafka payload); the JSON Schema declares
# MAGIC   `event_time` as `{"type": "string", "format": "date-time"}` so the timeseries column is TIMESTAMP.
# MAGIC - Streaming features support **`RollingWindow`** and **`SawtoothWindow`** (Beta, windows > 2 days)
# MAGIC   and materialize **online-only** with `StreamingMode()`.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.18.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("kafka_connection", "msk_kafka")
dbutils.widgets.text("topic", "nbo-session-events")
dbutils.widgets.text("online_store_name", "nbo")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
conn_name = dbutils.widgets.get("kafka_connection")
topic = dbutils.widgets.get("topic")
osn = dbutils.widgets.get("online_store_name")

from datetime import timedelta
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    KafkaStreamConfig, KafkaSubscriptionMode, StreamConnectionConfig,
    DirectSchemas, SchemaConfig, IngestionConfig, IngestionDestination,
    StreamSource, Feature, AggregationFunction, Count, RollingWindow,
    OnlineStoreConfig, OfflineStoreConfig, StreamingMode,
)
fe = FeatureEngineeringClient()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚠️ Preflight — Part 2 streaming online serving is currently gated
# MAGIC The `StreamingMode()` online materialization below depends on a platform capability that is
# MAGIC **not yet available on serverless / pre-DBR-19 runtimes**: the streaming Feature View's online
# MAGIC sink does not populate the Lakebase online table (it stays at 0 rows), so the live
# MAGIC `cust_clicks_10m` feature never reaches the serving endpoint. Rather than print a misleading
# MAGIC "success", this notebook **stops early with a clear message** unless you explicitly opt in.
# MAGIC
# MAGIC Set the widget `allow_streaming_online=true` (and run on a runtime where the streaming online
# MAGIC sink is supported) to proceed. Part 1 is fully functional and unaffected by this gate.
# COMMAND ----------
dbutils.widgets.dropdown("allow_streaming_online", "false", ["false", "true"])
ALLOW_STREAMING_ONLINE = dbutils.widgets.get("allow_streaming_online") == "true"

if not ALLOW_STREAMING_ONLINE:
    msg = (
        "Part 2 streaming online serving is gated: the StreamingMode() online sink does not "
        "populate the Lakebase online table on this runtime (known limitation, pre-DBR-19). "
        "Part 1 covers the full author-once / online-lookup / ranking story and runs end-to-end. "
        "To attempt Part 2 anyway on a supported runtime, set the widget "
        "allow_streaming_online=true and re-run."
    )
    print(msg)
    dbutils.notebook.exit(msg)

# COMMAND ----------
# MAGIC %md ## 1 · Create the Stream (per docs: this starts the managed ingestion pipeline)
# MAGIC Schema is **JSON Schema** format. `event_time` is a date-time string so the FV timeseries
# MAGIC column is TIMESTAMP.
# COMMAND ----------
STREAM_NAME = f"{catalog}.{schema}.session_events_stream"
INGEST_TABLE = f"{catalog}.{schema}.session_events_ingest"

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

# Per-category intent, keyed on (customer_id, product_category). At serving, each request row already
# carries both keys (product_category is a request column), so every offer row gets "how many times did
# this visitor touch THIS category in the last 10 minutes". This is the feature that makes the ranking
# react to a click: browse mortgages and the mortgage rows' scores move, the others don't.
CAT_KEYS = ["value.customer_id", "value.product_category"]
try:
    cat_views_10m = fe.get_feature(full_name=f"{catalog}.{schema}.cust_cat_views_10m")
except Exception:
    cat_views_10m = fe.create_feature(
        name="cust_cat_views_10m", source=stream_source, entity=CAT_KEYS,
        timeseries_column="value.event_time",
        function=AggregationFunction(operator=Count(input="value.event_id"),
                                     time_window=RollingWindow(window_duration=timedelta(minutes=10))),
        catalog_name=catalog, schema_name=schema)
    print("Created streaming feature cust_cat_views_10m.")

# Mobile-app activity only (cross-channel signal). Website clicks are the visitor's own session, which the
# bank's site already knows and sends as a request-time feature (ctx_session_cat_views), so this streaming
# feature counts only the OTHER channels. A source-level filter narrows what the window aggregates.
mobile_source = StreamSource(full_name=STREAM_NAME, filter_condition="value.device <> 'web'")
try:
    mobile_cat_views_10m = fe.get_feature(full_name=f"{catalog}.{schema}.cust_mobile_cat_views_10m")
except Exception:
    mobile_cat_views_10m = fe.create_feature(
        name="cust_mobile_cat_views_10m", source=mobile_source, entity=CAT_KEYS,
        timeseries_column="value.event_time",
        function=AggregationFunction(operator=Count(input="value.event_id"),
                                     time_window=RollingWindow(window_duration=timedelta(minutes=10))),
        catalog_name=catalog, schema_name=schema)
    print("Created streaming feature cust_mobile_cat_views_10m.")

# Long-window interest on the SAME stream with a SawtoothWindow (Beta): 30-day count, fresh at the leading
# edge, but only ~2 days of live streaming state (older history comes from the compacted ingestion table).
# StreamSource only, window must be > 2 days, and it serves ~2 days after materialization. Shown in the
# app's profile panel via the Feature Serving endpoint; the ranker does not depend on it.
cat_views_30d = None
try:
    from databricks.feature_engineering.entities import SawtoothWindow
    try:
        cat_views_30d = fe.get_feature(full_name=f"{catalog}.{schema}.cust_cat_views_30d")
    except Exception:
        cat_views_30d = fe.create_feature(
            name="cust_cat_views_30d", source=stream_source, entity=CAT_KEYS,
            timeseries_column="value.event_time",
            function=AggregationFunction(operator=Count(input="value.event_id"),
                                         time_window=SawtoothWindow(window_duration=timedelta(days=30))),
            catalog_name=catalog, schema_name=schema)
        print("Created Sawtooth feature cust_cat_views_30d.")
except ImportError:
    print("SawtoothWindow not available in this databricks-feature-engineering version; skipping cust_cat_views_30d.")

# COMMAND ----------
# MAGIC %md ## 3 · Materialize online-only with StreamingMode
# MAGIC Streaming features are online-only; `StreamingMode()` runs the continuous materialization
# MAGIC to the Lakebase online store.
# MAGIC
# MAGIC **Single-store invariant.** This streaming feature MUST land in the **same** online store as the
# MAGIC Part 1 batch features (`online_store_name` = `nbo` here) and the **same** schema. New Lakebase
# MAGIC Autoscaling online stores do not support a served model looking up features across multiple
# MAGIC online stores, so if the batch features are in one store and this streaming feature in another,
# MAGIC the re-logged ranker (nb09) fails to provision its serving role/OAuth token and won't deploy.
# MAGIC The preflight below fails loudly if the stores don't match rather than creating a broken split.
# COMMAND ----------
# Preflight: the batch features (materialized in nb03) must live in the SAME online store as osn.
batch_stores = {
    m.online_store_config.online_store_name
    for f in ["cust_avg_balance_30d", "cust_txn_count_7d", "cust_loyalty_tier"]
    for m in fe.list_materialized_features(feature_name=f"{catalog}.{schema}.{f}")
    if m.is_online and getattr(m, "online_store_config", None) and m.online_store_config.online_store_name
}
if batch_stores and batch_stores != {osn}:
    raise ValueError(
        f"Single-store invariant violated: Part 1 batch features are in online store(s) {batch_stores}, "
        f"but this notebook targets '{osn}'. Route-optimized serving (nb09) requires ALL of a model's "
        f"features in ONE online store. Re-run nb03 and nb07 with the same --var online_store_name, or "
        f"pass online_store_name={list(batch_stores)[0]} here."
    )

# Idempotent: only materialize features that are not already materialized online (re-runnable notebook).
# Each streaming feature gets its own materialize call so a Beta failure (Sawtooth) can't block the others.
for feat, name in [(clicks_10m, "cust_clicks_10m"), (cat_views_10m, "cust_cat_views_10m"),
                   (mobile_cat_views_10m, "cust_mobile_cat_views_10m"), (cat_views_30d, "cust_cat_views_30d")]:
    if feat is None:
        continue
    already = [m for m in fe.list_materialized_features(feature_name=f"{catalog}.{schema}.{name}") if m.is_online]
    if already:
        print(f"{name} already materialized online -> {already[0].table_name}")
        continue
    # A SawtoothWindow also needs an OFFLINE destination: the older part of its window is read from
    # compacted offline output, only the newest ~2 days come from the live stream.
    extra = {}
    if name == "cust_cat_views_30d":
        extra["offline_config"] = OfflineStoreConfig(catalog_name=catalog, schema_name=schema,
                                                     table_name_prefix="nbostream_off")
    try:
        fe.materialize_features(
            features=[feat],
            online_config=OnlineStoreConfig(
                catalog_name=catalog, schema_name=schema,
                table_name_prefix="nbostream", online_store_name=osn,
            ),
            trigger=StreamingMode(),
            **extra,
        )
        print(f"Materialized {name} online with StreamingMode.")
    except Exception as e:
        if name == "cust_cat_views_30d":  # Beta: report and continue; nothing downstream requires it
            print(f"WARNING: could not materialize {name} (Sawtooth, Beta): {type(e).__name__}: {e}")
        else:
            raise

# COMMAND ----------
# MAGIC %md ## 4 · Block until the ingestion pipeline is RUNNING (gate before producing)
# MAGIC Per docs, `create_stream` starts the ingestion pipeline automatically, but it reads from the
# MAGIC **latest Kafka offset** — so only events produced *after* it is RUNNING are captured. In a job
# MAGIC DAG this task therefore must **not** return until the pipeline is RUNNING; otherwise the
# MAGIC downstream producer (08) would feed events the pipeline can never see. We start it if IDLE and
# MAGIC poll until RUNNING (or fail loudly on FAILED), so the `depends_on` producer only runs once the
# MAGIC pipeline is live.
# COMMAND ----------
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
stream = w.feature_engineering.get_stream(name=STREAM_NAME)
pid = stream.ingestion_config.ingestion_pipeline_id
state = str(w.pipelines.get(pipeline_id=pid).state)
print("ingestion_pipeline_id:", pid, "| state:", state)
if "RUNNING" not in state:
    w.pipelines.start_update(pipeline_id=pid)
    print("Started ingestion pipeline; polling until RUNNING (allow ~5-7 min).")

DEADLINE_S = 15 * 60
POLL_S = 20
start = time.time()
while True:
    state = str(w.pipelines.get(pipeline_id=pid).state)
    if "RUNNING" in state:
        print(f"Ingestion pipeline is RUNNING after {int(time.time() - start)}s. Safe to produce (08).")
        break
    if "FAILED" in state:
        raise RuntimeError(f"Ingestion pipeline {pid} entered {state}; check the pipeline run for a validation error (see Pitfall #5).")
    if time.time() - start > DEADLINE_S:
        raise TimeoutError(f"Ingestion pipeline {pid} did not reach RUNNING within {DEADLINE_S}s (last state: {state}).")
    time.sleep(POLL_S)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next → notebook 09 (real-time serving)
# MAGIC After the ingestion pipeline is RUNNING and notebook 08 has produced events, notebook 09
# MAGIC re-logs the ranker with `cust_clicks_10m` added (online-lookup) and deploys the
# MAGIC route-optimized `nbo-ranker-realtime` endpoint. `create_training_set` reads the streaming
# MAGIC feature's ingestion table for point-in-time joins (leaf-node names: `customer_id`, `event_time`).
