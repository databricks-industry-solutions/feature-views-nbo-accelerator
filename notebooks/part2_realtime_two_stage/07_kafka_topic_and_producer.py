# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Kafka Topic + Synthetic Stream Producer
# MAGIC Produces synthetic in-session clickstream events into the MSK topic that backs the
# MAGIC `RollingWindow` streaming Feature Views (notebook 02) — the customer's **freshest
# MAGIC in-session intent**.
# MAGIC
# MAGIC **Auth — verified against this workspace (`fe-vm-ttan-vm`):**
# MAGIC - UC Kafka connection **`msk_kafka`** → AWS MSK (provisioned), IAM auth, public TLS port **9198**.
# MAGIC - Backed by UC **service credential `msk_kafka`** (role `ttan-fv-databricks-msk-access`).
# MAGIC   Its own comment says: *use via* `.option("databricks.serviceCredential", "msk_kafka")`.
# MAGIC - So we authenticate the **Spark Kafka connector** with the service credential — Databricks
# MAGIC   mints the MSK IAM token; **no static AWS credentials in the notebook**.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Read bootstrap servers from the UC connection.
# MAGIC 2. Produce synthetic events to the topic via Spark `write`/`writeStream` (`.format("kafka")`).
# MAGIC    (MSK auto-creates the topic on first produce; see the topic note if yours disables that.)

# COMMAND ----------
dbutils.widgets.text("catalog", "fins-industry-solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("kafka_connection", "msk_kafka", "UC Kafka connection name")
dbutils.widgets.text("service_credential", "msk_kafka", "UC service credential (MSK IAM)")
dbutils.widgets.text("topic", "nbo-session-events")
dbutils.widgets.text("mode", "bounded", "bounded | continuous")
dbutils.widgets.text("num_events", "500000")
dbutils.widgets.text("events_per_sec", "2000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
conn_name = dbutils.widgets.get("kafka_connection")
service_credential = dbutils.widgets.get("service_credential")
topic = dbutils.widgets.get("topic")
mode = dbutils.widgets.get("mode")
num_events = int(dbutils.widgets.get("num_events"))
events_per_sec = int(dbutils.widgets.get("events_per_sec"))

# COMMAND ----------
# MAGIC %md ## 1 · Resolve bootstrap servers from the UC connection
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
conn = w.connections.get(name=conn_name)
opts = dict(conn.options or {})
BOOTSTRAP = opts["bootstrap_servers"]  # e.g. b-1-public.ttanfvmsk...:9198,b-2-...,b-3-...
print("Connection:", conn.name, "| type:", conn.connection_type)
print("bootstrap:", BOOTSTRAP)

# Shared Kafka options. With databricks.serviceCredential set, Databricks wires the MSK IAM
# SASL callback for us; security.protocol/sasl.mechanism are set explicitly for clarity.
KAFKA_OPTS = {
    "kafka.bootstrap.servers": BOOTSTRAP,
    "databricks.serviceCredential": service_credential,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
}

# COMMAND ----------
# MAGIC %md ## 2 · Synthetic event schema
# MAGIC Events mirror the `session_events` batch table from notebook 01 so the streaming features
# MAGIC and the point-in-time training set stay consistent. The Kafka `value` is a JSON string;
# MAGIC notebook 02 exposes payload fields under `value.*` (and the key under `key.*`).
from pyspark.sql import functions as F

N_CUSTOMERS = 100_000  # keep in sync with notebook 01
EVENT_TYPES = ["page_view", "product_view", "calculator_use", "add_to_cart", "search"]
PRODUCT_CATEGORIES = ["credit_card", "savings", "personal_loan", "mortgage", "investment"]
DEVICES = ["ios", "android", "web"]


def to_events(df, id_col: str):
    """Map a DataFrame with a monotonic id column into Kafka key/value event rows."""
    ev = (
        df.withColumn("event_id", F.concat(F.lit("evt_"), F.col(id_col).cast("string")))
        .withColumn("customer_id",
                    F.concat(F.lit("cust_"),
                             (F.abs(F.hash(F.col(id_col))) % N_CUSTOMERS).cast("string")))
        .withColumn("event_time", (F.unix_timestamp() * 1000).cast("long"))
        .withColumn("event_type",
                    F.element_at(F.array(*[F.lit(x) for x in EVENT_TYPES]),
                                 (F.abs(F.hash(F.col(id_col), F.lit(1))) % len(EVENT_TYPES) + 1)))
        .withColumn("product_category",
                    F.element_at(F.array(*[F.lit(x) for x in PRODUCT_CATEGORIES]),
                                 (F.abs(F.hash(F.col(id_col), F.lit(2))) % len(PRODUCT_CATEGORIES) + 1)))
        .withColumn("dwell_ms", (F.abs(F.hash(F.col(id_col), F.lit(3))) % 44800 + 200).cast("int"))
        .withColumn("device",
                    F.element_at(F.array(*[F.lit(x) for x in DEVICES]),
                                 (F.abs(F.hash(F.col(id_col), F.lit(4))) % len(DEVICES) + 1)))
    )
    payload = F.struct("event_id", "customer_id", "event_time",
                       "event_type", "product_category", "dwell_ms", "device")
    return ev.select(
        F.col("customer_id").alias("key"),   # partition by customer
        F.to_json(payload).alias("value"),
    )

# COMMAND ----------
# MAGIC %md ## 3a · Bounded produce (seed the topic)
# MAGIC Batch-write `num_events` events. Good for the Asset Bundle job task and quick tests.
if mode == "bounded":
    base = spark.range(0, num_events).withColumnRenamed("id", "seq")
    events = to_events(base, "seq")
    (events.write.format("kafka").options(**KAFKA_OPTS).option("topic", topic).save())
    print(f"Produced {num_events} events to '{topic}'.")

# COMMAND ----------
# MAGIC %md ## 3b · Continuous produce (live demo)
# MAGIC Rate source → events → Kafka. Drives live RollingWindow features for the app demo.
# MAGIC Stop the stream from the cell menu (or set `mode=bounded` for the job).
if mode == "continuous":
    checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/kafka_producer"
    rate = (spark.readStream.format("rate")
            .option("rowsPerSecond", events_per_sec).load()
            .withColumnRenamed("value", "seq"))
    events = to_events(rate, "seq")
    q = (events.writeStream.format("kafka").options(**KAFKA_OPTS)
         .option("topic", topic)
         .option("checkpointLocation", checkpoint)
         .start())
    print(f"Streaming ~{events_per_sec} events/s to '{topic}'. Checkpoint: {checkpoint}")
    # q.awaitTermination()  # uncomment to block

# COMMAND ----------
# MAGIC %md
# MAGIC ## Topic pre-creation (only if MSK auto-create is disabled)
# MAGIC Provisioned MSK usually has `auto.create.topics.enable=true`, so the first produce creates
# MAGIC `nbo-session-events`. If your cluster disables it, pre-create the topic with the AWS CLI /
# MAGIC `kafka-topics.sh` using the `msk_kafka` IAM role, or ask the MSK admin. Partition guidance:
# MAGIC 6 partitions, replication factor 3.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next → notebook 02
# MAGIC Register this topic as a governed stream, then define RollingWindow features:
# MAGIC ```python
# MAGIC fe.create_stream(
# MAGIC     name=f"{catalog}.{schema}.session_events_stream",
# MAGIC     source_config=KafkaStreamConfig(
# MAGIC         subscription_mode=KafkaSubscriptionMode(subscribe="nbo-session-events")),
# MAGIC     connection_config=StreamConnectionConfig(uc_connection_name="msk_kafka"),
# MAGIC     schema_config=DirectSchemas(payload_schema=SchemaConfig(json_schema="{...}")),
# MAGIC     ingestion_config=IngestionConfig(
# MAGIC         ingestion_destination=IngestionDestination(
# MAGIC             delta_table_name=f"{catalog}.{schema}.session_events_ingest"),
# MAGIC         deduplication_columns=["value.event_id"]),
# MAGIC )
# MAGIC ```
