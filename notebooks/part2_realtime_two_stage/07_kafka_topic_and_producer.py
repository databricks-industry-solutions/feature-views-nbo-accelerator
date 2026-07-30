# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 07 · Kafka Topic + Synthetic In-Session Event Producer
# MAGIC Produces synthetic clickstream events into the MSK topic that backs the streaming
# MAGIC Feature View (notebook 08). Follows the Databricks Feature Views streaming docs:
# MAGIC <https://docs.databricks.com/aws/en/machine-learning/feature-store/streams>
# MAGIC
# MAGIC **Auth:** UC Kafka connection **`msk_kafka`** (AWS MSK, IAM,
# MAGIC public-TLS :9198), backed by UC **service credential `msk_kafka`**.
# MAGIC
# MAGIC Two auth paths, each for its job:
# MAGIC - **Producing** (Spark `write`) → `.option("databricks.serviceCredential", "msk_kafka")`.
# MAGIC   Databricks mints the MSK IAM token; do **not** also set `kafka.security.protocol` /
# MAGIC   `kafka.sasl.mechanism` (rejected as conflicting when a service credential is used).
# MAGIC - **Topic admin** (create topic) → the Spark connector can't do admin ops, so we fetch
# MAGIC   temporary AWS creds from the service credential and use a Kafka `AdminClient` with the
# MAGIC   AWS MSK IAM signer. (This MSK cluster has auto-create disabled, so the topic is pre-created.)
# MAGIC
# MAGIC **Doc-critical:** the Stream's ingestion pipeline reads from the **latest Kafka offset**, so
# MAGIC events must be produced *after* the stream exists (notebook 08), or supplied via a backfill
# MAGIC source. For the demo we run this producer in `continuous` mode after notebook 08 is live.
# MAGIC `event_time` is emitted as an **ISO-8601 timestamp string** (the streaming FV timeseries
# MAGIC column must be TIMESTAMP; the JSON Schema declares `format: date-time`).

# COMMAND ----------
# MAGIC %pip install confluent-kafka aws-msk-iam-sasl-signer-python
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("kafka_connection", "msk_kafka")
dbutils.widgets.text("service_credential", "msk_kafka")
dbutils.widgets.text("topic", "nbo-session-events")
dbutils.widgets.text("mode", "bounded", "bounded | continuous")
dbutils.widgets.text("num_events", "200000")
dbutils.widgets.text("events_per_sec", "2000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
conn_name = dbutils.widgets.get("kafka_connection")
service_credential = dbutils.widgets.get("service_credential")
topic = dbutils.widgets.get("topic")
mode = dbutils.widgets.get("mode")
num_events = int(dbutils.widgets.get("num_events"))
events_per_sec = int(dbutils.widgets.get("events_per_sec"))

import os
import re
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

w = WorkspaceClient()
conn = w.connections.get(name=conn_name)
BOOTSTRAP = dict(conn.options or {})["bootstrap_servers"]
REGION = (re.search(r"\.([a-z]{2}-[a-z]+-\d)\.amazonaws\.com", BOOTSTRAP) or [None, "us-west-2"])[1]
print("bootstrap:", BOOTSTRAP, "| region:", REGION)

# COMMAND ----------
# MAGIC %md ## 1 · Create the topic (idempotent) — temp AWS creds + Kafka AdminClient (MSK IAM)
# COMMAND ----------
import requests

resp = requests.post(
    f"{w.config.host}/api/2.1/unity-catalog/temporary-service-credentials",
    headers={**w.config.authenticate(), "Content-Type": "application/json"},
    json={"credential_name": service_credential},
)
resp.raise_for_status()
creds = resp.json()["aws_temp_credentials"]
os.environ["AWS_ACCESS_KEY_ID"] = creds["access_key_id"]
os.environ["AWS_SECRET_ACCESS_KEY"] = creds["secret_access_key"]
os.environ["AWS_SESSION_TOKEN"] = creds["session_token"]
os.environ["AWS_REGION"] = REGION

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka.admin import AdminClient, NewTopic

def _oauth_cb(_cfg):
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, expiry_ms / 1000.0

admin = AdminClient({
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "oauth_cb": _oauth_cb,
})
if topic in admin.list_topics(timeout=15).topics:
    print(f"Topic '{topic}' already exists.")
else:
    for t, f in admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=3)]).items():
        f.result(timeout=30)
        print(f"Created topic '{t}'.")

# COMMAND ----------
# MAGIC %md ## 2 · Synthetic event schema (matches the Stream's JSON Schema in notebook 08)
# COMMAND ----------
N_CUSTOMERS = 100_000
EVENT_TYPES = ["page_view", "product_view", "calculator_use", "add_to_cart", "search"]
CATS = ["credit_card", "savings", "personal_loan", "mortgage", "investment"]
DEVICES = ["ios", "android", "web"]

def to_events(df, id_col):
    ev = (
        df.withColumn("event_id", F.concat(F.lit("evt_"), F.col(id_col).cast("string")))
        .withColumn("customer_id", F.concat(F.lit("cust_"), (F.abs(F.hash(id_col)) % N_CUSTOMERS).cast("string")))
        .withColumn("event_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withColumn("event_type", F.element_at(F.array(*[F.lit(x) for x in EVENT_TYPES]), (F.abs(F.hash(id_col, F.lit(1))) % 5 + 1)))
        .withColumn("product_category", F.element_at(F.array(*[F.lit(x) for x in CATS]), (F.abs(F.hash(id_col, F.lit(2))) % 5 + 1)))
        .withColumn("dwell_ms", (F.abs(F.hash(id_col, F.lit(3))) % 44800 + 200).cast("int"))
        .withColumn("device", F.element_at(F.array(*[F.lit(x) for x in DEVICES]), (F.abs(F.hash(id_col, F.lit(4))) % 3 + 1)))
    )
    payload = F.struct("event_id", "customer_id", "event_time", "event_type", "product_category", "dwell_ms", "device")
    return ev.select(F.col("customer_id").alias("key"), F.to_json(payload).alias("value"))

KAFKA_OPTS = {"kafka.bootstrap.servers": BOOTSTRAP, "databricks.serviceCredential": service_credential}

# COMMAND ----------
# MAGIC %md ## 3a · Bounded produce (run AFTER notebook 08 so the ingestion pipeline captures it)
# COMMAND ----------
if mode == "bounded":
    events = to_events(spark.range(0, num_events).withColumnRenamed("id", "seq"), "seq")
    events.write.format("kafka").options(**KAFKA_OPTS).option("topic", topic).save()
    print(f"Produced {num_events} events to '{topic}'.")

# COMMAND ----------
# MAGIC %md ## 3b · Continuous produce (live demo + freshness benchmark)
# COMMAND ----------
if mode == "continuous":
    checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/kafka_producer"
    rate = spark.readStream.format("rate").option("rowsPerSecond", events_per_sec).load().withColumnRenamed("value", "seq")
    q = (to_events(rate, "seq").writeStream.format("kafka").options(**KAFKA_OPTS)
         .option("topic", topic).option("checkpointLocation", checkpoint).start())
    print(f"Streaming ~{events_per_sec} events/s to '{topic}'. Checkpoint: {checkpoint}")
    # q.awaitTermination()
