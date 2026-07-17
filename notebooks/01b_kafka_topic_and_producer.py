# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Kafka Topic + Synthetic Stream Producer
# MAGIC Creates the in-session events **topic** on the cluster referenced by the existing
# MAGIC **Unity Catalog Kafka connection**, then streams synthetic clickstream events into it.
# MAGIC These events feed the `RollingWindow` streaming Feature Views (notebook 02) that capture
# MAGIC the customer's **freshest in-session intent**.
# MAGIC
# MAGIC **Prereqs**
# MAGIC - A UC Kafka connection already exists in this workspace (`ConnectionType.KAFKA`).
# MAGIC   Tian & Sixuan have permission on it and its underlying IAM role.
# MAGIC - This notebook reads bootstrap servers + auth *from that UC connection* — no secrets
# MAGIC   hardcoded here.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Resolve the UC Kafka connection → bootstrap servers + auth options.
# MAGIC 2. Create the topic (idempotent) via the Kafka Admin API.
# MAGIC 3. Produce synthetic in-session events continuously (or a bounded batch).

# COMMAND ----------
# MAGIC %pip install confluent-kafka aws-msk-iam-sasl-signer-python
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
dbutils.widgets.text("kafka_connection", "msk_kafka", "UC Kafka connection name")
dbutils.widgets.text("topic", "nbo-session-events")
dbutils.widgets.text("region", "", "AWS region (blank = derive from bootstrap host)")
dbutils.widgets.text("mode", "bounded", "bounded | continuous")
dbutils.widgets.text("num_events", "100000")
dbutils.widgets.text("events_per_sec", "500")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
conn_name = dbutils.widgets.get("kafka_connection")
topic = dbutils.widgets.get("topic")
mode = dbutils.widgets.get("mode")
num_events = int(dbutils.widgets.get("num_events"))
events_per_sec = int(dbutils.widgets.get("events_per_sec"))

assert conn_name, "Set the 'kafka_connection' widget to the UC Kafka connection name."

# COMMAND ----------
# MAGIC %md ## 1 · Resolve the UC Kafka connection → bootstrap + auth
# MAGIC List connections to confirm the name, then read its options. The connection stores the
# MAGIC bootstrap servers and security config centrally; we translate them into client options.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Confirm the connection exists and inspect it.
conn = w.connections.get(name=conn_name)
print("Connection:", conn.name, "| type:", conn.connection_type)
# Connection options typically include bootstrap servers + security protocol.
# (Key names vary by setup; print to confirm what this connection exposes.)
opts = dict(conn.options or {})
print("Option keys:", sorted(opts.keys()))

# Common keys — adjust to match this connection's actual options after the print above.
BOOTSTRAP = opts.get("bootstrapServers") or opts.get("bootstrap.servers")
SECURITY_PROTOCOL = opts.get("security.protocol", "SASL_SSL")
print("bootstrap:", BOOTSTRAP, "| security.protocol:", SECURITY_PROTOCOL)

# COMMAND ----------
# MAGIC %md ### Build client auth from the connection (AWS MSK + IAM)
# MAGIC `msk_kafka` is an **MSK IAM** connection — auth is `SASL_SSL` + `OAUTHBEARER`, with tokens
# MAGIC minted by the AWS MSK IAM signer using the connection's underlying IAM role. The job/cluster
# MAGIC assumes that role, so no static credentials live in the notebook.
# MAGIC
# MAGIC The signer needs the region; derive it from the bootstrap host
# MAGIC (e.g. `...kafka.us-west-2.amazonaws.com:9098`) or set the `region` widget.
import re

def _derive_region(bootstrap: str) -> str:
    m = re.search(r"\.([a-z]{2}-[a-z]+-\d)\.amazonaws\.com", bootstrap or "")
    return m.group(1) if m else "us-west-2"

REGION = dbutils.widgets.get("region") or _derive_region(BOOTSTRAP)
print("MSK region:", REGION)

def _oauth_token_provider(_config):
    # Returns (token, expiry_ms) for confluent-kafka's OAUTHBEARER callback.
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, expiry_ms / 1000.0

def client_config():
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": _oauth_token_provider,
    }

# COMMAND ----------
# MAGIC %md ## 2 · Create the topic (idempotent)
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient(client_config())
existing = admin.list_topics(timeout=10).topics
if topic in existing:
    print(f"Topic '{topic}' already exists.")
else:
    fs = admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=3)])
    for t, f in fs.items():
        f.result()  # raises on failure
        print(f"Created topic '{t}'.")

# COMMAND ----------
# MAGIC %md ## 3 · Synthetic in-session event generator
# MAGIC Events mirror the `session_events` schema from notebook 01 so streaming features and the
# MAGIC point-in-time training set stay consistent. Payload lands under `value.*` for the FV.
import json
import time
import random

random.seed(42)

EVENT_TYPES = ["page_view", "product_view", "calculator_use", "add_to_cart", "search"]
PRODUCT_CATEGORIES = ["credit_card", "savings", "personal_loan", "mortgage", "investment"]
N_CUSTOMERS = 100_000  # keep in sync with notebook 01

def make_event(i: int) -> dict:
    # event_time is epoch millis; RollingWindow keys off value.event_time
    return {
        "event_id": f"evt_{i}",
        "customer_id": f"cust_{random.randint(0, N_CUSTOMERS - 1)}",
        "event_time": int(time.time() * 1000),
        "event_type": random.choice(EVENT_TYPES),
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "dwell_ms": random.randint(200, 45_000),
        "device": random.choice(["ios", "android", "web"]),
    }

# COMMAND ----------
# MAGIC %md ## 4 · Produce
from confluent_kafka import Producer

producer = Producer(client_config())
delay = 1.0 / max(events_per_sec, 1)

def _delivery(err, msg):
    if err is not None:
        print("Delivery failed:", err)

i = 0
try:
    while True:
        ev = make_event(i)
        producer.produce(
            topic,
            key=ev["customer_id"].encode(),
            value=json.dumps(ev).encode(),
            callback=_delivery,
        )
        i += 1
        if i % 1000 == 0:
            producer.poll(0)
            print(f"produced {i} events")
        time.sleep(delay)
        if mode == "bounded" and i >= num_events:
            break
finally:
    producer.flush()
    print(f"Done. Produced {i} events to '{topic}'.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next
# MAGIC In **notebook 02**, register this topic as a governed stream and define RollingWindow features:
# MAGIC ```python
# MAGIC fe.create_stream(
# MAGIC     name=f"{catalog}.{schema}.session_events_stream",
# MAGIC     source_config=KafkaStreamConfig(
# MAGIC         subscription_mode=KafkaSubscriptionMode(subscribe=topic)),
# MAGIC     connection_config=StreamConnectionConfig(uc_connection_name=conn_name),
# MAGIC     schema_config=DirectSchemas(payload_schema=SchemaConfig(json_schema="{...}")),
# MAGIC     ingestion_config=IngestionConfig(
# MAGIC         ingestion_destination=IngestionDestination(
# MAGIC             delta_table_name=f"{catalog}.{schema}.session_events_ingest"),
# MAGIC         deduplication_columns=["value.event_id"]),
# MAGIC )
# MAGIC ```
