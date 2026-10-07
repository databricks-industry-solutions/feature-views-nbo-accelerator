# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 · 08 · Kafka Topic + Synthetic In-Session Event Producer
# MAGIC Produces synthetic clickstream events into the MSK topic that backs the streaming
# MAGIC Feature View (notebook 07). Follows the Databricks Feature Views streaming docs:
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
# MAGIC events must be produced *after* the stream exists (notebook 07), or supplied via a backfill
# MAGIC source. For the demo we run this producer in `continuous` mode after notebook 07 is live.
# MAGIC `event_time` is emitted as an **ISO-8601 timestamp string** (the streaming FV timeseries
# MAGIC column must be TIMESTAMP; the JSON Schema declares `format: date-time`).

# COMMAND ----------
# MAGIC %pip install confluent-kafka aws-msk-iam-sasl-signer-python
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("kafka_connection", "msk_kafka")
dbutils.widgets.text("service_credential", "msk_kafka")
dbutils.widgets.text("topic", "nbo-session-events")
dbutils.widgets.text("mode", "bounded", "topic | bounded | continuous | replay")
dbutils.widgets.text("num_events", "200000")
dbutils.widgets.text("events_per_sec", "25")  # continuous: rows/sec. LOW (~20-25) for a clean feature-freshness-benchmark §2b freshness read; raise for a load test.
dbutils.widgets.text("duration_sec", "300")  # continuous mode: how long to keep bursting

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
conn_name = dbutils.widgets.get("kafka_connection")
service_credential = dbutils.widgets.get("service_credential")
topic = dbutils.widgets.get("topic")
mode = dbutils.widgets.get("mode")
num_events = int(dbutils.widgets.get("num_events"))
events_per_sec = int(dbutils.widgets.get("events_per_sec"))

# --- Preflight gate (same switch as notebook 07) ---
# This producer writes to MSK, which needs a working `msk_kafka` UC connection + service credential
# (AWS IAM). Part 2's streaming path is gated OFF by default so the e2e runs green on Part 1 (the
# fully-working path). Gating here — before any MSK access — makes a gated run skip cleanly instead
# of failing on temporary-service-credentials / AssumeRole. Opt in once the MSK infra is configured.
dbutils.widgets.dropdown("allow_streaming_online", "false", ["false", "true"])
if dbutils.widgets.get("allow_streaming_online") != "true":
    msg = ("Kafka/MSK producer skipped: Part 2 streaming is gated (allow_streaming_online=false). "
           "Part 1 covers the full author-once / online-lookup / ranking story end-to-end. To run "
           "Part 2, ensure the msk_kafka connection + service credential (MSK cluster) work and set "
           "the widget allow_streaming_online=true, then re-run.")
    print(msg); dbutils.notebook.exit(msg)

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

def _load_temp_creds():
    """Refresh the UC service credential before every MSK OAuth token mint.

    The temp credential lifetime is about one hour — the same length as the live demo job — so
    loading it only once makes a healthy producer fail at the hour boundary.
    """
    resp = requests.post(
        f"{w.config.host}/api/2.1/unity-catalog/temporary-service-credentials",
        headers={**w.config.authenticate(), "Content-Type": "application/json"},
        json={"credential_name": service_credential},
        timeout=30,
    )
    resp.raise_for_status()
    creds = resp.json()["aws_temp_credentials"]
    os.environ["AWS_ACCESS_KEY_ID"] = creds["access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds["secret_access_key"]
    os.environ["AWS_SESSION_TOKEN"] = creds["session_token"]
    os.environ["AWS_REGION"] = REGION

_load_temp_creds()

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

def _oauth_cb(_cfg):
    _load_temp_creds()
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
# mode=topic: only ensure the topic exists (runs before notebook 07 so its ingestion pipeline can subscribe).
if mode == "topic":
    dbutils.notebook.exit(f"Topic '{topic}' ready.")

# COMMAND ----------
# MAGIC %md ## 2 · Synthetic event schema (matches the Stream's JSON Schema in notebook 07)
# COMMAND ----------
N_CUSTOMERS = 100_000
EVENT_TYPES = ["page_view", "product_view", "calculator_use", "add_to_cart", "search"]
CATS = ["credit_card", "savings", "personal_loan", "mortgage", "investment"]
DEVICES = ["ios", "android", "web"]
CONTROL_PATH = f"/Volumes/{catalog}/{schema}/nbo_control/traffic.json"
STATUS_PATH = f"/Volumes/{catalog}/{schema}/nbo_control/traffic_status.json"

def traffic_enabled():
    """Warm pause/resume control written by the Databricks App.

    The producer process and Kafka connection stay alive while disabled, so Start resumes in under a
    second instead of paying a 60–90 second serverless startup.
    """
    try:
        with open(CONTROL_PATH) as f:
            return bool(_json.load(f).get("enabled", True))
    except Exception as e:
        print(f"WARNING: could not read {CONTROL_PATH}; keeping prior/default enabled state: {type(e).__name__}: {e}")
        return True

def write_traffic_status(*, enabled, started_at_ms, sent, delivered, target_eps, error=""):
    """Atomic heartbeat consumed by the app's 250 ms live endpoint.

    `delivered` comes from Kafka delivery callbacks, so the dashboard shows real acknowledged events,
    not an interpolated or warehouse-delayed estimate.
    """
    status = {
        "running": True, "enabled": bool(enabled), "started_at": started_at_ms,
        "sent": int(sent), "delivered": int(delivered), "target_eps": int(target_eps),
        "updated_at": int(_t.time() * 1000), "error": error,
    }
    tmp = f"{STATUS_PATH}.tmp"
    with open(tmp, "w") as f:
        _json.dump(status, f, separators=(",", ":"))
    os.replace(tmp, STATUS_PATH)

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

KAFKA_OPTS = {
    "kafka.bootstrap.servers": BOOTSTRAP,
    "databricks.serviceCredential": service_credential,
    # Public MSK can have brief network stalls. The Spark Kafka sink's 120s delivery default killed an
    # otherwise healthy one-hour demo run; allow recovery and keep the producer loop alive per burst.
    "kafka.request.timeout.ms": "60000",
    "kafka.delivery.timeout.ms": "300000",
    "kafka.max.block.ms": "300000",
    "kafka.retries": "20",
}

# COMMAND ----------
# MAGIC %md ## 3r · Replay historical session events (training signal for the streaming features)
# MAGIC Notebook 01 gives half the training labels 1-4 session events in the 10 minutes before the label
# MAGIC `ts` (event ids `evt_l*`). Replaying them with their **original** `event_time` puts that history in
# MAGIC the ingestion table, so `create_training_set` sees non-zero streaming features (`cust_mobile_cat_views_10m`) at label time and
# MAGIC the ranker learns to react to in-session clicks. A 5% sample of the background clickstream from the
# MAGIC same window rides along so the counts aren't label-only. Run once, after notebook 07 is RUNNING.
# COMMAND ----------
if mode == "replay":
    hist = (spark.table(f"`{catalog}`.{schema}.session_events")
            .where("event_time >= timestamp'2025-05-22' AND "
                   "(event_id LIKE 'evt_l%' OR pmod(xxhash64(event_id), 20) = 0)"))
    payload = F.struct("event_id", "customer_id",
                       F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_time"),
                       "event_type", "product_category", "dwell_ms", "device")
    (hist.select(F.col("customer_id").alias("key"), F.to_json(payload).alias("value"))
         .write.format("kafka").options(**KAFKA_OPTS).option("topic", topic).save())
    print(f"Replayed {hist.count():,} historical session events to '{topic}' with original event_time.")

# COMMAND ----------
# MAGIC %md ## 3a · Bounded produce (run AFTER notebook 07 so the ingestion pipeline captures it)
# COMMAND ----------
if mode == "bounded":
    events = to_events(spark.range(0, num_events).withColumnRenamed("id", "seq"), "seq")
    events.write.format("kafka").options(**KAFKA_OPTS).option("topic", topic).save()
    print(f"Produced {num_events} events to '{topic}'.")

# COMMAND ----------
# MAGIC %md ## 3b · Continuous produce (live demo + freshness benchmark)
# MAGIC Serverless job compute rejects an infinite Spark `writeStream` trigger. A repeated Spark
# MAGIC `DataFrame.write.format("kafka")` loop is also the wrong load generator: it launches a Spark job
# MAGIC for every burst and can stall for minutes while the workflow still says RUNNING.
# MAGIC
# MAGIC Continuous mode therefore uses one persistent `confluent-kafka` producer on the driver. It keeps
# MAGIC a single authenticated connection, batches asynchronously, and paces an exact target rate. The
# MAGIC task remains bounded by `duration_sec`, so a forgotten demo still stops itself.
# COMMAND ----------
if mode == "continuous":
    import json as _json
    import time as _t
    from datetime import datetime, timezone

    duration_sec = int(dbutils.widgets.get("duration_sec"))
    target_rate = max(1, events_per_sec)
    delivery_errors = []
    delivery = {"count": 0}

    def _delivered(err, _msg):
        if err is not None:
            delivery_errors.append(str(err))
        else:
            delivery["count"] += 1

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": _oauth_cb,
        "client.id": "nbo-live-traffic",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 10,
        "batch.num.messages": 10000,
        "message.timeout.ms": 300000,
        "request.timeout.ms": 60000,
    })

    started = _t.monotonic()
    started_at_ms = int(_t.time() * 1000)
    deadline = started + duration_sec
    sent = 0
    next_emit = started
    previous_enabled = None
    last_status = 0.0
    while _t.monotonic() < deadline:
        enabled = traffic_enabled()
        if enabled != previous_enabled:
            print(f"Traffic control: {'RUNNING' if enabled else 'PAUSED'} ({CONTROL_PATH})")
            previous_enabled = enabled
        if not enabled:
            producer.poll(0)
            next_emit = _t.monotonic()
            if _t.monotonic() - last_status >= 0.25:
                write_traffic_status(enabled=False, started_at_ms=started_at_ms, sent=sent,
                                     delivered=delivery["count"], target_eps=target_rate)
                last_status = _t.monotonic()
            _t.sleep(0.25)
            continue

        now = _t.monotonic()
        if now < next_emit:
            producer.poll(0)
            _t.sleep(min(0.02, next_emit - now))
            continue
        # Pace from the last scheduled message time. Cap catch-up to one second of events.
        batch = min(target_rate, max(1, int((now - next_emit) * target_rate) + 1))
        for _ in range(batch):
            seq = int(_t.time_ns()) + sent
            customer_id = f"cust_{seq % N_CUSTOMERS}"
            event = {
                "event_id": f"evt_live_{seq}",
                "customer_id": customer_id,
                "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "event_type": EVENT_TYPES[(seq // 3) % len(EVENT_TYPES)],
                "product_category": CATS[(seq // 7) % len(CATS)],
                "dwell_ms": 200 + (seq % 44800),
                "device": DEVICES[(seq // 11) % len(DEVICES)],
            }
            payload = _json.dumps(event, separators=(",", ":")).encode()
            while True:
                try:
                    producer.produce(topic, key=customer_id.encode(), value=payload, on_delivery=_delivered)
                    break
                except BufferError:
                    producer.poll(0.1)
            sent += 1
        next_emit += batch / target_rate
        producer.poll(0)
        if delivery_errors:
            raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")
        if _t.monotonic() - last_status >= 0.25:
            write_traffic_status(enabled=True, started_at_ms=started_at_ms, sent=sent,
                                 delivered=delivery["count"], target_eps=target_rate)
            last_status = _t.monotonic()
        _t.sleep(0.02)

    remaining = producer.flush(60)
    if remaining or delivery_errors:
        raise RuntimeError(
            f"Kafka producer ended with {remaining} undelivered message(s): "
            f"{delivery_errors[0] if delivery_errors else 'flush timeout'}")
    elapsed = _t.monotonic() - started
    write_traffic_status(enabled=False, started_at_ms=started_at_ms, sent=sent,
                         delivered=delivery["count"], target_eps=target_rate)
    print(
        f"Produced {delivery['count']:,} events over {elapsed:.1f}s to '{topic}' "
        f"(effective rate={delivery['count']/elapsed:.1f}/s, target={target_rate}/s).")
