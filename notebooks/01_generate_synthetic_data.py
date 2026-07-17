# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate Synthetic FSI Data
# MAGIC Fully synthetic, reproducible retail-banking dataset — no external licensing.
# MAGIC
# MAGIC Produces four Delta tables:
# MAGIC - `customers`      — demographics, tenure, loyalty tier, risk band
# MAGIC - `offers`         — NBO catalog (cards, savings, loans) + text for embeddings
# MAGIC - `transactions`   — batch history (spend, balances) for windowed features
# MAGIC - `session_events` — in-session clickstream (page views, calculators, dwell) for streaming features
# MAGIC - `labels`         — historical offer acceptances w/ timestamps (for point-in-time training)

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# MAGIC %md ## Config — scale knobs
N_CUSTOMERS = 100_000
N_OFFERS = 40
N_TRANSACTIONS = 5_000_000
N_SESSION_EVENTS = 20_000_000
SEED = 42

# COMMAND ----------
# MAGIC %md ## TODO: generate tables
# MAGIC - Use Spark range + rand() with fixed SEED for reproducibility.
# MAGIC - `session_events` must carry an event-time column for RollingWindow features.
# MAGIC - `labels` timestamps must precede nothing they shouldn't (clean point-in-time semantics).
# MAGIC - **Label columns must NOT exist in any feature source table.**
# MAGIC - For the streaming demo, notebook 03 replays `session_events` into Kafka
# MAGIC   (or use the synthetic producer in `scripts/`).

print("TODO: implement synthetic data generation")
