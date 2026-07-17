# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Latency Benchmark — Prove <300ms
# MAGIC Load-test the end-to-end online path and publish percentiles. This is a first-class
# MAGIC deliverable, not an afterthought — the <300ms claim must be measured and defensible.

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %md ## Two distinct numbers to report (keep them separate — honesty)
# MAGIC 1. **Freshness**: event → online availability p99 (target ~200ms, the launch figure)
# MAGIC 2. **Serving**: request → ranked response — retrieval + online read + rank + orchestration
# MAGIC
# MAGIC | Stage | Target | Measure |
# MAGIC |---|---|---|
# MAGIC | Vector Search retrieval | 30–50ms | per-call timing |
# MAGIC | Online feature lookup | 10–30ms | per-call timing |
# MAGIC | Ranking inference | 20–60ms | endpoint latency |
# MAGIC | Orchestration + network | 20–40ms | wall-clock minus components |
# MAGIC | **E2E p50 / p95 / p99** | **<300ms** | full harness |

# COMMAND ----------
# MAGIC %md ## Harness
# MAGIC - Concurrent request generator (see scripts/loadtest.py) hitting the serving app path.
# MAGIC - Warm the endpoints first (cold start / scale-to-zero skews p99).
# MAGIC - Record per-stage timings; write results to a Delta table for the dashboard.
RESULTS_TABLE = f"{catalog}.{schema}.latency_results"
# TODO: run harness, aggregate p50/p95/p99 per stage + E2E, write to RESULTS_TABLE
print(f"TODO: benchmark → {RESULTS_TABLE}")
