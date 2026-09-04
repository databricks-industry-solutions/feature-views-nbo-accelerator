# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Serving Benchmark — Feature Read + Rank (Part 1 online-lookup path)
# MAGIC The clean Feature Views proof point: **online feature lookup + ranking**, no retrieval.
# MAGIC The request carries only `{customer_id, offer_id, offer attrs}`; the route-optimized
# MAGIC `nbo-ranker-online` endpoint fetches the 5 customer features from the online store by
# MAGIC `customer_id` and scores the passed-in candidate set (here: the full 40-offer catalog).
# MAGIC
# MAGIC **Measured in-region** — this is the path that maps directly to the personalization
# MAGIC reference (~10ms feature-serving read + ~30ms model-serving inference):
# MAGIC
# MAGIC | Stage | In-region estimate | Notes |
# MAGIC |---|---|---|
# MAGIC | Online feature read + rank (40 offers) | **~15ms** | route-optimized endpoint, warm (see note) |
# MAGIC | (laptop-measured, incl ~80ms cross-region WAN RTT) | ~120–160ms | subtract WAN for in-region |
# MAGIC
# MAGIC No Vector Search here — for a 40-offer catalog you score everything directly. Retrieval is a
# MAGIC Part 2 concern (catalog-scale). This keeps Part 1's latency story to the two numbers that
# MAGIC matter for the Feature Views value prop: **feature read + rank**.

# COMMAND ----------
# MAGIC %pip install "databricks-sdk>=0.30" "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-online")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")

import time
import statistics
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------
# MAGIC %md ## Candidate set = the full offer catalog; customer features looked up online by key
# COMMAND ----------
offers = [r.asDict() for r in spark.table(f"`{catalog}`.{schema}.offers").collect()]
customers = [r.asDict() for r in
             spark.table(f"`{catalog}`.{schema}.customers").select(
                 "customer_id", "loyalty_tier", "risk_band").limit(200).collect()]
print(f"{len(customers)} customers × {len(offers)} offers")

# COMMAND ----------
# MAGIC %md ## Warm, then benchmark
# MAGIC A route-optimized endpoint requires an OAuth token **downscoped to the endpoint**. The
# MAGIC notebook runtime identity cannot mint one (`w.serving_endpoints_data_plane` → `OAuth tokens
# MAGIC are not available for runtime authentication`), so this queries via a **service principal**
# MAGIC using `client_credentials` + `authorization_details` (`query_inference_endpoint`). The SP id
# MAGIC and secret live in the `nbo` secret scope; the SP must have CAN_QUERY on the endpoint.
# COMMAND ----------
import requests, json

ep   = w.serving_endpoints.get(name=ranker_endpoint)
url  = ep.data_plane_info.query_info.endpoint_url            # full https://….../invocations
EPID = ep.id                                                 # alphanumeric endpoint id
host = w.config.host
CID  = dbutils.secrets.get("nbo", "sp_client_id")
CSEC = dbutils.secrets.get("nbo", "sp_client_secret")

# Downscope the SP token to query_inference_endpoint on THIS endpoint (route-optimized requirement).
authz = json.dumps([{"type": "workspace_permission", "object_type": "serving-endpoints",
                     "object_path": f"/serving-endpoints/{EPID}",
                     "actions": ["query_inference_endpoint"]}])
resp = requests.post(f"{host}/oidc/v1/token", auth=(CID, CSEC),
                     data={"grant_type": "client_credentials", "scope": "all-apis",
                           "authorization_details": authz})
if resp.status_code != 200:
    raise RuntimeError(
        f"SP token request failed: {resp.status_code} — {resp.text[:300]}\n"
        "A 403 here means this compute's egress IP is not on the workspace IP access list. "
        "Run this notebook on SERVERLESS compute (its egress NAT is allowlisted), not a classic "
        "all-purpose cluster."
    )
tok = resp.json()["access_token"]

def rank(customer_id):
    # Request carries ONLY customer_id + offer fields. The endpoint fetches the 5 customer
    # features (avg_balance_30d, spend_90d, txn_count_7d, loyalty_tier, risk_band) online.
    recs = [{
        "customer_id": customer_id,
        "offer_id": o["offer_id"],
        "product_category": o["product_category"],
        "base_reward": float(o["base_reward"]),
        "tier_requirement": int(o["tier_requirement"]),
    } for o in offers]
    r = requests.post(url, headers={"Authorization": f"Bearer {tok}"},
                      json={"dataframe_records": recs})
    r.raise_for_status()
    return r.json()["predictions"]

for i in range(12):  # warm
    rank(customers[i % len(customers)]["customer_id"])

N = 100
lat = []
for i in range(N):
    t0 = time.perf_counter()
    rank(customers[i % len(customers)]["customer_id"])
    lat.append((time.perf_counter() - t0) * 1000)

def pct(a, p):
    if not a:
        return None
    xs = sorted(a)
    # nearest-rank: the ceil(p/100 * n)-th value (1-based) → 0-based index below. int(n*p/100)
    # was off by one (e.g. p99 over N=100 returned the max, not the 99th value).
    idx = max(0, (p * len(xs) + 99) // 100 - 1)
    return round(xs[min(idx, len(xs) - 1)], 1)

print(f"N={N} (each = online feature read + rank over {len(offers)} offers)")
print(f"feature-read + rank  p50={pct(lat,50)}ms  p95={pct(lat,95)}ms  p99={pct(lat,99)}ms  mean={round(statistics.mean(lat),1)}ms")
print("NOTE: measured from this driver; subtract cross-region WAN RTT for the in-region number.")

# COMMAND ----------
# MAGIC %md ## Persist results to Delta for the dashboard
# COMMAND ----------
import pandas as pd
rows = [{"stage": "feature_read_and_rank", "p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99)}]
(spark.createDataFrame(pd.DataFrame(rows))
      .write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.part1_latency_results"))
print(f"Wrote {catalog}.{schema}.part1_latency_results")

# Stub the Part 2 results table so the dashboard's real-time freshness/serving section
# always resolves, even on a Part-1-only deploy (Part 2 is gated off by default). Part 2's
# the feature-freshness benchmark writes the Part 2 event->online freshness + serving latency.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.{schema}.part2_latency_results
    (stage STRING, p50 DOUBLE, p95 DOUBLE, p99 DOUBLE)
""")
print(f"Ensured {catalog}.{schema}.part2_latency_results exists (empty until Part 2 runs).")
