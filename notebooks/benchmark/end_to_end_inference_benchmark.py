# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Inference Benchmark — Decomposed Feature Lookup + Model Inference (LOCAL)
# MAGIC A per-stage breakdown of a single feature-read + rank call, in the shape of the reference table:
# MAGIC
# MAGIC | Metric | What it measures | p50 | p75 | p90 | p95 |
# MAGIC |---|---|---|---|---|---|
# MAGIC | **Feature lookup** (`model_lookup_ms`) | online feature read by `customer_id` | … | … | … | … |
# MAGIC | **Inference** (`model_inference_ms`) | LightGBM `predict_proba` over the offer catalog | … | … | … | … |
# MAGIC | **Total model time** (`model_total_ms`) | lookup + inference (in-container) | … | … | … | … |
# MAGIC | **End-to-end round trip** (`round_trip_ms`) | full data-plane call caller→response *(optional)* | … | … | … | … |
# MAGIC | **Network overhead** (`round_trip_ms - model_total_ms`) | round trip minus model time *(optional)* | … | … | … | … |
# MAGIC
# MAGIC **This is the "Option A" artifact kept LOCAL.** The `RankerWithTiming` pyfunc below reads the
# MAGIC customer features itself and self-reports its internal lookup-vs-inference split — exactly the
# MAGIC model you would `log_model` + deploy to get a server-side breakdown. Here it runs **in-process on
# MAGIC the driver** (no endpoint deploy, not wired into the Part 1 job).
# MAGIC
# MAGIC ### What is faithful, and the one caveat
# MAGIC - **Inference** is measured directly and is fully representative (the pipeline's `predict_proba`).
# MAGIC - **Feature lookup** has a pluggable backend (`LOOKUP_MODE`):
# MAGIC   - `"lakebase"` — a real Lakebase point-read by key (the true ~single-digit-ms online read). Requires
# MAGIC     the online-store connection; see the preflight cell. **Use this for the real lookup number.**
# MAGIC   - `"prefetch"` *(default)* — an in-memory read of pre-fetched feature values. Runs anywhere with no
# MAGIC     Lakebase wiring, but the lookup row is an **assembly-only lower bound (~0ms), not a Lakebase read** —
# MAGIC     it is labeled as such in the output so it is never mistaken for the real number.
# MAGIC - **Round trip / network overhead** are optional (`MEASURE_ENDPOINT`); they query the already-deployed
# MAGIC   `nbo-ranker-online` endpoint (a query, not a deploy). `databricks-feature-engineering` has no fast
# MAGIC   client-side online read, so this endpoint path is the other way to see the real in-container lookup cost.

# COMMAND ----------
# MAGIC %pip install "databricks-sdk>=0.30" "databricks-feature-engineering>=0.16.0" lightgbm scikit-learn pg8000
# MAGIC # pg8000 is a PURE-PYTHON Postgres driver. The psycopg2/psycopg3 binary wheels SIGABRT at native
# MAGIC # import on this serverless image (bundled libssl ABI clash), so a native driver is not usable here.
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "nbo")
dbutils.widgets.text("ranker_endpoint", "nbo-ranker-online")
dbutils.widgets.dropdown("lookup_mode", "prefetch", ["prefetch", "lakebase"])
dbutils.widgets.dropdown("measure_endpoint", "false", ["false", "true"])
dbutils.widgets.text("n_requests", "100")
# Lakebase connection (only needed when lookup_mode == "lakebase"). Fill from the preflight cell.
dbutils.widgets.text("lakebase_host", "")
dbutils.widgets.text("lakebase_dbname", "")           # blank → the catalog name (FE syncs online tables into a Postgres DB named after the catalog)
dbutils.widgets.text("lakebase_user", "")
dbutils.widgets.text("lakebase_endpoint_path", "")   # projects/<id>/branches/<id>/endpoints/<id>
dbutils.widgets.text("lakebase_token", "")           # optional: a pre-minted DB credential (avoids in-job SDK minting)
dbutils.widgets.text("lakebase_online_tables", "")   # optional: comma-separated Postgres tables; blank = auto-discover

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ranker_endpoint = dbutils.widgets.get("ranker_endpoint")
LOOKUP_MODE = dbutils.widgets.get("lookup_mode")
MEASURE_ENDPOINT = dbutils.widgets.get("measure_endpoint") == "true"
N = int(dbutils.widgets.get("n_requests"))
if N < 1:
    raise ValueError(f"n_requests must be >= 1 (got {N}).")

import time
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Feature contract (must match notebook 05's training set).
CUST_FEATURES = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
                 "cust_loyalty_tier", "cust_risk_band", "cust_annual_income", "cust_tenure_months"]
# Request-time columns: the 4 offer columns + the visitor's OfferMatch answers (ctx_*).
REQ = ["offer_id", "product_category", "base_reward", "tier_requirement",
       "ctx_goal", "ctx_credit", "ctx_income", "ctx_card_spend", "ctx_session_cat_views"]
# Neutral visitor-context answers for synthetic scoring frames.
CTX_DEFAULTS = {"ctx_goal": "none", "ctx_credit": "good", "ctx_income": 85000.0, "ctx_card_spend": 1500.0,
                "ctx_session_cat_views": 0}
CAT = ["product_category", "cust_loyalty_tier", "cust_risk_band", "ctx_goal", "ctx_credit"]
NUM = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d", "cust_annual_income", "cust_tenure_months",
       "base_reward", "tier_requirement", "ctx_income", "ctx_card_spend", "ctx_session_cat_views"]

def pct(a, p):
    if not a:
        return None
    xs = sorted(a)
    # nearest-rank: the ceil(p/100 * n)-th value (1-based) → 0-based index below. int(n*p/100)
    # was off by one (e.g. p99 over N=100 returned the max, not the 99th value).
    idx = max(0, (p * len(xs) + 99) // 100 - 1)
    return round(xs[min(idx, len(xs) - 1)], 1)

# COMMAND ----------
# MAGIC %md ## Candidate set + benchmark customers
# COMMAND ----------
offers = [r.asDict() for r in spark.table(f"`{catalog}`.{schema}.offers").collect()]
customers = [r.asDict() for r in
             spark.table(f"`{catalog}`.{schema}.customers").select("customer_id").limit(200).collect()]
print(f"{len(customers)} customers × {len(offers)} offers")

# COMMAND ----------
# MAGIC %md ## Acquire the ranker pipeline
# MAGIC Train the identical pipeline (notebook 05 hyperparameters) on a small label sample. Inference
# MAGIC latency is governed by `n_estimators` / `num_leaves` (fixed here), not by training-row count, so a
# MAGIC small-sample fit is a faithful stand-in for the `@prod` model's per-call inference cost. (To score the
# MAGIC exact registered artifact instead, load it from `models:/{catalog}.{schema}.nbo_ranker_online@prod`.)
# COMMAND ----------
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from lightgbm import LGBMClassifier

fe = FeatureEngineeringClient()

def gf(n):
    return fe.get_feature(full_name=f"{catalog}.{schema}.{n}")

cust_feats = [gf(n) for n in CUST_FEATURES]

TRAIN_SAMPLE = 50000
labels = (spark.table(f"`{catalog}`.{schema}.labels").limit(TRAIN_SAMPLE)
          .withColumn("updated_at", F.col("ts")))
offers_df = spark.table(f"`{catalog}`.{schema}.offers").select(
    "offer_id", "product_category", "base_reward", "tier_requirement")
labels = labels.join(offers_df, on="offer_id", how="left")

# ColumnSelection request features (the 4 offer columns + the ctx_* visitor columns, which the
# labels table already carries) were registered in notebook 05; fetch them to build the same training set.
offer_feats = [gf(c) for c in REQ]
ts = fe.create_training_set(df=labels, features=cust_feats + offer_feats, label="accepted",
                            exclude_columns=["record_id", "customer_id", "ts", "updated_at"])
tdf = ts.load_df().toPandas()

X = tdf[CAT + NUM].copy()
for c in CAT:
    X[c] = X[c].astype(str)
for c in NUM:
    X[c] = pd.to_numeric(X[c], errors="coerce")
yv = tdf["accepted"].astype(int)

ranker = Pipeline([
    ("pre", ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT),
         ("num", "passthrough", NUM)], remainder="drop")),
    ("clf", LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                           subsample=0.8, colsample_bytree=0.8, random_state=42)),
])
ranker.fit(X, yv)
print("ranker fitted (n_estimators=300, num_leaves=31)")

# COMMAND ----------
# MAGIC %md ## Pre-fetch the benchmark customers' feature values (offline, once)
# MAGIC These supply the feature vector for inference in every mode, and back the `"prefetch"` lookup.
# COMMAND ----------
cust_ids = [c["customer_id"] for c in customers]
# PIT keys: the agg features key off `ts`, the ColumnSelection features off `updated_at`.
# Set both to "now" so the lookup returns the latest available value (the online-equivalent).
keys_df = (spark.createDataFrame(pd.DataFrame({"customer_id": cust_ids}))
           .withColumn("ts", F.current_timestamp())
           .withColumn("updated_at", F.current_timestamp()))
feat_df = (fe.create_training_set(df=keys_df, features=cust_feats, label=None,
                                  exclude_columns=["ts", "updated_at"])
           .load_df().toPandas())
FEATURES = {row["customer_id"]: {k: row[k] for k in CUST_FEATURES}
            for _, row in feat_df.iterrows()}
missing = [cid for cid in cust_ids if cid not in FEATURES]
if missing:
    print(f"WARNING: {len(missing)} customers had no offline feature row; dropping them.")
    cust_ids = [cid for cid in cust_ids if cid in FEATURES]
if not cust_ids:
    raise RuntimeError("No benchmark customers have an offline feature row — cannot run. Confirm "
                       "materialization (notebook 03) populated the offline feature store for this schema.")
print(f"pre-fetched features for {len(FEATURES)} customers")

# COMMAND ----------
# MAGIC %md ## (Optional) Lakebase preflight — only for `lookup_mode = "lakebase"`
# MAGIC Discover the online-store connection + the Postgres table that holds the customer features, then
# MAGIC paste the values into the widgets above. Run notebook 03's inspection cell to see the materialized
# MAGIC online table names (`m.table_name`). Then, with the Lakebase CLI:
# MAGIC ```
# MAGIC databricks postgres list-projects --profile <p>
# MAGIC databricks postgres list-branches   projects/<id> --profile <p>
# MAGIC databricks postgres list-endpoints  projects/<id>/branches/<id> --profile <p>
# MAGIC databricks postgres get-endpoint    projects/<id>/branches/<id>/endpoints/<id> --profile <p>   # → status.hosts.host
# MAGIC ```
# MAGIC Set `lakebase_host`, `lakebase_user` (your username / SP client id), and
# MAGIC `lakebase_endpoint_path`. Leave `lakebase_dbname` blank to use the catalog name (FE syncs
# MAGIC online tables into a Postgres DB named after the catalog). Tables are auto-discovered; the 7
# MAGIC `cust_*` features may span several online tables, so set `lakebase_online_tables` only to
# MAGIC override discovery.
# COMMAND ----------
_lb_host = dbutils.widgets.get("lakebase_host")
# FE syncs each catalog's online tables into a Postgres database named after the CATALOG (not
# "databricks_postgres"); default to the catalog name when the widget is left blank.
_lb_dbname = dbutils.widgets.get("lakebase_dbname") or catalog
_lb_user = dbutils.widgets.get("lakebase_user")
_lb_ep = dbutils.widgets.get("lakebase_endpoint_path")
_lb_token_widget = dbutils.widgets.get("lakebase_token")
_lb_tables_widget = dbutils.widgets.get("lakebase_online_tables")

def _lakebase_token():
    """Return the Lakebase DB credential from the `lakebase_token` widget. Mint one with
    `databricks postgres generate-database-credential <endpoint_path>` (valid ~1h) and paste it in.
    A pre-minted token is required: it is the only path verified to work here, and it sidesteps the
    in-job SDK-auth / credential-API-shape issues, so no unverified SDK fallback is attempted."""
    if _lb_token_widget:
        return _lb_token_widget
    raise RuntimeError(
        "lookup_mode='lakebase' requires the lakebase_token widget. Mint one with "
        "`databricks postgres generate-database-credential " + (_lb_ep or "<endpoint_path>") + "` "
        "and paste it into the lakebase_token widget (it is valid for about an hour).")

def _lakebase_conn():
    """Open a connection to the Lakebase online store with pg8000 (pure Python — no native libpq,
    so no SIGABRT). TLS via a default SSL context; the OAuth DB credential is the password."""
    import pg8000, ssl
    if not (_lb_host and _lb_user):
        raise RuntimeError("lookup_mode='lakebase' needs lakebase_host and lakebase_user set — "
                           "see the preflight cell.")
    return pg8000.connect(host=_lb_host, port=5432, database=_lb_dbname, user=_lb_user,
                          password=_lakebase_token(), ssl_context=ssl.create_default_context())

def _discover_lookup_plan(conn):
    """Map each of the 7 customer features to the Postgres online table that serves it. FE spreads
    them across per-feature online tables; prefer base tables over *_latest_view / *_partial_aggregates.
    Filter in Python to avoid array-parameter binding differences across drivers."""
    cur = conn.cursor()
    cur.execute("SELECT table_schema, table_name, column_name FROM information_schema.columns")
    # keep only the served feature columns; skip system/internal schemas (incl. FE partition backing).
    _skip = {"pg_catalog", "information_schema", "pg_toast"}
    rows = [(s, t, c) for (s, t, c) in cur.fetchall()
            if c in CUST_FEATURES and s not in _skip and not str(s).startswith("__")]
    cur.close()
    def worse(tbl):  # views/partials sort after base tables
        return tbl.endswith("_latest_view") or tbl.endswith("_partial_aggregates")
    best = {}
    for sch, tbl, col in rows:
        if col not in best or (worse(best[col][1]) and not worse(tbl)):
            best[col] = (sch, tbl)
    plan = {}  # (schema, table) -> [cols]
    for col, key in best.items():
        plan.setdefault(key, []).append(col)
    return plan

# One pooled connection reused across the run (mirrors a warm serving container).
_LB = _lakebase_conn() if LOOKUP_MODE == "lakebase" else None
if LOOKUP_MODE == "lakebase":
    if _lb_tables_widget.strip():
        # explicit override: "schema.table" (or bare "table" → defaults to the `schema` widget) per
        # feature-bearing table (cols auto-detected per table). Always normalize to a (schema, table) tuple.
        def _parse_tbl(t):
            parts = t.strip().split(".", 1)
            return (parts[0], parts[1]) if len(parts) == 2 else (schema, parts[0])
        _LOOKUP_PLAN = {_parse_tbl(t): None for t in _lb_tables_widget.split(",") if t.strip()}
    else:
        _LOOKUP_PLAN = _discover_lookup_plan(_LB)
    print("lakebase lookup plan:", {f"{s}.{t}": c for (s, t), c in _LOOKUP_PLAN.items()})
    # Fail loudly if discovery covered no online table for some feature — otherwise every lookup
    # silently returns None and the reported lookup time is a meaningless ~0ms over empty reads.
    _covered = {c for cols in _LOOKUP_PLAN.values() if cols for c in cols}
    _missing_feats = [f for f in CUST_FEATURES if f not in _covered]
    if _missing_feats and not _lb_tables_widget.strip():
        raise RuntimeError(
            f"Lakebase discovery found no online table for {_missing_feats} in database "
            f"'{_lb_dbname}'. FE syncs online tables into a Postgres DB named after the CATALOG "
            f"('{catalog}') — set lakebase_dbname to that, or pass lakebase_online_tables. Refusing "
            "to run so the lookup number is not a meaningless ~0ms over empty reads.")

# COMMAND ----------
# MAGIC %md ## `RankerWithTiming` — the instrumented pyfunc (reads features + self-times)
# COMMAND ----------
import mlflow.pyfunc

def lookup_prefetch(cid):
    return dict(FEATURES[cid])

def lookup_lakebase(cid):
    """Read all 7 customer features by key across their online tables (one point-read per table),
    mirroring the serving container's online lookup."""
    feats = {}
    cur = _LB.cursor()
    try:
        for (sch, tbl), cols in _LOOKUP_PLAN.items():
            sel = ", ".join(cols) if cols else "*"
            # FE online (base) tables are keyed by the entity PK — exactly one row per customer_id,
            # the same latest value the serving container reads — so LIMIT 1 is deterministic here.
            cur.execute(f'SELECT {sel} FROM "{sch}"."{tbl}" WHERE customer_id = %s LIMIT 1', (cid,))
            row = cur.fetchone()
            if row is None:
                continue
            names = cols if cols else [d[0] for d in cur.description]
            for k, v in zip(names, row):
                if k in CUST_FEATURES:
                    feats[k] = v
    finally:
        cur.close()
    # A customer may be absent from a given online table (e.g. no recent txns → no sliding-window row).
    # Serving handles that as a null feature, so mirror it: fill missing with None rather than failing.
    for f in CUST_FEATURES:
        feats.setdefault(f, None)
    return feats

LOOKUP_FN = lookup_lakebase if LOOKUP_MODE == "lakebase" else lookup_prefetch

class RankerWithTiming(mlflow.pyfunc.PythonModel):
    """Serving-shaped model: request carries only customer_id + offer fields. The model reads the
    customer features itself (lookup), scores the candidate set (inference), and returns the scores
    with its internal timing split. This is the artifact you'd log_model + deploy for a server-side
    breakdown; here it runs in-process."""
    def __init__(self, model, lookup_fn):
        self._model = model
        self._lookup = lookup_fn

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        cid = model_input["customer_id"].iloc[0]
        t0 = time.perf_counter()
        feats = self._lookup(cid)                       # online feature read by key
        lookup_ms = (time.perf_counter() - t0) * 1000

        frame = model_input[REQ].copy()
        for k, v in feats.items():
            frame[k] = v
        for c in CAT:
            frame[c] = frame[c].astype(str)
        for c in NUM:
            frame[c] = pd.to_numeric(frame[c], errors="coerce")

        t1 = time.perf_counter()
        scores = self._model.predict_proba(frame[CAT + NUM])[:, 1]  # inference
        inference_ms = (time.perf_counter() - t1) * 1000

        out = pd.DataFrame({"score": scores})
        out["model_lookup_ms"] = lookup_ms
        out["model_inference_ms"] = inference_ms
        out["model_total_ms"] = lookup_ms + inference_ms
        return out

served = RankerWithTiming(ranker, LOOKUP_FN)

def _recs(customer_id):
    """Native-typed request records (python float/int, not numpy scalars) — JSON-serializable for
    the endpoint's dataframe_records query."""
    return [{
        "customer_id": customer_id,
        "offer_id": o["offer_id"],
        "product_category": o["product_category"],
        "base_reward": float(o["base_reward"]),
        "tier_requirement": int(o["tier_requirement"]),
        **CTX_DEFAULTS,
    } for o in offers]

def request_records(customer_id):
    return pd.DataFrame(_recs(customer_id))

# COMMAND ----------
# MAGIC %md ## Warm, then benchmark the in-process model (lookup + inference)
# COMMAND ----------
for i in range(12):
    served.predict(None, request_records(cust_ids[i % len(cust_ids)]))

lookup_ms, inference_ms, total_ms = [], [], []
for i in range(N):
    r = served.predict(None, request_records(cust_ids[i % len(cust_ids)]))
    lookup_ms.append(float(r["model_lookup_ms"].iloc[0]))
    inference_ms.append(float(r["model_inference_ms"].iloc[0]))
    total_ms.append(float(r["model_total_ms"].iloc[0]))

print(f"N={N}  (each = 1 customer × {len(offers)} offers)")
print(f"lookup    p50={pct(lookup_ms,50)}  p95={pct(lookup_ms,95)}ms  [{LOOKUP_MODE}]")
print(f"inference p50={pct(inference_ms,50)}  p95={pct(inference_ms,95)}ms")
print(f"total     p50={pct(total_ms,50)}  p95={pct(total_ms,95)}ms")

# COMMAND ----------
# MAGIC %md ## (Optional) End-to-end round trip via the deployed endpoint
# MAGIC Queries the existing `nbo-ranker-online` endpoint (not a deploy). Run this INTERACTIVELY — the
# MAGIC route-optimized data-plane query needs OAuth and fails under job/runtime auth. Round trip includes the real
# MAGIC in-container Lakebase read + inference + container overhead + network. A rough network+container
# MAGIC overhead proxy is the round trip minus the locally-measured model time — the two come from
# MAGIC independent measurement contexts, so it is approximate (differenced at the percentile level).
# COMMAND ----------
round_trip_ms = []
if MEASURE_ENDPOINT:
    dp = w.serving_endpoints_data_plane  # route-optimized → data-plane client
    def rank_endpoint(recs):
        return dp.query(name=ranker_endpoint, dataframe_records=recs).predictions
    # Prebuild native-typed records OUTSIDE the timed region so only the network call is measured.
    warm = [_recs(cust_ids[i % len(cust_ids)]) for i in range(12)]
    bench = [_recs(cust_ids[i % len(cust_ids)]) for i in range(N)]
    for recs in warm:
        rank_endpoint(recs)
    for recs in bench:
        t0 = time.perf_counter()
        rank_endpoint(recs)
        round_trip_ms.append((time.perf_counter() - t0) * 1000)
    print(f"round trip p50={pct(round_trip_ms,50)}  p95={pct(round_trip_ms,95)}ms (from this driver)")
else:
    print("MEASURE_ENDPOINT=false — skipping round trip / network overhead rows.")

# COMMAND ----------
# MAGIC %md ## Assemble the decomposed table (p50 / p75 / p90 / p95)
# COMMAND ----------
lookup_label = ("online Lakebase read by customer_id" if LOOKUP_MODE == "lakebase"
                else "in-memory assembly ONLY — not a Lakebase read (set lookup_mode='lakebase' or "
                     "enable MEASURE_ENDPOINT for the real read)")

def row(metric, measures, samples):
    return {"metric": metric, "what_it_measures": measures,
            "p50": pct(samples, 50), "p75": pct(samples, 75),
            "p90": pct(samples, 90), "p95": pct(samples, 95)}

rows = [
    row("Feature lookup (model_lookup_ms)", lookup_label, lookup_ms),
    row("Inference (model_inference_ms)", "LightGBM predict_proba over the offer catalog", inference_ms),
    row("Total model time (model_total_ms)", "lookup + inference (in-container)", total_ms),
]
# Extra line requested: model inference end-to-end (the full endpoint call) — present when measured.
if round_trip_ms:
    rows.append(row("End-to-end round trip (round_trip_ms)",
                    "full data-plane call, caller → response", round_trip_ms))
    # round_trip_ms and total_ms come from two INDEPENDENT measurement contexts (deployed endpoint
    # vs in-process), so pairing them per-request is invalid — difference at the percentile level.
    # The endpoint round trip already includes its own in-container lookup+inference, so this is a
    # rough network+container overhead proxy (not exact); floor at 0.
    net_row = {"metric": "Network + container overhead (round_trip - model_total, approx)",
               "what_it_measures": "round-trip percentile minus in-process model-time percentile (proxy)"}
    for _p in (50, 75, 90, 95):
        rt, tot = pct(round_trip_ms, _p), pct(total_ms, _p)
        # pct() returns None on an empty sample; only difference when both sides exist.
        net_row[f"p{_p}"] = round(max(0.0, rt - tot), 1) if (rt is not None and tot is not None) else None
    rows.append(net_row)

table = pd.DataFrame(rows)[["metric", "what_it_measures", "p50", "p75", "p90", "p95"]]
print(table.to_string(index=False))

# COMMAND ----------
# MAGIC %md ## Persist to Delta
# COMMAND ----------
out = table.copy()
out["lookup_mode"] = LOOKUP_MODE
out["measured_at"] = pd.Timestamp.now(tz="UTC").isoformat()
(spark.createDataFrame(out)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"`{catalog}`.{schema}.part1_latency_decomposed"))
print(f"Wrote {catalog}.{schema}.part1_latency_decomposed  (lookup_mode={LOOKUP_MODE})")
