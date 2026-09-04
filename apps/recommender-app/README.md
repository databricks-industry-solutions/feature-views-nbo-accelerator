# Recommender App — Real-Time Next-Best-Offer

Streamlit Databricks App that demos the **rank-all** recommender with a **live latency
meter**, proving the sub-300ms serving path.

## What it does
1. Pick a customer (loyalty tier + risk band shown).
2. **Recommend** scores the full offer catalog in one shot:
   - The `nbo-ranker-online` Model Serving endpoint fetches the customer's features
     from the online store **by `customer_id`** at request time, then scores every
     offer. No retrieval stage — for a catalog this size you rank everything directly
     (mirrors notebook `benchmark/feature_serving_benchmark.py`).
3. Feature-read+rank latency renders as a live metric, colored against the 300ms budget.
4. A reference panel reads the benchmarked percentiles from `part1_latency_results`
   (written by the feature-serving benchmark).

## Resources (wire via Apps UI → Configure → + Add resource)
| Key (`valueFrom`) | Resource | Permission |
|---|---|---|
| `sql-warehouse` | a SQL warehouse | Can use |
| `serving-endpoint` | `nbo-ranker-online` | Can query |

Catalog/schema are passed as plain env values in `app.yaml`.

## Run locally
```bash
pip install -r requirements.txt
export DATABRICKS_CONFIG_PROFILE=<your-profile>
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
streamlit run app.py
```

## Deploy
```bash
databricks apps deploy nbo-recommender \
  --source-code-path /Workspace/Users/<you>/nbo_accelerator/apps/recommender-app \
  -p <your-profile>
```
Then add the SQL warehouse + serving endpoint resources in the app's Configure tab
and grant the app's service principal `SELECT` on the catalog and `Can Query` on the endpoint.
