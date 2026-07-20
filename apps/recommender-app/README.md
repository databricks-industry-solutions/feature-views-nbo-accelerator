# Recommender App — Real-Time Next-Best-Offer

Streamlit Databricks App that demos the two-stage recommender with a **live latency
meter**, proving the sub-300ms serving path.

## What it does
1. Pick a customer (loyalty tier + risk band shown) and an in-session intent.
2. **Recommend** runs the two-stage path:
   - **Stage 1 — retrieval:** Vector Search ANN over `offers_index` (offer embeddings).
   - **Stage 2 — ranking:** the `nbo-ranker` Model Serving endpoint scores customer × candidates.
3. Per-stage + end-to-end latency render as live metrics, colored against the 300ms budget.
4. A reference panel reads the benchmarked percentiles from `latency_results` (notebook 06).

## Resources (wire via Apps UI → Configure → + Add resource)
| Key (`valueFrom`) | Resource | Permission |
|---|---|---|
| `sql-warehouse` | a SQL warehouse | Can use |
| `serving-endpoint` | `nbo-ranker` | Can query |

The Vector Search index/endpoint and catalog/schema are passed as plain env values
in `app.yaml` (the app queries the index via the SDK using the service principal).

## Run locally
```bash
pip install -r requirements.txt
export DATABRICKS_CONFIG_PROFILE=fe-vm-ttan-vm
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
streamlit run app.py
```

## Deploy
```bash
databricks apps deploy nbo-recommender \
  --source-code-path /Workspace/Users/<you>/nbo_accelerator/apps/recommender-app \
  -p fe-vm-ttan-vm
```
Then add the SQL warehouse + serving endpoint resources in the app's Configure tab
and grant the app's service principal `SELECT` on the catalog and `Can Query` on the endpoint.
