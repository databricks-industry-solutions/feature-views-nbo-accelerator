# Dashboards

AI/BI dashboard for the accelerator. `nbo_dashboard.json` is the serialized Lakeview
definition; deploy it after notebook `benchmark/feature_serving_benchmark.py` populates
`part1_latency_results`.

## Panels (as built)
- **KPI row** — customers, transactions (feature source), feature-read+rank **p95 vs 300ms budget**.
- **Serving latency by stage** — grouped bars of p50/p95/p99 for the `feature_read_and_rank`
  path (proves the sub-300ms path).
- **Offer acceptance by loyalty tier** — shows the ranker learned real signal
  (platinum ~60% → bronze ~26%).
- **Offer catalog by category** + **latency percentile table** — recommendation catalog and detail.

Source: `fins_industry_solutions.nbo` — `part1_latency_results`, `labels` × `customers`, `offers`.

## Deploy
Queries are validated against the live tables. Replace `<your-profile>`, `<warehouse-id>`,
and `<you>` with your own values:
```bash
python3 - <<'PY'
import json
d = json.load(open("dashboards/nbo_dashboard.json"))
payload = {"display_name": "NBO Feature Views Accelerator",
           "warehouse_id": "<warehouse-id>",
           "parent_path": "/Workspace/Users/<you>/nbo_accelerator",
           "serialized_dashboard": json.dumps(d)}
open("/tmp/dash.json", "w").write(json.dumps(payload))
PY
databricks api post /api/2.0/lakeview/dashboards -p <your-profile> --json @/tmp/dash.json
# then publish (embed_credentials runs published queries as the publisher's identity —
# only enable it if that identity is appropriate for everyone who can view the dashboard):
databricks api post /api/2.0/lakeview/dashboards/<id>/published -p <your-profile> \
  --json '{"embed_credentials": true, "warehouse_id": "<warehouse-id>"}'
```

## Future panels (need streaming path)
- **Feature freshness** — event → online-availability lag for streaming RollingWindow features,
  once the Part 2 streaming online path is unblocked (see Part 2 status in the root README).
- **Online store health** — Lakebase read QPS and capacity utilization.
