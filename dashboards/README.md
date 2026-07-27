# Dashboards

AI/BI dashboard for the accelerator. `nbo_dashboard.json` is the serialized Lakeview
definition; deploy it after Part 2 notebook 11 populates `latency_results`.

## Panels (as built)
- **KPI row** — customers, transactions (feature source), end-to-end serving **p95 vs 300ms budget**.
- **Serving latency by stage** — grouped bars of p50/p95/p99 for retrieval, ranking, and e2e
  (proves the sub-300ms path).
- **Offer acceptance by loyalty tier** — shows the ranker learned real signal
  (platinum ~60% → bronze ~26%).
- **Offer catalog by category** + **latency percentile table** — recommendation catalog and detail.

Source: `fins-industry-solutions.nbo` — `latency_results`, `labels` × `customers`, `offers`.

## Deploy
Queries are validated against the live tables. Deploy on `fe-vm-ttan-vm`:
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
databricks api post /api/2.0/lakeview/dashboards -p fe-vm-ttan-vm --json @/tmp/dash.json
# then publish:
databricks api post /api/2.0/lakeview/dashboards/<id>/published -p fe-vm-ttan-vm \
  --json '{"embed_credentials": true, "warehouse_id": "<warehouse-id>"}'
```

## Deployed instance
- Dashboard ID: `01f18466ba9b1d70b4214aacb51fa203` (published on `fe-vm-ttan-vm`).

## Future panels (need streaming path)
- **Feature freshness** — event → online-availability lag for streaming RollingWindow features
  (the ~200ms p99 launch figure), once notebook 01b/02 streaming is wired.
- **Online store health** — Lakebase read QPS and capacity utilization.
