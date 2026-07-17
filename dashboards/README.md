# Dashboards

AI/BI dashboard for the accelerator. Build after notebook 06 populates the results table.

## Panels
- **Latency percentiles** — E2E p50/p95/p99 vs the 300ms budget; per-stage breakdown.
- **Feature freshness** — event → online-availability lag for streaming features (~200ms p99).
- **Offer quality** — acceptance rate / NDCG of recommended offers on a holdout.
- **Online store health** — read QPS, Lakebase capacity utilization.

Source table: `${catalog}.${schema}.latency_results` (+ model eval tables).
