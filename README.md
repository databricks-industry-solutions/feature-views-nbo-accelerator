# Real-Time Next-Best-Offer with Databricks Feature Views

![Solution Accelerator](https://img.shields.io/badge/Solution-Accelerator-FF3621?logo=databricks)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Governed-00A972)
![Serverless](https://img.shields.io/badge/Serverless-Compute-1B3139)
![Feature Views](https://img.shields.io/badge/Feature%20Views-Declarative-FF3621)
![Latency](https://img.shields.io/badge/E2E%20Serving-%3C300ms-00A972)

> A production-grade, real-time **Next-Best-Offer (NBO)** recommender for financial services,
> built end-to-end on Databricks with **Feature Views** at the center. One declarative feature
> definition powers both point-in-time-correct offline training and sub-300ms online serving —
> proving *"author a feature once, serve it everywhere."*

---

## The story

A retail-banking customer is active in-session (browsing accounts, checking a card, viewing a
loan calculator). We recommend the **next best offer** — a credit card upgrade, a savings
product, a personal loan — in real time as their session intent evolves.

The differentiator: we **capture the customer's freshest in-session intent to drive engagement,
while reusing the same feature definitions offline for model training** — eliminating
train/serve skew. The entire online path (in-session features → candidate retrieval → ranking →
response) completes in **under 300ms**.

## Architecture

```
   In-session events ──► Kafka ──► FV RollingWindow (StreamingMode) ─┐
                                                                     │
Batch tables ──► FV Sliding/Tumbling + ColumnSelection ──────────────┼─► Lakebase Online Store
                                                                     │      (~200ms event→available p99)
                                                                     ▼
   Request ─► [1] Candidate retrieval   (Vector Search — offer embeddings, top-N)
              [2] Online feature lookup  (Lakebase read — customer × candidate offers)
              [3] Ranking model          (LightGBM/XGBoost ranker on Model Serving)
              [4] Return ranked offers
```

The **same** `Feature` objects that materialize to the online store also feed
`create_training_set()` with point-in-time joins to train the ranker — the anti-skew proof point.

## Latency budget (the headline claim)

**Measured** on `fe-vm-ttan-vm` (serverless, N=100 after warmup) — see notebook 06:

| Stage | p50 | p95 | p99 |
|---|---|---|---|
| Candidate retrieval (Vector Search ANN) | ~145ms | ~205ms | ~380ms |
| Ranking model inference (Model Serving) | ~30ms | ~55ms | ~290ms |
| **End-to-end** | **~175ms** | **~235ms** | ~660ms |

**p50 and p95 land under the 300ms budget** (mean ~190ms). p99 shows occasional tail spikes,
driven by the managed-embedding FMAPI hop in retrieval (query embedded server-side per request)
plus rare serving cold-slots. Tail-tightening options: embed queries client-side and pass
`query_vector`, use a provisioned-throughput embedding endpoint, or disable scale-to-zero for the demo.

> **Honesty note:** The `~200ms p99` figure from the Feature Views launch is *event-to-online-
> availability* (freshness). Our `<300ms` budget is the *serving retrieve + rank* path.
> We measure and publish **both** numbers separately.

## What's inside

| Path | Contents |
|---|---|
| `notebooks/` | Sequential build: setup → data → feature views → materialize → train → serve → benchmark |
| `apps/recommender-app/` | Databricks App: live NBO demo UI + real-time latency meter |
| `dashboards/` | AI/BI dashboard: feature freshness, endpoint latency percentiles, offer quality |
| `resources/` | Asset Bundle resource definitions (jobs, pipelines, app, endpoints) |
| `scripts/` | Synthetic data generation and load-test harness |
| `databricks.yml` | Databricks Asset Bundle — one-command deploy |

## Getting started

1. Clone this project into your Databricks workspace.
2. Open the **Asset Bundle Editor** (or use the Databricks CLI: `databricks bundle deploy`).
3. Run the deployment. Notebooks execute sequentially via the **Deployments** tab.
4. Launch the recommender app and open the latency dashboard.

### Prerequisites

- DBR **17.0 ML** or later; `databricks-feature-engineering >= 0.16.0`
- **Lakebase** online store, **Vector Search** endpoint, **Model Serving** enabled
- A **Kafka** source with a Unity Catalog connection (or the included synthetic event stream)
- A Unity Catalog catalog on **standard storage** (streaming Feature Views cannot use default storage)

## Notebook flow

| # | Notebook | Purpose |
|---|---|---|
| 00 | `00_setup` | Config, catalog/schema, online store, permissions |
| 01 | `01_generate_synthetic_data` | Customers, offers, transactions, labels (batch) |
| 01b | `01b_kafka_topic_and_producer` | Create topic via UC Kafka connection + stream synthetic in-session events |
| 02 | `02_define_feature_views` | Batch (Sliding/Tumbling), streaming (Rolling), ColumnSelection features |
| 03 | `03_materialize_features` | Offline Delta + online Lakebase materialization |
| 04 | `04_train_ranker` | Point-in-time training set → LightGBM ranker → UC registration |
| 05 | `05_deploy_serving` | Offer embeddings + Vector Search + Model Serving endpoints |
| 06 | `06_latency_benchmark` | Load test the E2E path; publish p50/p95/p99 |

## Contributing

Clone locally, validate with `databricks bundle validate`, and open a PR with peer review.

## License

&copy; 2026 Databricks, Inc. All rights reserved. Provided under the Databricks License.
See `LICENSE.md`. Third-party dependencies and their licenses are documented in `NOTICE.md`.
