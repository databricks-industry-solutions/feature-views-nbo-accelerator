# Real-Time Next-Best-Offer with Databricks Feature Views

![Solution Accelerator](https://img.shields.io/badge/Solution-Accelerator-FF3621?logo=databricks)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Governed-00A972)
![Serverless](https://img.shields.io/badge/Serverless-Compute-1B3139)
![Feature Views](https://img.shields.io/badge/Feature%20Views-Declarative-FF3621)
![Latency](https://img.shields.io/badge/E2E%20Serving-%3C300ms-00A972)

> A production-grade, real-time **Next-Best-Offer (NBO)** recommender for financial services,
> built end-to-end on Databricks with **Feature Views** at the center. One declarative feature
> definition powers both point-in-time-correct offline training and low-latency online serving —
> proving *"author a feature once, serve it everywhere."*

The accelerator is split into **two self-contained parts** so you can get the core Feature Views
win fast, then scale up to the full real-time recommender:

| | Part 1 — Feature Views Fundamentals | Part 2 — Real-Time Two-Stage Recommender |
|---|---|---|
| **Goal** | Author once → train + serve, sub-50ms feature-read + rank | Catalog-scale retrieval + freshest in-session intent |
| **Adds** | Batch feature views, online lookup, route-optimized ranking | Streaming (MSK) features + Vector Search retrieval |
| **No** | Vector Search, streaming | — (builds on Part 1) |
| **Headline** | feature-read + rank **p50 < 50ms in-region** | two-stage serving **p95 < 300ms**; freshness **~200ms p99** |
| **Notebooks** | `notebooks/part1_feature_views/` (00–06) | `notebooks/part2_realtime_two_stage/` (07–11) |
| **Job** | `resources/part1_job.yml` | `resources/part2_job.yml` (assumes Part 1 has run) |

Both parts share one catalog, one set of feature definitions, and **one ranker endpoint** — Part 2
reuses Part 1's ranker unchanged, only adding a retrieval stage in front and a streaming feature.

---

## The story

A retail-banking customer is active in-session (browsing accounts, checking a card, viewing a
loan calculator). We recommend the **next best offer** — a credit card upgrade, a savings
product, a personal loan — in real time as their session intent evolves. The differentiator:
**capture the customer's freshest in-session intent to drive engagement, while reusing the same
feature definitions offline for model training** — eliminating train/serve skew.

---

## Part 1 — Feature Views Fundamentals

*Define a feature once. It powers point-in-time-correct training AND a sub-50ms online ranking
endpoint — no train/serve skew, no retrieval machinery.*

```
Delta: customers, transactions, offers, labels
   │
   ▼  Feature objects (author once) — 3 aggregation + 2 attribute
   ├──────────────► OFFLINE Delta ──► create_training_set (point-in-time) ──► LightGBM ──► UC @prod
   └──────────────► ONLINE Lakebase (nbo_on_*)                                             │
                                                                                           ▼
   request {customer_id, offer_id[]}  ──►  Model Serving  (nbo-ranker-online, route-optimized)
                                              └─ ONLINE LOOKUP of 5 customer features by customer_id
                                                 → rank the candidate set → ranked offers
```

- **Feature modes:** `SlidingWindow` (30-day avg balance, 7-day txn count), `TumblingWindow`
  (90-day spend), `ColumnSelection` (loyalty tier, risk band).
- **Serving:** the request carries only `{customer_id, offer_id, offer attrs}`; the endpoint
  auto-fetches the customer features from the online store by `customer_id`. For the 40-offer
  catalog you score all offers — **no retrieval needed**.
- **Latency (measured):** online feature-read + rank **≈15ms in-region** (route-optimized endpoint,
  scale-to-zero off) — squarely in the personalization reference band (~10ms feature-read + ~30ms
  model-serving).

**Notebooks** (`notebooks/part1_feature_views/`):

| # | Notebook | Purpose |
|---|---|---|
| 00 | `00_setup` | Catalog/schema, Lakebase online store |
| 01 | `01_generate_data` | Synthetic customers, offers, transactions, signal-injected labels |
| 02 | `02_define_feature_views` | Batch features: Sliding / Tumbling / ColumnSelection |
| 03 | `03_materialize_features` | Offline Delta + online Lakebase |
| 04 | `04_train_ranker` | Point-in-time `create_training_set` → LightGBM → UC `@prod` |
| 05 | `05_deploy_ranking_endpoint` | Route-optimized **online-lookup** ranker (no VS) |
| 06 | `06_latency_benchmark` | Feature-read + rank latency |

---

## Part 2 — Real-Time Two-Stage Recommender

*Scale to a real catalog and capture the customer's freshest in-session intent — streaming
features + Vector Search retrieval feeding the same ranker.*

```
in-session events ─► MSK (msk_kafka) ─► RollingWindow streaming FV ─► nbo_stream_* ┐  (4th online feature)
                                                                                    │
Part 1 batch online features ───────────────────────────────────────────────────────┤
offers ─► embeddings ─► Vector Search index (offers_index)                            │
                                                                                    ▼
   request {customer_id, in-session context}
      │
      ▼  [1] RETRIEVE  Vector Search (query_vector, top-K)          ~90ms
      ▼  [2] RANK      Part 1 endpoint + online lookup              ~15ms + online read
      → ranked offers
```

- **Streaming features:** `create_stream` registers the MSK topic; `RollingWindow` computes
  `cust_clicks_10m` (in-session click count, last 10 min) — the blog's freshest-intent differentiator.
- **Retrieval:** managed-embedding Vector Search index over `offer_text`; retrieve top-K by semantic
  match to the in-session context. This is what makes ranking scale past a tiny catalog.
- **Two numbers, kept honest:** serving **p95 < 300ms** (retrieve + rank) *and* streaming freshness
  **~200ms p99** (event → online availability) — reported separately.

**Notebooks** (`notebooks/part2_realtime_two_stage/`):

| # | Notebook | Purpose |
|---|---|---|
| 07 | `07_kafka_topic_and_producer` | Seed the MSK topic + synthetic in-session event producer |
| 08 | `08_streaming_feature_views` | `RollingWindow` streaming FV (`cust_clicks_10m`) via `StreamingMode` |
| 09 | `09_vector_search_index` | Offer embeddings → Vector Search index |
| 10 | `10_two_stage_serving` | Stage 1 retrieve (VS) → Stage 2 rank (Part 1 endpoint) |
| 11 | `11_latency_and_freshness` | Serving p50/p95/p99 + streaming freshness |

### Why Vector Search is Part 2, not Part 1
For the 40-offer demo catalog you can score every offer directly (Part 1). Retrieval earns its
place only at catalog scale — thousands of products / eligibility-scoped offers — where ANN narrows
N→K under the latency budget. Part 2 shows the pattern on the small catalog but frames it as *"the
architecture you use at scale."*

---

## Serving-latency optimization journey (Part 2, measured on `fe-vm-ttan-vm`)

| Config | Retrieval p50 | Ranking p50 | E2E p95 | E2E p99 |
|---|---|---|---|---|
| v1 — `query_text` + proxy endpoint + scale-to-zero | ~145ms | ~30ms | ~235ms | ~660ms |
| **v2 — `query_vector` + route-optimized + warm** | **~90ms** | **≈15ms (in-region)** | **~120ms** | **~210ms** |

1. **Retrieval** — embed the session context client-side and pass `query_vector` instead of
   `query_text`; the managed-embedding FMAPI hop was ~50ms (retrieval p50 139→90ms, p99 352→178ms).
2. **Ranking** — recreate the endpoint with `route_optimized=True` + `scale_to_zero=False`; bypasses
   the serving proxy and removes the cold-start p99 tail. Route-optimized endpoints must be queried
   via the data-plane client (`w.serving_endpoints_data_plane`). In-region ranking ≈15ms. (A
   laptop-measured 85ms is dominated by ~80ms cross-region WAN RTT.)

---

## What's inside

| Path | Contents |
|---|---|
| `notebooks/part1_feature_views/` | Part 1 (00–06): batch feature views → train → online-lookup ranking |
| `notebooks/part2_realtime_two_stage/` | Part 2 (07–11): streaming FVs + Vector Search + two-stage serving |
| `apps/recommender-app/` | Databricks App: live NBO demo UI + real-time latency meter (Part 2 capstone) |
| `dashboards/` | AI/BI dashboard: latency percentiles, offer quality, feature freshness |
| `resources/` | Asset Bundle: `part1_job.yml`, `part2_job.yml`, app + dashboard |
| `databricks.yml` | Databricks Asset Bundle — one-command deploy |

## Getting started

1. Clone this project into your Databricks workspace.
2. Deploy the bundle: `databricks bundle deploy` (or the **Asset Bundle Editor**).
3. Run **`[NBO] Part 1`** end-to-end first. Then run **`[NBO] Part 2`** (it reuses Part 1's catalog,
   features, online store, and ranker).
4. Launch the recommender app and open the latency dashboard.

### Prerequisites

- DBR **17.0 ML** or later; `databricks-feature-engineering >= 0.16.0`; **serverless (latest env)**
- **Lakebase** online store, **Model Serving** (Part 1); **Vector Search** + a **Kafka/MSK** UC
  connection (Part 2)
- A Unity Catalog catalog on **standard storage** (streaming Feature Views cannot use default storage)

## Contributing

Clone locally, validate with `databricks bundle validate`, and open a PR with peer review.

## License

&copy; 2026 Databricks, Inc. All rights reserved. Provided under the Databricks License.
See `LICENSE.md`. Third-party dependencies and their licenses are documented in `NOTICE.md`.
