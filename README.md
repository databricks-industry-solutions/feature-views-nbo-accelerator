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

| | Part 1 — Feature Views Fundamentals | Part 2 — Real-Time Streaming Recommender |
|---|---|---|
| **Goal** | Author once → train + serve, sub-50ms feature-read + rank | Freshest in-session intent as a streaming feature |
| **Adds** | Batch feature views, online lookup, route-optimized ranking | Streaming (MSK) `RollingWindow` feature feeding the ranker |
| **No** | Vector Search, streaming | Vector Search (rank-all; retrieval is out of scope) |
| **Headline** | feature-read + rank **p50 < 50ms in-region** | feature-read + rank ≈15ms; freshness **~200ms p99** |
| **Notebooks** | `notebooks/part1_feature_views/` (00–06) | `notebooks/part2_realtime_two_stage/` (07–10) |
| **Job** | `resources/part1_job.yml` | `resources/part2_job.yml` (assumes Part 1 has run) |

Both parts share one catalog and one set of feature definitions. Part 2 re-logs the ranker with the
streaming `cust_clicks_10m` feature added — same online-lookup pattern, one more feature.

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

## Part 2 — Real-Time Streaming Recommender

*Capture the customer's freshest in-session intent — a streaming feature feeding the same ranker,
scored in real time. No Vector Search: personalization lives entirely in the ranker.*

```
in-session events ─► MSK (msk_kafka) ─► RollingWindow streaming FV (cust_clicks_10m) ─► online store
                                                                                          │
Part 1 batch online features (5) ─────────────────────────────────────────────────────────┤ (6 online features)
                                                                                          ▼
   request {customer_id, offer_id + offer attrs}   (all offers — rank-all, NO retrieval)
      │
      ▼  ONLINE LOOKUP of 6 customer features by customer_id (incl. live cust_clicks_10m)
      ▼  RANK on nbo-ranker-realtime (route-optimized)          ≈15ms in-region
      → ranked offers
```

- **Streaming feature:** `create_stream` registers the MSK topic; a `RollingWindow` computes
  `cust_clicks_10m` (in-session click count, last 10 min) via `StreamingMode` — the blog's
  freshest-intent differentiator. It's looked up online by `customer_id` like any batch feature.
- **Ranker consumes it:** the ranker is re-logged (notebook 09) with `cust_clicks_10m` in the
  training set, so the endpoint fetches it live at serve time and it actually changes recommendations.
- **Rank-all, no retrieval:** for the ~40-offer catalog we score every offer directly. Vector Search
  would add ~90ms for no benefit at this catalog size — deliberately out of scope (see note below).
- **Two numbers, kept honest and live-measured:** serving **feature-read + rank** (≈15ms in-region)
  *and* streaming freshness **event → online availability** (~200ms p99 reference), reported separately.

**Notebooks** (`notebooks/part2_realtime_two_stage/`):

| # | Notebook | Purpose |
|---|---|---|
| 07 | `07_kafka_topic_and_producer` | Seed the MSK topic + synthetic in-session event producer |
| 08 | `08_streaming_feature_views` | `RollingWindow` streaming FV (`cust_clicks_10m`) via `StreamingMode` |
| 09 | `09_realtime_serving` | Re-log ranker with the streaming feature → route-optimized rank-all endpoint |
| 10 | `10_latency_and_freshness` | Live serving latency + event→online freshness benchmark |

### Why no Vector Search here (and when you'd add it)
For the ~40-offer NBO catalog you can score every offer directly — retrieval adds a fixed ~90ms for
no benefit and no accuracy gain (the ranker is the accuracy engine). Vector Search retrieval earns
its place only at **catalog scale** (thousands of products / eligibility-scoped offers), where ANN
narrows N→K to keep ranking cost bounded under the latency budget. That's a documented extension, not
part of this real-time streaming demo.

---

## What's inside

| Path | Contents |
|---|---|
| `notebooks/part1_feature_views/` | Part 1 (00–06): batch feature views → train → online-lookup ranking |
| `notebooks/part2_realtime_two_stage/` | Part 2 (07–10): streaming FV + real-time rank-all serving + freshness |
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
- **Lakebase** online store, **Model Serving** (Part 1); a **Kafka/MSK** UC connection (Part 2)
- A Unity Catalog catalog on **standard storage** (streaming Feature Views cannot use default storage)

## Contributing

Clone locally, validate with `databricks bundle validate`, and open a PR with peer review.

## License

&copy; 2026 Databricks, Inc. All rights reserved. Provided under the Databricks License.
See `LICENSE.md`. Third-party dependencies and their licenses are documented in `NOTICE.md`.
