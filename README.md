# Real-Time Next-Best-Offer with Databricks Feature Views

![Solution Accelerator](https://img.shields.io/badge/Solution-Accelerator-FF3621?logo=databricks)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Governed-00A972)
![Serverless](https://img.shields.io/badge/Serverless-Compute-1B3139)
![Feature Views](https://img.shields.io/badge/Feature%20Views-Declarative-FF3621)
![Latency](https://img.shields.io/badge/E2E%20Serving-p95%20~32ms%20(measured)-00A972)

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
| **Approach** | Batch features only (no streaming) | Rank-all: score every offer directly |
| **Headline** | feature-read + rank **p50 ≈ 15ms in-region** | streaming in-session feature feeding the same ranker |
| **Notebooks** | `notebooks/part1_feature_views/` (00–06) | `notebooks/part2_realtime_two_stage/` (07–10) |
| **Job** | `resources/part1_job.yml` | `resources/part2_job.yml` (assumes Part 1 has run) |
| **Status** | ✅ runs end-to-end | ⚠️ **streaming online serving is gated** (see note below) |

Both parts share one catalog and one set of feature definitions. Part 2 re-logs the ranker with the
streaming `cust_clicks_10m` feature added — same online-lookup pattern, one more feature.

> **⚠️ Part 2 status (updated 2026-07-31, measured on FEVM).** Two of the three streaming gates are
> now cleared; the third is a confirmed platform gap awaiting the DBR-19 fix.
> - ✅ **Streaming online *materialization* works.** With **hyphen-free online identifiers** (store
>   `nbo`, table prefix `nbostream`) the `StreamingMode()` sink runs clean — the `lakebase_sink`
>   (postgresql) pipeline shows no quoted-identifier validation error and the online table
>   `cust_clicks_10m` populates. This clears the pre-DBR-19 quoted-`"schema"` rejection that
>   previously left the online table at 0 rows.
> - ✅ **Continuous streaming + freshness measured.** A serverless-safe producer (repeated bounded
>   bursts — serverless rejects an infinite `writeStream` trigger) feeds the topic continuously.
>   Measured steady-state **event→online freshness ≈ p50 ~110ms / p95 ~150–210ms / p99 ~160ms**
>   (warm pipeline, low-rate; `commit_time − event_time` in the online rows over a tight recent
>   window); the RollingWindow `cust_clicks_10m` computes live. (A heavy 3k-events/s flood pushes p95
>   into the seconds as the pipeline works through backlog — that's throughput-under-load, not
>   steady-state freshness.)
> - ⛔ **Streaming online *serving* is still gated (root cause pinned).** The streaming sink writes a
>   Postgres online table but does **not** register it as a UC *synced table* (no `source_table` /
>   `source_table_id` — batch online tables have these). Model Serving's feature-lookup engine uses
>   that synced-table registration to obtain an OAuth token; without it, the streaming table falls
>   back to password auth and fails (`KeyError: 'OAUTH_TOKEN'` → `password authentication failed`),
>   so the ranker re-logged with the streaming feature can't deploy. This is the platform-side half of
>   the DBR-19 fix, not a config error.
>
> **Part 1 is the fully-working, fully-served path** (endpoint live, **p95 ≈ 32ms measured**). Part 2
> proves the streaming feature reaches the online store and is fresh; only live *serving* of it awaits
> DBR 19. Notebook 08 still stops early unless you opt in (`allow_streaming_online=true`).

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
endpoint — no train/serve skew.*

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
  catalog you score every offer directly.
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
| 05 | `05_deploy_ranking_endpoint` | Route-optimized **online-lookup** ranker |
| 06 | `06_latency_benchmark` | Feature-read + rank latency |

---

## Part 2 — Real-Time Streaming Recommender

*Capture the customer's freshest in-session intent — a streaming feature feeding the same ranker,
scored in real time. Personalization lives entirely in the ranker.*

```
in-session events ─► MSK (msk_kafka) ─► RollingWindow streaming FV (cust_clicks_10m) ─► online store
                                                                                          │
Part 1 batch online features (5) ─────────────────────────────────────────────────────────┤ (6 online features)
                                                                                          ▼
   request {customer_id, offer_id + offer attrs}   (all offers — rank-all)
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
- **Rank-all:** for the ~40-offer catalog we score every offer directly, so there's no candidate
  narrowing step to add latency.
- **Two numbers, reported separately:** serving **feature-read + rank** (≈15ms in-region) *and*
  streaming freshness **event → online availability**. The freshness number requires the gated
  streaming online path (see the Part 2 status note above); serving latency is measured today in Part 1.

**Notebooks** (`notebooks/part2_realtime_two_stage/`):

| # | Notebook | Purpose |
|---|---|---|
| 07 | `07_kafka_topic_and_producer` | Seed the MSK topic + synthetic in-session event producer |
| 08 | `08_streaming_feature_views` | `RollingWindow` streaming FV (`cust_clicks_10m`) via `StreamingMode` |
| 09 | `09_realtime_serving` | Re-log ranker with the streaming feature → route-optimized rank-all endpoint |
| 10 | `10_latency_and_freshness` | Live serving latency + event→online freshness benchmark |

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

1. Clone this repo and configure a Databricks CLI profile for your workspace
   (`databricks auth login --host https://<your-workspace>.cloud.databricks.com -p <profile>`).
2. Deploy the bundle, passing your workspace-specific values as variables (nothing
   workspace-specific is committed — the host comes from your profile):
   ```bash
   databricks bundle deploy -t dev -p <profile> --var warehouse_id=<sql-warehouse-id>
   ```
   Override any variable with repeated `--var` flags (`--var catalog=… --var schema=…`).
   For convenience, copy the git-ignored **`deploy.local.sh`** template, fill in your profile +
   warehouse id, and run `./deploy.local.sh dev`.
3. Run **`[NBO] Part 1`** end-to-end first. Then run **`[NBO] Part 2`** (it reuses Part 1's catalog,
   features, online store, and ranker). Part 2 streaming online serving is gated — see the status
   note above.
4. Launch the recommender app and open the latency dashboard. Grant the app's service principal
   `SELECT` on the catalog and `Can Query` on `nbo-ranker-online`.

### Configurable variables (`--var name=value`)

| Variable | Default | Purpose |
|---|---|---|
| `warehouse_id` | *(required)* | SQL warehouse for the app + dashboard |
| `catalog` | `fins_industry_solutions` | UC catalog (standard storage) |
| `schema` | `nbo` | Schema for all assets |
| `online_store_name` | `nbo` | Lakebase online store (single word — hyphen-free target for the streaming sink) |
| `kafka_connection` | `msk_kafka` | UC Kafka connection (Part 2) |
| `service_credential` | `msk_kafka` | UC service credential for MSK IAM (Part 2) |
| `kafka_topic` | `nbo-session-events` | In-session events topic (Part 2) |

### Prerequisites

- DBR **17.0 ML** or later; `databricks-feature-engineering >= 0.16.0`; **serverless (latest env)**
- **Lakebase** online store, **Model Serving** (Part 1); a **Kafka/MSK** UC connection (Part 2, DBR 19+ for streaming online)
- A Unity Catalog catalog on **standard storage** (streaming Feature Views cannot use default storage)

## Contributing

Validate changes with `databricks bundle validate` before submitting. Issues and pull requests
are welcome.

## License

&copy; 2026 Databricks, Inc. All rights reserved. Provided under the Databricks License.
See `LICENSE.md`. Third-party dependencies and their licenses are documented in `NOTICE.md`.
