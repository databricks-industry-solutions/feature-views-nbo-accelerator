# Real-Time Next-Best-Offer with Databricks Feature Views

![Solution Accelerator](https://img.shields.io/badge/Solution-Accelerator-FF3621?logo=databricks)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Governed-00A972)
![Serverless](https://img.shields.io/badge/Serverless-Compute-1B3139)
![Feature Views](https://img.shields.io/badge/Feature%20Views-Declarative-FF3621)
![Serving](https://img.shields.io/badge/Feature--read%20%2B%20rank-p95%20~41ms%20(measured)-00A972)
![Freshness](https://img.shields.io/badge/Kafka→online%20freshness-p95%20~135ms%20(measured)-00A972)

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
| **Headline** | feature-read + rank **p50 ~33ms / p95 ~41ms** (incl WAN) | streaming feature serves live; **event→online p50 ~104ms / p95 ~135ms** |
| **Notebooks** | `notebooks/part1_feature_views/` (00–05) | `notebooks/part2_realtime_two_stage/` (07–09) |
| **Job** | `resources/part1_job.yml` | `resources/part2_job.yml` (assumes Part 1 has run) |
| **Status** | ✅ runs end-to-end | ✅ runs end-to-end (streaming feature serves live) |

Both parts share one catalog, **one schema, and one online store**, plus one set of feature
definitions. Part 2 re-logs the ranker with the streaming `cust_clicks_10m` feature added — same
online-lookup pattern, one more feature.

> **✅ Part 2 status (updated 2026-08-04, measured end-to-end on FEVM).** The streaming path works
> from Kafka event through to a live ranking that reflects in-session intent.
> - ✅ **Streaming online materialization.** With simple online identifiers (store `nbo`, table prefix
>   `nbostream`) the `StreamingMode()` sink runs clean and the online table populates. Keep online
>   store/table names plain — decorated names can make the sink quote its Postgres target and fail.
> - ✅ **Single online store + single schema (required).** Batch features (nb03) and the streaming
>   feature (nb07) **must** materialize into the **same** online store (`online_store_name=nbo`) and
>   the **same** schema. New Lakebase Autoscaling online stores do **not** support a served model
>   looking up features across multiple online stores — an endpoint whose batch features live in one
>   store and streaming feature in another fails to provision the serving role/OAuth token. Keeping
>   everything in one store is what makes the re-logged ranker deploy. (This was the real cause of the
>   earlier serving failure, not a synced-table-registration gap.)
> - ✅ **Streaming feature serves live.** The re-logged `nbo-ranker-realtime` (route-optimized) deploys
>   and looks up the live `cust_clicks_10m` online: identical offers score differently for a customer
>   with fresh in-session clicks vs none.
> - ✅ **Freshness measured.** Steady-state **event→online freshness ≈ p50 104ms / p95 135ms / p99
>   157ms** (min ~61ms; low rate ~25 ev/s, warm pipeline; `commit_time − event_time` over ~22k dedup'd
>   samples, excluding null-event window-expiry and timer rows). A heavy flood (e.g. 2000+ ev/s) pushes
>   this into seconds as the RollingWindow pipeline works through backlog — that's throughput-under-load,
>   not steady-state freshness.
> - ✅ **Serving latency measured.** Feature-read + rank-all **≈ p50 33ms / p95 41ms / p99 49ms**
>   (interactive, incl. cross-region WAN; in-region lower). Measured **warm** — see the cold-start note
>   below.
>
> **Endpoints scale to zero by default.** Both ranking endpoints are created with
> `scale_to_zero_enabled=True`, so a cloned accelerator costs nothing while idle. The trade-off: after
> an idle period the endpoint scales to zero and the next request pays a **cold start** (tens of
> seconds). The latency numbers above are warm-path numbers — send a few throwaway requests before
> benchmarking or demoing. If you need guaranteed-warm latency for a live demo, flip
> `scale_to_zero_enabled=False` in notebook 05 / 09 for its duration, and remember the endpoint then
> bills continuously until you delete it.
>
> **Querying a route-optimized endpoint** (the feature-serving & feature-freshness benchmarks): these endpoints accept **only** an OAuth token
> downscoped to the endpoint, which the notebook runtime identity cannot mint (raises `OAuth tokens are
> not available for runtime authentication`; PATs unsupported). the feature-serving & feature-freshness benchmarks therefore query via a **service
> principal** (`client_credentials` + `authorization_details`) whose id/secret live in the **`nbo` secret
> scope**, and they must run on **serverless** (only serverless egress is on the workspace IP access
> list) — see **Prerequisites** and
> [Query route-optimized endpoints](https://docs.databricks.com/aws/en/machine-learning/model-serving/query-route-optimization).
> Notebook 07 still stops early unless you opt in (`allow_streaming_online=true`).

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
- **Latency (measured):** online feature-read + rank **p50 ~33ms / p95 ~41ms / p99 ~49ms**
  (route-optimized endpoint, **warm**; measured interactively incl. cross-region WAN RTT, so
  in-region is lower) — squarely in the personalization reference band (~10ms feature-read + ~30ms
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

Latency benchmarks live in `notebooks/benchmark/` (see the Benchmarks section below).

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
- **Two numbers, reported separately:** serving **feature-read + rank** (p50 ~33ms / p95 ~41ms incl
  WAN) *and* streaming freshness **event → online availability** (p50 ~104ms / p95 ~135ms steady-state).
  Both are measured; see the Part 2 status note above.

**Notebooks** (`notebooks/part2_realtime_two_stage/`):

| # | Notebook | Purpose |
|---|---|---|
| 07 | `07_streaming_feature_views` | `RollingWindow` streaming FV (`cust_clicks_10m`) via `StreamingMode` |
| 08 | `08_kafka_topic_and_producer` | Seed the MSK topic + synthetic in-session event producer |
| 09 | `09_realtime_serving` | Re-log ranker with the streaming feature → route-optimized rank-all endpoint |

*Run order: the streaming FV / ingestion pipeline (07) must exist and be RUNNING before the
producer (08) emits events, so 07 precedes 08.*

---

## Benchmarks

*Latency benchmarks for both parts, consolidated in `notebooks/benchmark/`.*

| Notebook | Purpose |
|---|---|
| `feature_serving_benchmark` | Feature-read + rank latency (Part 1 online-lookup path) |
| `end_to_end_inference_benchmark` | Decomposed feature lookup vs. model inference vs. total (real Lakebase reads) |
| `feature_freshness_benchmark` | Event→online freshness + live serving latency (Part 2 streaming path) |

---

## What's inside

| Path | Contents |
|---|---|
| `notebooks/part1_feature_views/` | Part 1 (00–05): batch feature views → train → online-lookup ranking |
| `notebooks/part2_realtime_two_stage/` | Part 2 (07–09): streaming FV + real-time rank-all serving |
| `notebooks/benchmark/` | Latency benchmarks (feature serving, end-to-end inference, feature freshness) |
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
3. Run **`[NBO] Part 1`** end-to-end first, then **`[NBO] Part 2`** (it reuses Part 1's catalog,
   features, online store, and ranker). By default each notebook writes to a **per-user schema**
   `nbo_<username>` (auto-derived); pass `--var schema=<name>` (or the `schema` widget) to pin a
   shared one. The benchmark notebooks (06/10) need the `nbo` service-principal secret scope and must
   run on serverless — see **Prerequisites**. Part 2 streaming online serving is gated — see the
   status note above.
4. Launch the recommender app and open the latency dashboard. Set the app's `SCHEMA` env var to the
   schema you deployed into, and grant the app's service principal `SELECT` on the catalog and
   `Can Query` on `nbo-ranker-online`.

### Configurable variables (`--var name=value`)

| Variable | Default | Purpose |
|---|---|---|
| `warehouse_id` | *(required)* | SQL warehouse for the app + dashboard |
| `catalog` | `fins_industry_solutions` | UC catalog (standard storage; assumed to exist) |
| `schema` | *blank → auto* | Blank auto-derives a per-user `nbo_<username>` in every notebook. Set `--var schema=<name>` to pin one shared schema. |
| `online_store_name` | `nbo` | Lakebase online store — keep it a simple single word (the streaming sink is sensitive to decorated names). **Batch + streaming features must share this one store**; multi-store lookup is unsupported on new Lakebase stores and breaks route-optimized serving. |
| `kafka_connection` | `msk_kafka` | UC Kafka connection (Part 2) |
| `service_credential` | `msk_kafka` | UC service credential for MSK IAM (Part 2) |
| `kafka_topic` | `nbo-session-events` | In-session events topic (Part 2) |

### Prerequisites

This is a complete, customer-facing checklist. Several items require a **workspace/metastore admin**
or the customer's **AWS/MSK admin** — line them up before you start. "You" = the person running the
notebooks.

**Compute**
- **Serverless** enabled (jobs and notebooks run on serverless; no clusters are defined).
- Runtime equivalent to **DBR 17.0 ML+**; **DBR 19+** for Part 2 streaming-online materialization.
- `databricks-feature-engineering >= 0.16.0` (installed per-notebook).
- Benchmark notebooks **06** and **10 must run on serverless** (see the IP-access-list item below).

**Unity Catalog (admin)**
- A catalog on **standard storage** — streaming Feature Views **cannot** use Default Storage.
- Most users **cannot `CREATE CATALOG`**. Have your **metastore admin** pre-create the catalog and
  grant you `USE CATALOG` + `CREATE SCHEMA`. `00_setup` **assumes the catalog exists** by default;
  set the `create_catalog` widget to `true` only if you personally hold `CREATE CATALOG`.
- Data lands in a **per-user schema** `nbo_<username>` (auto-derived) — you need `CREATE SCHEMA` on
  the catalog. Override with `--var schema=` / the `schema` widget to share one schema.

**Lakebase (admin)**
- **Lakebase online store** enabled and privilege to create one (`00_setup` creates store `nbo`).
- Batch (Part 1) and streaming (Part 2) features **must share the same online store** — multi-store
  lookup is unsupported on new Lakebase stores and breaks route-optimized serving.

**Model Serving**
- Model Serving enabled; privilege to create **route-optimized** endpoints
  (`nbo-ranker-online`, `nbo-ranker-realtime`).

**Kafka / MSK — Part 2 only (metastore admin + customer AWS/MSK admin)**
- A **Unity Catalog Kafka connection** (default `msk_kafka`) holding the MSK bootstrap servers, and a
  **UC service credential** (default `msk_kafka`) for **MSK IAM** auth — created by a metastore admin
  with `CREATE CONNECTION`.
- The customer's **AWS/MSK admin** must: allow the service credential's IAM role to **describe/create/
  read/write** the topic (default `nbo-session-events`), and provide **network reachability from
  serverless to MSK** (PrivateLink or NAT + security-group rules). Either grant topic auto-create or
  pre-create the topic.

**Service principal for route-optimized queries — the feature-serving & feature-freshness benchmarks (admin)**
- Route-optimized endpoints accept **only** an OAuth token **downscoped to the endpoint**; the notebook
  runtime identity cannot mint one. the feature-serving & feature-freshness benchmarks therefore query via a **service principal**.
- Create/identify an SP with an **OAuth client id + secret**, grant it **CAN_QUERY** on both endpoints,
  and store its credentials in a secret scope named **`nbo`**:
  ```bash
  databricks secrets create-scope nbo
  databricks secrets put-secret  nbo sp_client_id       # SP application/client id
  databricks secrets put-secret  nbo sp_client_secret   # SP OAuth secret
  ```
- The workspace enforces an **IP access list**; only **serverless egress** is allowlisted, so run
  the feature-serving & feature-freshness benchmarks on serverless (a classic cluster gets a **403** on the token request).

**SQL warehouse + app grants**
- A **SQL warehouse** for the app + dashboard (`--var warehouse_id=<id>`).
- The **app's service principal** needs `SELECT` on the catalog and **CAN_QUERY** on `nbo-ranker-online`.

**Reproducibility**
- Synthetic data (nb01) is fully **row-deterministic** (hashed off the row id), so it regenerates
  identically on any compute shape / serverless autoscale.

## Contributing

Validate changes with `databricks bundle validate` before submitting. Issues and pull requests
are welcome.

## License

&copy; 2026 Databricks, Inc. All rights reserved. Provided under the Databricks License.
See `LICENSE.md`. Third-party dependencies and their licenses are documented in `NOTICE.md`.
