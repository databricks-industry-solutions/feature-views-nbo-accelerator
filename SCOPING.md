# Scoping & Plan — Real-Time NBO with Feature Views

**Owners:** Sixuan, Tian · **Status:** Scoping · **Date:** 2026-07-17

## Goal
Ship a Databricks industry-solution accelerator that demonstrates a real-world, end-to-end
**personalized recommendation** pipeline built on **Feature Views**, with a measured
**sub-300ms** online serving path.

## Decisions locked (2026-07-17)
| Fork | Decision |
|---|---|
| Domain | **Financial services** — retail-banking Next-Best-Offer |
| Data | **Fully synthetic** generator (reproducible, no licensing) |
| Serving | **Full two-stage** (Vector Search retrieval + ranking) **+ Databricks App** |
| First step | **Scaffold the repo** (this structure) |

## Why this proves the Feature Views story
- One declarative feature set powers **both** offline training (point-in-time, no skew) and
  online serving — the "author once, serve everywhere" claim.
- Exercises all FV modes: `SlidingWindow`, `TumblingWindow`, `RollingWindow` (streaming),
  `ColumnSelection`.
- Captures **freshest in-session intent** (streaming clickstream features) — the headline
  differentiator from the launch blog.

## Latency: two numbers, kept honest
1. **Freshness** — event → online availability p99 (~200ms; the launch figure).
2. **Serving** — request → ranked response, target **<300ms** (retrieval + online read +
   rank + orchestration). Measured via load test in notebook 06; **this is the make-or-break.**

## Phased plan
1. **Foundation** — workspace (FEVM), synthetic data, catalog/schema, online store.
2. **Feature Views core** — define + materialize all modes; validate point-in-time training set.
3. **Model** — two-stage recommender (offer embeddings + LightGBM ranker), UC-registered, served.
4. **Real-time + latency proof** — streaming features wired, app built, benchmark tuned to <300ms.
5. **Package** — dashboards, README, Asset Bundle, blueprint polish.

## Prerequisites / risks
- DBR 17.0 ML+, `databricks-feature-engineering>=0.16.0`.
- Lakebase online store, Vector Search, Model Serving enabled; **standard-storage** catalog.
- Kafka + UC connection for streaming features (or synthetic producer in `scripts/`).
- **Preview access** — confirm Feature Views GA/preview status in the target workspace.
- **Beta FV migration** — must use `create_feature()`/`register_feature()`; Beta FVs expire 2026-07-22.
- Latency risk — scale-to-zero cold starts; keep endpoints warm for the demo.

## Open questions for Sixuan + Tian
- Target workspace: existing or new FEVM deployment? Which region/ring has FV enabled?
- Scale of synthetic data for a credible-but-cheap demo (current defaults: 100K customers)?
- Is a Kafka source available, or do we ship the synthetic streaming producer as the default?
- Timeline / target milestone (internal demo, then industry-solutions repo submission)?

## Suggested next steps (post-scaffold)
1. Provision the target workspace with FV/Lakebase/VS/Model Serving.
2. Implement notebook 01 (synthetic data) — unblocks everything downstream.
3. Implement + validate notebook 02–03 (the FV core) end-to-end on real infra.
4. Spike the latency benchmark early (notebook 06) to de-risk the <300ms claim.
