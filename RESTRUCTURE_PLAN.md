# Restructure Plan — Two-Part Accelerator (for review)

**Owners:** Sixuan, Tian · **Status:** Proposed — review before implementation · **Date:** 2026-07-24

## Why restructure

Today the accelerator is one linear flow that mixes the **core Feature Views value** (author
once → train + serve, sub-300ms rank) with an **advanced two-stage retrieval** pattern (Vector
Search). That buries the fastest, most broadly-applicable win under machinery most first-time
users don't need on day one.

**Proposal:** split into two self-contained parts.

- **Part 1 — Feature Views Fundamentals** (the fast win): prove *author-once, serve-everywhere*
  and a sub-50ms feature-serving + ranking path. **No Vector Search, no streaming.** This is the
  90% use case and the fastest path to "it works."
- **Part 2 — Real-Time Two-Stage Recommender** (the real-world scale story): add **streaming
  in-session features** (MSK) and **Vector Search retrieval** to handle a large candidate catalog
  and freshest in-session intent. Builds directly on Part 1's feature views + ranker.

Each part runs end-to-end on its own, has its own README section, benchmark, and deploy target.
Part 2 assumes Part 1 has run (reuses the same catalog, features, and ranker).

---

## Target structure

```
notebooks/
  part1_feature_views/
    00_setup.py                     # catalog/schema, online store
    01_generate_data.py             # customers, offers, transactions, labels (batch only)
    02_define_feature_views.py      # batch FVs: Sliding / Tumbling / ColumnSelection
    03_materialize_features.py      # offline Delta + online Lakebase
    04_train_ranker.py              # point-in-time training set → LightGBM  ← "author once"
    05_deploy_ranking_endpoint.py   # route-optimized serving; rank a candidate set (NO retrieval)
    06_latency_benchmark.py         # feature read + rank; targets ~10ms + ~30ms reference

  part2_realtime_two_stage/
    07_kafka_topic_and_producer.py  # in-session events → MSK (msk_kafka)
    08_streaming_feature_views.py   # RollingWindow streaming FVs (freshest in-session intent)
    09_vector_search_index.py       # offer embeddings → VS index
    10_two_stage_serving.py         # Stage 1 retrieve (VS) → Stage 2 rank (Part 1 endpoint)
    11_latency_and_freshness.py     # serving p50/p95 + event→online freshness (~200ms)

apps/recommender-app/               # Part 2 demo (two-stage UI + live latency meter)
dashboards/nbo_dashboard.json       # spans both parts (latency, freshness, offer quality)
resources/                          # bundle: part1 job, part2 job, app, dashboard
```

Nothing is deleted — existing notebooks are **renumbered/moved**, and content that already runs
end-to-end is reused. New work is only in the two new Part 2 notebooks (08 streaming FVs wired to
a real RollingWindow feature, and the split of serving into 05 rank-only + 10 two-stage).

---

## Part 1 — Feature Views Fundamentals

**Story:** "Define a feature once. It powers point-in-time-correct training *and* a sub-50ms online
ranking endpoint — no train/serve skew, no retrieval machinery."

**Scope**
- Batch feature views only: `SlidingWindow` (30d avg balance, 7d txn count), `TumblingWindow`
  (90d spend), `ColumnSelection` (loyalty tier, risk band).
- Materialize to offline Delta (training) + online Lakebase (serving).
- Train ranker on the point-in-time training set → register `@prod`.
- Deploy a **route-optimized** ranking endpoint that scores a **candidate set passed in the
  request** (for a small NBO catalog you score all/eligibility-filtered offers — no retrieval).
- Benchmark **feature online-read + rank**. This is the path that maps to the ~10ms feature-serving
  + ~30ms model-serving reference.

**Headline metric:** online feature-read + rank **p50 well under 50ms in-region**.

**Design decision to confirm (⚠️ real dependency):** Part 1's cleanest form uses **online feature
lookup** — either `fe.log_model(training_set=...)` (endpoint auto-fetches features) or a **Feature
Serving endpoint**. That needs the online store populated. We hit a gap where **CronSchedule
aggregation online tables didn't backfill on demand** (only ColumnSelection/TableTrigger did). Two
options for Part 1:
- **(A) Fix the online path** — resolve the CronSchedule backfill (worth an ES ticket) so the
  endpoint does true online lookup. Best demonstration of Feature Views' online value.
- **(B) Fallback** — pass aggregate features in the request (as today), use online lookup only for
  the ColumnSelection features that do materialize. Works now; slightly less "pure."
Recommend targeting **(A)** for Part 1 since online serving *is* the point; keep **(B)** as the
documented fallback.

---

## Part 2 — Real-Time Two-Stage Recommender

**Story:** "Scale to a real catalog and capture the customer's freshest in-session intent —
streaming features + Vector Search retrieval feeding the same ranker."

**Scope**
- **Streaming in-session features:** Kafka topic + synthetic producer (`msk_kafka`), register the
  stream, define a `RollingWindow` feature (e.g. clicks / category dwell in last 10 min). This
  lights up the blog's differentiator and the freshness number.
- **Vector Search retrieval:** embed offers, build the index, retrieve top-K candidates by
  semantic match to in-session intent — the piece that makes ranking scale past a tiny catalog.
- **Two-stage serving:** Stage 1 VS retrieve → Stage 2 rank (reuses Part 1's route-optimized
  endpoint, now also fed the streaming features).
- **Benchmarks:** serving p50/p95/p99 (the <300ms end-to-end story) **and** event→online-
  availability freshness (~200ms p99, the launch figure) — reported separately and honestly.
- **Demo app + dashboard** live here.

**Headline metrics:** end-to-end two-stage serving **p95 < 300ms**; streaming freshness **~200ms p99**.

**Why Vector Search here (and not Part 1):** for a small NBO catalog you can score every offer
directly (Part 1). Retrieval earns its place only when the candidate pool is large (thousands of
products / content / eligibility-scoped offers) — then ANN narrows N→K under budget. Part 2 shows
the pattern at the small demo scale but frames it as "the architecture you use at catalog scale."

---

## Migration steps (once approved)

1. Create `notebooks/part1_feature_views/` and `part2_realtime_two_stage/`; move + renumber
   existing notebooks (00–04 unchanged; today's 05 splits into P1 `05_deploy_ranking_endpoint`
   rank-only and P2 `10_two_stage_serving`).
2. Strip Vector Search out of Part 1's serving/benchmark notebooks.
3. Author P2 `08_streaming_feature_views.py` (RollingWindow over the MSK stream) — the one genuinely
   new notebook; run it end-to-end.
4. Split the Asset Bundle into a **Part 1 job** and a **Part 2 job** (P2 depends on P1 objects).
5. Rewrite `README.md` as **Part 1 → Part 2**, each with its own architecture diagram, run steps,
   and honest latency table.
6. Update the app README to note it's the Part 2 capstone.

## Open questions for review

1. **Part 1 online lookup — target (A) fix the CronSchedule backfill, or ship (B) fallback first?**
   (Determines how "pure" the Part 1 online-serving story is.)
2. **Folder layout vs flat numbering** — two subfolders (proposed) vs one flat `00–11` sequence.
   Subfolders read better for a two-part narrative; flat is simpler for Asset Bundle globs.
3. **Should Part 1 also ship a tiny demo** (a minimal "score these offers" call + latency readout),
   or keep the app entirely in Part 2? Leaning: Part 1 stays notebook-only; app is the Part 2 payoff.
4. **Streaming feature to model (08)** — start with one RollingWindow feature (clicks_10m) for
   simplicity, or model 2–3 in-session signals (dwell, category affinity)?
5. **Repo split vs one repo** — keep both parts in one repo with two folders (proposed), or ever
   split into two industry-solutions submissions? Recommend one repo, two parts.
