# NBO Accelerator v2: plan

Mock: `mockups/nbo-v2/index.html`. Open it in a browser; no build or backend needed. Every number in it is simulated.

## Findings behind the plan (verified against `nbo_tian_tan` on fe-vm-ttan-vm)

1. **Personal loan always wins.** The label logit in `01_generate_data.py` is `customer_terms + offer_terms`. The offer term (`base_reward*0.6 - tier_requirement*0.3`) never interacts with the customer, so every customer gets the same offer order. The best single offer by that term is a personal_loan (0.264, vs 0.244 for the best investment offer).
2. **The ranker can't see most of the signal.** `04_train_ranker.py` uses `offer_id`, tier, risk, and three transaction aggregates. Income and tenure drive the labels but are not features. Offer attributes (category, reward, tier requirement) are not features either.
3. **Offer text doesn't match category.** `offer_text` and `product_category` come from independent hashes, so some `credit_card` offers read "High-Yield Savings Account" or "30-Year Fixed Mortgage". That shows on screen in the app.

## Workstreams (in run order)

| # | Workstream | PM item | Effort |
|---|---|---|---|
| A | Data + model: real personalization | #2, #5 | 1 day, plus about 2 h pipeline re-run |
| E | Sawtooth + CustomUDF features | #4 | 1 day of work, plus about 2 days of wall-clock warm-up |
| B | Lakeshore Bank storefront + persona controls | #1 (site + sliders) | 2 to 3 days |
| D | How it works explainer | #3 | 0.5 to 1 day |
| C | Scale dashboard | #1 (scale) | 2 days |

A goes first because every UI change depends on rankings that move. E starts as soon as MSK is back, since Sawtooth needs a couple of days before it serves.

### A. Data + model

- `01_generate_data.py`
  - Replace the additive logit with customer x offer interactions:
    - tier x category affinity (Bronze toward starter card and savings, Platinum toward investment and premium cards),
    - risk gating on credit products,
    - income fit for mortgage and investment,
    - tenure fit,
    - an eligibility penalty below `tier_requirement`.
  - The mock's `scoreInteraction()` is the working spec for the coefficients.
  - Add a session-intent term: count `session_events` for the same customer and category in the 10 minutes before the label `ts`. This makes the streaming features carry real signal.
  - Derive `offer_text` from `product_category` and add an offer `kind` (starter, mid, premium).
- `02_define_feature_views.py`
  - Add ColumnSelection features for `annual_income` and `tenure_months`.
  - Add offer-keyed ColumnSelection features (`product_category`, `base_reward`, `tier_requirement`). This also shows a second lookup entity.
- `04_train_ranker.py`: add the new features to the training set.
- Acceptance checks, as a notebook cell that asserts each one:
  - val AUC of at least 0.75,
  - across 10k customers, no category above 40% of top-1 picks,
  - the 4 demo personas get 4 different top-1 offers.

### E. Sawtooth + CustomUDF (both Beta, `databricks-feature-engineering` 0.18.x)

- **SawtoothWindow** (`from databricks.feature_engineering.entities import SawtoothWindow`)
  - Feature: `cust_product_views_30d` on the Kafka `StreamSource` in nb07.
  - Constraints:
    - StreamSource only.
    - `window_duration` must be over 2 days.
    - The feature serves about 2 days after materialization, and the ingestion table must cover the full window.
    - Serving needs `databricks-feature-lookup` 1.16.0 if `Last` is used.
    - Feature names must stay under 63 bytes.
- **CustomUDF**
  - `cust_spend_to_income` uses `FeatureViewSource([spend_90d, annual_income])` and a UC SQL/Python function.
  - `cust_intent_score` is computed over the streaming intent features.
  - Both are computed on demand at serving, not materialized.
  - Needs `EXECUTE` on the function, plus the UDF's packages in `extra_pip_requirements` on `fe.log_model`.
- Story for the deck: Sawtooth gives a long window on a stream that stays fresh at the leading edge, with about 2 days of live state instead of 30. CustomUDF gives derived features with no serving-side code.

### B. Lakeshore Bank site (new routes in `apps/recommender-ui`)

There are no demo controls. The presenter acts as a real visitor, and every input is something a real customer would do on the site.

- **Sign in.** A "Choose an account" modal lists real `customer_id`s with the matching archetypes, plus "Continue as guest". Signing in drives the online lookup of stored features (tier, tenure, balances).
- **OfferMatch (landing page, CardMatch-style).**
  - A sidebar of questions: goals, self-rated credit, income, monthly card spend. These feed request-time features (RequestSource). For signed-in users they are prefilled from the profile.
  - A "Find my offers" button leads to a results page: the top match with a match %, why-you-matched chips, and key terms, then a ranked grid with category filters.
- **Product pages** (Cards, Savings, Personal Loans, Mortgages, Investing).
  - Each has an editorial compare list, a loan calculator, and a "Recommended for you" rail.
  - Page views, "Learn more", "Apply", and calculator use emit session events to Kafka, which update the streaming features.
- **Real-time feedback.**
  - A "For you" pill in the header always shows the current #1 and pulses when it changes.
  - A toast explains the change ("New top match: X, because you used the loan calculator").
  - Reordered rows flash.
- **Databricks layer.** A presenter toggle outside the site annotates each element with what it reads from or writes to. It also shows a live trace: event -> feature updated -> re-ranked in N ms.
- Keep the current Recommender view as the "banker console" route.

### D. How it works

- Replace `ArchitectureView.tsx` with the 5-step guided walkthrough from the mock: define, stream, train, serve, observe.
- Each step highlights the active components and shows the real code excerpt from the notebooks.
- Step 4 shows a request waterfall with measured timings.

### C. Scale dashboard (in-app route)

Pattern from `databricks-field-eng/realtime-apps-feature-demo` and the RTM SME group's demos (Flight Tracker, Capital Markets):

- **Backend.** The app server holds a pooled Lakebase connection. The pg8000 recipe already works against the online tables. The UI polls a tiny JSON endpoint every 250 to 1000 ms.
- **Freshness.** Use `now - event_time` per newly seen key, one sample per event, and show p50 and p95. This matches our existing per-event freshness method.
- **Latency.** Show a "Ping e2e" button that does a real recommend call. Add a load-generator job, reusing `notebooks/benchmark/end_to_end_inference_benchmark.py`, that drives QPS. Read p50/p99 and QPS from endpoint metrics or the inference table.
- **Tiles.** Recommendations per second, features served per second, e2e p99, lookup p50, Kafka events per second, and freshness p95. Add a feature-view table (window type, mode, freshness) and the top-1 offer mix (personalization proof).

## Decisions needed

1. **Sliders vs. online lookup.** The endpoint looks features up by `customer_id`, so arbitrary slider values need one of two approaches:
   - (a) Snap each slider change to the nearest real customer. This is always honest, but values jump.
   - (b) Pass the slider values in the request as feature overrides. This is smooth, but the support for overriding looked-up features on this endpoint needs to be verified first.
   - Recommendation: (a) for presets and (b) for sliders, if (b) checks out.
2. **Click events from the site.**
   - (a) Produce to MSK straight from the app. This is the real Kafka path, but egress and IAM auth from Databricks Apps are unverified.
   - (b) Use Zerobus into a Delta table plus a streaming Delta source. This is simpler, but Sawtooth requires a StreamSource, so Sawtooth would still need a Kafka feed (the nb08 replay).
3. **Brand.** "Lakeshore Bank" is a placeholder.
4. **Today vs. Proposed toggle.** This exists for the PM review only and does not ship in the customer-facing app.

## Not verified yet

- Request-time override of looked-up features on the route-optimized endpoint (decision 1b).
- Kafka produce from a Databricks App to MSK (decision 2a).
- Sawtooth warm-up behavior with the nb08 historical replay. The ingestion table must cover 30 days before the feature serves.
- The interaction coefficients reaching AUC 0.75 and the 40% mix cap. They are tuned only in the mock's JS so far.
