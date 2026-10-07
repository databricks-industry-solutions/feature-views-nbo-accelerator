# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate Synthetic FSI Data
# MAGIC Fully synthetic, reproducible retail-banking dataset — no external licensing.
# MAGIC All randomness is a deterministic hash of the row `id`, so re-runs produce the **exact same
# MAGIC dataset on any cluster shape / serverless autoscale** (unlike Spark's partition-dependent
# MAGIC `rand(seed)`).
# MAGIC
# MAGIC Produces five Delta tables in `${catalog}.${schema}`:
# MAGIC - `customers`      — demographics, tenure, loyalty tier, risk band (latest-attribute source)
# MAGIC - `offers`         — NBO catalog (cards, savings, loans) + text for embeddings
# MAGIC - `transactions`   — batch spend/balance history for windowed features
# MAGIC - `session_events` — in-session clickstream for streaming features (event-time column)
# MAGIC - `labels`         — historical offer acceptances w/ timestamps (point-in-time training)
# MAGIC
# MAGIC **Point-in-time correctness:** transaction/event history spans Jan–Jun 2025; label
# MAGIC timestamps fall in May–Jun 2025 so every feature lookup has history behind it.
# MAGIC **Label columns must NOT exist in any feature source table** (they don't here).

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
spark.sql(f"USE `{catalog}`.{schema}")

# COMMAND ----------
# MAGIC %md ## Scale knobs
# COMMAND ----------
N_CUSTOMERS = 100_000
N_OFFERS = 40
N_TRANSACTIONS = 5_000_000
N_SESSION_EVENTS = 20_000_000
N_LABELS = 400_000

# Deterministic per-row uniform in [0,1): a stable hash of the row `id` + a per-column salt.
# Unlike Spark's rand(seed) (partition-dependent), this reproduces the EXACT same dataset on
# any cluster shape / serverless autoscale. Distinct salts keep columns decorrelated.
# CAST AS DOUBLE is load-bearing: `bigint / 1000000000.0` is DECIMAL division in Spark, so every
# column derived from u() (amounts, balances, base_reward, ...) would inherit a decimal type.
# Decimal breaks two downstream surfaces: MLflow model signatures reject decimal (fe.log_model
# would register a signature-less model that UC refuses), and the Lakebase online feature store
# rejects PostgreSQL NUMERIC (endpoint deploy fails with "Online feature store setup failed").
# Returning DOUBLE here fixes the whole class at the source.
def u(salt: str, key: str = "CAST(id AS STRING)") -> str:
    return f"CAST(pmod(xxhash64({key}, '{salt}'), 1000000000) / 1000000000.0 AS DOUBLE)"

# COMMAND ----------
# MAGIC %md ## Customers — latest-attribute source (ColumnSelection features read this)
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE customers AS
SELECT concat('cust_', id) AS customer_id,
       CAST(21 + ({u('c_age')}*54) AS INT) AS age,
       element_at(array('CA','NY','TX','FL','WA','IL','MA','GA'), CAST({u('c_state')}*8 AS INT)+1) AS state,
       element_at(array('bronze','silver','gold','platinum'), CAST(pow({u('c_tier')},2)*4 AS INT)+1) AS loyalty_tier,
       element_at(array('low','medium','high'), CAST({u('c_risk')}*3 AS INT)+1) AS risk_band,
       ROUND(30000 + pow({u('c_income')},2)*220000, 0) AS annual_income,
       CAST({u('c_tenure')}*120 AS INT) AS tenure_months,
       CAST(from_unixtime(1700000000 + CAST({u('c_updated')}*40000000 AS INT)) AS TIMESTAMP) AS updated_at
FROM range(0, {N_CUSTOMERS}) AS t(id)
""")

# COMMAND ----------
# MAGIC %md ## Offers — NBO catalog; ranked directly by the online-lookup ranker (nb 05)
# COMMAND ----------
# Exactly 8 distinct offers per category (40 total). Category and variant are derived from id, so
# every card has unique, category-correct copy — no grid full of identical "Starter Secured Card" rows.
spark.sql(f"""
CREATE OR REPLACE TABLE offers AS
WITH o AS (
  SELECT id,
         element_at(array('credit_card','savings','personal_loan','mortgage','investment'),
                    CAST(pmod(id, 5) AS INT)+1) AS product_category,
         CAST(floor(id/5) AS INT)+1 AS variant,
         ROUND({u('o_reward')}*100, 2) AS base_reward,
         LEAST(3, CAST(floor(floor(id/5)/3) AS INT)+1) AS tier_requirement
  FROM range(0, {N_OFFERS}) AS t(id)
)
SELECT concat('offer_', id) AS offer_id, product_category,
       element_at(map(
         'credit_card', array(
           'Starter Secured Card that builds credit with no annual fee',
           'Cash+ Everyday Card with 3% cashback on everyday purchases',
           'Voyager Travel Card with airline miles and no foreign transaction fee',
           'Dining Rewards Card with 4x points at restaurants',
           'Balance Transfer Card with 0% introductory APR',
           'Business Cashback Card with expense controls',
           'Summit Platinum Card with airport lounge access',
           'Premier Airline Card with companion benefits'),
         'savings', array(
           'Everyday Savings Account with no minimum balance',
           'High-Yield Savings Account with a competitive variable APY',
           'Goal Builder Savings with automated savings rules',
           '12-Month CD with a guaranteed fixed APY',
           'Money Market Account with checkwriting access',
           'Relationship Savings with loyalty rate boosts',
           'Premier CD Ladder with flexible maturities',
           'Private Client Cash Reserve with premium rates'),
         'personal_loan', array(
           'Small Expense Loan with simple fixed payments',
           'Fixed-Rate Personal Loan with funding in one day',
           'Debt Consolidation Loan with no origination fee',
           'Home Improvement Loan with flexible terms',
           'Major Purchase Loan with predictable monthly payments',
           'Medical Financing Loan with no prepayment penalty',
           'Premier Personal Loan with a relationship rate discount',
           'Private Client Credit Line with flexible draws'),
         'mortgage', array(
           'First-Time Homebuyer Mortgage with a low down payment',
           'HomeReady Mortgage with flexible income guidelines',
           '30-Year Fixed Mortgage with predictable payments',
           '15-Year Fixed Mortgage with faster equity building',
           'Adjustable-Rate Mortgage with a lower initial rate',
           'Mortgage Refinance with streamlined closing',
           'Home Equity Line of Credit with flexible access',
           'Jumbo Mortgage with private-client pricing'),
         'investment', array(
           'Starter Investment Account with guided portfolios',
           'Robo Portfolio with automatic rebalancing',
           'Sustainable Investing Portfolio with ESG preferences',
           'Traditional IRA with tax-deferred growth',
           'Roth IRA with tax-free qualified withdrawals',
           'Retirement Rollover Service with advisor guidance',
           'Managed Portfolio with a dedicated advisor',
           'Private Wealth Advisory with tax-aware strategies')
       )[product_category], variant) AS offer_text,
       base_reward, tier_requirement
FROM o
""")

# COMMAND ----------
# MAGIC %md ## Transactions — batch history for SlidingWindow / TumblingWindow features
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE transactions AS
SELECT concat('txn_', id) AS txn_id,
       concat('cust_', CAST({u('t_cust')}*{N_CUSTOMERS} AS INT)) AS customer_id,
       element_at(array('groceries','dining','travel','retail','utilities','entertainment','healthcare','fuel'),
                  CAST({u('t_cat')}*8 AS INT)+1) AS category,
       ROUND(5 + pow({u('t_amt')},2)*2000, 2) AS amount,
       ROUND(1000 + {u('t_bal')}*50000, 2) AS balance,
       CAST(from_unixtime(1735689600 + CAST({u('t_ts')}*15552000 AS INT)) AS TIMESTAMP) AS ts
FROM range(0, {N_TRANSACTIONS}) AS t(id)
""")

# COMMAND ----------
# MAGIC %md ## Session events — in-session clickstream (RollingWindow streaming features)
# MAGIC `event_time` is the event-time column; Part 2 notebook 08 replays these into Kafka/MSK.
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE session_events AS
SELECT concat('evt_', id) AS event_id,
       concat('cust_', CAST({u('s_cust')}*{N_CUSTOMERS} AS INT)) AS customer_id,
       concat('sess_', CAST({u('s_sess')}*500000 AS INT)) AS session_id,
       element_at(array('page_view','product_view','calculator_use','add_to_cart','search'),
                  CAST({u('s_etype')}*5 AS INT)+1) AS event_type,
       element_at(array('credit_card','savings','personal_loan','mortgage','investment'),
                  CAST({u('s_pcat')}*5 AS INT)+1) AS product_category,
       CAST(200 + {u('s_dwell')}*44800 AS INT) AS dwell_ms,
       element_at(array('ios','android','web'), CAST({u('s_dev')}*3 AS INT)+1) AS device,
       CAST(from_unixtime(1735689600 + CAST({u('s_time')}*15552000 AS INT)) AS TIMESTAMP) AS event_time
FROM range(0, {N_SESSION_EVENTS}) AS t(id)
""")

# COMMAND ----------
# MAGIC %md ## Labels — offer acceptances for point-in-time training
# MAGIC Each label is one offer **impression** with the context a real visitor would carry:
# MAGIC - **stored customer attributes** (looked up online by `customer_id`): tier, risk, income, tenure;
# MAGIC - **request-time answers** (`ctx_*`, passed in the request): stated goal, self-rated credit,
# MAGIC   stated income, monthly card spend;
# MAGIC - **in-session behavior**: half the impressions get 1-4 session events in the 10 minutes before
# MAGIC   `ts`, mostly in the visitor's latent "need" category. They reach the ranker two ways:
# MAGIC   - **website clicks** (`device = 'web'`) are what the bank's site sees in the visitor's own session,
# MAGIC     so they are a **request-time** feature, `ctx_session_cat_views` (the app sends the count);
# MAGIC   - **mobile-app activity** (`ios`/`android`) arrives on another channel: the events are appended to
# MAGIC     `session_events`, replayed into Kafka with their original `event_time` (notebook 08), and reach
# MAGIC     the ranker as the **streaming** feature `cust_mobile_cat_views_10m` (Part 2).
# MAGIC
# MAGIC Acceptance is a logistic model with **customer x offer interactions** (tier affinity by category,
# MAGIC risk gating on credit products, income and tenure fit, goal and in-session intent matches), so the
# MAGIC best offer differs from customer to customer.
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW labels_ctx AS
WITH base AS (
  SELECT concat('lbl_', id) AS record_id,
         concat('cust_', CAST({u('l_cust')}*{N_CUSTOMERS} AS INT)) AS customer_id,
         concat('offer_', CAST({u('l_offer')}*{N_OFFERS} AS INT)) AS offer_id,
         CAST(from_unixtime(1748000000 + CAST({u('l_ts')}*3000000 AS INT)) AS TIMESTAMP) AS ts,
         {u('l_noise')} AS noise, {u('l_need')} AS un, {u('l_goal')} AS ug, {u('l_goal2')} AS ug2,
         {u('l_cred')} AS uc, {u('l_cred2')} AS uc2, {u('l_inc')} AS ui, {u('l_spend')} AS us, {u('l_intent')} AS uk
  FROM range(0, {N_LABELS}) AS t(id)
),
c AS (
  SELECT b.*, cu.risk_band, cu.annual_income, cu.tenure_months,
         CASE cu.loyalty_tier WHEN 'bronze' THEN 0 WHEN 'silver' THEN 1 WHEN 'gold' THEN 2 ELSE 3 END AS tier_idx
  FROM base b JOIN customers cu ON b.customer_id = cu.customer_id
),
n AS (  -- latent need: what this visitor is actually in the market for, skewed by tier
  SELECT *, CASE
      WHEN un < element_at(array(.35, .30, .20, .15), tier_idx + 1) THEN 'credit_card'
      WHEN un < element_at(array(.70, .55, .35, .25), tier_idx + 1) THEN 'savings'
      WHEN un < element_at(array(.90, .80, .50, .30), tier_idx + 1) THEN 'personal_loan'
      WHEN un < element_at(array(.95, .90, .75, .55), tier_idx + 1) THEN 'mortgage'
      ELSE 'investment' END AS need
  FROM c
)
SELECT record_id, customer_id, offer_id, ts, noise, need, tier_idx, risk_band, annual_income, tenure_months,
       CASE WHEN ug < .6 THEN need
            WHEN ug < .7 THEN element_at(array('credit_card','savings','personal_loan','mortgage','investment'), CAST(ug2*5 AS INT)+1)
            ELSE 'none' END AS ctx_goal,
       CASE WHEN uc < .8 THEN CASE risk_band WHEN 'low' THEN 'excellent' WHEN 'medium' THEN 'good' ELSE 'fair' END
            ELSE element_at(array('excellent','good','fair'), CAST(uc2*3 AS INT)+1) END AS ctx_credit,
       CAST(ROUND(annual_income*(0.85 + 0.3*ui)/5000)*5000 AS DOUBLE) AS ctx_income,
       CAST(ROUND(annual_income/12*(0.05 + 0.15*us)/50)*50 AS DOUBLE) AS ctx_card_spend,
       CASE WHEN uk < .5 THEN CAST(1 + uk*8 AS INT) ELSE 0 END AS n_intent  -- half get 1..4 events
FROM n
""")

# In-session events in the 10 minutes before each impression (5..545 s earlier), 80% in the need category.
EK = "concat(record_id, '_', CAST(i AS STRING))"
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW label_session_events AS
SELECT record_id,
       concat('evt_l', substr(record_id, 5), '_', i) AS event_id, customer_id,
       concat('sess_l', substr(record_id, 5)) AS session_id,
       element_at(array('product_view','calculator_use','page_view','add_to_cart','search'),
                  CAST({u('e_type', EK)}*5 AS INT)+1) AS event_type,
       CASE WHEN {u('e_cat', EK)} < .8 THEN need
            ELSE element_at(array('credit_card','savings','personal_loan','mortgage','investment'),
                            CAST({u('e_cat2', EK)}*5 AS INT)+1) END AS product_category,
       CAST(200 + {u('e_dwell', EK)}*44800 AS INT) AS dwell_ms,
       element_at(array('ios','android','web'), CAST({u('e_dev', EK)}*3 AS INT)+1) AS device,
       timestampadd(SECOND, -(5 + CAST({u('e_t', EK)}*540 AS INT)), ts) AS event_time
FROM (SELECT *, explode(sequence(1, n_intent)) AS i FROM labels_ctx WHERE n_intent > 0)
""")
spark.sql("""
INSERT INTO session_events
SELECT event_id, customer_id, session_id, event_type, product_category, dwell_ms, device, event_time
FROM label_session_events
""")

spark.sql(f"""
CREATE OR REPLACE TABLE labels AS
WITH j AS (
  SELECT l.*, o.product_category AS cat, o.base_reward, o.tier_requirement,
         COALESCE(k.k_cat, 0) AS k_cat, COALESCE(k.k_web, 0) AS k_web
  FROM labels_ctx l
  JOIN offers o ON l.offer_id = o.offer_id
  LEFT JOIN (SELECT record_id, product_category, count(*) AS k_cat,
                    count_if(device = 'web') AS k_web   -- clicks on the bank's website (this session)
             FROM label_session_events GROUP BY 1, 2) k
         ON k.record_id = l.record_id AND k.product_category = o.product_category
),
s AS (
  SELECT *,
    (-1.6 + base_reward/100.0*0.35
     + element_at(map('credit_card', array(.5, .4, .2, .0), 'savings', array(.7, .4, .0, -.3),
                      'personal_loan', array(.2, .5, .1, -.7), 'mortgage', array(-.8, .0, .5, .3),
                      'investment', array(-1.0, -.3, .5, 1.1))[cat], tier_idx + 1)        -- tier x category
     - 1.4*greatest(tier_requirement - (tier_idx + 1), 0)                                 -- below eligibility
     + CASE WHEN cat IN ('personal_loan','mortgage') OR cat = 'credit_card'
            THEN CASE ctx_credit
              WHEN 'excellent' THEN .55*(tier_requirement - 1)                  -- premium credit products
              WHEN 'good'      THEN CASE WHEN tier_requirement = 2 THEN .25 ELSE -.15*abs(tier_requirement - 2) END
              ELSE .55 - 1.05*(tier_requirement - 1) END                       -- fair/building credit -> starter
            ELSE 0 END                                                         -- request-time credit x offer tier
     + CASE WHEN cat IN ('personal_loan','mortgage')
            THEN CASE risk_band WHEN 'low' THEN .2 WHEN 'medium' THEN -.15 ELSE -.65 END ELSE 0 END
     + CASE WHEN cat = 'credit_card' AND tier_requirement = 1                             -- starter card
            THEN CASE ctx_credit WHEN 'fair' THEN 1.1 WHEN 'good' THEN .15 ELSE -.55 END - tenure_months/120.0*.35
            ELSE 0 END
     + CASE WHEN cat = 'credit_card'
            THEN least(greatest((ctx_card_spend - 1750)/2500.0, -.9), 1.1)*(tier_requirement - 1)
            ELSE 0 END                                                         -- spend x card tier
     + CASE WHEN cat IN ('mortgage','investment','personal_loan')
            THEN least(greatest((ctx_income - 85000)/75000.0, -1.0), 1.1)*(tier_requirement - 1)
            ELSE 0 END                                                         -- stated income x offer tier
     + CASE WHEN cat = 'mortgage'   THEN least(greatest((ctx_income - 95000)/60000.0, -1.2), 1.0) ELSE 0 END
     + CASE WHEN cat = 'investment' THEN least(greatest((ctx_income - 80000)/90000.0, -1.0), .9) ELSE 0 END
     + CASE WHEN cat IN ('investment','mortgage') THEN tenure_months/120.0*.5 ELSE 0 END
     + CASE WHEN cat = 'savings' AND tier_requirement = 1 THEN (1 - tenure_months/120.0)*.4 ELSE 0 END
     + CASE WHEN cat = need     THEN .6 ELSE 0 END                                        -- latent need
     + CASE WHEN cat = ctx_goal THEN 2.2 ELSE 0 END                                       -- explicit stated goal: strongest signal
     + .9*least(k_web, 4)                                                                 -- this website visit (request-time)
     + .45*least(greatest(k_cat - k_web, 0), 4)                                           -- other channels (streaming)
    ) AS logit
  FROM j
)
SELECT record_id, customer_id, offer_id, ts, ctx_goal, ctx_credit, ctx_income, ctx_card_spend,
       CAST(k_web AS INT) AS ctx_session_cat_views,
       CAST(CASE WHEN (1.0/(1.0 + exp(-logit))) > noise THEN 1 ELSE 0 END AS INT) AS accepted
FROM s
""")

# COMMAND ----------
# MAGIC %md ## Verify
# COMMAND ----------
for t in ["customers", "offers", "transactions", "session_events", "labels"]:
    print(f"{t:16s} {spark.table(t).count():>12,}")
display(spark.sql("SELECT round(avg(accepted),4) AS accept_rate FROM labels"))
# Personalization check: acceptance by tier x category must vary (the old additive labels were flat).
display(spark.sql("""
SELECT c.loyalty_tier, o.product_category, round(avg(l.accepted), 3) AS accept_rate, count(*) AS n
FROM labels l JOIN customers c USING (customer_id) JOIN offers o USING (offer_id)
GROUP BY 1, 2 ORDER BY 1, 3 DESC"""))
