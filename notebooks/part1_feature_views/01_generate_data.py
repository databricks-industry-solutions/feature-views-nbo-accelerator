# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate Synthetic FSI Data
# MAGIC Fully synthetic, reproducible retail-banking dataset — no external licensing.
# MAGIC All randomness is seeded so re-runs are deterministic.
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
dbutils.widgets.text("schema", "nbo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE `{catalog}`.{schema}")

# COMMAND ----------
# MAGIC %md ## Scale knobs
# COMMAND ----------
N_CUSTOMERS = 100_000
N_OFFERS = 40
N_TRANSACTIONS = 5_000_000
N_SESSION_EVENTS = 20_000_000
N_LABELS = 400_000

# COMMAND ----------
# MAGIC %md ## Customers — latest-attribute source (ColumnSelection features read this)
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE customers AS
SELECT concat('cust_', id) AS customer_id,
       CAST(21 + (rand(1)*54) AS INT) AS age,
       element_at(array('CA','NY','TX','FL','WA','IL','MA','GA'), CAST(rand(2)*8 AS INT)+1) AS state,
       element_at(array('bronze','silver','gold','platinum'), CAST(pow(rand(3),2)*4 AS INT)+1) AS loyalty_tier,
       element_at(array('low','medium','high'), CAST(rand(4)*3 AS INT)+1) AS risk_band,
       ROUND(30000 + pow(rand(5),2)*220000, 0) AS annual_income,
       CAST(rand(6)*120 AS INT) AS tenure_months,
       CAST(from_unixtime(1700000000 + CAST(rand(7)*40000000 AS INT)) AS TIMESTAMP) AS updated_at
FROM range(0, {N_CUSTOMERS}) AS t(id)
""")

# COMMAND ----------
# MAGIC %md ## Offers — NBO catalog; ranked directly by the online-lookup ranker (nb 05)
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE offers AS
SELECT concat('offer_', id) AS offer_id,
       element_at(array('credit_card','savings','personal_loan','mortgage','investment'),
                  CAST(rand(11)*5 AS INT)+1) AS product_category,
       element_at(array(
         'Premium Rewards Credit Card with 3% cashback on all purchases',
         'High-Yield Savings Account with 4.5% APY and no minimum balance',
         'Personal Loan up to 50k with fixed low APR and flexible terms',
         '30-Year Fixed Mortgage with competitive rates and no origination fee',
         'Diversified Investment Portfolio with robo-advisor and low fees',
         'Travel Credit Card with airline miles and no foreign transaction fees',
         'Student Checking Account with no monthly fees and overdraft protection',
         'Home Equity Line of Credit with variable rate and easy access',
         'Retirement IRA with tax advantages and employer matching guidance',
         'Business Credit Card with expense tracking and cashback rewards'),
         CAST(rand(12)*10 AS INT)+1) AS offer_text,
       ROUND(rand(13)*100, 2) AS base_reward,
       CAST(rand(14)*3 AS INT)+1 AS tier_requirement
FROM range(0, {N_OFFERS}) AS t(id)
""")

# COMMAND ----------
# MAGIC %md ## Transactions — batch history for SlidingWindow / TumblingWindow features
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE transactions AS
SELECT concat('txn_', id) AS txn_id,
       concat('cust_', CAST(rand(21)*{N_CUSTOMERS} AS INT)) AS customer_id,
       element_at(array('groceries','dining','travel','retail','utilities','entertainment','healthcare','fuel'),
                  CAST(rand(22)*8 AS INT)+1) AS category,
       ROUND(5 + pow(rand(23),2)*2000, 2) AS amount,
       ROUND(1000 + rand(24)*50000, 2) AS balance,
       CAST(from_unixtime(1735689600 + CAST(rand(25)*15552000 AS INT)) AS TIMESTAMP) AS ts
FROM range(0, {N_TRANSACTIONS}) AS t(id)
""")

# COMMAND ----------
# MAGIC %md ## Session events — in-session clickstream (RollingWindow streaming features)
# MAGIC `event_time` is the event-time column; Part 2 notebook 07 replays these into Kafka/MSK.
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE session_events AS
SELECT concat('evt_', id) AS event_id,
       concat('cust_', CAST(rand(31)*{N_CUSTOMERS} AS INT)) AS customer_id,
       concat('sess_', CAST(rand(32)*500000 AS INT)) AS session_id,
       element_at(array('page_view','product_view','calculator_use','add_to_cart','search'),
                  CAST(rand(33)*5 AS INT)+1) AS event_type,
       element_at(array('credit_card','savings','personal_loan','mortgage','investment'),
                  CAST(rand(34)*5 AS INT)+1) AS product_category,
       CAST(200 + rand(35)*44800 AS INT) AS dwell_ms,
       element_at(array('ios','android','web'), CAST(rand(36)*3 AS INT)+1) AS device,
       CAST(from_unixtime(1735689600 + CAST(rand(37)*15552000 AS INT)) AS TIMESTAMP) AS event_time
FROM range(0, {N_SESSION_EVENTS}) AS t(id)
""")

# COMMAND ----------
# MAGIC %md ## Labels — offer acceptances for point-in-time training
# MAGIC Acceptance carries **real signal** (a logistic model of loyalty tier, risk band, income,
# MAGIC tenure, and offer reward-vs-tier fit) so the ranker learns something meaningful
# MAGIC (val AUC ≈ 0.70), not noise. ~36% base accept rate.
# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE labels AS
WITH base AS (
  SELECT concat('lbl_', id) AS record_id,
         concat('cust_', CAST(rand(41)*{N_CUSTOMERS} AS INT)) AS customer_id,
         concat('offer_', CAST(rand(42)*{N_OFFERS} AS INT)) AS offer_id,
         CAST(from_unixtime(1748000000 + CAST(rand(43)*3000000 AS INT)) AS TIMESTAMP) AS ts,
         rand(44) AS noise
  FROM range(0, {N_LABELS}) AS t(id)
),
joined AS (
  SELECT b.*, c.loyalty_tier, c.risk_band, c.annual_income, c.tenure_months,
         o.tier_requirement, o.base_reward
  FROM base b JOIN customers c ON b.customer_id = c.customer_id
              JOIN offers o    ON b.offer_id    = o.offer_id
),
scored AS (
  SELECT record_id, customer_id, offer_id, ts, noise,
         (-1.5
          + CASE loyalty_tier WHEN 'platinum' THEN 1.6 WHEN 'gold' THEN 1.0
                              WHEN 'silver' THEN 0.4 ELSE 0.0 END
          + CASE risk_band WHEN 'low' THEN 0.7 WHEN 'medium' THEN 0.2 ELSE -0.5 END
          + (annual_income/250000.0)*0.8
          + (tenure_months/120.0)*0.5
          + (base_reward/100.0)*0.6
          - tier_requirement*0.3) AS logit
  FROM joined
)
SELECT record_id, customer_id, offer_id, ts,
       CAST(CASE WHEN (1.0/(1.0+exp(-logit))) > noise THEN 1 ELSE 0 END AS INT) AS accepted
FROM scored
""")

# COMMAND ----------
# MAGIC %md ## Verify
# COMMAND ----------
for t in ["customers", "offers", "transactions", "session_events", "labels"]:
    print(f"{t:16s} {spark.table(t).count():>12,}")
display(spark.sql("SELECT round(avg(accepted),4) AS accept_rate FROM labels"))
