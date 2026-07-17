# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Train the Ranking Model
# MAGIC The anti-skew proof point: the **same** Feature objects from nb 02 build a
# MAGIC point-in-time-correct training set. Train a LightGBM ranker, register to UC.

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

# COMMAND ----------
# MAGIC %md ## Point-in-time training set
# MAGIC `create_training_set` joins each feature as-of each label row's timestamp — no lookahead.
labeled_df = spark.table(f"{catalog}.{schema}.labels")  # customer_id, offer_id, ts, accepted

# training_set = fe.create_training_set(
#     df=labeled_df,
#     features=ALL_FEATURES,          # from nb 02
#     label="accepted",
#     exclude_columns=["customer_id", "offer_id", "ts"],
# )
# train_pdf = training_set.load_df().toPandas()

# COMMAND ----------
# MAGIC %md ## Train LightGBM ranker (autolog) + Optuna HPO
# MAGIC Learning-to-rank grouped by customer session. Log with fe.log_model so the model
# MAGIC carries feature metadata and can score straight from the online store.
mlflow.lightgbm.autolog()
# TODO: LGBMRanker with group = per-session counts; Optuna for num_leaves/lr/n_estimators

# COMMAND ----------
# MAGIC %md ## Register to Unity Catalog with @prod alias
MODEL_NAME = f"{catalog}.{schema}.nbo_ranker"
# fe.log_model(model=..., artifact_path="ranker", flavor=mlflow.lightgbm,
#              training_set=training_set, registered_model_name=MODEL_NAME)
# TODO: set @prod alias on the winning version
print(f"TODO: train + register {MODEL_NAME}")
