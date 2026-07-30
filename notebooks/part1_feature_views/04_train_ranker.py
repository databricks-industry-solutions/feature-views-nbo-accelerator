# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Train the Ranking Model
# MAGIC The anti-skew proof point: the **same** Feature objects from notebook 02 build a
# MAGIC point-in-time-correct training set, then a LightGBM classifier ranks offers by
# MAGIC acceptance probability. Logged with `fe.log_model` so the serving endpoint auto-fetches
# MAGIC online features by entity key at request time (notebook 05).
# MAGIC
# MAGIC **Verified end-to-end on serverless** — 400K labels, val AUC ≈ 0.70, registered to UC
# MAGIC `nbo_ranker@prod`.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0" lightgbm scikit-learn
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "nbo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import mlflow
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------
# MAGIC %md ## Point-in-time training set from the registered Feature objects
# MAGIC `create_training_set` joins each feature as-of each label row's timestamp — no lookahead.
# MAGIC ColumnSelection features key off `updated_at`, so we mirror `ts → updated_at` on the labels.
# COMMAND ----------
def gf(name):
    return fe.get_feature(full_name=f"{catalog}.{schema}.{name}")

features = [gf(n) for n in ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d",
                            "cust_loyalty_tier", "cust_risk_band"]]

labels = (spark.table(f"`{catalog}`.{schema}.labels")     # record_id, customer_id, offer_id, ts, accepted
          .withColumn("updated_at", F.col("ts")))         # PIT key for ColumnSelection features

training_set = fe.create_training_set(
    df=labels,
    features=features,
    label="accepted",
    exclude_columns=["record_id", "customer_id", "ts", "updated_at"],
)
tdf = training_set.load_df().toPandas()
print(f"rows={len(tdf):,}  accept_rate={tdf['accepted'].mean():.3f}")

# COMMAND ----------
# MAGIC %md ## Train LightGBM (ordinal-encode categoricals, passthrough numerics)
# COMMAND ----------
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from lightgbm import LGBMClassifier

CAT = ["offer_id", "cust_loyalty_tier", "cust_risk_band"]
NUM = ["cust_avg_balance_30d", "cust_spend_90d", "cust_txn_count_7d"]

X = tdf[CAT + NUM].copy()
for c in CAT:
    X[c] = X[c].astype(str)
for c in NUM:
    X[c] = pd.to_numeric(X[c], errors="coerce")
y = tdf["accepted"].astype(int)

Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

pre = ColumnTransformer(
    [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT)],
    remainder="passthrough",
)
model = Pipeline([
    ("pre", pre),
    ("clf", LGBMClassifier(n_estimators=200, learning_rate=0.05, num_leaves=31,
                           subsample=0.8, colsample_bytree=0.8, random_state=42)),
])

# COMMAND ----------
# MAGIC %md ## Log with feature metadata + register to UC with @prod alias
# COMMAND ----------
MODEL_NAME = f"{catalog}.{schema}.nbo_ranker"

# Pin the serving env explicitly. Without this, MLflow auto-detects from the cluster and the
# serving-image build often fails. Exact-pin unpickle-sensitive libs (mlflow/sklearn/lightgbm);
# range-pin numpy/pandas below mlflow's caps. Do NOT add databricks-feature-engineering here —
# fe.log_model injects databricks-feature-lookup, and the two conflict over the same namespace.
import sklearn, lightgbm
pip_requirements = [
    f"mlflow=={mlflow.__version__}",
    f"scikit-learn=={sklearn.__version__}",
    f"lightgbm=={lightgbm.__version__}",
    "numpy>=1.26,<2",
    "pandas>=2.1,<3",
    "cloudpickle",
]

with mlflow.start_run(run_name="nbo_ranker") as run:
    model.fit(Xtr, ytr)
    auc = roc_auc_score(yte, model.predict_proba(Xte)[:, 1])
    mlflow.log_metric("val_auc", auc)
    print("val_auc:", auc)
    fe.log_model(model=model, artifact_path="ranker", flavor=mlflow.sklearn,
                 training_set=training_set, registered_model_name=MODEL_NAME,
                 pip_requirements=pip_requirements)

from mlflow.tracking import MlflowClient
c = MlflowClient(registry_uri="databricks-uc")
newest = max(c.search_model_versions(f"name='{MODEL_NAME}'"), key=lambda v: int(v.version))
c.set_registered_model_alias(MODEL_NAME, "prod", newest.version)
print(f"Registered {MODEL_NAME} v{newest.version} @prod")
