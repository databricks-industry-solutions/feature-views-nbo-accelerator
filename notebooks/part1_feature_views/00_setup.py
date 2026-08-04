# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Configure catalog/schema, create the Lakebase online store, and set permissions.
# MAGIC
# MAGIC **Prereqs:** DBR 17.0 ML+, `databricks-feature-engineering>=0.16.0`, a catalog on
# MAGIC standard storage (required for streaming Feature Views).

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "nbo_accelerator")
dbutils.widgets.text("schema", "main")
dbutils.widgets.text("online_store_name", "nbo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
online_store_name = dbutils.widgets.get("online_store_name")

# COMMAND ----------
# Catalog/schema provisioning is check-then-create (like the online store, features, and
# materialization below) so this is safely re-runnable AND portable across account types.
# On accounts with **Default Storage** enabled, a bare `CREATE CATALOG IF NOT EXISTS` throws
# INVALID_STATE ("Metastore storage root URL does not exist ... provide a storage location"):
# the analyzer validates the missing MANAGED LOCATION before IF NOT EXISTS can short-circuit,
# so it errors even when the catalog already exists. We therefore only CREATE when the catalog
# is genuinely absent, and let a pre-provisioned catalog (any storage type) be reused as-is.
from databricks.sdk import WorkspaceClient
_w = WorkspaceClient()

_catalog_exists = any(c.name == catalog for c in _w.catalogs.list())
if not _catalog_exists:
    # Absent → create it. Prefer a plain create; if the account requires an explicit managed
    # location (Default Storage disabled with no default root), surface a clear, actionable error.
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception as e:
        raise RuntimeError(
            f"Catalog {catalog!r} does not exist and could not be auto-created ({e}). "
            f"Create it once in the UI (or via `CREATE CATALOG {catalog} MANAGED LOCATION '<s3/abfss uri>'`) "
            f"on a standard-storage location, then re-run. Streaming Feature Views (Part 2) require "
            f"standard (non-default) storage."
        ) from e
    print(f"Created catalog {catalog}.")
else:
    print(f"Catalog {catalog} already exists — reusing it.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE `{catalog}`.`{schema}`")

# COMMAND ----------
# MAGIC %md ## Create the Lakebase online store
# MAGIC Name must be DNS-compliant: lowercase, alphanumeric, **no underscores**.
# MAGIC
# MAGIC **Naming matters for the streaming sink (Part 2).** Keep the online store name and the online
# MAGIC table prefixes simple (here: store `nbo`, prefixes `nbo_on` / `nbostream`). Streaming online
# MAGIC materialization can fail on identifiers that force the sink to quote its Postgres target, so
# MAGIC prefer plain single-word names over decorated ones. Batch (Part 1) is unaffected either way.
# MAGIC
# MAGIC **One store for everything.** Part 1's batch features and Part 2's streaming feature must
# MAGIC materialize into this **same** online store — a served model cannot look up features across
# MAGIC multiple online stores on new Lakebase stores, and splitting them breaks route-optimized
# MAGIC serving. Notebook 08 preflights this and fails loudly on a mismatch.
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Create the Lakebase online store (idempotent: skip if it already exists).
# Both batch (Part 1) and streaming (Part 2) materialization write here, so a clean
# deploy MUST create it — otherwise 08's materialize_features has no online store target.
try:
    existing = fe.get_online_store(name=online_store_name)
except Exception:
    existing = None
if existing is not None:
    print(f"Online store {online_store_name} already exists.")
else:
    fe.create_online_store(name=online_store_name, capacity="CU_1")
    print(f"Created online store {online_store_name} (CU_1). Allow a few min to reach AVAILABLE.")

print(f"Catalog={catalog}  Schema={schema}  OnlineStore={online_store_name}")
