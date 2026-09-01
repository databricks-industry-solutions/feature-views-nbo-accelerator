# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Configure catalog/schema and create the Lakebase online store.
# MAGIC
# MAGIC **Prereqs:** DBR 17.0 ML+, `databricks-feature-engineering>=0.16.0`, and a **pre-existing**
# MAGIC Unity Catalog catalog on **standard storage** (required for streaming Feature Views). This
# MAGIC notebook **assumes the catalog already exists** (most users cannot `CREATE CATALOG`); set
# MAGIC `create_catalog=true` only if you hold that privilege. Data lands in a **per-user schema**
# MAGIC `nbo_<username>` by default — override with the `schema` widget.

# COMMAND ----------
# MAGIC %pip install "databricks-feature-engineering>=0.16.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "fins_industry_solutions")
dbutils.widgets.text("schema", "")  # blank -> auto-derive nbo_<user>
dbutils.widgets.text("online_store_name", "nbo")
dbutils.widgets.dropdown("create_catalog", "false", ["false", "true"])  # true only if you hold CREATE CATALOG

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
if not schema:
    import re as _re
    _user = spark.sql("SELECT current_user()").first()[0]
    schema = "nbo_" + _re.sub(r"[^a-z0-9]+", "_", _user.split("@")[0].lower()).strip("_")
online_store_name = dbutils.widgets.get("online_store_name")
create_catalog = dbutils.widgets.get("create_catalog") == "true"

# COMMAND ----------
# Catalog handling is customer-safe by default: most users do NOT have CREATE CATALOG on the
# metastore, so we ASSUME the catalog already exists and simply reuse it. Only when you explicitly
# opt in (create_catalog=true) AND hold the privilege do we attempt to create it. Streaming Feature
# Views (Part 2) require a catalog on STANDARD storage (not Default Storage).
from databricks.sdk import WorkspaceClient
_w = WorkspaceClient()

_catalog_exists = any(c.name == catalog for c in _w.catalogs.list())
if _catalog_exists:
    print(f"Catalog {catalog} already exists — reusing it.")
elif not create_catalog:
    raise RuntimeError(
        f"Catalog {catalog!r} does not exist. Most users cannot CREATE CATALOG — ask your metastore "
        f"admin to create it on STANDARD storage (streaming Feature Views require non-default storage) "
        f"and grant you USE CATALOG + CREATE SCHEMA. If you DO hold the CREATE CATALOG privilege, set "
        f"the create_catalog widget to 'true' and re-run."
    )
else:
    # Opted in + privileged. Prefer a plain create; if the account requires an explicit managed
    # location (Default Storage disabled with no default root), surface a clear, actionable error.
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception as e:
        raise RuntimeError(
            f"Catalog {catalog!r} could not be created ({e}). Create it once in the UI (or via "
            f"`CREATE CATALOG {catalog} MANAGED LOCATION '<s3/abfss uri>'`) on a standard-storage "
            f"location, then re-run. Streaming Feature Views (Part 2) require standard (non-default) storage."
        ) from e
    print(f"Created catalog {catalog}.")

# Requires CREATE SCHEMA on the catalog (see README prerequisites).
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
