# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00_setup_environment
# MAGIC Run this **once** before the pipeline. Creates the Unity Catalog catalog,
# MAGIC the raw/bronze/silver/gold schemas, and the raw landing volume. Idempotent —
# MAGIC safe to re-run.

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir_catalog")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

print(f"Catalog '{CATALOG}' ready with schemas: raw, bronze, silver, gold")
print(f"Raw landing volume: /Volumes/{CATALOG}/raw/landing")

# COMMAND ----------

# MAGIC %md
# MAGIC If your workspace doesn't have Unity Catalog enabled, swap the volume path
# MAGIC in `00_config.py` (`RAW_VOLUME`) for a DBFS or ADLS path (e.g.
# MAGIC `/mnt/fhir/raw/landing` or `abfss://raw@<storage>.dfs.core.windows.net/landing`)
# MAGIC and use the `hive_metastore` catalog / plain database names instead.

# COMMAND ----------

