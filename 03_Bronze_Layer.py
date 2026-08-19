# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03_Bronze_Layer
# MAGIC Reads the raw JSON bundles for the run's resources/dates, flattens each
# MAGIC `Bundle.entry` into one row per FHIR resource instance, and MERGEs into a
# MAGIC Delta bronze table using **SCD Type 2**: a new row is added only when the
# MAGIC resource's content actually changed (detected via hash), preserving full
# MAGIC history (`valid_from`, `valid_to`, `is_current`).
# MAGIC
# MAGIC Metadata columns added per the assignment: `extraction_timestamp`,
# MAGIC `api_url_or_params`, plus `source_file`, `record_hash`.

# COMMAND ----------

# MAGIC %run ./utils_nb

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import datetime as dt

start_date, end_date = resolve_date_window()
resources = resolve_resource_list()
date_range = [start_date + dt.timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# COMMAND ----------

def load_raw_bundles(resource_type: str, dates) -> "DataFrame":
    """Read every raw JSON page for this resource/date range and explode
    Bundle.entry into one row per FHIR resource, keeping the page's call
    metadata attached to every row it produced."""
    paths = [raw_path(resource_type, d.isoformat()) for d in dates]
    existing = [p for p in paths if any(f.path for f in dbutils.fs.ls(p)) or True]

    raw_df = (
        spark.read
        .option("multiLine", True)
        .json(existing)              # each row = one raw Bundle page
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    exploded = (
        raw_df
        .withColumn("_entry", F.explode_outer("entry"))
        .select(
            F.col("_entry.resource").alias("resource_json_struct"),
            F.to_json(F.col("_entry.resource")).alias("resource_json"),
            F.col("_entry.resource.id").alias("resource_id"),
            F.col("_entry.resource.meta.lastUpdated").alias("fhir_last_updated"),
            "_source_file",
        )
        .withColumn("resource_type", F.lit(resource_type))
        .withColumn("record_hash", F.sha2(F.col("resource_json"), 256))
    )
    return exploded

# COMMAND ----------

def merge_scd2(resource_type: str, new_df) -> None:
    """SCD Type 2 upsert: close out the current row for any resource_id whose
    content hash changed, and insert the new version. First run creates the
    table with all rows current."""
    target_name = bronze_table(resource_type)

    # Pull in run-level metadata (mirrors the audit log so bronze rows
    # carry extraction_timestamp / api_url_or_params without a join at query time)
    audit = (
        spark.table(control_table())
        .filter((F.col("resource_type") == resource_type) & (F.col("status") == "SUCCESS"))
        .select(
            F.regexp_extract("api_url_or_params", r"^(https?://[^ ]+)", 1).alias("api_url_or_params"),
            "extraction_timestamp",
        )
        .orderBy(F.col("extraction_timestamp").desc())
        .limit(1)
    ).collect()
    api_url_or_params = audit[0]["api_url_or_params"] if audit else None
    extraction_timestamp = audit[0]["extraction_timestamp"] if audit else dt.datetime.utcnow().isoformat() + "Z"

    staged = (
        new_df
        .dropDuplicates(["resource_id", "record_hash"])
        .withColumn("extraction_timestamp", F.lit(extraction_timestamp))
        .withColumn("api_url_or_params", F.lit(api_url_or_params))
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    # Check if table exists using SQL (Spark Connect compatible)
    try:
        spark.sql(f"DESCRIBE TABLE {target_name}")
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        (staged.write.format("delta").mode("overwrite").saveAsTable(target_name))
        print(f"[{resource_type}] created bronze table with {staged.count()} current rows")
        return

    target = DeltaTable.forName(spark, target_name)

    # Step 1: find rows whose hash changed vs. the current version -> expire them
    changed = (
        staged.alias("s")
        .join(
            target.toDF().filter("is_current = true").alias("t"),
            on="resource_id",
            how="inner",
        )
        .filter(F.col("s.record_hash") != F.col("t.record_hash"))
        .select("s.resource_id")
    )

    if changed.count() > 0:
        change_ids = [r["resource_id"] for r in changed.collect()]
        (target.update(
            condition=(F.col("is_current") == True) & (F.col("resource_id").isin(change_ids)),  # noqa: E712
            set={"is_current": F.lit(False), "valid_to": F.current_timestamp()},
        ))

    # Step 2: insert brand-new resource_ids and new versions of changed ones
    existing_current = target.toDF().filter("is_current = true").select("resource_id", "record_hash")
    to_insert = staged.join(existing_current, ["resource_id", "record_hash"], "left_anti")

    if to_insert.count() > 0:
        to_insert.write.format("delta").mode("append").saveAsTable(target_name)

    print(f"[{resource_type}] expired {changed.count()} changed row(s), inserted {to_insert.count()} new version(s)")

# COMMAND ----------

for resource_type in resources:
    bronze_df = load_raw_bundles(resource_type, date_range)
    merge_scd2(resource_type, bronze_df)

# COMMAND ----------

for resource_type in resources:
    print(f"--- {bronze_table(resource_type)} (current rows) ---")
    display(spark.table(bronze_table(resource_type)).filter("is_current = true").limit(5))

# COMMAND ----------

