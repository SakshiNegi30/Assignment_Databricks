# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02_Raw_Ingestion
# MAGIC **Raw layer**: store every FHIR API page response *as-is* (JSON), bucketed
# MAGIC by `resource_type/batch_date`. This is the immutable, replayable landing
# MAGIC zone the assignment calls for — nothing is transformed here.
# MAGIC
# MAGIC Also writes one row per API call to the pipeline audit/control table
# MAGIC (`raw.pipeline_audit_log`) recording: resource, batch_date, page number,
# MAGIC request URL/params, extraction timestamp, save timestamp, record count,
# MAGIC and success/failure — this is the "when was each API called / when was
# MAGIC data saved" tracking the assignment asks for.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_fhir_api_client

# COMMAND ----------

import datetime as dt
from pyspark.sql import Row

start_date, end_date = resolve_date_window()
resources = resolve_resource_list()

dbutils.fs.mkdirs(RAW_VOLUME)

audit_rows = []
date_range = [start_date + dt.timedelta(days=i) for i in range((end_date - start_date).days + 1)]

for resource_type in resources:               # orchestration order enforced by resolve_resource_list()
    for batch_date in date_range:
        folder = raw_path(resource_type, batch_date.isoformat())
        dbutils.fs.mkdirs(folder)

        try:
            pages = fetch_resource_pages(resource_type, batch_date)
        except Exception as exc:  # noqa: BLE001
            audit_rows.append(Row(
                resource_type=resource_type,
                batch_date=batch_date.isoformat(),
                page_number=None,
                api_url_or_params=None,
                extraction_timestamp=dt.datetime.utcnow().isoformat() + "Z",
                save_timestamp=None,
                record_count=0,
                status="FAILED",
                error_message=str(exc)[:1000],
            ))
            continue

        for page_num, (bundle, called_url, extraction_ts) in enumerate(pages, start=1):
            file_path = f"{folder}/page_{page_num:03d}.json"
            dbutils.fs.put(file_path, json.dumps(bundle), overwrite=True)
            save_ts = dt.datetime.utcnow().isoformat() + "Z"

            audit_rows.append(Row(
                resource_type=resource_type,
                batch_date=batch_date.isoformat(),
                page_number=page_num,
                api_url_or_params=called_url,
                extraction_timestamp=extraction_ts,
                save_timestamp=save_ts,
                record_count=count_entries(bundle),
                status="SUCCESS",
                error_message=None,
            ))

# COMMAND ----------

# Persist audit log (append-only) so every historical run stays queryable
if audit_rows:
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    






















































































    
    schema = StructType([
        StructField("resource_type", StringType(), False),
        StructField("batch_date", StringType(), False),
        StructField("page_number", IntegerType(), True),
        StructField("api_url_or_params", StringType(), True),
        StructField("extraction_timestamp", StringType(), False),
        StructField("save_timestamp", StringType(), True),
        StructField("record_count", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ])
    
    audit_df = spark.createDataFrame(audit_rows, schema=schema)
    (audit_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(control_table()))

audit_table = spark.table(control_table())
display(audit_table.orderBy(audit_table.save_timestamp.desc()).limit(20))

# COMMAND ----------

failed = [r for r in audit_rows if r["status"] == "FAILED"]
if failed:
    raise RuntimeError(f"{len(failed)} resource/date combinations failed ingestion — see {control_table()}")

print(f"Raw ingestion complete: {len(audit_rows)} pages written across {len(resources)} resources, "
      f"{len(date_range)} day(s) ({start_date} to {end_date}).")

# COMMAND ----------

