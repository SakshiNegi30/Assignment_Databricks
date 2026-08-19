# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04_Silver_Layer
# MAGIC Cleans and deduplicates bronze (current-version rows only, per SCD2),
# MAGIC then parses each resource type's clinically-relevant fields out of the
# MAGIC raw FHIR JSON into typed, queryable columns. One notebook, driven by a
# MAGIC per-resource field-mapping dict — no per-resource hardcoded notebooks.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

resources = resolve_resource_list()

# COMMAND ----------

# Resource-specific extraction: FHIR resources don't share a schema, so each
# gets a small set of get_json_object paths. Add a resource here to extend
# the pipeline without touching the loop logic below.
FIELD_MAP = {
    "Patient": {
        "family_name": "$.name[0].family",
        "given_name": "$.name[0].given[0]",
        "gender": "$.gender",
        "birth_date": "$.birthDate",
        "active": "$.active",
    },
    "Encounter": {
        "status": "$.status",
        "class_code": "$.class.code",
        "patient_ref": "$.subject.reference",
        "period_start": "$.period.start",
        "period_end": "$.period.end",
    },
    "Observation": {
        "status": "$.status",
        "code_text": "$.code.text",
        "patient_ref": "$.subject.reference",
        "encounter_ref": "$.encounter.reference",
        "value_quantity": "$.valueQuantity.value",
        "value_unit": "$.valueQuantity.unit",
        "effective_datetime": "$.effectiveDateTime",
    },
    "Condition": {
        "clinical_status": "$.clinicalStatus.coding[0].code",
        "code_text": "$.code.text",
        "patient_ref": "$.subject.reference",
        "onset_datetime": "$.onsetDateTime",
        "recorded_date": "$.recordedDate",
    },
}

# COMMAND ----------

def build_silver(resource_type: str):
    bronze_df = spark.table(bronze_table(resource_type)).filter("is_current = true")

    # Clean + dedupe: drop rows with no resource_id, keep the most recently
    # extracted version per resource_id (defensive - bronze SCD2 should
    # already guarantee one current row per id)
    deduped = (
        bronze_df
        .filter(F.col("resource_id").isNotNull())
        .withColumn(
            "_rn",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"]).Window
                .partitionBy("resource_id")
                .orderBy(F.col("extraction_timestamp").desc())
            ),
        )
        .filter("_rn = 1")
        .drop("_rn")
    )

    parsed = deduped
    for col_name, json_path in FIELD_MAP.get(resource_type, {}).items():
        parsed = parsed.withColumn(col_name, F.get_json_object("resource_json", json_path))

    silver_df = parsed.select(
        "resource_id",
        *FIELD_MAP.get(resource_type, {}).keys(),
        "fhir_last_updated",
        "extraction_timestamp",
        "api_url_or_params",
        "valid_from",
        "record_hash",
    )

    (silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table(resource_type)))

    return silver_df.count()

# COMMAND ----------

for resource_type in resources:
    n = build_silver(resource_type)
    print(f"[{resource_type}] silver: {n} deduplicated, cleaned rows -> {silver_table(resource_type)}")

# COMMAND ----------

for resource_type in resources:
    display(spark.table(silver_table(resource_type)).limit(5))