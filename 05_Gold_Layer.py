# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05_Gold_Layer
# MAGIC Final warehouse layer: denormalized, reporting-optimized views built from
# MAGIC silver tables. These are what Power BI / analysts query — never the raw
# MAGIC or bronze layers.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.patient_summary
# MAGIC One row per patient with encounter, observation, and condition counts.

# COMMAND ----------

patient = spark.table(silver_table("Patient")).withColumn(
    "patient_ref", F.concat(F.lit("Patient/"), F.col("resource_id"))
)
encounter = spark.table(silver_table("Encounter"))
observation = spark.table(silver_table("Observation"))
condition = spark.table(silver_table("Condition"))

enc_counts = encounter.groupBy("patient_ref").agg(F.count("*").alias("encounter_count"))
obs_counts = observation.groupBy("patient_ref").agg(F.count("*").alias("observation_count"))
cond_counts = condition.groupBy("patient_ref").agg(F.count("*").alias("condition_count"))

patient_summary = (
    patient
    .join(enc_counts, "patient_ref", "left")
    .join(obs_counts, "patient_ref", "left")
    .join(cond_counts, "patient_ref", "left")
    .select(
        F.col("resource_id").alias("patient_id"),
        "family_name", "given_name", "gender", "birth_date", "active",
        F.coalesce("encounter_count", F.lit(0)).alias("encounter_count"),
        F.coalesce("observation_count", F.lit(0)).alias("observation_count"),
        F.coalesce("condition_count", F.lit(0)).alias("condition_count"),
    )
)

(patient_summary.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(gold_table("patient_summary")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.encounter_detail
# MAGIC Encounter grain, joined back to patient demographics.

# COMMAND ----------

encounter_detail = (
    encounter.alias("e")
    .join(
        patient.select(
            F.col("patient_ref"),
            F.col("family_name"), F.col("given_name"), F.col("gender"),
        ).alias("p"),
        "patient_ref", "left",
    )
    .select(
        F.col("e.resource_id").alias("encounter_id"),
        "e.status", "e.class_code", "e.period_start", "e.period_end",
        "e.patient_ref", "p.family_name", "p.given_name", "p.gender",
    )
)

(encounter_detail.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(gold_table("encounter_detail")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold.condition_by_patient
# MAGIC Active/inactive condition list per patient for clinical reporting.

# COMMAND ----------

condition_by_patient = (
    condition.alias("c")
    .join(
        patient.select("patient_ref", "family_name", "given_name").alias("p"),
        "patient_ref", "left",
    )
    .select(
        F.col("c.resource_id").alias("condition_id"),
        "c.patient_ref", "p.family_name", "p.given_name",
        "c.code_text", "c.clinical_status", "c.onset_datetime", "c.recorded_date",
    )
)

(condition_by_patient.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(gold_table("condition_by_patient")))

# COMMAND ----------

print("Gold layer built:")
for t in ["patient_summary", "encounter_detail", "condition_by_patient"]:
    full_name = gold_table(t)
    print(f"  {full_name}: {spark.table(full_name).count()} rows")