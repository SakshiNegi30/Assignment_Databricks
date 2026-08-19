# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Utils
# MAGIC Central configuration for the FHIR Medallion pipeline.
# MAGIC

# COMMAND ----------

# Widgets let the same notebook be re-run for different windows/resources
# from a Databricks Workflow without editing code.
dbutils.widgets.text("catalog", "fhir_catalog")
dbutils.widgets.text("start_date", "")   # yyyy-mm-dd, blank = auto (today - LOOKBACK_DAYS)
dbutils.widgets.text("end_date", "")     # yyyy-mm-dd, blank = today
dbutils.widgets.text("resource_type", "Patient,Encounter,Observation,Condition")

# COMMAND ----------

import datetime as dt

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"
PAGE_SIZE = 50                       # FHIR "_count" param
REQUEST_TIMEOUT_SECS = 30
MAX_RETRIES = 3
RETRY_BACKOFF_SECS = 2

# Orchestration order requested in the assignment
RESOURCE_ORDER = ["Patient", "Encounter", "Observation", "Condition"]

# Each resource's date field used for incremental extraction (_lastUpdated
# is the FHIR meta field common to all resource types, so we standardize on it)
INCREMENTAL_PARAM = "_lastUpdated"

LOOKBACK_DAYS = 3   # "2-3 days" of incremental data per the assignment

# ---------------------------------------------------------------------------
# Lakehouse layout (Unity Catalog: catalog -> schema -> table)
# ---------------------------------------------------------------------------
CATALOG = dbutils.widgets.get("catalog")

SCHEMA_RAW = "raw"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# Raw layer = landing zone for untouched API payloads, kept as files (not tables),
# bucketed by resource/date as the assignment specifies.
RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA_RAW}/landing"

def raw_path(resource_type: str, batch_date: str) -> str:
    """Folder for one resource/one day's raw API payloads."""
    return f"{RAW_VOLUME}/{resource_type}/{batch_date}"

def bronze_table(resource_type: str) -> str:
    return f"{CATALOG}.{SCHEMA_BRONZE}.{resource_type.lower()}"

def silver_table(resource_type: str) -> str:
    return f"{CATALOG}.{SCHEMA_SILVER}.{resource_type.lower()}"

def gold_table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA_GOLD}.{name.lower()}"

# Pipeline control / audit table (extraction + load metadata, one row per API call)
def control_table() -> str:
    return f"{CATALOG}.{SCHEMA_RAW}.pipeline_audit_log"

# ---------------------------------------------------------------------------
# Date window resolution
# ---------------------------------------------------------------------------
def resolve_date_window():
    end_str = dbutils.widgets.get("end_date").strip()
    start_str = dbutils.widgets.get("start_date").strip()

    end_date = dt.date.fromisoformat(end_str) if end_str else dt.date.today()
    start_date = (
        dt.date.fromisoformat(start_str) if start_str
        else end_date - dt.timedelta(days=LOOKBACK_DAYS - 1)
    )
    return start_date, end_date

def resolve_resource_list():
    raw = dbutils.widgets.get("resource_type").strip()
    resources = [r.strip() for r in raw.split(",") if r.strip()]
    # keep them in the assignment-mandated orchestration order
    return [r for r in RESOURCE_ORDER if r in resources]

# COMMAND ----------

print(f"Catalog          : {CATALOG}")
print(f"Resources        : {resolve_resource_list()}")
print(f"Date window      : {resolve_date_window()}")
print(f"Raw volume       : {RAW_VOLUME}")

# COMMAND ----------

