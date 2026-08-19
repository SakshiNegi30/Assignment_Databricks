# Assignment_Databricks

Architecture 

# FHIR Data Engineering Pipeline

## Overview

This project implements an end-to-end **FHIR data ingestion and analytics pipeline** using **HAPI FHIR R4, Azure Databricks, Delta Lake, Unity Catalog, and Power BI**.

The pipeline follows a **Medallion Architecture** with four layers:

**FHIR API → RAW → BRONZE → SILVER → GOLD → Power BI**

The design supports **incremental ingestion, pagination, historical tracking using SCD Type 2, data quality transformations, deduplication, and reporting-ready datasets**.

---

## Architecture

```text
FHIR API 
      │
      │ Paginated GET
      │ Incremental using _lastUpdated
      ▼
┌──────────────────────────────────────────────────────────┐
│ RAW                                                      │
│ /Volumes/fhir_catalog/raw/landing/{resource}/{date}/    │
│                                      page_NNN.json       │
│                                                          │
│ • Untouched JSON                                         │
│ • One file per API page                                  │
│ • Partitioned/bucketed by resource and ingestion date   │
└──────────────────────────────────────────────────────────┘
      │
      │ Parse Bundle.entry
      │ Generate content hash
      ▼
┌──────────────────────────────────────────────────────────┐
│ BRONZE                                                   │
│ fhir_catalog.bronze.patient                              │
│ fhir_catalog.bronze.encounter                            │
│ fhir_catalog.bronze.observation                          │
│ fhir_catalog.bronze.condition                            │
│                                                          │
│ • Delta tables                                           │
│ • SCD Type 2                                             │
│ • valid_from / valid_to / is_current                     │
│ • Ingestion metadata                                     │
└──────────────────────────────────────────────────────────┘
      │
      │ Clean
      │ Deduplicate
      │ Extract typed fields
      ▼
┌──────────────────────────────────────────────────────────┐
│ SILVER                                                   │
│ fhir_catalog.silver.patient                              │
│ fhir_catalog.silver.encounter                            │
│ fhir_catalog.silver.observation                          │
│ fhir_catalog.silver.condition                            │
│                                                          │
│ • Clean and standardized data                            │
│ • Typed columns                                          │
│ • One current row per resource_id                        │
└──────────────────────────────────────────────────────────┘
      │
      │ Join
      │ Aggregate
      ▼
┌──────────────────────────────────────────────────────────┐
│ GOLD                                                     │
│ fhir_catalog.gold.patient_summary                        │
│ fhir_catalog.gold.encounter_detail                       │
│ fhir_catalog.gold.condition_by_patient                   │
│                                                          │
│ • Reporting-ready Delta tables                           │
│ • Business-level aggregations                            │
└──────────────────────────────────────────────────────────┘
      │
      ▼
   Power BI
```

---

## 1. Source — HAPI FHIR R4

The pipeline consumes healthcare data from a **HAPI FHIR R4 API**.

The primary FHIR resources processed are:

* `Patient`
* `Encounter`
* `Observation`
* `Condition`

### Incremental Extraction

The API supports incremental extraction using the `_lastUpdated` parameter.

Example:

```text
GET /Patient?_lastUpdated=ge2026-08-19T00:00:00Z
```

Instead of extracting the complete dataset during every execution, the pipeline retrieves only resources that have been created or updated since the previous successful ingestion.

This reduces:

* API calls
* Network traffic
* Processing time
* Compute cost

---

## 2. Pagination

FHIR APIs can return large result sets. Therefore, the ingestion process handles API pagination.

Each API response is stored as an individual JSON file.

Example:

```text
/Volumes/fhir_catalog/raw/landing/
├── patient/
│   └── 2026-08-20/
│       ├── page_001.json
│       ├── page_002.json
│       └── page_003.json
│
├── encounter/
│   └── 2026-08-20/
│       ├── page_001.json
│       └── page_002.json
│
├── observation/
│   └── 2026-08-20/
│       └── page_001.json
│
└── condition/
    └── 2026-08-20/
        └── page_001.json
```

The raw files are intentionally kept **unchanged** to provide an immutable source copy and support replay or troubleshooting.

---

# 3. RAW Layer

### Storage

```text
/Volumes/fhir_catalog/raw/landing/{resource}/{date}/page_NNN.json
```

### Responsibilities

The RAW layer stores the API response exactly as received.

Key characteristics:

* Original JSON preserved
* No business transformations
* One file per API page
* Organized by resource and ingestion date
* Provides an audit/reprocessing layer

Example:

```text
raw/landing/patient/2026-08-20/page_001.json
```

The raw layer allows the pipeline to reprocess data without making another API request.

---

# 4. BRONZE Layer

The Bronze layer parses the FHIR Bundle and extracts individual resources from:

```text
Bundle.entry
```

Data is stored as Delta tables under:

```text
fhir_catalog.bronze.patient
fhir_catalog.bronze.encounter
fhir_catalog.bronze.observation
fhir_catalog.bronze.condition
```

### Content Hash

A hash is generated from the resource content to identify whether the resource content has changed.

This helps with:

* Change detection
* Deduplication
* SCD Type 2 processing
* Idempotent ingestion

Example metadata:

```text
resource_id
resource_type
resource_json
content_hash
ingestion_timestamp
source_file
valid_from
valid_to
is_current
```

### SCD Type 2

The Bronze layer maintains historical versions of resources using SCD Type 2.

Example:

```text
resource_id | version | valid_from | valid_to | is_current
------------|---------|------------|----------|-----------
P001        | 1       | Jan 01     | Jan 15   | false
P001        | 2       | Jan 15     | NULL     | true
```

When the same FHIR resource is updated, the previous version is closed and a new version is inserted.

This provides complete historical tracking.

---

# 5. SILVER Layer

The Silver layer converts the semi-structured FHIR data into clean, standardized, typed datasets.

Tables:

```text
fhir_catalog.silver.patient
fhir_catalog.silver.encounter
fhir_catalog.silver.observation
fhir_catalog.silver.condition
```

### Transformations

Typical transformations include:

* JSON parsing
* Data type conversion
* Null handling
* Column standardization
* Deduplication
* Data quality validation
* Nested field extraction
* Date/time standardization
* FHIR-specific field extraction

The Silver layer contains **one current row per `resource_id`**.

Example:

```text
Patient
    ↓
resource.id
    ↓
patient_id
```

Nested FHIR attributes are converted into relational columns that are easier for downstream analytics.

---

# 6. GOLD Layer

The Gold layer contains business-oriented datasets designed for analytics and reporting.

### Patient Summary

```text
fhir_catalog.gold.patient_summary
```

Contains patient-level information and relevant aggregated metrics.

Possible attributes include:

```text
patient_id
patient_name
gender
birth_date
total_encounters
total_conditions
last_observation_date
```

### Encounter Detail

```text
fhir_catalog.gold.encounter_detail
```

Combines patient and encounter information for detailed analysis.

### Condition by Patient

```text
fhir_catalog.gold.condition_by_patient
```

Provides patient-level condition information and aggregations.

The Gold layer is optimized for consumption by reporting tools such as **Power BI**.

---

# 7. Data Flow

The complete processing flow is:

```text
HAPI FHIR API
      ↓
Incremental API Extraction
      ↓
Pagination
      ↓
Raw JSON Files
      ↓
Parse Bundle.entry
      ↓
Content Hash
      ↓
Bronze Delta Tables
      ↓
SCD Type 2
      ↓
Clean + Deduplicate
      ↓
Typed Silver Tables
      ↓
Joins + Aggregations
      ↓
Gold Delta Tables
      ↓
Power BI
```

---

# 8. Key Design Features

## Incremental Processing

Uses `_lastUpdated` to avoid repeatedly extracting unchanged FHIR resources.

## Pagination Handling

Large API responses are split into multiple pages and stored independently.

## Immutable RAW Layer

Original API responses are preserved for auditability and reprocessing.

## SCD Type 2

Historical changes to FHIR resources are retained in the Bronze layer.

## Content Hashing

Content hashes help identify changes and prevent unnecessary duplicate processing.

## Delta Lake

Delta tables provide:

* ACID transactions
* Schema enforcement
* Time travel
* Reliable MERGE operations
* Data versioning

## Medallion Architecture

The separation into RAW, Bronze, Silver, and Gold provides clear boundaries between ingestion, historical storage, transformation, and analytics.

## Unity Catalog

Tables are organized using the three-level namespace:

```text
catalog.schema.table
```

Example:

```text
fhir_catalog.silver.patient
```

This provides centralized governance, permissions, and data discovery.

---

# 9. Technology Stack

| Component      | Technology                         |
| -------------- | ---------------------------------- |
| Healthcare API | HAPI FHIR R4                       |
| Cloud Platform | Microsoft Azure                    |
| Processing     | Azure Databricks                   |
| Storage        | Databricks Volumes / Cloud Storage |
| Table Format   | Delta Lake                         |
| Governance     | Unity Catalog                      |
| Transformation | PySpark / Spark SQL                |
| Architecture   | Medallion Architecture             |
| Reporting      | Power BI                           |

---

# 10. Benefits

This architecture provides:

* Scalable FHIR ingestion
* Incremental processing
* Historical data tracking
* Reliable and repeatable pipelines
* Data lineage and auditability
* Structured healthcare datasets
* Separation of ingestion and transformation concerns
* Analytics-ready data for Power BI

---

## End-to-End Summary

The solution extracts FHIR resources incrementally from the HAPI FHIR R4 API, handles API pagination, and stores the untouched responses in the RAW layer. The responses are then parsed into individual resources and stored as historical Delta records in Bronze using SCD Type 2 and content hashing.

The Silver layer cleans, deduplicates, standardizes, and converts the semi-structured FHIR resources into typed datasets. Finally, the Gold layer joins and aggregates the Silver data into reporting-ready datasets consumed by Power BI.

**Overall pattern:**

```text
FHIR API
   ↓
RAW
   ↓
BRONZE + SCD2
   ↓
SILVER
   ↓
GOLD
   ↓
Power BI
```

            
Every raw API call is also logged to fhir_catalog.raw.pipeline_audit_log (resource, batch_date, page, request URL/params, extraction timestamp, save timestamp, record count, success/failure) — this is the run-level audit trail, independent of the SCD2 row-level history in bronze.

Table relationships (gold layer)
gold.patient_summary — one row per patient, with encounter_count, observation_count, condition_count rolled up.
gold.encounter_detail — one row per encounter, joined to patient_summary via patient_ref (Patient/{id}).
gold.condition_by_patient — one row per condition, joined to patient demographics via the same patient_ref convention.
Silver tables join on FHIR's own reference convention (Encounter.subject.reference = "Patient/<id>"), so no synthetic keys were invented.
