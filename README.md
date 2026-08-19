# Assignment_Databricks

Architecture 

FHIR API
      │  paginated GET, incremental via _lastUpdated
      ▼
RAW        /Volumes/fhir_catalog/raw/landing/{resource}/{date}/page_NNN.json
            (untouched JSON, one file per page, bucketed by resource + date)
      │  parse Bundle.entry, hash content
      ▼
BRONZE      fhir_catalog.bronze.{patient|encounter|observation|condition}
            (Delta, SCD Type 2: valid_from/valid_to/is_current, +metadata)
      │  clean, dedupe, extract typed fields
      ▼
SILVER      fhir_catalog.silver.{patient|encounter|observation|condition}
            (Delta, one current row per resource_id, typed columns)
      │  join, aggregate
      ▼
GOLD        fhir_catalog.gold.patient_summary
            fhir_catalog.gold.encounter_detail
            fhir_catalog.gold.condition_by_patient
            (Delta, reporting-ready — this is what Power BI queries)
            
Every raw API call is also logged to fhir_catalog.raw.pipeline_audit_log (resource, batch_date, page, request URL/params, extraction timestamp, save timestamp, record count, success/failure) — this is the run-level audit trail, independent of the SCD2 row-level history in bronze.

Table relationships (gold layer)
gold.patient_summary — one row per patient, with encounter_count, observation_count, condition_count rolled up.
gold.encounter_detail — one row per encounter, joined to patient_summary via patient_ref (Patient/{id}).
gold.condition_by_patient — one row per condition, joined to patient demographics via the same patient_ref convention.
Silver tables join on FHIR's own reference convention (Encounter.subject.reference = "Patient/<id>"), so no synthetic keys were invented.
