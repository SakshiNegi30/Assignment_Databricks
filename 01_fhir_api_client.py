# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01_fhir_api_client
# MAGIC Thin, reusable client around the public HAPI FHIR API.
# MAGIC Handles pagination (`Bundle.link[rel=next]`), retries, and incremental
# MAGIC date filtering via `_lastUpdated`. No resource-specific logic lives here —
# MAGIC it is called generically for Patient / Encounter / Observation / Condition.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import requests
import time
import json
import datetime as dt
from typing import List, Dict, Tuple


def _get_with_retry(url: str, params: dict = None) -> dict:
    """GET with basic retry/backoff. Raises on final failure so the caller
    (and the audit log) can record the failure explicitly rather than
    silently skipping data."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                headers={"Accept": "application/fhir+json"},
                timeout=REQUEST_TIMEOUT_SECS,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - deliberately broad, we retry any failure
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECS * attempt)
    raise RuntimeError(f"GET {url} failed after {MAX_RETRIES} attempts: {last_exc}")


def fetch_resource_pages(
    resource_type: str,
    batch_date: dt.date,
    page_size: int = PAGE_SIZE,
) -> List[Tuple[dict, str, str]]:
    """
    Fetch every page of `resource_type` updated on `batch_date`, following
    the FHIR Bundle's 'next' link for pagination.

    Returns a list of (bundle_json, request_url, extraction_timestamp_iso)
    tuples - one entry per page - so the raw layer can persist each page
    exactly as returned, alongside call metadata.
    """
    day_start = batch_date.isoformat()
    day_end = (batch_date + dt.timedelta(days=1)).isoformat()

    search_url = f"{FHIR_BASE_URL}/{resource_type}"
    params = {
        "_count": page_size,
        INCREMENTAL_PARAM: [f"ge{day_start}", f"lt{day_end}"],
        "_sort": INCREMENTAL_PARAM,
    }

    pages = []
    next_url, next_params = search_url, params

    while next_url:
        extraction_ts = dt.datetime.utcnow().isoformat() + "Z"
        bundle = _get_with_retry(next_url, next_params)

        # Reconstruct the fully-qualified URL actually called, for the metadata column
        called_url = requests.Request("GET", next_url, params=next_params).prepare().url
        pages.append((bundle, called_url, extraction_ts))

        # Pagination: after the first request we must follow the literal
        # 'next' link Bundle returns (it already encodes all params/cursor)
        next_link = next(
            (link["url"] for link in bundle.get("link", []) if link.get("relation") == "next"),
            None,
        )
        next_url, next_params = (next_link, None) if next_link else (None, None)

    return pages


def count_entries(bundle: dict) -> int:
    return len(bundle.get("entry", []))

# COMMAND ----------

