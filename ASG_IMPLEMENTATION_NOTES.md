# ASG ContentFlow — Implementation Notes (June 2026)

## Overview

This document summarizes the new implementations and fixes delivered for the ASG Puerto Rico document validation pipeline.

---

## 1. API Callback Executor (Step 5 Integration)

**File:** `contentflow-lib/contentflow/executors/api_callback_executor.py`

### Purpose
Posts aggregated validation results back to the Registros API via the `apply-analysis` endpoint, completing the end-to-end automation loop.

### Flow
1. Receives all document validation results from upstream validators
2. Aggregates pass/fail status across all documents
3. Builds the `apply-analysis` payload with `TramiteGuid`, `StatusId`, `Notes`, and `RejectedRequirements`
4. Uploads the payload JSON to blob storage (`callback-results` container) — **always**, even in dry-run mode
5. If `dry_run=false`: authenticates via JWT and POSTs to the Registros API

### Configuration (Web UI Settings)
| Setting | Description | Default |
|---------|-------------|---------|
| `api_base_url` | Registros API base URL | Required |
| `username` | API authentication username | Required |
| `password` | API authentication password | Required |
| `api_key` | API key for JWT auth | Required |
| `success_status_id` | StatusId when all docs pass | `2` |
| `failure_status_id` | StatusId when any doc fails | `1002` |
| `dry_run` | Skip actual API POST if true | `false` |
| `storage_account_name` | Azure storage account | Auto-detected |
| `container_name` | Blob container for payloads | `callback-results` |

### Payload Structure
```json
{
  "TramiteGuid": "61e6a39d-...",
  "StatusId": 1002,
  "Notes": "Case returned; AI validation found discrepancies in X document(s)...",
  "RejectedRequirements": [
    {
      "TramiteRequirementId": "uuid-of-requirement",
      "RejectionReasonId": 57,
      "RejectionReason": "Your certificate is not current. Please request it again."
    }
  ]
}
```

### Files Modified
- `contentflow-lib/contentflow/executors/api_callback_executor.py` — **NEW**
- `contentflow-lib/contentflow/executors/__init__.py` — added safe import
- `contentflow-lib/executor_catalog.yaml` — added executor entry

---

## 2. Date Validation False-Positive Fix

**File:** `contentflow-lib/contentflow/executors/field_validation_executor.py`

### Problem
All documents were being rejected with `"Expiration or Issue date not found or could not be validated"` (RejectionReasonId 7), even document types that do NOT require date validation (e.g., Policía, Good Standing, Incorporación).

### Root Cause
The `_generate_reject_reasons()` function unconditionally generated a "date not found" rejection when neither `expiration_date` nor `issue_date` was extracted — regardless of whether the document type actually requires a date.

### Fix
Added a `_requires_date` check that references `VALIDATION_FIELDS_BY_FILENAME`. The "date not found" rejection now only fires if the document's required fields include `expiration_date` or `issue_date`.

### Impact
| Document Type | Required Fields | Before Fix | After Fix |
|---|---|---|---|
| Policía/Antecedentes Penales | `ssn_last_four` | ❌ False "date not found" | ✅ No date rejection |
| Good Standing/Existencia | `company_name`, `registration_number` | ❌ False "date not found" | ✅ No date rejection |
| Incorporación | `company_name`, `registration_number` | ❌ False "date not found" | ✅ No date rejection |
| Resolución Corporativa | `company_name` | ❌ False "date not found" | ✅ No date rejection |
| DTRH, CRIM, CFSE, HACIENDA, etc. | includes `issue_date`/`expiration_date` | Correctly rejected | Same — still rejected |

### Result
Callback payload reduced from **13 rejections → 10 rejections** (3 false positives eliminated).

---

## 3. Dry-Run Blob Upload Fix

**File:** `contentflow-lib/contentflow/executors/api_callback_executor.py`

### Problem
When `dry_run=true`, the executor returned early **before** uploading the payload JSON to blob storage. The `callback-results` container remained empty.

### Fix
Moved the `_upload_callback_payload()` call **before** the dry-run early return. Now:
1. Build payload ✅
2. Upload JSON to blob ✅ (always happens)
3. If `dry_run=true` → skip API POST, return early
4. If `dry_run=false` → authenticate & POST to Registros API

---

## 4. Current RejectionReasonId Mapping

| ID | Key | Description | Status |
|---|---|---|---|
| 1 | `company_name_mismatch` | Company name does not match | ⚠️ Placeholder |
| 2 | `ein_ssn_mismatch` | EIN/SSN does not match | ⚠️ Placeholder |
| 3 | `registration_mismatch` | Registration number mismatch | ⚠️ Placeholder |
| 4 | `certificate_mismatch` | Certificate number mismatch | ⚠️ Placeholder |
| 5 | `naics_mismatch` | NAICS code mismatch | ⚠️ Placeholder |
| 6 | `field_mismatch_generic` | Generic field mismatch | ⚠️ Placeholder |
| 7 | `date_not_found` | Date could not be validated | ⚠️ Placeholder |
| 8 | `member_not_found` | Authorized member not found | ⚠️ Placeholder |
| 9 | `outstanding_debt` | Outstanding debt on certificate | ⚠️ Placeholder |
| 10 | `debt_status_unknown` | Debt status undetermined | ⚠️ Placeholder |
| 11 | `asume_non_compliant` | ASUME compliance not marked | ⚠️ Placeholder |
| 12 | `asume_status_unknown` | ASUME compliance undetermined | ⚠️ Placeholder |
| **57** | `certificate_expired` | Certificate expired / not current | ✅ Confirmed |
| **144** | `wrong_document` | Document is not the required one | ✅ Confirmed |

> **Action Required:** Customer must provide official RejectionReasonId mapping from:
> `GET {{ApiBaseUrl}}/document-intelligence/reject-reasons?requirementId={id}`

---

## 5. Remaining Items

| Item | Priority | Details |
|------|----------|---------|
| 5-year tax filing check (SC-6088) | Medium | Validate Planillas cover last 5 tax years |
| Official RejectionReasonId mapping | High | Customer must provide from their DB |
| Set `dry_run=false` | High | Flip once staging API confirmed ready |
| Improve date extraction | Medium | 6 docs still fail on "date not found" — extractor not finding dates |
| Git push | High | All changes committed locally, needs push |

---

## 6. Deployment Notes

### Docker Caching Issue
`azd deploy` may not pick up file changes due to Docker layer caching. If new code isn't reflected in production:

```bash
# Force fresh build with new tag
az acr build --registry creedhbkibz54xe --image contentflow-worker:v22 --file contentflow-worker/Dockerfile . --no-cache

# Update container app to use new image
az containerapp update -n worker-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --image creedhbkibz54xe.azurecr.io/contentflow-worker:v22
```

### Key Resource Names
| Resource | Value |
|----------|-------|
| Subscription | `a5a04507-...` |
| Resource Group | `ASG-RG-AILZ-CONTENTFLOW` |
| Worker Container App | `worker-eedhbkibz54xe` |
| API Container App | `api-eedhbkibz54xe` |
| Storage Account | `steedhbkibz54xe` |
| ACR | `creedhbkibz54xe.azurecr.io` |
| AOAI | `aif-v5phq6yzpn4aq` (deployment: "chat") |

### Pipeline runs on the **WORKER**, not the API.
