# ASG Puerto Rico – ContentFlow Document Validation Pipeline
## Project Handover Document v1.0

**Project Name:** ASG AILZ ContentFlow  
**Client:** Administración de Servicios Generales (ASG), Puerto Rico  
**Date:** June 8, 2026  
**Prepared By:** Ratul Ghosh  
**Repository:** [https://github.com/ratul442/ASG_AILZ_CONTENTFLOW](https://github.com/ratul442/ASG_AILZ_CONTENTFLOW)  
**Branch:** `feature/asg-executors`

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Azure Infrastructure](#3-azure-infrastructure)
4. [Codebase Structure](#4-codebase-structure)
5. [Custom Executors (ASG-Specific)](#5-custom-executors-asg-specific)
6. [Pipeline Configuration](#6-pipeline-configuration)
7. [Deployment Guide](#7-deployment-guide)
8. [Configuration & Settings](#8-configuration--settings)
9. [System Health & Monitoring](#9-system-health--monitoring)
10. [Testing & Validation](#10-testing--validation)
11. [Known Issues & Resolutions](#11-known-issues--resolutions)
12. [Future Enhancements](#12-future-enhancements)

---

## 1. Project Overview

### 1.1 Purpose

The ASG Puerto Rico ContentFlow project automates the validation of government-issued certificates submitted by vendors seeking to do business with the Puerto Rico government. The system processes 19 document types per vendor application (tramite), extracting key fields using Azure Document Intelligence, comparing them against API-provided reference data, and validating certificates against live government portals using browser automation.

### 1.2 Scope

- **Input:** PDF documents per tramite (vendor application), fetched from the ASG API. Document count varies per tramite.
- **Processing:** OCR extraction → field extraction → field validation → browser-based portal validation
- **Output:** Per-document validation reports uploaded to Azure Blob Storage (`validation-results` container)

#### Tramite 1 — `61e6a39d-b36c-43e0-a624-d421d52bfea8` (19 documents)

| Index | Filename | Document Type |
|-------|----------|---------------|
| _00 | ITDG-Company-Profile.pdf | Company Profile (excluded) |
| _01 | ITDG-Declaracion-Jurada.pdf | Declaración Jurada (excluded) |
| _02 | ITDG-Resolucion-Corporativa.pdf | Resolución Corporativa (optional) |
| _03 | ITDG-Entity-Information-SAM-Exp-7-8-2026.pdf | SAM Entity Information |
| _04 | ITDG-Certificado-Negativo-de-Antecedentes-Penales.pdf | Antecedentes Penales (Policía) |
| _05 | SC-6088 Radicación Planillas Ingresos | Hacienda – Radicación Planillas |
| _06 | SC-6096 Certificación de Deuda Hacienda | Hacienda – Deuda |
| _07 | SC-2942 Planillas IVU | Hacienda – IVU |
| _08 | Registro de Comerciante | Merchant Registration |
| _09 | Hacienda (other) | Hacienda Certificate |
| _10 | DTRH Desempleo/Incapacidad | DTRH – Seguro Desempleo |
| _11 | Choferil | Seguro Choferil |
| _12 | CFSE Certificado de Deuda | Fondo del Seguro del Estado |
| _13 | DTRH Seguro | DTRH – Seguro |
| _14 | Certificado de Incorporación | Incorporación |
| _15 | Good Standing / Estado | Certificado de Existencia |
| _16 | ITDG-ASUME-Certificacion-Patronal-de-Cumplimiento-06-13-2026.pdf | ASUME |
| _17 | DTRH | DTRH Certificate |
| _18 | ITDG-CRIM-Certificacion.pdf | CRIM |

#### Tramite 2 — `3ee2e341-a5f4-4123-8b12-75870a7735f6` (15 documents)
*Canonical prefix: `3456413e-0975-4e43-9d6a-9344b674c9ca`*

| Index | Filename | Document Type |
|-------|----------|---------------|
| _00 | ITDG-Company-Profile.pdf | Company Profile (excluded) |
| _01 | ITDG-Declaracion-Jurada-ASG-633-2025.pdf | Declaración Jurada (excluded) |
| _02 | ITDG-Resolucion-Corporativa-ASG-674-2025.pdf | Resolución Corporativa (optional) |
| _03 | ITDG-Entity-Information-SAM-Exp-7-23-2025.pdf | SAM Entity Information |
| _04 | ITDG-Certificado-Negativo-de-Antecedentes-Penales_RNegron-y-MCr... | Antecedentes Penales (Policía) |
| _05 | ITDG-Certificado-de-Incorporacion.pdf | Certificado de Incorporación |
| _06 | ESTADO_Good-Standing_20250522035404.pdf | Good Standing / Estado |
| _07 | HACIENDA_Certificación-de-Radicación-de-Planillas-de-Contribución... | Hacienda – Radicación Planillas |
| _08 | HACIENDA_Certificación-De-Deuda-(SC-6096)_20250522031906.pdf | Hacienda – Deuda (SC-6096) |
| _09 | ITDG-Certificacion-de-Registro-de-Comerciante-Radicacion-de-Planill... | Merchant Registration + Radicación |
| _10 | DTRH_Certificación-de-Registro-como-Patrono-y-de-Deuda-por-Concep... | DTRH – Registro Patrono / Deuda |
| _11 | DTRH_Certificación-de-Registro-como-Patrono-y-de-Deuda-por-Concep... | DTRH – Registro Patrono / Deuda (2) |
| _12 | CFSE_Certificado-de-Deuda_20250522031904.pdf | CFSE – Certificado de Deuda |
| _13 | ITDG-ASUME-Certificacion-Patronal-de-Cumplimiento-06-21-2025.pdf | ASUME |
| _14 | ITDG-CRIM-Certificacion-de-Todos-los-Conceptos-Radicacion-Planilla... | CRIM |

> **Note:** Tramite 2 has 15 documents vs Tramite 1's 19. Document ordering differs — e.g., Incorporación is _05 (not _14), Good Standing is _06 (not _15). The system uses **filename-based classification** (not index-based) to handle these variations.

### 1.3 Technology Stack

| Component | Technology |
|-----------|-----------|
| **Platform** | ContentFlow v0.1.2 (Microsoft Agent Framework) |
| **Language** | Python 3.13, TypeScript 5.8+ |
| **API Framework** | FastAPI 0.128.0 |
| **Container Runtime** | Azure Container Apps |
| **Document AI** | Azure Document Intelligence (prebuilt-layout) |
| **LLM** | Azure OpenAI (gpt-5-nano / gpt-4.1-mini) |
| **Browser Automation** | Playwright (Chromium headless) |
| **Storage** | Azure Blob Storage, Azure Cosmos DB |
| **Infrastructure** | Bicep (Azure AI Landing Zone integrated) |
| **CI/CD** | Azure Developer CLI (`azd`) |

---

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Azure AI Landing Zone (AILZ)                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │         Container Apps Environment (Internal)              │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │  │
│  │  │  API Service │  │   Worker    │  │  Web (React) │       │  │
│  │  │  (FastAPI)   │  │  Service    │  │   Frontend   │       │  │
│  │  │  *Runs       │  │             │  │              │       │  │
│  │  │  Pipeline*   │  │             │  │              │       │  │
│  │  └──────┬───────┘  └─────────────┘  └──────────────┘       │  │
│  │         │                                                   │  │
│  │    ┌────▼────┐   ┌──────────┐  ┌───────────┐               │  │
│  │    │ Storage │   │ Cosmos   │  │   AOAI    │               │  │
│  │    │ Account │   │   DB     │  │ (gpt-5)  │               │  │
│  │    └─────────┘   └──────────┘  └───────────┘               │  │
│  └───────────────────────────────────────────────────────────┘  │
│         ▲                                                        │
│  ┌──────┴──────┐  ┌────────────────┐                            │
│  │  JumpBox VM │  │  NAT Gateway   │ → Outbound to portals     │
│  │  (deploy)   │  │                │                            │
│  └─────────────┘  └────────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Pipeline Execution Flow

```
ASG API (tramite data)
    │
    ▼
api_fetch_executor ─── Fetches vendor data + document list from ASG API
    │
    ▼
blob_file_copy_executor ─── Copies PDFs from source to processing blob
    │
    ▼
blob_output_executor ─── Stores raw documents
    │
    ▼
content_retriever ─── Retrieves document content for processing
    │
    ▼
DI_extractor ─── Azure Document Intelligence (OCR + layout)
    │
    ▼
field_extractor_executor ─── Regex + table + AI field extraction
    │
    ▼
field_validation_executor ─── Compares extracted fields vs API data
    │
    ▼
browser_validation_executor ─── Validates certs against live portals
    │
    ▼
blob_output_executor ─── Uploads final validation reports
```

#### Step-by-Step Explanation

1. **API Fetch (`api_fetch_executor`):**  
   The pipeline begins by calling the ASG (Administración de Servicios Generales) REST API with the provided TramiteGuid. This returns the full tramite record — including the company name, EIN/SSN, NAICS codes, authorized members, and a list of all submitted document URLs. Each document is represented as a `Content` item with a canonical ID (e.g., `61e6a39d-..._03`) and the source filename. The API data is stored in `content.data` and travels with each document through the rest of the pipeline, so downstream executors can compare extracted fields against it.

2. **Blob File Copy (`blob_file_copy_executor`):**  
   The executor downloads each PDF from the source URL provided by the ASG API and uploads it to the `content` container in Azure Blob Storage (`steedhbkibz54xe`). This ensures all documents are stored in the project's own storage account for reliable, repeatable processing — even if the source API becomes unavailable later.

3. **Blob Output (`blob_output_executor`):**  
   Persists the raw document metadata and blob references. This serves as a checkpoint — if the pipeline is re-run, documents don't need to be re-fetched.

4. **Content Retriever (`content_retriever`):**  
   Retrieves the document binary content from blob storage and loads it into memory so downstream executors (Document Intelligence, field extraction) can process it.

5. **Document Intelligence Extractor (`DI_extractor`):**  
   Sends each PDF to Azure Document Intelligence using the `prebuilt-layout` model. This performs OCR on scanned documents and extracts structured output including:
   - **Markdown text** — The full document text with headings, paragraphs, and inline structure
   - **Tables** — Structured table data with rows, columns, headers, and cell values
   - **Key-value pairs** — Detected form fields  
   
   The output is stored in `content.data["doc_intelligence_output"]` with keys `text` (markdown) and `tables` (list of table objects).

6. **Field Extractor (`field_extractor_executor`):**  
   This is the first ASG-custom executor. It takes the Document Intelligence markdown and tables and extracts specific business fields needed for validation. It uses a **multi-strategy approach** in priority order:
   - **Regex patterns** (14+ patterns) scan the markdown text for fields like `company_name`, `ein_ssn`, `issue_date`, `expiration_date`, `certificate_number`, `merchant_registration`, `unique_entity_id`, etc.
   - **Table extraction** scans DI tables using `TABLE_KEY_MAPPING` with Spanish/English key names (e.g., "Nombre del Patrono" → `company_name`, "Número Patronal Federal" → `ein_ssn`). Two scanning strategies handle different table layouts: header→row1 (for CRIM-style tables) and adjacent-cell (for SAM/ASUME where the label and value sit in neighboring columns).
   - **AI extraction** (Azure OpenAI fallback) — If regex and table scanning miss critical fields, the full markdown text is sent to gpt-5-nano with a structured prompt asking it to extract the same fields. The LLM returns JSON which is merged with regex/table results.
   - **Spanish month normalization** converts month names like "Ago", "Dic", "Enero" to standard numeric equivalents for date parsing.
   
   The extracted fields are saved as a JSON blob to the `json-fields` container and stored in `content.data["extracted_fields"]`.

7. **Field Validation (`field_validation_executor`):**  
   The second ASG-custom executor. It compares the extracted fields against the reference API data that was fetched in step 1. For each document:
   - **Document classification:** The executor uses filename regex patterns (not index) to determine which fields to validate for this document type. For example, a SAM document validates `company_name` + `unique_entity_id`, while a Hacienda SC-6088 validates `company_name` + `ein_ssn`.
   - **Excluded documents:** _00 (Company Profile) and _01 (Declaración Jurada) are skipped with `status: "excluded"`.
   - **Field comparison:** Each field is compared against the corresponding API value using string similarity (`SequenceMatcher`, threshold 0.65). Special handling exists for EIN/SSN (full match or last-4-digit match), NAICS codes (prefix matching), and registration numbers (digits-only comparison).
   - **Date validation (3-step logic):**
     - If an `expiration_date` is present → check whether it's expired
     - If only an `issue_date` is present → check against a validity window (30 days for SC-* Hacienda certs, 90 days for CRIM/DTRH/ASUME/SAM)
     - If neither date found → flag as "date not found"
   - **Member verification:** For Policía (Antecedentes Penales) documents, the executor checks whether all authorized members from the API data appear in the document text.
   - **Reject reasons:** Generated in Spanish for any mismatches, expired dates, or missing members.
   - **Error resilience:** A `_safe_process` wrapper catches any unexpected exception and still uploads an error report — ensuring every document always produces a validation report.
   
   The validation report JSON is uploaded to the `validation-results` container at path `validation_reports/YYYY/MM/DD/<canonical_id>.json`.

8. **Browser Validation (`browser_validation_executor`):**  
   The third ASG-custom executor. It uses Playwright (headless Chromium) to validate certificates against live Puerto Rico government web portals. For each document, the executor:
   - **Matches the filename** to a portal using `FILENAME_PORTAL_PATTERNS` (e.g., "SC-6088" → Hacienda/SURI, "CRIM" → CRIM portal, "Antecedentes Penales" → Validación PR).
   - **Launches a browser**, navigates to the portal, fills in the extracted fields (certificate number, EIN, etc.), submits the form, and reads the portal's response.
   - **Captures the validation result** — whether the portal confirms the certificate is valid, expired, or not found.
   - Supported portals: **Hacienda SURI** (`suri.hacienda.pr.gov`), **CRIM** (`portal.crim360.com`), **Validación PR** (`validacion.pr.gov`), and **SAM.gov** (`sam.gov`).
   
   The browser validation report is uploaded to the same `validation-results` container alongside the field validation report.

9. **Final Blob Output (`blob_output_executor`):**  
   The final executor persists all pipeline results — extracted fields, validation reports, and browser validation reports — to blob storage as the definitive output of the pipeline run.

### 2.3 Key Design Decision

> **The pipeline runs on the API service (`api-eedhbkibz54xe`), NOT the worker service.** This is because the pipeline is triggered via the Web UI which calls the API endpoint directly.

---

## 3. Azure Infrastructure

### 3.1 Resource Details

| Resource | Name | Type |
|----------|------|------|
| **Subscription** | `a5a04507-0bb9-45a4-a020-9db842709c8a` | Azure Subscription |
| **Resource Group** | `ASG-RG-AILZ-CONTENTFLOW` | Resource Group (East US) |
| **Container Apps Env** | `cae-v5phq6yzpn4aq` | Internal (VNet-integrated) |
| **API Container App** | `api-eedhbkibz54xe` | Runs pipeline execution |
| **Worker Container App** | `worker-eedhbkibz54xe` | Background processing |
| **Web Container App** | (web frontend) | React UI |
| **Storage Account** | `steedhbkibz54xe` | Blob + Queue storage |
| **Cosmos DB** | `cosmos-eedhbkibz54xe` | Pipeline metadata & config |
| **Azure OpenAI** | `aif-v5phq6yzpn4aq` | LLM for field extraction |
| **Container Registry** | `creedhbkibz54xe.azurecr.io` | Docker images |
| **NAT Gateway** | `ng-v5phq6yzpn4aq` | Outbound internet (portals) |
| **Managed Identity** | Principal: `3d8d3494-cc2a-41aa-9106-a0b174e55a43` | Authentication |

### 3.2 Storage Containers

| Container | Purpose |
|-----------|---------|
| `content` | Raw document content |
| `extraction` | Document Intelligence output |
| `json-fields` | Extracted field JSON files |
| `validation-results` | Validation reports (field + browser) |

### 3.3 Cosmos DB

**Account:** `cosmos-eedhbkibz54xe`  
**Database:** `contentflow`  
**API:** NoSQL (Core SQL)

| Container | Purpose |
|-----------|---------|
| `pipelines` | Pipeline definitions — stores the YAML/JSON pipeline configurations created via the Web UI. Each document represents a pipeline with its executor sequence, settings, and metadata. |
| `pipeline_executions` | Pipeline execution history — logs every pipeline run with status, start/end times, input parameters (e.g., TramiteGuid), and per-executor results. Used for tracking and debugging. |
| `executor_catalog` | Executor registry — stores the catalog of available executors (field_extractor, field_validation, browser_validation, etc.) with their settings schemas and descriptions. |
| `vaults` | Vault definitions — stores vault configurations for secure credential/secret management used by executors. |
| `vault_executions` | Vault execution tracking — logs vault-related execution history. |
| `vault_exec_locks` | Distributed locks — prevents concurrent vault executions from conflicting. Uses TTL-based locks (default 300s). |
| `vault_crawl_checkpoints` | Crawl checkpoints — stores progress markers for source workers that crawl/poll external data sources. |

### 3.4 AOAI Configuration

| Setting | Value |
|---------|-------|
| **Endpoint** | `https://aif-v5phq6yzpn4aq.cognitiveservices.azure.com/` |
| **Deployment** | `chat` (gpt-5-nano) |
| **API Version** | `2024-12-01-preview` |
| **RBAC Roles** | Cognitive Services User + Cognitive Services OpenAI User |

### 3.5 RBAC / Identity

The Managed Identity (`3d8d3494-cc2a-41aa-9106-a0b174e55a43`) has the following role assignments:

- **Storage Blob Data Contributor** on `steedhbkibz54xe`
- **Cognitive Services User** on `aif-v5phq6yzpn4aq`
- **Cognitive Services OpenAI User** on `aif-v5phq6yzpn4aq`
- **AcrPull** on `creedhbkibz54xe`

---

## 4. Codebase Structure

```
contentflow/
├── azure.yaml                    # azd deployment config
├── contentflow-api/              # FastAPI API service
│   ├── Dockerfile
│   ├── main.py
│   ├── requirements.txt
│   └── app/
│       ├── routers/              # API endpoints (health, pipelines, executors)
│       ├── services/             # Business logic
│       └── settings.py
├── contentflow-worker/           # Background worker service
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── contentflow-lib/              # Shared library (executors, models, connectors)
│   ├── pyproject.toml
│   ├── contentflow/
│   │   ├── executors/            # ← ASG custom executors live here
│   │   ├── models/
│   │   ├── connectors/
│   │   ├── pipeline/
│   │   └── utils/
│   └── samples/
├── contentflow-web/              # React + TypeScript frontend
│   ├── src/
│   └── package.json
└── infra/
    └── bicep/                    # Infrastructure as Code
        ├── main.bicep
        └── modules/
```

### 4.1 Key Files Modified for ASG

| File | Description |
|------|-------------|
| `contentflow-lib/contentflow/executors/field_extractor_executor.py` | Custom field extraction with regex, table scanning, Spanish months, AI fallback |
| `contentflow-lib/contentflow/executors/field_validation_executor.py` | Validates extracted fields against ASG API data, date validation, member verification |
| `contentflow-lib/contentflow/executors/browser_validation_executor.py` | Playwright-based validation against Hacienda, CRIM, Validación PR, SAM.gov portals |
| `contentflow-lib/contentflow/executors/api_fetch_executor.py` | Fetches tramite data from ASG API |
| `contentflow-lib/contentflow/executors/azure_blob_file_copy_executor.py` | Copies document PDFs between blob containers |

---

## 5. Custom Executors (ASG-Specific)

### 5.1 Field Extractor Executor (`field_extractor_executor.py`)

**Purpose:** Extracts structured fields from Document Intelligence markdown output.

**Extraction Strategies (in priority order):**
1. **Regex patterns** — 14+ regex patterns for company_name, EIN, dates, registration numbers, etc.
2. **Table extraction** — Scans DI tables for key-value pairs using `TABLE_KEY_MAPPING` with Spanish/English keys
3. **Multi-column table scanning** — Two strategies:
   - Strategy 1: Header row → first data row (for CRIM-style tables)
   - Strategy 2: Adjacent cells — label in one column, value in next (for SAM, ASUME)
4. **AI extraction** — Azure OpenAI fallback for fields not found by regex/tables

**Key Features:**
- Spanish month normalization (Ene→Jan, Ago→Aug, etc.)
- `COMPANY_HEADERS` with `el\s*patrono|the\s*employer` patterns
- Fuzzy key matching for date/company/EIN patterns in tables
- Uploads extracted JSON to `json-fields` blob container

**Fields Extracted:**
`document_title`, `company_name`, `ein_ssn`, `issue_date`, `expiration_date`, `certificate_number`, `registration_number`, `merchant_registration`, `naics_code`, `agent_type`, `unique_entity_id`, `ssn_last_four`, `page_count`

### 5.2 Field Validation Executor (`field_validation_executor.py`)

**Purpose:** Compares extracted fields against ASG API reference data.

**Key Features:**
- **Filename-based classification** — Regex patterns match document filenames to determine which fields to validate
- **Excluded documents** — _00 (Company Profile), _01 (Declaración Jurada) produce `status: "excluded"` reports
- **Optional documents** — _02 (Resolución Corporativa) validated but non-determinative
- **Similarity scoring** — String similarity using `SequenceMatcher` with configurable threshold (default: 0.65)
- **EIN/SSN matching** — Full match and last-4-digit match support
- **NAICS code matching** — Prefix-based matching against API code list
- **Date validation (3-step logic):**
  1. If `expiration_date` present → check if expired
  2. If `issue_date` present → check against validity window (30 days for SC-*, 90 days for others)
  3. If neither found → flag as "date not found"
- **Spanish date parsing** — Handles "22 de mayo de 2025", abbreviated months (Ago, Dic, etc.)
- **Member verification** — For Policía documents, verifies authorized members appear in document text
- **Reject reasons** — Generates structured reject reasons in Spanish
- **Error resilience** — `_safe_process` wrapper catches all exceptions, uploads error reports

**Validation Field Mapping per Document Type:**

| Document | Fields Validated |
|----------|-----------------|
| SAM (_03) | `company_name`, `unique_entity_id` |
| Policía (_04) | `company_name`, `ssn_last_four` |
| Hacienda SC-6088 (_05) | `company_name`, `ein_ssn` |
| Hacienda SC-6096 (_06) | `company_name`, `ein_ssn` |
| IVU SC-2942 (_07) | `company_name`, `ein_ssn` |
| Merchant Registration (_08) | `company_name`, `ein_ssn`, `merchant_registration` |
| ASUME (_16) | `company_name` |
| CRIM (_18) | `company_name` |

### 5.3 Browser Validation Executor (`browser_validation_executor.py`)

**Purpose:** Validates certificates against live government web portals using Playwright browser automation.

**Supported Portals:**

| Portal | URL | Documents |
|--------|-----|-----------|
| **Hacienda (SURI)** | `https://suri.hacienda.pr.gov/` | SC-6088, SC-6096, SC-2942, Merchant Registration |
| **CRIM** | `https://portal.crim360.com/certificados` | CRIM certificates |
| **Validación PR** | `https://validacion.pr.gov/` | Policía, DTRH, Choferil, ASUME |
| **SAM.gov** | `https://sam.gov/` | SAM Entity Information |

**Key Features:**
- Filename-based portal matching (`FILENAME_PORTAL_PATTERNS`)
- Headless Chromium via Playwright
- Screenshot capture (configurable)
- Timeout handling (default: 20 seconds per portal)
- Reports uploaded to `validation-results` blob container

**Configuration:**
- `enabled_portals`: Comma-separated list (e.g., `hacienda,crim,validacion_pr`)
- `timeout_seconds`: Per-portal timeout
- `headless`: Browser mode (default: true)

---

## 6. Pipeline Configuration

The pipeline is configured via the **ContentFlow Web UI** and stored in the platform's database. The pipeline sequence is:

```yaml
Pipeline: ASG Document Validation
TramiteGuid: 61e6a39d-b36c-43e0-a624-d421d52bfea8

Executor Sequence:
  1. api_fetch_executor          # Fetch tramite data from ASG API
  2. blob_file_copy_executor     # Copy PDFs to processing container
  3. blob_output_executor        # Store raw documents
  4. content_retriever           # Retrieve document content
  5. DI_extractor                # Azure Document Intelligence OCR
  6. field_extractor_executor    # Extract fields from markdown
  7. field_validation_executor   # Validate fields against API
  8. browser_validation_executor # Validate against live portals
  9. blob_output_executor        # Upload final reports
```

### 6.1 Key Pipeline Settings

| Setting | Value |
|---------|-------|
| `storage_account_name` | `steedhbkibz54xe` |
| `aoai_endpoint` | `https://aif-v5phq6yzpn4aq.cognitiveservices.azure.com/` |
| `aoai_deployment` | `chat` |
| `enabled_portals` | `hacienda,crim,validacion_pr` |
| `similarity_threshold` | `0.65` |
| `container_name` (validation) | `validation-results` |
| `container_name` (fields) | `json-fields` |

---

## 7. Deployment Guide

ContentFlow supports two deployment modes. The ASG project uses **AILZ-Integrated Mode** (private endpoints).

### 7.1 Deployment Modes

| Feature | Basic Mode | AILZ-Integrated Mode (ASG) |
|---------|-----------|---------------------------|
| **Network** | Public endpoints | Private endpoints |
| **VNet** | None | Existing AILZ VNet |
| **DNS** | Public DNS | Private DNS Zones |
| **Security** | Development | Enterprise / Compliance |
| **Access** | Direct internet | JumpBox VM via Bastion |

### 7.2 Prerequisites

- **Azure CLI** (`az`) — installed and authenticated
- **Azure Developer CLI** (`azd`) — installed and authenticated
- **JumpBox VM** — access via Azure Bastion in the AILZ VNet
- **Git** — for cloning/updating the repo on the JumpBox
- **Python 3.12+** — for local testing (optional)

### 7.3 Initial AILZ Deployment (First Time)

This was already completed for the ASG project. Documented here for reference.

#### Step 1: Set up AI Landing Zone

The AILZ must exist before deploying ContentFlow. It provides:
- Virtual Network with subnets (`pe-subnet` for private endpoints, `aca-env-subnet` for Container Apps)
- Private DNS Zones (blob, cosmos, ACR, app config, cognitive services, container apps)
- JumpBox VM for secure deployment
- NAT Gateway for outbound internet

#### Step 2: Gather AILZ Resource IDs

```bash
# On your local machine or JumpBox
cd contentflow/infra/scripts
./get-ailz-resources.sh --auto-set
```

This auto-discovers VNet, subnets, DNS zones, and JumpBox, then sets `azd` environment variables.

#### Step 3: Configure azd Environment

```bash
azd auth login
azd env new prod-ailz
azd env set DEPLOYMENT_MODE ailz-integrated

# Resource IDs (auto-set by script, or set manually):
azd env set EXISTING_VNET_RESOURCE_ID "/subscriptions/a5a04507-.../virtualNetworks/<vnet>"
azd env set PRIVATE_ENDPOINT_SUBNET_NAME "pe-subnet"
azd env set CONTAINER_APPS_SUBNET_NAME "aca-env-subnet"
azd env set EXISTING_BLOB_PRIVATE_DNS_ZONE_ID "/subscriptions/.../privatelink.blob.core.windows.net"
azd env set EXISTING_COSMOS_PRIVATE_DNS_ZONE_ID "/subscriptions/.../privatelink.documents.azure.com"
azd env set EXISTING_ACR_PRIVATE_DNS_ZONE_ID "/subscriptions/.../privatelink.azurecr.io"
azd env set EXISTING_APP_CONFIG_PRIVATE_DNS_ZONE_ID "/subscriptions/.../privatelink.azconfig.io"
azd env set EXISTING_COGNITIVE_SERVICES_PRIVATE_DNS_ZONE_ID "/subscriptions/.../privatelink.cognitiveservices.azure.com"
azd env set EXISTING_CONTAINER_APPS_ENV_PRIVATE_DNS_ZONE_ID "/subscriptions/.../privatelink.azurecontainerapps.io"
```

#### Step 4: Provision Infrastructure + Deploy Services

```bash
azd up
```

This single command:
1. Provisions all Azure resources (Container Apps, Storage, Cosmos DB, ACR, App Config, etc.)
2. Creates private endpoints and DNS records
3. Builds Docker images and pushes to ACR
4. Deploys Container Apps (API, Worker, Web)
5. Seeds App Configuration with settings
6. Assigns RBAC roles to the Managed Identity

**Duration:** 10–15 minutes

### 7.4 Updating Code (Day-to-Day Deployments)

This is the most common operation — deploying code changes after modifying executors.

#### From JumpBox VM (`C:\contentflow`):

```powershell
# 1. Navigate to project root
cd C:\contentflow

# 2. Ensure updated code is on the JumpBox
#    (copy files from local machine, or git pull)

# 3. Verify the fix is present (example check)
Select-String -Path "contentflow-lib\contentflow\executors\field_validation_executor.py" -Pattern "_generate_reject_reasons"

# 4. Deploy all services
azd deploy

# 5. Verify the new revision is running
az containerapp show -n api-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --query "properties.latestRevisionName" -o tsv
```

> **⚠️ Critical:** Code is edited locally (VS Code) but deployed from the JumpBox. **Always copy updated files to the JumpBox** before running `azd deploy`.

#### What `azd deploy` Does

1. Sends source code + Dockerfile to ACR for remote build
2. ACR builds the Docker image using `COPY contentflow-lib` + `pip install -e .`
3. Pushes the new image to ACR
4. Creates a new Container App revision with the new image
5. Routes traffic to the new revision

### 7.5 Forcing Fresh Builds (Cache Busting)

If `azd deploy` uses cached Docker layers and doesn't pick up code changes:

```powershell
# Option 1: Manual ACR build with explicit tag
az acr build --registry creedhbkibz54xe --image contentflow-api:v2 --file contentflow-api/Dockerfile .

# Then update the container app to use the new image
az containerapp update -n api-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --image creedhbkibz54xe.azurecr.io/contentflow-api:v2

# Option 2: Add a cache-bust comment to the Dockerfile
# Add/change a comment line before `COPY contentflow-lib` to invalidate the layer cache
```

### 7.6 Deploying Individual Services

```powershell
# Deploy only the API service (most common — executors run here)
azd deploy api

# Deploy only the Worker service
azd deploy worker

# Deploy only the Web frontend
azd deploy web
```

### 7.7 Checking Logs

```powershell
# API service logs (where pipeline runs)
az containerapp logs show -n api-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --type console --follow

# Filter for specific executor
az containerapp logs show -n api-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --type console | Select-String "field_validation"

# Worker service logs
az containerapp logs show -n worker-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --type console --follow
```

### 7.8 Rollback

If a deployment introduces issues, revert to the previous revision:

```powershell
# List revisions
az containerapp revision list -n api-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW -o table

# Activate a previous revision
az containerapp revision activate -n api-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --revision <previous-revision-name>

# Route all traffic to it
az containerapp ingress traffic set -n api-eedhbkibz54xe -g ASG-RG-AILZ-CONTENTFLOW --revision-weight <previous-revision-name>=100
```

### 7.9 Full Re-Provision (Nuclear Option)

If infrastructure needs to be recreated:

```powershell
# Tear down everything
azd down --force --purge

# Re-provision + deploy
azd up
```

> **⚠️ Warning:** This destroys all resources including Cosmos DB data, blob storage, and pipeline configs. Use only as a last resort.

---

## 8. Configuration & Settings

### 8.1 Docker Base Images

Both API and Worker Dockerfiles use `python:3.13-slim` from Docker Hub. Rate limits may apply.

**API Dockerfile key steps:**
1. Install system deps (Playwright/Chromium dependencies)
2. `COPY contentflow-lib` → `pip install -e .` (editable mode)
3. `COPY contentflow-api` → `pip install -r requirements.txt`
4. Install Playwright + Chromium browser
5. Run with `uvicorn`

### 8.2 Environment Variables

#### Core Infrastructure Variables (injected by Bicep into Container Apps)

| Variable | Purpose | Set By |
|----------|---------|--------|
| `AZURE_APP_CONFIG_ENDPOINT` | Azure App Configuration endpoint (e.g., `https://<name>.azconfig.io`) | Bicep infra |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | Application Insights telemetry connection string | Bicep infra |
| `AZURE_CLIENT_ID` | User-assigned Managed Identity client ID for `DefaultAzureCredential` | Bicep infra |

#### API Service Variables (from App Config or env)

| Variable | Default | Purpose |
|----------|---------|---------|
| `COSMOS_DB_ENDPOINT` | `""` | Cosmos DB account endpoint |
| `COSMOS_DB_NAME` | `contentflow` | Database name |
| `COSMOS_DB_CONTAINER_PIPELINES` | `pipelines` | Pipeline definitions container |
| `COSMOS_DB_CONTAINER_PIPELINE_EXECUTIONS` | `pipeline_executions` | Execution history container |
| `COSMOS_DB_CONTAINER_EXECUTOR_CATALOG` | `executor_catalog` | Executor catalog container |
| `COSMOS_DB_CONTAINER_VAULTS` | `vaults` | Vault definitions container |
| `COSMOS_DB_CONTAINER_BATCH_EXECUTIONS` | `batch_executions` | Batch execution tracking |
| `BLOB_STORAGE_ACCOUNT_NAME` | `""` | Storage account for blob operations |
| `BLOB_STORAGE_CONTAINER_NAME` | `content` | Default blob container |
| `STORAGE_ACCOUNT_WORKER_QUEUE_URL` | `""` | Worker queue endpoint URL |
| `STORAGE_WORKER_QUEUE_NAME` | `contentflow-execution-requests` | Queue name for job dispatching |
| `WORKER_ENGINE_API_ENDPOINT` | `http://localhost:8099` | Worker health check URL |
| `API_SERVER_HOST` | `0.0.0.0` | API listen address |
| `API_SERVER_PORT` | `8000` | API listen port |
| `API_SERVER_WORKERS` | `1` | Uvicorn worker count |
| `LOG_LEVEL` | `INFO` | Logging level |
| `DEBUG` | `false` | Debug mode |
| `ALLOW_ORIGINS` | `*` | CORS allowed origins |

#### Worker Service Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `WORKER_NAME` | `contentflow-worker` | Worker instance name |
| `NUM_PROCESSING_WORKERS` | `2` | Number of parallel processing workers |
| `NUM_SOURCE_WORKERS` | `2` | Number of source polling workers |
| `QUEUE_POLL_INTERVAL_SECONDS` | `5` | Queue polling interval |
| `QUEUE_VISIBILITY_TIMEOUT_SECONDS` | `300` | Queue message visibility timeout |
| `QUEUE_MAX_MESSAGES` | `32` | Max messages per queue poll |
| `MAX_TASK_RETRIES` | `3` | Retry count for failed tasks |
| `TASK_TIMEOUT_SECONDS` | `600` | Max task execution time |
| `SOURCE_WORKER_POLL_INTERVAL_SECONDS` | `60` | Source worker polling interval |
| `SCHEDULER_SLEEP_INTERVAL_SECONDS` | `5` | Scheduler loop interval |
| `LOCK_TTL_SECONDS` | `300` | Distributed lock TTL |
| `DEFAULT_POLLING_INTERVAL_SECONDS` | `300` | Default poll interval |
| `API_ENABLED` | `true` | Enable worker health API |
| `API_HOST` | `0.0.0.0` | Worker API listen address |
| `API_PORT` | `8099` | Worker API listen port |

#### Executor-Level Variables (used inside pipeline executors)

| Variable | Used By | Purpose |
|----------|---------|---------|
| `BLOB_STORAGE_ACCOUNT_NAME` | field_extractor, field_validation, browser_validation | Fallback storage account name |
| `STORAGE_ACCOUNT_NAME` | field_extractor, field_validation | Secondary fallback storage name |
| `AZURE_CLIENT_ID` | AzureBlobConnector | Managed Identity for blob auth |

#### Web Frontend Variables

| Variable | Purpose |
|----------|---------|
| `VITE_API_BASE_URL` | API base URL for the React frontend (e.g., `https://<api-fqdn>/api/`) |

#### Azure App Configuration

All API and Worker settings are stored in **Azure App Configuration** with key prefixes:
- `contentflow.common.*` — Shared settings (Cosmos, Storage, Queue)
- `contentflow.api.*` — API-specific settings (server config, CORS)
- `contentflow.worker.*` — Worker-specific settings (concurrency, timeouts)

Settings are loaded at startup with a 5-minute refresh interval. A `sentinel` key triggers config reload when changed. If App Config is unavailable, the system falls back to **environment variables**.

### 8.3 Managed Identity Authentication

All Azure SDK operations use `DefaultAzureCredential` which picks up the user-assigned managed identity configured on the Container Apps.

---

## 9. System Health & Monitoring

### 9.1 Health Check Endpoints

#### API Service (`api-eedhbkibz54xe`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `GET /api/health` | GET | Full system health — checks all 5 backend services |
| `GET /api/health/{service_name}` | GET | Individual service health check |

**Available service names:** `app_config`, `cosmos_db`, `blob_storage`, `storage_queue`, `worker`

**Response model (`SystemHealth`):**
```json
{
  "status": "connected | degraded | error",
  "timestamp": "2026-06-10T...",
  "services": [
    {
      "service_name": "cosmos_db",
      "status": "connected | error",
      "response_time_ms": 45.2,
      "endpoint": "https://...",
      "error": null,
      "timestamp": "...",
      "details": {}
    }
  ]
}
```

**What each health check does:**
1. **`app_config`** — Connects to Azure App Configuration, reads a test key
2. **`cosmos_db`** — Connects to Cosmos DB, reads database properties
3. **`blob_storage`** — Connects to Azure Blob Storage, gets container properties
4. **`storage_queue`** — Connects to Azure Storage Queue, gets queue properties
5. **`worker`** — HTTP GET to the Worker API endpoint (`WORKER_ENGINE_API_ENDPOINT`)

#### Worker Service (`worker-eedhbkibz54xe`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `GET /health` | GET | Basic health ping (returns `status: "healthy"`) |
| `GET /status` | GET | Detailed worker status (running, worker counts) |

**Worker status response:**
```json
{
  "worker_name": "contentflow-worker",
  "running": true,
  "processing_workers": 2,
  "source_workers": 2
}
```

#### Container Apps Liveness Probes

Both API and Worker Container Apps have **liveness probes** configured at path `/` in the Bicep infrastructure. Azure Container Apps automatically restarts containers that fail health probes.

### 9.2 Application Insights & Monitoring

- **Application Insights** is enabled via `APPLICATIONINSIGHTS_CONNECTION_STRING`
- OpenTelemetry integration via `azure-monitor-opentelemetry` SDK
- **Log Analytics Workspace** collects container logs, metrics, and traces
- Logs can be queried in the Azure Portal → Log Analytics → `ContainerAppConsoleLogs_CL`

**Useful Log Analytics queries:**
```kusto
// All field_validation logs
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "api-eedhbkibz54xe"
| where Log_s contains "field_validation"
| order by TimeGenerated desc
| take 100

// All errors in the last hour
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "api-eedhbkibz54xe"
| where Log_s contains "ERROR"
| where TimeGenerated > ago(1h)
| order by TimeGenerated desc

// Pipeline execution tracking
ContainerAppConsoleLogs_CL
| where Log_s contains "Uploaded validation report"
| order by TimeGenerated desc
```

### 9.3 Web UI Health Dashboard

The ContentFlow Web Frontend calls `GET /api/health` and `GET /api/health/detailed` to display service status in the UI. Each backend service shows a green/red indicator based on the health check response.

---

## 10. Testing & Validation

### 10.1 Test Tramites

| # | TramiteGuid | Canonical Prefix | Documents | Status |
|---|-------------|-----------------|-----------|--------|
| 1 | `61e6a39d-b36c-43e0-a624-d421d52bfea8` | `61e6a39d-b36c-43e0-a624-d421d52bfea8` | 19 (_00–_18) | Validated ✅ |
| 2 | `3ee2e341-a5f4-4123-8b12-75870a7735f6` | `3456413e-0975-4e43-9d6a-9344b674c9ca` | 15 (_00–_14) | Available |

### 10.2 Validation Report Structure

```json
{
  "canonical_id": "61e6a39d-b36c-43e0-a624-d421d52bfea8_03",
  "source_filename": "..._ITDG-Entity-Information-SAM-Exp-7-8-2026.pdf",
  "validated_at": "2026-06-04T09:53:26.381358+00:00",
  "validation": {
    "status": "validated",
    "validation_score": 1.0,
    "fields_compared": 1,
    "fields_matched": 1,
    "fields_mismatched": 0,
    "field_results": {
      "unique_entity_id": {
        "extracted_value": "GUAYG1T5MJ14",
        "api_value": "GUAYG1T5MJ14",
        "similarity": 1.0,
        "status": "match"
      }
    },
    "rejectReasons": []
  }
}
```

### 10.3 Validation Report Statuses

| Status | Meaning |
|--------|---------|
| `validated` | Full validation completed |
| `excluded` | Document excluded from validation (_00, _01) |
| `skipped` | No extracted fields found |
| `error` | Processing error (traceback logged, report still uploaded) |

### 10.4 Blob Path Pattern

```
validation-results/
  └── validation_reports/
      └── 2026/06/04/
          ├── 61e6a39d-..._00.json
          ├── 61e6a39d-..._01.json
          ├── ...
          └── 61e6a39d-..._18.json
```

---

## 11. Known Issues & Resolutions

### 11.1 Resolved Issues

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| **Only _00 and _01 validation reports** | `_generate_reject_reasons` used `filename` variable without it being passed as parameter | Added `filename` parameter to method signature and call site |
| **Wrong issue_date "1/1/2003"** | Regex had `dated?` which matched "Fecha de Inicio de Operaciones" | Removed `dated?` from issue_date regex pattern |
| **Missing dates for CRIM (_18)** | Dates in multi-column tables not scanned | Added Strategy 1: header→row1 table scanning |
| **Missing dates for SAM (_03)** | Label and value in adjacent table cells | Added Strategy 2: adjacent-cell scanning |
| **Missing dates for ASUME (_16)** | "Fecha de emisión" not in patterns | Added to adjacent-cell scan + TABLE_KEY_MAPPING |
| **Spanish months (Ago, Dic) not parsed** | No Spanish month normalization | Added `SPANISH_MONTHS` dict for normalization |
| **Docker Hub rate limit** | `python:3.13-slim` from Docker Hub hits anonymous pull limits | Retry after rate limit window or cache in ACR with `az acr import` |
| **AOAI endpoint broken** | Pipeline YAML had literal "to" in URL | Corrected in Web UI settings |
| **Exceptions crash silently** | No error handling around validation logic | Added `_safe_process` wrapper — always uploads error report |
| **No report for "no extracted fields"** | Early return without upload | Added upload call to "no extracted fields" path |

### 11.2 Open Items

| Item | Status | Notes |
|------|--------|-------|
| `sam_gov` portal not in `enabled_portals` | Open | Add `sam_gov` to pipeline settings in Web UI |
| Date extraction accuracy | Monitor | Multi-column scanning added but may need tuning for new document formats |
| Docker Hub rate limits | Mitigated | Consider importing base image to ACR permanently |

---

## 12. Future Enhancements

1. **Add `sam_gov` to enabled portals** — Validate SAM certificates against sam.gov
2. **ACR base image caching** — Import `python:3.13-slim` to ACR to avoid Docker Hub rate limits
3. **Per-document confidence scoring** — Aggregate field + browser validation into a single confidence score
4. **Automated pipeline triggers** — Queue-based processing for new tramites
5. **Historical validation tracking** — Cosmos DB storage for validation history and trending
6. **Additional document types** — Extend field extraction patterns for new PR government certificates
7. **CI/CD pipeline** — GitHub Actions for automated testing and deployment

---

## Appendix A: Document Index Reference

| Index | Document Type | Validation | Portal |
|-------|--------------|-----------|--------|
| _00 | Company Profile | Excluded | — |
| _01 | Declaración Jurada | Excluded | — |
| _02 | Resolución Corporativa | Optional (company_name) | — |
| _03 | SAM Entity Information | company_name, unique_entity_id | sam_gov |
| _04 | Antecedentes Penales (Policía) | company_name, ssn_last_four | validacion_pr |
| _05 | SC-6088 Radicación Planillas Ingresos | company_name, ein_ssn | hacienda |
| _06 | SC-6096 Certificación de Deuda Hacienda | company_name, ein_ssn | hacienda |
| _07 | SC-2942 Planillas IVU | company_name, ein_ssn | hacienda |
| _08 | Registro de Comerciante | company_name, ein_ssn, merchant_registration | hacienda |
| _09 | Hacienda (other) | company_name, ein_ssn | hacienda |
| _10 | Desempleo/Incapacidad (DTRH) | company_name, ein_ssn | validacion_pr |
| _11 | Choferil | company_name, ein_ssn | validacion_pr |
| _12 | CFSE (Fondo Seguro Estado) | company_name | — |
| _13 | DTRH Seguro | company_name, ein_ssn | validacion_pr |
| _14 | Certificado de Incorporación | company_name | — |
| _15 | Good Standing / Estado | company_name | — |
| _16 | ASUME | company_name | validacion_pr |
| _17 | DTRH | company_name, ein_ssn | validacion_pr |
| _18 | CRIM | company_name | crim |

---

## Appendix B: Date Validity Windows

| Document Pattern | Window (Days) |
|-----------------|---------------|
| SC-2942 / IVU | 30 |
| SC-6088 / Radicación Planillas | 30 |
| SC-6096 / Hacienda Deuda | 30 |
| CRIM | 90 |
| DTRH | 90 |
| CFSE | 90 |
| ASUME | 90 |
| SAM | 90 |
| All others | 90 (default) |

---

## Appendix C: Contact & Access

| Role | Name | Contact |
|------|------|---------|
| Developer | Ratul Ghosh | v-ratulghosh@microsoft.com |

**Azure Portal:** [https://portal.azure.com](https://portal.azure.com)  
**Resource Group:** ASG-RG-AILZ-CONTENTFLOW  
**JumpBox:** Access via Azure Bastion in the AILZ VNet
