import json
import logging
import re
from datetime import datetime, timezone
from .base import BaseExecutor

logger = logging.getLogger("contentflow.executors.field_extractor_executor")

from agent_framework import WorkflowContext
from typing import Union, List
from ..connectors import AzureBlobConnector

# Azure OpenAI imports
try:
    from azure.identity import DefaultAzureCredential
    from openai import AzureOpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False
    logger.warning("openai package not available — AI extraction will be disabled")

class FieldExtractorExecutor(BaseExecutor):
    """
    Extracts and maps specific fields from Document Intelligence output.
    Uses the 'doc_intelligence_output' key which contains 'text' (markdown)
    and 'tables' from the prebuilt-layout model.
    Uploads extracted fields as JSON to the 'json_fields' blob container.
    """
    def __init__(self, id: str, settings=None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)

        # Blob output settings
        self.storage_account_name = self.get_setting("storage_account_name", default=None)
        self.credential_type = self.get_setting("credential_type", default="default_azure_credential")
        self.credential_key = self.get_setting("credential_key", default=None)
        self.container_name = self.get_setting("container_name", default="json-fields")
        self._connector = None

        # Azure OpenAI settings for AI-powered extraction
        self.aoai_endpoint = self.get_setting("aoai_endpoint", default="https://aiaifce5y5fcseqg4m5.cognitiveservices.azure.com/")
        self.aoai_deployment = self.get_setting("aoai_deployment", default="gpt-4.1-mini")
        self.ai_extraction_enabled = self.get_setting("ai_extraction_enabled", default=True)
        self._openai_client = None
        # Regex patterns to extract fields from DI markdown text
        # Each tuple: (field_name, compiled_regex, group_index)
        self.field_patterns = [
            ("document_title", re.compile(r"^#\s+(.+)$", re.MULTILINE), 1),
            ("company_name", re.compile(r"(?:empresa|company|corporaci[oó]n|entidad|nombre)\s*[:\-]?\s*(.+)", re.IGNORECASE), 1),
            ("registration_number", re.compile(r"(?:n[uú]mero\s*de\s*registro|registration\s*(?:no|number|#)|registro\s*(?:no|#))\s*[:\-]?\s*([\w\-]{3,})", re.IGNORECASE), 1),
            # certificate_number: require the captured value to start with a digit or contain digits
            ("certificate_number", re.compile(r"(?:certificaci[oó]n\s*(?:no|n[uú]m(?:ero)?|number|#)|certificate\s*(?:no|number|#))\s*[:\-]?\s*(\d[\w\-]*)", re.IGNORECASE), 1),
            ("date", re.compile(r"(?:fecha|date)\s*[:\-]?\s*(\d{1,2}[\s/\-]\w{3,}[\s/\-]\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4})", re.IGNORECASE), 1),
            ("issue_date", re.compile(r"(?:fecha\s*de\s*(?:emisi[oó]n|expedici[oó]n)|issue\s*date|dated?|emitido|expedido|generado)\s*[:\-]?\s*(\d{1,2}[\s/\-]\w{3,}[\s/\-]\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})", re.IGNORECASE), 1),
            ("ein_ssn", re.compile(r"(?:EIN|SSN|FEIN|employer\s*identification|patronal|n[uú]mero\s*patronal\s*federal)\s*[:\-#]?\s*(\d[\d\-]{3,})", re.IGNORECASE), 1),
            ("naics_code", re.compile(r"(?:NAICS|c[oó]digo\s*NAICS)\s*[:\-]?\s*(\d{4,6})", re.IGNORECASE), 1),
            ("merchant_registration", re.compile(r"(?:registro\s*de\s*comerciante|merchant\s*registration|n[uú]mero\s*de\s*comerciante)\s*[:\-]?\s*([\w\-]{3,})", re.IGNORECASE), 1),
            ("expiration_date", re.compile(r"(?:expira|expires?|vence|vigencia|valid\s*(?:until|through)|v[aá]lido\s*hasta)\s*[:\-]?\s*(\d{1,2}[\s/\-]\w{3,}[\s/\-]\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})", re.IGNORECASE), 1),
            ("total_amount", re.compile(r"(?:total|monto|amount|balance|deuda)\s*[:\-]?\s*\$?\s*([\d,]+\.\d{2})", re.IGNORECASE), 1),
            ("agent_type", re.compile(r"(?:tipo\s*de\s*agente|agent\s*type|tipo\s*de\s*representante|authorized\s*agent)\s*[:\-]?\s*(.+)", re.IGNORECASE), 1),
            ("unique_entity_id", re.compile(r"(?:unique\s*entity\s*id(?:entifier)?|UEI|SAM\s*(?:entity\s*)?ID)\s*[:\-]?\s*([A-Z0-9]{12})", re.IGNORECASE), 1),
            ("application_number", re.compile(r"(?:n[uú]mero\s*de\s*solicitud|application\s*(?:no|number|#)|solicitud\s*(?:no|n[uú]m(?:ero)?|#))\s*[:\-]?\s*([\w\-]{3,})", re.IGNORECASE), 1),
        ]

    async def process_content_item(self, content):
        # Read from the correct key — try multiple possible field names
        di_output = (
            content.data.get("doc_intell_output")
            or content.data.get("doc_intelligence_output")
            or content.data.get("doc_intelligence_result")
            or {}
        )
        text = di_output.get("text", "")
        tables = di_output.get("tables", [])

        fields = {}

        # 1. Extract fields via regex patterns on the full text
        for field_name, pattern, group_idx in self.field_patterns:
            match = pattern.search(text)
            if match:
                fields[field_name] = match.group(group_idx).strip()

        # Whitelist of fields we actually use for validation — discard everything else
        ALLOWED_FIELDS = {
            "document_title", "company_name", "ein_ssn", "issue_date", "expiration_date",
            "certificate_number", "registration_number", "merchant_registration",
            "naics_code", "agent_type", "ssn_last_four", "page_count",
            "unique_entity_id", "application_number",
            # Alternate names that get normalized later
            "id_de_contribuyente", "id_de_correspondencia",
        }

        # 2. Extract key-value pairs from tables (2-column tables)
        for table in tables:
            cells = table.get("cells", [])
            col_count = table.get("column_count", 0)
            if col_count == 2:
                row_data = {}
                for cell in cells:
                    row_data.setdefault(cell["row_index"], {})[cell["column_index"]] = cell.get("content", "")
                for row_idx, cols in row_data.items():
                    key = cols.get(0, "").strip()
                    value = cols.get(1, "").strip()
                    if key and value:
                        normalized_key = re.sub(r'\s+', '_', key.lower().strip())
                        normalized_key = re.sub(r'[^a-z0-9_]', '', normalized_key)
                        # Skip bad keys: too short, purely numeric/date-like, or value looks like a label
                        if not normalized_key or len(normalized_key) < 3:
                            continue
                        if re.match(r'^\d', normalized_key):
                            continue  # Skip keys starting with digits (e.g., dates as keys)
                        if re.match(r'^(fecha|date|page|total|__|_$)', normalized_key):
                            continue  # Skip generic/noisy keys
                        if value.lower() == key.lower():
                            continue  # Skip when value is just a repeat of the key
                        if normalized_key not in fields:
                            fields[normalized_key] = value

        # 3a. Filter to only allowed fields — remove noisy KVP extractions
        fields = {k: v for k, v in fields.items() if k in ALLOWED_FIELDS}

        # 3. Store page count from DI output
        pages = di_output.get("pages", [])
        if pages:
            fields["page_count"] = len(pages)

        # 4. AI-powered extraction — fills gaps that regex missed
        if self.ai_extraction_enabled and HAS_OPENAI and text:
            ai_fields = await self._ai_extract_fields(text)
            if ai_fields:
                # AI values override regex — AI is more reliable for key fields
                # For ein_ssn and company_name, always prefer AI since regex often grabs wrong values
                AI_ALWAYS_WINS = {"ein_ssn", "company_name", "naics_code", "registration_number"}
                for key, ai_value in ai_fields.items():
                    if not ai_value or ai_value == "null":
                        continue
                    current = fields.get(key, "")
                    # Always use AI for critical fields, or if regex got empty/boilerplate
                    if (key in AI_ALWAYS_WINS
                        or not current
                        or len(str(current).strip()) < 4
                        or str(current).lower().startswith(("de ", "del ", "es ", "se "))
                        or key not in fields):
                        fields[key] = ai_value
                        logger.debug(f"AI override: {key} = '{ai_value}' (was: '{current}')")

        content.data["extracted_fields"] = fields
        logger.info(f"Extracted {len(fields)} fields from content {getattr(content.id, 'canonical_id', 'unknown')}")

        # Upload extracted fields to blob storage
        await self._upload_fields_to_blob(content, fields)

        return content

    async def _ai_extract_fields(self, text: str) -> dict:
        """Use Azure OpenAI to extract fields that regex may have missed."""
        try:
            client = self._get_openai_client()
            if not client:
                return {}

            # Truncate text to avoid token limits (first 6000 chars for better context)
            truncated_text = text[:6000] if len(text) > 6000 else text

            system_prompt = """You are a document field extractor for Puerto Rico government certificates and business documents.
Extract the following fields from the document text. Return ONLY a JSON object with these keys:
- company_name: The name of the company/entity the certificate is about (NOT the issuing agency)
- issue_date: The date the document was issued/generated (format: MM/DD/YYYY or as found)
- expiration_date: The date the document expires (format: MM/DD/YYYY or as found)
- ein_ssn: The Federal EIN/FEIN number (format: XX-XXXXXXX, digits and dashes only)
- merchant_registration: The merchant registration number
- certificate_number: The certificate number
- registration_number: The registration number if different from certificate number
- naics_code: The NAICS code (4-6 digit industry code)
- agent_type: The type of authorized agent/representative
- ssn_last_four: The last 4 digits of a Social Security Number (look for masked patterns like xxx-xx-9456 or ***-**-1234)
- unique_entity_id: The Unique Entity Identifier (UEI) or SAM Entity ID (12-character alphanumeric)
- application_number: The application/solicitud number ("Número de Solicitud") — especially for Policía criminal record certificates

CRITICAL RULES:
- For company_name: Extract the SUBJECT company name — the business entity the certificate is issued FOR/ABOUT, NOT the government agency issuing it. The company name is typically a proper noun (e.g., "ASG FACILITY SOLUTIONS LLC", "MI EMPRESA INC"). Do NOT return generic words like "doméstica con", "Jurídica", "corporación", or partial phrases. If the document is about a company, its name usually appears near the top or after labels like "Nombre", "Entidad", "Corporación", "empresa".
- For ein_ssn: Extract ONLY the Federal Employer Identification Number (FEIN/EIN) in XX-XXXXXXX format. Do NOT use the "Número de Contribuyente" or "ID de Contribuyente" (which may be a local PR tax ID like XXXXX-XXXXX). The FEIN is specifically labeled as "Número Patronal Federal (FEIN)", "Federal EIN", "EIN", or "Employer Identification Number". If you see both a contribuyente ID and a FEIN, use the FEIN. Do NOT use employer account numbers (like "Cuenta Patronal") as the EIN.
- For merchant_registration: Look for "Registro de Comerciante", "Merchant Registration Number", or "Número de Comerciante". This is a numeric or alphanumeric code, NOT a name or label.
- For agent_type: Look for "Tipo de Agente" or agent classification. Common values include "Agente retenedor", "Persona jurídica", "Persona natural".
- For dates: Look for any dates in the document. Issue date may appear as "fecha de emisión", "expedido", "generado", "dated". Expiration may appear as "válido hasta", "expira", "vence".
- If a field cannot be found, set its value to null.
- Return ONLY valid JSON, no explanation."""

            response = client.chat.completions.create(
                model=self.aoai_deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Extract fields from this document:\n\n{truncated_text}"}
                ],
                temperature=0.0,
                max_tokens=500,
                response_format={"type": "json_object"},
            )

            result_text = response.choices[0].message.content.strip()
            ai_fields = json.loads(result_text)

            # Clean nulls
            return {k: v for k, v in ai_fields.items() if v is not None and v != "null" and v != ""}

        except Exception as e:
            logger.warning(f"AI extraction failed (non-fatal): {e}")
            return {}

    def _get_openai_client(self):
        """Lazily initialize the Azure OpenAI client."""
        if self._openai_client is not None:
            return self._openai_client

        if not HAS_OPENAI or not self.aoai_endpoint:
            return None

        try:
            credential = DefaultAzureCredential()
            token = credential.get_token("https://cognitiveservices.azure.com/.default")

            self._openai_client = AzureOpenAI(
                azure_endpoint=self.aoai_endpoint,
                azure_ad_token=token.token,
                api_version="2024-12-01-preview",
            )
            return self._openai_client
        except Exception as e:
            logger.warning(f"Failed to initialize OpenAI client: {e}")
            return None

    async def _get_connector(self):
        """Lazily initialize the blob connector."""
        if self._connector is None:
            account = self.storage_account_name
            if not account:
                # Try to get from environment
                import os
                account = os.environ.get("BLOB_STORAGE_ACCOUNT_NAME", os.environ.get("STORAGE_ACCOUNT_NAME", ""))
            if not account:
                logger.warning("No storage_account_name configured; skipping blob upload of extracted fields.")
                return None
            self._connector = AzureBlobConnector(
                name=f"{self.id}_blob",
                settings={
                    "account_name": account,
                    "credential_type": self.credential_type,
                    "credential_key": self.credential_key,
                }
            )
            await self._connector.initialize()
        return self._connector

    async def _upload_fields_to_blob(self, content, fields: dict):
        """Upload extracted fields as JSON to the json_fields container."""
        try:
            connector = await self._get_connector()
            if connector is None:
                return

            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            now = datetime.now(timezone.utc)
            blob_path = f"extracted_fields/{now.strftime('%Y/%m/%d')}/{canonical_id}.json"

            payload = {
                "canonical_id": canonical_id,
                "source_filename": getattr(content.id, 'filename', None),
                "extracted_at": now.isoformat(),
                "fields": fields,
            }
            data = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")

            result = await connector.upload_blob(
                container_name=self.container_name,
                blob_path=blob_path,
                data=data,
                overwrite=True,
            )
            logger.info(f"Uploaded extracted fields to {self.container_name}/{blob_path}")
            content.summary_data["extracted_fields_blob"] = {
                "container": self.container_name,
                "blob_path": blob_path,
                "blob_size": len(data),
                "write_status": "success",
            }
        except Exception as e:
            logger.error(f"Failed to upload extracted fields to blob: {e}")
            content.summary_data["extracted_fields_blob"] = {
                "write_status": "error",
                "error": str(e),
            }

    async def process_input(
        self,
        input: Union['Content', List['Content']],
        ctx: WorkflowContext[Union['Content', List['Content']], Union['Content', List['Content']]]
    ) -> Union['Content', List['Content']]:
        # Support both single Content and list of Content
        if isinstance(input, list):
            return [await self.process_content_item(item) for item in input]
        else:
            return await self.process_content_item(input)
