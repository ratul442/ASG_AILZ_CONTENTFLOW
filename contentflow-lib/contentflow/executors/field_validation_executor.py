import json
import logging
import os
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from .base import BaseExecutor

logger = logging.getLogger("contentflow.executors.field_validation_executor")

from agent_framework import WorkflowContext
from typing import Union, List
from ..connectors import AzureBlobConnector


class FieldValidationExecutor(BaseExecutor):
    """
    Compares extracted fields (from Document Intelligence) against the
    source API data (tramite data) to validate document authenticity.

    For each document, it maps extracted field names to API data field names,
    computes similarity scores, and produces a validation report uploaded
    to blob storage.
    """

    # Documents excluded from validation (by suffix index in canonical_id)
    EXCLUDED_DOC_INDICES = {0, 1}  # _00 = Company Profile, _01 = Declaracion Jurada

    # Optional documents - validated but non-determinative for final verdict
    OPTIONAL_DOC_INDICES = {2}  # _02 = Resolucion Corporativa

    # Filename patterns for excluded/optional/member-verification documents
    EXCLUDED_FILENAME_PATTERNS = [r"Company.Profile", r"Declaracion.Jurada"]
    OPTIONAL_FILENAME_PATTERNS = [r"Resolucion.Corporativa"]
    MEMBER_VERIFICATION_FILENAME_PATTERNS = [r"Antecedentes.*Penales|Policia|Policía"]

    # Filename-based validation field mapping (case-insensitive regex → fields to validate)
    # Order matters: first match wins
    VALIDATION_FIELDS_BY_FILENAME = [
        (r"Company.Profile|Declaracion.Jurada",           None),  # excluded
        (r"Resolucion.Corporativa",                        {"company_name"}),
        (r"SAM|Entity.*Information.*SAM",                   {"company_name", "unique_entity_id"}),
        (r"Antecedentes.*Penales|Policia|Policía",         {"company_name", "ssn_last_four"}),
        (r"SC-6088|Radicacion.*Planillas.*Ingresos",       {"company_name", "ein_ssn"}),
        (r"SC-6096|HACIENDA.*Deuda|Deuda.*HACIENDA",       {"company_name", "ein_ssn"}),
        (r"SC-2942|IVU|Planillas.*IVU",                    {"company_name", "ein_ssn"}),
        (r"Registro.*Comerciante|Merchant",                {"company_name", "ein_ssn", "merchant_registration"}),
        (r"HACIENDA",                                      {"company_name", "ein_ssn"}),
        (r"Desempleo|Incapacidad|DTRH.*Seguro",            {"company_name", "ein_ssn"}),
        (r"Choferil",                                      {"company_name", "ein_ssn"}),
        (r"DTRH",                                          {"company_name", "ein_ssn"}),
        (r"CFSE|Fondo.*Seguro.*Estado",                    {"company_name", "ein_ssn"}),
        (r"ASUME",                                         {"company_name"}),
        (r"CRIM",                                          {"company_name"}),
        (r"Incorporacion",                                 {"company_name"}),
        (r"Existencia|Good.Standing|ESTADO",               {"company_name"}),
    ]

    # Legacy per-index mapping (fallback if filename doesn't match)
    VALIDATION_FIELDS_PER_DOC = {
        2:  {"company_name"},
        3:  {"company_name", "unique_entity_id"},
        4:  {"company_name", "ssn_last_four"},
        7:  {"company_name", "ein_ssn"},
        8:  {"company_name", "ein_ssn"},
        9:  {"company_name", "ein_ssn"},
        10: {"company_name", "ein_ssn"},
        11: {"company_name", "ein_ssn"},
        12: {"company_name"},
        14: {"company_name"},
    }

    # Documents that require authorized member verification
    MEMBER_VERIFICATION_DOC_INDICES = {4}  # Legacy fallback

    # Table-extracted field aliases that map to standard fields
    TABLE_FIELD_ALIASES = {
        "nombre_del_patrono": "company_name",
        "nombre_comercial": "company_name",
        "nombre_de_la_entidad": "company_name",
        "nombre_del_comerciante": "company_name",
        "employer_name": "company_name",
        "entity_name": "company_name",
        "nmero_patronal_federal_fein": "ein_ssn",
        "numero_patronal_federal": "ein_ssn",
        "fein": "ein_ssn",
        "federal_ein": "ein_ssn",
        "employer_identification_number": "ein_ssn",
        "nmero_de_registro": "registration_number",
        "numero_de_comerciante": "merchant_registration",
        "nmero_de_comerciante": "merchant_registration",
        "merchant_number": "merchant_registration",
        "id_de_contribuyente": "registration_number",
        "taxpayer_id": "registration_number",
        "seguro_social": "ssn_last_four",
        "social_security": "ssn_last_four",
        "tipo_de_agente": "agent_type",
        "tipo_de_representante": "agent_type",
        "authorized_agent": "agent_type",
        "unique_entity_identifier": "unique_entity_id",
        "uei": "unique_entity_id",
    }

    # Mapping: extracted_field_name -> list of API data keys to compare against
    FIELD_MAPPING = {
        "company_name": ["companyName"],
        "ein_ssn": ["companySsn", "companySsnLastFour"],
        "ssn_last_four": ["companySsnLastFour"],
        "registration_number": ["merchantRegistrationNumber", "registrationCertificateNumber"],
        "merchant_registration": ["merchantRegistrationNumber", "registrationCertificateNumber"],
        "certificate_number": ["registrationCertificateNumber"],
        "naics_code": ["naicsCodes"],
        "total_amount": [],
        "date": [],
        "expiration_date": [],
        "issue_date": [],
        "document_title": [],
        "agent_type": ["agentType"],
        "unique_entity_id": ["uniqueEntityId", "samEntityId"],
    }

    def __init__(self, id: str, settings=None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)

        self.similarity_threshold = self.get_setting("similarity_threshold", default=0.65)
        self.storage_account_name = self.get_setting("storage_account_name", default=None)
        self.credential_type = self.get_setting("credential_type", default="default_azure_credential")
        self.credential_key = self.get_setting("credential_key", default=None)
        self.container_name = self.get_setting("container_name", default="validation-results")
        self.local_output_folder = self.get_setting("local_output_folder", default=None)
        self._connector = None

    def _normalize(self, value: str) -> str:
        """Normalize a string for comparison: lowercase, strip, remove punctuation."""
        if not value:
            return ""
        value = str(value).lower().strip()
        value = re.sub(r'[^\w\s]', '', value)
        value = re.sub(r'\s+', ' ', value)
        return value

    def _similarity(self, a: str, b: str) -> float:
        """Compute similarity ratio between two strings."""
        a_norm = self._normalize(a)
        b_norm = self._normalize(b)
        if not a_norm or not b_norm:
            return 0.0
        # Check exact match first
        if a_norm == b_norm:
            return 1.0
        # Check containment
        if a_norm in b_norm or b_norm in a_norm:
            return 0.9
        return SequenceMatcher(None, a_norm, b_norm).ratio()

    def _compare_naics(self, extracted_code: str, api_naics: list) -> dict:
        """Compare extracted NAICS code against API NAICS codes list."""
        if not api_naics or not extracted_code:
            return {"match": False, "similarity": 0.0, "details": "Missing data"}
        extracted_clean = re.sub(r'\D', '', str(extracted_code))
        for entry in api_naics:
            api_code = str(entry.get("code", "")).strip()
            if extracted_clean == api_code or extracted_clean.startswith(api_code) or api_code.startswith(extracted_clean):
                return {"match": True, "similarity": 1.0, "api_value": api_code, "extracted_value": extracted_clean}
        return {"match": False, "similarity": 0.0, "api_values": [e.get("code") for e in api_naics], "extracted_value": extracted_clean}

    def _compare_ssn(self, extracted_ssn: str, api_ssn: str, api_last_four: str) -> dict:
        """Compare extracted EIN/SSN - supports full or last-four matching."""
        extracted_clean = re.sub(r'\D', '', str(extracted_ssn))
        if not extracted_clean:
            return {"match": False, "similarity": 0.0, "details": "No extracted value"}

        # Full match
        api_clean = re.sub(r'\D', '', str(api_ssn)) if api_ssn else ""
        if api_clean and extracted_clean == api_clean:
            return {"match": True, "similarity": 1.0, "match_type": "full", "extracted_value": extracted_ssn}

        # Last 4 digits match
        last_four_clean = str(api_last_four).strip() if api_last_four else ""
        if last_four_clean and extracted_clean.endswith(last_four_clean):
            return {"match": True, "similarity": 0.95, "match_type": "last_four", "extracted_value": extracted_ssn}

        return {"match": False, "similarity": self._similarity(extracted_clean, api_clean), "extracted_value": extracted_ssn, "api_value": api_ssn}

    def _get_doc_index(self, content) -> int:
        """Extract the document index (e.g., 3 from '..._03') from canonical_id."""
        canonical_id = getattr(content.id, 'canonical_id', '') or ''
        m = re.search(r'_(\d+)$', canonical_id)
        return int(m.group(1)) if m else -1

    def _get_filename(self, content) -> str:
        """Get filename from content for classification."""
        filename = getattr(content.id, 'filename', '') or ''
        if not filename:
            filename = getattr(content.id, 'canonical_id', '') or ''
        return filename

    def _is_excluded_by_filename(self, filename: str) -> bool:
        """Check if document should be excluded based on filename."""
        for pattern in self.EXCLUDED_FILENAME_PATTERNS:
            if re.search(pattern, filename, re.IGNORECASE):
                return True
        return False

    def _is_optional_by_filename(self, filename: str) -> bool:
        """Check if document is optional based on filename."""
        for pattern in self.OPTIONAL_FILENAME_PATTERNS:
            if re.search(pattern, filename, re.IGNORECASE):
                return True
        return False

    def _needs_member_verification(self, filename: str, doc_index: int) -> bool:
        """Check if document needs member verification based on filename."""
        for pattern in self.MEMBER_VERIFICATION_FILENAME_PATTERNS:
            if re.search(pattern, filename, re.IGNORECASE):
                return True
        return doc_index in self.MEMBER_VERIFICATION_DOC_INDICES

    def _get_validation_fields_by_filename(self, filename: str) -> set:
        """Determine which fields to validate based on filename patterns."""
        for pattern, fields in self.VALIDATION_FIELDS_BY_FILENAME:
            if re.search(pattern, filename, re.IGNORECASE):
                return fields  # None means excluded
        return None  # No match — validate all fields (legacy behavior)

    def _resolve_table_aliases(self, extracted: dict) -> dict:
        """Resolve table-extracted keys to standard field names when standard field is empty/bad."""
        resolved = dict(extracted)
        for table_key, standard_field in self.TABLE_FIELD_ALIASES.items():
            if table_key in extracted and table_key != standard_field:
                alias_value = extracted[table_key]
                current_value = resolved.get(standard_field, "")
                # Use alias if standard field is empty or looks like boilerplate
                if not current_value or len(current_value.strip()) < 4 or current_value.lower().startswith(("de ", "del ", "es ")):
                    resolved[standard_field] = alias_value
                    logger.debug(f"Resolved table alias '{table_key}' -> '{standard_field}': '{alias_value}'")
        return resolved

    def _extract_ssn_last_four(self, extracted: dict) -> str:
        """Extract last 4 digits of SSN from masked patterns like xxx-xx-9456."""
        for key, value in extracted.items():
            if not isinstance(value, str):
                continue
            m = re.search(r'[xX*]{2,}[-\s]?[xX*]{2,}[-\s]?(\d{4})', value)
            if m:
                return m.group(1)
        return ""

    @staticmethod
    def _find_authorized_members(data, depth=0):
        """Recursively search for authorizedMembers in nested data."""
        if depth > 5 or not isinstance(data, dict):
            return []
        for key in ("authorizedMembers", "members", "authorized_members"):
            val = data.get(key)
            if isinstance(val, list) and val:
                return val
        # Search nested dicts
        for v in data.values():
            if isinstance(v, dict):
                found = FieldValidationExecutor._find_authorized_members(v, depth + 1)
                if found:
                    return found
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        found = FieldValidationExecutor._find_authorized_members(item, depth + 1)
                        if found:
                            return found
        return []

    def _verify_authorized_members(self, extracted: dict, api_data: dict, content=None) -> list:
        """For CRIM certs, verify ALL authorized member names from API appear in document."""
        reasons = []
        members = self._find_authorized_members(api_data)
        if not members:
            logger.info("No authorizedMembers found in API data — skipping member verification.")
            logger.info(f"API data top-level keys: {list(api_data.keys()) if isinstance(api_data, dict) else type(api_data)}")
            return reasons

        # Build searchable text from extracted fields
        all_text = " ".join(str(v) for v in extracted.values() if isinstance(v, str)).lower()
        all_text += " " + " ".join(str(k) for k in extracted.keys()).lower()

        # Also search the original Document Intelligence text (raw OCR output)
        if content is not None:
            di_output = (
                content.data.get("doc_intell_output")
                or content.data.get("doc_intelligence_output")
                or content.data.get("doc_intelligence_result")
                or {}
            )
            raw_text = di_output.get("text", "")
            if raw_text:
                all_text += " " + raw_text.lower()

        # Normalize accents for comparison
        import unicodedata
        def strip_accents(s):
            return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

        all_text_normalized = strip_accents(all_text)

        member_details = []
        missing_members = []
        found_members = []
        for member in members:
            if isinstance(member, dict):
                first = member.get("firstName", "") or ""
                last = member.get("lastName", "") or ""
                second_surname = member.get("secondLastName", "") or member.get("secondSurname", "") or member.get("second_surname", "") or ""
                full_name = member.get("name", "") or ""
                if not full_name and (first or last):
                    parts = [first, last, second_surname]
                    full_name = " ".join(p for p in parts if p).strip()
                elif full_name and second_surname and second_surname.lower() not in full_name.lower():
                    full_name = f"{full_name} {second_surname}".strip()
                member_name = full_name
                position = member.get("position", "")
            else:
                member_name = str(member)
                position = ""

            if not member_name:
                continue

            # Check if any significant part of the name appears in document text
            name_parts = [p for p in member_name.lower().split() if len(p) > 2]
            matched_parts = [p for p in name_parts if p in all_text or strip_accents(p) in all_text_normalized]
            found = len(matched_parts) > 0

            detail = {
                "name": member_name,
                "position": position,
                "status": "match" if found else "unmatch",
                "matched_parts": matched_parts,
                "unmatched_parts": [p for p in name_parts if p not in matched_parts],
            }
            member_details.append(detail)

            if found:
                found_members.append(member_name)
            else:
                missing_members.append(member_name)

        logger.info(f"Member verification: found={found_members}, missing={missing_members}")
        self._last_member_details = member_details

        if missing_members:
            reasons.append({
                "id": 900,
                "reason": f"Authorized member(s) not found in document: {', '.join(missing_members)}. All authorized members must be verified."
            })
        return reasons

    async def process_content_item(self, content):
        """Compare extracted fields against API source data."""
        canonical_id = getattr(content.id, 'canonical_id', 'unknown')
        doc_index = self._get_doc_index(content)

        # ── Check if document is EXCLUDED from validation ──
        filename = self._get_filename(content)
        is_excluded = self._is_excluded_by_filename(filename) or doc_index in self.EXCLUDED_DOC_INDICES
        if is_excluded:
            logger.info(f"Document {canonical_id} ('{filename}') is EXCLUDED from validation.")
            validation_result = {
                "status": "excluded",
                "reason": "Document designated for exclusion from validation process.",
                "doc_index": doc_index,
                "rejectReasons": [],
            }
            content.data["validation_result"] = validation_result
            await self._upload_validation_report(content, validation_result)
            self._save_local(content, validation_result)
            return content

        extracted = content.data.get("extracted_fields", {})
        api_data = content.data

        if not extracted:
            logger.info(f"No extracted fields for {canonical_id} - skipping validation")
            validation_result = {
                "status": "skipped",
                "reason": "no_extracted_fields",
                "field_count": 0,
                "rejectReasons": [],
            }
            content.data["validation_result"] = validation_result
            await self._upload_validation_report(content, validation_result)
            self._save_local(content, validation_result)
            return content

        # ── Resolve table-extracted aliases to standard field names ──
        extracted = self._resolve_table_aliases(extracted)

        # ── Extract SSN last 4 from masked patterns ──
        ssn_last_four = self._extract_ssn_last_four(extracted)
        if ssn_last_four and "ssn_last_four" not in extracted:
            extracted["ssn_last_four"] = ssn_last_four

        # ── Determine which fields to validate for this document type ──
        allowed_fields = self._get_validation_fields_by_filename(filename)
        if allowed_fields is None:
            allowed_fields = self.VALIDATION_FIELDS_PER_DOC.get(doc_index)
        logger.info(f"Validation fields for '{filename}' (idx {doc_index}): {allowed_fields}")

        results = {}
        matched = 0
        mismatched = 0
        not_compared = 0

        for field_name, field_value in extracted.items():
            if field_name == "page_count":
                continue

            # Skip fields not required for this document type
            if allowed_fields is not None and field_name not in allowed_fields:
                results[field_name] = {
                    "extracted_value": field_value,
                    "status": "not_required",
                    "details": "Field not required for validation of this document type.",
                }
                not_compared += 1
                continue

            api_keys = self.FIELD_MAPPING.get(field_name, [])

            if not api_keys:
                # No API field to compare against
                results[field_name] = {
                    "extracted_value": field_value,
                    "status": "no_api_mapping",
                    "details": "No corresponding API field to compare against",
                }
                not_compared += 1
                continue

            # Skip garbage values for registration/merchant fields (must contain digits)
            if field_name in ("merchant_registration", "registration_number", "certificate_number"):
                if not re.search(r'\d', str(field_value)):
                    results[field_name] = {
                        "extracted_value": field_value,
                        "status": "no_api_mapping",
                        "details": f"Value '{field_value}' does not contain digits — likely extraction error. Skipped.",
                    }
                    not_compared += 1
                    continue

            # Special handling for NAICS
            if field_name == "naics_code":
                api_naics = api_data.get("naicsCodes", [])
                comparison = self._compare_naics(field_value, api_naics)
                comparison["status"] = "match" if comparison["match"] else "mismatch"
                results[field_name] = comparison
                if comparison["match"]:
                    matched += 1
                else:
                    mismatched += 1
                continue

            # Special handling for EIN/SSN
            if field_name == "ein_ssn":
                # Validate EIN format: must contain a dash (XX-XXXXXXX) or be 7-9 digits
                ein_clean = re.sub(r'\D', '', str(field_value))
                has_ein_format = bool(re.match(r'^\d{2}-\d{5,7}$', str(field_value).strip())) or (7 <= len(ein_clean) <= 9)
                if not has_ein_format:
                    # Not a valid EIN — likely account number (e.g., _11 choferil). Skip EIN comparison.
                    results[field_name] = {
                        "extracted_value": field_value,
                        "status": "no_api_mapping",
                        "details": f"Value '{field_value}' does not match EIN format (XX-XXXXXXX). Skipped EIN comparison.",
                    }
                    not_compared += 1
                    continue
                comparison = self._compare_ssn(
                    field_value,
                    api_data.get("companySsn", ""),
                    api_data.get("companySsnLastFour", ""),
                )
                comparison["status"] = "match" if comparison["match"] else "mismatch"
                results[field_name] = comparison
                if comparison["match"]:
                    matched += 1
                else:
                    mismatched += 1
                continue

            # Special handling for SSN last 4
            if field_name == "ssn_last_four":
                api_last_four = str(api_data.get("companySsnLastFour", "")).strip()
                extracted_clean = re.sub(r'\D', '', str(field_value))[-4:]
                is_match = bool(api_last_four and extracted_clean == api_last_four)
                results[field_name] = {
                    "extracted_value": field_value,
                    "api_value": api_last_four,
                    "match": is_match,
                    "status": "match" if is_match else "mismatch",
                }
                if is_match:
                    matched += 1
                else:
                    mismatched += 1
                continue

            # Generic string comparison
            best_score = 0.0
            best_api_key = None
            best_api_value = None

            for api_key in api_keys:
                api_value = api_data.get(api_key, "")
                if isinstance(api_value, (list, dict)):
                    api_value = json.dumps(api_value)
                # For numeric fields, also try digits-only comparison
                score = self._similarity(str(field_value), str(api_value))
                if field_name in ("merchant_registration", "registration_number", "certificate_number"):
                    extracted_digits = re.sub(r'\D', '', str(field_value))
                    api_digits = re.sub(r'\D', '', str(api_value))
                    if extracted_digits and api_digits and extracted_digits == api_digits:
                        score = 1.0
                if score > best_score:
                    best_score = score
                    best_api_key = api_key
                    best_api_value = api_value

            is_match = best_score >= self.similarity_threshold
            results[field_name] = {
                "extracted_value": field_value,
                "api_field": best_api_key,
                "api_value": best_api_value,
                "similarity": round(best_score, 3),
                "status": "match" if is_match else "mismatch",
            }

            if is_match:
                matched += 1
            else:
                mismatched += 1

        total_compared = matched + mismatched
        validation_score = round(matched / total_compared, 3) if total_compared > 0 else 0.0

        # Generate reject reasons based on mismatches and validation issues
        reject_reasons = self._generate_reject_reasons(results, extracted, api_data, filename)

        # ── Member Verification (for Policía documents) ──
        is_member_verification_doc = self._needs_member_verification(filename, doc_index)
        if is_member_verification_doc:
            member_reasons = self._verify_authorized_members(extracted, api_data, content)
            member_reason_id = len(reject_reasons) + 1
            for mr in member_reasons:
                mr["id"] = member_reason_id
                reject_reasons.append(mr)
                member_reason_id += 1
        # ── Determine if document is optional ──
        is_optional = self._is_optional_by_filename(filename) or doc_index in self.OPTIONAL_DOC_INDICES

        validation_result = {
            "status": "validated",
            "optional": is_optional,
            "validation_score": validation_score,
            "total_fields_extracted": len(extracted),
            "fields_compared": total_compared,
            "fields_matched": matched,
            "fields_mismatched": mismatched,
            "fields_not_compared": not_compared,
            "threshold": self.similarity_threshold,
            "field_results": results,
            "rejectReasons": reject_reasons,
        }

        if is_member_verification_doc:
            member_details = getattr(self, '_last_member_details', [])
            validation_result["member_verification"] = {
                "status": "checked",
                "total_members": len(member_details),
                "matched": sum(1 for m in member_details if m["status"] == "match"),
                "unmatched": sum(1 for m in member_details if m["status"] == "unmatch"),
                "members": member_details,
            }
        if is_optional:
            validation_result["optional_note"] = "This document is optional. Validation results are non-determinative for final case verdict."

        content.data["validation_result"] = validation_result
        logger.info(
            f"Validation for {getattr(content.id, 'canonical_id', 'unknown')}: "
            f"score={validation_score}, matched={matched}, mismatched={mismatched}"
        )

        # Upload validation report to blob
        await self._upload_validation_report(content, validation_result)

        # Save locally if configured
        self._save_local(content, validation_result)

        return content

    def _generate_reject_reasons(self, field_results: dict, extracted: dict, api_data: dict, filename: str = "") -> list:
        """
        Generate reject reasons based on field comparison results.
        Each reason has an id and a descriptive reason string.
        """
        reasons = []
        reason_id = 1

        for field_name, result in field_results.items():
            status = result.get("status", "")

            if status == "mismatch":
                extracted_val = result.get("extracted_value", extracted.get(field_name, ""))
                api_val = result.get("api_value", "")
                similarity = result.get("similarity", 0.0)

                if field_name == "company_name":
                    reasons.append({
                        "id": reason_id,
                        "reason": f"Nombre de empresa no coincide. Documento muestra '{extracted_val}', API indica '{api_val}'. Similitud: {similarity}"
                    })
                elif field_name == "ein_ssn":
                    reasons.append({
                        "id": reason_id,
                        "reason": f"EIN/SSN no coincide con los datos registrados. Valor extraído: '{extracted_val}'"
                    })
                elif field_name == "registration_number" or field_name == "merchant_registration":
                    reasons.append({
                        "id": reason_id,
                        "reason": f"Número de registro no coincide. Documento muestra '{extracted_val}', API indica '{api_val}'"
                    })
                elif field_name == "certificate_number":
                    reasons.append({
                        "id": reason_id,
                        "reason": f"Número de certificación no coincide. Documento muestra '{extracted_val}', API indica '{api_val}'"
                    })
                elif field_name == "naics_code":
                    api_values = result.get("api_values", [])
                    reasons.append({
                        "id": reason_id,
                        "reason": f"Código NAICS no coincide. Documento muestra '{extracted_val}', códigos registrados: {api_values}"
                    })
                else:
                    reasons.append({
                        "id": reason_id,
                        "reason": f"Campo '{field_name}' no coincide con datos de API. Valor: '{extracted_val}', Esperado: '{api_val}'"
                    })
                reason_id += 1

        # ── Date Validation (3-step logic) ──────────────────────────────
        # Step 1: Try to parse expiration_date and issue_date
        # Per-document validity windows (days) when no expiration date is present
        VALIDITY_WINDOWS = [
            (r"SC-2942|IVU|Planillas.*IVU", 30),
            (r"SC-6088|Radicacion.*Planillas.*Ingresos", 30),
            (r"SC-6096|HACIENDA.*Deuda|Deuda.*HACIENDA", 30),
            (r"CRIM", 90),
            (r"DTRH", 90),
            (r"CFSE|Fondo.*Seguro", 90),
            (r"ASUME", 90),
            (r"SAM", 90),
        ]
        validity_window_days = 90  # default
        for pattern, days in VALIDITY_WINDOWS:
            if re.search(pattern, filename, re.IGNORECASE):
                validity_window_days = days
                break
        logger.info(f"Date validity window for '{filename}': {validity_window_days} days")
        now = datetime.now()

        # Spanish month name mapping
        _SPANISH_MONTHS = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
            "ene": "01", "feb": "02", "mar": "03", "abr": "04",
            "may": "05", "jun": "06", "jul": "07", "ago": "08",
            "sep": "09", "oct": "10", "nov": "11", "dic": "12",
        }

        def _parse_date(raw: str):
            """Try multiple date formats including Spanish months, return datetime or None."""
            if not raw:
                return None
            raw = raw.strip()

            # Normalize Spanish: "22 de mayo de 2025" → "22 mayo 2025"
            normalized = re.sub(r'\bde\b', '', raw, flags=re.IGNORECASE).strip()
            normalized = re.sub(r'\s+', ' ', normalized)

            # Replace Spanish month names with numeric
            for sp_month, num in _SPANISH_MONTHS.items():
                pattern = re.compile(re.escape(sp_month), re.IGNORECASE)
                if pattern.search(normalized):
                    normalized = pattern.sub(num, normalized)
                    break

            # Try many date formats on both raw and normalized versions
            date_formats = (
                "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", "%Y-%m-%d",
                "%d %m %Y", "%m %d %Y",
                "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y",
                "%d-%b-%Y", "%d-%B-%Y",
                "%m/%d/%y", "%d/%m/%y",
            )
            for candidate in (normalized, raw):
                for fmt in date_formats:
                    try:
                        return datetime.strptime(candidate.strip(), fmt)
                    except ValueError:
                        continue
            return None

        expiration_raw = extracted.get("expiration_date", "")
        issue_raw = extracted.get("issue_date", "") or extracted.get("date", "")
        exp_date = _parse_date(expiration_raw)
        issue_date = _parse_date(issue_raw)

        if exp_date:
            # Step 3 – Expiration Date present → compare to today
            if exp_date < now:
                reasons.append({
                    "id": reason_id,
                    "reason": "Your certificate is not current. Please request it again."
                })
                reason_id += 1
        elif issue_date:
            # Step 2 – No expiration, but issue date found → validity window check
            days_elapsed = (now - issue_date).days
            if days_elapsed > validity_window_days:
                reasons.append({
                    "id": reason_id,
                    "reason": f"Your certification is not current. Please request it again. It must be less than {validity_window_days} days since issuance. (Issued: {issue_raw}, {days_elapsed} days ago)"
                })
                reason_id += 1
        else:
            # Step 1 fail – Neither date found
            reasons.append({
                "id": reason_id,
                "reason": "Expiration or Issue date not found or could not be validated."
            })
            reason_id += 1

        # Check if document title suggests it's not the correct document type
        doc_title = extracted.get("document_title", "")
        required_docs = api_data.get("requiredDocuments", [])
        if doc_title and required_docs:
            title_norm = self._normalize(doc_title)
            found_match = False
            for req_doc in required_docs:
                req_name = self._normalize(req_doc.get("name", "") if isinstance(req_doc, dict) else str(req_doc))
                if req_name and (req_name in title_norm or title_norm in req_name or self._similarity(title_norm, req_name) > 0.6):
                    found_match = True
                    break
            if not found_match:
                reasons.append({
                    "id": reason_id,
                    "reason": f"El Documento presentado no es el requerido. Título detectado: '{doc_title}'"
                })
                reason_id += 1

        return reasons

    def _save_local(self, content, validation_result: dict):
        """Save validation report as a local JSON file."""
        folder = self.local_output_folder
        if not folder:
            return
        try:
            os.makedirs(folder, exist_ok=True)
            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            filename = f"{canonical_id}_validation.json"
            filepath = os.path.join(folder, filename)
            payload = {
                "canonical_id": canonical_id,
                "source_filename": getattr(content.id, 'filename', None),
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "validation": validation_result,
            }
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved validation report locally to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save local validation report: {e}")

    async def _get_connector(self):
        if self._connector is None:
            account = self.storage_account_name
            if not account:
                import os
                account = os.environ.get("BLOB_STORAGE_ACCOUNT_NAME", os.environ.get("STORAGE_ACCOUNT_NAME", ""))
            if not account:
                logger.warning("No storage_account_name configured; skipping blob upload of validation report.")
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

    async def _upload_validation_report(self, content, validation_result: dict):
        try:
            connector = await self._get_connector()
            if connector is None:
                return

            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            now = datetime.now(timezone.utc)
            blob_path = f"validation_reports/{now.strftime('%Y/%m/%d')}/{canonical_id}.json"

            payload = {
                "canonical_id": canonical_id,
                "source_filename": getattr(content.id, 'filename', None),
                "validated_at": now.isoformat(),
                "validation": validation_result,
            }
            data = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")

            await connector.upload_blob(
                container_name=self.container_name,
                blob_path=blob_path,
                data=data,
                overwrite=True,
            )
            logger.info(f"Uploaded validation report to {self.container_name}/{blob_path}")
            content.summary_data["validation_report_blob"] = {
                "container": self.container_name,
                "blob_path": blob_path,
                "blob_size": len(data),
                "write_status": "success",
            }
        except Exception as e:
            logger.error(f"Failed to upload validation report: {e}")
            content.summary_data["validation_report_blob"] = {
                "write_status": "error",
                "error": str(e),
            }

    async def _safe_process(self, content):
        """Wrap process_content_item so crashes always produce a validation report."""
        try:
            return await self.process_content_item(content)
        except Exception as e:
            import traceback
            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            logger.error(f"FIELD VALIDATION CRASHED for {canonical_id}: {type(e).__name__}: {e}")
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            validation_result = {
                "status": "error",
                "reason": f"Validation processing error: {type(e).__name__}: {e}",
                "rejectReasons": [],
            }
            content.data["validation_result"] = validation_result
            try:
                await self._upload_validation_report(content, validation_result)
                self._save_local(content, validation_result)
            except Exception as upload_err:
                logger.error(f"Failed to upload error report for {canonical_id}: {upload_err}")
            return content

    async def process_input(
        self,
        input: Union['Content', List['Content']],
        ctx: WorkflowContext[Union['Content', List['Content']], Union['Content', List['Content']]]
    ) -> Union['Content', List['Content']]:
        if isinstance(input, list):
            return [await self._safe_process(item) for item in input]
        else:
            return await self._safe_process(input)
