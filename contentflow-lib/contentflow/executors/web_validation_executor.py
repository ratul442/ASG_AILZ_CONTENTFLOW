import json
import logging
import os
import re
import aiohttp
from datetime import datetime, timezone
from .base import BaseExecutor

logger = logging.getLogger("contentflow.executors.web_validation_executor")

from agent_framework import WorkflowContext
from typing import Union, List
from ..connectors import AzureBlobConnector


class WebValidationExecutor(BaseExecutor):
    """
    Validates extracted document fields against external web portals.

    Currently supports:
    - CRIM (Centro de Recaudación de Ingresos Municipales): https://portal.crim360.com/certificados
      Validates certificate number + issue date

    Can be extended to support additional portals by adding new validation
    methods and configuring the portal_validations setting.
    """

    # Supported portals and the fields they require
    PORTAL_CONFIG = {
        "crim": {
            "name": "CRIM - Centro de Recaudación de Ingresos Municipales",
            "url": "https://portal.crim360.com/certificados",
            "validation_url": "https://portal.crim360.com/api/certificados/validar",
            "required_fields": ["certificate_number", "date"],
            "description": "Validates electronic certifications issued by CRIM",
        },
        "validacion_pr": {
            "name": "Validación Electrónica - Gobierno de Puerto Rico",
            "url": "https://validacion.pr.gov/",
            "required_fields": ["certificate_number", "ein_ssn"],
            "description": "Validates electronic certificates issued by PR government agencies (e.g., Department of Labor)",
            "agencies": {
                "department_of_labor": "Department of Labor",
                "dtrh": "Departamento del Trabajo y Recursos Humanos",
                "cfse": "Corporación del Fondo del Seguro del Estado",
                "hacienda": "Departamento de Hacienda",
                "estado": "Departamento de Estado",
                "salud": "Departamento de Salud",
            },
        },
    }

    def __init__(self, id: str, settings=None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)

        # Which portals to validate against (support both list and comma-separated string)
        portals_setting = self.get_setting("enabled_portals", default=["crim", "validacion_pr"])
        if isinstance(portals_setting, str):
            self.enabled_portals = [p.strip() for p in portals_setting.split(",") if p.strip()]
        else:
            self.enabled_portals = portals_setting
        self.timeout_seconds = self.get_setting("timeout_seconds", default=30)
        self.storage_account_name = self.get_setting("storage_account_name", default=None)
        self.credential_type = self.get_setting("credential_type", default="default_azure_credential")
        self.credential_key = self.get_setting("credential_key", default=None)
        self.container_name = self.get_setting("container_name", default="validation-results")
        self.local_output_folder = self.get_setting("local_output_folder", default=None)
        self._connector = None

    async def process_content_item(self, content):
        """Validate extracted fields against configured external web portals."""
        extracted = content.data.get("extracted_fields", {})

        if not extracted:
            logger.info(f"No extracted fields for {getattr(content.id, 'canonical_id', 'unknown')} — skipping web validation")
            content.data["web_validation_result"] = {
                "status": "skipped",
                "reason": "no_extracted_fields",
            }
            return content

        portal_results = {}

        for portal_id in self.enabled_portals:
            config = self.PORTAL_CONFIG.get(portal_id)
            if not config:
                logger.warning(f"Unknown portal '{portal_id}' — skipping")
                continue

            # Check if we have the required fields
            required = config["required_fields"]
            missing = [f for f in required if not extracted.get(f)]

            if missing:
                portal_results[portal_id] = {
                    "status": "skipped",
                    "reason": f"Missing required fields: {missing}",
                    "portal_name": config["name"],
                    "portal_url": config["url"],
                }
                continue

            # Dispatch to the appropriate validation method
            if portal_id == "crim":
                result = await self._validate_crim(extracted, config)
            elif portal_id == "validacion_pr":
                result = await self._validate_validacion_pr(extracted, content.data, config)
            else:
                result = {"status": "error", "reason": f"No validation handler for '{portal_id}'"}

            portal_results[portal_id] = result

        # Generate reject reasons from web validation
        reject_reasons = self._generate_web_reject_reasons(portal_results)

        web_validation_result = {
            "status": "validated",
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "portals_checked": len(portal_results),
            "portal_results": portal_results,
            "rejectReasons": reject_reasons,
        }

        content.data["web_validation_result"] = web_validation_result
        logger.info(
            f"Web validation for {getattr(content.id, 'canonical_id', 'unknown')}: "
            f"portals_checked={len(portal_results)}, reject_reasons={len(reject_reasons)}"
        )

        # Upload report
        await self._upload_report(content, web_validation_result)
        self._save_local(content, web_validation_result)

        return content

    async def _validate_crim(self, extracted: dict, config: dict) -> dict:
        """
        Validate a certificate against the CRIM portal.

        The CRIM portal at https://portal.crim360.com/certificados accepts:
        - Número de certificación (certificate_number)
        - Fecha de emisión (date/issue date)

        It returns whether the certificate is valid or not.
        """
        cert_number = extracted.get("certificate_number", "").strip()
        issue_date = extracted.get("date", "").strip()

        if not cert_number or not issue_date:
            return {
                "status": "skipped",
                "reason": "Certificate number or date not available",
                "portal_name": config["name"],
                "portal_url": config["url"],
            }

        # Normalize date to expected format (DD/MM/YYYY or MM/DD/YYYY)
        normalized_date = self._normalize_date(issue_date)

        logger.info(f"Validating certificate '{cert_number}' dated '{normalized_date}' against CRIM portal")

        try:
            async with aiohttp.ClientSession() as session:
                # Attempt API-based validation
                payload = {
                    "numeroCertificacion": cert_number,
                    "fechaEmision": normalized_date,
                }

                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "ContentFlow-Validator/1.0",
                }

                # Try the API endpoint first
                try:
                    async with session.post(
                        config["validation_url"],
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
                        ssl=False,
                    ) as response:
                        status_code = response.status

                        if status_code == 200:
                            resp_data = await response.json()
                            is_valid = resp_data.get("valid", resp_data.get("isValid", resp_data.get("resultado", False)))
                            return {
                                "status": "valid" if is_valid else "invalid",
                                "portal_name": config["name"],
                                "portal_url": config["url"],
                                "certificate_number": cert_number,
                                "issue_date": normalized_date,
                                "response": resp_data,
                                "http_status": status_code,
                            }
                        elif status_code == 404:
                            return {
                                "status": "not_found",
                                "portal_name": config["name"],
                                "portal_url": config["url"],
                                "certificate_number": cert_number,
                                "issue_date": normalized_date,
                                "http_status": status_code,
                                "details": "Certificate not found in CRIM portal",
                            }
                        elif status_code == 400:
                            return {
                                "status": "invalid_request",
                                "portal_name": config["name"],
                                "portal_url": config["url"],
                                "certificate_number": cert_number,
                                "issue_date": normalized_date,
                                "http_status": status_code,
                                "details": "Bad request - check certificate number and date format",
                            }
                        else:
                            return {
                                "status": "error",
                                "portal_name": config["name"],
                                "portal_url": config["url"],
                                "certificate_number": cert_number,
                                "issue_date": normalized_date,
                                "http_status": status_code,
                                "details": f"Unexpected response: {status_code}",
                            }

                except aiohttp.ClientError as e:
                    # If API endpoint doesn't exist, try form-based approach
                    logger.warning(f"CRIM API call failed: {e}. Trying form submission.")
                    return await self._validate_crim_form(session, cert_number, normalized_date, config)

        except Exception as e:
            logger.error(f"CRIM validation failed: {e}")
            return {
                "status": "error",
                "portal_name": config["name"],
                "portal_url": config["url"],
                "certificate_number": cert_number,
                "issue_date": normalized_date,
                "error": str(e),
            }

    async def _validate_crim_form(self, session: aiohttp.ClientSession, cert_number: str, date: str, config: dict) -> dict:
        """
        Fallback: Submit form data to the CRIM portal page directly.
        """
        try:
            form_data = aiohttp.FormData()
            form_data.add_field("numeroCertificacion", cert_number)
            form_data.add_field("fechaEmision", date)

            async with session.post(
                config["url"],
                data=form_data,
                timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
                ssl=False,
            ) as response:
                status_code = response.status
                body = await response.text()

                # Parse the response HTML for validation result
                is_valid = self._parse_crim_response(body)

                return {
                    "status": "valid" if is_valid else ("invalid" if is_valid is False else "inconclusive"),
                    "portal_name": config["name"],
                    "portal_url": config["url"],
                    "certificate_number": cert_number,
                    "issue_date": date,
                    "http_status": status_code,
                    "method": "form_submission",
                }

        except Exception as e:
            return {
                "status": "error",
                "portal_name": config["name"],
                "portal_url": config["url"],
                "certificate_number": cert_number,
                "issue_date": date,
                "error": str(e),
                "method": "form_submission",
            }

    def _parse_crim_response(self, html: str) -> bool | None:
        """
        Parse CRIM portal HTML response to determine if certificate is valid.
        Returns True (valid), False (invalid), or None (inconclusive).
        """
        html_lower = html.lower()

        # Look for positive indicators
        valid_patterns = [
            "certificación válida",
            "certificacion valida",
            "certificado válido",
            "válido",
            "vigente",
            "valid",
        ]
        invalid_patterns = [
            "no se encontró",
            "no encontrado",
            "no válido",
            "inválido",
            "invalido",
            "vencido",
            "expired",
            "not found",
            "error",
        ]

        for pattern in valid_patterns:
            if pattern in html_lower:
                return True

        for pattern in invalid_patterns:
            if pattern in html_lower:
                return False

        return None  # Inconclusive

    def _detect_agency(self, extracted: dict, api_data: dict) -> str:
        """Detect which PR government agency issued the document."""
        title = (extracted.get("document_title", "") or "").lower()
        filename = str(api_data.get("source_filename", "") or "").lower()
        combined = f"{title} {filename}"

        agency_keywords = {
            "department_of_labor": ["labor", "trabajo", "dtrh", "patrono", "desempleo", "obrero"],
            "cfse": ["cfse", "fondo del seguro", "seguro del estado", "compensaci"],
            "hacienda": ["hacienda", "income tax", "planilla", "contribuci", "ivu"],
            "estado": ["estado", "departamento de estado", "state department"],
            "salud": ["salud", "health", "sanitaria"],
        }

        for agency_id, keywords in agency_keywords.items():
            for kw in keywords:
                if kw in combined:
                    return agency_id
        return "department_of_labor"

    async def _validate_validacion_pr(self, extracted: dict, api_data: dict, config: dict) -> dict:
        """
        Validate a certificate against https://validacion.pr.gov/

        Required: Agency (auto-detected), Certificate Number, Last 4 digits of SSN/EIN.
        """
        cert_number = extracted.get("certificate_number", "").strip()
        ein_ssn = extracted.get("ein_ssn", "").strip()

        if not cert_number or not ein_ssn:
            return {
                "status": "skipped",
                "reason": f"Missing: certificate_number={'present' if cert_number else 'missing'}, ein_ssn={'present' if ein_ssn else 'missing'}",
                "portal_name": config["name"],
                "portal_url": config["url"],
            }

        ssn_digits = re.sub(r'\D', '', ein_ssn)
        last_four = ssn_digits[-4:] if len(ssn_digits) >= 4 else ssn_digits

        agency_id = self._detect_agency(extracted, api_data)
        agency_name = config["agencies"].get(agency_id, "Department of Labor")

        logger.info(f"Validating against validacion.pr.gov: agency='{agency_name}', cert='{cert_number}', last4='{last_four}'")

        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "ContentFlow-Validator/1.0",
                    "Origin": "https://validacion.pr.gov",
                    "Referer": "https://validacion.pr.gov/",
                }

                payload = {
                    "agencia": agency_name,
                    "numeroCertificado": cert_number,
                    "ultimosDigitos": last_four,
                }

                api_urls = [
                    "https://validacion.pr.gov/api/validate",
                    "https://validacion.pr.gov/api/certificados/validar",
                    "https://validacion.pr.gov/api/v1/validate",
                ]

                for api_url in api_urls:
                    try:
                        async with session.post(
                            api_url, json=payload, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=self.timeout_seconds), ssl=False,
                        ) as response:
                            if response.status == 200:
                                try:
                                    resp_data = await response.json()
                                except Exception:
                                    resp_data = {"raw_response": (await response.text())[:500]}

                                is_valid = self._parse_validacion_pr_response(resp_data)
                                return {
                                    "status": "valid" if is_valid else ("invalid" if is_valid is False else "inconclusive"),
                                    "portal_name": config["name"],
                                    "portal_url": config["url"],
                                    "agency": agency_name,
                                    "certificate_number": cert_number,
                                    "last_four_ssn": last_four,
                                    "response": resp_data,
                                    "http_status": response.status,
                                    "method": "api",
                                }
                            else:
                                continue
                    except aiohttp.ClientError:
                        continue

                # Fallback: form submission
                logger.warning("validacion.pr.gov API not found. Trying form submission.")
                return await self._validate_validacion_pr_form(session, agency_name, cert_number, last_four, config)

        except Exception as e:
            logger.error(f"validacion.pr.gov validation failed: {e}")
            return {
                "status": "error",
                "portal_name": config["name"],
                "portal_url": config["url"],
                "agency": agency_name,
                "certificate_number": cert_number,
                "last_four_ssn": last_four,
                "error": str(e),
            }

    async def _validate_validacion_pr_form(self, session, agency, cert_number, last_four, config):
        """Fallback: form submission to validacion.pr.gov."""
        try:
            form_data = aiohttp.FormData()
            form_data.add_field("agencia", agency)
            form_data.add_field("numeroCertificado", cert_number)
            form_data.add_field("ultimosDigitos", last_four)

            async with session.post(
                config["url"], data=form_data,
                timeout=aiohttp.ClientTimeout(total=self.timeout_seconds), ssl=False,
            ) as response:
                body = await response.text()
                is_valid = self._parse_validacion_pr_html(body)
                return {
                    "status": "valid" if is_valid else ("invalid" if is_valid is False else "inconclusive"),
                    "portal_name": config["name"],
                    "portal_url": config["url"],
                    "agency": agency,
                    "certificate_number": cert_number,
                    "last_four_ssn": last_four,
                    "http_status": response.status,
                    "method": "form_submission",
                }
        except Exception as e:
            return {
                "status": "error", "portal_name": config["name"], "portal_url": config["url"],
                "agency": agency, "certificate_number": cert_number, "last_four_ssn": last_four,
                "error": str(e), "method": "form_submission",
            }

    def _parse_validacion_pr_response(self, resp_data):
        """Parse validacion.pr.gov API response."""
        if not isinstance(resp_data, dict):
            return None
        for key in ["valid", "isValid", "valido", "resultado", "Resultado", "status"]:
            val = resp_data.get(key)
            if val is not None:
                if isinstance(val, bool):
                    return val
                if isinstance(val, str):
                    low = val.lower()
                    if any(w in low for w in ("válido", "vigente", "activo", "valid", "true")):
                        return True
                    if any(w in low for w in ("no válido", "vencido", "expirado", "invalid", "false")):
                        return False
        return None

    def _parse_validacion_pr_html(self, html):
        """Parse validacion.pr.gov HTML for Resultado field."""
        html_lower = html.lower()
        for p in ["no válido", "no valido", "vencido", "expirado", "no encontrado", "inválido"]:
            if p in html_lower:
                return False
        for p in ["válido", "vigente", "valid", "activo"]:
            if p in html_lower:
                return True
        return None

    def _normalize_date(self, date_str: str) -> str:
        """Normalize extracted date to DD/MM/YYYY format for CRIM portal."""
        date_str = date_str.strip()

        # Already in DD/MM/YYYY format
        if re.match(r'^\d{2}/\d{2}/\d{4}$', date_str):
            return date_str

        # Try various formats
        formats = [
            ("%d/%m/%Y", None),
            ("%m/%d/%Y", "%d/%m/%Y"),
            ("%d-%m-%Y", "%d/%m/%Y"),
            ("%Y-%m-%d", "%d/%m/%Y"),
            ("%d %B %Y", "%d/%m/%Y"),
            ("%d %b %Y", "%d/%m/%Y"),
            ("%B %d, %Y", "%d/%m/%Y"),
        ]

        for in_fmt, out_fmt in formats:
            try:
                dt = datetime.strptime(date_str, in_fmt)
                if out_fmt:
                    return dt.strftime(out_fmt)
                return date_str
            except ValueError:
                continue

        # Return as-is if can't parse
        return date_str

    def _generate_web_reject_reasons(self, portal_results: dict) -> list:
        """Generate reject reasons from web portal validation results."""
        reasons = []
        reason_id = 1

        for portal_id, result in portal_results.items():
            status = result.get("status", "")
            portal_name = result.get("portal_name", portal_id)
            cert_number = result.get("certificate_number", "")

            if status == "invalid":
                reasons.append({
                    "id": reason_id,
                    "reason": f"Certificación no válida según {portal_name}. Número: {cert_number}. Anejar certificación vigente"
                })
                reason_id += 1
            elif status == "not_found":
                reasons.append({
                    "id": reason_id,
                    "reason": f"Certificación no encontrada en {portal_name}. Número: {cert_number}. Verificar número de certificación"
                })
                reason_id += 1
            elif status == "error":
                reasons.append({
                    "id": reason_id,
                    "reason": f"No se pudo validar certificación en {portal_name}. Error de conexión. Verificar manualmente"
                })
                reason_id += 1

        return reasons

    # --- Storage methods ---

    def _save_local(self, content, web_validation_result: dict):
        """Save web validation report as a local JSON file."""
        folder = self.local_output_folder
        if not folder:
            return
        try:
            os.makedirs(folder, exist_ok=True)
            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            filename = f"{canonical_id}_web_validation.json"
            filepath = os.path.join(folder, filename)
            payload = {
                "canonical_id": canonical_id,
                "source_filename": getattr(content.id, 'filename', None),
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "web_validation": web_validation_result,
            }
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved web validation report locally to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save local web validation report: {e}")

    async def _get_connector(self):
        if self._connector is None:
            account = self.storage_account_name
            if not account:
                account = os.environ.get("BLOB_STORAGE_ACCOUNT_NAME", os.environ.get("STORAGE_ACCOUNT_NAME", ""))
            if not account:
                logger.warning("No storage_account_name configured; skipping blob upload.")
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

    async def _upload_report(self, content, web_validation_result: dict):
        try:
            connector = await self._get_connector()
            if connector is None:
                return

            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            now = datetime.now(timezone.utc)
            blob_path = f"web_validation_reports/{now.strftime('%Y/%m/%d')}/{canonical_id}.json"

            payload = {
                "canonical_id": canonical_id,
                "source_filename": getattr(content.id, 'filename', None),
                "validated_at": now.isoformat(),
                "web_validation": web_validation_result,
            }
            data = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")

            await connector.upload_blob(
                container_name=self.container_name,
                blob_path=blob_path,
                data=data,
                overwrite=True,
            )
            logger.info(f"Uploaded web validation report to {self.container_name}/{blob_path}")
            content.summary_data["web_validation_report_blob"] = {
                "container": self.container_name,
                "blob_path": blob_path,
                "blob_size": len(data),
                "write_status": "success",
            }
        except Exception as e:
            logger.error(f"Failed to upload web validation report: {e}")
            content.summary_data["web_validation_report_blob"] = {
                "write_status": "error",
                "error": str(e),
            }

    async def process_input(
        self,
        input: Union['Content', List['Content']],
        ctx: WorkflowContext[Union['Content', List['Content']], Union['Content', List['Content']]]
    ) -> Union['Content', List['Content']]:
        if isinstance(input, list):
            return [await self.process_content_item(item) for item in input]
        else:
            return await self.process_content_item(input)
