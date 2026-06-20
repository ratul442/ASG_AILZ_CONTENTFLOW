"""Browser-based certificate validation executor using Playwright.

Validates extracted document fields against external government web portals
that require multi-step navigation (SPAs, dropdowns, form submissions).
Uses Playwright for real browser automation.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union

try:
    from playwright.async_api import async_playwright, Page, BrowserContext
except ImportError:
    raise ImportError(
        "playwright is required for browser validation. "
        "Install it with: pip install playwright && playwright install"
    )

from agent_framework import WorkflowContext
from .base import BaseExecutor
from ..models import Content, ContentIdentifier
from ..connectors import AzureBlobConnector

logger = logging.getLogger("contentflow.executors.browser_validation_executor")


class BrowserValidationExecutor(BaseExecutor):
    """
    Validates certificates against external web portals using Playwright browser automation.

    Unlike simple HTTP-based validation, this executor launches a real browser to handle:
    - Single Page Applications (SPAs) with JavaScript rendering
    - Multi-step navigation (clicking menus, selecting dropdowns)
    - Dynamic form submissions with CSRF tokens and session cookies

    Supported portals:
    - hacienda: SURI (https://suri.hacienda.pr.gov/) - Filing certificates, Merchant Registration, etc.
    - crim: CRIM (https://portal.crim360.com/certificados) - Municipal certifications
    - validacion_pr: Validación PR (https://validacion.pr.gov/) - Government-wide validation

    Configuration (settings):
        - enabled_portals (str): Comma-separated portals to validate (e.g., "hacienda,crim,validacion_pr")
        - timeout_seconds (int): Browser navigation timeout per portal (default: 30)
        - screenshot_enabled (bool): Capture screenshots of validation results (default: false)
        - screenshot_output_dir (str): Directory for screenshots (default: "./validation_screenshots")
        - headless (bool): Run browser in headless mode (default: true)
        - storage_account_name (str): Azure Blob Storage account for uploading reports
        - container_name (str): Blob container name (default: "validation-results")
        - credential_type (str): Azure credential type (default: "default_azure_credential")
        - credential_key (str): Azure credential key (optional)

    Input:
        Content item(s) with 'extracted_fields' in data (from field_extractor).

    Output:
        Content item(s) with 'browser_validation_result' added to data, containing:
        - portal_results: Per-portal validation results
        - rejectReasons: List of rejection reasons with id and reason
    """

    # Portal definitions with navigation steps
    PORTAL_CONFIG = {
        "hacienda": {
            "name": "Hacienda - SURI (Sistema Unificado de Rentas Internas)",
            "url": "https://suri.hacienda.pr.gov/_/",
            "required_fields": ["ein_ssn", "certificate_number"],
            "description": "Validates certificates and licenses issued by Departamento de Hacienda",
            "certificate_types": {
                "debt_certificate": "Certificación de Deuda",
                "filing_income_tax": "Certificación de Radicación - Planilla de Contribución sobre Ingresos",
                "filing_sales_tax": "Certificación de Radicación - Planilla Mensual de Impuesto sobre Ventas y Uso",
                "merchant_registration": "Certificado de Registro de Comerciante",
                "negative_merchant": "Inscripción Negativa en Registro de Comerciante",
                "withholding_waiver": "Certificado de Relevo de Retención en el Origen por Servicios Prestados en P. R.",
                "merchant_exemption": "Certificado de Exención de Comerciante",
                "licenses": "Licencias",
                "estate_gift_release": "Relevo de Herencia y de Donación",
                "estate_gift_auth": "Carta de Autorización de Herencia o Donación",
                "vehicle_excise": "Certificación de Pago de Arbitrios sobre Vehículos",
                "return_specialist": "ID de Especialista en Planillas",
            },
        },
        "crim": {
            "name": "CRIM - Centro de Recaudación de Ingresos Municipales",
            "url": "https://portal.crim360.com/certificados",
            "required_fields": ["certificate_number", "date"],
            "description": "Validates electronic certifications issued by CRIM",
        },
        "sam_gov": {
            "name": "SAM.gov - System for Award Management",
            "url": "https://sam.gov/search",
            "required_fields": ["unique_entity_id"],
            "description": "Validates Unique Entity ID (UEI) registration on SAM.gov",
        },
        "validacion_pr": {
            "name": "Validación Electrónica - Gobierno de Puerto Rico",
            "url": "https://validacion.pr.gov/",
            "required_fields": ["certificate_number", "ein_ssn"],
            "description": "Validates electronic certificates issued by PR government agencies",
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

    def __init__(self, id: str, settings: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)

        # Portal settings
        portals_setting = self.get_setting("enabled_portals", default="hacienda,crim,validacion_pr,sam_gov")
        if isinstance(portals_setting, str):
            self.enabled_portals = [p.strip() for p in portals_setting.split(",") if p.strip()]
        else:
            self.enabled_portals = portals_setting

        # Browser settings
        self.timeout_seconds = self.get_setting("timeout_seconds", default=20)
        self.headless = self.get_setting("headless", default=True)
        self.user_agent = self.get_setting(
            "user_agent",
            default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        )

        # Screenshot settings
        self.screenshot_enabled = self.get_setting("screenshot_enabled", default=False)
        self.screenshot_output_dir = self.get_setting("screenshot_output_dir", default="./validation_screenshots")

        # Blob storage settings
        self.storage_account_name = self.get_setting("storage_account_name", default=None)
        self.credential_type = self.get_setting("credential_type", default="default_azure_credential")
        self.credential_key = self.get_setting("credential_key", default=None)
        self.container_name = self.get_setting("container_name", default="validation-results")
        self.local_output_folder = self.get_setting("local_output_folder", default=None)
        self._connector = None

        # Create screenshot directory if needed
        if self.screenshot_enabled:
            os.makedirs(self.screenshot_output_dir, exist_ok=True)

    # Filename patterns → portal mapping (case-insensitive)
    # Replaces hardcoded index mapping so it works across different tramites
    FILENAME_PORTAL_PATTERNS = {
        "hacienda": [
            r"SC-6088|Radicacion.*Planillas.*Ingresos",
            r"SC-6096|Certificacion.*De.*Deuda.*HACIENDA|HACIENDA.*Deuda",
            r"SC-2942|Planillas.*IVU|IVU",
            r"Registro.*Comerciante|Merchant.*Registration",
            r"HACIENDA_|HACIENDA-",
        ],
        "crim": [
            r"CRIM",
        ],
        "validacion_pr": [
            r"Antecedentes.*Penales|Policia|Policía|Criminal",
            r"Desempleo|Incapacidad|DTRH",
            r"Choferil",
            r"ASUME",
        ],
        "sam_gov": [
            r"SAM|Entity.*Information.*SAM",
        ],
    }

    def _classify_doc_type_from_filename(self, content) -> str:
        """Get the source filename from content."""
        filename = getattr(content.id, 'filename', '') or ''
        if not filename:
            # Try to extract from canonical_id or other data fields
            filename = getattr(content.id, 'canonical_id', '') or ''
        return filename

    def _get_eligible_portals(self, canonical_id: str, content=None) -> list:
        """Return portals relevant for this document based on filename patterns."""
        # Get filename from content or canonical_id
        filename = ''
        if content is not None:
            filename = self._classify_doc_type_from_filename(content)
        if not filename:
            filename = canonical_id

        eligible = []
        for portal_id in self.enabled_portals:
            patterns = self.FILENAME_PORTAL_PATTERNS.get(portal_id, [])
            for pattern in patterns:
                if re.search(pattern, filename, re.IGNORECASE):
                    eligible.append(portal_id)
                    break

        if not eligible:
            logger.info(f"No eligible portals for document: {filename}")
        else:
            logger.info(f"Eligible portals for '{filename}': {eligible}")
        return eligible

    @staticmethod
    def _normalize_fields(extracted: dict) -> dict:
        """Create a normalised copy of extracted_fields so that alternate
        key names are mapped to the canonical names expected by portals.

        Canonical keys:
          ein_ssn          – Taxpayer ID / EIN / SSN
          certificate_number – Certificate / correspondence number
          date             – Issue / emission date (for CRIM)
        """
        norm = dict(extracted)  # shallow copy

        # --- ein_ssn ---------------------------------------------------------
        if not norm.get("ein_ssn"):
            for alt in ("id_de_contribuyente", "taxpayer_id", "contribuyente"):
                val = extracted.get(alt, "")
                if val:
                    norm["ein_ssn"] = val
                    break
        # If still missing, try to build from ssn_last_four
        if not norm.get("ein_ssn") and extracted.get("ssn_last_four"):
            norm["ein_ssn"] = extracted["ssn_last_four"]

        # --- certificate_number ----------------------------------------------
        if not norm.get("certificate_number"):
            for alt in ("id_de_correspondencia", "correspondence_id",
                        "registration_number", "merchant_registration",
                        "letter_id", "solicitud_number", "numero_solicitud",
                        "request_number", "application_number"):
                val = extracted.get(alt, "")
                if val and val.lower() not in ("nombre", ""):
                    norm["certificate_number"] = val
                    break

        # --- date (for CRIM) -------------------------------------------------
        if not norm.get("date"):
            for alt in ("issue_date", "emission_date", "fecha_emision"):
                val = extracted.get(alt, "")
                if val:
                    norm["date"] = val
                    break

        # --- unique_entity_id (for SAM.gov) --------------------------------
        if not norm.get("unique_entity_id"):
            for alt in ("uniqueEntityId", "uei", "sam_uei", "entity_id"):
                val = extracted.get(alt, "")
                if val:
                    norm["unique_entity_id"] = val
                    break

        return norm

    async def process_content_item(self, content):
        """Validate extracted fields against configured portals using browser automation."""
        extracted = content.data.get("extracted_fields", {})

        if not extracted:
            logger.info(f"No extracted fields for {getattr(content.id, 'canonical_id', 'unknown')} — skipping browser validation")
            content.data["browser_validation_result"] = {
                "status": "skipped",
                "reason": "no_extracted_fields",
            }
            return content

        # Normalise alternate field names so portals can find them
        extracted = self._normalize_fields(extracted)
        logger.info(
            f"Normalised fields for {getattr(content.id, 'canonical_id', 'unknown')}: "
            f"ein_ssn={extracted.get('ein_ssn','')!r}, "
            f"certificate_number={extracted.get('certificate_number','')!r}, "
            f"date={extracted.get('date','')!r}"
        )

        # Only check portals relevant to this document
        canonical_id = getattr(content.id, 'canonical_id', 'unknown')
        eligible_portals = self._get_eligible_portals(canonical_id, content=content)
        if not eligible_portals:
            content.data["browser_validation_result"] = {
                "status": "skipped",
                "reason": "no_eligible_portals_for_doc_type",
            }
            return content

        portal_results = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=self.user_agent)

            try:
                for portal_id in eligible_portals:
                    config = self.PORTAL_CONFIG.get(portal_id)
                    if not config:
                        logger.warning(f"Unknown portal '{portal_id}' — skipping")
                        continue

                    # Check required fields — Policía (_04) uses application_number instead of certificate_number
                    is_policia = False
                    try:
                        doc_idx = int(canonical_id.rsplit('_', 1)[-1])
                        is_policia = (doc_idx == 4)
                    except (ValueError, IndexError):
                        pass

                    if is_policia and portal_id == "validacion_pr":
                        # Policía only needs application_number (mapped to certificate_number)
                        if not extracted.get("certificate_number"):
                            portal_results[portal_id] = {
                                "status": "skipped",
                                "reason": "Missing required fields: ['application_number']",
                                "portal_name": config["name"],
                                "portal_url": config["url"],
                            }
                            continue
                    else:
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

                    # Dispatch to portal-specific handler
                    try:
                        if portal_id == "hacienda":
                            h_data = dict(content.data)
                            h_data["source_filename"] = getattr(content.id, 'filename', None) or ''
                            result = await self._validate_hacienda(context, extracted, h_data, config)
                        elif portal_id == "crim":
                            result = await self._validate_crim(context, extracted, config)
                        elif portal_id == "sam_gov":
                            result = await self._validate_sam_gov(context, extracted, config)
                        elif portal_id == "validacion_pr":
                            vp_data = dict(content.data)
                            vp_data["canonical_id"] = canonical_id
                            result = await self._validate_validacion_pr(context, extracted, vp_data, config)
                        else:
                            result = {"status": "error", "reason": f"No handler for '{portal_id}'"}

                        portal_results[portal_id] = result

                    except Exception as e:
                        logger.error(f"Browser validation failed for {portal_id}: {e}")
                        portal_results[portal_id] = {
                            "status": "error",
                            "portal_name": config["name"],
                            "portal_url": config["url"],
                            "error": str(e),
                        }

            finally:
                await context.close()
                await browser.close()

        # Map internal statuses to customer-facing output
        for pid, res in portal_results.items():
            res["final_status"] = self._to_final_status(res.get("status", ""))

        # Generate reject reasons
        reject_reasons = self._generate_reject_reasons(portal_results)

        browser_validation_result = {
            "status": "validated",
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "portals_checked": len(portal_results),
            "portal_results": portal_results,
            "rejectReasons": reject_reasons,
        }

        content.data["browser_validation_result"] = browser_validation_result
        logger.info(
            f"Browser validation for {getattr(content.id, 'canonical_id', 'unknown')}: "
            f"portals_checked={len(portal_results)}, reject_reasons={len(reject_reasons)}"
        )

        # Upload and save
        await self._upload_report(content, browser_validation_result)
        self._save_local(content, browser_validation_result)

        return content

    # ─── HACIENDA SURI ──────────────────────────────────────────────

    def _detect_certificate_type(self, extracted: dict, api_data: dict = None) -> str:
        """Detect Hacienda certificate type from document title, filename, and requirementName.

        IMPORTANT: Check more specific types (debt, IVU) before generic ones (planilla/radicación)
        to avoid false matches.
        """
        title = (extracted.get("document_title", "") or "").lower()
        filename = ""
        requirement = ""
        if api_data:
            filename = str(api_data.get("source_filename", "") or "").lower()
            requirement = str(api_data.get("requirementName", "") or "").lower()
        combined = f"{title} {filename} {requirement}"

        # Ordered list — more specific types MUST come before generic ones
        keyword_map = [
            ("debt_certificate", ["sc-6096", "sc 6096", "deuda", "debt", "certificación de deuda", "certificado de deuda"]),
            ("filing_sales_tax", ["sc-2942", "sc 2942", "ivu", "ventas y uso", "sales and use tax", "registro de comerciante", "declaración mensual"]),
            ("merchant_registration", ["merchant registration", "certificado de registro de comerciante"]),
            ("negative_merchant", ["negative merchant", "inscripción negativa"]),
            ("withholding_waiver", ["withholding waiver", "relevo de retención"]),
            ("merchant_exemption", ["merchant exemption", "exención"]),
            ("licenses", ["licencia", "license"]),
            ("estate_gift_release", ["estate", "gift release", "herencia"]),
            ("estate_gift_auth", ["authorization letter", "carta de autorización"]),
            ("vehicle_excise", ["vehicle excise", "arbitrio", "vehículo"]),
            ("return_specialist", ["return specialist", "especialista en planillas"]),
            ("filing_income_tax", ["sc-6088", "sc 6088", "planilla", "income tax", "contribución sobre ingresos", "radicación"]),
        ]

        for cert_type, keywords in keyword_map:
            for kw in keywords:
                if kw in combined:
                    logger.info(f"Detected cert type '{cert_type}' from keyword '{kw}'")
                    return cert_type

        return "filing_income_tax"

    async def _validate_hacienda(self, context: BrowserContext, extracted: dict, api_data: dict, config: dict) -> dict:
        """
        Validate certificate via Hacienda SURI using Playwright.

        Based on proven working approach:
        1. Go to https://suri.hacienda.pr.gov/_/
        2. Click "Valide certificados y licencias"
        3. Select certificate type
        4. Fill visible text inputs (nth 0 = taxpayer, nth 1 = letter)
        5. Click visible Buscar button
        6. Read result from #caption2_Dd-69
        """
        taxpayer_id = re.sub(r'\D', '', extracted.get("ein_ssn", "") or extracted.get("id_de_contribuyente", ""))
        letter_id = (extracted.get("certificate_number", "") or extracted.get("id_de_correspondencia", "")).strip()
        cert_type = self._detect_certificate_type(extracted, api_data)
        cert_type_name = config["certificate_types"].get(cert_type, "Certificación de Radicación - Planilla de Contribución sobre Ingresos")

        logger.info(f"Hacienda SURI: taxpayer='{taxpayer_id}', letter='{letter_id}', type='{cert_type_name}'")

        page = await context.new_page()
        try:
            # Step 1: Navigate to SURI SPA — note the /_/ suffix
            suri_url = "https://suri.hacienda.pr.gov/_/"
            await page.goto(suri_url, wait_until="domcontentloaded", timeout=120000)
            await page.wait_for_timeout(5000)
            logger.debug("Hacienda: Loaded SPA homepage")

            # Step 2: Click "Valide certificados y licencias"
            validate_link = page.get_by_text("Valide certificados y licencias", exact=False).first
            if await validate_link.count() > 0:
                await validate_link.click()
                await page.wait_for_timeout(5000)
                logger.debug("Hacienda: Clicked 'Valide certificados y licencias'")
            else:
                return {
                    "status": "error",
                    "portal_name": config["name"],
                    "portal_url": suri_url,
                    "error": "Could not find 'Valide certificados y licencias' link",
                    "method": "browser",
                }

            # Step 3: Select certificate type
            cert_type_link = page.get_by_text(cert_type_name, exact=False).first
            if await cert_type_link.count() > 0:
                await cert_type_link.click()
                await page.wait_for_timeout(5000)
                logger.debug(f"Hacienda: Selected certificate type '{cert_type_name}'")
            else:
                # Try all known certificate types
                found = False
                for display_name in config["certificate_types"].values():
                    link = page.get_by_text(display_name, exact=False).first
                    if await link.count() > 0:
                        await link.click()
                        await page.wait_for_timeout(5000)
                        cert_type_name = display_name
                        found = True
                        break
                if not found:
                    return {
                        "status": "error",
                        "portal_name": config["name"],
                        "portal_url": suri_url,
                        "error": f"Could not find certificate type '{cert_type_name}'",
                        "method": "browser",
                    }

            # Wait for form to render
            await page.wait_for_timeout(3000)

            # Step 4: Fill visible text inputs — nth(0) = taxpayer, nth(1) = letter
            visible_inputs = page.locator("input[type='text']:visible")
            input_count = await visible_inputs.count()
            logger.debug(f"Hacienda: Found {input_count} visible text inputs")

            if input_count == 0:
                return {
                    "status": "error",
                    "portal_name": config["name"],
                    "portal_url": suri_url,
                    "error": f"No visible inputs found",
                    "method": "browser",
                }

            if input_count == 1:
                # Some cert types (IVU/SC-2942) only show one input field
                await visible_inputs.nth(0).fill(taxpayer_id)
                await page.wait_for_timeout(1000)
                logger.debug(f"Hacienda: Single input mode — filled taxpayer='{taxpayer_id}'")
            else:
                await visible_inputs.nth(0).fill(taxpayer_id)
                await page.wait_for_timeout(1000)
                await visible_inputs.nth(1).fill(letter_id)
                await page.wait_for_timeout(1000)
                logger.debug(f"Hacienda: Filled taxpayer='{taxpayer_id}', letter='{letter_id}'")

            # Step 5: Click visible Buscar button
            buscar_button = page.locator("button:visible").filter(has_text="Buscar").first
            if await buscar_button.count() > 0:
                await buscar_button.click()
                logger.debug("Hacienda: Clicked Buscar")
            else:
                return {
                    "status": "error",
                    "portal_name": config["name"],
                    "portal_url": suri_url,
                    "error": "Could not find visible Buscar button",
                    "method": "browser",
                }

            # Step 6: Wait for result and read from #caption2_Dd-69
            await page.wait_for_timeout(8000)

            result_text = ""
            result_locator = page.locator("#caption2_Dd-69")
            if await result_locator.count() > 0:
                result_text = (await result_locator.first.inner_text()).strip()
                logger.info(f"Hacienda: Result text from #caption2_Dd-69: '{result_text}'")
            else:
                # Fallback: try to read any visible result area
                result_text = await page.locator("body").text_content() or ""
                result_text = result_text[:500]
                logger.warning("Hacienda: #caption2_Dd-69 not found, using body text fallback")

            # Determine status from result text
            result = self._parse_hacienda_result(result_text)

            # Capture screenshot if enabled
            screenshot_path = None
            if self.screenshot_enabled:
                screenshot_path = await self._save_screenshot(page, "hacienda", extracted)

            return {
                "status": result["status"],
                "portal_name": config["name"],
                "portal_url": config["url"],
                "taxpayer_id": taxpayer_id,
                "letter_id": letter_id,
                "certificate_type": cert_type_name,
                "result_text": result.get("text", ""),
                "method": "browser",
                "screenshot_path": screenshot_path,
            }

        except Exception as e:
            logger.error(f"Hacienda browser validation failed: {e}")
            # Try to capture screenshot on error
            if self.screenshot_enabled:
                try:
                    await self._save_screenshot(page, "hacienda_error", extracted)
                except Exception:
                    pass
            return {
                "status": "error",
                "portal_name": config["name"],
                "portal_url": config["url"],
                "taxpayer_id": taxpayer_id,
                "letter_id": letter_id,
                "error": str(e),
                "method": "browser",
            }
        finally:
            await page.close()

    # ─── SAM.GOV ────────────────────────────────────────────────────

    async def _validate_sam_gov(self, context: BrowserContext, extracted: dict, config: dict) -> dict:
        """
        Validate Unique Entity ID on SAM.gov.

        1. Go to https://sam.gov/search
        2. Enter UEI in search box
        3. Click Search
        4. Check if entity is found and active
        """
        uei = (extracted.get("unique_entity_id", "") or "").strip()
        logger.info(f"SAM.gov: UEI='{uei}'")

        page = await context.new_page()
        try:
            # Step 1: Navigate to SAM.gov search
            await page.goto(config["url"], wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            logger.debug("SAM.gov: Page loaded")

            # Step 2: Find search input and enter UEI
            search_input = page.locator("input[type='text']:visible, input[type='search']:visible").first
            if await search_input.count() > 0:
                await search_input.fill(uei)
                await page.wait_for_timeout(1000)
                logger.debug(f"SAM.gov: Filled UEI '{uei}'")
            else:
                return {
                    "status": "error",
                    "portal_name": config["name"],
                    "portal_url": config["url"],
                    "error": "Could not find search input",
                    "method": "browser",
                }

            # Step 3: Click Search button
            search_btn = page.locator("button:visible").filter(has_text="Search").first
            if await search_btn.count() > 0:
                await search_btn.click()
                logger.debug("SAM.gov: Clicked Search")
            else:
                # Try pressing Enter
                await search_input.press("Enter")
                logger.debug("SAM.gov: Pressed Enter")

            # Step 4: Wait for results
            await page.wait_for_timeout(5000)

            # Step 5: Parse result
            body_text = await page.locator("body").text_content() or ""
            text_lower = body_text.lower()

            # Check for the UEI in results (indicates entity found)
            if uei.lower() in text_lower:
                # Check if active
                if any(w in text_lower for w in ["active", "activo", "active registration"]):
                    status = "valid"
                elif any(w in text_lower for w in ["expired", "inactive", "expirado"]):
                    status = "invalid"
                else:
                    status = "valid"  # Found but can't determine active/inactive
            elif "no results" in text_lower or "0 results" in text_lower:
                status = "not_found"
            else:
                status = "inconclusive"

            return {
                "status": status,
                "portal_name": config["name"],
                "portal_url": config["url"],
                "unique_entity_id": uei,
                "result_text": body_text[:500],
                "method": "browser",
            }

        except Exception as e:
            logger.error(f"SAM.gov browser validation failed: {e}")
            return {
                "status": "error",
                "portal_name": config["name"],
                "portal_url": config["url"],
                "unique_entity_id": uei,
                "error": str(e),
                "method": "browser",
            }
        finally:
            await page.close()

    # ─── CRIM ───────────────────────────────────────────────────────

    async def _validate_crim(self, context: BrowserContext, extracted: dict, config: dict) -> dict:
        """
        Validate certificate via CRIM portal using Playwright.

        Portal: https://portal.crim360.com/certificados
        Fields: "Número de certificación" (text) + "Fecha de emisión" (date picker, format D/mon/YYYY)
        Buttons: "Limpiar campos" + "Buscar" (orange)
        Success: Table populates with Número de solicitud, Tipo de certificación, Propiedad, etc.
        Failure: "No se encontró la certificación con la información suministrada."
        """
        cert_number = extracted.get("certificate_number", "").strip()
        issue_date_raw = extracted.get("date", "").strip()

        # Parse the date into components for the date picker
        day, month, year = "", "", ""
        if issue_date_raw:
            import re as _re
            # Try MM/DD/YYYY
            m = _re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', issue_date_raw)
            if m:
                p1, p2, yr = m.groups()
                if int(p1) <= 12:
                    month, day, year = p1, p2, yr
                else:
                    day, month, year = p1, p2, yr
            # Try YYYY-MM-DD
            m2 = _re.match(r'^(\d{4})-(\d{2})-(\d{2})$', issue_date_raw)
            if m2:
                year, month, day = m2.groups()
            # Try DD/MM/YYYY where first > 12
            if not year and m:
                day, month, year = p1, p2, yr

        # CRIM date picker uses Spanish month abbreviations: ene, feb, mar, abr, may, jun, jul, ago, sep, oct, nov, dic
        MONTH_NAMES_ES = {
            "1": "ene", "2": "feb", "3": "mar", "4": "abr", "5": "may", "6": "jun",
            "7": "jul", "8": "ago", "9": "sep", "10": "oct", "11": "nov", "12": "dic",
            "01": "ene", "02": "feb", "03": "mar", "04": "abr", "05": "may", "06": "jun",
            "07": "jul", "08": "ago", "09": "sep", "10": "oct", "11": "nov", "12": "dic",
        }
        month_name = MONTH_NAMES_ES.get(month, month)
        # Format: D/mon/YYYY (e.g., 6/may/2026, 22/may/2025)
        crim_date = f"{int(day)}/{month_name}/{year}" if day and month_name and year else issue_date_raw

        logger.info(f"CRIM: cert='{cert_number}', date_raw='{issue_date_raw}', crim_date='{crim_date}'")

        page = await context.new_page()
        try:
            # Step 1: Navigate to CRIM
            await page.goto(config["url"], wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            logger.debug("CRIM: Page loaded")

            # Step 2: Fill certificate number (first text input)
            cert_input = page.locator("input[type='text']:visible").first
            if await cert_input.count() > 0:
                await cert_input.fill(cert_number)
                await page.wait_for_timeout(500)
                logger.debug(f"CRIM: Filled cert number '{cert_number}'")
            else:
                return {
                    "status": "error",
                    "portal_name": config["name"],
                    "portal_url": config["url"],
                    "error": "Could not find certificate number input",
                    "method": "browser",
                }

            # Step 3: Fill date — try the date input (may be type='text' or type='date' or a picker)
            # The CRIM date field accepts text in D/mon/YYYY format
            date_filled = False
            # Try all input types that could be a date field
            for date_selector in [
                "input[type='date']:visible",
                "input[placeholder*='echa']:visible",
                "input[aria-label*='echa']:visible",
                "input[type='text']:visible",
            ]:
                date_inputs = page.locator(date_selector)
                count = await date_inputs.count()
                for i in range(count):
                    inp = date_inputs.nth(i)
                    # Skip the cert number input (already filled)
                    val = await inp.input_value()
                    if val == cert_number:
                        continue
                    # This should be the date field
                    await inp.click()
                    await page.wait_for_timeout(300)
                    # Clear and type the date
                    await inp.fill("")
                    await inp.type(crim_date, delay=50)
                    await page.wait_for_timeout(500)
                    # Press Escape to close any date picker popup
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(300)
                    date_filled = True
                    logger.debug(f"CRIM: Filled date '{crim_date}' via {date_selector}")
                    break
                if date_filled:
                    break

            if not date_filled:
                logger.warning(f"CRIM: Could not fill date field with '{crim_date}'")

            # Step 4: Click "Buscar" button (orange)
            buscar_btn = page.locator("button:visible").filter(has_text="Buscar").first
            if await buscar_btn.count() > 0:
                await buscar_btn.click()
                logger.debug("CRIM: Clicked Buscar")
            else:
                # Try anchor tag styled as button
                buscar_link = page.locator("a:visible").filter(has_text="Buscar").first
                if await buscar_link.count() > 0:
                    await buscar_link.click()
                    logger.debug("CRIM: Clicked Buscar (link)")
                else:
                    logger.warning("CRIM: Could not find Buscar button")

            # Step 5: Wait for result
            await page.wait_for_timeout(5000)

            # Step 6: Read result — check for error message or table data
            result_text = ""
            status = "inconclusive"

            # Check for the specific CRIM error message
            error_msg = page.locator("text=No se encontró la certificación")
            if await error_msg.count() > 0:
                result_text = "No se encontró la certificación con la información suministrada."
                status = "not_found"
                logger.info(f"CRIM: Certificate not found")
            else:
                # Check if result table has data rows (success)
                table_rows = page.locator("table tbody tr, table tr").all()
                rows = await page.locator("table tbody tr").count()
                if rows > 0:
                    # Read table content
                    table_text = await page.locator("table").first.text_content() or ""
                    result_text = table_text.strip()[:500]
                    # If table has actual data (not just headers), it's valid
                    if any(kw in result_text.lower() for kw in ["solicitud", "propiedad", cert_number.lower()]):
                        status = "valid"
                        logger.info(f"CRIM: Certificate found in table")
                    else:
                        status = "inconclusive"
                else:
                    # Fallback: read main content area
                    main_content = await page.evaluate("""
                        () => {
                            const nav = document.querySelector('nav, header, .navbar');
                            if (nav) nav.remove();
                            const main = document.querySelector('main, .container, .content') || document.body;
                            return main.innerText.substring(0, 500);
                        }
                    """)
                    result_text = main_content or ""
                    if "no se encontró" in result_text.lower():
                        status = "not_found"

            screenshot_path = None
            if self.screenshot_enabled:
                screenshot_path = await self._save_screenshot(page, "crim", extracted)

            return {
                "status": status,
                "portal_name": config["name"],
                "portal_url": config["url"],
                "certificate_number": cert_number,
                "issue_date": crim_date,
                "result_text": result_text,
                "method": "browser",
                "screenshot_path": screenshot_path,
            }

        except Exception as e:
            logger.error(f"CRIM browser validation failed: {e}")
            return {
                "status": "error",
                "portal_name": config["name"],
                "portal_url": config["url"],
                "certificate_number": cert_number,
                "issue_date": crim_date,
                "error": str(e),
                "method": "browser",
            }
        finally:
            await page.close()

    # ─── VALIDACIÓN PR ──────────────────────────────────────────────

    def _detect_agency(self, extracted: dict, api_data: dict) -> str:
        """Detect which PR government agency issued the document.

        Customer's Validación PR documents:
          _04: Criminal Record (Policía) → Negociado de la Policía
          _10: DTRH Unemployment/Disability Insurance → DTRH
          _11: DTRH Choferil (Driver Insurance) → DTRH
          _14: ASUME Child Support → ASUME
        """
        # Try doc index first for precise mapping
        canonical_id = str(api_data.get("canonical_id", "") or "")
        try:
            idx = int(canonical_id.rsplit('_', 1)[-1])
        except (ValueError, IndexError):
            idx = -1

        # Exact dropdown option text from validacion.pr.gov
        DOC_INDEX_TO_AGENCY = {
            4:  "Policía",
            10: "Departamento del Trabajo y Recursos Humanos",
            11: "Departamento del Trabajo y Recursos Humanos",
            14: "ASUME - patrono",
        }
        if idx in DOC_INDEX_TO_AGENCY:
            return DOC_INDEX_TO_AGENCY[idx]

        # Fallback: keyword detection from title + filename
        title = (extracted.get("document_title", "") or "").lower()
        filename = str(api_data.get("source_filename", "") or "").lower()
        # Also check canonical_id for filename patterns
        combined = f"{title} {filename} {canonical_id.lower()}"

        agency_keywords = {
            "Policía": ["policia", "policía", "penal", "antecedentes", "criminal"],
            "ASUME - patrono": ["asume", "child support", "pensión alimentaria", "familia"],
            "Departamento del Trabajo y Recursos Humanos": ["labor", "trabajo", "dtrh", "patrono", "desempleo", "obrero", "choferil", "incapacidad"],
            "ADSEF": ["adsef"],
        }

        for agency_name, keywords in agency_keywords.items():
            for kw in keywords:
                if kw in combined:
                    return agency_name
        return "Department of Labor"

    async def _validate_validacion_pr(self, context: BrowserContext, extracted: dict, api_data: dict, config: dict) -> dict:
        """
        Validate certificate via validacion.pr.gov using Playwright.

        Navigation: Single page with dropdown + two fields.
        1. Go to https://validacion.pr.gov/
        2. Select Agency from dropdown
        3. Enter Número de Certificado
        4. Enter Últimos 4 dígitos del SSN
        5. Click Validar and read result
        """
        cert_number = extracted.get("certificate_number", "").strip()
        ein_ssn = extracted.get("ein_ssn", "").strip()
        ssn_digits = re.sub(r'\D', '', ein_ssn)
        last_four = ssn_digits[-4:] if len(ssn_digits) >= 4 else ssn_digits
        agency = self._detect_agency(extracted, api_data)

        logger.info(f"Validación PR: agency='{agency}', cert='{cert_number}', last4='{last_four}'")

        # Policía (_04) uses a different portal flow
        if agency == "Policía":
            return await self._validate_policia(context, extracted, api_data, config)

        page = await context.new_page()
        try:
            # Step 1: Navigate
            await page.goto(config["url"], wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            logger.debug("Validación PR: Page loaded")

            # Step 2: Select Agency from dropdown
            agency_select = page.locator("select:visible").first
            if await agency_select.count() > 0:
                options = await agency_select.locator("option").all_text_contents()
                logger.debug(f"Validación PR: Available agencies: {options}")
                selected = False
                for opt in options:
                    opt_clean = opt.strip()
                    if not opt_clean or opt_clean == '-':
                        continue
                    # Exact match first
                    if agency.lower() == opt_clean.lower():
                        await agency_select.select_option(label=opt_clean)
                        agency = opt_clean
                        selected = True
                        logger.debug(f"Validación PR: Exact match agency '{opt_clean}'")
                        break
                if not selected:
                    # Partial match fallback
                    for opt in options:
                        opt_clean = opt.strip()
                        if not opt_clean or opt_clean == '-':
                            continue
                        if (agency.lower() in opt_clean.lower() or
                            opt_clean.lower() in agency.lower()):
                            await agency_select.select_option(label=opt_clean)
                            agency = opt_clean
                            selected = True
                            logger.debug(f"Validación PR: Partial match agency '{opt_clean}'")
                            break
                if not selected and len(options) > 1:
                    await agency_select.select_option(index=1)
                    logger.debug("Validación PR: Selected first available agency")
                await page.wait_for_timeout(1000)

            # Step 3: Fill visible text inputs — nth(0) = cert, nth(1) = SSN last 4
            visible_inputs = page.locator("input[type='text']:visible")
            input_count = await visible_inputs.count()
            logger.debug(f"Validación PR: Found {input_count} visible text inputs")

            if input_count >= 1:
                await visible_inputs.nth(0).fill(cert_number)
                await page.wait_for_timeout(500)
                logger.debug(f"Validación PR: Filled cert '{cert_number}'")

            if input_count >= 2:
                await visible_inputs.nth(1).fill(last_four)
                await page.wait_for_timeout(500)
                logger.debug(f"Validación PR: Filled SSN last4 '{last_four}'")

            # Step 4: Click visible "Validar" button
            validar_btn = page.locator("a:visible, button:visible").filter(has_text="Validar").first
            if await validar_btn.count() > 0:
                await validar_btn.click()
                logger.debug("Validación PR: Clicked Validar")
            else:
                logger.warning("Validación PR: Could not find visible Validar button")

            # Step 5: Wait for Verificación section to populate
            await page.wait_for_timeout(5000)

            # Step 6: Read result from the Verificación section
            # The page has labeled fields: Certificado, Nombre, Fecha Emisión, Fecha Expiración, Resultado
            result = await self._parse_validacion_pr_result_v2(page)

            screenshot_path = None
            if self.screenshot_enabled:
                screenshot_path = await self._save_screenshot(page, "validacion_pr", extracted)

            return {
                "status": result["status"],
                "portal_name": config["name"],
                "portal_url": config["url"],
                "agency": agency,
                "certificate_number": cert_number,
                "last_four_ssn": last_four,
                "result_text": result.get("text", ""),
                "result_details": result.get("details", {}),
                "method": "browser",
                "screenshot_path": screenshot_path,
            }

        except Exception as e:
            logger.error(f"Validación PR browser validation failed: {e}")
            return {
                "status": "error",
                "portal_name": config["name"],
                "portal_url": config["url"],
                "agency": agency,
                "certificate_number": cert_number,
                "last_four_ssn": last_four,
                "error": str(e),
                "method": "browser",
            }
        finally:
            await page.close()

    # ─── POLICÍA (ANTECEDENTES PENALES) ────────────────────────────

    async def _validate_policia(self, context: BrowserContext, extracted: dict, api_data: dict, config: dict) -> dict:
        """
        Validate Certificado de Antecedentes Penales via the Policía portal.

        Flow (from customer screenshot):
        1. Navigate to https://www.pr.gov/antecedentes-penales
        2. Click "Validar Certificado" tab
        3. Enter "Número de Certificado" (application_number mapped to certificate_number)
        4. Read result — fail message or user info displayed
        """
        cert_number = (
            extracted.get("application_number", "").strip()
            or extracted.get("certificate_number", "").strip()
        )
        policia_url = "https://www.pr.gov/antecedentes-penales"

        logger.info(f"Policía: Validating cert/application number '{cert_number}'")

        page = await context.new_page()
        try:
            # Step 1: Navigate
            await page.goto(policia_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            logger.debug("Policía: Page loaded")

            # Step 2: Click "Validar Certificado" tab
            validar_tab = page.locator("a:visible, button:visible, li:visible").filter(has_text="Validar Certificado")
            if await validar_tab.count() > 0:
                await validar_tab.first.click()
                await page.wait_for_timeout(2000)
                logger.debug("Policía: Clicked 'Validar Certificado' tab")
            else:
                # Try clicking by exact text
                await page.get_by_text("Validar Certificado", exact=False).first.click()
                await page.wait_for_timeout(2000)
                logger.debug("Policía: Clicked 'Validar Certificado' via text match")

            # Step 3: Enter Número de Certificado
            cert_input = page.locator("input[type='text']:visible, input[type='number']:visible").first
            if await cert_input.count() > 0:
                await cert_input.fill(cert_number)
                await page.wait_for_timeout(500)
                logger.debug(f"Policía: Filled cert number '{cert_number}'")
            else:
                return {
                    "status": "error",
                    "portal_name": "Policía - Certificado de Antecedentes Penales",
                    "portal_url": policia_url,
                    "error": "Could not find certificate number input field",
                    "method": "browser",
                }

            # Step 4: Click submit / search button (look for "Intentar Nuevamente" or submit button)
            submit_btn = page.locator("button:visible, input[type='submit']:visible, a.btn:visible").filter(
                has_text=re.compile(r"buscar|validar|verificar|consultar|submit|enviar", re.IGNORECASE)
            ).first
            if await submit_btn.count() > 0:
                await submit_btn.click()
            else:
                # Try pressing Enter
                await cert_input.press("Enter")
            await page.wait_for_timeout(5000)
            logger.debug("Policía: Submitted form")

            # Step 5: Read result
            body_text = await page.locator("body").text_content() or ""
            body_lower = body_text.lower()

            screenshot_path = None
            if self.screenshot_enabled:
                screenshot_path = await self._save_screenshot(page, "policia", extracted)

            # Check for failure patterns from the customer screenshot:
            # "no pudo ser identificado", "nunca existió o ya expiró",
            # "no es valido y no debe aceptarse"
            policia_fail_patterns = [
                "no pudo ser identificado",
                "no es valido y no debe aceptarse",
                "no es válido y no debe aceptarse",
                "nunca existió o ya expiró",
                "nunca existio o ya expiro",
            ]

            is_fail = any(p in body_lower for p in policia_fail_patterns)

            # Check for success: user information displayed (name, no criminal record)
            success_patterns = [
                "no tiene antecedentes penales",
                "no registra antecedentes",
                "certificado negativo",
                "negative certificate",
            ]
            is_valid = any(p in body_lower for p in success_patterns)

            if is_fail:
                status = "invalid"
                # Extract the specific error message
                result_text = ""
                for p in policia_fail_patterns:
                    idx = body_lower.find(p)
                    if idx >= 0:
                        # Get surrounding context
                        start = max(0, idx - 50)
                        end = min(len(body_text), idx + 200)
                        result_text = body_text[start:end].strip()
                        result_text = re.sub(r'\s+', ' ', result_text)
                        break
            elif is_valid:
                status = "valid"
                result_text = "Certificado válido — no tiene antecedentes penales"
            else:
                status = "inconclusive"
                result_text = re.sub(r'\s+', ' ', body_text[:500]).strip()

            return {
                "status": status,
                "portal_name": "Policía - Certificado de Antecedentes Penales",
                "portal_url": policia_url,
                "application_number": cert_number,
                "result_text": result_text,
                "method": "browser",
                "screenshot_path": screenshot_path,
            }

        except Exception as e:
            logger.error(f"Policía browser validation failed: {e}")
            return {
                "status": "error",
                "portal_name": "Policía - Certificado de Antecedentes Penales",
                "portal_url": policia_url,
                "application_number": cert_number,
                "error": str(e),
                "method": "browser",
            }
        finally:
            await page.close()

    # ─── SHARED HELPERS ─────────────────────────────────────────────

    async def _fill_field(self, page: Page, locator, value: str, fallback_selectors: list):
        """Fill a field using the primary locator or fallback CSS selectors."""
        if locator and await locator.count() > 0:
            await locator.fill(value)
            return

        for selector in fallback_selectors:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.fill(value)
                    return
            except Exception:
                continue

        logger.warning(f"Could not find input field for value '{value[:20]}...' — tried {len(fallback_selectors)} selectors")

    def _parse_hacienda_result(self, result_text: str) -> dict:
        """Parse SURI result text from #caption2_Dd-69 element."""
        if not result_text or result_text == "Element not found":
            return {"status": "inconclusive", "text": result_text or ""}

        text_lower = result_text.lower()

        # Check for valid indicators
        valid_patterns = [
            "válida", "válido", "vigente", "activo", "active",
            "certificación válida", "certificate is valid",
            "radicó", "cumplimiento",
        ]
        for pattern in valid_patterns:
            if pattern in text_lower:
                return {"status": "valid", "text": result_text}

        # Check for invalid/error indicators
        invalid_patterns = [
            "no se encontró", "no encontrado", "no válido", "no valido",
            "inválido", "invalido", "vencido", "expirado", "expired",
            "not found", "not valid", "no existe", "error",
            "no radicó", "incumplimiento",
        ]
        for pattern in invalid_patterns:
            if pattern in text_lower:
                return {"status": "invalid", "text": result_text}

        # If we got text but can't determine status
        return {"status": "inconclusive", "text": result_text}

    async def _parse_page_result(self, page: Page) -> dict:
        """Parse the current page for validation result keywords."""
        body_text = await page.locator("body").text_content()
        if not body_text:
            return {"status": "inconclusive", "text": ""}

        text_lower = body_text.lower()

        # Check for invalid indicators first (more specific)
        invalid_patterns = [
            "no se encontró", "no encontrado", "no válido", "no valido",
            "inválido", "invalido", "vencido", "expirado", "expired",
            "not found", "not valid", "certificado no",
        ]
        for pattern in invalid_patterns:
            if pattern in text_lower:
                return {"status": "invalid", "text": body_text[:500]}

        # Check for valid indicators
        valid_patterns = [
            "certificación válida", "certificado válido", "certificado vigente",
            "válido", "vigente", "activo", "valid", "certificate is valid",
        ]
        for pattern in valid_patterns:
            if pattern in text_lower:
                return {"status": "valid", "text": body_text[:500]}

        return {"status": "inconclusive", "text": body_text[:500]}

    async def _parse_validacion_pr_result(self, page: Page) -> dict:
        """Parse validacion.pr.gov structured Verificación section.

        The page has a clear layout:
          Certificado:     <value>
          Nombre:          <value>
          Fecha Emisión:   <value>
          Fecha Expiración:<value>
          Resultado:       <value>
        """
        details = {}
        body_text = await page.locator("body").text_content() or ""

        # Extract structured fields using regex on the page text
        field_patterns = {
            "certificate": r"Certificado:\s*([^\n]+)",
            "name": r"Nombre:\s*([^\n]+)",
            "issue_date": r"Fecha\s*Emisi[oó]n:\s*([^\n]+)",
            "expiration_date": r"Fecha\s*Expiraci[oó]n:\s*([^\n]+)",
            "result": r"Resultado:\s*([^\n]+)",
        }

        for key, pattern in field_patterns.items():
            m = re.search(pattern, body_text, re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                if val:
                    details[key] = val

        logger.info(f"Validación PR parsed details: {details}")

        # First check the full page for Policía-specific failure message
        body_text = await page.locator("body").text_content() or ""
        body_lower = body_text.lower()

        # Policía Criminal Record specific error:
        # "no pudo ser identificado" / "no es valido y no debe aceptarse" / "nunca existió o ya expiró"
        policia_fail_patterns = [
            "no pudo ser identificado",
            "no es valido y no debe aceptarse",
            "no es válido y no debe aceptarse",
            "nunca existió o ya expiró",
        ]
        for pattern in policia_fail_patterns:
            if pattern in body_lower:
                return {
                    "status": "invalid",
                    "text": body_text[:500],
                    "details": details,
                }

        # Determine status from Resultado field
        resultado = details.get("result", "").lower()
        if resultado:
            if any(w in resultado for w in ["válido", "vigente", "activo", "valido"]):
                status = "valid"
            elif any(w in resultado for w in ["no válido", "vencido", "expirado", "inválido", "invalido", "no valido"]):
                status = "invalid"
            else:
                status = "inconclusive"
        else:
            # Check if Nombre field was populated (= success on Validación PR)
            if details.get("name") and len(details["name"]) > 2:
                status = "valid"
            # No Resultado field found — check general page
            else:
                fallback = await self._parse_page_result(page)
                status = fallback["status"]

        # Build a clean result_text from details
        result_text = "; ".join(f"{k}: {v}" for k, v in details.items()) if details else body_text[:500]

        return {
            "status": status,
            "text": result_text,
            "details": details,
        }

    async def _parse_crim_result(self, page: Page, page_text: str) -> dict:
        """Parse CRIM portal result after clicking Buscar.

        After a successful lookup CRIM shows a result table/card.
        After a failed lookup it shows an error message.
        We look for specific result keywords in the page text that appeared
        AFTER the initial page load.
        """
        text_lower = page_text.lower()

        # CRIM-specific error/not-found messages
        not_found = [
            "no se encontr", "no encontrado", "no existe",
            "certificación no encontrada", "no results",
            "certificado no encontrado", "no hay resultados",
        ]
        for pattern in not_found:
            if pattern in text_lower:
                return {"status": "not_found", "text": page_text[:500]}

        # CRIM-specific valid indicators
        valid = [
            "certificación válida", "certificado válido", "válida",
            "vigente", "activo", "activa", "emitido",
        ]
        for pattern in valid:
            if pattern in text_lower:
                return {"status": "valid", "text": page_text[:500]}

        # Expired / invalid
        invalid = [
            "vencido", "vencida", "expirado", "expirada",
            "inválido", "inválida", "invalido", "invalida",
            "no válido", "no válida", "no valido",
        ]
        for pattern in invalid:
            if pattern in text_lower:
                return {"status": "invalid", "text": page_text[:500]}

        # If we see the certificate number echoed back, the lookup succeeded
        # but we can't determine validity
        return {"status": "inconclusive", "text": page_text[:500]}

    async def _parse_validacion_pr_result_v2(self, page: Page) -> dict:
        """Parse validacion.pr.gov Verificación section by reading the
        structured fields that appear after clicking Validar.

        The page renders results as label/value pairs in separate DOM elements.
        We must traverse the DOM to find the VALUE element next to each LABEL,
        not just regex the full page text (which mixes labels together).
        """
        details = {}

        # Strategy 1: DOM traversal — find label elements and read their sibling/value elements
        js_extract = """
        () => {
            const result = {};
            const labels = ['Certificado', 'Nombre', 'Fecha Emisión', 'Fecha Expiración', 'Resultado'];

            // Helper: get text of an element, collapsing whitespace
            function getCleanText(el) {
                if (!el) return '';
                return (el.textContent || el.innerText || '').replace(/\s+/g, ' ').trim();
            }

            // Strategy A: Look for elements whose text content exactly matches a label
            // and then read the next sibling, parent's next sibling, or next element
            const allElements = document.querySelectorAll('label, span, strong, b, th, td, div, p, dt, dd');
            for (const el of allElements) {
                const elText = getCleanText(el);
                for (const label of labels) {
                    // Match label text (with or without colon)
                    const labelClean = label.replace(':', '').trim();
                    const elClean = elText.replace(':', '').trim();
                    if (elClean === labelClean || elClean === labelClean + ':') {
                        // Try multiple strategies to find the value
                        let value = '';

                        // 1. Next element sibling
                        let next = el.nextElementSibling;
                        if (next) {
                            value = getCleanText(next);
                        }

                        // 2. If no value, try parent's next sibling
                        if (!value && el.parentElement) {
                            let parentNext = el.parentElement.nextElementSibling;
                            if (parentNext) {
                                value = getCleanText(parentNext);
                            }
                        }

                        // 3. If element is a <dt>, look for the next <dd>
                        if (!value && el.tagName === 'DT') {
                            let dd = el.nextElementSibling;
                            while (dd && dd.tagName !== 'DD') dd = dd.nextElementSibling;
                            if (dd) value = getCleanText(dd);
                        }

                        // 4. If element is <td>, try next <td> in same row
                        if (!value && el.tagName === 'TD') {
                            let nextTd = el.nextElementSibling;
                            if (nextTd && nextTd.tagName === 'TD') {
                                value = getCleanText(nextTd);
                            }
                        }

                        // Clean value: remove embedded label text and page chrome
                        if (value) {
                            // Strip any label names that leaked into the value
                            for (const l of labels) {
                                value = value.replace(l + ':', '').replace(l, '');
                            }
                            value = value.replace('GOBIERNO DE PUERTO RICO', '').replace(/\s+/g, ' ').trim();
                        }
                        if (value && value.length < 200 && value.length > 0) {
                            result[label] = value;
                        }
                        break;
                    }
                }
            }

            // Strategy B: If Strategy A found nothing, try input/select elements
            // that might contain the result values
            if (Object.keys(result).length === 0) {
                const inputs = document.querySelectorAll('input[readonly], input[disabled], .result-value, .field-value');
                const inputValues = Array.from(inputs).map(i => i.value || getCleanText(i)).filter(v => v);
                if (inputValues.length >= 2) {
                    const fieldNames = ['certificate', 'name', 'issue_date', 'expiration_date', 'result'];
                    for (let i = 0; i < Math.min(inputValues.length, fieldNames.length); i++) {
                        result[labels[i]] = inputValues[i];
                    }
                }
            }

            return result;
        }
        """
        try:
            js_details = await page.evaluate(js_extract)
            if js_details:
                details = {
                    "certificate": js_details.get("Certificado", ""),
                    "name": js_details.get("Nombre", ""),
                    "issue_date": js_details.get("Fecha Emisión", ""),
                    "expiration_date": js_details.get("Fecha Expiración", ""),
                    "result": js_details.get("Resultado", ""),
                }
                # Clean whitespace from all values
                import re as _re
                details = {k: _re.sub(r'\s+', ' ', v).strip() for k, v in details.items() if v and _re.sub(r'\s+', ' ', v).strip()}
        except Exception as e:
            logger.warning(f"Validación PR JS extraction failed: {e}")

        logger.info(f"Validación PR v2 parsed details: {details}")

        # Also check the full page text for Policía-specific failure messages
        body_text = await page.locator("body").text_content() or ""
        body_lower = body_text.lower()

        policia_fail_patterns = [
            "no pudo ser identificado",
            "no es valido y no debe aceptarse",
            "no es válido y no debe aceptarse",
            "nunca existió o ya expiró",
        ]
        for pattern in policia_fail_patterns:
            if pattern in body_lower:
                return {
                    "status": "invalid",
                    "text": body_text[:500],
                    "details": details,
                }

        # Determine status from Resultado field AND all detail values + body text
        all_values = " ".join(str(v) for v in details.values()).lower()
        resultado = details.get("result", "").lower()

        # Check for failure/expiry patterns across ALL captured values and body text
        fail_patterns = ["no válido", "no valido", "vencido", "expirado",
                         "inválido", "invalido", "expired", "certificado expirado"]
        valid_patterns_kw = ["válido", "vigente", "activo", "valido", "valid"]

        if any(p in all_values for p in fail_patterns) or any(p in body_lower for p in fail_patterns):
            status = "invalid"
        elif any(p in all_values for p in valid_patterns_kw):
            status = "valid"
        elif resultado:
            status = "inconclusive"
        elif details.get("name") and len(details["name"]) > 2:
            status = "valid"
        else:
            status = "inconclusive"

        result_text = "; ".join(f"{k}: {v}" for k, v in details.items()) if details else "No verification details found"

        return {
            "status": status,
            "text": result_text,
            "details": details,
        }

    async def _save_screenshot(self, page: Page, portal_id: str, extracted: dict) -> Optional[str]:
        """Save a screenshot of the current page state."""
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            cert = extracted.get("certificate_number", "unknown")[:20]
            filename = f"{portal_id}_{cert}_{timestamp}.png"
            filepath = os.path.join(self.screenshot_output_dir, filename)
            await page.screenshot(path=filepath, full_page=True)
            logger.debug(f"Screenshot saved: {filepath}")
            return filepath
        except Exception as e:
            logger.warning(f"Failed to save screenshot: {e}")
            return None

    # ─── REJECT REASONS ─────────────────────────────────────────────

    @staticmethod
    def _to_final_status(internal_status: str) -> str:
        """Map internal statuses to customer-facing output.

        Customer requires exactly three values:
          Validated  – portal confirmed the certificate is valid
          Fail       – portal confirmed the certificate is NOT valid
          Could not be validated – portal unreachable / error / inconclusive
        """
        if internal_status == "valid":
            return "Validated"
        elif internal_status in ("invalid", "not_found"):
            return "Fail"
        else:  # error, inconclusive, skipped, etc.
            return "Could not be validated"

    def _generate_reject_reasons(self, portal_results: dict) -> list:
        """Generate reject reasons from browser validation results."""
        reasons = []
        reason_id = 1

        for portal_id, result in portal_results.items():
            status = result.get("status", "")
            final = self._to_final_status(status)
            portal_name = result.get("portal_name", portal_id)
            cert_number = result.get("certificate_number", result.get("letter_id", ""))

            if final == "Fail":
                reasons.append({
                    "id": reason_id,
                    "reason": f"Certificación no válida según {portal_name}. Número: {cert_number}. Anejar certificación vigente"
                })
                reason_id += 1
            elif final == "Could not be validated":
                error = result.get("error", result.get("reason", ""))
                reasons.append({
                    "id": reason_id,
                    "reason": f"No se pudo validar certificación en {portal_name}. {error}. Verificar manualmente"
                })
                reason_id += 1

        return reasons

    # ─── STORAGE ────────────────────────────────────────────────────

    def _save_local(self, content, result: dict):
        """Save validation report locally."""
        folder = self.local_output_folder
        if not folder:
            return
        try:
            os.makedirs(folder, exist_ok=True)
            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            filename = f"{canonical_id}_browser_validation.json"
            filepath = os.path.join(folder, filename)
            payload = {
                "canonical_id": canonical_id,
                "source_filename": getattr(content.id, 'filename', None),
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "browser_validation": result,
            }
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved browser validation report to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save local report: {e}")

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

    async def _upload_report(self, content, result: dict):
        try:
            connector = await self._get_connector()
            if connector is None:
                return

            canonical_id = getattr(content.id, 'canonical_id', 'unknown')
            now = datetime.now(timezone.utc)
            blob_path = f"browser_validation_reports/{now.strftime('%Y/%m/%d')}/{canonical_id}.json"

            payload = {
                "canonical_id": canonical_id,
                "source_filename": getattr(content.id, 'filename', None),
                "validated_at": now.isoformat(),
                "browser_validation": result,
            }
            data = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")

            await connector.upload_blob(
                container_name=self.container_name,
                blob_path=blob_path,
                data=data,
                overwrite=True,
            )
            logger.info(f"Uploaded browser validation report to {self.container_name}/{blob_path}")
            content.summary_data["browser_validation_report_blob"] = {
                "container": self.container_name,
                "blob_path": blob_path,
                "write_status": "success",
            }
        except Exception as e:
            logger.error(f"Failed to upload report: {e}")
            content.summary_data["browser_validation_report_blob"] = {
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
