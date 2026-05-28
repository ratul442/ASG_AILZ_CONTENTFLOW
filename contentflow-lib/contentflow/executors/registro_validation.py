
import logging
import json
import tempfile
from typing import Any, Dict, List, Optional
import requests
import os
from . import ParallelExecutor
from ..connectors.document_intelligence_connector import DocumentIntelligenceConnector

logger = logging.getLogger("contentflow.executors.registro_validation")

def build_sas_url(document_path: str, sas_base_url: str, sas_token: str) -> str:
    document_path = document_path.lstrip('/')
    return f"{sas_base_url}{document_path}?{sas_token}"

class ASGDocumentValidationAIExecutor(ParallelExecutor):
    def __init__(self, id: str, settings: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)
        self.api_base_url = self.get_setting("api_base_url")
        self.user = self.get_setting("user")
        self.password = self.get_setting("password")
        self.api_key = self.get_setting("api_key")
        self.sas_base_url = self.get_setting("sas_base_url", default="https://rulrupstagingstorage.file.core.windows.net/")
        self.sas_token = self.get_setting("sas_token")
        self.doc_intelligence_endpoint = self.get_setting("doc_intelligence_endpoint")
        self.doc_intelligence_credential_type = self.get_setting("doc_intelligence_credential_type", default="azure_key_credential")
        self.doc_intelligence_credential_key = self.get_setting("doc_intelligence_credential_key")
        self.model_id = self.get_setting("model_id", default="prebuilt-document")
        self.token = None
        self.doc_intelligence_connector = DocumentIntelligenceConnector(
            name="doc_intelligence_connector",
            settings={
                "endpoint": self.doc_intelligence_endpoint,
                "credential_type": self.doc_intelligence_credential_type,
                "credential_key": self.doc_intelligence_credential_key
            }
        )

    async def authenticate(self) -> None:
        logger.info("Authenticating with ASG API...")
        url = f"{self.api_base_url}/Authentication/Credentials"
        payload = {
            "user": self.user,
            "password": self.password,
            "apiKey": self.api_key,
        }
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        self.token = resp.json().get("token")
        logger.info("Authentication successful.")

    async def get_case_details(self, tramite_guid: str) -> Dict[str, Any]:
        logger.info(f"Retrieving case details for tramite_guid: {tramite_guid}")
        url = f"{self.api_base_url}/document-intelligence/details?tramiteGuid={tramite_guid}"
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def download_document(self, document_url: str) -> str:
        if not document_url.startswith("http"):
            document_url = build_sas_url(document_url, self.sas_base_url, self.sas_token)
        logger.info(f"Downloading document from {document_url}")
        response = requests.get(document_url)
        response.raise_for_status()
        fd, temp_path = tempfile.mkstemp(suffix=os.path.splitext(document_url)[-1])
        with os.fdopen(fd, 'wb') as tmp:
            tmp.write(response.content)
        logger.info(f"Document downloaded to {temp_path}")
        return temp_path

    async def extract_fields_with_ai(self, pdf_path: str) -> Dict[str, Optional[str]]:
        await self.doc_intelligence_connector.initialize()
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        result = await self.doc_intelligence_connector.analyze_document(
            document_bytes=pdf_bytes,
            model_id=self.model_id
        )
        fields = {}
        # Parse key-value pairs from result
        kv_pairs = result.get("key_value_pairs", [])
        for kv in kv_pairs:
            key_text = kv.get("key", "").strip().lower()
            value_text = kv.get("value", None)
            if "name" in key_text and not fields.get("name"):
                fields["name"] = value_text
            elif "merchant registration number" in key_text and not fields.get("merchant_registration_number"):
                fields["merchant_registration_number"] = value_text
            elif "agent type" in key_text and not fields.get("agent_type"):
                fields["agent_type"] = value_text
            elif "naics code" in key_text and not fields.get("naics_code"):
                fields["naics_code"] = value_text
            elif "registration certificate number" in key_text and not fields.get("registration_certificate_number"):
                fields["registration_certificate_number"] = value_text
            elif ("ssn" in key_text or "ein" in key_text) and not fields.get("ssn_ein"):
                fields["ssn_ein"] = value_text
        logger.info(f"Extracted fields: {fields}")
        return fields

    async def validate_document(self, requirement: Dict[str, Any], api_data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        document_url = requirement.get("documentPath")
        if not document_url:
            logger.info(f"No document to process for requirement {requirement.get('requirementId')}")
            return None
        local_path = await self.download_document(document_url)
        fields = await self.extract_fields_with_ai(local_path)
        os.remove(local_path)
        reject_reasons = []
        if fields.get("name") and fields["name"] != api_data.get("companyName"):
            reject_reasons.append({"id": 144, "reason": "El Nombre no coincide con el registro"})
        if fields.get("merchant_registration_number") and fields["merchant_registration_number"] != api_data.get("merchantRegistrationNumber"):
            reject_reasons.append({"id": 144, "reason": "El Número de Registro de Comerciante no coincide con el registro"})
        if fields.get("agent_type") and fields["agent_type"] != api_data.get("agentType"):
            reject_reasons.append({"id": 144, "reason": "El Tipo de Agente no coincide con el registro"})
        if fields.get("naics_code") and fields["naics_code"] not in [str(n.get("code")) for n in api_data.get("naicsCodes", [])]:
            reject_reasons.append({"id": 144, "reason": "El Código NAICS no coincide con el registro"})
        api_reg_cert = api_data.get("registrationCertificateNumber")
        if fields.get("registration_certificate_number") and api_reg_cert and fields["registration_certificate_number"] != api_reg_cert:
            reject_reasons.append({"id": 144, "reason": "El Número de Certificado de Registro no coincide con el registro"})
        api_ssn_last4 = api_data.get("companySsnLast4")
        if fields.get("ssn_ein") and api_ssn_last4 and not fields["ssn_ein"].endswith(api_ssn_last4):
            reject_reasons.append({"id": 144, "reason": "El SSN/EIN no coincide con el registro"})
        return reject_reasons if reject_reasons else None

    async def submit_analysis_result(
        self,
        tramite_guid: str,
        status_id: int,
        notes: str,
        rejected_requirements: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        url = f"{self.api_base_url}/document-intelligence/apply-analysis"
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {
            "TramiteGuid": tramite_guid,
            "StatusId": status_id,
            "Notes": notes,
        }
        if rejected_requirements:
            payload["RejectedRequirements"] = rejected_requirements
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        logger.info(f"Submitted analysis result for {tramite_guid} with status {status_id}")

    async def process_content_item(self, content: Dict[str, Any]) -> Dict[str, Any]:
        # Entry point for ContentFlow async pipeline
        tramite_guid = content.get("tramite_guid")
        await self.authenticate()
        case_details = await self.get_case_details(tramite_guid)
        rejected_requirements = []
        for req in case_details.get("tramiteRequirements", []):
            reasons = await self.validate_document(req, case_details)
            if reasons:
                rejected_requirements.append({
                    "TramiteRequirementId": req["tramiteRequirementId"],
                    "RejectionReasonId": reasons[0]["id"],
                    "RejectionReason": reasons[0]["reason"]
                })
        status_id = 2 if not rejected_requirements else 1002
        notes = "All documents validated successfully." if not rejected_requirements else "One or more documents failed validation."
        await self.submit_analysis_result(tramite_guid, status_id, notes, rejected_requirements if rejected_requirements else None)
        logger.info(f"Validation completed for {tramite_guid}: {notes}")
        return {
            "tramite_guid": tramite_guid,
            "status_id": status_id,
            "notes": notes,
            "rejected_requirements": rejected_requirements
        }

    def authenticate(self) -> None:
        logger.info("Authenticating with ASG API...")
        url = f"{self.api_base_url}/Authentication/Credentials"
        payload = {
            "user": self.user,
            "password": self.password,
            "apiKey": self.api_key,
        }
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        self.token = resp.json().get("token")
        logger.info("Authentication successful.")

    def get_case_details(self, tramite_guid: str) -> Dict[str, Any]:
        logger.info(f"Retrieving case details for tramite_guid: {tramite_guid}")
        url = f"{self.api_base_url}/document-intelligence/details?tramiteGuid={tramite_guid}"
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def download_document(self, document_url: str) -> str:
        """
        Download a document from a given URL (absolute or relative SAS path).
        Returns the local file path.
        """
        # If the document_url is a relative path, build the full SAS URL
        if not document_url.startswith("http"):
            document_url = build_sas_url(document_url, self.sas_base_url, self.sas_token)
        logger.info(f"Downloading document from {document_url}")
        response = requests.get(document_url)
        response.raise_for_status()
        import os
        fd, temp_path = tempfile.mkstemp(suffix=os.path.splitext(document_url)[-1])
        with os.fdopen(fd, 'wb') as tmp:
            tmp.write(response.content)
        logger.info(f"Document downloaded to {temp_path}")
        return temp_path

    def extract_fields_with_ai(self, pdf_path: str) -> Dict[str, Optional[str]]:
        logger.info(f"Extracting fields from {pdf_path} using Azure Form Recognizer")
        with open(pdf_path, "rb") as f:
            poller = self.fr_client.begin_analyze_document("prebuilt-document", document=f)
        result = poller.result()
        fields = {}
        for kv in result.key_value_pairs:
            key_text = kv.key.content.strip().lower() if kv.key else ""
            value_text = kv.value.content.strip() if kv.value else None
            if "name" in key_text and not fields.get("name"):
                fields["name"] = value_text
            elif "merchant registration number" in key_text and not fields.get("merchant_registration_number"):
                fields["merchant_registration_number"] = value_text
            elif "agent type" in key_text and not fields.get("agent_type"):
                fields["agent_type"] = value_text
            elif "naics code" in key_text and not fields.get("naics_code"):
                fields["naics_code"] = value_text
            elif "registration certificate number" in key_text and not fields.get("registration_certificate_number"):
                fields["registration_certificate_number"] = value_text
            elif ("ssn" in key_text or "ein" in key_text) and not fields.get("ssn_ein"):
                fields["ssn_ein"] = value_text
        logger.info(f"Extracted fields: {fields}")
        return fields

    def validate_document(self, requirement: Dict[str, Any], api_data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        document_url = requirement.get("documentPath")
        if not document_url:
            logger.info(f"No document to process for requirement {requirement.get('requirementId')}")
            return None  # No document to validate

        # Build SAS URL if needed
        if not document_url.startswith("http"):
            document_url = build_sas_url(document_url)

        local_path = self.download_document(document_url)
        fields = self.extract_fields_with_ai(local_path)
        os.remove(local_path)
        reject_reasons = []
        # Name
        if fields.get("name") and fields["name"] != api_data.get("companyName"):
            reject_reasons.append({"id": 144, "reason": "El Nombre no coincide con el registro"})
        # Merchant Registration Number
        if fields.get("merchant_registration_number") and fields["merchant_registration_number"] != api_data.get("merchantRegistrationNumber"):
            reject_reasons.append({"id": 144, "reason": "El Número de Registro de Comerciante no coincide con el registro"})
        # Agent Type
        if fields.get("agent_type") and fields["agent_type"] != api_data.get("agentType"):
            reject_reasons.append({"id": 144, "reason": "El Tipo de Agente no coincide con el registro"})
        # NAICS Code (compare with any code in the list)
        if fields.get("naics_code") and fields["naics_code"] not in [str(n.get("code")) for n in api_data.get("naicsCodes", [])]:
            reject_reasons.append({"id": 144, "reason": "El Código NAICS no coincide con el registro"})
        # Registration Certificate Number (if available in API)
        api_reg_cert = api_data.get("registrationCertificateNumber")
        if fields.get("registration_certificate_number") and api_reg_cert and fields["registration_certificate_number"] != api_reg_cert:
            reject_reasons.append({"id": 144, "reason": "El Número de Certificado de Registro no coincide con el registro"})
        # SSN / EIN (compare with last 4 digits if available)
        api_ssn_last4 = api_data.get("companySsnLast4")
        if fields.get("ssn_ein") and api_ssn_last4 and not fields["ssn_ein"].endswith(api_ssn_last4):
            reject_reasons.append({"id": 144, "reason": "El SSN/EIN no coincide con el registro"})
        return reject_reasons if reject_reasons else None

    def submit_analysis_result(
        self,
        tramite_guid: str,
        status_id: int,
        notes: str,
        rejected_requirements: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        url = f"{self.api_base_url}/document-intelligence/apply-analysis"
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {
            "TramiteGuid": tramite_guid,
            "StatusId": status_id,
            "Notes": notes,
        }
        if rejected_requirements:
            payload["RejectedRequirements"] = rejected_requirements
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        logger.info(f"Submitted analysis result for {tramite_guid} with status {status_id}")

    def run(self, tramite_guid: str) -> Dict[str, Any]:
        self.authenticate()
        case_details = self.get_case_details(tramite_guid)
        rejected_requirements = []
        for req in case_details.get("tramiteRequirements", []):
            reasons = self.validate_document(req, case_details)
            if reasons:
                rejected_requirements.append({
                    "TramiteRequirementId": req["tramiteRequirementId"],
                    "RejectionReasonId": reasons[0]["id"],
                    "RejectionReason": reasons[0]["reason"]
                })
        status_id = 2 if not rejected_requirements else 1002
        notes = "All documents validated successfully." if not rejected_requirements else "One or more documents failed validation."
        self.submit_analysis_result(tramite_guid, status_id, notes, rejected_requirements if rejected_requirements else None)
        logger.info(f"Validation completed for {tramite_guid}: {notes}")
        return {
            "tramite_guid": tramite_guid,
            "status_id": status_id,
            "notes": notes,
            "rejected_requirements": rejected_requirements
        }

    def batch_run(self, tramite_guids: List[str], output_path: str = "validation_results.json"):
        logger.info(f"Starting batch validation for {len(tramite_guids)} tramite_guids")
        all_results = []
        for guid in tramite_guids:
            try:
                result = self.run(guid)
                all_results.append(result)
            except Exception as e:
                logger.error(f"Error processing {guid}: {e}")
                all_results.append({
                    "tramite_guid": guid,
                    "error": str(e)
                })
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        logger.info(f"Batch validation results written to {output_path}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Batch validate tramite_guids using Azure Document Intelligence.")
    parser.add_argument("tramite_guids", nargs="+", help="List of tramite_guids to process")
    parser.add_argument("--output", default="validation_results.json", help="Output JSON file path")
    args = parser.parse_args()

    executor = ASGDocumentValidationAIExecutor(
        api_base_url=os.getenv("API_BASE_URL"),
        user=os.getenv("API_USER"),
        password=os.getenv("API_PASSWORD"),
        api_key=os.getenv("API_KEY"),
        form_recognizer_endpoint=os.getenv("FORM_RECOGNIZER_ENDPOINT"),
        form_recognizer_key=os.getenv("FORM_RECOGNIZER_KEY"),
    )
    executor.batch_run(args.tramite_guids, args.output)