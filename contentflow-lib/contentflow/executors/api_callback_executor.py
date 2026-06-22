"""
API Callback Executor — Posts validation results back to the Registros API.


1. Aggregates validation results from all documents
2. Determines overall case status (pass/fail)
3. Maps rejected documents to RejectedRequirements with RejectionReasonIds
4. POSTs to: POST {{ApiBaseUrl}}/document-intelligence/apply-analysis
"""

import json
import logging
import requests
from typing import Any, Dict, List, Optional, Union

from agent_framework import WorkflowContext
from .base import BaseExecutor
from ..models import Content

logger = logging.getLogger("contentflow.executors.api_callback_executor")


class APICallbackExecutor(BaseExecutor):
    """
    Posts aggregated validation results back to the Registros API.

    Configuration (settings dict):
        - api_base_url (str): Base API URL (e.g., https://rulrup-staging-api.azurewebsites.net/api/)
        - username (str): API username for authentication
        - password (str): API password
        - api_key (str): API key (JWT)
        - success_status_id (int): StatusId for successful validation (default: 2)
        - failure_status_id (int): StatusId for failed validation (default: 1002)
        - dry_run (bool): If true, log the payload but don't POST (default: false)
    """

    # StatusId values — placeholders until confirmed by Registros team
    DEFAULT_SUCCESS_STATUS_ID = 2       # Submission Successful
    DEFAULT_FAILURE_STATUS_ID = 1002    # Returned to Customer

    def __init__(self, id: str, settings: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)

        self.api_base_url = self.get_setting("api_base_url", required=True)
        self.username = self.get_setting("username", required=True)
        self.password = self.get_setting("password", required=True)
        self.api_key = self.get_setting("api_key", required=True)
        self.success_status_id = self.get_setting("success_status_id", default=self.DEFAULT_SUCCESS_STATUS_ID)
        self.failure_status_id = self.get_setting("failure_status_id", default=self.DEFAULT_FAILURE_STATUS_ID)
        self.dry_run = self.get_setting("dry_run", default=False)
        # Blob storage settings for saving callback payload
        self.storage_account_name = self.get_setting("storage_account_name", default=None)
        self.credential_type = self.get_setting("credential_type", default="default_azure_credential")
        self.credential_key = self.get_setting("credential_key", default=None)
        self.container_name = self.get_setting("container_name", default="callback-results")
        self._connector = None

    def _authenticate(self) -> str:
        """Authenticate against the Registros API and return a Bearer token."""
        auth_url = f"{self.api_base_url}Authentication/Credentials"
        payload = {
            "Username": self.username,
            "Password": self.password,
            "ApiKey": self.api_key,
        }
        try:
            resp = requests.post(auth_url, json=payload, verify=False, timeout=30)
            resp.raise_for_status()
            resp_json = resp.json()
            token = (
                resp_json.get("accessToken")
                or resp_json.get("token")
                or resp_json.get("Token")
                or resp_json.get("access_token")
            )
            if not token:
                raise RuntimeError(f"No token in auth response. Keys: {list(resp_json.keys())}")
            return token
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise

    def _build_analysis_payload(self, tramite_guid: str, content_items: List[Content]) -> dict:
        """
        Aggregate validation results from all documents into the apply-analysis payload.

        Returns:
            {
                "TramiteGuid": "...",
                "StatusId": 2 or 1002,
                "Notes": "...",
                "RejectedRequirements": [...]
            }
        """
        rejected_requirements = []
        total_docs = 0
        passed_docs = 0
        failed_docs = 0
        excluded_docs = 0
        error_docs = 0

        for content in content_items:
            validation = content.data.get("validation_result", {})
            status = validation.get("status", "unknown")
            reject_reasons = validation.get("rejectReasons", [])

            # Get the tramiteRequirementId for this document
            # It's stored in the API data that was fetched in Step 3
            tramite_requirement_id = self._find_requirement_id(content)

            if status == "excluded":
                excluded_docs += 1
                continue

            total_docs += 1

            if status == "error":
                error_docs += 1
                failed_docs += 1
                if tramite_requirement_id:
                    rejected_requirements.append({
                        "TramiteRequirementId": tramite_requirement_id,
                        "RejectionReasonId": 7,  # placeholder — processing error
                        "RejectionReason": validation.get("reason", "Validation processing error"),
                    })
                continue

            if status == "skipped":
                # No extracted fields — flag as issue
                failed_docs += 1
                if tramite_requirement_id:
                    rejected_requirements.append({
                        "TramiteRequirementId": tramite_requirement_id,
                        "RejectionReasonId": 7,
                        "RejectionReason": "No fields could be extracted from this document.",
                    })
                continue

            # status == "validated"
            if reject_reasons:
                failed_docs += 1
                if tramite_requirement_id:
                    # Add each rejection reason as a separate entry
                    for reason in reject_reasons:
                        rejected_requirements.append({
                            "TramiteRequirementId": tramite_requirement_id,
                            "RejectionReasonId": reason.get("RejectionReasonId", 6),
                            "RejectionReason": reason.get("reason", "Validation failed"),
                        })
            else:
                passed_docs += 1

        # Determine overall status
        has_failures = len(rejected_requirements) > 0
        status_id = self.failure_status_id if has_failures else self.success_status_id

        # Build notes summary
        if has_failures:
            notes = (
                f"Case returned; AI validation found discrepancies in {failed_docs} document(s). "
                f"{passed_docs} passed, {failed_docs} failed, {excluded_docs} excluded, {error_docs} errors."
            )
        else:
            notes = (
                f"All requirements validated successfully. "
                f"{passed_docs} documents passed, {excluded_docs} excluded."
            )

        return {
            "TramiteGuid": tramite_guid,
            "StatusId": status_id,
            "Notes": notes,
            "RejectedRequirements": rejected_requirements,
        }

    def _find_requirement_id(self, content: Content) -> Optional[str]:
        """
        Find the TramiteRequirementId for this document.

        The API response from Step 3 stores tramiteRequirements in content.data.
        Each requirement has a tramiteRequirementId and documentPath.
        We match by canonical_id index or filename.
        """
        # Try to get from content data directly (set by api_fetch_executor)
        req_id = content.data.get("tramiteRequirementId")
        if req_id:
            return req_id

        # Try to match by document index against tramiteRequirements
        tramite_requirements = content.data.get("tramiteRequirements", [])
        doc_index = self._get_doc_index(content)

        if doc_index >= 0 and doc_index < len(tramite_requirements):
            req = tramite_requirements[doc_index]
            return req.get("tramiteRequirementId")

        # Fallback: try matching by filename
        filename = getattr(content.id, 'filename', '') or ''
        for req in tramite_requirements:
            doc_path = req.get("documentPath", "")
            if doc_path and filename and filename.lower() in doc_path.lower():
                return req.get("tramiteRequirementId")

        logger.warning(
            f"Could not find TramiteRequirementId for document "
            f"{getattr(content.id, 'canonical_id', 'unknown')} (index {doc_index})"
        )
        return None

    def _get_doc_index(self, content) -> int:
        """Extract document index from canonical_id (e.g., 3 from '..._03')."""
        import re
        canonical_id = getattr(content.id, 'canonical_id', '') or ''
        m = re.search(r'_(\d+)$', canonical_id)
        return int(m.group(1)) if m else -1

    def _get_tramite_guid(self, content_items: List[Content]) -> str:
        """Extract the TramiteGuid from content data."""
        for content in content_items:
            guid = content.data.get("tramiteGuid")
            if guid:
                return guid
            # Check metadata
            meta = getattr(content.id, 'metadata', {}) or {}
            guid = meta.get("tramite_guid")
            if guid:
                return guid
        return "unknown"

    async def process_input(
        self,
        input: Union[Content, List[Content]],
        ctx: WorkflowContext,
    ) -> Union[Content, List[Content]]:
        """
        Aggregate all document validation results and POST to Registros API.
        """
        # Normalize to list
        content_items = input if isinstance(input, list) else [input]

        tramite_guid = self._get_tramite_guid(content_items)
        logger.info(f"APICallbackExecutor: Processing {len(content_items)} documents for tramite {tramite_guid}")

        # Build the apply-analysis payload
        payload = self._build_analysis_payload(tramite_guid, content_items)

        logger.info(f"Apply-analysis payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")

        # Store payload in first content item for downstream access
        if content_items:
            content_items[0].data["apply_analysis_payload"] = payload
            content_items[0].summary_data["callback_status"] = "pending"

        # Always upload payload to blob storage (even in dry_run mode)
        await self._upload_callback_payload(tramite_guid, payload)

        if self.dry_run:
            logger.info("DRY RUN — skipping actual POST to Registros API")
            if content_items:
                content_items[0].summary_data["callback_status"] = "dry_run"
            return input

        # Authenticate and POST
        try:
            token = self._authenticate()
            url = f"{self.api_base_url}document-intelligence/apply-analysis"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            resp = requests.post(
                url,
                json=payload,
                headers=headers,
                verify=False,
                timeout=60,
            )

            logger.info(f"Apply-analysis response: {resp.status_code} — {resp.text[:500]}")

            if resp.status_code in (200, 201, 204):
                logger.info(f"Successfully posted analysis result for tramite {tramite_guid}")
                if content_items:
                    content_items[0].summary_data["callback_status"] = "success"
                    content_items[0].summary_data["callback_response"] = {
                        "status_code": resp.status_code,
                        "body": resp.text[:500],
                    }
            else:
                logger.error(
                    f"Apply-analysis POST failed: {resp.status_code} — {resp.text[:500]}"
                )
                if content_items:
                    content_items[0].summary_data["callback_status"] = "error"
                    content_items[0].summary_data["callback_response"] = {
                        "status_code": resp.status_code,
                        "body": resp.text[:500],
                    }

        except Exception as e:
            logger.error(f"Failed to POST analysis result: {e}")
            if content_items:
                content_items[0].summary_data["callback_status"] = "error"
                content_items[0].summary_data["callback_error"] = str(e)

        return input

    async def _get_connector(self):
        """Lazily initialize the blob connector."""
        if self._connector is None:
            import os
            account = self.storage_account_name
            if not account:
                account = os.environ.get("BLOB_STORAGE_ACCOUNT_NAME", os.environ.get("STORAGE_ACCOUNT_NAME", ""))
            if not account:
                logger.warning("No storage_account_name configured; skipping blob upload of callback payload.")
                return None
            from ..connectors import AzureBlobConnector
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

    async def _upload_callback_payload(self, tramite_guid: str, payload: dict):
        """Upload the callback payload JSON to a dedicated blob container."""
        try:
            connector = await self._get_connector()
            if connector is None:
                return

            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            blob_path = f"callback_payloads/{now.strftime('%Y/%m/%d')}/{tramite_guid}.json"

            data = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")

            await connector.upload_blob(
                container_name=self.container_name,
                blob_path=blob_path,
                data=data,
                overwrite=True,
            )
            logger.info(f"Uploaded callback payload to {self.container_name}/{blob_path}")
        except Exception as e:
            logger.error(f"Failed to upload callback payload to blob: {e}")
