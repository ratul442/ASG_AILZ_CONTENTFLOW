from agent_framework import WorkflowContext
from typing import Union, List
import logging
from typing import Any, Dict, Optional
import requests
import json
from .input_executor import InputExecutor
from ..models import Content, ContentIdentifier


logger = logging.getLogger("contentflow.executors.api_fetch_executor")

logger.info("APIFetchExecutor module loaded.")


# Updated APIFetchExecutor for ASG Registros staging API
class APIFetchExecutor(InputExecutor):
    """
    Fetches case details from the ASG Registros staging API using authentication and outputs as Content item.
    
    Configuration (settings dict):
        - api_base_url (str): Base API URL (e.g., https://rulrup-staging-api.azurewebsites.net/api/)
        - username (str): API username
        - password (str): API password
        - api_key (str): API key (JWT)
        - tramite_guid (str): The tramiteGuid to fetch
        - sas_base_url (str): SAS base URL for blob storage
        - sas_token (str): SAS token for blob storage
    """
    def __init__(self, id: str = None, settings: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)
        self.api_base_url = self.get_setting("api_base_url", required=True)
        self.username = self.get_setting("username", required=True)
        self.password = self.get_setting("password", required=True)
        self.api_key = self.get_setting("api_key", required=True)
        self.tramite_guid = self.get_setting("tramite_guid", required=True)
        self.sas_base_url = self.get_setting("sas_base_url", required=True)
        self.sas_token = self.get_setting("sas_token", required=True)

    def _build_sas_url(self, document_path: str) -> str:
        if not document_path:
            return None
        # Remove base URL and query if present
        if document_path.startswith(self.sas_base_url):
            document_path = document_path[len(self.sas_base_url):]
        document_path = document_path.split('?')[0]
        document_path = document_path.lstrip('/')
        return f"{self.sas_base_url}{document_path}?{self.sas_token}"

    def _authenticate(self) -> str:
        auth_url = f"{self.api_base_url}Authentication/Credentials"
        payload = {
            "Username": self.username,
            "Password": self.password,
            "ApiKey": self.api_key
        }
        try:
            resp = requests.post(auth_url, json=payload)
            resp.raise_for_status()
            token = resp.json().get("token")
            if not token:
                logger.error("Authentication failed: No token returned.")
                raise RuntimeError("Authentication failed: No token returned.")
            return token
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise

    def _patch_document_paths(self, data: dict) -> None:
        tramite_requirements = data.get("tramiteRequirements", [])
        for req in tramite_requirements:
            doc_path = req.get("documentPath")
            if doc_path:
                req["documentPath"] = self._build_sas_url(doc_path)
            # Patch additionalDocuments
            for add_doc in req.get("additionalDocuments", []):
                add_doc_path = add_doc.get("documentPath")
                if add_doc_path:
                    add_doc["documentPath"] = self._build_sas_url(add_doc_path)

    async def crawl(self, checkpoint_timestamp=None, continuation_token=None, **kwargs):
        logger.info("APIFetchExecutor.crawl() method entered")
        logger.info(f"Authenticating for tramite_guid: {self.tramite_guid}")
        token = self._authenticate()
        url = f"{self.api_base_url}document-intelligence/details?tramiteGuid={self.tramite_guid}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            logger.info("Fetched case details: %s", json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to retrieve case details: {e}")
            raise

        # Patch all document paths (including additionalDocuments)
        self._patch_document_paths(data)

        # Log all SAS URLs
        tramite_requirements = data.get("tramiteRequirements", [])
        logger.info("Full SAS URLs for document paths:")
        for req in tramite_requirements:
            doc_path = req.get("documentPath")
            if doc_path:
                logger.info("- %s", doc_path)
            for add_doc in req.get("additionalDocuments", []):
                add_doc_path = add_doc.get("documentPath")
                if add_doc_path:
                    logger.info("- %s", add_doc_path)

        # Wrap in Content item for downstream executors with proper identifier
        content_id = ContentIdentifier(
            canonical_id=self.tramite_guid,
            unique_id=self.tramite_guid,
            source_name=self.get_setting("storage_account_name", default="stce5y5fcsedfpc"),
            source_type="azure_blob",
            container=self.get_setting("blob_container_name", default="content"),
            metadata={"tramite_guid": self.tramite_guid}
        )
        content = Content(id=content_id, data=data)
        logger.info(f"Yielding content with id: {content_id.canonical_id}")
        if not data or not tramite_requirements:
            logger.warning("No tramiteRequirements found in API response. No content will be yielded.")
        yield content, None

    async def process_input(
        self,
        input: Union['Content', List['Content']],
        ctx: WorkflowContext[Union['Content', List['Content']], Union['Content', List['Content']]]
    ) -> Union['Content', List['Content']]:
        """
        Fetch data from the API by invoking crawl() and return the resulting Content items.
        """
        logger.info(f"{self.id}: process_input called, invoking crawl()...")
        content_items = []

        async for item_or_batch, continuation_token in self.crawl(checkpoint_timestamp=None):
            if item_or_batch is not None:
                if isinstance(item_or_batch, list):
                    content_items.extend(item_or_batch)
                else:
                    content_items.append(item_or_batch)

            if continuation_token is None:
                break

        logger.info(f"{self.id}: crawl() returned {len(content_items)} content item(s)")

        if len(content_items) == 1:
            return content_items[0]
        return content_items
