import copy
import logging
import requests
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timezone
from urllib.parse import quote
from agent_framework import WorkflowContext
from . import ParallelExecutor
from ..models import Content, ContentIdentifier
from ..connectors import AzureBlobConnector

logger = logging.getLogger("contentflow.executors.azure_blob_file_copy_executor")

# Log executor start for visibility
logger.info("AzureBlobFileCopyExecutor module loaded.")

class AzureBlobFileCopyExecutor(ParallelExecutor):
    """
    Downloads files from SAS URLs in Content data and uploads them to Azure Blob Storage as binary blobs.
    
    Configuration (settings dict):
        - storage_account_name (str): Azure Storage account name (required)
        - credential_type (str): Authentication type (default: "default_azure_credential")
        - credential_key (str): Storage account key (for azure_key_credential)
        - container_name (str): Target blob container name (required)
        - path_template (str): Path pattern for organizing blobs (supports {field} placeholders)
        - filename_template (str): Filename pattern (supports {field} placeholders)
        - overwrite_existing (bool): Whether to overwrite existing blobs (default: True)
    """
    def __init__(self, id: str, settings: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(id=id, settings=settings, **kwargs)
        self.storage_account_name = self.get_setting("storage_account_name", required=True)
        self.credential_type = self.get_setting("credential_type", default="default_azure_credential")
        self.credential_key = self.get_setting("credential_key", default=None)
        self.container_name = self.get_setting("container_name", required=True)
        self.path_template = self.get_setting("path_template", default="{pipeline_name}/{year}/{month}/{day}")
        self.filename_template = self.get_setting("filename_template", default="{document_id}_{timestamp}.bin")
        self.overwrite_existing = self.get_setting("overwrite_existing", default=True)
        self._connector = None

    async def _get_connector(self) -> AzureBlobConnector:
        if self._connector is None:
            connector_settings = {
                "account_name": self.storage_account_name,
                "credential_type": self.credential_type
            }
            if self.credential_key:
                connector_settings["credential_key"] = self.credential_key
            self._connector = AzureBlobConnector(
                name=f"blob_connector_{self.id}",
                settings=connector_settings
            )
            await self._connector.initialize()
        return self._connector

    def _find_document_urls(self, data: dict) -> List[Dict[str, str]]:
        """
        Find all document URLs in tramiteRequirements and additionalDocuments.
        Returns a list of dicts with keys: 'url', 'type', 'name'.
        """
        urls = []
        for req in data.get("tramiteRequirements", []):
            doc_path = req.get("documentPath")
            if doc_path:
                urls.append({"url": doc_path, "type": "main", "name": req.get("name") or "document"})
            for add_doc in req.get("additionalDocuments", []):
                add_doc_path = add_doc.get("documentPath")
                if add_doc_path:
                    urls.append({"url": add_doc_path, "type": "additional", "name": add_doc.get("name") or "additional_document"})
        return urls

    async def process_input(
        self,
        input: Union[Content, List[Content]],
        ctx: WorkflowContext
    ) -> Union[Content, List[Content]]:
        """Override process_input to return multiple Content items (one per PDF)."""
        if isinstance(input, list):
            all_results = []
            for content in input:
                result = await self._copy_and_split(content)
                all_results.extend(result)
            return all_results
        return await self._copy_and_split(input)

    async def _copy_and_split(self, content: Content) -> List[Content]:
        """Copy all PDFs to blob storage, return one Content per successful upload."""
        logger.info("AzureBlobFileCopyExecutor.process_content_item() called")
        if not content or not content.data:
            logger.error("Content must have data")
            raise ValueError("Content must have data")
        blob_connector = await self._get_connector()
        urls = self._find_document_urls(content.data)
        logger.info(f"Found {len(urls)} document URLs to process.")
        now = datetime.now(timezone.utc)
        output_contents = []
        for idx, doc in enumerate(urls):
            try:
                logger.info(f"Downloading file from {doc['url']}")
                # Azure Files REST API requires specific headers
                headers = {
                    "x-ms-version": "2024-11-04"
                }
                response = requests.get(doc["url"], stream=True, headers=headers, verify=False)
                logger.info(f"Download response status: {response.status_code}, headers: {dict(response.headers)}")
                response.raise_for_status()
                file_bytes = response.content
                # Build blob path and filename
                path = self.path_template.format(
                    pipeline_name=getattr(content, 'pipeline_name', 'pipeline'),
                    year=now.strftime("%Y"),
                    month=now.strftime("%m"),
                    day=now.strftime("%d")
                )
                # Extract original filename from URL (without query params)
                url_path = doc["url"].split("?")[0]
                original_filename = url_path.split("/")[-1]
                original_stem = original_filename.rsplit(".", 1)[0] if "." in original_filename else original_filename
                original_ext = original_filename.rsplit(".", 1)[1] if "." in original_filename else "bin"
                canonical_id = getattr(content.id, 'canonical_id', f'doc{idx}')
                filename = f"{canonical_id}_{idx:02d}_{original_stem}.{original_ext}"
                blob_path = path + filename
                logger.info(f"Uploading file to blob path: {blob_path}")
                result = await blob_connector.upload_blob(
                    container_name=self.container_name,
                    blob_path=blob_path,
                    data=file_bytes,
                    overwrite=self.overwrite_existing,
                    metadata={"source_url": quote(doc["url"], safe=":/?#[]@!$&'()*+,;=-._~")}
                )
                logger.info(f"Uploaded {doc['url']} to {blob_path}")

                # Create a new Content item for this PDF
                child_content = Content(
                    id=ContentIdentifier(
                        canonical_id=f"{canonical_id}_{idx:02d}",
                        unique_id=f"{canonical_id}_{idx:02d}",
                        source_type="azure_blob",
                        source_name=self.storage_account_name,
                        container=self.container_name,
                        path=blob_path,
                        filename=filename,
                        metadata=content.id.metadata.copy() if content.id and content.id.metadata else {}
                    ),
                    data=copy.deepcopy(content.data),
                    summary_data={
                        "blob_file_copy": {
                            "blob_path": blob_path,
                            "blob_size": len(file_bytes),
                            "blob_etag": result.get('etag'),
                            "blob_last_modified": result.get('last_modified').isoformat() if result.get('last_modified') else None,
                            "write_status": "success",
                            "source_url": doc["url"],
                            "requirement_name": doc.get("name", ""),
                            "original_filename": original_filename,
                        }
                    }
                )
                output_contents.append(child_content)

            except Exception as e:
                logger.error(f"Failed to upload {doc['url']}: {e}")
                # Skip failed uploads — don't create a Content for them

        logger.info(f"Completed processing {len(urls)} files. Created {len(output_contents)} Content items.")
        return output_contents

    async def process_content_item(self, content: Content) -> Content:
        # Not used — process_input is overridden
        raise NotImplementedError("Use process_input instead")
