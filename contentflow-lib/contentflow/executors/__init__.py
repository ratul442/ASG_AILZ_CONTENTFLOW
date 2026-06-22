"""Executor implementations for content processing workflows."""

import logging as _logging
_logger = _logging.getLogger(__name__)

def _safe_import(module_path, class_name):
    """Safely import an executor class, returning None if dependencies are missing."""
    try:
        import importlib
        mod = importlib.import_module(module_path, package="contentflow.executors")
        return getattr(mod, class_name)
    except (ImportError, NameError, AttributeError) as e:
        _logger.debug(f"Optional executor {class_name} unavailable: {e}")
        return None

from .base import BaseExecutor

# Parallel processing executor
from .parallel_executor import ParallelExecutor

# Input executor
from .input_executor import InputExecutor

# Core executors (always available)
from .azure_blob_input_discovery import AzureBlobInputDiscoveryExecutor
from .azure_blob_content_retriever import AzureBlobContentRetrieverExecutor
from .azure_blob_output_executor import AzureBlobOutputExecutor
from .content_retriever import ContentRetrieverExecutor
from .ai_search_index_output import AISearchIndexOutputExecutor
from .azure_document_intelligence_extractor import AzureDocumentIntelligenceExtractorExecutor
from .azure_content_understanding_extractor import AzureContentUnderstandingExtractorExecutor
from .recursive_text_chunker_executor import RecursiveTextChunkerExecutor
from .csv_extractor import CSVExtractorExecutor
from .table_row_splitter_executor import TableRowSplitterExecutor
from .summarization_executor import SummarizationExecutor
from .entity_extraction_executor import EntityExtractionExecutor
from .sentiment_analysis_executor import SentimentAnalysisExecutor
from .content_classifier_executor import ContentClassifierExecutor
from .pii_detector_executor import PIIDetectorExecutor
from .keyword_extractor_executor import KeywordExtractorExecutor
from .language_detector_executor import LanguageDetectorExecutor
from .translation_executor import TranslationExecutor
from .field_mapper_executor import FieldMapperExecutor
from .field_selector_executor import FieldSelectorExecutor
from .field_validation_executor import FieldValidationExecutor
from .web_validation_executor import WebValidationExecutor
from .api_callback_executor import APICallbackExecutor
from .gptrag_search_index_doc_generator import GPTRAGSearchIndexDocumentGeneratorExecutor
from .pass_through import PassThroughExecutor
from .cosmos_db_lookup_executor import CosmosDBLookupExecutor

# Document Set executors
from .document_set_initializer import DocumentSetInitializerExecutor
from .document_set_collector import DocumentSetCollectorExecutor
from .cross_document_executor import CrossDocumentExecutor

# Control Flow executors
from .for_each_content import ForEachContentExecutor

# Optional executors (may fail if dependencies like openai, playwright, etc. are missing)
AzureOpenAIAgentExecutor = _safe_import(".azure_openai_agent_executor", "AzureOpenAIAgentExecutor")
AzureOpenAIEmbeddingsExecutor = _safe_import(".azure_openai_embeddings_executor", "AzureOpenAIEmbeddingsExecutor")
PDFExtractorExecutor = _safe_import(".pdf_extractor", "PDFExtractorExecutor")
WordExtractorExecutor = _safe_import(".word_extractor", "WordExtractorExecutor")
PowerPointExtractorExecutor = _safe_import(".powerpoint_extractor", "PowerPointExtractorExecutor")
ExcelExtractorExecutor = _safe_import(".excel_extractor", "ExcelExtractorExecutor")
BrowserValidationExecutor = _safe_import(".browser_validation_executor", "BrowserValidationExecutor")
WebScrapingExecutor = _safe_import(".web_scraping_executor", "WebScrapingExecutor")
CrossDocumentComparisonExecutor = _safe_import(".cross_document_comparison", "CrossDocumentComparisonExecutor")
CrossDocumentFieldAggregatorExecutor = _safe_import(".cross_document_field_aggregator", "CrossDocumentFieldAggregatorExecutor")

from .executor_registry import ExecutorRegistry
from .executor_config import ExecutorConfig, ExecutorInstanceConfig

__all__ = [
    # Base
    "BaseExecutor",
    "InputExecutor",
    # Parallel processing
    "ParallelExecutor",
    # Specialized executors
    "AzureBlobInputDiscoveryExecutor",
    "AzureBlobContentRetrieverExecutor",
    "AzureBlobOutputExecutor",
    "ContentRetrieverExecutor",
    "AISearchIndexOutputExecutor",
    "AzureDocumentIntelligenceExtractorExecutor",
    "AzureContentUnderstandingExtractorExecutor",
    "PDFExtractorExecutor",
    "WebScrapingExecutor",
    "RecursiveTextChunkerExecutor",
    "WordExtractorExecutor",
    "PowerPointExtractorExecutor",
    "ExcelExtractorExecutor",
    "CSVExtractorExecutor",
    "TableRowSplitterExecutor",
    "AzureOpenAIAgentExecutor",
    "AzureOpenAIEmbeddingsExecutor",
    "SummarizationExecutor",
    "EntityExtractionExecutor",
    "SentimentAnalysisExecutor",
    "ContentClassifierExecutor",
    "PIIDetectorExecutor",
    "KeywordExtractorExecutor",
    "LanguageDetectorExecutor",
    "TranslationExecutor",
    "FieldMapperExecutor",
    "FieldSelectorExecutor",
    "FieldValidationExecutor",
    "WebValidationExecutor",
    "BrowserValidationExecutor",
    "GPTRAGSearchIndexDocumentGeneratorExecutor",
    "PassThroughExecutor",
    "CosmosDBLookupExecutor",
    # Document Set
    "DocumentSetInitializerExecutor",
    "DocumentSetCollectorExecutor",
    "CrossDocumentExecutor",
    "CrossDocumentComparisonExecutor",
    "CrossDocumentFieldAggregatorExecutor",
    # Control Flow
    "ForEachContentExecutor",
    # # Knowledge Graph
    # "KnowledgeGraphEntityExtractorExecutor",
    # "KnowledgeGraphWriterExecutor",
    # "KnowledgeGraphQueryExecutor",
    # "KnowledgeGraphEnrichmentExecutor",
    # Registry and config
    "ExecutorRegistry",
    "ExecutorConfig",
    "ExecutorInstanceConfig",
]
