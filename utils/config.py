"""
Shared configuration, constants, and table definitions for db-migrator.
"""
from typing import Dict, List, Optional
from dataclasses import dataclass, field

# Storage namespace prefix for localStorage keys
STORAGE_PREFIX = "db_migrator_"

# Storage keys
STORAGE_KEYS = {
    "source_connection": f"{STORAGE_PREFIX}source_connection",
    "target_connection": f"{STORAGE_PREFIX}target_connection",
    "table_prefix": f"{STORAGE_PREFIX}table_prefix",
    "selected_users": f"{STORAGE_PREFIX}selected_users",
    "document_filters": f"{STORAGE_PREFIX}document_filters",
    "mapping_config": f"{STORAGE_PREFIX}mapping_config",
    "last_profile": f"{STORAGE_PREFIX}last_profile",
}


@dataclass
class TableDefinition:
    """Definition of a source table with its logical name and query."""
    logical_name: str
    name_template: str  # Use {prefix} as placeholder
    has_prefix: bool = True
    query_template: Optional[str] = None


# Logical table definitions with prefix templates
TABLE_DEFINITIONS: Dict[str, TableDefinition] = {
    "users": TableDefinition(
        logical_name="users",
        name_template="{prefix}_users",
        query_template="""
            SELECT id, name, letter_checkbox, created_at, last_connected, times_connected,
                   token_used, words_used, phone_number, company_name, company_name_in_hebrew,
                   job, department, email, __group_id__, token_limit, model, history_categories,
                   enabled_features, azure_oid, subfeatures, last_name
            FROM public.{table_name}
        """
    ),
    "folders": TableDefinition(
        logical_name="folders",
        name_template="{prefix}_folders",
        query_template="""
            SELECT id, folder_name, owner_id, parent_id, created_at, folder_type
            FROM public.{table_name}
        """
    ),
    "custom_documents": TableDefinition(
        logical_name="custom_documents",
        name_template="{prefix}_custom_documents",
        query_template="""
            SELECT doc_id, created_at, owner_id, doc_name_origin, doc_title, doc_size,
                   folder_id, doc_description, doc_type, vector_methods, doc_summery,
                   doc_summery_modified_by, doc_summery_modified_at, tags, embedding_model,
                   blob_source, version, doc_checksum, data_integration_doc_metadata
            FROM public.{table_name}
        """
    ),
    "embeddings": TableDefinition(
        logical_name="embeddings",
        name_template="{prefix}",  # Just the prefix itself
        query_template="""
            SELECT id, external_id, collection, document, metadata, embeddings
            FROM public.{table_name}
        """
    ),
    "users_groups": TableDefinition(
        logical_name="users_groups",
        name_template="{prefix}_users_groups",
        query_template="""
            SELECT id, group_name, default_model, default_max_tokens_per_user, enabled_features
            FROM public.{table_name}
        """
    ),
    "agents": TableDefinition(
        logical_name="agents",
        name_template="playground_bot_generator_config",
        has_prefix=False,
        query_template="""
            SELECT bot_id, user_id, bot_data, tags, folder_id, created_at
            FROM public.{table_name}
        """
    ),
}

# Extraction order (respecting foreign keys)
EXTRACTION_ORDER = [
    "users_groups",
    "users",
    "folders",
    "custom_documents",
    "embeddings",
    "agents",
]


def get_table_name(logical_name: str, prefix: str) -> str:
    """Get the actual table name given a logical name and prefix."""
    if logical_name not in TABLE_DEFINITIONS:
        raise ValueError(f"Unknown table: {logical_name}")
    
    table_def = TABLE_DEFINITIONS[logical_name]
    if table_def.has_prefix:
        return table_def.name_template.format(prefix=prefix)
    return table_def.name_template


def get_all_table_names(prefix: str) -> Dict[str, str]:
    """Get all actual table names for a given prefix."""
    return {
        logical: get_table_name(logical, prefix)
        for logical in TABLE_DEFINITIONS.keys()
    }


def get_query_for_table(logical_name: str, prefix: str) -> str:
    """Get the SELECT query for a table."""
    if logical_name not in TABLE_DEFINITIONS:
        raise ValueError(f"Unknown table: {logical_name}")
    
    table_def = TABLE_DEFINITIONS[logical_name]
    table_name = get_table_name(logical_name, prefix)
    return table_def.query_template.format(table_name=table_name)


# Default V4 to V5 mapping configuration
DEFAULT_MAPPINGS = {
    "users": {
        "source_table": "{prefix}_users",
        "target_table": "users",
        "target_schema": "user_db",
        "columns": [
            {"source": "id", "target": "id", "type": "varchar(255)"},
            {"source": "name", "target": "firstname", "type": "varchar(255)"},
            {"source": "last_name", "target": "lastname", "type": "varchar(255)"},
            {"source": "email", "target": "email", "type": "varchar(255)"},
            {"source": "phone_number", "target": "mobile_user_id", "type": "varchar(255)"},
            {"source": "azure_oid", "target": "organization_id", "type": "uuid"},
            {"source": "__group_id__", "target": "__group_id__", "type": "varchar(255)", "flag": "needs manual mapping"},
            {"source": "created_at", "target": "created_at", "type": "timestamp"},
        ],
        "drop_columns": ["letter_checkbox", "times_connected", "token_used", "words_used", 
                         "company_name", "company_name_in_hebrew", "job", "department",
                         "token_limit", "model", "history_categories", "enabled_features",
                         "subfeatures", "last_connected"],
    },
    "folders": {
        "source_table": "{prefix}_folders",
        "target_table": "folders",
        "target_schema": "document_db",
        "columns": [
            {"source": "id", "target": "id", "type": "uuid"},
            {"source": "folder_name", "target": "folder_name", "type": "varchar(255)"},
            {"source": "owner_id", "target": "user_id", "type": "uuid"},
            {"source": "parent_id", "target": "parent_id", "type": "uuid"},
            {"source": "created_at", "target": "created_at", "type": "timestamp"},
            {"source": "folder_type", "target": "folder_type", "type": "varchar(255)", "flag": "needs manual mapping"},
        ],
    },
    "custom_documents": {
        "source_table": "{prefix}_custom_documents",
        "target_table": "documents",
        "target_schema": "document_db",
        "columns": [
            {"source": "doc_id", "target": "id", "type": "uuid"},
            {"source": "doc_name_origin", "target": "blob_name", "type": "varchar(255)"},
            {"source": "doc_title", "target": "indexer_type", "type": "varchar(255)", "flag": "needs review"},
            {"source": "owner_id", "target": "user_id", "type": "uuid"},
            {"source": "folder_id", "target": "folder_id", "type": "uuid", "flag": "needs mapping"},
            {"source": "tags", "target": "tags", "type": "jsonb", "flag": "needs mapping"},
            {"source": "created_at", "target": "created_at", "type": "timestamp"},
        ],
    },
    "embeddings": {
        "source_table": "{prefix}",
        "target_table": "embeddings",
        "target_schema": "document_db",
        "columns": [
            {"source": "id", "target": "id", "type": "uuid"},
            {"source": "external_id", "target": "external_id", "type": "varchar(255)"},
            {"source": "collection", "target": "collection", "type": "varchar(255)"},
            {"source": "document", "target": "document", "type": "text"},
            {"source": "metadata", "target": "metadata", "type": "jsonb"},
            {"source": "embeddings", "target": "embeddings", "type": "vector"},
        ],
        "flag": "needs schema review for chunks vs embeddings split",
    },
    "agents": {
        "source_table": "playground_bot_generator_config",
        "target_table": "agents",
        "target_schema": "completion_db",
        "columns": [
            {"source": "bot_id", "target": "id", "type": "uuid"},
            {"source": "user_id", "target": "name", "type": "varchar(128)", "flag": "needs review, seems wrong"},
            {"source": "bot_data", "target": "bot_data", "type": "jsonb", "flag": "needs JSON field mapping"},
            {"source": "tags", "target": "tags", "type": "jsonb", "flag": "needs mapping"},
            {"source": "folder_id", "target": "folder_id", "type": "uuid", "flag": "needs mapping"},
        ],
    },
    "users_groups": {
        "source_table": "{prefix}_users_groups",
        "target_table": "users_groups",
        "target_schema": "user_db",
        "flag": "needs target schema definition",
        "columns": [
            {"source": "id", "target": "id", "type": "uuid"},
            {"source": "group_name", "target": "group_name", "type": "varchar(255)"},
            {"source": "default_model", "target": "default_model", "type": "varchar(255)"},
            {"source": "default_max_tokens_per_user", "target": "default_max_tokens_per_user", "type": "integer"},
            {"source": "enabled_features", "target": "enabled_features", "type": "jsonb"},
        ],
    },
}


# Session state keys
class SessionKeys:
    SOURCE_CONNECTION = "source_connection"
    TARGET_CONNECTION = "target_connection"
    TABLE_PREFIX = "table_prefix"
    RESOLVED_TABLES = "resolved_tables"
    SELECTED_USERS = "selected_users"
    SELECTED_USER_IDS = "selected_user_ids"
    DOCUMENT_FILTERS = "document_filters"
    EXTRACTED_DATA = "extracted_data"
    TRANSFORMED_DATA = "transformed_data"
    MAPPING_CONFIG = "mapping_config"
    MIGRATION_LOG = "migration_log"
