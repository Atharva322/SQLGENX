from functools import lru_cache
import json

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env.local", env_file_encoding="utf-8", extra="ignore"
    )

    database_url: str = Field(
        default="postgresql://text2sql_user:text2sql_pass@localhost:5432/sample_company",
        alias="DATABASE_URL",
    )
    connection_urls_json: str = Field(default="", alias="CONNECTION_URLS_JSON")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    llm_provider: str = Field(default="anthropic", alias="LLM_PROVIDER")
    llm_model: str = Field(default="claude-sonnet-4-20250514", alias="LLM_MODEL")

    max_result_rows: int = Field(default=1000, alias="MAX_RESULT_ROWS")
    max_subquery_depth: int = Field(default=3, alias="MAX_SUBQUERY_DEPTH")
    max_explain_rows: int = Field(default=1_000_000, alias="MAX_EXPLAIN_ROWS")
    enable_multi_query_validation: bool = Field(
        default=True, alias="ENABLE_MULTI_QUERY_VALIDATION"
    )
    multi_query_complexity_threshold: int = Field(
        default=2, alias="MULTI_QUERY_COMPLEXITY_THRESHOLD"
    )
    db_connect_timeout_seconds: int = Field(default=5, alias="DB_CONNECT_TIMEOUT_SECONDS")
    db_pool_timeout_seconds: int = Field(default=10, alias="DB_POOL_TIMEOUT_SECONDS")
    db_pool_recycle_seconds: int = Field(default=1800, alias="DB_POOL_RECYCLE_SECONDS")
    rag_enabled: bool = Field(default=True, alias="RAG_ENABLED")
    rag_embedding_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2", alias="RAG_EMBEDDING_MODEL"
    )
    rag_embedding_local_only: bool = Field(default=True, alias="RAG_EMBEDDING_LOCAL_ONLY")
    rag_top_k_schema: int = Field(default=5, alias="RAG_TOP_K_SCHEMA")
    rag_top_k_examples: int = Field(default=3, alias="RAG_TOP_K_EXAMPLES")
    rag_min_feedback_confidence: float = Field(default=0.65, alias="RAG_MIN_FEEDBACK_CONFIDENCE")
    schema_linking_enabled: bool = Field(default=True, alias="SCHEMA_LINKING_ENABLED")
    schema_synonym_source: str = Field(default="", alias="SCHEMA_SYNONYM_SOURCE")
    schema_link_top_k: int = Field(default=8, alias="SCHEMA_LINK_TOP_K")
    constrained_sql_enabled: bool = Field(default=True, alias="CONSTRAINED_SQL_ENABLED")
    constrained_sql_strict_identifiers: bool = Field(
        default=True, alias="CONSTRAINED_SQL_STRICT_IDENTIFIERS"
    )
    identifier_resolution_fail_fast_enabled: bool = Field(
        default=True, alias="IDENTIFIER_RESOLUTION_FAIL_FAST_ENABLED"
    )
    fail_fast_min_link_confidence: float = Field(
        default=0.25, alias="FAIL_FAST_MIN_LINK_CONFIDENCE"
    )
    fail_fast_max_unresolved: int = Field(default=6, alias="FAIL_FAST_MAX_UNRESOLVED")
    fail_fast_require_low_confidence: bool = Field(
        default=True, alias="FAIL_FAST_REQUIRE_LOW_CONFIDENCE"
    )
    join_grounding_strict_enabled: bool = Field(default=True, alias="JOIN_GROUNDING_STRICT_ENABLED")
    alternative_sql_adaptive_enabled: bool = Field(
        default=True, alias="ALTERNATIVE_SQL_ADAPTIVE_ENABLED"
    )
    alternative_sql_complexity_threshold: int = Field(
        default=2, alias="ALTERNATIVE_SQL_COMPLEXITY_THRESHOLD"
    )
    multi_query_easy_skip_enabled: bool = Field(default=True, alias="MULTI_QUERY_EASY_SKIP_ENABLED")
    intermediate_trace_logging_enabled: bool = Field(
        default=True, alias="INTERMEDIATE_TRACE_LOGGING_ENABLED"
    )
    phase1_cache_enabled: bool = Field(default=True, alias="PHASE1_CACHE_ENABLED")
    schema_cache_ttl_seconds: int = Field(default=300, alias="SCHEMA_CACHE_TTL_SECONDS")
    schema_cache_max_entries: int = Field(default=32, alias="SCHEMA_CACHE_MAX_ENTRIES")
    prompt_asset_cache_enabled: bool = Field(default=True, alias="PROMPT_ASSET_CACHE_ENABLED")
    prompt_asset_cache_max_entries: int = Field(default=32, alias="PROMPT_ASSET_CACHE_MAX_ENTRIES")
    context_index_enabled: bool = Field(default=False, alias="CONTEXT_INDEX_ENABLED")
    context_index_path: str = Field(default=".tmp/context_index.sqlite3", alias="CONTEXT_INDEX_PATH")
    context_index_embedding_model: str = Field(
        default="deterministic-hash-v1", alias="CONTEXT_INDEX_EMBEDDING_MODEL"
    )
    context_index_chunking_version: str = Field(
        default="schema-doc-v1", alias="CONTEXT_INDEX_CHUNKING_VERSION"
    )
    context_index_rrf_k: int = Field(default=60, alias="CONTEXT_INDEX_RRF_K")
    adaptive_validation_enabled: bool = Field(default=False, alias="ADAPTIVE_VALIDATION_ENABLED")
    adaptive_validation_mode: str = Field(default="shadow", alias="ADAPTIVE_VALIDATION_MODE")
    risk_fast_max_score: float = Field(default=0.34, alias="RISK_FAST_MAX_SCORE")
    risk_standard_max_score: float = Field(default=0.67, alias="RISK_STANDARD_MAX_SCORE")
    risk_low_link_confidence: float = Field(default=0.55, alias="RISK_LOW_LINK_CONFIDENCE")
    risk_low_model_confidence: float = Field(default=0.55, alias="RISK_LOW_MODEL_CONFIDENCE")
    risk_low_retrieval_margin: float = Field(default=0.08, alias="RISK_LOW_RETRIEVAL_MARGIN")
    risk_high_scan_rows: int = Field(default=100_000, alias="RISK_HIGH_SCAN_ROWS")
    ast_validation_enabled: bool = Field(default=True, alias="AST_VALIDATION_ENABLED")
    ast_allow_select_star: bool = Field(default=True, alias="AST_ALLOW_SELECT_STAR")
    async_runtime_enabled: bool = Field(default=True, alias="ASYNC_RUNTIME_ENABLED")
    async_query_max_workers: int = Field(default=4, alias="ASYNC_QUERY_MAX_WORKERS")
    async_query_queue_limit: int = Field(default=8, alias="ASYNC_QUERY_QUEUE_LIMIT")
    async_query_queue_timeout_seconds: float = Field(
        default=0.25, alias="ASYNC_QUERY_QUEUE_TIMEOUT_SECONDS"
    )
    async_query_total_timeout_seconds: float = Field(
        default=30.0, alias="ASYNC_QUERY_TOTAL_TIMEOUT_SECONDS"
    )
    async_query_retry_after_seconds: int = Field(
        default=2, alias="ASYNC_QUERY_RETRY_AFTER_SECONDS"
    )
    provider_timeout_seconds: float = Field(default=20.0, alias="PROVIDER_TIMEOUT_SECONDS")
    retrieval_timeout_seconds: float = Field(default=5.0, alias="RETRIEVAL_TIMEOUT_SECONDS")
    explain_timeout_seconds: float = Field(default=5.0, alias="EXPLAIN_TIMEOUT_SECONDS")
    statement_timeout_seconds: float = Field(default=10.0, alias="STATEMENT_TIMEOUT_SECONDS")

    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    def connection_urls(self) -> dict[str, str]:
        if not self.connection_urls_json.strip():
            return {}
        try:
            parsed = json.loads(self.connection_urls_json)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items()}
        except json.JSONDecodeError:
            pass
        return {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
