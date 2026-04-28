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
    rag_enabled: bool = Field(default=True, alias="RAG_ENABLED")
    rag_embedding_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2", alias="RAG_EMBEDDING_MODEL"
    )
    rag_embedding_local_only: bool = Field(default=True, alias="RAG_EMBEDDING_LOCAL_ONLY")
    rag_top_k_schema: int = Field(default=5, alias="RAG_TOP_K_SCHEMA")
    rag_top_k_examples: int = Field(default=3, alias="RAG_TOP_K_EXAMPLES")
    rag_min_feedback_confidence: float = Field(default=0.65, alias="RAG_MIN_FEEDBACK_CONFIDENCE")

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
