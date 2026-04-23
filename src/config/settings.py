from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env.local', env_file_encoding='utf-8', extra='ignore')

    database_url: str = Field(default='postgresql://text2sql_user:text2sql_pass@localhost:5432/sample_company', alias='DATABASE_URL')
    openai_api_key: str = Field(default='', alias='OPENAI_API_KEY')
    anthropic_api_key: str = Field(default='', alias='ANTHROPIC_API_KEY')
    llm_provider: str = Field(default='anthropic', alias='LLM_PROVIDER')
    llm_model: str = Field(default='claude-sonnet-4-20250514', alias='LLM_MODEL')

    max_result_rows: int = Field(default=1000, alias='MAX_RESULT_ROWS')
    max_subquery_depth: int = Field(default=3, alias='MAX_SUBQUERY_DEPTH')
    max_explain_rows: int = Field(default=1_000_000, alias='MAX_EXPLAIN_ROWS')

    api_host: str = Field(default='0.0.0.0', alias='API_HOST')
    api_port: int = Field(default=8000, alias='API_PORT')
    log_level: str = Field(default='INFO', alias='LOG_LEVEL')


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
