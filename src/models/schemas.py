from pydantic import BaseModel, Field
from typing import Any


class QueryOptions(BaseModel):
    row_limit: int | None = Field(default=None, ge=1, le=5000)
    enable_multi_query_validation: bool = True


class QueryRequest(BaseModel):
    question: str = Field(min_length=3, description="Natural language question")
    session_id: str | None = None
    options: QueryOptions | None = None


class ConfidenceSignals(BaseModel):
    syntax_validity: float = 0.0
    alignment_score: float = 0.0
    sanity_score: float = 0.0
    multi_query_agreement: float = 0.0
    schema_coverage: float = 0.0


class ExecutionMeta(BaseModel):
    execution_time_ms: int = 0
    rows_returned: int = 0
    explain_plan: list[str] = Field(default_factory=list)


class QueryResponse(BaseModel):
    sql: str
    explanation: str
    results: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    signals: ConfidenceSignals
    warnings: list[str] = Field(default_factory=list)
    execution_meta: ExecutionMeta


class SchemaResponse(BaseModel):
    tables: list[dict[str, Any]] = Field(default_factory=list)


class HistoryItem(BaseModel):
    question: str
    sql: str
    confidence: float


class HistoryResponse(BaseModel):
    items: list[HistoryItem] = Field(default_factory=list)
