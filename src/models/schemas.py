from pydantic import BaseModel, Field
from typing import Any, Literal


class QueryOptions(BaseModel):
    row_limit: int | None = Field(default=None, ge=1, le=5000)
    enable_multi_query_validation: bool = True


class QueryRequest(BaseModel):
    question: str = Field(min_length=3, description="Natural language question")
    connection_id: str | None = None
    session_id: str | None = None
    sql_override: str | None = Field(
        default=None, description="Optional user-edited SQL to execute instead of generated SQL."
    )
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
    stage_latencies_ms: dict[str, int] = Field(default_factory=dict)
    llm_token_usage: dict[str, Any] = Field(default_factory=dict)
    failure_classification: str | None = None


class ReasoningMeta(BaseModel):
    strategy: str = "single_pass"
    selected_candidate: str = "primary"
    candidate_scores: list[dict[str, Any]] = Field(default_factory=list)
    validator_notes: list[str] = Field(default_factory=list)
    query_plan: dict[str, Any] = Field(default_factory=dict)


class SchemaCandidate(BaseModel):
    identifier: str
    kind: Literal["table", "column"]
    canonical_table: str | None = None
    canonical_column: str | None = None
    score: float = 0.0
    evidence: list[str] = Field(default_factory=list)
    matched_synonyms: list[str] = Field(default_factory=list)


class ResolvedIdentifierSet(BaseModel):
    tables: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    join_hints: list[str] = Field(default_factory=list)


class LinkingContext(BaseModel):
    normalized_question: str
    schema_fingerprint: str
    candidates: list[SchemaCandidate] = Field(default_factory=list)
    resolved: ResolvedIdentifierSet = Field(default_factory=ResolvedIdentifierSet)
    ambiguous: bool = False
    ambiguity_reasons: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    synonym_hits: list[str] = Field(default_factory=list)
    unresolved_identifiers: list[str] = Field(default_factory=list)
    resolution_status: str = "resolved"
    join_grounding_status: str = "unknown"
    retrieval_meta: dict[str, Any] = Field(default_factory=dict)


class QueryPlanDraft(BaseModel):
    intent: str = "select"
    target_tables: list[str] = Field(default_factory=list)
    target_columns: list[str] = Field(default_factory=list)
    grouping: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    join_path: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ConstraintValidationResult(BaseModel):
    passed: bool = True
    blocked_identifiers: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    violation_type: str | None = None
    enforced_policy: str | None = None


class AccessedSchema(BaseModel):
    tables: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)


class QueryResponse(BaseModel):
    query_id: str
    connection_id: str
    session_id: str
    sql: str
    explanation: str
    accessed: AccessedSchema = Field(default_factory=AccessedSchema)
    results: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    signals: ConfidenceSignals
    warnings: list[str] = Field(default_factory=list)
    execution_meta: ExecutionMeta
    reasoning: ReasoningMeta = Field(default_factory=ReasoningMeta)
    linking_meta: LinkingContext | None = None
    constraint_meta: ConstraintValidationResult | None = None


class SchemaResponse(BaseModel):
    tables: list[dict[str, Any]] = Field(default_factory=list)


class FeedbackPayload(BaseModel):
    verdict: Literal["correct", "incorrect"]
    notes: str | None = None


class HistoryItem(BaseModel):
    query_id: str
    connection_id: str
    session_id: str
    question: str
    sql: str
    explanation: str
    confidence: float
    signals: ConfidenceSignals
    warnings: list[str] = Field(default_factory=list)
    results: list[dict[str, Any]] = Field(default_factory=list)
    execution_meta: ExecutionMeta
    reasoning: ReasoningMeta = Field(default_factory=ReasoningMeta)
    linking_meta: LinkingContext | None = None
    constraint_meta: ConstraintValidationResult | None = None
    feedback: FeedbackPayload | None = None


class HistoryResponse(BaseModel):
    items: list[HistoryItem] = Field(default_factory=list)


class FeedbackRequest(BaseModel):
    query_id: str = Field(min_length=3)
    session_id: str | None = None
    verdict: Literal["correct", "incorrect"]
    notes: str | None = None


class FeedbackResponse(BaseModel):
    query_id: str
    stored: bool
    target_file: str


class ConnectionsResponse(BaseModel):
    connections: dict[str, str] = Field(default_factory=dict)


class ConnectionsHealthResponse(BaseModel):
    connections: dict[str, dict[str, Any]] = Field(default_factory=dict)
