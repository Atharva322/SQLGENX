from __future__ import annotations

from dataclasses import dataclass, field

from src.llm.client import LLMClient
from src.models.schemas import QueryPlanDraft
from src.observability.request_trace import RequestTrace


def normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().split()).lower()


@dataclass
class QueryContext:
    question: str
    connection_id: str
    schema: dict
    schema_fingerprint: str
    trace: RequestTrace
    prompt: str = ""
    final_prompt: str = ""
    query_plan: QueryPlanDraft | None = None
    _backtranslations: dict[str, str] = field(default_factory=dict)

    def prompt_for_validation(self) -> str:
        return self.final_prompt or self.prompt

    def back_translate(self, llm: LLMClient, sql: str, prompt_context: str | None = None) -> str:
        key = normalize_sql(sql)
        if key not in self._backtranslations:
            self._backtranslations[key] = llm.back_translate_sql(
                sql=sql,
                prompt_context=prompt_context if prompt_context is not None else self.prompt_for_validation(),
            )
        return self._backtranslations[key]

