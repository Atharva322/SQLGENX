from time import perf_counter

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import get_settings
from src.db.engine import SessionLocal
from src.guardrails.rules import apply_guardrails
from src.llm.client import LLMClient
from src.models.schemas import ConfidenceSignals, ExecutionMeta, HistoryItem, QueryResponse
from src.services.prompt_builder import build_prompt
from src.validation.alignment import score_alignment
from src.validation.multi_query import score_multi_query_agreement
from src.validation.sanity import score_result_sanity


class QueryService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.llm = LLMClient()
        self.history: list[HistoryItem] = []

    def _execute_read_only(self, sql: str) -> tuple[list[dict], list[str], int]:
        start = perf_counter()
        explain_plan: list[str] = []
        rows: list[dict] = []

        try:
            with SessionLocal() as session:
                plan_result = session.execute(text(f'EXPLAIN {sql}'))
                explain_plan = [str(r[0]) for r in plan_result.fetchall()]

                result = session.execute(text(sql))
                keys = list(result.keys())
                for row in result.fetchall()[: self.settings.max_result_rows]:
                    rows.append(dict(zip(keys, row)))
                session.rollback()
        except SQLAlchemyError as exc:
            rows = [{'error': str(exc)}]

        elapsed_ms = int((perf_counter() - start) * 1000)
        return rows, explain_plan, elapsed_ms

    def process_question(self, question: str) -> QueryResponse:
        prompt = build_prompt(question)
        generated = self.llm.generate_structured_sql(question=question, prompt_context=prompt)

        guardrail = apply_guardrails(
            sql=generated.sql,
            max_rows=self.settings.max_result_rows,
            max_subquery_depth=self.settings.max_subquery_depth,
            explain_estimated_rows=None,
            explain_row_limit=self.settings.max_explain_rows,
        )

        warnings = list(guardrail.reasons)
        rows: list[dict] = []
        explain: list[str] = []
        elapsed_ms = 0

        if guardrail.allowed:
            rows, explain, elapsed_ms = self._execute_read_only(guardrail.sql)
        else:
            warnings.append('Query execution skipped due to guardrails.')

        alignment = score_alignment(question=question, sql=guardrail.sql)
        sanity = score_result_sanity(rows)
        agreement = score_multi_query_agreement(question=question, primary_rows=rows)

        signals = ConfidenceSignals(
            syntax_validity=1.0,
            alignment_score=alignment,
            sanity_score=sanity,
            multi_query_agreement=agreement,
            schema_coverage=0.5,
        )
        confidence = round(
            (
                signals.syntax_validity
                + signals.alignment_score
                + signals.sanity_score
                + signals.multi_query_agreement
                + signals.schema_coverage
            )
            / 5,
            3,
        )

        response = QueryResponse(
            sql=guardrail.sql,
            explanation=generated.explanation,
            results=rows,
            confidence=confidence,
            signals=signals,
            warnings=warnings,
            execution_meta=ExecutionMeta(
                execution_time_ms=elapsed_ms,
                rows_returned=len(rows),
                explain_plan=explain,
            ),
        )
        self.history.append(
            HistoryItem(question=question, sql=response.sql, confidence=response.confidence)
        )
        return response
