from pathlib import Path
from time import perf_counter
from uuid import uuid4
import json

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import get_settings
from src.db.engine import available_connections, get_session_factory
from src.db.schema_introspector import get_schema_summary
from src.guardrails.rules import (
    apply_guardrails,
    detect_malicious_prompt_intent,
    parse_explain_total_rows,
)
from src.llm.client import GeneratedSQL, LLMClient
from src.models.schemas import (
    AccessedSchema,
    ConfidenceSignals,
    ExecutionMeta,
    FeedbackPayload,
    FeedbackResponse,
    HistoryItem,
    QueryResponse,
)
from src.services.prompt_builder import build_prompt
from src.utils.audit import log_blocked_query, log_execution_event
from src.validation.alignment import verify_sql_alignment
from src.validation.multi_query import (
    evaluate_multi_query_agreement,
    should_run_multi_query_validation,
)
from src.validation.sanity import analyze_result_sanity


class QueryService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.llm = LLMClient()
        self.history: list[HistoryItem] = []

    def _new_query_id(self) -> str:
        return f"qry_{uuid4().hex[:12]}"

    def _normalize_session_id(self, session_id: str | None) -> str:
        if session_id and session_id.strip():
            return session_id.strip()
        return "default"

    def _normalize_connection_id(self, connection_id: str | None) -> str:
        connections = available_connections()
        if connection_id and connection_id in connections:
            return connection_id
        return "default"

    def get_connections(self) -> dict[str, str]:
        return available_connections()

    def _run_explain(self, sql: str, connection_id: str) -> list[str]:
        SessionLocal = get_session_factory(connection_id)
        with SessionLocal() as session:
            session.execute(text("SET TRANSACTION READ ONLY"))
            plan_result = session.execute(text(f"EXPLAIN {sql}"))
            plan_lines: list[str] = []
            for row in plan_result.fetchall():
                plan_lines.append(" | ".join(str(value) for value in row))
            session.rollback()
            return plan_lines

    def _execute_read_only(
        self,
        sql: str,
        max_rows: int,
        connection_id: str,
        precomputed_explain: list[str] | None = None,
    ) -> tuple[list[dict], list[str], int]:
        start = perf_counter()
        explain_plan: list[str] = precomputed_explain or []
        rows_df = pd.DataFrame()
        SessionLocal = get_session_factory(connection_id)

        try:
            with SessionLocal() as session:
                session.execute(text("SET TRANSACTION READ ONLY"))

                if not explain_plan:
                    plan_result = session.execute(text(f"EXPLAIN {sql}"))
                    for row in plan_result.fetchall():
                        explain_plan.append(" | ".join(str(value) for value in row))

                result = session.execute(text(sql))
                fetched_rows = result.fetchall()
                keys = list(result.keys())
                rows_df = pd.DataFrame(fetched_rows, columns=keys)
                if len(rows_df) > max_rows:
                    rows_df = rows_df.head(max_rows)
                session.rollback()
        except SQLAlchemyError as exc:
            rows_df = pd.DataFrame([{"error": str(exc)}])

        elapsed_ms = int((perf_counter() - start) * 1000)
        return rows_df.to_dict(orient="records"), explain_plan, elapsed_ms

    def _schema_coverage_score(
        self,
        question: str,
        accessed_tables: list[str],
        accessed_columns: list[str],
        connection_id: str,
    ) -> float:
        schema = get_schema_summary(connection_id=connection_id)
        question_tokens = {token.lower() for token in question.split() if len(token) > 2}
        expected: set[str] = set()
        for table in schema.get("tables", []):
            table_name = table.get("table", "").lower()
            if table_name and table_name in question_tokens:
                expected.add(table_name)
            for column in table.get("columns", []):
                col_name = column.get("name", "").lower()
                if col_name and col_name in question_tokens:
                    expected.add(f"{table_name}.{col_name}")

        if not expected:
            return 0.7 if accessed_tables or accessed_columns else 0.5

        used = {t.lower() for t in accessed_tables}.union({c.lower() for c in accessed_columns})
        overlap = len(expected.intersection(used))
        return round(max(0.0, min(1.0, overlap / max(1, len(expected)))), 3)

    def _build_response(
        self,
        query_id: str,
        connection_id: str,
        session_id: str,
        question: str,
        generated: GeneratedSQL,
        guarded_sql: str,
        syntax_valid: bool,
        warnings: list[str],
        rows: list[dict],
        explain: list[str],
        elapsed_ms: int,
    ) -> QueryResponse:
        prompt = build_prompt(question, connection_id=connection_id)
        back_translated_question = self.llm.back_translate_sql(
            sql=guarded_sql, prompt_context=prompt
        )
        alignment = verify_sql_alignment(
            original_question=question,
            back_translated_question=back_translated_question,
        )
        warnings.extend(alignment.warnings)
        log_execution_event(
            "alignment_check",
            {
                "connection_id": connection_id,
                "question": question,
                "back_translated_question": back_translated_question,
                "score": alignment.score,
            },
        )

        sanity = analyze_result_sanity(rows)
        warnings.extend(sanity.warnings)

        multi_query_score = 0.5
        if should_run_multi_query_validation(question=question, sql=guarded_sql):
            alt_generated = self.llm.generate_alternative_sql(
                question=question, prompt_context=prompt, primary_sql=guarded_sql
            )
            alt_guardrail = apply_guardrails(
                sql=alt_generated.sql,
                max_rows=self.settings.max_result_rows,
                max_subquery_depth=self.settings.max_subquery_depth,
                explain_estimated_rows=None,
                explain_row_limit=self.settings.max_explain_rows,
            )
            if alt_guardrail.allowed:
                alt_rows, _, _ = self._execute_read_only(
                    alt_guardrail.sql,
                    max_rows=self.settings.max_result_rows,
                    connection_id=connection_id,
                )
                multi_query = evaluate_multi_query_agreement(rows, alt_rows)
                multi_query_score = multi_query.score
                warnings.extend(multi_query.warnings)
                if multi_query.comparison_summary:
                    warnings.append(f"Multi-query validation: {multi_query.comparison_summary}")
            else:
                multi_query_score = 0.4
                warnings.append("Alternative validation query blocked by guardrails.")

        schema_coverage = self._schema_coverage_score(
            question=question,
            accessed_tables=generated.accessed_tables,
            accessed_columns=generated.accessed_columns,
            connection_id=connection_id,
        )

        signals = ConfidenceSignals(
            syntax_validity=1.0 if syntax_valid else 0.0,
            alignment_score=alignment.score,
            sanity_score=sanity.score,
            multi_query_agreement=multi_query_score,
            schema_coverage=schema_coverage,
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

        return QueryResponse(
            query_id=query_id,
            connection_id=connection_id,
            session_id=session_id,
            sql=guarded_sql,
            explanation=generated.explanation,
            accessed=AccessedSchema(
                tables=generated.accessed_tables,
                columns=generated.accessed_columns,
            ),
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

    def process_question(
        self,
        question: str,
        connection_id: str | None = None,
        session_id: str | None = None,
        row_limit_override: int | None = None,
        sql_override: str | None = None,
    ) -> QueryResponse:
        max_rows = row_limit_override or self.settings.max_result_rows
        resolved_session_id = self._normalize_session_id(session_id)
        resolved_connection_id = self._normalize_connection_id(connection_id)
        query_id = self._new_query_id()
        prompt = build_prompt(question, connection_id=resolved_connection_id)

        generated = (
            GeneratedSQL(
                sql=sql_override or "",
                explanation="User-edited SQL executed with guardrails.",
                accessed_tables=[],
                accessed_columns=[],
                model_confidence=0.6,
            )
            if sql_override
            else self.llm.generate_structured_sql(question=question, prompt_context=prompt)
        )

        intent_reasons = detect_malicious_prompt_intent(question)
        if intent_reasons:
            guarded_sql = generated.sql
            warnings = intent_reasons + [
                "Query blocked due to malicious/destructive user intent."
            ]
            response = self._build_response(
                query_id=query_id,
                connection_id=resolved_connection_id,
                session_id=resolved_session_id,
                question=question,
                generated=generated,
                guarded_sql=guarded_sql,
                syntax_valid=False,
                warnings=warnings,
                rows=[],
                explain=[],
                elapsed_ms=0,
            )
            self.history.append(
                HistoryItem(
                    query_id=response.query_id,
                    connection_id=response.connection_id,
                    session_id=response.session_id,
                    question=question,
                    sql=response.sql,
                    explanation=response.explanation,
                    confidence=response.confidence,
                    signals=response.signals,
                    warnings=response.warnings,
                    results=response.results,
                    execution_meta=response.execution_meta,
                    feedback=None,
                )
            )
            log_blocked_query(question=question, sql=generated.sql, reasons=warnings)
            return response

        if generated.sql.strip().upper() == "UNANSWERABLE":
            warnings = [
                "Model returned UNANSWERABLE for missing schema coverage or ambiguity.",
                "No SQL executed.",
            ]
            response = self._build_response(
                query_id=query_id,
                connection_id=resolved_connection_id,
                session_id=resolved_session_id,
                question=question,
                generated=generated,
                guarded_sql=generated.sql,
                syntax_valid=False,
                warnings=warnings,
                rows=[],
                explain=[],
                elapsed_ms=0,
            )
            self.history.append(
                HistoryItem(
                    query_id=response.query_id,
                    connection_id=response.connection_id,
                    session_id=response.session_id,
                    question=question,
                    sql=response.sql,
                    explanation=response.explanation,
                    confidence=response.confidence,
                    signals=response.signals,
                    warnings=response.warnings,
                    results=response.results,
                    execution_meta=response.execution_meta,
                    feedback=None,
                )
            )
            return response

        initial_guardrail = apply_guardrails(
            sql=generated.sql,
            max_rows=max_rows,
            max_subquery_depth=self.settings.max_subquery_depth,
            explain_estimated_rows=None,
            explain_row_limit=self.settings.max_explain_rows,
        )
        guarded_sql = initial_guardrail.sql
        warnings = list(initial_guardrail.reasons)
        rows: list[dict] = []
        explain: list[str] = []
        elapsed_ms = 0
        estimated_rows = 0
        syntax_valid = initial_guardrail.syntax_valid

        if initial_guardrail.allowed:
            try:
                explain = self._run_explain(guarded_sql, connection_id=resolved_connection_id)
                estimated_rows = parse_explain_total_rows(explain)
            except SQLAlchemyError as exc:
                warnings.append(f"EXPLAIN failed: {exc}")

            final_guardrail = apply_guardrails(
                sql=guarded_sql,
                max_rows=max_rows,
                max_subquery_depth=self.settings.max_subquery_depth,
                explain_estimated_rows=estimated_rows,
                explain_row_limit=self.settings.max_explain_rows,
            )
            warnings.extend(final_guardrail.reasons)

            if final_guardrail.allowed:
                rows, explain, elapsed_ms = self._execute_read_only(
                    final_guardrail.sql,
                    max_rows=max_rows,
                    connection_id=resolved_connection_id,
                    precomputed_explain=explain,
                )
                log_execution_event(
                    "query_executed",
                    {
                        "query_id": query_id,
                        "connection_id": resolved_connection_id,
                        "session_id": resolved_session_id,
                        "question": question,
                        "sql": final_guardrail.sql,
                        "rows_returned": len(rows),
                        "execution_time_ms": elapsed_ms,
                        "estimated_rows": estimated_rows,
                    },
                )
            else:
                log_blocked_query(
                    question=question, sql=final_guardrail.sql, reasons=final_guardrail.reasons
                )
                warnings.append("Query execution skipped due to guardrails.")
        else:
            log_blocked_query(question=question, sql=guarded_sql, reasons=initial_guardrail.reasons)
            warnings.append("Query execution skipped due to guardrails.")

        response = self._build_response(
            query_id=query_id,
            connection_id=resolved_connection_id,
            session_id=resolved_session_id,
            question=question,
            generated=generated,
            guarded_sql=guarded_sql,
            syntax_valid=syntax_valid,
            warnings=warnings,
            rows=rows,
            explain=explain,
            elapsed_ms=elapsed_ms,
        )

        self.history.append(
            HistoryItem(
                query_id=response.query_id,
                connection_id=response.connection_id,
                session_id=response.session_id,
                question=question,
                sql=response.sql,
                explanation=response.explanation,
                confidence=response.confidence,
                signals=response.signals,
                warnings=response.warnings,
                results=response.results,
                execution_meta=response.execution_meta,
                feedback=None,
            )
        )
        return response

    def get_history(self, session_id: str | None = None) -> list[HistoryItem]:
        if not session_id:
            return list(self.history)
        resolved = self._normalize_session_id(session_id)
        return [item for item in self.history if item.session_id == resolved]

    def _feedback_target_file(self, verdict: str) -> Path:
        if verdict == "correct":
            return Path("data") / "feedback_fewshots.jsonl"
        return Path("evals") / "feedback_incorrect_cases.jsonl"

    def store_feedback(
        self, query_id: str, verdict: str, notes: str | None = None, session_id: str | None = None
    ) -> FeedbackResponse:
        target = None
        for item in self.history:
            if item.query_id == query_id:
                if session_id and item.session_id != self._normalize_session_id(session_id):
                    continue
                target = item
                break

        if target is None:
            raise ValueError(f"Unknown query_id: {query_id}")

        target.feedback = FeedbackPayload(verdict=verdict, notes=notes)
        path = self._feedback_target_file(verdict)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "query_id": target.query_id,
            "connection_id": target.connection_id,
            "session_id": target.session_id,
            "question": target.question,
            "sql": target.sql,
            "verdict": verdict,
            "notes": notes,
            "confidence": target.confidence,
            "signals": target.signals.model_dump(),
            "warnings": target.warnings,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")

        log_execution_event("feedback_stored", payload)
        return FeedbackResponse(query_id=query_id, stored=True, target_file=str(path))
