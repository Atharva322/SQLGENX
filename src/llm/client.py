from dataclasses import dataclass
import json
import re

from anthropic import Anthropic
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

from src.config.settings import get_settings


@dataclass
class GeneratedSQL:
    sql: str
    explanation: str
    accessed_tables: list[str]
    accessed_columns: list[str]
    model_confidence: float
    token_usage: dict[str, int | str]


class StructuredSQLResponse(BaseModel):
    sql: str = Field(min_length=1)
    explanation: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    tables_accessed: list[str] = Field(default_factory=list)
    columns_accessed: list[str] = Field(default_factory=list)


class LLMClient:
    """Provider abstraction for OpenAI/Anthropic text-to-SQL generation."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def _system_prompt(self) -> str:
        return (
            "You are a Text-to-SQL assistant. Return strictly read-only SQL. "
            "Never return DDL or DML statements."
        )

    def _user_prompt(self, question: str, prompt_context: str) -> str:
        return (
            f"Question: {question}\n\n"
            f"{prompt_context}\n\n"
            "If the schema cannot answer the question, set sql to UNANSWERABLE.\n"
            "If the question is ambiguous, set sql to UNANSWERABLE and explain ambiguity.\n"
            "Return valid JSON with keys: "
            "sql, explanation, confidence, tables_accessed, columns_accessed."
        )

    def _json_chat_openai(self, system: str, user: str) -> dict:
        client = OpenAI(api_key=self.settings.openai_api_key)
        response = client.chat.completions.create(
            model=self.settings.llm_model or "gpt-4o-mini",
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        raw = response.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        usage = getattr(response, "usage", None)
        parsed["_meta"] = {
            "provider": "openai",
            "model": self.settings.llm_model or "gpt-4o-mini",
            "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
            "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
            "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
        }
        return parsed

    def _json_chat_anthropic(self, system: str, user: str) -> dict:
        client = Anthropic(api_key=self.settings.anthropic_api_key)
        message = client.messages.create(
            model=self.settings.llm_model or "claude-3-5-sonnet-latest",
            max_tokens=1000,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        parts = [part.text for part in message.content if getattr(part, "type", "") == "text"]
        raw = "".join(parts).strip() or "{}"
        parsed = json.loads(raw)
        usage = getattr(message, "usage", None)
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        parsed["_meta"] = {
            "provider": "anthropic",
            "model": self.settings.llm_model or "claude-3-5-sonnet-latest",
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        }
        return parsed

    def _text_chat_openai(self, system: str, user: str) -> str:
        client = OpenAI(api_key=self.settings.openai_api_key)
        response = client.chat.completions.create(
            model=self.settings.llm_model or "gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        return (response.choices[0].message.content or "").strip()

    def _text_chat_anthropic(self, system: str, user: str) -> str:
        client = Anthropic(api_key=self.settings.anthropic_api_key)
        message = client.messages.create(
            model=self.settings.llm_model or "claude-3-5-sonnet-latest",
            max_tokens=600,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        parts = [part.text for part in message.content if getattr(part, "type", "") == "text"]
        return "".join(parts).strip()

    def _placeholder(self) -> StructuredSQLResponse:
        return StructuredSQLResponse(
            sql="SELECT 'Text-to-SQL scaffold ready' AS status",
            explanation=(
                "Placeholder SQL returned by scaffold. "
                "Configure provider keys to enable live generation."
            ),
            confidence=0.45,
            tables_accessed=[],
            columns_accessed=[],
        )

    def _provider(self) -> str:
        return (self.settings.llm_provider or "").strip().lower()

    def _is_openai_enabled(self) -> bool:
        return self._provider() == "openai" and bool(self.settings.openai_api_key)

    def _is_anthropic_enabled(self) -> bool:
        return self._provider() == "anthropic" and bool(self.settings.anthropic_api_key)

    def generate_structured_sql(self, question: str, prompt_context: str) -> GeneratedSQL:
        response: StructuredSQLResponse
        meta: dict[str, int | str] = {
            "provider": self._provider() or "none",
            "model": self.settings.llm_model or "",
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        try:
            if self._is_openai_enabled():
                parsed = self._json_chat_openai(self._system_prompt(), self._user_prompt(question, prompt_context))
                meta = parsed.pop("_meta", meta)
                response = StructuredSQLResponse.model_validate(parsed)
            elif self._is_anthropic_enabled():
                parsed = self._json_chat_anthropic(self._system_prompt(), self._user_prompt(question, prompt_context))
                meta = parsed.pop("_meta", meta)
                response = StructuredSQLResponse.model_validate(parsed)
            else:
                response = self._placeholder()
        except (ValidationError, json.JSONDecodeError, Exception):
            response = self._placeholder()

        return GeneratedSQL(
            sql=response.sql.strip(),
            explanation=response.explanation,
            accessed_tables=response.tables_accessed,
            accessed_columns=response.columns_accessed,
            model_confidence=response.confidence,
            token_usage=meta,
        )

    def back_translate_sql(self, sql: str, prompt_context: str = "") -> str:
        system = "You explain SQL in plain English question form."
        user = (
            "Given this SQL, write the exact user question it answers in one sentence.\n\n"
            f"SQL:\n{sql}\n\n"
            f"Schema context:\n{prompt_context}\n"
        )
        try:
            if self._is_openai_enabled():
                text = self._text_chat_openai(system, user)
                if text:
                    return text
            if self._is_anthropic_enabled():
                text = self._text_chat_anthropic(system, user)
                if text:
                    return text
        except Exception:
            pass
        return self._heuristic_back_translation(sql)

    def generate_alternative_sql(
        self, question: str, prompt_context: str, primary_sql: str
    ) -> GeneratedSQL:
        variation_prompt = (
            f"{prompt_context}\n\n"
            f"Original user question: {question}\n"
            f"Primary SQL approach:\n{primary_sql}\n\n"
            "Generate an alternative SQL approach that answers the same question, "
            "ideally with different join/aggregation strategy when possible. "
            "Return JSON with keys: sql, explanation, confidence, tables_accessed, columns_accessed."
        )
        try:
            if self._is_openai_enabled():
                parsed = self._json_chat_openai(self._system_prompt(), variation_prompt)
                meta = parsed.pop(
                    "_meta",
                    {
                        "provider": "openai",
                        "model": self.settings.llm_model or "gpt-4o-mini",
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    },
                )
                response = StructuredSQLResponse.model_validate(parsed)
                return GeneratedSQL(
                    sql=response.sql.strip(),
                    explanation=response.explanation,
                    accessed_tables=response.tables_accessed,
                    accessed_columns=response.columns_accessed,
                    model_confidence=response.confidence,
                    token_usage=meta,
                )
            if self._is_anthropic_enabled():
                parsed = self._json_chat_anthropic(self._system_prompt(), variation_prompt)
                meta = parsed.pop(
                    "_meta",
                    {
                        "provider": "anthropic",
                        "model": self.settings.llm_model or "claude-3-5-sonnet-latest",
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    },
                )
                response = StructuredSQLResponse.model_validate(parsed)
                return GeneratedSQL(
                    sql=response.sql.strip(),
                    explanation=response.explanation,
                    accessed_tables=response.tables_accessed,
                    accessed_columns=response.columns_accessed,
                    model_confidence=response.confidence,
                    token_usage=meta,
                )
        except (ValidationError, json.JSONDecodeError, Exception):
            pass
        return GeneratedSQL(
            sql=primary_sql,
            explanation="Fallback alternative SQL reuses primary query.",
            accessed_tables=[],
            accessed_columns=[],
            model_confidence=0.4,
            token_usage={
                "provider": self._provider() or "none",
                "model": self.settings.llm_model or "",
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        )

    def _heuristic_back_translation(self, sql: str) -> str:
        normalized = " ".join(sql.strip().split())
        table_match = re.search(r"\bFROM\s+([a-zA-Z_][\w\.]*)", normalized, flags=re.IGNORECASE)
        group_match = re.search(r"\bGROUP\s+BY\b", normalized, flags=re.IGNORECASE)
        where_match = re.search(r"\bWHERE\b", normalized, flags=re.IGNORECASE)
        table = table_match.group(1) if table_match else "the dataset"
        if group_match and where_match:
            return f"What aggregated metrics from {table} satisfy the query filters?"
        if group_match:
            return f"What aggregated metrics are grouped from {table}?"
        if where_match:
            return f"What rows from {table} satisfy the filters?"
        return f"What records are selected from {table}?"
