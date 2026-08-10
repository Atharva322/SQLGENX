from __future__ import annotations

from contextvars import ContextVar
from time import perf_counter
import hashlib
import re
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine

from src.observability.request_trace import RequestTrace


_current_trace: ContextVar[RequestTrace | None] = ContextVar(
    "sqlgenx_current_trace", default=None
)
_listeners_attached: set[int] = set()


def set_current_trace(trace: RequestTrace | None):
    return _current_trace.set(trace)


def reset_current_trace(token: Any) -> None:
    _current_trace.reset(token)


def current_trace() -> RequestTrace | None:
    return _current_trace.get()


def safe_sql_fingerprint(statement: str) -> str:
    normalized = " ".join(statement.split()).lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def classify_statement(statement: str) -> str:
    normalized = " ".join(statement.strip().split()).lower()
    if normalized.startswith("explain"):
        return "explain"
    if normalized.startswith("set transaction") or normalized.startswith("rollback"):
        return "read_only_config"
    if "information_schema" in normalized or "pg_catalog" in normalized:
        return "schema"
    if re.match(r"^(select|with)\b", normalized):
        return "query"
    return "other"


def attach_engine_observer(engine: Engine) -> None:
    engine_id = id(engine)
    if engine_id in _listeners_attached:
        return

    @event.listens_for(engine, "before_cursor_execute")
    def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        context._sqlgenx_started_at = perf_counter()
        context._sqlgenx_statement_kind = classify_statement(statement)
        context._sqlgenx_statement_fingerprint = safe_sql_fingerprint(statement)

    @event.listens_for(engine, "after_cursor_execute")
    def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        trace = current_trace()
        if trace is None:
            return
        started_at = getattr(context, "_sqlgenx_started_at", perf_counter())
        kind = getattr(context, "_sqlgenx_statement_kind", classify_statement(statement))
        latency_ms = int((perf_counter() - started_at) * 1000)
        trace.record_db_statement(kind, latency_ms=latency_ms, failed=False)

    @event.listens_for(engine, "handle_error")
    def _handle_error(exception_context):
        trace = current_trace()
        if trace is None:
            return
        context = getattr(exception_context, "execution_context", None)
        started_at = getattr(context, "_sqlgenx_started_at", perf_counter())
        statement = exception_context.statement or ""
        kind = getattr(context, "_sqlgenx_statement_kind", classify_statement(statement))
        latency_ms = int((perf_counter() - started_at) * 1000)
        trace.record_db_statement(kind, latency_ms=latency_ms, failed=True)

    _listeners_attached.add(engine_id)
