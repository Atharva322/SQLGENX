from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from types import MappingProxyType
from typing import Any, Iterator, Mapping


SECRET_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "database_url",
    "db_url",
    "password",
    "token",
}

CANONICAL_STAGES = {
    "schema_introspection_ms",
    "feedback_load_ms",
    "schema_linking_ms",
    "prompt_format_ms",
    "query_plan_generation_ms",
    "primary_sql_generation_ms",
    "alternative_sql_generation_ms",
    "candidate_validation_ms",
    "intent_guardrail_ms",
    "sql_guardrail_ms",
    "explain_ms",
    "query_execution_ms",
    "alignment_validation_ms",
    "agreement_validation_ms",
    "sanity_validation_ms",
    "schema_coverage_ms",
    "response_build_ms",
    "history_audit_ms",
    "framework_overhead_ms",
}


def _is_secret_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in {"prompt_tokens", "completion_tokens", "total_tokens"}:
        return False
    return any(secret in lowered for secret in SECRET_KEYS)


def redact_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _is_secret_key(str(key)) else redact_secrets(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_secrets(item) for item in value)
    return value


@dataclass(frozen=True)
class TraceSnapshot:
    request_id: str
    stage_durations_ms: Mapping[str, int]
    total_duration_ms: int
    trace_coverage_ratio: float
    llm_call_count: int
    db_round_trip_count: int
    cache_state: Mapping[str, Any]
    validation_level: str
    failure_stage: str | None
    timeout_stage: str | None
    provider_attempt_count: int
    provider_success_count: int
    provider_failure_count: int
    provider_timeout_count: int
    provider_latency_ms: int
    llm_token_usage: Mapping[str, Any]
    llm_operations: tuple[Mapping[str, Any], ...]
    db_operation_count: int
    sql_statement_count: int
    schema_introspection_count: int
    explain_count: int
    query_execution_count: int
    alternative_execution_count: int
    db_error_count: int
    sql_statement_latency_ms: int


@dataclass
class RequestTrace:
    request_id: str
    stage_durations_ms: dict[str, int] = field(default_factory=dict)
    llm_call_count: int = 0
    db_round_trip_count: int = 0
    provider_attempt_count: int = 0
    provider_success_count: int = 0
    provider_failure_count: int = 0
    provider_timeout_count: int = 0
    provider_latency_ms: int = 0
    llm_token_usage: dict[str, Any] = field(default_factory=dict)
    llm_operations: list[dict[str, Any]] = field(default_factory=list)
    db_operation_count: int = 0
    sql_statement_count: int = 0
    schema_introspection_count: int = 0
    explain_count: int = 0
    query_execution_count: int = 0
    alternative_execution_count: int = 0
    db_error_count: int = 0
    sql_statement_latency_ms: int = 0
    cache_state: dict[str, Any] = field(default_factory=dict)
    validation_level: str = "standard"
    failure_stage: str | None = None
    timeout_stage: str | None = None
    _started_at: float = field(default_factory=perf_counter)
    finished_at: float | None = None
    _active_stage: str | None = None

    @contextmanager
    def timer(self, stage: str) -> Iterator[None]:
        if stage not in CANONICAL_STAGES:
            raise ValueError(f"Unknown trace stage: {stage}")
        if self._active_stage is not None:
            raise RuntimeError(
                f"Trace stage {stage} cannot start while {self._active_stage} is active"
            )
        self._active_stage = stage
        started_at = perf_counter()
        try:
            yield
        except Exception:
            self.mark_failure(stage)
            raise
        finally:
            elapsed_ms = int((perf_counter() - started_at) * 1000)
            self.stage_durations_ms[stage] = self.stage_durations_ms.get(stage, 0) + elapsed_ms
            self._active_stage = None

    def increment_llm_calls(self, count: int = 1) -> None:
        self.llm_call_count = max(0, self.llm_call_count + max(0, count))

    def increment_db_round_trips(self, count: int = 1) -> None:
        self.db_round_trip_count = max(0, self.db_round_trip_count + max(0, count))

    def set_cache_state(self, name: str, state: Any) -> None:
        self.cache_state[name] = redact_secrets(state)

    def mark_failure(self, stage: str) -> None:
        if self.failure_stage is None:
            self.failure_stage = stage

    def mark_timeout(self, stage: str) -> None:
        self.timeout_stage = stage
        self.mark_failure(stage)

    def total_duration_ms(self) -> int:
        ended_at = self.finished_at or perf_counter()
        return int((ended_at - self._started_at) * 1000)

    def covered_duration_ms(self) -> int:
        return sum(self.stage_durations_ms.values())

    def coverage_ratio(self) -> float:
        total = self.total_duration_ms()
        if total <= 0:
            return 1.0
        return round(min(1.0, self.covered_duration_ms() / total), 3)

    def on_attempt(self, operation: str, provider: str, model: str) -> None:
        self.provider_attempt_count += 1
        self.llm_call_count = self.provider_attempt_count
        self.llm_operations.append(
            redact_secrets(
                {
                    "operation": operation,
                    "provider": provider,
                    "model": model,
                    "status": "attempt",
                }
            )
        )

    def on_success(self, operation: str, usage: dict, latency_ms: int) -> None:
        self.provider_success_count += 1
        self.provider_latency_ms += max(0, latency_ms)
        self._merge_usage(usage)
        self.llm_operations.append(
            redact_secrets(
                {
                    "operation": operation,
                    "status": "success",
                    "latency_ms": max(0, latency_ms),
                    "usage": usage,
                }
            )
        )

    def on_failure(self, operation: str, error_type: str, latency_ms: int) -> None:
        self.provider_failure_count += 1
        if "timeout" in error_type.lower():
            self.provider_timeout_count += 1
            self.mark_timeout(operation)
        else:
            self.mark_failure(operation)
        self.provider_latency_ms += max(0, latency_ms)
        self.llm_operations.append(
            redact_secrets(
                {
                    "operation": operation,
                    "status": "failure",
                    "error_type": error_type,
                    "latency_ms": max(0, latency_ms),
                }
            )
        )

    def record_db_statement(
        self,
        statement_kind: str,
        latency_ms: int = 0,
        failed: bool = False,
    ) -> None:
        self.sql_statement_count += 1
        self.db_operation_count += 1
        self.db_round_trip_count = self.sql_statement_count
        self.sql_statement_latency_ms += max(0, latency_ms)
        if statement_kind == "schema":
            self.schema_introspection_count += 1
        elif statement_kind == "explain":
            self.explain_count += 1
        elif statement_kind == "query":
            self.query_execution_count += 1
        elif statement_kind == "alternative":
            self.alternative_execution_count += 1
        if failed:
            self.db_error_count += 1

    def finish(self) -> None:
        if self.finished_at is not None:
            return
        self.finished_at = perf_counter()
        measured = sum(
            duration
            for stage, duration in self.stage_durations_ms.items()
            if stage != "framework_overhead_ms"
        )
        self.stage_durations_ms["framework_overhead_ms"] = max(
            0, self.total_duration_ms() - measured
        )

    def snapshot(self) -> TraceSnapshot:
        self.finish()
        stage_copy = dict(self.stage_durations_ms)
        cache_copy = redact_secrets(dict(self.cache_state))
        usage_copy = redact_secrets(dict(self.llm_token_usage))
        operation_copy = tuple(
            MappingProxyType(redact_secrets(dict(operation)))
            for operation in self.llm_operations
        )
        return TraceSnapshot(
            request_id=self.request_id,
            stage_durations_ms=MappingProxyType(stage_copy),
            total_duration_ms=self.total_duration_ms(),
            trace_coverage_ratio=self.coverage_ratio(),
            llm_call_count=self.llm_call_count,
            db_round_trip_count=self.db_round_trip_count,
            cache_state=MappingProxyType(cache_copy),
            validation_level=self.validation_level,
            failure_stage=self.failure_stage,
            timeout_stage=self.timeout_stage,
            provider_attempt_count=self.provider_attempt_count,
            provider_success_count=self.provider_success_count,
            provider_failure_count=self.provider_failure_count,
            provider_timeout_count=self.provider_timeout_count,
            provider_latency_ms=self.provider_latency_ms,
            llm_token_usage=MappingProxyType(usage_copy),
            llm_operations=operation_copy,
            db_operation_count=self.db_operation_count,
            sql_statement_count=self.sql_statement_count,
            schema_introspection_count=self.schema_introspection_count,
            explain_count=self.explain_count,
            query_execution_count=self.query_execution_count,
            alternative_execution_count=self.alternative_execution_count,
            db_error_count=self.db_error_count,
            sql_statement_latency_ms=self.sql_statement_latency_ms,
        )

    def _merge_usage(self, usage: dict) -> None:
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            self.llm_token_usage[key] = int(self.llm_token_usage.get(key, 0) or 0) + int(
                usage.get(key, 0) or 0
            )
        for key in ("provider", "model"):
            if usage.get(key):
                self.llm_token_usage[key] = usage[key]
