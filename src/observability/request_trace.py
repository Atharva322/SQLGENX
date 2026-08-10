from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Iterator


SECRET_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "database_url",
    "db_url",
    "password",
    "token",
}


def _is_secret_key(key: str) -> bool:
    lowered = key.lower()
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


@dataclass
class RequestTrace:
    request_id: str
    stage_durations_ms: dict[str, int] = field(default_factory=dict)
    llm_call_count: int = 0
    db_round_trip_count: int = 0
    cache_state: dict[str, Any] = field(default_factory=dict)
    validation_level: str = "standard"
    failure_stage: str | None = None
    timeout_stage: str | None = None
    _started_at: float = field(default_factory=perf_counter)

    @contextmanager
    def timer(self, stage: str) -> Iterator[None]:
        started_at = perf_counter()
        try:
            yield
        except Exception:
            self.mark_failure(stage)
            raise
        finally:
            elapsed_ms = int((perf_counter() - started_at) * 1000)
            self.stage_durations_ms[stage] = self.stage_durations_ms.get(stage, 0) + elapsed_ms

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
        return int((perf_counter() - self._started_at) * 1000)

    def covered_duration_ms(self) -> int:
        return sum(self.stage_durations_ms.values())

    def coverage_ratio(self) -> float:
        total = self.total_duration_ms()
        if total <= 0:
            return 1.0
        return round(min(1.0, self.covered_duration_ms() / total), 3)
