from __future__ import annotations

from typing import Protocol


class LLMCallObserver(Protocol):
    def on_attempt(self, operation: str, provider: str, model: str) -> None: ...

    def on_success(self, operation: str, usage: dict, latency_ms: int) -> None: ...

    def on_failure(self, operation: str, error_type: str, latency_ms: int) -> None: ...
