from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter

from src.config.settings import get_settings
from src.connections.models import ConnectionErrorCode, ConnectionPrepareResponse
from src.connections.service import DEMO_OWNER_ID, get_connection_service
from src.db.schema_introspector import get_schema_summary, refresh_schema_summary
from src.services.prompt_builder import select_relevant_feedback_examples
from src.services.schema_linker import run_schema_linking
from src.utils.audit import log_execution_event


@dataclass(frozen=True)
class PreparedContextState:
    owner_id: str
    connection_id: str
    connection_version: int
    schema_fingerprint: str | None
    table_count: int
    status: str
    prepared_at: str | None = None
    elapsed_ms: int | None = None
    safe_error_code: ConnectionErrorCode | None = None

    @property
    def ready(self) -> bool:
        return self.status == "ready"


class ConnectionPrepareService:
    def __init__(self) -> None:
        self._states: dict[tuple[str, str], PreparedContextState] = {}

    def get_status(self, owner_id: str, connection_id: str) -> ConnectionPrepareResponse:
        resolved = self._resolve(owner_id, connection_id)
        state = self._states.get((owner_id, resolved.id))
        if state is None:
            return ConnectionPrepareResponse(
                connection_id=resolved.id,
                owner_id=owner_id,
                ready=False,
                status="not_started",
            )
        if state.connection_version != resolved.version:
            return self._response(state, status="stale", ready=False)
        return self._response(state)

    def prepare(self, owner_id: str, connection_id: str, *, refresh: bool = False) -> ConnectionPrepareResponse:
        resolved = self._resolve(owner_id, connection_id)
        key = (owner_id, resolved.id)
        self._states[key] = PreparedContextState(
            owner_id=owner_id,
            connection_id=resolved.id,
            connection_version=resolved.version,
            schema_fingerprint=resolved.schema_fingerprint,
            table_count=0,
            status="preparing",
        )
        log_execution_event(
            "schema_prepare_started",
            {"connection_id": resolved.id, "owner_id": owner_id, "connection_version": resolved.version},
        )
        start = perf_counter()
        try:
            if refresh:
                schema = refresh_schema_summary(connection_id=resolved.id, owner_id=owner_id)
            else:
                schema = get_schema_summary(connection_id=resolved.id, owner_id=owner_id)
            if schema.get("error"):
                raise RuntimeError("schema introspection failed")
            fingerprint = str(schema.get("schema_fingerprint") or resolved.schema_fingerprint or "")
            table_count = len(schema.get("tables", []))
            self._prewarm_linking(owner_id, resolved.id, schema, fingerprint)
            state = PreparedContextState(
                owner_id=owner_id,
                connection_id=resolved.id,
                connection_version=resolved.version,
                schema_fingerprint=fingerprint or None,
                table_count=table_count,
                status="ready",
                prepared_at=datetime.now(timezone.utc).isoformat(),
                elapsed_ms=int((perf_counter() - start) * 1000),
            )
            self._states[key] = state
            log_execution_event(
                "schema_prepare_completed",
                {
                    "connection_id": resolved.id,
                    "owner_id": owner_id,
                    "connection_version": resolved.version,
                    "schema_fingerprint": state.schema_fingerprint,
                    "table_count": table_count,
                    "elapsed_ms": state.elapsed_ms,
                },
            )
            return self._response(state)
        except Exception:
            state = PreparedContextState(
                owner_id=owner_id,
                connection_id=resolved.id,
                connection_version=resolved.version,
                schema_fingerprint=resolved.schema_fingerprint,
                table_count=0,
                status="failed",
                prepared_at=datetime.now(timezone.utc).isoformat(),
                elapsed_ms=int((perf_counter() - start) * 1000),
                safe_error_code="introspection_failed",
            )
            self._states[key] = state
            log_execution_event(
                "schema_prepare_failed",
                {
                    "connection_id": resolved.id,
                    "owner_id": owner_id,
                    "connection_version": resolved.version,
                    "elapsed_ms": state.elapsed_ms,
                    "safe_error_code": state.safe_error_code,
                },
            )
            return self._response(state)

    def invalidate(self, owner_id: str, connection_id: str) -> None:
        resolved_id = connection_id or "default"
        self._states.pop((owner_id, resolved_id), None)
        log_execution_event("schema_prepare_invalidated", {"connection_id": resolved_id, "owner_id": owner_id})

    def is_ready(self, owner_id: str, connection_id: str) -> bool:
        return self.get_status(owner_id, connection_id).ready

    def _resolve(self, owner_id: str, connection_id: str):
        return get_connection_service().get_public(owner_id, connection_id or "default")

    def _prewarm_linking(self, owner_id: str, connection_id: str, schema: dict, schema_fingerprint: str) -> None:
        settings = get_settings()
        prompts = [
            prompt.strip()
            for prompt in str(getattr(settings, "schema_prepare_warmup_prompts", "")).split("|")
            if prompt.strip()
        ]
        for prompt in prompts:
            examples = select_relevant_feedback_examples(
                prompt,
                connection_id=connection_id,
                schema_fingerprint=schema_fingerprint,
                max_examples=int(getattr(settings, "rag_top_k_examples", 3)),
                min_confidence=float(getattr(settings, "rag_min_feedback_confidence", 0.65)),
            )
            run_schema_linking(
                question=prompt,
                schema=schema,
                feedback_examples=examples,
                top_k_schema=int(getattr(settings, "rag_top_k_schema", 5)),
                top_k_examples=int(getattr(settings, "rag_top_k_examples", 3)),
                connection_id=connection_id,
            )

    def _response(
        self,
        state: PreparedContextState,
        *,
        status: str | None = None,
        ready: bool | None = None,
    ) -> ConnectionPrepareResponse:
        resolved_status = status or state.status
        return ConnectionPrepareResponse(
            connection_id=state.connection_id,
            owner_id=state.owner_id,
            ready=state.ready if ready is None else ready,
            status=resolved_status,  # type: ignore[arg-type]
            schema_fingerprint=state.schema_fingerprint,
            table_count=state.table_count,
            prepared_at=state.prepared_at,
            elapsed_ms=state.elapsed_ms,
            safe_error_code=state.safe_error_code,
        )


_PREPARE_SERVICE = ConnectionPrepareService()


def get_connection_prepare_service() -> ConnectionPrepareService:
    return _PREPARE_SERVICE
