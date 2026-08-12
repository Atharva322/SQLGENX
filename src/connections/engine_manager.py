from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EngineKey:
    owner_id: str
    connection_id: str
    connection_version: int


class EngineManager:
    """Phase 0 lifecycle contract; existing env engine cache remains in src.db.engine."""

    def dispose(self, key: EngineKey) -> None:
        return None
