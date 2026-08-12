from __future__ import annotations

from src.config.settings import get_settings
from src.connections.models import (
    ConnectionAccessError,
    ConnectionNotFoundError,
    PublicConnection,
    StoredConnection,
    public_connection_from_stored,
    public_connection_from_url,
)


class LegacyEnvConnectionRepository:
    """Temporary Phase 0 repository over DATABASE_URL/CONNECTION_URLS_JSON."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def urls(self) -> dict[str, str]:
        urls = {"default": self.settings.database_url}
        urls.update(self.settings.connection_urls())
        return urls

    def list_public(self) -> list[PublicConnection]:
        return [
            public_connection_from_url(connection_id, url)
            for connection_id, url in sorted(self.urls().items())
        ]

    def public_by_id(self, connection_id: str) -> PublicConnection | None:
        url = self.urls().get(connection_id)
        return public_connection_from_url(connection_id, url) if url else None


class InMemoryConnectionRepository:
    """Development runtime repository for Phase 1 connection CRUD."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], StoredConnection] = {}

    def upsert(self, record: StoredConnection) -> StoredConnection:
        self._records[(record.owner_id, record.id)] = record
        return record

    def get(self, owner_id: str, connection_id: str) -> StoredConnection:
        try:
            return self._records[(owner_id, connection_id)]
        except KeyError as exc:
            raise ConnectionNotFoundError(f"Connection '{connection_id}' was not found.") from exc

    def list(self, owner_id: str) -> list[StoredConnection]:
        return sorted(
            [record for (owner, _), record in self._records.items() if owner == owner_id],
            key=lambda record: record.id,
        )

    def delete(self, owner_id: str, connection_id: str) -> StoredConnection:
        try:
            return self._records.pop((owner_id, connection_id))
        except KeyError as exc:
            raise ConnectionNotFoundError(f"Connection '{connection_id}' was not found.") from exc

    def public_list(self, owner_id: str) -> list[PublicConnection]:
        return [public_connection_from_stored(record) for record in self.list(owner_id)]

    def ensure_owned(self, owner_id: str, connection_id: str) -> StoredConnection:
        record = self.get(owner_id, connection_id)
        if record.owner_id != owner_id:
            raise ConnectionAccessError(f"Connection '{connection_id}' is not owned by this user.")
        return record


_RUNTIME_REPOSITORY = InMemoryConnectionRepository()


def runtime_connection_repository() -> InMemoryConnectionRepository:
    return _RUNTIME_REPOSITORY
