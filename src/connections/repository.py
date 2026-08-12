from __future__ import annotations

from src.config.settings import get_settings
from src.connections.models import PublicConnection, public_connection_from_url


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
