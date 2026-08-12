from __future__ import annotations

from typing import Protocol


class SecretStore(Protocol):
    def put(self, secret_id: str, value: str) -> None:
        ...

    def get(self, secret_id: str) -> str:
        ...

    def delete(self, secret_id: str) -> None:
        ...


class EphemeralSecretStore:
    """Development-only in-memory secret store for later runtime-connection phases."""

    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def put(self, secret_id: str, value: str) -> None:
        self._values[secret_id] = value

    def get(self, secret_id: str) -> str:
        return self._values[secret_id]

    def delete(self, secret_id: str) -> None:
        self._values.pop(secret_id, None)


_EPHEMERAL_SECRET_STORE = EphemeralSecretStore()


def get_secret_store() -> EphemeralSecretStore:
    return _EPHEMERAL_SECRET_STORE
