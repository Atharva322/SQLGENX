from __future__ import annotations

from src.adapters.base import AdapterCapability, AdapterInfo


class AdapterRegistryError(ValueError):
    pass


class AdapterRegistry:
    def __init__(self, adapters: list[AdapterInfo] | None = None) -> None:
        self._adapters: dict[str, AdapterInfo] = {}
        for adapter in adapters or []:
            self.register(adapter)

    def register(self, adapter: AdapterInfo) -> None:
        if adapter.key in self._adapters:
            raise AdapterRegistryError(f"duplicate adapter key: {adapter.key}")
        self._adapters[adapter.key] = adapter

    def get(self, key: str) -> AdapterInfo:
        try:
            return self._adapters[key]
        except KeyError as exc:
            raise AdapterRegistryError(f"unknown adapter key: {key}") from exc

    def list(self, *, include_experimental: bool = False, include_hidden: bool = False) -> list[AdapterInfo]:
        states = {"verified"}
        if include_experimental:
            states.add("experimental")
        if include_hidden:
            states.add("hidden")
        return sorted(
            [adapter for adapter in self._adapters.values() if adapter.release_state in states],
            key=lambda adapter: adapter.key,
        )


def default_adapter_registry() -> AdapterRegistry:
    return AdapterRegistry(
        [
            AdapterInfo(
                key="postgresql",
                display_name="PostgreSQL",
                release_state="verified",
                sqlglot_dialect="postgres",
                driver_name="psycopg2",
                default_port=5432,
                supported_server_versions=("13", "14", "15", "16"),
                capabilities=AdapterCapability(
                    read_only_execution=True,
                    schema_introspection=True,
                    explain=True,
                    row_limit=True,
                    supports_tls=True,
                    notes=("Legacy environment connections are preserved during Phase 0.",),
                ),
            ),
            AdapterInfo(
                key="mysql",
                display_name="MySQL / MariaDB",
                release_state="experimental",
                sqlglot_dialect="mysql",
                driver_name="mysql2",
                default_port=3306,
                supported_server_versions=("8.0", "10.6+ MariaDB"),
                capabilities=AdapterCapability(
                    read_only_execution=False,
                    schema_introspection=False,
                    explain=False,
                    row_limit=False,
                    supports_tls=True,
                    notes=("Hidden from normal API catalog until Phase 3 verification.",),
                ),
            ),
        ]
    )
