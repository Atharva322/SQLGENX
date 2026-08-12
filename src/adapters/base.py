from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol

AdapterReleaseState = Literal["hidden", "experimental", "verified"]


@dataclass(frozen=True)
class AdapterCapability:
    read_only_execution: bool
    schema_introspection: bool
    explain: bool
    row_limit: bool
    supports_tls: bool
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class AdapterInfo:
    key: str
    display_name: str
    release_state: AdapterReleaseState
    sqlglot_dialect: str
    driver_name: str
    default_port: int | None
    supported_server_versions: tuple[str, ...] = ()
    capabilities: AdapterCapability = field(
        default_factory=lambda: AdapterCapability(
            read_only_execution=False,
            schema_introspection=False,
            explain=False,
            row_limit=False,
            supports_tls=False,
        )
    )


class DatabaseAdapter(Protocol):
    info: AdapterInfo
