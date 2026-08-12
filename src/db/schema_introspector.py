import hashlib
import json

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from src.cache.ttl import TTLCache
from src.config.settings import get_settings
from src.db.engine import get_engine


_SCHEMA_CACHE = TTLCache[str, dict](max_size=32, ttl_seconds=300)


def compute_schema_fingerprint(schema_summary: dict) -> str:
    canonical_tables: list[dict] = []
    for table in schema_summary.get("tables", []):
        table_name = str(table.get("table", ""))
        columns = table.get("columns", [])
        canonical_columns = [
            {
                "name": str(col.get("name", "")),
                "type": str(col.get("type", "")),
                "nullable": bool(col.get("nullable", True)),
            }
            for col in columns
        ]
        canonical_columns.sort(key=lambda col: col["name"])
        canonical_tables.append({"table": table_name, "columns": canonical_columns})

    canonical_tables.sort(key=lambda item: item["table"])
    payload = json.dumps({"tables": canonical_tables}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def clear_schema_summary_cache() -> None:
    _SCHEMA_CACHE.clear()


def _cache_key(connection_id: str | None = None, owner_id: str | None = None) -> str:
    return f"{owner_id or 'legacy'}:{connection_id or 'default'}"


def refresh_schema_summary(connection_id: str | None = None, owner_id: str | None = None) -> dict:
    try:
        summary = _introspect_schema(connection_id=connection_id, owner_id=owner_id)
    except TypeError:
        summary = _introspect_schema(connection_id=connection_id)
    _SCHEMA_CACHE.set(_cache_key(connection_id, owner_id), summary)
    return summary


def _introspect_schema(connection_id: str | None = None, owner_id: str | None = None) -> dict:
    try:
        inspector = inspect(get_engine(connection_id, owner_id=owner_id))
        tables = []
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            fks = inspector.get_foreign_keys(table_name)
            tables.append(
                {
                    "table": table_name,
                    "columns": [
                        {
                            "name": col["name"],
                            "type": str(col["type"]),
                            "nullable": col.get("nullable", True),
                        }
                        for col in columns
                    ],
                    "foreign_keys": fks,
                }
            )
        summary = {"tables": tables}
        summary["schema_fingerprint"] = compute_schema_fingerprint(summary)
        return summary
    except SQLAlchemyError as exc:
        return {"tables": [], "error": f"Schema introspection failed: {exc}"}


def get_schema_summary(connection_id: str | None = None, owner_id: str | None = None) -> dict:
    """Introspect DB schema for prompt construction, with bounded TTL caching."""
    settings = get_settings()
    key = _cache_key(connection_id, owner_id)
    if getattr(settings, "phase1_cache_enabled", True):
        global _SCHEMA_CACHE
        max_entries = int(getattr(settings, "schema_cache_max_entries", 32))
        ttl_seconds = int(getattr(settings, "schema_cache_ttl_seconds", 300))
        if (
            _SCHEMA_CACHE.max_size != max_entries
            or _SCHEMA_CACHE.ttl_seconds != ttl_seconds
        ):
            _SCHEMA_CACHE = TTLCache(
                max_size=max_entries,
                ttl_seconds=ttl_seconds,
            )
        cached = _SCHEMA_CACHE.get(key)
        if cached is not None:
            return cached
    try:
        summary = _introspect_schema(connection_id=connection_id, owner_id=owner_id)
    except TypeError:
        summary = _introspect_schema(connection_id=connection_id)
    if getattr(settings, "phase1_cache_enabled", True):
        _SCHEMA_CACHE.set(key, summary)
    return summary
