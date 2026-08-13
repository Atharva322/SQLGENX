from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import get_settings
from src.connections.models import ConnectionErrorCode, SQLiteConnectionConfig
from src.db.engine import _safe_connection_error_code
from src.db.schema_introspector import compute_schema_fingerprint


class SQLiteAdapter:
    key = "sqlite"
    dialect = "sqlite"
    sqlglot_dialect = "sqlite"
    allowed_suffixes = {".db", ".sqlite", ".sqlite3"}

    def validate_config(self, config: SQLiteConnectionConfig) -> None:
        self._resolve_database_path(config)

    def build_url(self, config: SQLiteConnectionConfig) -> str:
        path = self._resolve_database_path(config)
        return URL.create("sqlite", database=str(path)).render_as_string(hide_password=False)

    def engine_options(self, connect_timeout_seconds: int, pool_timeout_seconds: int, pool_recycle_seconds: int) -> dict:
        return {
            "future": True,
            "connect_args": {
                "check_same_thread": False,
                "timeout": float(connect_timeout_seconds),
            },
        }

    def test_connection(self, engine: Engine) -> tuple[bool, ConnectionErrorCode | None]:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, None
        except SQLAlchemyError as exc:
            return False, _safe_connection_error_code(str(exc))  # type: ignore[return-value]

    def inspect_schema(self, engine: Engine) -> dict:
        inspector = inspect(engine)
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

    def configure_read_only(self, connection) -> None:
        connection.execute(text("PRAGMA query_only = ON"))

    def explain(self, connection, sql: str) -> list[str]:
        rows = connection.execute(text(f"EXPLAIN QUERY PLAN {sql}")).fetchall()
        return [" | ".join(str(value) for value in row) for row in rows]

    def _resolve_database_path(self, config: SQLiteConnectionConfig) -> Path:
        raw = str(config.database).strip()
        if raw in {":memory:", ""}:
            raise ValueError("SQLite database must be a file path inside SQLITE_ALLOWED_DIRECTORY.")
        path = Path(raw).expanduser()
        settings = get_settings()
        allowed_root = Path(settings.sqlite_allowed_directory).expanduser().resolve(strict=False)
        candidate = path if path.is_absolute() else allowed_root / path
        resolved = candidate.resolve(strict=False)
        try:
            resolved.relative_to(allowed_root)
        except ValueError as exc:
            raise ValueError("SQLite database path is outside SQLITE_ALLOWED_DIRECTORY.") from exc
        if resolved.suffix.lower() not in self.allowed_suffixes:
            raise ValueError("SQLite database path must end with .db, .sqlite, or .sqlite3.")
        if not resolved.parent.exists():
            raise ValueError("SQLite database parent directory does not exist.")
        return resolved


sqlite_adapter = SQLiteAdapter()
