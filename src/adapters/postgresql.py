from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import SQLAlchemyError

from src.connections.models import ConnectionErrorCode, PostgresConnectionConfig
from src.db.engine import _safe_connection_error_code
from src.db.schema_introspector import compute_schema_fingerprint


class PostgreSQLAdapter:
    key = "postgresql"
    sqlglot_dialect = "postgres"

    def validate_config(self, config: PostgresConnectionConfig) -> None:
        # Pydantic validates the shape; this hook exists for adapter-owned rules.
        return None

    def build_url(self, config: PostgresConnectionConfig) -> str:
        query = {}
        if config.tls_mode:
            query["sslmode"] = config.tls_mode
        return URL.create(
            "postgresql",
            username=config.username,
            password=config.password,
            host=config.host,
            port=config.port,
            database=config.database,
            query=query,
        ).render_as_string(hide_password=False)

    def engine_options(self, connect_timeout_seconds: int, pool_timeout_seconds: int, pool_recycle_seconds: int) -> dict:
        return {
            "future": True,
            "pool_pre_ping": True,
            "pool_timeout": pool_timeout_seconds,
            "pool_recycle": pool_recycle_seconds,
            "connect_args": {"connect_timeout": connect_timeout_seconds},
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
        connection.execute(text("SET TRANSACTION READ ONLY"))

    def explain(self, connection, sql: str) -> list[str]:
        rows = connection.execute(text(f"EXPLAIN {sql}")).fetchall()
        return [str(row[0]) for row in rows]


postgresql_adapter = PostgreSQLAdapter()
