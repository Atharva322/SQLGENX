from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import SQLAlchemyError

from src.connections.models import ConnectionErrorCode, MySQLConnectionConfig
from src.db.engine import _safe_connection_error_code
from src.db.schema_introspector import compute_schema_fingerprint


class MySQLAdapter:
    key = "mysql"
    dialect = "mysql"
    sqlglot_dialect = "mysql"

    def validate_config(self, config: MySQLConnectionConfig) -> None:
        if config.tls_mode in {"verify-ca", "verify-full"}:
            raise ValueError("MySQL certificate verification modes require a CA bundle configuration.")

    def build_url(self, config: MySQLConnectionConfig) -> str:
        query = {"charset": getattr(config, "charset", "utf8mb4") or "utf8mb4"}
        return URL.create(
            "mysql+pymysql",
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
                version = str(conn.execute(text("SELECT VERSION()")).scalar_one_or_none() or "")
                if not version:
                    return False, "unsupported_version"
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
        connection.execute(text("SET SESSION TRANSACTION READ ONLY"))

    def explain(self, connection, sql: str) -> list[str]:
        rows = connection.execute(text(f"EXPLAIN {sql}")).mappings().all()
        return [" | ".join(f"{key}={value}" for key, value in row.items()) for row in rows]


mysql_adapter = MySQLAdapter()
