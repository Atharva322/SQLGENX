from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import SQLAlchemyError

from src.connections.models import ConnectionErrorCode, SnowflakeConnectionConfig
from src.db.engine import _safe_connection_error_code
from src.db.schema_introspector import compute_schema_fingerprint


class SnowflakeAdapter:
    key = "snowflake"
    dialect = "snowflake"
    sqlglot_dialect = "snowflake"

    def validate_config(self, config: SnowflakeConnectionConfig) -> None:
        if "." in config.account_identifier:
            first_label = config.account_identifier.split(".", 1)[0]
            if not first_label:
                raise ValueError("Snowflake account identifier is invalid.")
        if config.query_timeout_seconds > 600:
            raise ValueError("Snowflake query timeout must be 600 seconds or less.")

    def build_url(self, config: SnowflakeConnectionConfig) -> str:
        query = {
            "warehouse": config.warehouse,
            "authenticator": config.authenticator,
            "application": "SQLGENX",
        }
        if config.role:
            query["role"] = config.role
        return URL.create(
            "snowflake",
            username=config.username,
            password=config.password,
            host=config.account_identifier,
            database=f"{config.database}/{config.schema_name}",
            query=query,
        ).render_as_string(hide_password=False)

    def engine_options(
        self, connect_timeout_seconds: int, pool_timeout_seconds: int, pool_recycle_seconds: int
    ) -> dict:
        return {
            "future": True,
            "pool_pre_ping": True,
            "pool_timeout": pool_timeout_seconds,
            "pool_recycle": pool_recycle_seconds,
            "connect_args": {
                "login_timeout": connect_timeout_seconds,
                "network_timeout": connect_timeout_seconds,
            },
        }

    def test_connection(self, engine: Engine) -> tuple[bool, ConnectionErrorCode | None]:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                version = conn.execute(text("SELECT CURRENT_VERSION()")).scalar_one_or_none()
                if not version:
                    return False, "unsupported_version"
            return True, None
        except SQLAlchemyError as exc:
            return False, _safe_connection_error_code(str(exc))  # type: ignore[return-value]

    def inspect_schema(self, engine: Engine) -> dict:
        inspector = inspect(engine)
        tables = []
        for schema_name in inspector.get_schema_names():
            if schema_name.upper() in {"INFORMATION_SCHEMA"}:
                continue
            for table_name in inspector.get_table_names(schema=schema_name):
                columns = inspector.get_columns(table_name, schema=schema_name)
                fks = inspector.get_foreign_keys(table_name, schema=schema_name)
                tables.append(
                    {
                        "schema": schema_name,
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
        connection.execute(text("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30"))

    def apply_query_timeout(self, connection, timeout_seconds: int) -> None:
        timeout = max(1, min(int(timeout_seconds), 600))
        connection.execute(text(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}"))

    def explain(self, connection, sql: str) -> list[str]:
        rows = connection.execute(text(f"EXPLAIN USING TEXT {sql}")).fetchall()
        return [str(row[0]) for row in rows]


snowflake_adapter = SnowflakeAdapter()
