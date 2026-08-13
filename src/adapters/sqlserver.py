from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import SQLAlchemyError

from src.connections.models import ConnectionErrorCode, SQLServerConnectionConfig
from src.db.engine import _safe_connection_error_code
from src.db.schema_introspector import compute_schema_fingerprint


class SQLServerAdapter:
    key = "sqlserver"
    dialect = "tsql"
    sqlglot_dialect = "tsql"

    def validate_config(self, config: SQLServerConnectionConfig) -> None:
        driver = getattr(config, "odbc_driver", "ODBC Driver 18 for SQL Server")
        if not driver.strip():
            raise ValueError("SQL Server ODBC driver name is required.")

    def build_url(self, config: SQLServerConnectionConfig) -> str:
        encrypt = "no" if getattr(config, "tls_mode", "require") == "disable" else "yes"
        trust_cert = "yes" if getattr(config, "trust_server_certificate", False) else "no"
        query = {
            "driver": getattr(config, "odbc_driver", "ODBC Driver 18 for SQL Server"),
            "Encrypt": encrypt,
            "TrustServerCertificate": trust_cert,
        }
        return URL.create(
            "mssql+pyodbc",
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
            "connect_args": {"timeout": connect_timeout_seconds},
        }

    def test_connection(self, engine: Engine) -> tuple[bool, ConnectionErrorCode | None]:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                version = conn.execute(text("SELECT SERVERPROPERTY('ProductVersion')")).scalar_one_or_none()
                if not version:
                    return False, "unsupported_version"
            return True, None
        except SQLAlchemyError as exc:
            return False, _safe_connection_error_code(str(exc))  # type: ignore[return-value]

    def inspect_schema(self, engine: Engine) -> dict:
        inspector = inspect(engine)
        tables = []
        for schema_name in inspector.get_schema_names():
            if schema_name.lower() in {"information_schema", "sys"}:
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
        # SQL Server has no portable session-level read-only mode; Phase 4 keeps
        # the adapter experimental until permission-scoped integration gates exist.
        connection.execute(text("SET LOCK_TIMEOUT 10000"))

    def explain(self, connection, sql: str) -> list[str]:
        rows = connection.execute(text(f"SET SHOWPLAN_TEXT ON; {sql}; SET SHOWPLAN_TEXT OFF")).fetchall()
        return [str(row[0]) for row in rows]


sqlserver_adapter = SQLServerAdapter()
