from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import get_engine


def get_schema_summary(connection_id: str | None = None) -> dict:
    """Introspect DB schema for prompt construction."""
    try:
        inspector = inspect(get_engine(connection_id))
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
        return {"tables": tables}
    except SQLAlchemyError as exc:
        return {"tables": [], "error": f"Schema introspection failed: {exc}"}
