import hashlib
import json

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import get_engine


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
        summary = {"tables": tables}
        summary["schema_fingerprint"] = compute_schema_fingerprint(summary)
        return summary
    except SQLAlchemyError as exc:
        return {"tables": [], "error": f"Schema introspection failed: {exc}"}
