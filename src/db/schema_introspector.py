from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import engine


def get_schema_summary() -> dict:
    """Introspect DB schema for prompt construction."""
    try:
        inspector = inspect(engine)
        tables = []
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            fks = inspector.get_foreign_keys(table_name)
            tables.append(
                {
                    'table': table_name,
                    'columns': [
                        {
                            'name': col['name'],
                            'type': str(col['type']),
                            'nullable': col.get('nullable', True),
                        }
                        for col in columns
                    ],
                    'foreign_keys': fks,
                }
            )
        return {'tables': tables}
    except SQLAlchemyError as exc:
        return {'tables': [], 'error': f'Schema introspection failed: {exc}'}
