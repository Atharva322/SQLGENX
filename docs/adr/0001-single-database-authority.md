# ADR 0001: FastAPI Owns Database Authority

Status: Accepted

FastAPI is the sole authority for database connectivity, schema introspection, SQL generation, validation, and execution. Browser-facing routes may proxy to FastAPI, but they must not create independent database pools or execute SQL through a separate path.

This keeps schema selection, query history, safety gates, trace metadata, and execution bound to one connection identity.
