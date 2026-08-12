# ADR 0004: Connection Ownership And Exact IDs

Status: Accepted

Connection IDs are exact, owner-scoped identifiers. Unknown or unauthorized IDs fail closed with a not-found error and must never resolve to `default` or another available connection.

Phase 0 preserves legacy environment-defined connections without introducing runtime users. Later phases will add owner/workspace fields to connection metadata and immutable query records.
