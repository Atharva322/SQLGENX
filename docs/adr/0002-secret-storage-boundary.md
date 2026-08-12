# ADR 0002: Secret Storage Boundary

Status: Accepted

Connection metadata and credentials are separate concerns. Public API responses may return redacted metadata such as ID, host, port, database, username, TLS mode, verification state, and schema fingerprint.

Passwords, secret references, full URLs, and driver query parameters containing secrets must not appear in API responses, traces, logs, or history snapshots. Phase 0 keeps legacy environment connections and introduces only the secret-store interface; runtime credential persistence starts in a later phase.
