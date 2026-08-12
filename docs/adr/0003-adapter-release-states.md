# ADR 0003: Adapter Release States

Status: Accepted

Every database adapter has a reviewed release state:

- `hidden`: incomplete or unavailable to users.
- `experimental`: development/admin opt-in only.
- `verified`: visible in the normal adapter catalog.

Normal catalog responses expose verified adapters only. Promotion to verified requires adapter-specific integration, security, dialect, and text-to-SQL evaluation evidence.
