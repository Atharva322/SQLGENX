from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NetworkPolicyDecision:
    allowed: bool
    safe_error_code: str | None = None


class NetworkPolicy:
    """Placeholder Phase 0 network policy contract."""

    def validate_destination(self, host: str, port: int) -> NetworkPolicyDecision:
        if not host or port <= 0:
            return NetworkPolicyDecision(False, "invalid_config")
        return NetworkPolicyDecision(True)
