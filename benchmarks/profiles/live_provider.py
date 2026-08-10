from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any


def run(repetitions: int, limit: int | None, concurrency: int = 1) -> dict[str, Any]:
    provider = os.environ.get("LLM_PROVIDER", "")
    has_credentials = bool(os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY"))
    if not provider or not has_credentials:
        raise SystemExit("live-provider profile requires LLM_PROVIDER and provider credentials")
    return {
        "profile": "live-provider",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "skipped": True,
        "skip_reason": "Live-provider runner is manual-only in this branch; use protected secrets and cost controls.",
        "repetitions": repetitions,
        "limit": limit,
        "concurrency": concurrency,
    }
