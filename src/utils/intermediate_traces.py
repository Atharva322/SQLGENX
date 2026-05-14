from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from src.config.settings import get_settings


def log_intermediate_trace(record: dict[str, Any]) -> None:
    settings = get_settings()
    if not settings.intermediate_trace_logging_enabled:
        return
    out = Path("logs") / "intermediate_agent_traces.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    enriched = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        **record,
    }
    with out.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(enriched) + "\n")
