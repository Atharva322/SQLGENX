from __future__ import annotations

import os


def configure_deterministic_provider() -> None:
    os.environ["LLM_PROVIDER"] = "deterministic"
    os.environ["LLM_MODEL"] = "sqlgenx-fixture-v1"
    os.environ.setdefault("RAG_EMBEDDING_LOCAL_ONLY", "true")

    from src.config.settings import get_settings

    get_settings.cache_clear()
