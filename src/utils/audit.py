from pathlib import Path
from typing import Any

from loguru import logger

from src.config.settings import get_settings


_configured = False


def get_audit_logger():
    global _configured
    if _configured:
        return logger

    settings = get_settings()
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger.add(
        logs_dir / "query_audit.log",
        rotation="10 MB",
        retention="14 days",
        level=settings.log_level.upper(),
        enqueue=False,
    )
    _configured = True
    return logger


def log_blocked_query(question: str, sql: str, reasons: list[str]) -> None:
    log = get_audit_logger()
    log.warning(
        "blocked_query question={question} sql={sql} reasons={reasons}",
        question=question,
        sql=sql,
        reasons=reasons,
    )


def log_execution_event(event: str, payload: dict[str, Any]) -> None:
    log = get_audit_logger()
    log.info("execution_event event={event} payload={payload}", event=event, payload=payload)
