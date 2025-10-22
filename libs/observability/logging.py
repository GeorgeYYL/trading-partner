# libs/observability/logging.py
from __future__ import annotations

import logging
import structlog
from typing import Optional

_CONFIGURED = False


def _level_to_int(level: str | int) -> int:
    """Accept 'INFO' / 'info' / 20 / logging.INFO and return an int level."""
    if isinstance(level, int):
        return level
    try:
        return getattr(logging, str(level).upper())
    except Exception:
        return logging.INFO


def setup_logging(level: str | int = "INFO") -> None:
    """
    Configure stdlib logging + structlog in a consistent, idempotent way.
    Call as early as possible (e.g., at FastAPI startup).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    lvl = _level_to_int(level)

    # stdlib logging: simple baseline formatter/handler
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)-7s %(name)s - %(message)s",
    )

    # structlog pipeline
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),         # add timestamp
            structlog.stdlib.add_log_level,                      # add level
            structlog.processors.StackInfoRenderer(),            # optional
            structlog.processors.format_exc_info,                # optional
            structlog.processors.JSONRenderer(),                 # JSON output
        ],
        wrapper_class=structlog.make_filtering_bound_logger(lvl),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _CONFIGURED = True
