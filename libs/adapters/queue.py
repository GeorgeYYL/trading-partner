from __future__ import annotations
from typing import Protocol, Optional
from .errors import QueueUnavailable, InvalidMessage  # (see errors below)
from libs.contracts.job_models import JobMessage


class QueueAdapter(Protocol):
    def enqueue(self, msg: JobMessage) -> str:
        """Put one message; returns message_id. Raise InvalidMessage/QueueUnavailable."""
        ...

    def try_pop(self) -> Optional[tuple[str, JobMessage]]:
        """Pop one message if available. Returns (message_id, msg) or None."""
        ...

    def ack(self, message_id: str) -> None:
        """Acknowledge successful processing; idempotent."""
        ...

    def ping(self) -> bool:
        return True