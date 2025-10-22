from __future__ import annotations
from collections import deque
from typing import Optional, Tuple
from libs.contracts.job_models import JobMessage
from .errors import InvalidMessage


class InMemoryQueueAdapter:
    def __init__(self) -> None:
        self._q: deque[Tuple[str, JobMessage]] = deque()
        self._seq = 0
        self._inflight: set[str] = set()

    def enqueue(self, msg: JobMessage) -> str:
        if not msg.job_id or not msg.idempotency_key:
            raise InvalidMessage("missing job_id/idempotency_key")
        self._seq += 1
        mid = f"m{self._seq}"
        self._q.append((mid, msg))
        return mid

    def try_pop(self) -> Optional[tuple[str, JobMessage]]:
        if not self._q:
            return None
        mid, msg = self._q.popleft()
        self._inflight.add(mid)
        return mid, msg

    def ack(self, message_id: str) -> None:
        self._inflight.discard(message_id)

    def ping(self) -> bool:
        return True