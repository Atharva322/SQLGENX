from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from threading import RLock
from time import monotonic
from typing import Generic, TypeVar


K = TypeVar("K")
V = TypeVar("V")


@dataclass
class CacheEntry(Generic[V]):
    value: V
    expires_at: float


class TTLCache(Generic[K, V]):
    def __init__(self, max_size: int = 128, ttl_seconds: int = 300) -> None:
        self.max_size = max(1, max_size)
        self.ttl_seconds = max(0, ttl_seconds)
        self._values: OrderedDict[K, CacheEntry[V]] = OrderedDict()
        self._lock = RLock()

    def get(self, key: K) -> V | None:
        now = monotonic()
        with self._lock:
            entry = self._values.get(key)
            if entry is None:
                return None
            if entry.expires_at < now:
                self._values.pop(key, None)
                return None
            self._values.move_to_end(key)
            return deepcopy(entry.value)

    def set(self, key: K, value: V) -> None:
        expires_at = monotonic() + self.ttl_seconds
        with self._lock:
            self._values[key] = CacheEntry(deepcopy(value), expires_at)
            self._values.move_to_end(key)
            while len(self._values) > self.max_size:
                self._values.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._values.clear()

