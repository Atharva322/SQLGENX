from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Callable, TypeVar


T = TypeVar("T")


class AsyncRuntimeOverloaded(RuntimeError):
    def __init__(self, *, retry_after_seconds: int, queue_depth: int, capacity: int) -> None:
        super().__init__("Async query runtime is overloaded.")
        self.retry_after_seconds = retry_after_seconds
        self.queue_depth = queue_depth
        self.capacity = capacity


class AsyncRuntimeTimeout(TimeoutError):
    def __init__(self, *, timeout_seconds: float, stage: str) -> None:
        super().__init__(f"Async query runtime timed out during {stage}.")
        self.timeout_seconds = timeout_seconds
        self.stage = stage


@dataclass(frozen=True)
class AsyncRuntimeConfig:
    max_workers: int = 4
    queue_limit: int = 8
    queue_timeout_seconds: float = 0.25
    request_timeout_seconds: float = 30.0
    overload_retry_after_seconds: int = 2


class BoundedAsyncRuntime:
    def __init__(self, config: AsyncRuntimeConfig) -> None:
        self.config = config
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, config.max_workers),
            thread_name_prefix="sqlgenx-query",
        )
        self._semaphore = asyncio.Semaphore(max(1, config.max_workers))
        self._lock = asyncio.Lock()
        self._accepted = 0
        self._closed = False

    @property
    def queue_depth(self) -> int:
        return max(0, self._accepted - self.config.max_workers)

    async def run_blocking(self, func: Callable[..., T], *args, stage: str = "request", **kwargs) -> T:
        async with self._lock:
            if self._closed:
                raise AsyncRuntimeOverloaded(
                    retry_after_seconds=self.config.overload_retry_after_seconds,
                    queue_depth=self.queue_depth,
                    capacity=self.capacity,
                )
            if self._accepted >= self.capacity:
                raise AsyncRuntimeOverloaded(
                    retry_after_seconds=self.config.overload_retry_after_seconds,
                    queue_depth=self.queue_depth,
                    capacity=self.capacity,
                )
            self._accepted += 1

        acquired = False
        try:
            try:
                await asyncio.wait_for(
                    self._semaphore.acquire(),
                    timeout=self.config.queue_timeout_seconds,
                )
                acquired = True
            except TimeoutError as exc:
                raise AsyncRuntimeOverloaded(
                    retry_after_seconds=self.config.overload_retry_after_seconds,
                    queue_depth=self.queue_depth,
                    capacity=self.capacity,
                ) from exc

            loop = asyncio.get_running_loop()
            call = partial(func, *args, **kwargs)
            future = loop.run_in_executor(self._executor, call)
            try:
                return await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=self.config.request_timeout_seconds,
                )
            except TimeoutError as exc:
                raise AsyncRuntimeTimeout(
                    timeout_seconds=self.config.request_timeout_seconds,
                    stage=stage,
                ) from exc
        finally:
            if acquired:
                self._semaphore.release()
            async with self._lock:
                self._accepted = max(0, self._accepted - 1)

    @property
    def capacity(self) -> int:
        return max(1, self.config.max_workers) + max(0, self.config.queue_limit)

    async def shutdown(self) -> None:
        async with self._lock:
            self._closed = True
        self._executor.shutdown(wait=True, cancel_futures=True)


_query_runtime: BoundedAsyncRuntime | None = None


def get_query_runtime(config: AsyncRuntimeConfig) -> BoundedAsyncRuntime:
    global _query_runtime
    if _query_runtime is None:
        _query_runtime = BoundedAsyncRuntime(config)
    return _query_runtime


async def close_query_runtime() -> None:
    global _query_runtime
    runtime = _query_runtime
    _query_runtime = None
    if runtime is not None:
        await runtime.shutdown()
