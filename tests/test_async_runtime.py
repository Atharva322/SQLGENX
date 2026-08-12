from __future__ import annotations

import asyncio
import threading
import time

import pytest

from src.runtime.async_runtime import (
    AsyncRuntimeConfig,
    AsyncRuntimeOverloaded,
    AsyncRuntimeTimeout,
    BoundedAsyncRuntime,
)


@pytest.mark.asyncio
async def test_runtime_runs_blocking_work_and_releases_slot() -> None:
    runtime = BoundedAsyncRuntime(
        AsyncRuntimeConfig(max_workers=1, queue_limit=0, request_timeout_seconds=1)
    )
    try:
        assert await runtime.run_blocking(lambda: "ok") == "ok"
        assert await runtime.run_blocking(lambda: "again") == "again"
        assert runtime.queue_depth == 0
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_rejects_overload_without_unbounded_queue() -> None:
    runtime = BoundedAsyncRuntime(
        AsyncRuntimeConfig(max_workers=1, queue_limit=0, request_timeout_seconds=1)
    )
    started = threading.Event()
    release = threading.Event()

    def blocking() -> str:
        started.set()
        release.wait(timeout=1)
        return "done"

    try:
        first = asyncio.create_task(runtime.run_blocking(blocking))
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(0.01)
        assert started.is_set()
        with pytest.raises(AsyncRuntimeOverloaded) as exc_info:
            await runtime.run_blocking(lambda: "rejected")
        assert exc_info.value.capacity == 1
        release.set()
        assert await first == "done"
    finally:
        release.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_timeout_path_releases_capacity() -> None:
    runtime = BoundedAsyncRuntime(
        AsyncRuntimeConfig(
            max_workers=2,
            queue_limit=0,
            request_timeout_seconds=0.05,
        )
    )

    try:
        with pytest.raises(AsyncRuntimeTimeout) as exc_info:
            await runtime.run_blocking(lambda: time.sleep(0.2), stage="provider")
        assert exc_info.value.stage == "provider"
        assert await runtime.run_blocking(lambda: "after-timeout") == "after-timeout"
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_cancellation_releases_capacity() -> None:
    runtime = BoundedAsyncRuntime(
        AsyncRuntimeConfig(
            max_workers=2,
            queue_limit=0,
            request_timeout_seconds=1,
        )
    )
    started = threading.Event()
    release = threading.Event()

    def blocking() -> str:
        started.set()
        release.wait(timeout=1)
        return "cancelled-work-finished"

    try:
        task = asyncio.create_task(runtime.run_blocking(blocking))
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(0.01)
        assert started.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await runtime.run_blocking(lambda: "after-cancel") == "after-cancel"
    finally:
        release.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_shutdown_rejects_new_work() -> None:
    runtime = BoundedAsyncRuntime(AsyncRuntimeConfig(max_workers=1, queue_limit=0))
    await runtime.shutdown()
    with pytest.raises(AsyncRuntimeOverloaded):
        await runtime.run_blocking(lambda: "closed")
