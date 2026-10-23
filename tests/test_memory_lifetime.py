import asyncio
import sqlite3
import threading
from types import SimpleNamespace

import pytest

import signaldesk.memory as memory_module
from signaldesk.memory import SQLiteStore, open_memory_store


async def test_cancelled_constructor_is_drained_and_its_connection_closed(tmp_path, monkeypatch):
    constructed, release, completed = threading.Event(), threading.Event(), threading.Event()
    instances = []

    class GatedStore(SQLiteStore):
        def __init__(self, path):
            super().__init__(path)
            instances.append(self)
            constructed.set()
            release.wait(3)
            completed.set()

    monkeypatch.setattr(memory_module, "SQLiteStore", GatedStore)

    async def opening():
        async with open_memory_store(
            SimpleNamespace(memory_url=f"sqlite:///{tmp_path}/opening.db")
        ):
            raise AssertionError("Cancelled context entry must never yield")

    pending = asyncio.create_task(opening())
    try:
        assert await asyncio.to_thread(constructed.wait, 2)
        pending.cancel()
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert completed.is_set() and instances[0]._closed
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            instances[0]._connection.execute("SELECT 1")
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        for instance in instances:
            instance.close()


async def test_second_cancellation_cannot_abandon_store_close(tmp_path, monkeypatch):
    closing, release = threading.Event(), threading.Event()
    yielded = asyncio.Event()
    instances = []

    class GatedCloseStore(SQLiteStore):
        def close(self):
            closing.set()
            release.wait(3)
            super().close()

    monkeypatch.setattr(memory_module, "SQLiteStore", GatedCloseStore)

    async def running():
        async with open_memory_store(
            SimpleNamespace(memory_url=f"sqlite:///{tmp_path}/closing.db")
        ) as store:
            instances.append(store)
            yielded.set()
            await asyncio.Event().wait()

    pending = asyncio.create_task(running())
    try:
        await asyncio.wait_for(yielded.wait(), 2)
        pending.cancel()
        assert await asyncio.to_thread(closing.wait, 2)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert instances[0]._closed
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        for instance in instances:
            instance.close()


def test_partial_sqlite_constructor_failure_closes_allocated_connection(tmp_path, monkeypatch):
    class FailedConnection:
        closed = False
        row_factory = None

        def execute(self, _):
            raise sqlite3.OperationalError("controlled setup failure")

        def close(self):
            self.closed = True

    connection = FailedConnection()
    monkeypatch.setattr(memory_module.sqlite3, "connect", lambda *args, **kwargs: connection)
    with pytest.raises(sqlite3.OperationalError, match="controlled setup failure"):
        SQLiteStore(tmp_path / "failed.db")
    assert connection.closed
