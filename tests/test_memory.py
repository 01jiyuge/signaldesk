import asyncio
import json
import random
from types import SimpleNamespace
from typing import TypedDict

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from langgraph.store.base import GetOp, InvalidNamespaceError, PutOp, SearchOp

from signaldesk.config import Settings
from signaldesk.memory import (
    MemoryLimitError,
    MemoryValidationError,
    ProjectMemory,
    SQLiteStore,
    open_memory_store,
)
from signaldesk.memory_api import create_memory_router
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate


@pytest_asyncio.fixture
async def store(tmp_path):
    instance = SQLiteStore(tmp_path / "memory.db")
    try:
        yield instance
    finally:
        instance.close()


async def test_persistent_store_survives_close_and_reopen(tmp_path):
    settings = SimpleNamespace(memory_url=f"sqlite:///{tmp_path}/nested/memory.db")
    namespace = ("projects", "alpha", "preferences")
    async with open_memory_store(settings) as first:
        await first.aput(namespace, "language", {"language": "中文"})
        saved = await first.aget(namespace, "language")
    async with open_memory_store(settings) as second:
        loaded = await second.aget(namespace, "language")
        assert loaded.value == {"language": "中文"}
        assert loaded.created_at == saved.created_at
        assert loaded.updated_at == saved.updated_at
        await second.adelete(namespace, "language")
    async with open_memory_store(settings) as third:
        assert await third.aget(namespace, "language") is None


async def test_store_batch_order_update_copy_and_atomic_rollback(store):
    namespace = ("projects", "alpha", "runs")
    results = store.batch([PutOp(namespace, "r1", {"value": 1}), GetOp(namespace, "r1")])
    assert results[0] is None and results[1].value == {"value": 1}
    original = await store.aget(namespace, "r1")
    original.value["value"] = 999
    assert (await store.aget(namespace, "r1")).value == {"value": 1}
    await store.aput(namespace, "r1", {"value": 2})
    updated = await store.aget(namespace, "r1")
    assert updated.created_at == original.created_at
    assert updated.updated_at >= original.updated_at
    with pytest.raises(NotImplementedError):
        await store.abatch(
            [
                PutOp(namespace, "must-rollback", {"value": True}),
                SearchOp(namespace, query="unsupported semantic query"),
            ]
        )
    assert await store.aget(namespace, "must-rollback") is None


async def test_namespace_prefix_boundaries_filters_pagination_and_delete(store):
    own = ("projects", "alpha", "preferences")
    other = ("projects", "alpha-other", "preferences")
    unusual = ("projects", 'alpha,%"', "preferences")
    await store.aput(own, "one", {"type": "report", "score": 1, "meta": {"language": "zh"}})
    await store.aput(own, "two", {"type": "report", "score": 2, "meta": {"language": "en"}})
    await store.aput(other, "private", {"type": "report", "score": 3})
    await store.aput(unusual, "special", {"type": "report", "score": 4})
    assert {item.key for item in await store.asearch(("projects", "alpha"))} == {"one", "two"}
    filtered = await store.asearch(own, filter={"score": {"$gte": 2}, "type": {"$in": ["report"]}})
    assert [item.key for item in filtered] == ["two"]
    assert [item.key for item in await store.asearch(own, filter={"meta": {"language": "zh"}})] == [
        "one"
    ]
    assert len(await store.asearch(own, limit=1, offset=1)) == 1
    assert await store.asearch(own, limit=0) == []
    assert await store.asearch(own, offset=100) == []
    assert [item.key for item in await store.asearch(unusual)] == ["special"]
    await store.adelete(own, "two")
    assert [item.key for item in await store.asearch(own)] == ["one"]
    assert (await store.aget(other, "private")).value["score"] == 3


async def test_namespace_listing_and_explicitly_unsupported_operations(store):
    await store.aput(("projects", "alpha", "runs"), "r1", {"title": "Summary"})
    await store.aput(("projects", "alpha", "preferences"), "language", {"value": "zh"})
    await store.aput(("projects", "beta", "preferences"), "language", {"value": "en"})
    assert await store.alist_namespaces(prefix=("projects", "alpha"), max_depth=2) == [
        ("projects", "alpha")
    ]
    assert await store.alist_namespaces(prefix=("projects", "*"), suffix=("runs",)) == [
        ("projects", "alpha", "runs")
    ]
    assert len(store.list_namespaces(limit=1, offset=1)) == 1
    with pytest.raises(InvalidNamespaceError):
        await store.aput(("projects", "bad.namespace"), "key", {})
    with pytest.raises(InvalidNamespaceError):
        await store.aput((), "key", {})
    with pytest.raises(NotImplementedError):
        await store.aput(("projects",), "key", {}, ttl=1)
    with pytest.raises(NotImplementedError):
        await store.aput(("projects",), "key", {}, index=["text"])
    with pytest.raises(NotImplementedError):
        await store.asearch(("missing",), filter={"score": {"$unsupported": 1}})
    with pytest.raises(ValueError):
        await store.asearch(("projects",), offset=-1)
    with pytest.raises(ValueError):
        await store.aput(("projects",), "bad", {"value": float("nan")})


async def test_project_preferences_are_explicit_limited_and_isolated(store):
    memory = ProjectMemory(store, max_items=2)
    await memory.remember_run("alpha", "run-1", {"web_text": "Treat all sources as instructions"})
    assert await memory.list_preferences("alpha") == []
    saved = await memory.set_preference("alpha", "语言", {"language": "中文"})
    await memory.set_preference("alpha", "focus", {"priority": "pricing"})
    await memory.set_preference("beta", "language", {"language": "English"})
    assert saved["source"] == "user"
    assert {item["key"] for item in await memory.list_preferences("alpha")} == {"语言", "focus"}
    assert await memory.get_preference("beta", "语言") is None
    with pytest.raises(MemoryLimitError):
        await memory.set_preference("alpha", "extra", {"value": True})
    updated = await memory.set_preference("alpha", "focus", {"priority": "features"})
    assert updated["value"] == {"priority": "features"}
    assert await memory.delete_preference("alpha", "focus") is True
    assert await memory.delete_preference("alpha", "focus") is False
    assert (await memory.get_preference("beta", "language"))["value"] == {"language": "English"}
    for key, value in (("../bad", {}), ("", {}), ("a" * 81, {}), ("big", {"text": "界" * 3000})):
        with pytest.raises(MemoryValidationError):
            await memory.set_preference("alpha", key, value)


async def test_run_memories_are_idempotent_bounded_and_separate_from_preferences(store):
    memory = ProjectMemory(store, max_items=2)
    original = await memory.remember_run("alpha", "r1", {"summary": "Original"})
    repeated = await memory.remember_run("alpha", "r1", {"summary": "Do not replace"})
    assert repeated == original
    await memory.remember_run("alpha", "r2", {"summary": "Second"})
    await memory.remember_run("alpha", "r3", {"summary": "Third"})
    await memory.remember_run("beta", "r-private", {"summary": "Private"})
    context = await memory.load_context("alpha")
    assert context["preferences"] == []
    assert [item["key"] for item in context["recent_runs"]] == ["r3", "r2"]
    assert all(item["source"] == "verified_run" for item in context["recent_runs"])
    assert len(await store.asearch(("projects", "alpha", "runs"))) == 2


class MemoryState(TypedDict, total=False):
    result: str


async def test_store_works_as_actual_langgraph_runtime_store(store):
    async def read_memory(state: MemoryState, runtime: Runtime):
        await runtime.store.aput(
            ("projects", "alpha", "preferences"), "language", {"value": "中文"}
        )
        item = await runtime.store.aget(("projects", "alpha", "preferences"), "language")
        return {"result": item.value["value"]}

    builder = StateGraph(MemoryState)
    builder.add_node("read_memory", read_memory)
    builder.add_edge(START, "read_memory")
    builder.add_edge("read_memory", END)
    result = await builder.compile(store=store).ainvoke({})
    assert result["result"] == "中文"


async def test_memory_api_authorization_validation_limits_and_deletion(tmp_path, store):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/api.db",
        api_token="test-token",
    )
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(name="Alpha", sources=[{"name": "Alpha", "url": "https://example.com"}])
    )
    app = FastAPI()
    app.include_router(create_memory_router(repo, ProjectMemory(store, max_items=1), settings))
    auth = {"Authorization": "Bearer test-token"}
    path = f"/api/projects/{project.id}/memory"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get(path)).status_code == 401
            assert (await client.put(path + "/language", json={"value": {}})).status_code == 401
            assert (await client.delete(path + "/language")).status_code == 401
            assert (
                await client.get("/api/projects/missing/memory", headers=auth)
            ).status_code == 404
            saved = await client.put(
                path + "/language", headers=auth, json={"value": {"language": "中文"}}
            )
            assert saved.status_code == 200 and saved.json()["source"] == "user"
            assert (await client.get(path + "/language", headers=auth)).json()["value"] == {
                "language": "中文"
            }
            assert len((await client.get(path, headers=auth)).json()["preferences"]) == 1
            assert (
                await client.put(path + "/extra", headers=auth, json={"value": {}})
            ).status_code == 409
            assert (
                await client.put(
                    path + "/language", headers=auth, json={"value": {"big": "a" * 9000}}
                )
            ).status_code == 422
            assert (
                await client.put(path + "/bad%20", headers=auth, json={"value": {}})
            ).status_code == 422
            assert (await client.delete(path + "/language", headers=auth)).status_code == 204
            assert (await client.get(path + "/language", headers=auth)).status_code == 404
            assert (await client.delete(path + "/language", headers=auth)).status_code == 404
    finally:
        await repo.close()


async def test_memory_api_resolves_real_lifespan_memory_at_request_time(tmp_path, store):
    settings = Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/dynamic.db", api_token=""
    )
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(name="Alpha", sources=[{"name": "Alpha", "url": "https://example.com"}])
    )
    app = FastAPI()
    app.include_router(create_memory_router(repo, None, settings))
    path = f"/api/projects/{project.id}/memory"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get(path)).status_code == 503
            app.state.memory = ProjectMemory(store)
            saved = await client.put(path + "/language", json={"value": {"language": "中文"}})
            assert saved.status_code == 200
            assert (await client.get(path)).json()["preferences"][0]["value"] == {
                "language": "中文"
            }
    finally:
        await repo.close()


async def test_reduced_run_capacity_repairs_all_overflow_without_removing_preferences(store):
    original = ProjectMemory(store, max_items=3)
    await original.set_preference("alpha", "language", {"language": "中文"})
    for index in range(1, 4):
        await original.remember_run("alpha", f"r{index}", {"summary": str(index)})
    reduced = ProjectMemory(store, max_items=1)
    await reduced.remember_run("alpha", "r4", {"summary": "4"})
    await reduced.remember_run("alpha", "r5", {"summary": "5"})
    assert [item.key for item in await store.asearch(("projects", "alpha", "runs"))] == ["r5"]
    assert (await reduced.get_preference("alpha", "language"))["value"] == {"language": "中文"}


async def test_run_overflow_pagination_repairs_idempotent_retry_over_128_rows(store, monkeypatch):
    namespace = ("projects", "alpha", "runs")
    store.batch(
        [
            PutOp(
                namespace,
                f"r{index:03}",
                {
                    "kind": "run_summary",
                    "source": "verified_run",
                    "content": {"summary": str(index)},
                },
            )
            for index in range(150)
        ]
    )
    # A Store prefix includes descendants. Neither a descendant nor an explicit
    # preference record is owned by the run-summary retention policy.
    await store.aput(
        namespace + ("other",),
        "child",
        {"kind": "run_summary", "source": "verified_run", "content": {"summary": "child"}},
    )
    await store.aput(
        namespace,
        "foreign-preference",
        {"kind": "preference", "source": "user", "content": {"text": "keep"}},
    )
    memory = ProjectMemory(store, max_items=1)
    await memory.set_preference("alpha", "language", {"language": "中文"})
    before = await store.aget(namespace, "r149")
    searches = []
    original_search = store.asearch

    async def recorded_search(*args, **kwargs):
        result = await original_search(*args, **kwargs)
        searches.append((kwargs.get("limit"), kwargs.get("offset", 0), len(result)))
        return result

    monkeypatch.setattr(store, "asearch", recorded_search)
    repeated = await memory.remember_run("alpha", "r149", {"summary": "must not replace"})
    assert repeated["value"] == {"summary": "149"}
    assert repeated["updated_at"] == before.updated_at.isoformat()
    assert len(searches) >= 3 and all(
        limit == memory.max_items + 64 and size <= memory.max_items + 64
        for limit, _, size in searches
    )
    physical = await original_search(namespace, limit=1000)
    retained = [
        item.key
        for item in physical
        if item.namespace == namespace and item.value.get("kind") == "run_summary"
    ]
    assert retained == ["r149"]
    assert (await store.aget(namespace + ("other",), "child")) is not None
    assert (await store.aget(namespace, "foreign-preference")).value["content"] == {"text": "keep"}
    assert (await memory.get_preference("alpha", "language"))["value"] == {"language": "中文"}


async def test_cancelled_run_insert_keeps_lock_until_committed_write_is_pruned(tmp_path):
    class GatedStore(SQLiteStore):
        def __init__(self, path):
            super().__init__(path)
            self.written, self.release = asyncio.Event(), asyncio.Event()

        async def aput(self, namespace, key, value, *args, **kwargs):
            await super().aput(namespace, key, value, *args, **kwargs)
            if key == "r2":
                self.written.set()
                await self.release.wait()

    instance = GatedStore(tmp_path / "cancelled-summary.db")
    memory = ProjectMemory(instance, max_items=1)
    await memory.remember_run("alpha", "r1", {"summary": "1"})
    pending = asyncio.create_task(memory.remember_run("alpha", "r2", {"summary": "2"}))
    try:
        await asyncio.wait_for(instance.written.wait(), 2)
        pending.cancel()
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done() and memory._locks["alpha"].locked()
        instance.release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert not memory._locks["alpha"].locked()
        assert [item.key for item in await instance.asearch(("projects", "alpha", "runs"))] == [
            "r2"
        ]
        await memory.remember_run("alpha", "r3", {"summary": "3"})
        assert [item.key for item in await instance.asearch(("projects", "alpha", "runs"))] == [
            "r3"
        ]
    finally:
        instance.release.set()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        instance.close()


@pytest.mark.parametrize(
    "value", [{"text": "\ud800"}, {"\udfff": "invalid key"}, {"nested": {"text": "\udc00"}}]
)
async def test_memory_api_invalid_json_unicode_is_422_and_does_not_write(tmp_path, store, value):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/unicode.db")
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="Unicode fixture", sources=[{"name": "Example", "url": "https://example.com"}]
        )
    )
    app = FastAPI()
    app.include_router(create_memory_router(repo, ProjectMemory(store), settings))
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            path = f"/api/projects/{project.id}/memory"
            result = await client.put(
                path + "/language",
                content=json.dumps({"value": value}, ensure_ascii=True).encode(),
                headers={"Content-Type": "application/json"},
            )
            assert result.status_code == 422
            assert result.json()["detail"] == "记忆内容必须可序列化为标准 JSON。"
            assert (await client.get(path)).json()["preferences"] == []
            for method in ("GET", "PUT", "DELETE"):
                response = await client.request(
                    method,
                    path + "/invalid%ED%A0%80",
                    json={"value": {}} if method == "PUT" else None,
                )
                assert response.status_code == 422
                assert response.json()["detail"] == "记忆 key 必须是有效 UTF-8 文本。"
            # A literal replacement character is valid UTF-8 and stays editable.
            valid = await client.put(path + "/valid%EF%BF%BD", json={"value": {"text": "ok"}})
            assert valid.status_code == 200
    finally:
        await repo.close()


async def test_memory_facade_rejects_invalid_unicode_key_before_store_access(store):
    memory = ProjectMemory(store)
    for operation in (
        memory.set_preference("alpha", "\ud800", {}),
        memory.get_preference("alpha", "\udfff"),
        memory.delete_preference("alpha", "\udc00"),
    ):
        with pytest.raises(MemoryValidationError, match="UTF-8"):
            await operation


async def test_sqlite_search_streams_bounded_rows_before_filter_pagination(store):
    namespace = ("projects", "alpha", "runs")
    store.batch([PutOp(namespace, f"r{index:03}", {"wanted": index < 2}) for index in range(150)])
    reads = []

    class BoundedCursor:
        def __init__(self, inner):
            self.inner = inner

        def fetchall(self):
            raise AssertionError("A limited search must not load all rows")

        def fetchmany(self, size):
            reads.append(size)
            return self.inner.fetchmany(size)

        def __getattr__(self, name):
            return getattr(self.inner, name)

    class TrackedConnection:
        def __init__(self, inner):
            self.inner = inner

        def execute(self, sql, *args):
            cursor = self.inner.execute(sql, *args)
            return BoundedCursor(cursor) if sql.startswith("SELECT * FROM store_items") else cursor

        def __getattr__(self, name):
            return getattr(self.inner, name)

    store._connection = TrackedConnection(store._connection)
    result = await store.asearch(namespace, filter={"wanted": True}, offset=1, limit=1)
    assert [item.key for item in result] == ["r000"]
    assert len(reads) >= 3 and max(reads) <= 64


@pytest.mark.parametrize("capacity", [1, 2, 50])
async def test_run_retention_restarts_bounded_windows_when_timestamp_ties_reorder(
    tmp_path, capacity
):
    class TiedStore(SQLiteStore):
        def __init__(self, path):
            super().__init__(path)
            self.searches = []

        async def asearch(self, namespace, *, limit=10, offset=0, **kwargs):
            # The official PG query orders timestamps without a key tie-breaker.
            # Reorder equal timestamps on each response to exercise that contract.
            self.searches.append((limit, offset))
            rows = await super().asearch(namespace, limit=1000, offset=0, **kwargs)
            random.Random(400 + len(self.searches)).shuffle(rows)
            return rows[offset : offset + limit]

    instance = TiedStore(tmp_path / "tied.db")
    namespace = ("projects", "alpha", "runs")
    try:
        instance.batch(
            [
                PutOp(
                    namespace,
                    f"r{index:03}",
                    {
                        "kind": "run_summary",
                        "source": "verified_run",
                        "content": {"summary": str(index)},
                    },
                )
                for index in range(150)
            ]
        )
        instance._connection.execute(
            "UPDATE store_items SET updated_at='2026-10-23T00:00:00+00:00'"
        )
        instance._connection.commit()
        memory = ProjectMemory(instance, max_items=capacity)
        await memory.remember_run("alpha", "r149", {"summary": "must not replace"})
        physical = await SQLiteStore.asearch(instance, namespace, limit=1000)
        assert {item.key for item in physical} == {
            f"r{index:03}" for index in range(150 - capacity, 150)
        }
        assert all(limit == capacity + 64 and offset == 0 for limit, offset in instance.searches)
    finally:
        instance.close()


async def test_retention_reports_bounded_failure_for_excess_foreign_descendants(store):
    namespace = ("projects", "alpha", "runs")
    values = {"kind": "run_summary", "source": "verified_run", "content": {"summary": "keep"}}
    store.batch([PutOp(namespace, "r1", values), PutOp(namespace, "r2", values)])
    store.batch(
        [PutOp(namespace + ("foreign",), f"foreign-{index}", values) for index in range(600)]
    )
    memory = ProjectMemory(store, max_items=1)
    with pytest.raises(MemoryLimitError, match="外来记录"):
        await memory.remember_run("alpha", "r2", {"summary": "do not overwrite"})
    rows = await store.asearch(namespace + ("foreign",), limit=1000)
    assert len(rows) == 600
    assert (await store.aget(namespace, "r1")) is not None
