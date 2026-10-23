"""Exact project-memory ownership over the official Store prefix protocol."""

import random
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from langgraph.store.base import BaseStore, PutOp, SearchItem, SearchOp
from langgraph.store.memory import InMemoryStore

from signaldesk.config import Settings
from signaldesk.memory import (
    MemoryLimitError,
    MemoryValidationError,
    ProjectMemory,
    SQLiteStore,
)
from signaldesk.memory_api import create_memory_router
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate


def value(category, label):
    kind, source = (
        ("preference", "user") if category == "preferences" else ("run_summary", "verified_run")
    )
    return {"kind": kind, "source": source, "content": {"text": label}}


@pytest_asyncio.fixture(params=["sqlite", "official_memory"])
async def store(request, tmp_path):
    instance = SQLiteStore(tmp_path / "memory.db") if request.param == "sqlite" else InMemoryStore()
    try:
        yield instance
    finally:
        if isinstance(instance, SQLiteStore):
            instance.close()


async def populate(store, project, *, foreign=90):
    ops = []
    for category in ("preferences", "runs"):
        namespace = ("projects", project, category)
        # Both insertion-order and updated-at-first Stores see a foreign prefix
        # before the owned record. Exact get and prefix search have different scope.
        for index in range(foreign):
            ops.append(PutOp(namespace + ("foreign",), f"before-{index}", value(category, "keep")))
        ops.append(PutOp(namespace, "owned", value(category, "own fixture")))
        for index in range(foreign):
            ops.append(PutOp(namespace + ("foreign",), f"after-{index}", value(category, "keep")))
        ops.append(PutOp(("projects", "other", category), "private", value(category, "other")))
    await store.abatch(ops)


async def test_descendants_do_not_hide_exact_records_or_consume_preference_quota(store):
    await populate(store, "alpha")
    memory = ProjectMemory(store, max_items=2)
    context = await memory.load_context("alpha")
    assert [item["key"] for item in context["preferences"]] == ["owned"]
    assert [item["key"] for item in context["recent_runs"]] == ["owned"]
    await memory.set_preference("alpha", "second", {"text": "explicit preference"})
    assert {item["key"] for item in await memory.list_preferences("alpha")} == {
        "owned",
        "second",
    }
    with pytest.raises(MemoryLimitError, match="最多保存 2 条"):
        await memory.set_preference("alpha", "third", {})
    for category in ("preferences", "runs"):
        descendants = await store.asearch(("projects", "alpha", category, "foreign"), limit=1000)
        assert len(descendants) == 180
        assert (await store.aget(("projects", "other", category), "private")) is not None


@pytest.mark.parametrize(
    ("kind", "source"), [("run_summary", "verified_run"), ("preference", "agent")]
)
async def test_unrelated_exact_category_is_not_counted_overwritten_or_deleted(store, kind, source):
    namespace = ("projects", "alpha", "preferences")
    original = {"kind": kind, "source": source, "content": {"text": "keep foreign record"}}
    await store.aput(namespace, "foreign", original)
    memory = ProjectMemory(store, max_items=1)
    assert await memory.list_preferences("alpha") == []
    assert await memory.get_preference("alpha", "foreign") is None
    await memory.set_preference("alpha", "language", {"language": "中文"})
    with pytest.raises(MemoryValidationError, match="其他类别"):
        await memory.set_preference("alpha", "foreign", {"text": "must not overwrite"})
    assert await memory.delete_preference("alpha", "foreign") is False
    assert (await store.aget(namespace, "foreign")).value == original


class TiedPrefixStore(BaseStore):
    """Valid prefix pages with arbitrary ordering among equal updated_at values."""

    def __init__(self, inner):
        self.inner, self.searches = inner, []

    def batch(self, ops):
        raise NotImplementedError("This protocol fixture is asynchronous")

    async def abatch(self, ops):
        results = []
        for op in ops:
            if not isinstance(op, SearchOp):
                results.extend(await self.inner.abatch([op]))
                continue
            self.searches.append((op.limit, op.offset))
            rows = await self.inner.asearch(op.namespace_prefix, filter=op.filter, limit=10000)
            at = datetime(2026, 10, 23, tzinfo=UTC)
            rows = [
                SearchItem(
                    namespace=item.namespace,
                    key=item.key,
                    value=item.value,
                    created_at=at,
                    updated_at=at,
                )
                for item in rows
            ]
            random.Random(400 + len(self.searches)).shuffle(rows)
            results.append(rows[op.offset : op.offset + op.limit])
        return results


async def test_reduced_limit_reads_complete_prefix_before_stable_tie_selection(store):
    ops = []
    for category in ("preferences", "runs"):
        namespace = ("projects", "alpha", category)
        ops.extend(PutOp(namespace, f"own-{i:03}", value(category, str(i))) for i in range(150))
        ops.extend(
            PutOp(namespace + ("foreign",), f"child-{i:03}", value(category, "keep"))
            for i in range(140)
        )
    await store.abatch(ops)
    tied = TiedPrefixStore(store)
    memory = ProjectMemory(tied, max_items=1)
    first = await memory.load_context("alpha")
    second = await memory.load_context("alpha")
    for context in (first, second):
        assert [item["key"] for item in context["preferences"]] == ["own-149"]
        assert [item["key"] for item in context["recent_runs"]] == ["own-149"]
    assert len(tied.searches) > 4 and all(offset == 0 for _, offset in tied.searches)
    assert max(limit for limit, _ in tied.searches) <= 8 * (memory.max_items + 64) + 1
    # Reading a reduced limit selects a bounded view; retention is a separate
    # write operation. It must not remove either overflow or foreign records.
    assert len(await store.asearch(("projects", "alpha", "runs"), limit=1000)) == 290


async def test_excess_prefix_is_an_explicit_bounded_error_not_empty_or_partial_context(store):
    await populate(store, "alpha", foreign=300)
    memory = ProjectMemory(store, max_items=1)
    with pytest.raises(MemoryLimitError, match="无法完整读取"):
        await memory.list_preferences("alpha")
    with pytest.raises(MemoryLimitError, match="无法完整读取"):
        await memory.load_context("alpha")
    with pytest.raises(MemoryLimitError, match="无法完整读取"):
        await memory.set_preference("alpha", "new", {})
    assert (await memory.get_preference("alpha", "owned"))["value"] == {"text": "own fixture"}
    for category in ("preferences", "runs"):
        assert len(await store.asearch(("projects", "alpha", category), limit=1000)) == 601


async def test_store_duplicate_page_is_explicit_no_progress_error(store):
    namespace = ("projects", "alpha", "preferences")
    await store.aput(namespace, "owned", value("preferences", "keep"))
    saved = (await store.asearch(namespace))[0]

    class RepeatingStore(TiedPrefixStore):
        async def abatch(self, ops):
            self.searches.append(True)
            return [[saved] * op.limit for op in ops]

    repeating = RepeatingStore(store)
    with pytest.raises(MemoryLimitError, match="稳定进展"):
        await ProjectMemory(repeating, max_items=2).list_preferences("alpha")
    assert len(repeating.searches) == 1
    assert (await store.aget(namespace, "owned")).value == saved.value


async def test_memory_http_reads_and_quota_stay_exact_with_descendant_consumers(tmp_path, store):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/api.db",
        api_token="",
        openai_api_key="",
        reranker_api_key="",
    )
    repo = Repository(settings)
    await repo.initialize()
    try:
        project = await repo.create_project(
            ProjectCreate(
                name="Memory namespace fixture",
                sources=[{"name": "Fixture", "url": "https://fixture.example/"}],
            )
        )
        await populate(store, project.id)
        app = FastAPI()
        app.include_router(create_memory_router(repo, ProjectMemory(store, max_items=2), settings))
        path = f"/api/projects/{project.id}/memory"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(path)
            assert result.status_code == 200
            assert [item["key"] for item in result.json()["preferences"]] == ["owned"]
            assert [item["key"] for item in result.json()["recent_runs"]] == ["owned"]
            created = await client.put(path + "/second", json={"value": {"language": "中文"}})
            assert created.status_code == 200
            assert (await client.put(path + "/third", json={"value": {}})).status_code == 409
    finally:
        await repo.close()
