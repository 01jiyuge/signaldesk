"""Opt-in real PostgreSQL tests; each test owns and drops only a random schema."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from signaldesk.collector import content_hash
from signaldesk.config import Settings
from signaldesk.db import EVENT_HISTORY_INDEX, Event, Run, Snapshot
from signaldesk.repository import Repository
from signaldesk.retrieval import Retriever
from signaldesk.schemas import ProjectCreate, RunCreate, SnapshotRecord


@pytest_asyncio.fixture
async def postgres_schema():
    supplied = os.environ.get("SIGNALDESK_TEST_POSTGRES_URL", "")
    if not supplied:
        pytest.skip("Set SIGNALDESK_TEST_POSTGRES_URL to run real PostgreSQL integration checks")
    database_url = make_url(supplied).set(drivername="postgresql+asyncpg")
    schema = f"signaldesk_test_{uuid4().hex}"
    admin = create_async_engine(database_url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    settings = Settings(
        _env_file=None,
        database_url=database_url.render_as_string(hide_password=False),
        enable_fulltext_search=True,
        enable_vector_search=False,
        reranker_provider="none",
        openai_api_key="fixture-not-used",
        embedding_model="fixture-3d",
        embedding_dimensions=3,
        openai_base_url=None,
        reranker_api_key="",
        model="",
        mcp_urls=[],
        mcp_allowed_tools=[],
    )
    repo = Repository(settings)
    await repo.engine.dispose()
    repo.engine = create_async_engine(
        database_url, connect_args={"server_settings": {"search_path": f"{schema},public"}}
    )
    repo.sessions = async_sessionmaker(repo.engine, expire_on_commit=False)
    retrievers = []
    try:
        await repo.initialize()
        yield repo, settings, schema, retrievers
    finally:
        for retriever in retrievers:
            await retriever.close()
        await repo.close()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


async def project(repo, name="Fixture"):
    return await repo.create_project(
        ProjectCreate(
            name=name,
            sources=[
                {"name": name, "url": f"https://fixture.example/{index}"} for index in range(2)
            ],
        )
    )


async def revision(repo, item, title, body, sequence, *, status="completed", source=0):
    run = await repo.create_run(item.id, RunCreate())
    snapshot = SnapshotRecord(
        id=f"{run.id}-snapshot",
        run_id=run.id,
        project_id=item.id,
        source_name=item.name,
        url=f"https://fixture.example/{source}",
        title=title,
        text=body,
        content_hash=content_hash(body),
        created_at=(datetime(2026, 10, 22, tzinfo=UTC) + timedelta(seconds=sequence)).isoformat(),
    )
    await repo.save_snapshot(snapshot)
    await repo.update_run(run.id, status=status)
    return snapshot


async def test_postgres_real_gin_fulltext_obeys_latest_successful_project_scope(postgres_schema):
    repo, settings, schema, retrievers = postgres_schema
    own = await project(repo, "Alpha")
    old = await revision(repo, own, "OldBeacon", "obsoletebeacon", 1)
    latest = await revision(repo, own, "TitleBeacon", "BodyBeacon currentbeacon", 2)
    failed = await revision(repo, own, "FailedBeacon", "failedbeacon", 3, status="failed")
    other = await project(repo, "Beta")
    private = await revision(repo, other, "TitleBeacon", "privatebeacon currentbeacon", 4)
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()
    async with repo.engine.connect() as connection:
        definition = await connection.scalar(
            text(
                "SELECT indexdef FROM pg_indexes WHERE schemaname=:schema AND indexname='ix_snapshots_fulltext'"
            ),
            {"schema": schema},
        )
    assert "using gin" in definition.lower()
    for query in ["TitleBeacon", "BodyBeacon", "currentbeacon"]:
        hits = await retriever.search(own.id, query)
        assert [hit.evidence_id for hit in hits] == [latest.id]
        assert "fulltext" in hits[0].retrieval_methods
        assert not {old.id, failed.id, private.id} & {hit.evidence_id for hit in hits}
    for query in ["OldBeacon", "obsoletebeacon", "failedbeacon", "privatebeacon"]:
        assert await retriever.search(own.id, query) == []


async def test_postgres_latest_sources_precede_history_candidate_limit(postgres_schema):
    repo, settings, _, retrievers = postgres_schema
    own = await project(repo, "Long-lived sources")
    target = await revision(repo, own, "套餐信息", "套餐价格从29变为39。", 0)
    noisy = await revision(repo, own, "Updates", "Unrelated update", 1, source=1)
    # One completed run can contain multiple source snapshots. Bulk insertion
    # recreates a large imported history without paying 1000 transaction costs.
    at = datetime(2026, 10, 23, tzinfo=UTC)
    rows = [
        dict(
            id=f"history-{index:04d}",
            run_id=noisy.run_id,
            project_id=own.id,
            source_name="Updates",
            url=noisy.url,
            title="Updates",
            text="Unrelated update",
            content_hash=content_hash("Unrelated update"),
            created_at=at,
        )
        for index in range(1000)
    ]
    async with repo.engine.begin() as connection:
        await connection.execute(insert(Snapshot), rows)
    assert [hit.id for hit in await repo.search_snapshots(own.id, "价格")] == [target.id]
    assert [hit.id for hit in await repo.search_snapshots(own.id, "Unrelated")] == ["history-0999"]
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()
    assert [hit.evidence_id for hit in await retriever.search(own.id, "价格")] == [target.id]


class ScriptedEmbeddings:
    def __init__(self, **kwargs):
        self.document_batches = []
        self.queries = []

    async def aembed_documents(self, documents):
        self.document_batches.append(documents)
        return [
            [0.0, 1.0, 0.0] if "replacementbody" in document else [1.0, 0.0, 0.0]
            for document in documents
        ]

    async def aembed_query(self, query):
        self.queries.append(query)
        return [1.0, 0.0, 0.0]


async def test_postgres_unchanged_live_run_backfills_missing_vectors_once(
    postgres_schema, monkeypatch
):
    from langgraph.checkpoint.memory import InMemorySaver

    from signaldesk.workflow import Workflow

    repo, settings, _, retrievers = postgres_schema
    own = await repo.create_project(
        ProjectCreate(
            name="Enable vectors later",
            sources=[{"name": "Pricing", "url": "https://fixture.example/pricing"}],
        )
    )
    baseline = await repo.create_run(own.id, RunCreate(mode="live"))
    body = "Current pricing body"
    old = SnapshotRecord(
        id="before-vector-enabled",
        run_id=baseline.id,
        project_id=own.id,
        source_name="Pricing",
        url=str(own.sources[0].url),
        title="Pricing",
        text=body,
        content_hash=content_hash(body),
        created_at="2026-01-01T00:00:00Z",
    )
    await repo.save_snapshot(old)
    await repo.update_run(baseline.id, status="completed")
    provider = ScriptedEmbeddings()
    monkeypatch.setattr("signaldesk.retrieval.OpenAIEmbeddings", lambda **kwargs: provider)
    settings = settings.model_copy(
        update={"enable_vector_search": True, "model": "openai:fixture-not-called"}
    )
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()

    async def local_fetch(url, config):
        return "Pricing", body

    async def forbidden_model(*args, **kwargs):
        raise AssertionError("Unchanged sources must not invoke the research model")

    monkeypatch.setattr("signaldesk.workflow.fetch_page", local_fetch)
    workflow = Workflow(repo, settings, InMemorySaver(), retriever)
    monkeypatch.setattr(workflow.research, "generate", forbidden_model)
    for number in range(2):
        run = await repo.create_run(own.id, RunCreate(mode="live"))
        await workflow.run(run.id)
        result = await repo.get_run(run.id)
        assert result.status == "completed"
        assert result.report["stats"]["model_call_skipped"]
        assert result.report["stats"]["unchanged_sources"] == 1
        hits = await retriever.search(own.id, "vector_only_without_lexical_overlap")
        current = (await repo.get_snapshots(run.id))[0]
        assert [hit.evidence_id for hit in hits] == [current.id]
        assert hits[0].retrieval_methods == ["vector"]
        assert len(provider.document_batches) == 1
        assert number == 0 or current.id != old.id


async def test_postgres_persist_retry_reuses_committed_baseline_embedding(
    postgres_schema, monkeypatch
):
    from langgraph.checkpoint.memory import InMemorySaver

    from signaldesk.schemas import Evidence, ResearchReport
    from signaldesk.workflow import Workflow

    repo, settings, _, retrievers = postgres_schema
    settings = settings.model_copy(update={"enable_vector_search": True})
    provider = ScriptedEmbeddings()
    monkeypatch.setattr("signaldesk.retrieval.OpenAIEmbeddings", lambda **kwargs: provider)
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()
    own = await project(repo, "Retry indexing")
    run = await repo.create_run(own.id, RunCreate(mode="live"))
    snapshot = SnapshotRecord(
        id="committed-vector-before-report",
        run_id=run.id,
        project_id=own.id,
        source_name="Pricing",
        url=str(own.sources[0].url),
        title="Pricing",
        text="Current body",
        content_hash=content_hash("Current body"),
        created_at="2026-01-01T00:00:00Z",
    )
    await repo.save_snapshot(snapshot)
    workflow = Workflow(repo, settings, InMemorySaver(), retriever)
    state = {
        "run_id": run.id,
        "snapshots": [snapshot.model_dump()],
        "evidence": [
            Evidence(
                id=snapshot.id,
                source_name=snapshot.source_name,
                url=snapshot.url,
                title=snapshot.title,
                text=snapshot.text,
                change_type="baseline",
            ).model_dump()
        ],
        "report": ResearchReport(
            title="Retry fixture",
            summary="Local fixture",
            findings=[],
            recommendations=[],
            limitations=[],
        ).model_dump(),
    }
    original_update = repo.update_run
    fail_once = True

    async def fail_after_embedding(run_id, **fields):
        nonlocal fail_once
        if fields.get("report") and fail_once:
            fail_once = False
            raise RuntimeError("Fixture: report commit failed after vector commit")
        return await original_update(run_id, **fields)

    monkeypatch.setattr(repo, "update_run", fail_after_embedding)
    with pytest.raises(RuntimeError, match="report commit failed"):
        await workflow._persist(state)
    assert len(provider.document_batches) == 1
    assert (await repo.get_run(run.id)).report is None
    await workflow._persist(state)
    assert (await repo.get_run(run.id)).status == "completed"
    assert len(provider.document_batches) == 1


async def test_postgres_vector_reuses_same_hash_for_latest_snapshot_and_excludes_old_hash(
    postgres_schema,
    monkeypatch,
):
    repo, settings, _, retrievers = postgres_schema
    async with repo.engine.connect() as connection:
        available = await connection.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name='vector')")
        )
    if not available:
        pytest.skip("This PostgreSQL installation has no pgvector extension available")
    provider = ScriptedEmbeddings()
    monkeypatch.setattr("signaldesk.retrieval.OpenAIEmbeddings", lambda **kwargs: provider)
    settings = settings.model_copy(update={"enable_vector_search": True})
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()
    own = await project(repo, "Alpha")
    old = await revision(repo, own, "First", "samebody", 1)
    await retriever.index_snapshots([old])
    latest = await revision(repo, own, "First", "samebody", 2)
    other = await project(repo, "Beta")
    private = await revision(repo, other, "Private", "privatebody", 3)
    await retriever.index_snapshots([private])
    failed = await revision(repo, own, "Failed", "failedbody", 4, status="failed")
    await retriever.index_snapshots([failed])
    hits = await retriever.search(own.id, "vector_only_query_without_lexical_match")
    assert [hit.evidence_id for hit in hits] == [latest.id]
    assert hits[0].retrieval_methods == ["vector"]
    assert hits[0].text == "samebody"
    assert not {old.id, private.id, failed.id} & {hit.evidence_id for hit in hits}
    async with repo.engine.connect() as connection:
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM evidence_embeddings WHERE snapshot_id=:id"),
                {"id": latest.id},
            )
            == 0
        )

    replacement = await revision(repo, own, "Replacement", "replacementbody", 5)
    assert await retriever.search(own.id, "vector_only_query_without_lexical_match") == []
    await retriever.index_snapshots([replacement])
    refreshed = await retriever.search(own.id, "vector_only_query_without_lexical_match")
    assert [hit.evidence_id for hit in refreshed] == [replacement.id]
    assert refreshed[0].text == "replacementbody"
    assert provider.queries and provider.document_batches


async def test_postgres_full_app_store_checkpointer_fulltext_and_agui(postgres_schema, monkeypatch):
    from signaldesk.app import create_app

    repo, settings, schema, _ = postgres_schema
    postgres_url = (
        make_url(settings.database_url)
        .set(drivername="postgresql")
        .update_query_dict({"options": f"-csearch_path={schema},public"})
        .render_as_string(hide_password=False)
    )
    settings = settings.model_copy(
        update={
            "checkpoint_url": postgres_url,
            "memory_url": postgres_url,
            "api_token": "",
        }
    )
    monkeypatch.setattr("signaldesk.app.Repository", lambda config: repo)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        assert type(app.state.memory.store).__name__ == "AsyncPostgresStore"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            item = (
                await client.post(
                    "/api/projects",
                    json={
                        "name": "Postgres full stack",
                        "sources": [
                            {"name": "Northstar", "url": "https://northstar.example/pricing"}
                        ],
                    },
                )
            ).json()
            path = f"/api/projects/{item['id']}"
            assert (
                await client.put(path + "/memory/language", json={"value": {"language": "中文"}})
            ).status_code == 200
            created = (
                await client.post(path + "/runs", json={"mode": "demo", "demo_revision": 1})
            ).json()
            for _ in range(200):
                run = (await client.get(f"/api/runs/{created['id']}")).json()
                if run["status"] in {"completed", "failed"}:
                    break
                await asyncio.sleep(0.01)
            assert run["status"] == "completed", run
            context = (await client.get(path + "/memory")).json()
            assert context["preferences"][0]["value"]["language"] == "中文"
            assert context["recent_runs"][0]["value"]["finding_count"] == 1
            hits = (await client.get(path + "/search", params={"q": "workspace"})).json()
            assert hits and "fulltext" in hits[0]["retrieval_methods"]
            stream = await client.get(f"/api/runs/{run['id']}/ag-ui")
            assert stream.status_code == 200 and "RUN_FINISHED" in stream.text
            checkpoint = await app.state.workflow.graph.aget_state(
                {"configurable": {"thread_id": run["id"]}}
            )
            assert checkpoint.values["report"]["title"] == run["report"]["title"]


@pytest.mark.skipif(os.getenv("SIGNALDESK_TEST_DOCKER") != "1", reason="Also requires real Docker")
async def test_postgres_a2a_binding_and_real_sandbox_audit(postgres_schema):
    from signaldesk.a2a_store import A2ARequestStore
    from signaldesk.sandbox import SandboxService

    repo, settings, _, _ = postgres_schema
    settings = settings.model_copy(update={"sandbox_enabled": True})
    own = await project(repo, "Postgres extensions")
    bindings = A2ARequestStore(repo)
    first, created = await bindings.create_or_get(own.id, RunCreate(), "pg-extension-message")
    assert created
    second, created = await A2ARequestStore(repo).create_or_get(
        own.id, RunCreate(), "pg-extension-message"
    )
    assert second.id == first.id and not created
    assert await bindings.contains(first.id)
    assert len(await repo.list_runs(own.id)) == 1
    service = SandboxService(repo, settings)
    await service.initialize()
    try:
        result = await service.execute(own.id, "result = sum(input_data)", [3, 8], run_id=first.id)
        assert result.status == "completed" and result.result == 11
        assert result.cleanup_confirmed
        execution_id = result.execution_id
    finally:
        await service.shutdown()
    recovered = SandboxService(repo, settings)
    await recovered.initialize()
    try:
        stored = await recovered.get(own.id, execution_id)
        assert stored.run_id == first.id and stored.result.result == 11
        assert [event.kind for event in await repo.list_events(first.id)][-2:] == [
            "sandbox_started",
            "sandbox_finished",
        ]
    finally:
        await recovered.shutdown()


async def test_postgres_legacy_history_upgrade_keysets_and_cached_remote_records(postgres_schema):
    from signaldesk.a2a_client import A2ADelegate
    from signaldesk.a2a_store import A2ARequestStore, delegations
    from signaldesk.sandbox import SandboxExecution, SandboxService

    repo, settings, _, _ = postgres_schema
    own = await project(repo, "History migration")
    await A2ARequestStore(repo).initialize()
    old_id = str(uuid4())
    async with repo.engine.begin() as connection:
        # Reconstruct the previous release's schema before initializing a Store.
        await connection.execute(text("DROP INDEX IF EXISTS ix_a2a_delegations_history"))
        await connection.execute(text("ALTER TABLE a2a_delegations DROP COLUMN created_at"))
        await connection.execute(
            text(
                "INSERT INTO a2a_delegations (id,message_id,payload_hash,project_id,remote,endpoint,remote_project_id,status) "
                "VALUES (:id,'pg-legacy','legacy',:project,'offline','https://offline.fixture/a2a/','target','PENDING')"
            ),
            {"id": old_id, "project": own.id},
        )
    first, second = A2ARequestStore(repo), A2ARequestStore(repo)
    await asyncio.gather(first.initialize(), second.initialize())
    assert (await first.get_delegation(old_id))["created_at"] is None
    ids = []
    at = datetime(2026, 10, 23, tzinfo=UTC)
    for index in range(4):
        row = await first.prepare_delegation(
            project_id=own.id,
            run_id=None,
            remote="offline",
            endpoint="https://offline.fixture/a2a/",
            remote_project_id="target",
            message_id=f"pg-history-{index}",
            mode="demo",
            demo_revision=1,
        )
        ids.append(row["id"])
        async with repo.sessions.begin() as session:
            await session.execute(
                delegations.update().where(delegations.c.id == row["id"]).values(created_at=at)
            )
    expected = sorted(ids, reverse=True) + [old_id]
    collected = []
    cursor = None
    while True:
        page = await first.page_delegations(own.id, limit=2, cursor=cursor)
        collected.extend(x["id"] for x in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert collected == expected
    cached = await A2ADelegate(settings, repo).local_record(own.id, old_id)
    assert cached["task"] is None and cached["created_at"] is None and "endpoint" not in cached
    service = SandboxService(repo, settings)
    await service.initialize()
    try:
        async with repo.sessions.begin() as session:
            for identifier in ids:
                session.add(
                    SandboxExecution(
                        id=identifier,
                        project_id=own.id,
                        run_id=None,
                        status="completed",
                        image="fixture",
                        code_hash="0" * 64,
                        result=None,
                        created_at=at,
                        updated_at=at,
                    )
                )
        page = await service.page(own.id, limit=2)
        tail = await service.page(own.id, limit=2, cursor=page["next_cursor"])
        assert [x["id"] for x in page["items"] + tail["items"]] == sorted(ids, reverse=True)
        assert tail["next_cursor"] is None
    finally:
        await service.shutdown()


async def test_postgres_memory_bulk_equal_times_repairs_retention_without_touching_preferences(
    postgres_schema,
):
    from langgraph.store.base import PutOp

    from signaldesk.memory import ProjectMemory, open_memory_store

    repo, settings, schema, _ = postgres_schema
    own = await project(repo, "Store retention")
    url = (
        make_url(settings.database_url)
        .set(drivername="postgresql")
        .update_query_dict({"options": f"-csearch_path={schema},public"})
        .render_as_string(hide_password=False)
    )
    settings = settings.model_copy(update={"memory_url": url})
    namespace = ("projects", own.id, "runs")
    async with open_memory_store(settings) as store:
        memory = ProjectMemory(store, max_items=1)
        await memory.set_preference(own.id, "language", {"value": "中文"})
        await store.abatch(
            [
                PutOp(
                    namespace,
                    f"seed-{i}",
                    {"kind": "run_summary", "source": "verified_run", "content": {"value": i}},
                )
                for i in range(150)
            ]
        )
        seeded = await store.asearch(namespace, limit=200)
        assert len(seeded) == 150 and len({item.updated_at for item in seeded}) == 1
        await memory.remember_run(own.id, "seed-0", {"value": "must not overwrite existing"})
        remaining = await store.asearch(namespace, limit=200)
        assert len(remaining) == 1
        await memory.remember_run(own.id, "latest", {"value": 200})
        assert [item.key for item in await store.asearch(namespace, limit=200)] == ["latest"]
        assert (await memory.get_preference(own.id, "language"))["value"] == {"value": "中文"}


async def test_postgres_run_history_pages_are_light_and_keep_old_active_visible(postgres_schema):
    from fastapi import FastAPI

    from signaldesk.api import create_router
    from signaldesk.jobs import JobManager

    repo, settings, schema, _ = postgres_schema
    own = await project(repo, "Paged history")
    other = await project(repo, "Other history")
    at = datetime(2026, 10, 23, tzinfo=UTC)
    identifiers = [str(UUID(int=i + 1)) for i in range(30)]
    old_active = str(UUID(int=100))
    body = {"summary": "REPORT_BODY_MUST_NOT_APPEAR_" + "x" * 100_000}
    rows = [
        dict(
            id=identifier,
            project_id=own.id,
            mode="demo",
            demo_revision=1,
            status="completed",
            stage="completed",
            error=None,
            report=body,
            created_at=at,
            updated_at=at,
        )
        for identifier in identifiers
    ]
    rows.extend(
        [
            dict(
                rows[0],
                id=old_active,
                report=None,
                status="running",
                stage="collect",
                created_at=at - timedelta(days=1),
            ),
            dict(rows[0], id=str(uuid4()), project_id=other.id),
        ]
    )
    async with repo.engine.begin() as connection:
        await connection.execute(insert(Run), rows)
        definition = await connection.scalar(
            text(
                "SELECT indexdef FROM pg_indexes WHERE schemaname=:schema AND indexname='ix_runs_project_created_id'"
            ),
            {"schema": schema},
        )
    assert "created_at DESC" in definition and "id DESC" in definition

    async def forbidden_runner(*args, **kwargs):
        raise AssertionError("Read-only paging cannot start a research run")

    jobs = JobManager(repo, forbidden_runner)
    app = FastAPI()
    app.include_router(create_router(repo, jobs, settings.model_copy(update={"api_token": ""})))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://fixture"
    ) as client:
        path = f"/api/projects/{own.id}/runs/page"
        first = await client.get(path)
        assert first.status_code == 200 and len(first.content) < 12_000
        page = first.json()
        assert [item["id"] for item in page["items"]] == sorted(identifiers, reverse=True)[:20]
        assert page["active_run"]["id"] == old_active and "report" not in page["active_run"]
        assert all("report" not in item for item in page["items"])
        assert "REPORT_BODY_MUST_NOT_APPEAR" not in first.text
        cursor = page["next_cursor"]
        new_id = str(uuid4())
        async with repo.engine.begin() as connection:
            await connection.execute(
                insert(Run), [dict(rows[0], id=new_id, created_at=at + timedelta(days=1))]
            )
        tail = (await client.get(path, params={"cursor": cursor})).json()
        assert [item["id"] for item in tail["items"]] == sorted(identifiers, reverse=True)[20:] + [
            old_active
        ]
        assert tail["active_run"]["id"] == old_active and not tail["has_more"]
        assert tail["next_cursor"] is None
        assert (
            await client.get(f"/api/projects/{other.id}/runs/page", params={"cursor": cursor})
        ).status_code == 422
        detail = await client.get(f"/api/runs/{identifiers[0]}")
        assert detail.json()["report"] == body
        assert (await client.post(f"/api/projects/{own.id}/runs", json={})).status_code == 409
    await jobs.shutdown()


async def test_postgres_memory_exact_reads_skip_descendants_and_return_bounded_scan_409(
    postgres_schema, monkeypatch
):
    from fastapi import FastAPI
    from langgraph.store.base import PutOp

    from signaldesk.memory import MemoryLimitError, ProjectMemory, open_memory_store
    from signaldesk.memory_api import create_memory_router

    repo, settings, schema, _ = postgres_schema
    own = await project(repo, "Exact Store reads")
    url = (
        make_url(settings.database_url)
        .set(drivername="postgresql")
        .update_query_dict({"options": f"-csearch_path={schema},public"})
        .render_as_string(hide_password=False)
    )
    settings = settings.model_copy(update={"memory_url": url, "api_token": ""})
    preferences = ("projects", own.id, "preferences")
    runs = ("projects", own.id, "runs")
    async with open_memory_store(settings) as store:
        memory = ProjectMemory(store, max_items=2)
        await memory.set_preference(own.id, "language", {"value": "中文"})
        await memory.remember_run(own.id, "own-run", {"value": "current context"})
        await store.abatch(
            [
                PutOp(
                    namespace + ("foreign",),
                    f"child-{index}",
                    {"kind": kind, "source": source, "content": {"value": index}},
                )
                for namespace, kind, source in [
                    (preferences, "preference", "user"),
                    (runs, "run_summary", "verified_run"),
                ]
                for index in range(80)
            ]
            + [
                PutOp(
                    preferences,
                    "foreign-kind",
                    {"kind": "other", "source": "system", "content": {"value": "keep"}},
                )
            ]
        )
        app = FastAPI()
        app.include_router(create_memory_router(repo, memory, settings))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://fixture"
        ) as client:
            path = f"/api/projects/{own.id}/memory"
            response = await client.get(path)
            assert response.status_code == 200
            assert [x["key"] for x in response.json()["preferences"]] == ["language"]
            assert [x["key"] for x in response.json()["recent_runs"]] == ["own-run"]
            assert (
                await client.put(path + "/focus", json={"value": {"value": "pricing"}})
            ).status_code == 200
            assert {x["key"] for x in (await client.get(path)).json()["preferences"]} == {
                "language",
                "focus",
            }
            assert (await client.delete(path + "/foreign-kind")).status_code == 404
            assert await store.aget(preferences, "foreign-kind") is not None
            assert await store.aget(preferences + ("foreign",), "child-0") is not None

            async def refuse_incomplete(*args, **kwargs):
                raise MemoryLimitError("Fixture: namespace exceeds the bounded scan")

            monkeypatch.setattr(memory, "load_context", refuse_incomplete)
            response = await client.get(path)
            assert response.status_code == 409 and "bounded scan" in response.json()["detail"]


async def test_postgres_event_pages_seek_safely_and_upgrade_existing_index(postgres_schema):
    from fastapi import FastAPI

    from signaldesk.api import create_router
    from signaldesk.jobs import JobManager
    from signaldesk.repository import MAX_EVENT_CURSOR

    repo, settings, schema, _ = postgres_schema
    own = await project(repo, "Event replay")
    other = await project(repo, "Other events")
    run = await repo.create_run(own.id, RunCreate())
    foreign = await repo.create_run(other.id, RunCreate())
    at = datetime(2026, 10, 23, tzinfo=UTC)
    async with repo.engine.begin() as conn:
        await conn.execute(
            insert(Event),
            [
                {
                    "run_id": target.id,
                    "kind": "stage",
                    "message": f"PRIVATE_EVENT_BODY_{number}",
                    "data": {"stage": "collect", "raw_exception": "PRIVATE_EVENT_BODY"},
                    "created_at": at,
                }
                for number in range(253)
                for target in (run, foreign)
            ],
        )
        await conn.execute(text(f'DROP INDEX "{schema}"."{EVENT_HISTORY_INDEX.name}"'))
    await repo.initialize()
    await repo.initialize()
    async with repo.engine.connect() as conn:
        definition = await conn.scalar(
            text("SELECT indexdef FROM pg_indexes WHERE schemaname=:schema AND indexname=:name"),
            {"schema": schema, "name": EVENT_HISTORY_INDEX.name},
        )
    assert "(run_id, id)" in definition
    expected = await repo.list_events(run.id)
    assert len(expected) == 254

    async def forbidden_runner(*_args, **_kwargs):
        raise AssertionError("Event paging cannot start research")

    jobs = JobManager(repo, forbidden_runner)
    app = FastAPI()
    token = "pg-event-fixture-token"
    app.include_router(create_router(repo, jobs, settings.model_copy(update={"api_token": token})))
    auth = {"Authorization": f"Bearer {token}"}
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://fixture"
        ) as client:
            path = f"/api/runs/{run.id}/events/page"
            assert (await client.get(path)).status_code == 401
            first = await client.get(path, headers=auth)
            assert first.status_code == 200 and "PRIVATE_EVENT_BODY" not in first.text
            page = first.json()
            assert len(page["items"]) == 100 and page["has_more"]
            seen = [item["id"] for item in page["items"]]
            assert seen == [item.id for item in expected[:100]]
            appended = await repo.append_event(run.id, "completed", "Finished after page one")
            after = page["next_after"]
            while page["has_more"]:
                page = (await client.get(path, headers=auth, params={"after": after})).json()
                assert all(
                    item["run_id"] == run.id and item["id"] > after for item in page["items"]
                )
                seen.extend(item["id"] for item in page["items"])
                after = page["next_after"]
            assert seen == [item.id for item in expected] + [appended.id]
            assert len(seen) == len(set(seen))
            assert (await client.get(path, headers=auth, params={"after": after})).json() == {
                "items": [],
                "next_after": after,
                "has_more": False,
                "producer_active": False,
            }
            maximum = await client.get(path, headers=auth, params={"after": MAX_EVENT_CURSOR})
            assert maximum.status_code == 200 and maximum.json()["next_after"] == MAX_EVENT_CURSOR
            assert (
                await client.get(path, headers=auth, params={"after": MAX_EVENT_CURSOR + 1})
            ).status_code == 422
            assert (await client.get(path, headers=auth, params={"limit": 201})).status_code == 422
            legacy = await client.get(f"/api/runs/{run.id}/events", headers=auth)
            assert isinstance(legacy.json(), list) and "PRIVATE_EVENT_BODY" in legacy.text
    finally:
        await jobs.shutdown()
