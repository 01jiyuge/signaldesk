"""Opt-in real PostgreSQL tests; each test owns and drops only a random schema."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from signaldesk.collector import content_hash
from signaldesk.config import Settings
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
