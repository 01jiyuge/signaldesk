"""User-visible retrieval failures, bounded reranking, and long-lived history."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from openai import APITimeoutError
from sqlalchemy import insert, text

from signaldesk.app import create_app
from signaldesk.config import Settings
from signaldesk.db import Run, Snapshot
from signaldesk.errors import PublicTaskError
from signaldesk.reranking import Reranker
from signaldesk.schemas import ProjectCreate, RunCreate, SnapshotRecord


@pytest_asyncio.fixture
async def retrieval_system(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/resilience.db",
        enable_fulltext_search=True,
        enable_vector_search=False,
        reranker_provider="lexical",
        embedding_dimensions=3,
        request_timeout_seconds=1,
        reranker_timeout_seconds=1,
        reranker_max_response_bytes=1024,
        openai_api_key="",
        reranker_api_key="",
        model="",
        api_token="",
        enable_a2a=False,
        sandbox_enabled=False,
    )
    app = create_app(settings)
    repo, retriever = app.state.repo, app.state.retriever
    await repo.initialize()
    await retriever.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="Retrieval fixture",
            sources=[
                {"name": "Pricing", "url": "https://fixture.example/pricing"},
                {"name": "Updates", "url": "https://fixture.example/updates"},
            ],
        )
    )
    run = await repo.create_run(project.id, RunCreate())
    snapshot = SnapshotRecord(
        id="pricing-latest",
        run_id=run.id,
        project_id=project.id,
        source_name="Pricing",
        url=str(project.sources[0].url),
        title="套餐信息 pricingneedle",
        text="套餐价格从29变为39。",
        content_hash="fixture-only",
        created_at="2026-01-01T00:00:00+00:00",
    )
    await repo.save_snapshot(snapshot)
    await repo.update_run(run.id, status="completed")
    try:
        yield app, repo, retriever, settings, project, snapshot
    finally:
        await retriever.close()
        await repo.close()


async def indexed_fixture(repo, snapshot):
    # The availability check preceding provider I/O is portable SQL. Provider
    # failures occur before the PostgreSQL-specific vector distance query.
    async with repo.engine.begin() as connection:
        await connection.execute(
            text("CREATE TABLE evidence_embeddings (snapshot_id TEXT, project_id TEXT)")
        )
        await connection.execute(
            text("INSERT INTO evidence_embeddings VALUES (:id, :project)"),
            {"id": snapshot.id, "project": snapshot.project_id},
        )


class ScriptedEmbeddings:
    def __init__(self, failure=None, *, wait=False):
        self.failure, self.wait = failure, wait
        self.started, self.finished = asyncio.Event(), asyncio.Event()

    async def result(self):
        self.started.set()
        try:
            if self.wait:
                await asyncio.Event().wait()
            raise self.failure
        finally:
            self.finished.set()

    async def aembed_query(self, query):
        return await self.result()

    async def aembed_documents(self, documents):
        return await self.result()


@pytest.mark.parametrize("failure", ["sdk_timeout", "http_timeout", "unknown"])
async def test_search_api_maps_query_embedding_failure_to_safe_retryable_503(
    retrieval_system, failure
):
    app, repo, retriever, _, project, snapshot = retrieval_system
    await indexed_fixture(repo, snapshot)
    request = httpx.Request("POST", "https://embeddings.fixture/")
    error = {
        "sdk_timeout": APITimeoutError(request=request),
        "http_timeout": httpx.ReadTimeout("Bearer fixture-secret private-body", request=request),
        "unknown": RuntimeError("Bearer fixture-secret private-body"),
    }[failure]
    retriever.embeddings = ScriptedEmbeddings(error)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            f"/api/projects/{project.id}/search", params={"q": "pricingneedle"}
        )
    assert response.status_code == 503
    assert response.json()["code"] == "embedding_query_failed"
    assert response.json()["retryable"] is True and response.json()["suggestion"]
    assert "fixture-secret" not in response.text and "private-body" not in response.text


async def test_index_embedding_failure_is_safe_and_writes_no_vectors(retrieval_system):
    _, repo, retriever, _, _, snapshot = retrieval_system
    await indexed_fixture(repo, snapshot)
    retriever.embeddings = ScriptedEmbeddings(RuntimeError("fixture-secret private-body"))
    with pytest.raises(PublicTaskError) as caught:
        await retriever.index_snapshots([snapshot])
    assert caught.value.code == "embedding_index_failed" and caught.value.retryable
    assert "fixture-secret" not in str(caught.value)
    async with repo.engine.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM evidence_embeddings")) == 1


@pytest.mark.parametrize(
    ("operation", "result"),
    [
        ("query", [1.0, 0.0]),
        ("query", [float("nan"), 0.0, 1.0]),
        ("index", []),
        ("index", [[True, 0.0, 1.0]]),
    ],
)
async def test_invalid_embedding_returns_fail_before_vector_sql(
    retrieval_system, operation, result
):
    _, repo, retriever, _, project, snapshot = retrieval_system
    await indexed_fixture(repo, snapshot)

    class InvalidEmbeddings:
        async def aembed_query(self, query):
            return result

        async def aembed_documents(self, documents):
            return result

    retriever.embeddings = InvalidEmbeddings()
    with pytest.raises(PublicTaskError) as caught:
        if operation == "query":
            await retriever.search(project.id, "pricingneedle")
        else:
            await retriever.index_snapshots([snapshot])
    assert caught.value.code == f"embedding_{operation}_failed"
    assert caught.value.retryable
    assert isinstance(caught.value.__cause__, ValueError)


@pytest.mark.parametrize("operation", ["query", "index"])
@pytest.mark.parametrize("stop", ["deadline", "cancel"])
async def test_embedding_total_deadline_and_caller_cancellation_close_provider_work(
    retrieval_system, operation, stop
):
    _, repo, retriever, _, project, snapshot = retrieval_system
    await indexed_fixture(repo, snapshot)
    provider = ScriptedEmbeddings(wait=True)
    retriever.embeddings = provider
    coroutine = (
        retriever.search(project.id, "pricingneedle")
        if operation == "query"
        else retriever.index_snapshots([snapshot])
    )
    task = asyncio.create_task(coroutine)
    await asyncio.wait_for(provider.started.wait(), timeout=2)
    if stop == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(PublicTaskError) as caught:
            await asyncio.wait_for(task, timeout=3)
        assert caught.value.code == f"embedding_{operation}_failed"
        assert caught.value.retryable
        assert isinstance(caught.value.__cause__, TimeoutError)
    assert provider.finished.is_set()


async def test_chinese_current_source_survives_thousand_other_source_revisions(retrieval_system):
    _, repo, retriever, _, project, snapshot = retrieval_system
    assert [hit.evidence_id for hit in await retriever.search(project.id, "价格")] == [snapshot.id]
    # This substring is not a standalone unicode61 token. Lexical recall must
    # still find the current source after the other source accumulates history.
    assert await retriever._fulltext(project.id, "价格", 5) == []
    runs, snapshots = [], []
    at = datetime(2026, 1, 2, tzinfo=UTC)
    for index in range(1000):
        run_id = str(uuid4())
        runs.append(
            dict(
                id=run_id,
                project_id=project.id,
                mode="demo",
                demo_revision=1,
                status="completed",
                stage="completed",
                created_at=at,
                updated_at=at,
            )
        )
        snapshots.append(
            dict(
                id=f"updates-{index:04d}",
                run_id=run_id,
                project_id=project.id,
                source_name="Updates",
                url=str(project.sources[1].url),
                title="Updates",
                text="Unrelated update",
                content_hash="same-fixture-content",
                created_at=at,
            )
        )
    async with repo.engine.begin() as connection:
        await connection.execute(insert(Run), runs)
        await connection.execute(insert(Snapshot), snapshots)
    assert [hit.evidence_id for hit in await retriever.search(project.id, "价格")] == [snapshot.id]
    assert [hit.evidence_id for hit in await retriever.search(project.id, "pricingneedle")] == [
        snapshot.id
    ]
    assert [hit.id for hit in await repo.search_snapshots(project.id, "Unrelated")] == [
        "updates-0999"
    ]


async def test_equal_score_equal_time_sources_have_stable_order(retrieval_system):
    _, repo, retriever, _, project, snapshot = retrieval_system
    run = await repo.create_run(project.id, RunCreate())
    second = snapshot.model_copy(
        update={
            "id": "z-same-score",
            "run_id": run.id,
            "url": str(project.sources[1].url),
        }
    )
    await repo.save_snapshot(second)
    await repo.update_run(run.id, status="completed")
    expected = [second.id, snapshot.id]
    assert [hit.id for hit in await repo.search_snapshots(project.id, "pricingneedle")] == expected
    assert [hit.id for hit in await retriever._fulltext(project.id, "pricingneedle", 5)] == expected
    assert [
        hit.evidence_id for hit in await retriever.search(project.id, "pricingneedle")
    ] == expected


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks, *, delay=0):
        self.chunks, self.delay = chunks, delay
        self.yielded = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            self.yielded += 1
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize("failure", ["declared_size", "stream_size", "gzip", "invalid_length"])
async def test_reranker_rejects_response_before_unbounded_read_or_decode(retrieval_system, failure):
    app, _, retriever, settings, project, _ = retrieval_system
    stream = ChunkStream([b"x" * 600, b"y" * 600, b"must-never-be-read"])
    headers = {
        "declared_size": {"content-length": "1025"},
        "stream_size": {},
        "gzip": {"content-encoding": "gzip"},
        "invalid_length": {"content-length": "private-body"},
    }[failure]
    requests = []

    def transport(request):
        requests.append(request)
        return httpx.Response(200, headers=headers, stream=stream)

    settings = settings.model_copy(
        update={
            "reranker_provider": "http",
            "reranker_url": "https://reranker.fixture/",
            "reranker_model": "fixture",
            "reranker_api_key": "fixture-secret",
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as remote:
        retriever.reranker = Reranker(settings, remote)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                f"/api/projects/{project.id}/search", params={"q": "pricingneedle"}
            )
    assert response.status_code == 503 and response.json()["code"] == "reranker_failed"
    assert "private-body" not in response.text and "fixture-secret" not in response.text
    assert len(requests) == 1 and requests[0].headers["accept-encoding"] == "identity"
    assert stream.yielded == (2 if failure == "stream_size" else 0)
    assert stream.closed


async def test_reranker_total_deadline_stops_slow_stream_and_closes_response(retrieval_system):
    app, _, retriever, settings, project, _ = retrieval_system
    stream = ChunkStream(
        [b'{"results":', b"[", b'{"index":0,', b'"relevance_score":1}', b"]}"], delay=0.3
    )
    settings = settings.model_copy(
        update={
            "reranker_provider": "http",
            "reranker_url": "https://reranker.fixture/",
            "reranker_model": "fixture",
        }
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
    ) as remote:
        retriever.reranker = Reranker(settings, remote)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await asyncio.wait_for(
                client.get(f"/api/projects/{project.id}/search", params={"q": "pricingneedle"}),
                timeout=3,
            )
    assert response.status_code == 503 and response.json()["code"] == "reranker_failed"
    assert stream.yielded < len(stream.chunks) and stream.closed


async def test_reranker_does_not_follow_redirects_from_injected_client(retrieval_system):
    app, _, retriever, settings, project, _ = retrieval_system
    requests = []

    def transport(request):
        requests.append(request)
        if request.url.path == "/rerank":
            return httpx.Response(307, headers={"location": "/unapproved-target"})
        return httpx.Response(200, json={"results": [{"index": 0, "relevance_score": 1}]})

    settings = settings.model_copy(
        update={
            "reranker_provider": "http",
            "reranker_url": "https://reranker.fixture/rerank",
            "reranker_model": "fixture",
            "reranker_api_key": "fixture-secret",
        }
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(transport), follow_redirects=True
    ) as remote:
        retriever.reranker = Reranker(settings, remote)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                f"/api/projects/{project.id}/search", params={"q": "pricingneedle"}
            )
    assert response.status_code == 503 and response.json()["code"] == "reranker_failed"
    assert len(requests) == 1 and requests[0].url.path == "/rerank"
    assert requests[0].headers["authorization"] == "Bearer fixture-secret"
