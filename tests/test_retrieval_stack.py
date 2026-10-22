import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, text

from signaldesk.collector import content_hash
from signaldesk.config import Settings
from signaldesk.db import Snapshot
from signaldesk.errors import PublicTaskError
from signaldesk.repository import Repository
from signaldesk.reranking import Reranker
from signaldesk.retrieval import Retriever
from signaldesk.schemas import ProjectCreate, RunCreate, SearchHit, SnapshotRecord


@pytest_asyncio.fixture
async def sqlite_retrieval(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/retrieval-stack.db",
        enable_fulltext_search=True,
        enable_vector_search=False,
        reranker_provider="none",
        model="",
        openai_api_key="",
        reranker_api_key="",
    )
    repo = Repository(settings)
    await repo.initialize()
    retrievers = []
    try:
        yield repo, settings, retrievers
    finally:
        for retriever in retrievers:
            await retriever.close()
        await repo.close()


async def new_project(repo, name="Fixture"):
    return await repo.create_project(
        ProjectCreate(
            name=name,
            sources=[
                {"name": name, "url": f"https://fixture.example/{index}"} for index in range(3)
            ],
        )
    )


async def save_revision(
    repo, project, *, title, body, sequence, status="completed", source=0, identifier=None
):
    run = await repo.create_run(project.id, RunCreate())
    snapshot = SnapshotRecord(
        id=identifier or f"{run.id}-snapshot",
        run_id=run.id,
        project_id=project.id,
        source_name=project.name,
        url=f"https://fixture.example/{source}",
        title=title,
        text=body,
        content_hash=content_hash(body),
        created_at=(datetime(2026, 10, 22, tzinfo=UTC) + timedelta(seconds=sequence)).isoformat(),
    )
    await repo.save_snapshot(snapshot)
    await repo.update_run(run.id, status=status)
    return snapshot


async def test_sqlite_fts_backfill_and_insert_update_delete_lifecycle(sqlite_retrieval):
    repo, settings, retrievers = sqlite_retrieval
    project = await new_project(repo)
    initial = await save_revision(repo, project, title="TitleBeacon", body="BodyBeacon", sequence=1)
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()
    await retriever.initialize()  # Restarting/index initialization must be idempotent.
    for query in ["TitleBeacon", "BodyBeacon"]:
        hits = await retriever.search(project.id, query)
        assert [hit.evidence_id for hit in hits] == [initial.id]
        assert "fulltext" in hits[0].retrieval_methods
    async with repo.engine.connect() as connection:
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM snapshot_fts WHERE snapshot_id=:id"), {"id": initial.id}
            )
            == 1
        )

    inserted = await save_revision(
        repo, project, title="InsertedBeacon", body="InsertBody", sequence=2, source=1
    )
    assert [hit.evidence_id for hit in await retriever.search(project.id, "InsertedBeacon")] == [
        inserted.id
    ]
    changed = inserted.model_copy(
        update={
            "title": "RevisedBeacon",
            "text": "ChangedBeacon",
            "content_hash": content_hash("ChangedBeacon"),
        }
    )
    await repo.save_snapshot(changed)
    assert await retriever.search(project.id, "InsertedBeacon") == []
    assert await retriever.search(project.id, "InsertBody") == []
    assert [hit.evidence_id for hit in await retriever.search(project.id, "ChangedBeacon")] == [
        inserted.id
    ]
    async with repo.sessions.begin() as session:
        await session.execute(delete(Snapshot).where(Snapshot.id == inserted.id))
    assert await retriever.search(project.id, "ChangedBeacon") == []
    async with repo.engine.connect() as connection:
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM snapshot_fts WHERE snapshot_id=:id"), {"id": inserted.id}
            )
            == 0
        )


async def test_fulltext_never_revives_superseded_failed_or_other_project_evidence(sqlite_retrieval):
    repo, settings, retrievers = sqlite_retrieval
    project = await new_project(repo, "Alpha")
    old = await save_revision(repo, project, title="Old", body="obsoleteonly", sequence=1)
    current = await save_revision(repo, project, title="Current", body="currentonly", sequence=2)
    failed = await save_revision(
        repo, project, title="Failed", body="failedonly", sequence=3, status="failed"
    )
    other = await new_project(repo, "Beta")
    private = await save_revision(
        repo, other, title="Private", body="privateonly currentonly", sequence=4
    )
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()
    for query in ["obsoleteonly", "failedonly", "privateonly"]:
        assert await retriever.search(project.id, query) == []
    hits = await retriever.search(project.id, "currentonly")
    assert [hit.evidence_id for hit in hits] == [current.id]
    assert not {old.id, failed.id, private.id} & {hit.evidence_id for hit in hits}
    assert set(hits[0].retrieval_methods) == {"lexical", "fulltext"}
    assert await retriever.search(project.id, "!!!") == []


@pytest.mark.parametrize("enable_fulltext", [False, True])
async def test_timestamp_ties_never_revive_old_lexical_content(sqlite_retrieval, enable_fulltext):
    repo, settings, retrievers = sqlite_retrieval
    settings = settings.model_copy(update={"enable_fulltext_search": enable_fulltext})
    project = await new_project(repo)
    await save_revision(
        repo,
        project,
        title="Old revision",
        body="obsoleteonly",
        sequence=1,
        identifier="snapshot-a-old",
    )
    current = await save_revision(
        repo,
        project,
        title="Current revision",
        body="currentonly",
        sequence=1,
        identifier="snapshot-z-current",
    )
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()

    assert await retriever.search(project.id, "obsoleteonly") == []
    for query in ["currentonly", "obsoleteonly currentonly"]:
        hits = await retriever.search(project.id, query)
        assert [hit.evidence_id for hit in hits] == [current.id]
        assert "lexical" in hits[0].retrieval_methods
        assert ("fulltext" in hits[0].retrieval_methods) is enable_fulltext


async def test_rrf_retains_recall_provenance_and_explicit_lexical_rerank_reorders(sqlite_retrieval):
    repo, settings, retrievers = sqlite_retrieval
    settings = settings.model_copy(update={"reranker_provider": "lexical"})
    project = await new_project(repo)
    relevant = await save_revision(
        repo, project, title="Enterprise pricing", body="enterprise pricing", sequence=1
    )
    noisy = await save_revision(
        repo,
        project,
        title="General notes",
        body="pricing " * 20 + "enterprise",
        sequence=2,
        source=1,
    )
    assert (await repo.search_snapshots(project.id, "enterprise pricing"))[0].id == noisy.id
    retriever = Retriever(repo, settings)
    retrievers.append(retriever)
    await retriever.initialize()
    hits = await retriever.search(project.id, "enterprise pricing", limit=2)
    assert [hit.evidence_id for hit in hits] == [relevant.id, noisy.id]
    assert all(hit.strategy == "hybrid_rrf+lexical_rerank" for hit in hits)
    assert all(set(hit.retrieval_methods) == {"lexical", "fulltext"} for hit in hits)
    assert hits[0].rerank_score > hits[1].rerank_score
    assert all(hit.retrieval_score > 0 and hit.retrieval_score != hit.rerank_score for hit in hits)


def rerank_hits():
    return [
        SearchHit(
            evidence_id=f"fixture-{index}",
            source_name="Fixture",
            url=f"https://fixture.example/{index}",
            title=f"Fixture {index}",
            text=f"Public fixture document {index}",
            score=0.03 - index * 0.001,
            strategy="hybrid_rrf",
            retrieval_methods=["lexical", "fulltext"],
            created_at="2026-10-22T00:00:00Z",
        )
        for index in range(3)
    ]


def http_settings():
    return Settings(
        _env_file=None,
        reranker_provider="http",
        reranker_url="https://reranker.fixture/rerank",
        reranker_model="fixture-neural-ranker",
        reranker_api_key="fixture-key",
        openai_api_key="",
    )


async def test_http_reranker_uses_provider_scores_and_retains_original_recall():
    seen = []

    def transport(request):
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "results": [
                    {"index": 0, "relevance_score": 0.2},
                    {"index": 2, "relevance_score": 0.9},
                ]
            },
        )

    originals = rerank_hits()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        results = await Reranker(http_settings(), client).rank("fixture query", originals, 2)
    assert [hit.evidence_id for hit in results] == ["fixture-2", "fixture-0"]
    assert results[0].score == 0.9 and results[0].retrieval_score == originals[2].score
    assert results[0].strategy == "hybrid_rrf+model_rerank"
    assert results[0].retrieval_methods == originals[2].retrieval_methods
    assert originals[2].rerank_score is None
    payload = json.loads(seen[0].content)
    assert payload["model"] == "fixture-neural-ranker" and payload["top_n"] == 2
    assert payload["documents"] == [f"{hit.title}\n{hit.text}" for hit in originals]
    assert seen[0].headers["authorization"] == "Bearer fixture-key"


@pytest.mark.parametrize(
    "results",
    [
        [{"index": 0, "relevance_score": 0.8}, {"index": 0, "relevance_score": 0.7}],
        [{"index": -1, "relevance_score": 0.8}, {"index": 1, "relevance_score": 0.7}],
        [{"index": 99, "relevance_score": 0.8}, {"index": 1, "relevance_score": 0.7}],
        [{"index": True, "relevance_score": 0.8}, {"index": 1, "relevance_score": 0.7}],
        [{"index": "0", "relevance_score": 0.8}, {"index": 1, "relevance_score": 0.7}],
        [{"index": 0, "relevance_score": True}, {"index": 1, "relevance_score": 0.7}],
        [{"index": 0, "relevance_score": float("nan")}, {"index": 1, "relevance_score": 0.7}],
        [{"index": 0, "relevance_score": float("inf")}, {"index": 1, "relevance_score": 0.7}],
        [{"index": 0, "relevance_score": 10**400}, {"index": 1, "relevance_score": 0.7}],
        [
            {"index": 0, "relevance_score": "private-service-data"},
            {"index": 1, "relevance_score": 0.7},
        ],
        [{"index": 0, "relevance_score": 0.8}],
        {"invalid": "private-service-data"},
    ],
)
async def test_http_reranker_rejects_invalid_rankings_with_safe_actionable_error(results):
    def transport(request):
        return httpx.Response(
            200,
            content=json.dumps({"results": results}),
            headers={"content-type": "application/json"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        with pytest.raises(PublicTaskError) as caught:
            await Reranker(http_settings(), client).rank("fixture query", rerank_hits(), 2)
    assert caught.value.code == "reranker_failed"
    assert caught.value.retryable and caught.value.suggestion
    assert "fixture-key" not in str(caught.value)
    assert "private-service-data" not in str(caught.value)


@pytest.mark.parametrize("failure", ["http", "timeout", "invalid_json"])
async def test_http_reranker_does_not_expose_error_response_or_credentials(failure):
    def transport(request):
        if failure == "timeout":
            raise httpx.ReadTimeout(
                "Authorization: Bearer fixture-key; private-response", request=request
            )
        if failure == "invalid_json":
            return httpx.Response(200, text="private-response Authorization: Bearer fixture-key")
        return httpx.Response(500, text="private-response Authorization: Bearer fixture-key")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        with pytest.raises(PublicTaskError) as caught:
            await Reranker(http_settings(), client).rank("fixture query", rerank_hits(), 2)
    assert caught.value.code == "reranker_failed"
    assert "private-response" not in str(caught.value)
    assert "fixture-key" not in str(caught.value)
