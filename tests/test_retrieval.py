from datetime import UTC, datetime

import pytest_asyncio

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.retrieval import Retriever
from signaldesk.schemas import ProjectCreate, RunCreate, SnapshotRecord


@pytest_asyncio.fixture
async def retrieval_setup(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/retrieval.db",
        enable_vector_search=False,
        enable_fulltext_search=False,
        reranker_provider="none",
        model="",
        openai_api_key="",
    )
    repo = Repository(settings)
    await repo.initialize()
    try:
        yield repo, settings
    finally:
        await repo.close()


async def add_snapshot(repo, name, text, *, status="completed", url="https://example.com/pricing"):
    project = await repo.create_project(
        ProjectCreate(name=name, sources=[{"name": name, "url": url}])
    )
    run = await repo.create_run(project.id, RunCreate())
    snapshot = SnapshotRecord(
        id=f"{run.id}-snapshot",
        run_id=run.id,
        project_id=project.id,
        source_name=name,
        url=url,
        title="Pricing",
        text=text,
        content_hash="test-hash",
        created_at=datetime.now(UTC).isoformat(),
    )
    await repo.save_snapshot(snapshot)
    await repo.update_run(run.id, status=status)
    return project, run, snapshot


async def test_retrieval_only_returns_own_project_completed_snapshots(retrieval_setup):
    repo, settings = retrieval_setup
    alpha, _, own = await add_snapshot(repo, "Alpha", "Alpha pricing: Pro costs $25.")
    beta, _, private = await add_snapshot(repo, "Beta", "Beta pricing: secret enterprise discount.")
    failed = await repo.create_run(alpha.id, RunCreate())
    unverified = own.model_copy(
        update={
            "id": f"{failed.id}-snapshot",
            "run_id": failed.id,
            "url": "https://example.com/unverified",
            "text": "Unverified pricing: Pro costs $1.",
        }
    )
    await repo.save_snapshot(unverified)
    await repo.update_run(failed.id, status="failed")
    retriever = Retriever(repo, settings)
    await retriever.initialize()
    hits = await retriever.search(alpha.id, "pricing")
    assert [hit.evidence_id for hit in hits] == [own.id]
    assert all(hit.strategy == "lexical" for hit in hits)
    assert private.id not in {hit.evidence_id for hit in hits}
    assert await retriever.search(alpha.id, "secret") == []
    assert [hit.evidence_id for hit in await retriever.search(beta.id, "pricing")] == [private.id]


async def test_disabled_vector_search_never_constructs_or_calls_embedding_provider(
    retrieval_setup, monkeypatch
):
    repo, settings = retrieval_setup
    project, _, snapshot = await add_snapshot(repo, "Alpha", "Pro pricing is $25.")

    def forbidden_provider(**kwargs):
        raise AssertionError("Disabled vector search must not construct an embedding provider")

    monkeypatch.setattr("signaldesk.retrieval.OpenAIEmbeddings", forbidden_provider)
    retriever = Retriever(repo, settings)
    await retriever.initialize()
    await retriever.index_snapshots([snapshot])
    hits = await retriever.search(project.id, "pricing")
    assert [hit.evidence_id for hit in hits] == [snapshot.id]
    assert retriever.embeddings is None


async def test_lexical_retrieval_prefers_latest_successful_snapshot(retrieval_setup):
    repo, settings = retrieval_setup
    project, _, old = await add_snapshot(repo, "Alpha", "Pro pricing is $25.")
    newer_run = await repo.create_run(project.id, RunCreate(demo_revision=2))
    newer = old.model_copy(
        update={
            "id": f"{newer_run.id}-snapshot",
            "run_id": newer_run.id,
            "text": "Pro pricing is $35.",
            "created_at": datetime.now(UTC).isoformat(),
        }
    )
    await repo.save_snapshot(newer)
    await repo.update_run(newer_run.id, status="completed")
    retriever = Retriever(repo, settings)
    hits = await retriever.search(project.id, "pricing")
    assert [hit.evidence_id for hit in hits] == [newer.id]
    assert "$35" in hits[0].text
