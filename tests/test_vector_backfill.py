"""Incremental indexing obeys successful provenance and retry ownership."""

import pytest
from sqlalchemy import text

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.retrieval import Retriever
from signaldesk.schemas import ProjectCreate, RunCreate, SnapshotRecord


@pytest.mark.parametrize(
    "old_status,other_project,other_hash,already_indexed,missing",
    [
        ("completed", False, False, False, False),
        ("failed", False, False, False, True),
        ("completed", True, False, False, True),
        ("completed", False, True, False, True),
        ("failed", False, False, True, False),
    ],
    ids=["reuse-success", "exclude-failed", "exclude-other-project", "changed-hash", "retry"],
)
async def test_backfill_only_embeds_missing_current_sources(
    tmp_path, monkeypatch, old_status, other_project, other_hash, already_indexed, missing
):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/backfill.db",
        enable_vector_search=False,
        model="",
        openai_api_key="",
    )
    repo = Repository(settings)
    retriever = Retriever(repo, settings)
    await repo.initialize()
    try:
        payload = ProjectCreate(
            name="Backfill", sources=[{"name": "Pricing", "url": "https://fixture.example/pricing"}]
        )
        own = await repo.create_project(payload)
        prior_project = await repo.create_project(payload) if other_project else own
        before = await repo.create_run(prior_project.id, RunCreate(mode="live"))
        old = SnapshotRecord(
            id="old",
            run_id=before.id,
            project_id=prior_project.id,
            source_name="Pricing",
            url=str(own.sources[0].url),
            title="Pricing",
            text="Original content",
            content_hash="other-hash" if other_hash else "same-hash",
            created_at="2026-01-01T00:00:00Z",
        )
        await repo.save_snapshot(old)
        await repo.update_run(before.id, status=old_status)
        current_run = await repo.create_run(own.id, RunCreate(mode="live"))
        current = old.model_copy(
            update={
                "id": "current",
                "run_id": current_run.id,
                "project_id": own.id,
                "content_hash": "same-hash",
                "created_at": "2026-01-02T00:00:00Z",
            }
        )
        await repo.save_snapshot(current)
        async with repo.engine.begin() as conn:
            # The reuse decision is portable SQL; no vector operations or model
            # calls are needed to check ownership and successful-source scope.
            await conn.execute(
                text("CREATE TABLE evidence_embeddings(snapshot_id TEXT, project_id TEXT)")
            )
            await conn.execute(
                text("INSERT INTO evidence_embeddings VALUES (:id,:project)"),
                {"id": old.id, "project": old.project_id},
            )
            if already_indexed:
                await conn.execute(
                    text("INSERT INTO evidence_embeddings VALUES (:id,:project)"),
                    {"id": current.id, "project": own.id},
                )
        batches = []

        async def record(snapshots):
            batches.append([s.id for s in snapshots])

        monkeypatch.setattr(retriever, "index_snapshots", record)
        retriever.embeddings = object()
        await retriever.backfill_missing_snapshots([current])
        assert batches == [[current.id] if missing else []]
        retriever.embeddings = None
        await retriever.backfill_missing_snapshots([current])
        assert len(batches) == 1
    finally:
        await retriever.close()
        await repo.close()
