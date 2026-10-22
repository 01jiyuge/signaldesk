import asyncio

import pytest

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.retrieval import Retriever
from signaldesk.schemas import ProjectCreate, RunCreate
from signaldesk.workflow import Workflow


async def test_collector_failure_cancels_and_joins_sibling_writes(tmp_path, monkeypatch):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/db.sqlite")
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="Failure lifecycle",
            sources=[
                {"name": "Northstar", "url": "https://northstar.example/pricing"},
                {"name": "Orbit", "url": "https://orbit.example/changelog"},
            ],
        )
    )
    run = await repo.create_run(project.id, RunCreate())
    sibling_started = asyncio.Event()
    sibling_stopped = asyncio.Event()

    async def failing_save(snapshot):
        if snapshot.source_name == "Northstar":
            await sibling_started.wait()
            raise RuntimeError("Database write failed")
        sibling_started.set()
        try:
            await asyncio.sleep(10)
        finally:
            sibling_stopped.set()

    monkeypatch.setattr(repo, "save_snapshot", failing_save)
    try:
        workflow = Workflow(repo, settings, None, Retriever(repo, settings))
        with pytest.raises(ExceptionGroup):
            await workflow._collect({"run_id": run.id})
        assert sibling_stopped.is_set(), (
            "The failed run must not leave a collector writing in the background"
        )
    finally:
        await repo.close()
