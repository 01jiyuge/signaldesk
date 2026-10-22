import asyncio

import httpx
import pytest

from signaldesk.app import create_app
from signaldesk.checkpoints import open_checkpointer
from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.retrieval import Retriever
from signaldesk.schemas import ProjectCreate, RunCreate
from signaldesk.workflow import Workflow


def local_settings(tmp_path):
    return Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/business.db",
        checkpoint_url=f"sqlite:///{tmp_path}/checkpoints.db",
        memory_url=f"sqlite:///{tmp_path}/memory.db",
    )


def project_payload():
    return {
        "name": "AI SaaS 演示",
        "sources": [
            {"name": "Northstar", "url": "https://northstar.example/pricing"},
            {"name": "Orbit", "url": "https://orbit.example/changelog"},
        ],
    }


async def wait_run(client, run_id):
    for _ in range(200):
        response = await client.get(f"/api/runs/{run_id}")
        run = response.json()
        if run["status"] in {"completed", "failed", "cancelled"}:
            return run
        await asyncio.sleep(0.01)
    pytest.fail("Run did not finish")


async def test_full_demo_baseline_change_unchanged_and_restart(tmp_path):
    settings = local_settings(tmp_path)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            project = (await client.post("/api/projects", json=project_payload())).json()
            finished = []
            for revision in (1, 2, 2):
                created = await client.post(
                    f"/api/projects/{project['id']}/runs",
                    json={"mode": "demo", "demo_revision": revision},
                )
                assert created.status_code == 202
                run = await wait_run(client, created.json()["id"])
                assert run["status"] == "completed", run
                finished.append(run)
            baseline, changed, unchanged = finished
            assert len(baseline["report"]["findings"]) == 2
            assert len(changed["report"]["findings"]) == 2
            assert unchanged["report"]["findings"] == []
            assert all(
                item["change_type"] == "changed"
                for item in changed["report"]["evidence"]
                if not item["is_previous"]
            )
            price = changed["report"]["findings"][0]
            assert "$29" in price["detail"] and "$39" in price["detail"]
            assert len(price["citations"]) == 2
            hits = (
                await client.get(f"/api/projects/{project['id']}/search", params={"q": "SSO"})
            ).json()
            assert hits and "fulltext" in hits[0]["retrieval_methods"]
            events = (await client.get(f"/api/runs/{changed['id']}/events")).json()
            assert [event["data"]["stage"] for event in events if event["kind"] == "stage"] == [
                "collect",
                "compare",
                "research",
                "verify",
                "persist",
            ]
    restarted = create_app(settings)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://test"
        ) as client:
            saved = (await client.get(f"/api/runs/{changed['id']}")).json()
            assert saved["report"] == changed["report"]
            assert saved["status"] == "completed"


async def test_checkpoint_resume_skips_finished_collection(tmp_path, monkeypatch):
    settings = local_settings(tmp_path)
    repo = Repository(settings)
    await repo.initialize()
    retriever = Retriever(repo, settings)
    project = await repo.create_project(ProjectCreate.model_validate(project_payload()))
    run = await repo.create_run(project.id, RunCreate())
    try:
        async with open_checkpointer(settings.checkpoint_url) as checkpointer:
            workflow = Workflow(repo, settings, checkpointer, retriever)
            original = workflow.research.generate

            async def fail_research(*args, **kwargs):
                raise RuntimeError("Controlled interruption after collection")

            monkeypatch.setattr(workflow.research, "generate", fail_research)
            with pytest.raises(RuntimeError):
                await workflow.run(run.id)
            assert len(await repo.get_snapshots(run.id)) == 2

            def forbidden_refetch(*args):
                raise AssertionError("Completed collection must not be replayed")

            monkeypatch.setattr("signaldesk.workflow.demo_page", forbidden_refetch)
            monkeypatch.setattr(workflow.research, "generate", original)
            # Simulate a new process loading a persisted checkpoint.
        async with open_checkpointer(settings.checkpoint_url) as checkpointer:
            resumed = Workflow(repo, settings, checkpointer, retriever)
            await resumed.run(run.id, resume=True)
            assert (await repo.get_run(run.id)).status == "completed"
            assert len(await repo.get_snapshots(run.id)) == 2
    finally:
        await repo.close()
