import asyncio

import httpx
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import delete

from signaldesk.api import create_router
from signaldesk.config import Settings
from signaldesk.db import Event
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository


@pytest_asyncio.fixture
async def api(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/api.db",
        api_token="test-token",
        model="",
        openai_api_key="",
    )
    repo = Repository(settings)
    await repo.initialize()
    started = asyncio.Event()
    calls = []

    async def runner(run_id, resume=False):
        calls.append((run_id, resume))
        started.set()
        await asyncio.Event().wait()

    jobs = JobManager(repo, runner)
    app = FastAPI()
    app.include_router(create_router(repo, jobs, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client, repo, jobs, started, calls
    await jobs.shutdown()
    await repo.close()


def project_payload(name="Alpha"):
    return {"name": name, "sources": [{"name": name, "url": "https://example.com/pricing"}]}


AUTH = {"Authorization": "Bearer test-token"}


async def test_auth_health_and_validation(api):
    client, _, _, _, _ = api
    response = await client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["live_configured"] is False
    assert (await client.get("/api/projects")).status_code == 401
    assert (
        await client.get("/api/projects", headers={"Authorization": "Bearer wrong"})
    ).status_code == 401
    assert (
        await client.post("/api/projects", headers=AUTH, json={"name": "Missing sources"})
    ).status_code == 422
    assert (await client.get("/api/projects/missing", headers=AUTH)).status_code == 404
    assert (await client.get("/api/runs/missing/events", headers=AUTH)).status_code == 404


async def test_run_conflict_cancel_resume_and_project_filter(api):
    client, repo, _, started, calls = api
    project = (await client.post("/api/projects", headers=AUTH, json=project_payload())).json()
    other = (await client.post("/api/projects", headers=AUTH, json=project_payload("Beta"))).json()
    response = await client.post(
        f"/api/projects/{project['id']}/runs", headers=AUTH, json={"mode": "demo"}
    )
    assert response.status_code == 202
    run = response.json()
    await asyncio.wait_for(started.wait(), timeout=3)
    assert (
        await client.post(f"/api/projects/{project['id']}/runs", headers=AUTH, json={})
    ).status_code == 409
    assert (await client.get(f"/api/runs?project_id={other['id']}", headers=AUTH)).json() == []
    assert (await client.post(f"/api/runs/{run['id']}/resume", headers=AUTH)).status_code == 409
    cancelled = await client.post(f"/api/runs/{run['id']}/cancel", headers=AUTH)
    assert cancelled.json()["status"] == "cancelled"
    events = (await client.get(f"/api/runs/{run['id']}/events", headers=AUTH)).json()
    assert events[-1]["kind"] == "cancelled"
    assert (
        await client.get(f"/api/runs/{run['id']}/events?after={events[-1]['id']}", headers=AUTH)
    ).json() == []
    resumed = await client.post(f"/api/runs/{run['id']}/resume", headers=AUTH)
    assert resumed.status_code == 202
    # Yield until the resumed runner has actually been scheduled.
    for _ in range(100):
        if len(calls) == 2:
            break
        await asyncio.sleep(0.01)
    assert calls[-1] == (run["id"], True)
    assert (await repo.get_run(run["id"])).status == "running"


async def test_live_mode_requires_configuration_and_search_is_project_scoped(api):
    client, _, _, _, _ = api
    project = (await client.post("/api/projects", headers=AUTH, json=project_payload())).json()
    live = await client.post(
        f"/api/projects/{project['id']}/runs", headers=AUTH, json={"mode": "live"}
    )
    assert live.status_code == 422
    assert (await client.get("/api/runs", headers=AUTH)).json() == []
    assert (
        await client.get(f"/api/projects/{project['id']}/search?q=pricing", headers=AUTH)
    ).json() == []
    assert (
        await client.get("/api/projects/missing/search?q=pricing", headers=AUTH)
    ).status_code == 404


async def test_edit_conflicts_with_active_run_and_keeps_old_configuration(api):
    client, repo, _, _, _ = api
    project = (await client.post("/api/projects", headers=AUTH, json=project_payload())).json()
    changed = project_payload("Edited")
    changed["description"] = "New description"
    changed["sources"] = [{"name": "Other", "url": "https://example.org/new"}]
    assert (await client.put(f"/api/projects/{project['id']}", json=changed)).status_code == 401
    assert (
        await client.put("/api/projects/missing", headers=AUTH, json=changed)
    ).status_code == 404
    assert (
        await client.put(f"/api/projects/{project['id']}", headers=AUTH, json={"name": "Invalid"})
    ).status_code == 422
    run = (await client.post(f"/api/projects/{project['id']}/runs", headers=AUTH, json={})).json()
    assert (
        await client.put(f"/api/projects/{project['id']}", headers=AUTH, json=changed)
    ).status_code == 409
    await client.post(f"/api/runs/{run['id']}/cancel", headers=AUTH)
    response = await client.put(f"/api/projects/{project['id']}", headers=AUTH, json=changed)
    assert response.status_code == 200
    assert response.json()["name"] == "Edited"
    assert response.json()["created_at"] == project["created_at"]
    assert (await repo.get_run_project(run["id"])).name == "Alpha"
    assert (await client.get(f"/api/runs?project_id={project['id']}", headers=AUTH)).json()[0][
        "id"
    ] == run["id"]
    # New runs have a configuration snapshot, so editing the project does not
    # prevent a cancelled run resuming with its original inputs.
    assert (await client.post(f"/api/runs/{run['id']}/resume", headers=AUTH)).status_code == 202


async def test_legacy_resume_is_invalidated_by_project_edit(api):
    client, repo, _, _, _ = api
    project = (await client.post("/api/projects", headers=AUTH, json=project_payload())).json()
    run = (await client.post(f"/api/projects/{project['id']}/runs", headers=AUTH, json={})).json()
    await client.post(f"/api/runs/{run['id']}/cancel", headers=AUTH)
    # Simulate a run written by V0.1 before immutable configuration existed.
    async with repo.sessions.begin() as session:
        await session.execute(
            delete(Event).where(Event.run_id == run["id"], Event.kind == "configuration")
        )
    assert (await repo.get_run_project(run["id"])).name == "Alpha"
    assert (
        await client.put(
            f"/api/projects/{project['id']}", headers=AUTH, json=project_payload("Edited")
        )
    ).status_code == 200
    assert await repo.get_run_project(run["id"]) is None
    response = await client.post(f"/api/runs/{run['id']}/resume", headers=AUTH)
    assert response.status_code == 409
    assert "请新建任务" in response.json()["detail"]


async def test_report_export_auth_status_format_and_attachment_headers(api):
    from test_exports import complete_run

    client, repo, _, _, _ = api
    project = (await client.post("/api/projects", headers=AUTH, json=project_payload())).json()
    run = (await client.post(f"/api/projects/{project['id']}/runs", headers=AUTH, json={})).json()
    path = f"/api/runs/{run['id']}/export"
    assert (await client.get(path)).status_code == 401
    assert (await client.get(path, headers=AUTH)).status_code == 409
    assert (await client.get(path + "?format=html", headers=AUTH)).status_code == 422
    assert (await client.get("/api/runs/missing/export", headers=AUTH)).status_code == 404
    await client.post(f"/api/runs/{run['id']}/cancel", headers=AUTH)
    await repo.update_run(run["id"], status="queued")
    await repo.update_run(run["id"], status="completed", report=complete_run().report)
    markdown = await client.get(path, headers=AUTH)
    assert markdown.status_code == 200
    assert markdown.headers["content-type"].startswith("text/markdown")
    assert (
        markdown.headers["content-disposition"]
        == f'attachment; filename="signaldesk-{run["id"]}.md"'
    )
    assert markdown.headers["x-content-type-options"] == "nosniff"
    assert "原文差异" in markdown.text
    exported = await client.get(path + "?format=json", headers=AUTH)
    assert exported.headers["content-type"].startswith("application/json")
    assert exported.json()["report"] == complete_run().report
    assert exported.json()["id"] == run["id"]


async def test_immediate_start_cancel_does_not_leave_sqlite_writer_locked(api):
    client, repo, _, _, _ = api
    project = (await client.post("/api/projects", headers=AUTH, json=project_payload())).json()
    for _ in range(10):
        response = await client.post(f"/api/projects/{project['id']}/runs", headers=AUTH, json={})
        assert response.status_code == 202
        run_id = response.json()["id"]
        cancelled = await client.post(f"/api/runs/{run_id}/cancel", headers=AUTH)
        assert cancelled.status_code == 200
        assert (await repo.get_run(run_id)).status == "cancelled"
