import asyncio
import json
from uuid import uuid4

import httpx
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import inspect
from test_sandbox import FakeCLI

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.sandbox import DockerSandbox, SandboxExecution, SandboxService
from signaldesk.sandbox_api import create_sandbox_router
from signaldesk.schemas import ProjectCreate, RunCreate

AUTH = {"Authorization": "Bearer sandbox-test-token"}


@pytest_asyncio.fixture
async def api(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/sandbox.db",
        sandbox_enabled=True,
        sandbox_max_concurrent=1,
        api_token="sandbox-test-token",
    )
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="Sandbox test",
            sources=[{"name": "Source", "url": "https://example.com"}],
        )
    )
    other = await repo.create_project(
        ProjectCreate(
            name="Other",
            sources=[{"name": "Source", "url": "https://example.org"}],
        )
    )
    cli = FakeCLI()
    service = SandboxService(repo, settings, executor=DockerSandbox(settings, cli=cli))
    await service.initialize()
    app = FastAPI()
    app.state.sandbox = service
    app.include_router(create_sandbox_router(repo, None, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client, repo, service, cli, project, other, settings
    await service.shutdown()
    await repo.close()


async def wait_for_terminal(service, project_id, execution_id):
    for _ in range(100):
        execution = await service.get(project_id, execution_id)
        if execution.status not in ("queued", "running"):
            return execution
        await asyncio.sleep(0.01)
    raise AssertionError("Execution did not become terminal")


async def test_http_auth_validation_scope_and_execution_audit(api):
    client, repo, service, cli, project, other, _ = api
    path = f"/api/projects/{project.id}/sandbox"
    assert (await client.get(path + "/capabilities")).status_code == 401
    assert (
        await client.get(path + "/executions", headers={"Authorization": "Bearer wrong"})
    ).status_code == 401
    assert (
        await client.get("/api/projects/missing/sandbox/capabilities", headers=AUTH)
    ).status_code == 404
    capability = (await client.get(path + "/capabilities", headers=AUTH)).json()
    assert capability["available"] and capability["limits"]["memory_mb"] == 128
    for body in [{"code": ""}, {"code": "result=1", "image": "attacker"}, {"code": "x" * 20001}]:
        assert (await client.post(path + "/executions", headers=AUTH, json=body)).status_code == 422
    assert (
        await client.post(
            path + "/executions", headers=AUTH, json={"code": "x", "input_data": "x" * 65537}
        )
    ).status_code == 422
    foreign_run = await repo.create_run(other.id, RunCreate())
    assert (
        await client.post(
            path + "/executions", headers=AUTH, json={"code": "result=1", "run_id": foreign_run.id}
        )
    ).status_code == 404
    run = await repo.create_run(project.id, RunCreate())
    created = await client.post(
        path + "/executions",
        headers=AUTH,
        json={
            "code": "result = input_data['private']",
            "input_data": {"private": "test-only"},
            "run_id": run.id,
        },
    )
    assert created.status_code == 202
    execution_id = created.json()["id"]
    finished = await wait_for_terminal(service, project.id, execution_id)
    assert finished.status == "completed" and finished.result.result == {"answer": 42}
    detail = await client.get(path + "/executions/" + execution_id, headers=AUTH)
    assert detail.json()["id"] == execution_id
    other_path = f"/api/projects/{other.id}/sandbox/executions/{execution_id}"
    assert (await client.get(other_path, headers=AUTH)).status_code == 404
    assert (await client.post(other_path + "/cancel", headers=AUTH)).status_code == 404
    assert (
        await client.post(path + "/executions/" + execution_id + "/cancel", headers=AUTH)
    ).status_code == 409
    assert len((await client.get(path + "/executions", headers=AUTH)).json()) == 1
    assert (
        await client.get(f"/api/projects/{other.id}/sandbox/executions", headers=AUTH)
    ).json() == []
    audit = [event for event in await repo.list_events(run.id) if event.kind.startswith("sandbox_")]
    assert [event.kind for event in audit] == ["sandbox_started", "sandbox_finished"]
    assert "test-only" not in json.dumps([event.model_dump() for event in audit])
    async with repo.sessions() as session:
        stored = await session.get(SandboxExecution, execution_id)
        assert len(stored.code_hash) == 64
        assert "test-only" not in json.dumps(stored.result)
    async with repo.engine.connect() as connection:
        columns = await connection.run_sync(
            lambda sync: inspect(sync).get_columns("sandbox_executions")
        )
        assert {column["name"] for column in columns}.isdisjoint({"code", "input_data"})


async def test_cancel_running_and_queued_durable_shutdown(api):
    client, _, service, cli, project, _, _ = api
    cli.hold = True
    path = f"/api/projects/{project.id}/sandbox/executions"
    first = (await client.post(path, headers=AUTH, json={"code": "result=1"})).json()
    await asyncio.wait_for(cli.started.wait(), 1)
    second = (await client.post(path, headers=AUTH, json={"code": "result=2"})).json()
    cancelled = await client.post(path + f"/{second['id']}/cancel", headers=AUTH)
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    assert (await service.get(project.id, first["id"])).status == "running"
    await service.shutdown()
    assert (await service.get(project.id, first["id"])).status == "cancelled"
    assert cli.active == 0


async def test_synchronous_tool_cancellation_cancels_background_execution(api):
    _, _, service, cli, project, _, _ = api
    cli.hold = True
    task = asyncio.create_task(service.execute(project.id, "result=1"))
    await asyncio.wait_for(cli.started.wait(), 1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    executions = await service.list(project.id)
    assert executions[0].status == "cancelled"
    assert executions[0].result.cleanup_confirmed


async def test_restart_retains_results_and_marks_old_jobs_interrupted_without_probing_disabled(api):
    _, repo, service, cli, project, _, settings = api
    completed = await service.execute(project.id, "result=1")
    execution_id = str(uuid4())
    from datetime import UTC, datetime

    async with repo.sessions.begin() as session:
        session.add(
            SandboxExecution(
                id=execution_id,
                project_id=project.id,
                run_id=None,
                status="running",
                image=settings.sandbox_image,
                code_hash="0" * 64,
                result=None,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
    disabled = settings.model_copy(update={"sandbox_enabled": False})
    clean_cli = FakeCLI()
    restarted = SandboxService(repo, disabled, executor=DockerSandbox(disabled, cli=clean_cli))
    await restarted.initialize()
    assert clean_cli.calls == []
    old = await restarted.get(project.id, execution_id)
    assert old.status == "interrupted" and not old.result.cleanup_confirmed
    assert (await restarted.get(project.id, completed.execution_id)).status == "completed"
    await restarted.shutdown()
    # Enabling after a disabled restart still recovers its unconfirmed container.
    enabled_cli = FakeCLI()
    enabled = SandboxService(repo, settings, executor=DockerSandbox(settings, cli=enabled_cli))
    await enabled.initialize()
    assert any(args[0] == "rm" for args, _ in enabled_cli.calls)
    recovered = await enabled.get(project.id, execution_id)
    assert recovered.status == "interrupted" and recovered.result.cleanup_confirmed
    await enabled.shutdown()


async def test_unavailable_is_503_and_does_not_create_execution(api):
    client, _, service, cli, project, _, _ = api
    cli.daemon = False
    path = f"/api/projects/{project.id}/sandbox/executions"
    response = await client.post(path, headers=AUTH, json={"code": "result=1"})
    assert response.status_code == 503
    assert "SECRET" not in response.text
    assert await service.list(project.id) == []
