import asyncio
import json
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest
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


async def test_audit_failure_keeps_successful_result_and_hides_raw_error(api, monkeypatch, caplog):
    _, repo, service, _, project, _, _ = api
    run = await repo.create_run(project.id, RunCreate())
    original = repo.append_event

    async def broken_finished(run_id, kind, message, data=None):
        if kind == "sandbox_finished":
            raise RuntimeError("fixture-sensitive-database-detail")
        return await original(run_id, kind, message, data)

    monkeypatch.setattr(repo, "append_event", broken_finished)
    result = await service.execute(project.id, "result=42", run_id=run.id)
    assert result.status == "completed" and result.result == {"answer": 42}
    stored = await service.get(project.id, result.execution_id)
    assert stored.status == "completed" and stored.result == result
    assert "fixture-sensitive-database-detail" not in caplog.text
    assert "could not be saved" in caplog.text


async def test_shutdown_joins_terminal_producer_and_does_not_audit_it_as_cancelled(
    api, monkeypatch
):
    _, repo, service, _, project, _, _ = api
    run = await repo.create_run(project.id, RunCreate())
    reached, stopped = asyncio.Event(), asyncio.Event()
    original = repo.append_event

    async def blocked_finished(run_id, kind, message, data=None):
        if kind == "sandbox_finished":
            reached.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        return await original(run_id, kind, message, data)

    monkeypatch.setattr(repo, "append_event", blocked_finished)
    row = await service.start(project.id, "result=42", run_id=run.id)
    await asyncio.wait_for(reached.wait(), 1)
    producer = service._tasks[row.id]
    assert (await service.get(project.id, row.id)).status == "completed"
    await asyncio.wait_for(service.shutdown(), 1)
    assert stopped.is_set() and producer.done()
    assert not service._tasks
    assert (await service.get(project.id, row.id)).result.result == {"answer": 42}
    assert "sandbox_cancelled" not in [event.kind for event in await repo.list_events(run.id)]


async def test_actual_cancellation_audits_transition_once_even_during_shutdown(api):
    _, repo, service, cli, project, _, _ = api
    run = await repo.create_run(project.id, RunCreate())
    cli.hold = True
    row = await service.start(project.id, "result=42", run_id=run.id)
    await asyncio.wait_for(cli.started.wait(), 1)
    await service.shutdown()
    assert (await service.get(project.id, row.id)).status == "cancelled"
    events = [event.kind for event in await repo.list_events(run.id)]
    assert events.count("sandbox_cancelled") == 1
    assert "sandbox_finished" not in events


async def test_repeated_shutdown_cancellation_still_joins_and_persists(api, monkeypatch):
    _, repo, service, cli, project, _, _ = api
    run = await repo.create_run(project.id, RunCreate())
    cli.hold = True
    row = await service.start(project.id, "result=42", run_id=run.id)
    await asyncio.wait_for(cli.started.wait(), 1)
    auditing, release = asyncio.Event(), asyncio.Event()
    original = repo.append_event

    async def blocked_cancelled(run_id, kind, message, data=None):
        if kind == "sandbox_cancelled":
            auditing.set()
            await release.wait()
        return await original(run_id, kind, message, data)

    monkeypatch.setattr(repo, "append_event", blocked_cancelled)
    closing = asyncio.create_task(service.shutdown())
    await asyncio.wait_for(auditing.wait(), 1)
    closing.cancel()
    await asyncio.sleep(0)
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()
    release.set()
    await asyncio.gather(closing, return_exceptions=True)
    assert closing.cancelled()
    assert not service._tasks and cli.active == 0
    assert (await service.get(project.id, row.id)).status == "cancelled"
    events = [event.kind for event in await repo.list_events(run.id)]
    assert events.count("sandbox_cancelled") == 1


async def test_shutdown_owns_start_that_is_still_committing_its_queue_row(api, monkeypatch):
    _, repo, service, cli, project, _, _ = api
    cli.hold = True
    committing, release = asyncio.Event(), asyncio.Event()
    original_begin = repo.sessions.begin

    @asynccontextmanager
    async def gated_begin():
        async with original_begin() as session:
            yield session
            if asyncio.current_task().get_name().startswith("sandbox-start:"):
                committing.set()
                await release.wait()

    monkeypatch.setattr(repo.sessions, "begin", gated_begin)
    starting = asyncio.create_task(
        service.start(project.id, "result=42"), name="start-during-shutdown"
    )
    await asyncio.wait_for(committing.wait(), 1)
    closing = asyncio.create_task(service.shutdown())
    await asyncio.sleep(0)
    assert not closing.done()
    release.set()
    row = await starting
    await asyncio.wait_for(closing, 1)
    assert not service._tasks and cli.active == 0
    assert (await service.get(project.id, row.id)).status == "cancelled"


def gate_committed_start(repo, monkeypatch):
    committed, release = asyncio.Event(), asyncio.Event()
    original_begin = repo.sessions.begin

    @asynccontextmanager
    async def after_commit():
        async with original_begin() as session:
            yield session
        # This boundary is after the real transaction committed, not before it.
        # The caller must not be able to cancel away producer registration.
        if (
            asyncio.current_task().get_name().startswith("sandbox-start:")
            and not committed.is_set()
        ):
            committed.set()
            await release.wait()

    monkeypatch.setattr(repo.sessions, "begin", after_commit)
    return committed, release


@pytest.mark.parametrize("entry", ["http", "tool"])
async def test_cancelled_start_caller_cannot_orphan_committed_execution(api, monkeypatch, entry):
    client, repo, service, cli, project, _, _ = api
    run = await repo.create_run(project.id, RunCreate())
    cli.hold = True
    committed, release = gate_committed_start(repo, monkeypatch)
    action = (
        client.post(
            f"/api/projects/{project.id}/sandbox/executions",
            headers=AUTH,
            json={"code": "result=42", "run_id": run.id},
        )
        if entry == "http"
        else service.execute(project.id, "result=42", run_id=run.id)
    )
    request = asyncio.create_task(action)
    try:
        await asyncio.wait_for(committed.wait(), 1)
        assert len(await service.list(project.id)) == 1
        assert not service._tasks and service._start_operations
        request.cancel()
        await asyncio.sleep(0)
        request.cancel()
        assert not request.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(request, 1)
        rows = await service.list(project.id)
        assert len(rows) == 1 and not service._start_operations
        if entry == "http":
            await asyncio.wait_for(cli.started.wait(), 1)
            assert rows[0].id in service._tasks
            assert (await service.get(project.id, rows[0].id)).status == "running"
            await service.cancel(project.id, rows[0].id)
        final = await service.get(project.id, rows[0].id)
        assert final.status == "cancelled" and final.result.cleanup_confirmed
        assert not service._tasks and cli.active == 0
        assert sum(args[0] == "start" for args, _ in cli.calls) <= 1
        events = [
            event
            for event in await repo.list_events(run.id)
            if event.data.get("execution_id") == final.id
        ]
        assert sum(event.kind == "sandbox_cancelled" for event in events) == 1
    finally:
        release.set()
        await asyncio.gather(request, return_exceptions=True)


async def test_shutdown_drains_disconnected_committed_start_before_snapshot(api, monkeypatch):
    client, repo, service, cli, project, _, _ = api
    cli.hold = True
    run = await repo.create_run(project.id, RunCreate())
    committed, release = gate_committed_start(repo, monkeypatch)
    closing_entered = asyncio.Event()
    request = asyncio.create_task(
        client.post(
            f"/api/projects/{project.id}/sandbox/executions",
            headers=AUTH,
            json={"code": "result=42", "run_id": run.id},
        )
    )
    closing = None
    try:
        await asyncio.wait_for(committed.wait(), 1)
        request.cancel()

        async def close_service():
            closing_entered.set()
            await service.shutdown()

        closing = asyncio.create_task(close_service())
        await asyncio.wait_for(closing_entered.wait(), 1)
        assert service._closing and not closing.done() and service._start_operations
        denied = await client.post(
            f"/api/projects/{project.id}/sandbox/executions",
            headers=AUTH,
            json={"code": "result=99"},
        )
        assert denied.status_code == 503 and len(await service.list(project.id)) == 1
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(request, 1)
        await asyncio.wait_for(closing, 1)
        final = (await service.list(project.id))[0]
        assert final.status == "cancelled" and final.result.cleanup_confirmed
        assert not service._start_operations and not service._tasks and cli.active == 0
        assert [event.kind for event in await repo.list_events(run.id)].count(
            "sandbox_cancelled"
        ) == 1
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (request, closing) if task is not None), return_exceptions=True
        )


async def test_concurrent_starts_obey_capacity_and_concurrent_cancel_frees_one_slot(api):
    client, repo, service, cli, project, _, settings = api
    cli.hold = True
    run = await repo.create_run(project.id, RunCreate())
    endpoint = f"/api/projects/{project.id}/sandbox/executions"
    capacity = max(8, settings.sandbox_max_concurrent * 8)
    results = await asyncio.gather(
        *[
            client.post(endpoint, headers=AUTH, json={"code": f"result={index}", "run_id": run.id})
            for index in range(capacity + 1)
        ]
    )
    assert sum(response.status_code == 202 for response in results) == capacity
    rejected = [response for response in results if response.status_code != 202]
    assert len(rejected) == 1 and rejected[0].status_code == 429
    assert rejected[0].headers["retry-after"] == "5"
    await asyncio.wait_for(cli.started.wait(), 1)
    assert len(service._tasks) == capacity and not service._start_operations
    assert cli.active == cli.peak == settings.sandbox_max_concurrent
    queued = next(row for row in await service.list(project.id, 100) if row.status == "queued")
    cancellations = await asyncio.gather(
        *[client.post(endpoint + f"/{queued.id}/cancel", headers=AUTH) for _ in range(2)]
    )
    assert any(response.status_code == 200 for response in cancellations)
    assert all(response.status_code in (200, 409) for response in cancellations)
    events = [
        event
        for event in await repo.list_events(run.id)
        if event.data.get("execution_id") == queued.id
    ]
    assert sum(event.kind == "sandbox_cancelled" for event in events) == 1
    assert len(service._tasks) == capacity - 1
    replacement = await client.post(endpoint, headers=AUTH, json={"code": "result=99"})
    assert replacement.status_code == 202 and len(service._tasks) == capacity
    assert len(await service.list(project.id, 100)) == capacity + 1
    assert cli.peak == settings.sandbox_max_concurrent
