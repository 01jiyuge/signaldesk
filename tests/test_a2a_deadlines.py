"""Total remote deadlines with real HTTP, durable ACKs and cooperative cleanup."""

import asyncio
import json
import math
import socket
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
import uvicorn
from a2a.types import Task, TaskState, TaskStatus
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from signaldesk.a2a_client import A2ADelegate, create_a2a_delegation_router
from signaldesk.a2a_server import create_a2a_app, create_agent_card_router
from signaldesk.config import Settings
from signaldesk.errors import PublicTaskError
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate


@asynccontextmanager
async def serve(factory):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(uvicorn.Config(factory(url), log_level="error", lifespan="off"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield url
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)
        sock.close()


@pytest_asyncio.fixture
async def system(tmp_path):
    def settings(name):
        return Settings(
            _env_file=None,
            database_url=f"sqlite+aiosqlite:///{tmp_path}/{name}.db",
            model="",
            openai_api_key="",
            reranker_api_key="",
            api_token="fixture-only-token",
            a2a_timeout_seconds=1,
            a2a_remote_agents={},
            a2a_remote_tokens={},
            a2a_remote_projects={},
        )

    local_settings, remote_settings = settings("local"), settings("remote")
    local_repo, remote_repo = Repository(local_settings), Repository(remote_settings)
    await local_repo.initialize()
    await remote_repo.initialize()

    async def project(repo):
        return await repo.create_project(
            ProjectCreate(
                name="Deadline fixture",
                sources=[{"name": "Fixture", "url": "https://fixture.example/"}],
            )
        )

    local_project, remote_project = await project(local_repo), await project(remote_repo)

    async def blocking_runner(*args, **kwargs):
        await asyncio.Event().wait()

    jobs = JobManager(remote_repo, blocking_runner)
    try:
        yield SimpleNamespace(
            local_settings=local_settings,
            remote_settings=remote_settings,
            local_repo=local_repo,
            remote_repo=remote_repo,
            local_project=local_project,
            remote_project=remote_project,
            jobs=jobs,
        )
    finally:
        await jobs.shutdown()
        await local_repo.close()
        await remote_repo.close()


class SlowResponse:
    """A real peer trickles bytes faster than HTTPX's per-read inactivity limit."""

    def __init__(self, app):
        self.app, self.slow_method = app, None
        self.calls = []

    async def __call__(self, scope, receive, send):
        method = "card"
        if scope.get("method") == "POST":
            body = bytearray()
            while True:
                message = await receive()
                body.extend(message.get("body", b""))
                if not message.get("more_body", False):
                    break
            method = json.loads(body)["method"]
            self.calls.append(method)
            original_receive = receive
            replayed = False

            async def receive():
                nonlocal replayed
                if not replayed:
                    replayed = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await original_receive()

        async def slow_send(message):
            body = message.get("body", b"")
            if method == self.slow_method and message["type"] == "http.response.body" and body:
                width = math.ceil(len(body) / 15)
                for start in range(0, len(body), width):
                    await asyncio.sleep(0.1)
                    await send(
                        {
                            **message,
                            "body": body[start : start + width],
                            "more_body": start + width < len(body)
                            or message.get("more_body", False),
                        }
                    )
            else:
                await send(message)

        await self.app(scope, receive, slow_send)


@pytest.mark.parametrize("phase", ["card", "SendMessage", "GetTask", "CancelTask"])
async def test_manual_routes_bound_card_and_rpc_slow_drip_without_duplicate_post(system, phase):
    remote = None

    def remote_factory(url):
        nonlocal remote
        settings = system.remote_settings.model_copy(update={"a2a_public_url": url + "/a2a/"})
        app = FastAPI()
        app.mount("/a2a", create_a2a_app(system.remote_repo, system.jobs, settings))
        app.include_router(create_agent_card_router(settings))
        remote = SlowResponse(app)
        return remote

    async with serve(remote_factory) as remote_url:
        settings = system.local_settings.model_copy(
            update={
                "a2a_remote_agents": {"remote": remote_url + "/a2a/"},
                "a2a_remote_tokens": {"remote": "fixture-only-token"},
            }
        )
        app = FastAPI()
        app.include_router(create_a2a_delegation_router(system.local_repo, settings))

        @app.exception_handler(PublicTaskError)
        async def error(request, exc):
            return JSONResponse(
                {"code": exc.code, "detail": str(exc)},
                status_code=503 if exc.retryable else 422,
            )

        async with serve(lambda url: app) as local_url:
            async with httpx.AsyncClient(
                base_url=local_url,
                timeout=4,
                headers={"Authorization": "Bearer fixture-only-token"},
            ) as client:
                endpoint = f"/api/projects/{system.local_project.id}/a2a/delegations"
                payload = {
                    "remote": "remote",
                    "remote_project_id": system.remote_project.id,
                    "message_id": "one-remote-post",
                    "mode": "demo",
                }
                if phase in {"GetTask", "CancelTask"}:
                    initial = await client.post(endpoint, json=payload)
                    assert initial.status_code == 202
                    delegation_id = initial.json()["id"]
                    remote.slow_method = phase
                    if phase == "GetTask":
                        response = await client.get(f"/api/a2a/delegations/{delegation_id}")
                    else:
                        response = await client.post(f"/api/a2a/delegations/{delegation_id}/cancel")
                else:
                    remote.slow_method = phase
                    response = await client.post(endpoint, json=payload)
                assert response.status_code == 503, response.text
                assert response.json()["code"] == "a2a_timeout"
                assert "fixture-only-token" not in response.text
                assert remote.calls.count("SendMessage") == (0 if phase == "card" else 1)
                assert len(await system.remote_repo.list_runs()) == (0 if phase == "card" else 1)
                saved = await A2ADelegate(settings, system.local_repo).list(system.local_project.id)
                assert len(saved) == 1
                if phase in {"card", "SendMessage"}:
                    assert saved[0]["status"] == "PENDING" and saved[0]["task"] is None
                if phase == "CancelTask":
                    assert saved[0]["status"] != "TASK_STATE_CANCELED"
                    remote.slow_method = None
                    retried = await client.post(f"/api/a2a/delegations/{delegation_id}/cancel")
                    assert retried.status_code == 200
                    assert retried.json()["status"] == "TASK_STATE_CANCELED"
                    assert remote.calls.count("CancelTask") == 1


class ControlledClient:
    def __init__(self, project_id):
        self.project_id, self.send_count = project_id, 0

    def task(self, state=TaskState.TASK_STATE_WORKING):
        return Task(
            id="remote-task",
            context_id=self.project_id,
            status=TaskStatus(state=state),
        )

    async def send_message(self, params):
        self.send_count += 1
        yield SimpleNamespace(HasField=lambda field: field == "task", task=self.task())

    async def get_task(self, params):
        return self.task()

    async def cancel_task(self, params):
        return self.task(TaskState.TASK_STATE_CANCELED)


def controlled_delegate(system, monkeypatch, *, block_close=False):
    settings = system.local_settings.model_copy(
        update={"a2a_remote_agents": {"remote": "http://fixture.invalid/a2a/"}}
    )
    delegate = A2ADelegate(settings, system.local_repo)
    client = ControlledClient(system.remote_project.id)
    closing, closed = asyncio.Event(), asyncio.Event()
    state = {"block_close": block_close, "release_close": asyncio.Event()}

    async def close():
        closing.set()
        try:
            if state["block_close"]:
                await state["release_close"].wait()
        finally:
            closed.set()

    @asynccontextmanager
    async def connected(remote):
        try:
            yield client
        finally:
            await delegate._close_owned(close)

    monkeypatch.setattr(delegate, "_connected_client", connected)
    return delegate, client, closing, closed, state


async def send(delegate, system):
    return await delegate.send(
        "remote",
        system.remote_project.id,
        local_project_id=system.local_project.id,
        message_id="one-remote-post",
        mode="demo",
    )


@pytest.mark.parametrize("operation", ["send", "get", "cancel"])
async def test_confirmed_result_survives_deadline_in_client_close(system, monkeypatch, operation):
    delegate, client, closing, closed, state = controlled_delegate(system, monkeypatch)
    initial = await send(delegate, system) if operation != "send" else None
    state["block_close"] = True
    closing.clear()
    closed.clear()
    asyncio.get_running_loop().call_later(1.05, state["release_close"].set)
    if operation == "send":
        result = await asyncio.wait_for(send(delegate, system), 3)
    elif operation == "get":
        result = await asyncio.wait_for(delegate.get(initial["id"]), 3)
    else:
        result = await asyncio.wait_for(delegate.cancel(initial["id"]), 3)
    assert closing.is_set() and closed.is_set()
    assert client.send_count == 1
    expected = "TASK_STATE_CANCELED" if operation == "cancel" else "TASK_STATE_WORKING"
    assert result["status"] == expected
    saved = await delegate.local_record(system.local_project.id, result["id"])
    assert saved["status"] == expected and saved["task"]["id"] == "remote-task"
    assert not any(task.get_name() == "a2a-client-close" for task in asyncio.all_tasks())


async def test_caller_cancel_during_close_joins_cleanup_and_preserves_ack(system, monkeypatch):
    delegate, client, closing, closed, state = controlled_delegate(
        system, monkeypatch, block_close=True
    )
    operation = asyncio.create_task(send(delegate, system))
    try:
        await asyncio.wait_for(closing.wait(), 2)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not closed.is_set()
        state["release_close"].set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(operation, 2)
        assert closed.is_set() and client.send_count == 1
        saved = await delegate.list(system.local_project.id)
        assert len(saved) == 1 and saved[0]["task"]["id"] == "remote-task"
        assert not any(task.get_name() == "a2a-client-close" for task in asyncio.all_tasks())
    finally:
        state["release_close"].set()
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def test_close_grace_cancels_and_joins_cooperative_close_without_losing_ack(
    system, monkeypatch
):
    delegate, client, _, closed, _ = controlled_delegate(system, monkeypatch, block_close=True)
    monkeypatch.setattr("signaldesk.a2a_client.CLIENT_CLOSE_GRACE_SECONDS", 0.05)
    result = await asyncio.wait_for(send(delegate, system), 2)
    assert result["task"]["id"] == "remote-task" and client.send_count == 1
    assert closed.is_set()
    assert not any(task.get_name() == "a2a-client-close" for task in asyncio.all_tasks())


@pytest.mark.parametrize("stop", ["deadline", "cancel_twice"])
async def test_ack_commit_is_owned_until_registered_without_reposting(system, monkeypatch, stop):
    delegate, client, _, closed, _ = controlled_delegate(system, monkeypatch)
    original = delegate.bindings.record_task
    committed, release = asyncio.Event(), asyncio.Event()

    async def delayed_ack(*args, **kwargs):
        result = await original(*args, **kwargs)
        committed.set()
        await release.wait()
        return result

    monkeypatch.setattr(delegate.bindings, "record_task", delayed_ack)
    operation = asyncio.create_task(send(delegate, system))
    try:
        await asyncio.wait_for(committed.wait(), 2)
        if stop == "cancel_twice":
            operation.cancel()
            await asyncio.sleep(0)
            operation.cancel()
            await asyncio.sleep(0)
        else:
            elapsed = asyncio.Event()
            asyncio.get_running_loop().call_later(1.05, elapsed.set)
            await elapsed.wait()
        assert not operation.done()
        release.set()
        if stop == "cancel_twice":
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(operation, 2)
        else:
            result = await asyncio.wait_for(operation, 2)
            assert result["task"]["id"] == "remote-task"
        assert closed.is_set() and client.send_count == 1
        saved = await delegate.list(system.local_project.id)
        assert len(saved) == 1 and saved[0]["task"]["id"] == "remote-task"
        retried = await send(delegate, system)
        assert retried["id"] == saved[0]["id"] and client.send_count == 1
        assert not any(task.get_name().startswith("a2a-record-") for task in asyncio.all_tasks())
    finally:
        release.set()
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
