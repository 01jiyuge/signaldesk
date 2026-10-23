import asyncio
import socket
from contextlib import asynccontextmanager

import httpx
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from langgraph.checkpoint.memory import InMemorySaver

from signaldesk.a2a_client import A2ADelegate, create_a2a_delegation_router
from signaldesk.a2a_server import create_a2a_app, create_agent_card_router
from signaldesk.config import Settings
from signaldesk.errors import PublicTaskError
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository
from signaldesk.retrieval import Retriever
from signaldesk.schemas import ProjectCreate
from signaldesk.workflow import Workflow


def settings_for(tmp_path, name="remote", **updates):
    base = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/{name}.db",
        api_token="fixture-remote-token",
        model="",
        openai_api_key="",
        reranker_api_key="",
        reranker_provider="none",
        enable_vector_search=False,
        enable_fulltext_search=False,
    )
    return base.model_copy(
        update={
            "a2a_public_url": "http://remote.fixture/a2a/",
            "a2a_timeout_seconds": 5,
            "a2a_remote_agents": {},
            "a2a_remote_tokens": {},
            **updates,
        }
    )


def application(repo, jobs, settings):
    app = FastAPI()
    app.mount("/a2a", create_a2a_app(repo, jobs, settings))
    app.include_router(create_agent_card_router(settings))
    return app


@pytest_asyncio.fixture
async def protocol_system(tmp_path):
    settings = settings_for(tmp_path)
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="A2A fixture",
            sources=[{"name": "Northstar", "url": "https://northstar.example/pricing"}],
        )
    )
    started = asyncio.Event()

    async def blocking(run_id, resume=False):
        started.set()
        await asyncio.Event().wait()

    jobs = JobManager(repo, blocking)
    app = application(repo, jobs, settings)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://remote.fixture",
            headers={"Authorization": "Bearer fixture-remote-token"},
        ) as http:
            yield repo, jobs, app, http, project, settings, started
    finally:
        await jobs.shutdown()
        await repo.close()


def send_body(project_id, message_id="fixture-message", **updates):
    return {
        "jsonrpc": "2.0",
        "id": "rpc-request",
        "method": "SendMessage",
        "params": {
            "message": {
                "messageId": message_id,
                "contextId": project_id,
                "role": "ROLE_USER",
                "parts": [
                    {
                        "data": {
                            "project_id": project_id,
                            "mode": "demo",
                            "demo_revision": 1,
                            **updates,
                        },
                        "mediaType": "application/json",
                    }
                ],
            },
            "configuration": {"returnImmediately": True},
        },
    }


async def rpc(http, method, **params):
    return (
        await http.post(
            "/a2a/",
            json={"jsonrpc": "2.0", "id": "fixture-request", "method": method, "params": params},
            headers={"A2A-Version": "0.3" if "/" in method else "1.0"},
        )
    ).json()


async def test_official_card_send_get_cancel_auth_and_persistent_retry(protocol_system):
    repo, jobs, app, http, project, settings, started = protocol_system
    for path in ["/.well-known/agent-card.json", "/a2a/.well-known/agent-card.json"]:
        card = (await http.get(path)).json()
        assert card["supportedInterfaces"][0]["protocolBinding"] == "JSONRPC"
        assert card["securitySchemes"]["bearer"]["httpAuthSecurityScheme"]["scheme"] == "bearer"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://remote.fixture"
    ) as anonymous:
        assert (await anonymous.post("/a2a/", json=send_body(project.id))).status_code == 401
        assert (await anonymous.get("/.well-known/agent-card.json")).status_code == 401
    http.headers["A2A-Version"] = "1.0"
    result = (await http.post("/a2a/", json=send_body(project.id))).json()
    task = result["result"]["task"]
    assert task["contextId"] == project.id
    task_id = task["id"]
    await asyncio.wait_for(started.wait(), 2)
    assert (await rpc(http, "GetTask", id=task_id))["result"]["id"] == task_id
    assert (await http.post("/a2a/", json=send_body(project.id))).json()["result"]["task"][
        "id"
    ] == task_id
    assert len(await repo.list_runs()) == 1
    assert next(
        event for event in await repo.list_events(task_id) if event.kind == "a2a_origin"
    ).data == {"delegation_depth": 1}
    conflict = (await http.post("/a2a/", json=send_body(project.id, demo_revision=2))).json()
    assert conflict["error"]["code"] == -32602
    cancelled = await rpc(http, "CancelTask", id=task_id)
    assert cancelled["result"]["status"]["state"] == "TASK_STATE_CANCELED"
    assert (await rpc(http, "CancelTask", id=task_id))["result"]["id"] == task_id
    assert (await rpc(http, "GetTask", id="unknown-task"))["error"]["code"] == -32001
    assert (await rpc(http, "GetTask", id=task_id, tenant="other"))["error"]["code"] == -32001
    assert (await http.post("/a2a/", content=b"x" * 65537)).status_code == 413


async def test_sdk_v03_compatibility_supports_requested_method_names(protocol_system):
    repo, jobs, app, http, project, settings, started = protocol_system
    old = {
        "jsonrpc": "2.0",
        "id": "legacy",
        "method": "message/send",
        "params": {
            "message": {
                "kind": "message",
                "messageId": "legacy-message",
                "role": "user",
                "contextId": project.id,
                "parts": [
                    {
                        "kind": "data",
                        "data": {"project_id": project.id, "mode": "demo", "demo_revision": 1},
                    }
                ],
            },
            "configuration": {"blocking": False},
        },
    }
    response = (await http.post("/a2a/", json=old)).json()
    task_id = response["result"]["id"]
    assert response["result"]["kind"] == "task"
    assert (await rpc(http, "tasks/get", id=task_id))["result"]["id"] == task_id
    assert (await rpc(http, "tasks/cancel", id=task_id))["result"]["status"]["state"] == "canceled"


@asynccontextmanager
async def running_server(app, factory=None):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    if factory:
        app = factory(f"http://127.0.0.1:{port}")
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", lifespan="off")
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)
        sock.close()


async def test_real_http_two_instances_delegate_and_complete_demo_graph(tmp_path):
    remote_settings = settings_for(tmp_path)
    remote_repo = Repository(remote_settings)
    await remote_repo.initialize()
    retriever = Retriever(remote_repo, remote_settings)
    await retriever.initialize()
    workflow = Workflow(remote_repo, remote_settings, InMemorySaver(), retriever)
    jobs = JobManager(remote_repo, workflow.run)
    remote_project = await remote_repo.create_project(
        ProjectCreate(
            name="Remote fixture",
            sources=[{"name": "Northstar", "url": "https://northstar.example/pricing"}],
        )
    )
    local_settings = settings_for(tmp_path, "local", api_token="fixture-local-token")
    local_repo = Repository(local_settings)
    await local_repo.initialize()
    local_project = await local_repo.create_project(
        ProjectCreate(
            name="Local fixture",
            sources=[{"name": "Orbit", "url": "https://orbit.example/changelog"}],
        )
    )
    try:
        async with running_server(
            None,
            factory=lambda url: application(
                remote_repo,
                jobs,
                remote_settings.model_copy(update={"a2a_public_url": url + "/a2a/"}),
            ),
        ) as remote_url:
            local_settings = local_settings.model_copy(
                update={
                    "a2a_remote_agents": {"researcher": remote_url + "/a2a/"},
                    "a2a_remote_tokens": {"researcher": "fixture-remote-token"},
                }
            )
            local_app = FastAPI()
            local_app.include_router(create_a2a_delegation_router(local_repo, local_settings))

            @local_app.exception_handler(PublicTaskError)
            async def public_error(request, exc):
                return JSONResponse({"code": exc.code, "detail": str(exc)}, status_code=422)

            async with (
                running_server(local_app) as local_url,
                httpx.AsyncClient(
                    base_url=local_url, headers={"Authorization": "Bearer fixture-local-token"}
                ) as http,
            ):
                endpoint = f"/api/projects/{local_project.id}/a2a/delegations"
                payload = {
                    "remote": "researcher",
                    "remote_project_id": remote_project.id,
                    "message_id": "across-http",
                    "mode": "demo",
                }
                sent = await http.post(endpoint, json=payload)
                assert sent.status_code == 202, sent.text
                delegation_id = sent.json()["id"]
                task_id = sent.json()["task"]["id"]
                assert delegation_id != task_id
                async with asyncio.timeout(5):
                    while True:
                        response = await http.get(f"/api/a2a/delegations/{delegation_id}")
                        assert response.status_code == 200, response.text
                        if response.json()["status"] == "TASK_STATE_COMPLETED":
                            break
                        await asyncio.sleep(0.02)
                report = response.json()["task"]["artifacts"][0]["parts"][0]["data"]
                assert report["mode"] == "demo" and report["evidence"]
                ended = await http.post(f"/api/a2a/delegations/{delegation_id}/cancel")
                assert ended.status_code == 409
                assert ended.json()["detail"] == "远端任务已结束，无法取消。"
                assert (await http.post(endpoint, json=payload)).json()["id"] == delegation_id
                assert len(await remote_repo.list_runs()) == 1
                assert await local_repo.list_runs() == []
                assert (await http.get(f"/api/a2a/delegations/{task_id}")).status_code == 404
                assert (
                    await http.post(f"/api/a2a/delegations/{task_id}/cancel")
                ).status_code == 404
                assert (
                    await http.post("/api/projects/missing/a2a/delegations", json=payload)
                ).status_code == 404
                missing = await http.post(
                    endpoint,
                    json={
                        **payload,
                        "message_id": "missing-remote",
                        "remote_project_id": "missing",
                    },
                )
                assert missing.status_code == 404, missing.text
                rejected = await http.post(
                    endpoint, json={**payload, "remote": "http://127.0.0.1:1/"}
                )
                assert rejected.json()["code"] == "a2a_remote_not_allowed"
                denied = await http.post(
                    endpoint, json=payload, headers={"Authorization": "Bearer fixture-wrong-token"}
                )
                assert denied.status_code == 401
    finally:
        await jobs.shutdown()
        await retriever.close()
        await remote_repo.close()
        await local_repo.close()


@pytest.mark.parametrize("stop", ["timeout", "cancel"])
async def test_delegate_wait_cancels_remote_task_and_keeps_durable_registration(
    protocol_system, tmp_path, stop
):
    remote_repo, jobs, app, http, remote_project, remote_settings, started = protocol_system
    local_settings = settings_for(
        tmp_path, "wait-local", a2a_remote_projects={"researcher": remote_project.id}
    )
    local_repo = Repository(local_settings)
    await local_repo.initialize()
    local_project = await local_repo.create_project(
        ProjectCreate(
            name="Local wait fixture",
            sources=[{"name": "Orbit", "url": "https://orbit.example/changelog"}],
        )
    )
    from signaldesk.schemas import RunCreate

    local_run = await local_repo.create_run(local_project.id, RunCreate())
    try:
        async with running_server(
            None,
            factory=lambda url: application(
                remote_repo,
                jobs,
                remote_settings.model_copy(update={"a2a_public_url": url + "/a2a/"}),
            ),
        ) as remote_url:
            local_settings = local_settings.model_copy(
                update={
                    "a2a_remote_agents": {"researcher": remote_url + "/a2a/"},
                    "a2a_remote_tokens": {"researcher": "fixture-remote-token"},
                }
            )
            delegate = A2ADelegate(local_settings, local_repo)
            waiter = asyncio.create_task(
                delegate.delegate_wait(
                    "researcher",
                    remote_project.id,
                    local_project_id=local_project.id,
                    local_run_id=local_run.id,
                    message_id=f"wait-{stop}",
                    mode="demo",
                    timeout_seconds=0.7 if stop == "timeout" else 5,
                )
            )
            await asyncio.wait_for(started.wait(), 2)
            if stop == "cancel":
                waiter.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiter
            else:
                with pytest.raises(PublicTaskError) as caught:
                    await waiter
                assert caught.value.code == "a2a_timeout"
            remote_run = (await remote_repo.list_runs())[0]
            assert remote_run.status == "cancelled" and not jobs.is_active(remote_run.id)
            # Reconstructing the client/repository proves the public ID is not
            # an in-memory alias for an arbitrary remote task ID.
            from sqlalchemy import select

            from signaldesk.a2a_store import delegations

            async with local_repo.sessions() as session:
                record = (await session.execute(select(delegations))).mappings().one()
                delegation_id = record["id"]
            reopened = Repository(local_repo.settings)
            try:
                observed = await A2ADelegate(local_settings, reopened).get(delegation_id)
                assert observed["status"] == "TASK_STATE_CANCELED"
                assert observed["task"]["id"] == remote_run.id
            finally:
                await reopened.close()
            await local_repo.append_event(
                local_run.id, "a2a_origin", "Fixture inbound origin", {"delegation_depth": 1}
            )
            with pytest.raises(PublicTaskError) as recursive:
                await delegate.delegate_wait(
                    "researcher",
                    remote_project.id,
                    local_project_id=local_project.id,
                    local_run_id=local_run.id,
                    message_id="recursive",
                    mode="demo",
                )
            assert recursive.value.code == "a2a_recursion_blocked"
            with pytest.raises(PublicTaskError) as unauthorized_project:
                await delegate.delegate_wait(
                    "researcher",
                    "other-project",
                    local_project_id=local_project.id,
                    local_run_id=local_run.id,
                    message_id="wrong-target",
                    mode="demo",
                )
            assert unauthorized_project.value.code == "a2a_remote_project_not_allowed"
    finally:
        await local_repo.close()


@pytest.mark.parametrize(
    "bad_card", ["foreign-interface", "redirect", "invalid-json", "provider-error"]
)
async def test_remote_card_cannot_redirect_credentials_and_errors_are_safe(
    protocol_system, monkeypatch, bad_card
):
    from google.protobuf.json_format import MessageToDict

    from signaldesk.a2a_server import build_agent_card

    repo, jobs, app, http, project, settings, started = protocol_system
    settings = settings.model_copy(
        update={
            "a2a_remote_agents": {"researcher": "https://allowed.fixture/a2a/"},
            "a2a_remote_tokens": {"researcher": "fixture-independent-remote-token"},
        }
    )
    seen = []

    def transport(request):
        seen.append(request)
        assert request.headers["authorization"] == "Bearer fixture-independent-remote-token"
        if request.method == "GET":
            if bad_card == "redirect":
                return httpx.Response(302, headers={"location": "https://foreign.fixture/card"})
            if bad_card == "invalid-json":
                return httpx.Response(
                    200, text="private-provider-response fixture-independent-remote-token"
                )
            card_url = (
                "https://foreign.fixture/execute"
                if bad_card == "foreign-interface"
                else "https://allowed.fixture/a2a/"
            )
            card = build_agent_card(settings.model_copy(update={"a2a_public_url": card_url}))
            return httpx.Response(200, json=MessageToDict(card))
        assert bad_card == "provider-error"
        return httpx.Response(
            500, text="private-provider-response fixture-independent-remote-token"
        )

    original = httpx.AsyncClient

    def client_factory(**kwargs):
        from signaldesk.a2a_client import BoundedResponseTransport

        kwargs["transport"] = BoundedResponseTransport(httpx.MockTransport(transport))
        return original(**kwargs)

    monkeypatch.setattr("signaldesk.a2a_client.httpx.AsyncClient", client_factory)
    with pytest.raises(PublicTaskError) as caught:
        await A2ADelegate(settings, repo).send(
            "researcher",
            "remote-fixture-project",
            local_project_id=project.id,
            message_id=f"bad-{bad_card}",
        )
    assert caught.value.code == "a2a_remote_failed"
    assert "fixture-independent-remote-token" not in str(caught.value)
    assert "private-provider-response" not in str(caught.value)
    assert all(str(request.url).startswith("https://allowed.fixture/a2a/") for request in seen)
    assert len(seen) == (2 if bad_card == "provider-error" else 1)


@pytest.mark.parametrize("phase", ["card", "task", "declared-length", "compressed"])
async def test_remote_response_is_bounded_before_sdk_read_or_json_parsing(
    protocol_system, monkeypatch, phase
):
    from google.protobuf.json_format import MessageToDict

    from signaldesk.a2a_client import MAX_REMOTE_RESPONSE_BYTES, BoundedResponseTransport
    from signaldesk.a2a_server import build_agent_card

    repo, jobs, app, http, project, settings, started = protocol_system
    settings = settings.model_copy(
        update={
            "a2a_public_url": "https://bounded.fixture/a2a/",
            "a2a_remote_agents": {"bounded": "https://bounded.fixture/a2a/"},
        }
    )

    class LongStream(httpx.AsyncByteStream):
        def __init__(self):
            self.count, self.closed = 0, False

        async def __aiter__(self):
            for _ in range(1000):
                self.count += 1
                yield b"x" * 65536

        async def aclose(self):
            self.closed = True

    stream = LongStream()
    parsed_methods = []
    original_json = httpx.Response.json

    def response_json(response, *args, **kwargs):
        parsed_methods.append(response.request.method)
        return original_json(response, *args, **kwargs)

    monkeypatch.setattr(httpx.Response, "json", response_json)

    def transport(request):
        assert request.headers["accept-encoding"] == "identity"
        if phase == "task" and request.method == "GET":
            return httpx.Response(200, json=MessageToDict(build_agent_card(settings)))
        headers = {"content-type": "application/json"}
        if phase == "declared-length":
            headers["content-length"] = str(MAX_REMOTE_RESPONSE_BYTES + 1)
        if phase == "compressed":
            headers["content-encoding"] = "gzip"
        return httpx.Response(200, headers=headers, stream=stream)

    original_client = httpx.AsyncClient

    def client_factory(**kwargs):
        kwargs["transport"] = BoundedResponseTransport(httpx.MockTransport(transport))
        return original_client(**kwargs)

    monkeypatch.setattr("signaldesk.a2a_client.httpx.AsyncClient", client_factory)
    with pytest.raises(PublicTaskError) as caught:
        await A2ADelegate(settings, repo).send(
            "bounded", "remote-project", local_project_id=project.id, message_id=f"bounded-{phase}"
        )
    assert caught.value.code == (
        "a2a_response_encoding_unsupported" if phase == "compressed" else "a2a_response_too_large"
    )
    assert stream.closed
    assert stream.count == (
        0 if phase in {"declared-length", "compressed"} else MAX_REMOTE_RESPONSE_BYTES // 65536 + 1
    )
    assert parsed_methods == (["GET"] if phase == "task" else [])


async def test_exact_response_limit_still_uses_real_official_sdk_flow(protocol_system, monkeypatch):
    import json

    from google.protobuf.json_format import MessageToDict

    from signaldesk.a2a_client import MAX_REMOTE_RESPONSE_BYTES, BoundedResponseTransport
    from signaldesk.a2a_server import build_agent_card

    repo, jobs, app, http, project, settings, started = protocol_system
    settings = settings.model_copy(
        update={
            "a2a_public_url": "https://bounded.fixture/a2a/",
            "a2a_remote_agents": {"bounded": "https://bounded.fixture/a2a/"},
        }
    )
    card = json.dumps(MessageToDict(build_agent_card(settings))).encode()
    padded_card = card + b" " * (MAX_REMOTE_RESPONSE_BYTES - len(card))

    def transport(request):
        if request.method == "GET":
            return httpx.Response(200, content=padded_card)
        rpc_id = json.loads(request.content)["id"]
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {
                    "task": {
                        "id": "remote-task",
                        "contextId": "remote-project",
                        "status": {"state": "TASK_STATE_SUBMITTED"},
                    }
                },
            },
        )

    original_client = httpx.AsyncClient

    def client_factory(**kwargs):
        kwargs["transport"] = BoundedResponseTransport(httpx.MockTransport(transport))
        return original_client(**kwargs)

    monkeypatch.setattr("signaldesk.a2a_client.httpx.AsyncClient", client_factory)
    response = await A2ADelegate(settings, repo).send(
        "bounded", "remote-project", local_project_id=project.id, message_id="exact-limit"
    )
    assert response["task"]["id"] == "remote-task"


async def test_local_delegation_list_restores_pending_history_without_network_or_private_fields(
    protocol_system, monkeypatch
):
    from signaldesk.a2a_store import A2ARequestStore

    repo, jobs, app, http, project, settings, started = protocol_system
    store = A2ARequestStore(repo)
    other = await repo.create_project(
        ProjectCreate(
            name="Other fixture",
            sources=[{"name": "Orbit", "url": "https://orbit.example/changelog"}],
        )
    )
    records = []
    for index, owner in enumerate([project, project, other]):
        records.append(
            await store.prepare_delegation(
                project_id=owner.id,
                run_id=None,
                remote="offline",
                endpoint="https://private-endpoint.fixture/a2a/",
                remote_project_id="target",
                message_id=f"private-message-{index}",
                mode="demo",
                demo_revision=1,
            )
        )
    await store.record_task(
        records[0]["id"],
        {
            "id": "registered-remote-task",
            "contextId": "target",
            "status": {"state": "TASK_STATE_COMPLETED"},
        },
    )

    async def forbidden_network(*args, **kwargs):
        raise AssertionError("Listing local history must not contact a remote")

    monkeypatch.setattr(A2ADelegate, "_client", forbidden_network)
    local_app = FastAPI()
    local_app.include_router(create_a2a_delegation_router(repo, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=local_app),
        base_url="http://local.fixture",
        headers={"Authorization": "Bearer fixture-remote-token"},
    ) as local:
        result = await local.get(f"/api/projects/{project.id}/a2a/delegations")
        assert result.status_code == 200
        data = result.json()
        assert {item["id"] for item in data} == {item["id"] for item in records[:2]}
        assert {item["status"] for item in data} == {"TASK_STATE_COMPLETED", "PENDING"}
        assert next(item for item in data if item["status"] == "PENDING")["task"] is None
        assert all(
            set(item)
            == {
                "id",
                "project_id",
                "run_id",
                "remote",
                "remote_project_id",
                "status",
                "task",
                "created_at",
            }
            for item in data
        )
        assert (
            "private-endpoint.fixture" not in result.text and "private-message" not in result.text
        )
        assert "fixture-remote-token" not in result.text
        first = await local.get(f"/api/projects/{project.id}/a2a/delegations?limit=1&offset=0")
        second = await local.get(f"/api/projects/{project.id}/a2a/delegations?limit=1&offset=1")
        assert first.json()[0]["id"] != second.json()[0]["id"]
        assert (await local.get("/api/projects/missing/a2a/delegations")).status_code == 404
        assert (
            await local.get(
                f"/api/projects/{project.id}/a2a/delegations",
                headers={"Authorization": "Bearer wrong"},
            )
        ).status_code == 401


async def test_a2a_task_projection_hides_legacy_raw_error(protocol_system):
    from signaldesk.a2a_store import A2ARequestStore
    from signaldesk.schemas import RunCreate

    repo, jobs, app, http, project, settings, started = protocol_system
    run, _ = await A2ARequestStore(repo).create_or_get(project.id, RunCreate(), "legacy-failure")
    await repo.update_run(
        run.id,
        status="failed",
        error="Authorization: Bearer fixture-legacy-secret; private provider response",
    )
    response = await rpc(http, "GetTask", id=run.id)
    assert response["result"]["status"]["state"] == "TASK_STATE_FAILED"
    rendered = str(response)
    assert "fixture-legacy-secret" not in rendered and "private provider response" not in rendered
    assert "任务执行失败" in rendered


@pytest.mark.parametrize(
    "state",
    [
        "TASK_STATE_FAILED",
        "TASK_STATE_CANCELED",
        "TASK_STATE_INPUT_REQUIRED",
        "TASK_STATE_AUTH_REQUIRED",
        "TASK_STATE_REJECTED",
        "TASK_STATE_UNSPECIFIED",
        "TASK_STATE_COMPLETED",
    ],
)
async def test_delegate_wait_only_returns_success_for_completed(
    protocol_system, monkeypatch, state
):
    from signaldesk.schemas import RunCreate

    repo, jobs, app, http, project, settings, started = protocol_system
    local_run = await repo.create_run(project.id, RunCreate())
    settings = settings.model_copy(update={"a2a_remote_projects": {"researcher": "remote-project"}})
    delegate = A2ADelegate(settings, repo)

    async def scripted_send(*args, **kwargs):
        return {"id": "fixture-delegation", "status": state}

    monkeypatch.setattr(delegate, "send", scripted_send)
    call = delegate.delegate_wait(
        "researcher",
        "remote-project",
        local_project_id=project.id,
        local_run_id=local_run.id,
        message_id="strict-state",
        mode="demo",
    )
    if state == "TASK_STATE_COMPLETED":
        assert (await call)["status"] == state
    else:
        with pytest.raises(PublicTaskError) as caught:
            await call
        assert caught.value.code == "a2a_remote_task_failed" and state in caught.value.suggestion


async def test_official_sdk_cancel_race_becomes_safe_http_conflict(protocol_system, monkeypatch):
    import json

    from google.protobuf.json_format import MessageToDict

    import signaldesk.a2a_client as client_module
    from signaldesk.a2a_server import build_agent_card

    repo, jobs, app, http, project, settings, started = protocol_system
    settings = settings.model_copy(
        update={
            "a2a_public_url": "https://race.fixture/a2a/",
            "a2a_remote_agents": {"race": "https://race.fixture/a2a/"},
        }
    )
    delegate = A2ADelegate(settings, repo)
    record = await delegate.bindings.prepare_delegation(
        project_id=project.id,
        run_id=None,
        remote="race",
        endpoint="https://race.fixture/a2a/",
        remote_project_id="remote-project",
        message_id="cancel-race",
        mode="demo",
        demo_revision=1,
    )
    task = {
        "id": "remote-race-task",
        "contextId": "remote-project",
        "status": {"state": "TASK_STATE_WORKING"},
    }
    await delegate.bindings.record_task(record["id"], task)
    calls = []

    def transport(request):
        if request.method == "GET":
            return httpx.Response(200, json=MessageToDict(build_agent_card(settings)))
        body = json.loads(request.content)
        calls.append(body["method"])
        if body["method"] == "GetTask":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": task})
        assert body["method"] == "CancelTask"
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": body["id"],
                "error": {
                    "code": -32002,
                    "message": "private provider response fixture-remote-secret",
                },
            },
        )

    original_transport = client_module.BoundedResponseTransport
    monkeypatch.setattr(
        client_module,
        "BoundedResponseTransport",
        lambda: original_transport(httpx.MockTransport(transport)),
    )
    with pytest.raises(PublicTaskError) as caught:
        await delegate.cancel(record["id"])
    assert caught.value.code == "a2a_task_not_cancelable"
    assert str(caught.value) == "远端任务已结束，无法取消。"
    local_app = FastAPI()
    local_app.include_router(create_a2a_delegation_router(repo, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=local_app),
        base_url="http://local.fixture",
        headers={"Authorization": "Bearer fixture-remote-token"},
    ) as local:
        response = await local.post(f"/api/a2a/delegations/{record['id']}/cancel")
    assert response.status_code == 409 and response.json()["detail"] == str(caught.value)
    assert (
        "private provider response" not in response.text
        and "fixture-remote-secret" not in response.text
    )
    assert calls == ["GetTask", "CancelTask", "GetTask", "CancelTask"]


async def test_cancellation_after_remote_acceptance_waits_for_same_http_ack(
    protocol_system, tmp_path
):
    from signaldesk.schemas import RunCreate

    remote_repo, jobs, app, http, remote_project, remote_settings, started = protocol_system
    accepted, release_ack = asyncio.Event(), asyncio.Event()
    send_count = 0

    class DelayAcceptedResponse:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        async def __call__(self, scope, receive, send):
            import json

            body = bytearray()
            method = None

            async def read():
                nonlocal method, send_count
                message = await receive()
                if message["type"] == "http.request":
                    body.extend(message.get("body", b""))
                    if not message.get("more_body", False) and body:
                        method = json.loads(body).get("method")
                        if method == "SendMessage":
                            send_count += 1
                return message

            async def write(message):
                if method == "SendMessage" and message["type"] == "http.response.start":
                    # The server has created/submitted the task. Keep the
                    # actual HTTP acknowledgement from reaching the client.
                    accepted.set()
                    await release_ack.wait()
                await send(message)

            await self.wrapped(scope, read, write)

    local_settings = settings_for(
        tmp_path, "delayed-ack", a2a_remote_projects={"researcher": remote_project.id}
    )
    local_repo = Repository(local_settings)
    await local_repo.initialize()
    local_project = await local_repo.create_project(
        ProjectCreate(
            name="Local ACK fixture",
            sources=[{"name": "Orbit", "url": "https://orbit.example/changelog"}],
        )
    )
    local_run = await local_repo.create_run(local_project.id, RunCreate())
    waiter = None
    try:
        async with running_server(
            None,
            factory=lambda url: DelayAcceptedResponse(
                application(
                    remote_repo,
                    jobs,
                    remote_settings.model_copy(update={"a2a_public_url": url + "/a2a/"}),
                )
            ),
        ) as remote_url:
            local_settings = local_settings.model_copy(
                update={
                    "a2a_remote_agents": {"researcher": remote_url + "/a2a/"},
                    "a2a_remote_tokens": {"researcher": "fixture-remote-token"},
                }
            )
            delegate = A2ADelegate(local_settings, local_repo)
            waiter = asyncio.create_task(
                delegate.delegate_wait(
                    "researcher",
                    remote_project.id,
                    local_project_id=local_project.id,
                    local_run_id=local_run.id,
                    message_id="cancel-before-http-ack",
                    mode="demo",
                )
            )
            await asyncio.wait_for(accepted.wait(), 2)
            await asyncio.wait_for(started.wait(), 2)
            assert (await remote_repo.list_runs())[0].status == "running"
            # Cancel strictly while send has not received the response; release
            # the same response afterwards, rather than sleeping or retrying.
            waiter.cancel()
            release_ack.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(waiter, 3)
            remote_run = (await remote_repo.list_runs())[0]
            assert remote_run.status == "cancelled"
            assert not jobs.is_active(remote_run.id)
            assert send_count == 1
            records = await delegate.list(local_project.id)
            assert len(records) == 1
            assert records[0]["status"] == "TASK_STATE_CANCELED"
            assert records[0]["task"]["id"] == remote_run.id
    finally:
        release_ack.set()
        if waiter is not None and not waiter.done():
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        await local_repo.close()


@pytest.mark.parametrize("stop", ["cancel-twice", "timeout-then-cancel"])
async def test_delegation_cleanup_owns_send_through_repeated_cancellation(
    protocol_system, monkeypatch, stop
):
    from signaldesk.schemas import RunCreate

    repo, jobs, app, http, project, settings, started = protocol_system
    local_run = await repo.create_run(project.id, RunCreate())
    settings = settings.model_copy(update={"a2a_remote_projects": {"researcher": "remote-project"}})
    delegate = A2ADelegate(settings, repo)
    accepted, release_ack = asyncio.Event(), asyncio.Event()
    cancelling, release_cancel = asyncio.Event(), asyncio.Event()
    owned_sends, calls = [], []

    async def delayed_send(*args, **kwargs):
        calls.append("send")
        owned_sends.append(asyncio.current_task())
        accepted.set()
        await release_ack.wait()
        return {"id": "same-delegation", "status": "TASK_STATE_WORKING"}

    async def delayed_cancel(delegation_id):
        assert delegation_id == "same-delegation"
        calls.append("cancel")
        cancelling.set()
        await release_cancel.wait()
        return {"id": delegation_id, "status": "TASK_STATE_CANCELED"}

    monkeypatch.setattr(delegate, "send", delayed_send)
    monkeypatch.setattr(delegate, "cancel", delayed_cancel)
    waiter = asyncio.create_task(
        delegate.delegate_wait(
            "researcher",
            "remote-project",
            local_project_id=project.id,
            local_run_id=local_run.id,
            message_id=stop,
            mode="demo",
            timeout_seconds=0.01 if stop == "timeout-then-cancel" else 5,
        )
    )
    try:
        await asyncio.wait_for(accepted.wait(), 1)
        if stop == "cancel-twice":
            waiter.cancel()
        release_ack.set()
        await asyncio.wait_for(cancelling.wait(), 1)
        # Interrupt cleanup itself. It must remain owned by this invocation,
        # then propagate cancellation rather than returning a timeout failure.
        waiter.cancel()
        release_cancel.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(waiter, 1)
        assert calls == ["send", "cancel"]
        assert len(owned_sends) == 1 and owned_sends[0].done()
        assert not any(
            task.get_name() == f"a2a-cleanup-{local_run.id}" for task in asyncio.all_tasks()
        )
    finally:
        release_ack.set()
        release_cancel.set()
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_unacknowledged_send_is_cancelled_and_drained_after_finite_grace(
    protocol_system, monkeypatch
):
    from signaldesk.schemas import RunCreate

    repo, jobs, app, http, project, settings, started = protocol_system
    local_run = await repo.create_run(project.id, RunCreate())
    settings = settings.model_copy(update={"a2a_remote_projects": {"researcher": "remote-project"}})
    delegate = A2ADelegate(settings, repo)
    accepted, drained = asyncio.Event(), asyncio.Event()
    sends = []

    async def no_ack(*args, **kwargs):
        sends.append(asyncio.current_task())
        accepted.set()
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    async def no_remote_id(*args, **kwargs):
        raise AssertionError("Without an acknowledged task ID no cancellation POST is possible")

    monkeypatch.setattr(delegate, "send", no_ack)
    monkeypatch.setattr(delegate, "cancel", no_remote_id)
    monkeypatch.setattr("signaldesk.a2a_client.SEND_ACK_GRACE_SECONDS", 0.01)
    monkeypatch.setattr("signaldesk.a2a_client.SEND_DRAIN_GRACE_SECONDS", 0.1)
    waiter = asyncio.create_task(
        delegate.delegate_wait(
            "researcher",
            "remote-project",
            local_project_id=project.id,
            local_run_id=local_run.id,
            message_id="no-ack",
            mode="demo",
            timeout_seconds=0.01,
        )
    )
    await asyncio.wait_for(accepted.wait(), 1)
    with pytest.raises(PublicTaskError) as caught:
        await asyncio.wait_for(waiter, 1)
    assert caught.value.code == "a2a_timeout"
    assert len(sends) == 1 and sends[0].done() and drained.is_set()
    assert not any(
        task.get_name() in {f"a2a-send-{local_run.id}", f"a2a-cleanup-{local_run.id}"}
        for task in asyncio.all_tasks()
    )


async def test_known_durable_ack_survives_pending_send_client_close(protocol_system, monkeypatch):
    from signaldesk.schemas import RunCreate

    repo, jobs, app, http, project, settings, started = protocol_system
    local_run = await repo.create_run(project.id, RunCreate())
    settings = settings.model_copy(update={"a2a_remote_projects": {"researcher": "remote-project"}})
    delegate = A2ADelegate(settings, repo)
    closing, closed = asyncio.Event(), asyncio.Event()
    calls, sends = [], []
    acknowledged = {"id": "durable-before-close", "status": "TASK_STATE_WORKING"}

    async def closing_send(*args, **kwargs):
        calls.append("send")
        sends.append(asyncio.current_task())
        # send records this result after the DB commit, before leaving its
        # SDK/client context. Closing the context can still be asynchronous.
        kwargs["_acknowledged"].set_result(acknowledged)
        closing.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    async def cancel_known(delegation_id):
        assert delegation_id == acknowledged["id"]
        calls.append("cancel")
        return {**acknowledged, "status": "TASK_STATE_CANCELED"}

    monkeypatch.setattr(delegate, "send", closing_send)
    monkeypatch.setattr(delegate, "cancel", cancel_known)
    waiter = asyncio.create_task(
        delegate.delegate_wait(
            "researcher",
            "remote-project",
            local_project_id=project.id,
            local_run_id=local_run.id,
            message_id="close-after-ack",
            mode="demo",
        )
    )
    await asyncio.wait_for(closing.wait(), 1)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(waiter, 1)
    assert calls == ["send", "cancel"]
    assert len(sends) == 1 and sends[0].done() and closed.is_set()


@asynccontextmanager
async def controlled_a2a_remote(repo, jobs, settings, on_send, *, on_get=None, on_cancel=None):
    def application_for(url):
        configured = settings.model_copy(update={"a2a_public_url": url + "/a2a/"})
        server = create_a2a_app(repo, jobs, configured)
        server.state.bridge.on_message_send = on_send
        if on_get is not None:
            server.state.bridge.on_get_task = on_get
        if on_cancel is not None:
            server.state.bridge.on_cancel_task = on_cancel
        app = FastAPI()
        app.mount("/a2a", server)
        app.include_router(create_agent_card_router(configured))
        return app

    async with running_server(None, factory=application_for) as url:
        yield url


@pytest.mark.parametrize("lost_ack", [False, True])
async def test_concurrent_delegates_send_one_request_and_confirmed_retry_is_local(
    protocol_system, monkeypatch, lost_ack
):
    from a2a.types import Task, TaskState, TaskStatus
    from a2a.utils.errors import InternalError

    from signaldesk.a2a_store import IdempotencyConflict

    repo, jobs, app, http, project, settings, started = protocol_system
    accepted, release_ack, second_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    remote_tasks = []

    # This official-SDK remote deliberately does not deduplicate message IDs.
    # The A2A specification permits that behavior, so the local retry contract
    # must not depend on SignalDesk's own incoming-message deduplication.
    async def send_remote(params, context):
        task = Task(
            id=f"external-task-{len(remote_tasks) + 1}",
            context_id="external-project",
            status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
        )
        remote_tasks.append(task.id)
        accepted.set()
        await release_ack.wait()
        if lost_ack:
            raise InternalError("Fixture accepted work without delivering an acknowledgement")
        return task

    tasks = []
    try:
        async with controlled_a2a_remote(repo, jobs, settings, send_remote) as remote_url:
            configured = settings.model_copy(
                update={
                    "a2a_remote_agents": {"external": remote_url + "/a2a/"},
                    "a2a_remote_tokens": {"external": "fixture-remote-token"},
                }
            )
            first, second = A2ADelegate(configured, repo), A2ADelegate(configured, repo)
            original_serialize = second.bindings.serialize_send

            @asynccontextmanager
            async def announce_waiter(message_id):
                second_entered.set()
                async with original_serialize(message_id) as gate:
                    yield gate

            monkeypatch.setattr(second.bindings, "serialize_send", announce_waiter)
            kwargs = dict(
                local_project_id=project.id, mode="demo", message_id="same-concurrent-message"
            )
            tasks.append(asyncio.create_task(first.send("external", "external-project", **kwargs)))
            await asyncio.wait_for(accepted.wait(), 1)
            tasks.append(asyncio.create_task(second.send("external", "external-project", **kwargs)))
            await asyncio.wait_for(second_entered.wait(), 1)
            release_ack.set()
            results = await asyncio.gather(*tasks, return_exceptions=lost_ack)
            assert len(remote_tasks) == 1
            if lost_ack:
                assert all(isinstance(value, PublicTaskError) for value in results)
                assert {value.code for value in results} == {
                    "a2a_remote_failed",
                    "a2a_task_pending",
                }
                pending = await first.list(project.id)
                assert (
                    len(pending) == 1
                    and pending[0]["status"] == "PENDING"
                    and pending[0]["task"] is None
                )
            else:
                assert results[0] == results[1]
                assert results[0]["task"]["id"] == remote_tasks[0]
            assert repo._a2a_send_gates == {}
        if not lost_ack:
            # The acknowledgement is durable: even with the remote stopped and
            # a new Delegate instance, retrying returns the same canonical task.
            replay = await A2ADelegate(configured, repo).send(
                "external", "external-project", **kwargs
            )
            assert replay == results[0]
            with pytest.raises(IdempotencyConflict):
                await A2ADelegate(configured, repo).send(
                    "external", "external-project", **kwargs, demo_revision=2
                )
    finally:
        release_ack.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("finish", ["cancel", "complete"])
async def test_delayed_http_get_cannot_overwrite_canonical_terminal_task(protocol_system, finish):
    from a2a.types import Artifact, Part, Task, TaskState, TaskStatus

    repo, jobs, app, http, project, settings, started = protocol_system
    stale_ready, release_stale = asyncio.Event(), asyncio.Event()
    current = TaskState.TASK_STATE_WORKING
    get_calls = 0

    def snapshot():
        task = Task(
            id="same-external-task", context_id="external-project", status=TaskStatus(state=current)
        )
        if current == TaskState.TASK_STATE_COMPLETED:
            task.artifacts.append(
                Artifact(artifact_id="final-report", parts=[Part(text="Finished report evidence")])
            )
        return task

    async def send_remote(params, context):
        return snapshot()

    async def get_remote(params, context):
        nonlocal get_calls
        get_calls += 1
        task = snapshot()
        if get_calls == 1:
            stale_ready.set()
            await release_stale.wait()
        return task

    async def cancel_remote(params, context):
        nonlocal current
        current = TaskState.TASK_STATE_CANCELED
        return snapshot()

    stale = None
    try:
        async with controlled_a2a_remote(
            repo, jobs, settings, send_remote, on_get=get_remote, on_cancel=cancel_remote
        ) as remote_url:
            configured = settings.model_copy(
                update={
                    "a2a_remote_agents": {"external": remote_url + "/a2a/"},
                    "a2a_remote_tokens": {"external": "fixture-remote-token"},
                }
            )
            first, second = A2ADelegate(configured, repo), A2ADelegate(configured, repo)
            original = await first.send(
                "external",
                "external-project",
                local_project_id=project.id,
                message_id="terminal-race",
                mode="demo",
            )
            stale = asyncio.create_task(first.get(original["id"]))
            await asyncio.wait_for(stale_ready.wait(), 1)
            if finish == "cancel":
                final = await second.cancel(original["id"])
                expected = "TASK_STATE_CANCELED"
            else:
                current = TaskState.TASK_STATE_COMPLETED
                final = await second.get(original["id"])
                expected = "TASK_STATE_COMPLETED"
            assert final["status"] == expected
            release_stale.set()
            delayed = await stale
            assert delayed == final
            cached = (await first.list(project.id))[0]
            assert cached == final
            if finish == "complete":
                assert (
                    cached["task"]["artifacts"][0]["parts"][0]["text"] == "Finished report evidence"
                )
    finally:
        release_stale.set()
        if stale is not None:
            await asyncio.gather(stale, return_exceptions=True)


@asynccontextmanager
async def inbound_commit_gate(repo, jobs, settings):
    from a2a.utils.errors import InternalError

    committed, release = asyncio.Event(), asyncio.Event()
    requests, committed_runs = [], []

    def make_application(url):
        configured = settings.model_copy(update={"a2a_public_url": url + "/a2a/"})
        server = create_a2a_app(repo, jobs, configured)
        bridge = server.state.bridge
        original_create, original_send = bridge.bindings.create_or_get, bridge.on_message_send

        async def pause_after_commit(*args, **kwargs):
            run, created = await original_create(*args, **kwargs)
            if created:
                committed_runs.append(run.id)
                committed.set()
                await release.wait()
            return run, created

        async def capture_request(params, context):
            requests.append(asyncio.current_task())
            try:
                return await original_send(params, context)
            except asyncio.CancelledError:
                # Simulate a cancelled server request over an actual socket,
                # while returning a controlled SDK error rather than logging
                # an expected ASGI cancellation traceback from this fixture.
                raise InternalError("Controlled disconnected incoming request") from None

        bridge.bindings.create_or_get = pause_after_commit
        bridge.on_message_send = capture_request
        app = FastAPI()
        app.mount("/a2a", server)
        app.include_router(create_agent_card_router(configured))
        return app

    try:
        async with running_server(None, factory=make_application) as url:
            yield url, committed, release, requests, committed_runs
    finally:
        release.set()


async def test_inbound_http_cancel_after_commit_keeps_owned_submission_and_retry(protocol_system):
    repo, jobs, app, http, project, settings, started = protocol_system
    first = None
    async with inbound_commit_gate(repo, jobs, settings) as (
        url,
        committed,
        release,
        requests,
        committed_runs,
    ):
        configured = settings.model_copy(
            update={
                "a2a_remote_agents": {"incoming": url + "/a2a/"},
                "a2a_remote_tokens": {"incoming": "fixture-remote-token"},
            }
        )
        client = A2ADelegate(configured, repo)
        kwargs = dict(
            local_project_id=project.id, message_id="cancelled-inbound-create", mode="demo"
        )
        try:
            first = asyncio.create_task(client.send("incoming", project.id, **kwargs))
            await asyncio.wait_for(committed.wait(), 2)
            assert len(committed_runs) == 1 and jobs._operations
            assert (await repo.get_run(committed_runs[0])).status == "queued"
            requests[0].cancel()
            release.set()
            with pytest.raises(PublicTaskError) as caught:
                await asyncio.wait_for(first, 2)
            assert caught.value.code == "a2a_remote_failed"
            await asyncio.wait_for(started.wait(), 2)
            run_id = committed_runs[0]
            producer = jobs._tasks[run_id]
            assert jobs.is_active(run_id) and (await repo.get_run(run_id)).status == "running"
            retry = await client.send("incoming", project.id, **kwargs)
            assert retry["task"]["id"] == run_id
            assert len(await repo.list_runs()) == 1 and jobs._tasks[run_id] is producer
            assert not jobs._operations and not jobs._run_locks
        finally:
            release.set()
            if first is not None:
                await asyncio.gather(first, return_exceptions=True)


async def test_inbound_http_shutdown_joins_committed_submission_and_refuses_new_requests(
    protocol_system, monkeypatch
):
    repo, jobs, app, http, project, settings, started = protocol_system
    closing = asyncio.Event()
    original_shutdown = jobs._shutdown

    async def observe_shutdown():
        closing.set()
        await original_shutdown()

    monkeypatch.setattr(jobs, "_shutdown", observe_shutdown)
    first = shutdown = None
    async with inbound_commit_gate(repo, jobs, settings) as (
        url,
        committed,
        release,
        requests,
        committed_runs,
    ):
        async with httpx.AsyncClient(
            base_url=url,
            headers={"Authorization": "Bearer fixture-remote-token", "A2A-Version": "1.0"},
        ) as wire:
            try:
                first = asyncio.create_task(
                    wire.post("/a2a/", json=send_body(project.id, "shutdown-inbound-create"))
                )
                await asyncio.wait_for(committed.wait(), 2)
                shutdown = asyncio.create_task(jobs.shutdown())
                await asyncio.wait_for(closing.wait(), 2)
                assert jobs._closing and not shutdown.done() and jobs._operations
                denied = (
                    await wire.post("/a2a/", json=send_body(project.id, "after-shutdown-started"))
                ).json()
                assert (
                    denied["error"]["code"] == -32603 and "正在关闭" in denied["error"]["message"]
                )
                assert len(await repo.list_runs()) == 1
                release.set()
                response, _ = await asyncio.wait_for(asyncio.gather(first, shutdown), 2)
                assert response.json()["error"]["code"] == -32603
                failed = await repo.get_run(committed_runs[0])
                assert failed.status == "failed" and "任务服务正在关闭" in failed.error
                assert not started.is_set()
                assert not jobs._operations and not jobs._tasks and not jobs._run_locks
            finally:
                release.set()
                await asyncio.gather(
                    *(task for task in (first, shutdown) if task is not None),
                    return_exceptions=True,
                )


async def test_inbound_blocking_poll_does_not_hold_lifecycle_transition_open(protocol_system):
    repo, jobs, app, http, project, settings, started = protocol_system
    request = None
    async with running_server(
        None,
        factory=lambda url: application(
            repo, jobs, settings.model_copy(update={"a2a_public_url": url + "/a2a/"})
        ),
    ) as url:
        async with httpx.AsyncClient(
            base_url=url,
            headers={"Authorization": "Bearer fixture-remote-token", "A2A-Version": "1.0"},
        ) as wire:
            body = send_body(project.id, "blocking-outside-operation")
            body["params"]["configuration"]["returnImmediately"] = False
            try:
                request = asyncio.create_task(wire.post("/a2a/", json=body))
                await asyncio.wait_for(started.wait(), 2)
                assert not request.done() and not jobs._operations and not jobs._run_locks
                await asyncio.wait_for(jobs.shutdown(), 2)
                response = (await asyncio.wait_for(request, 2)).json()
                assert response["result"]["task"]["status"]["state"] == "TASK_STATE_FAILED"
                assert (await repo.list_runs())[0].status == "interrupted"
            finally:
                if request is not None:
                    await asyncio.gather(request, return_exceptions=True)
