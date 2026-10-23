"""HTTP authentication/cursor limits and project-lock ownership regressions."""

import asyncio

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException

from signaldesk.a2a_client import create_a2a_delegation_router
from signaldesk.a2a_server import create_a2a_app, create_agent_card_router
from signaldesk.agui import create_agui_router
from signaldesk.api import create_router
from signaldesk.auth import authenticate_bearer
from signaldesk.config import Settings
from signaldesk.jobs import JobManager
from signaldesk.memory_api import create_memory_router
from signaldesk.repository import MAX_EVENT_CURSOR, ActiveRunError, Repository
from signaldesk.sandbox_api import create_sandbox_router
from signaldesk.schemas import ProjectCreate, RunCreate

AUTH = {"Authorization": "Bearer fixture-token"}
PROTECTED_PATHS = [
    "/api/projects",
    "/api/projects/missing/memory",
    "/api/projects/missing/sandbox/executions",
    "/api/runs/missing/ag-ui",
    "/.well-known/agent-card.json",
    "/a2a/.well-known/agent-card.json",
    "/api/a2a/delegations/missing",
    "/api/projects/missing/a2a/delegations",
]


@pytest_asyncio.fixture
async def boundary_api(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/boundaries.db",
        api_token="fixture-token",
    )
    repo = Repository(settings)
    await repo.initialize()

    async def runner(*_args, **_kwargs):
        await asyncio.Event().wait()

    jobs = JobManager(repo, runner)
    app = FastAPI()
    app.include_router(create_router(repo, jobs, settings))
    app.include_router(create_memory_router(repo, None, settings))
    app.include_router(create_sandbox_router(repo, None, settings))
    app.include_router(create_agui_router(repo, settings))
    app.include_router(create_agent_card_router(settings))
    app.include_router(create_a2a_delegation_router(repo, settings))
    app.mount("/a2a", create_a2a_app(repo, jobs, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        try:
            yield client, repo, settings
        finally:
            await jobs.shutdown()
            await repo.close()


@pytest.mark.parametrize("path", PROTECTED_PATHS)
@pytest.mark.parametrize(
    "authorization",
    [None, b"Basic fixture-token", b"Bearer wrong-token", b"Bearer malformed-\xe9"],
)
async def test_all_authentication_entries_return_401_for_invalid_credentials(
    boundary_api, path, authorization
):
    client, _, _ = boundary_api
    headers = [] if authorization is None else [(b"authorization", authorization)]
    response = await client.get(path, headers=headers)
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert "malformed" not in response.text and "wrong-token" not in response.text


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("/api/projects", 200),
        ("/api/projects/missing/memory", 404),
        ("/api/projects/missing/sandbox/executions", 404),
        ("/api/runs/missing/ag-ui", 404),
        ("/.well-known/agent-card.json", 200),
        ("/a2a/.well-known/agent-card.json", 200),
    ],
)
async def test_ascii_token_and_unconfigured_local_auth_still_work(boundary_api, path, status):
    client, _, settings = boundary_api
    assert (await client.get(path, headers=AUTH)).status_code == status
    settings.api_token = ""
    assert (await client.get(path)).status_code == status
    assert (
        await client.get(path, headers=[(b"authorization", b"Bearer malformed-\xe9")])
    ).status_code == status


def test_shared_auth_compares_ascii_bytes_and_rejects_non_ascii_configuration(monkeypatch):
    comparisons = []

    def compare(left, right):
        comparisons.append((left, right))
        return left == right

    monkeypatch.setattr("signaldesk.auth.secrets.compare_digest", compare)
    authenticate_bearer("fixture-token", "bEaReR", "fixture-token")
    assert comparisons == [(b"fixture-token", b"fixture-token")]
    with pytest.raises(HTTPException) as error:
        authenticate_bearer("invalid-\ud800", "Bearer", "fixture-token")
    assert error.value.status_code == 401
    with pytest.raises(HTTPException) as error:
        authenticate_bearer("fixture-token", "Bearer", "invalid-\ud800")
    assert error.value.status_code == 401


@pytest.mark.parametrize("after", [-1, MAX_EVENT_CURSOR + 1, 2**53 - 1, 2**80])
@pytest.mark.parametrize("suffix", ["events", "ag-ui"])
async def test_event_cursors_reject_out_of_database_range(boundary_api, after, suffix):
    client, _, _ = boundary_api
    response = await client.get(
        f"/api/runs/missing/{suffix}", params={"after": str(after)}, headers=AUTH
    )
    assert response.status_code == 422


@pytest.mark.parametrize("after", [0, MAX_EVENT_CURSOR])
async def test_maximum_database_cursor_can_read_json_events(boundary_api, after):
    client, repo, _ = boundary_api
    project = await repo.create_project(project_input())
    run = await repo.create_run(project.id, RunCreate())
    response = await client.get(
        f"/api/runs/{run.id}/events", params={"after": str(after)}, headers=AUTH
    )
    assert response.status_code == 200
    assert (response.json() == []) is (after == MAX_EVENT_CURSOR)
    stream = await client.get("/api/runs/missing/ag-ui", params={"after": str(after)}, headers=AUTH)
    assert stream.status_code == 404


@pytest.mark.parametrize("cursor", [str(MAX_EVENT_CURSOR + 1), str(2**80), "-1"])
async def test_agui_header_cursor_has_same_database_limit(boundary_api, cursor):
    client, _, _ = boundary_api
    response = await client.get(
        "/api/runs/missing/ag-ui", headers={**AUTH, "Last-Event-ID": cursor}
    )
    assert response.status_code == 422


async def test_agui_header_accepts_maximum_database_cursor(boundary_api):
    client, _, _ = boundary_api
    response = await client.get(
        "/api/runs/missing/ag-ui", headers={**AUTH, "Last-Event-ID": str(MAX_EVENT_CURSOR)}
    )
    assert response.status_code == 404


def project_input(name="Boundary fixture"):
    return ProjectCreate(
        name=name, sources=[{"name": "Public fixture", "url": "https://example.com"}]
    )


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():  # noqa: ASYNC110 - observe ownership without mutating production code
            await asyncio.sleep(0)


async def test_missing_project_edits_do_not_retain_permanent_locks(boundary_api):
    client, repo, _ = boundary_api
    payload = project_input().model_dump(mode="json")
    for index in range(100):
        response = await client.put(f"/api/projects/missing-{index}", headers=AUTH, json=payload)
        assert response.status_code == 404
    assert not repo._project_locks


async def test_cancelled_lock_waiter_does_not_orphan_owner_or_surviving_waiter(boundary_api):
    _, repo, _ = boundary_api
    project = await repo.create_project(project_input())
    edit = create = None
    try:
        async with repo._locked_project_session(project.id):
            edit = asyncio.create_task(repo.update_project(project.id, project_input("Edited")))
            create = asyncio.create_task(repo.create_run(project.id, RunCreate()))
            await until(lambda: repo._project_locks[project.id][1] == 3)
            lock = repo._project_locks[project.id][0]
            edit.cancel()
            with pytest.raises(asyncio.CancelledError):
                await edit
            assert repo._project_locks[project.id] == (lock, 2)
            assert not create.done()
        run = await asyncio.wait_for(create, timeout=3)
        assert run.status == "queued"
        assert not repo._project_locks
        with pytest.raises(ActiveRunError):
            await repo.create_run(project.id, RunCreate())
        assert not repo._project_locks
    finally:
        for task in (edit, create):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (edit, create) if task is not None), return_exceptions=True
        )


async def test_cancelled_lock_owner_releases_registry_and_sqlite_writer(boundary_api):
    _, repo, _ = boundary_api
    project = await repo.create_project(project_input())
    entered = asyncio.Event()

    async def hold():
        async with repo._locked_project_session(project.id):
            entered.set()
            await asyncio.Event().wait()

    owner = asyncio.create_task(hold())
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        assert not repo._project_locks
        run = await asyncio.wait_for(repo.create_run(project.id, RunCreate()), timeout=3)
        assert run.status == "queued" and not repo._project_locks
    finally:
        if not owner.done():
            owner.cancel()
        await asyncio.gather(owner, return_exceptions=True)
