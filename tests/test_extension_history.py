"""Extension history across restarts, inserts between pages and legacy schemas."""

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import insert, inspect

from signaldesk.a2a_client import A2ADelegate, create_a2a_delegation_router
from signaldesk.a2a_store import A2ARequestStore, delegations
from signaldesk.config import Settings
from signaldesk.history import HistoryCursorError, decode_cursor
from signaldesk.repository import Repository
from signaldesk.sandbox import SandboxExecution, SandboxService
from signaldesk.sandbox_api import create_sandbox_router
from signaldesk.schemas import ProjectCreate

AUTH = {"Authorization": "Bearer history-fixture"}
BASE = datetime(2026, 10, 23, tzinfo=UTC)


async def project(repo, name="History"):
    return await repo.create_project(
        ProjectCreate(name=name, sources=[{"name": "Source", "url": "https://example.com"}])
    )


async def seed(repo, owner, resource, index, *, at=BASE):
    identifier = str(UUID(int=index + 1))
    async with repo.sessions.begin() as session:
        if resource == "sandbox":
            session.add(
                SandboxExecution(
                    id=identifier,
                    project_id=owner.id,
                    run_id=None,
                    status="completed",
                    image="fixture-only",
                    code_hash="0" * 64,
                    result=None,
                    created_at=at,
                    updated_at=at,
                )
            )
        else:
            await session.execute(
                insert(delegations).values(
                    id=identifier,
                    message_id=f"fixture-message-{index}",
                    payload_hash="0" * 64,
                    project_id=owner.id,
                    run_id=None,
                    remote="offline",
                    endpoint="https://never-called.fixture/a2a/",
                    remote_project_id="target",
                    status="PENDING",
                    task=None,
                    created_at=at,
                )
            )
    return identifier


@pytest_asyncio.fixture
async def system(tmp_path, monkeypatch):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/history.db",
        api_token="history-fixture",
    )
    repo = Repository(settings)
    await repo.initialize()
    owner, other = await project(repo), await project(repo, "Other")
    sandbox = SandboxService(repo, settings)
    await sandbox.initialize()

    def no_network(*args, **kwargs):
        raise AssertionError("Local history and saved selection must work with remotes offline")

    monkeypatch.setattr(A2ADelegate, "_client", no_network)
    app = FastAPI()
    app.include_router(create_sandbox_router(repo, sandbox, settings))
    app.include_router(create_a2a_delegation_router(repo, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        try:
            yield repo, owner, other, client
        finally:
            await sandbox.shutdown()
            await repo.close()


def endpoint(owner, resource):
    suffix = "sandbox/executions" if resource == "sandbox" else "a2a/delegations"
    return f"/api/projects/{owner.id}/{suffix}"


@pytest.mark.parametrize("resource", ["sandbox", "a2a"])
async def test_keyset_walk_survives_new_insert_and_equal_times_without_duplicate_or_loss(
    system, resource
):
    repo, own, other, client = system
    ids = [
        await seed(repo, own, resource, i, at=BASE + timedelta(seconds=i // 2)) for i in range(6)
    ]
    await seed(repo, other, resource, 20, at=BASE + timedelta(days=1))
    path = endpoint(own, resource)
    assert (await client.get(path + "/page")).status_code == 401
    first = (await client.get(path + "/page?limit=2", headers=AUTH)).json()
    assert [x["id"] for x in first["items"]] == ids[::-1][:2]
    assert first["has_more"] and first["next_cursor"]
    # A new first-page item must not shift offset-based pages and duplicate rows.
    inserted = await seed(repo, own, resource, 21, at=BASE + timedelta(days=2))
    collected = [x["id"] for x in first["items"]]
    cursor = first["next_cursor"]
    while cursor:
        page = (
            await client.get(path + "/page", params={"limit": 2, "cursor": cursor}, headers=AUTH)
        ).json()
        collected.extend(x["id"] for x in page["items"])
        cursor = page["next_cursor"]
        assert page["has_more"] == (cursor is not None)
    assert collected == ids[::-1] and inserted not in collected
    # Selecting an older item restores from a local project-scoped endpoint.
    detail = await client.get(path + "/" + ids[0], headers=AUTH)
    assert detail.status_code == 200 and detail.json()["id"] == ids[0]
    assert (
        await client.get(endpoint(other, resource) + "/" + ids[0], headers=AUTH)
    ).status_code == 404


@pytest.mark.parametrize("resource", ["sandbox", "a2a"])
async def test_page_cursor_scope_limits_and_legacy_array_contract(system, resource):
    repo, own, other, client = system
    for i in range(3):
        await seed(repo, own, resource, i)
    path = endpoint(own, resource)
    page = (await client.get(path + "/page?limit=1", headers=AUTH)).json()
    cursor = page["next_cursor"]
    assert (
        await client.get(
            endpoint(other, resource) + "/page", params={"cursor": cursor}, headers=AUTH
        )
    ).status_code == 422
    other_kind = "a2a" if resource == "sandbox" else "sandbox"
    assert (
        await client.get(
            endpoint(own, other_kind) + "/page", params={"cursor": cursor}, headers=AUTH
        )
    ).status_code == 422
    for params in ({"limit": 0}, {"limit": 101}, {"cursor": "not valid!"}, {"cursor": "a" * 513}):
        assert (await client.get(path + "/page", params=params, headers=AUTH)).status_code == 422
    assert isinstance((await client.get(path, headers=AUTH)).json(), list)
    assert (
        await client.get(path.replace(own.id, "missing") + "/page", headers=AUTH)
    ).status_code == 404
    assert (await client.get(endpoint(other, resource) + "/page", headers=AUTH)).json() == {
        "items": [],
        "next_cursor": None,
        "has_more": False,
    }


async def test_unknown_legacy_times_sort_last_and_are_never_invented_or_exposed_with_secrets(
    system,
):
    repo, own, _, client = system
    unknown = [await seed(repo, own, "a2a", i, at=None) for i in range(3)]
    known = await seed(repo, own, "a2a", 10)
    path = endpoint(own, "a2a")
    ids, times, cursor = [], [], None
    while True:
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        response = await client.get(path + "/page", params=params, headers=AUTH)
        page = response.json()
        assert (
            "never-called.fixture" not in response.text and "fixture-message" not in response.text
        )
        ids.extend(x["id"] for x in page["items"])
        times.extend(x["created_at"] for x in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert ids == [known, *unknown[::-1]]
    assert times[0] is not None and times[1:] == [None] * 3


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"v": True},
        {"v": 1, "project": "p", "resource": "a2a", "at": "2026-10-23", "id": str(UUID(int=1))},
        {
            "v": 1,
            "project": "p",
            "resource": "a2a",
            "at": "9999-12-31T23:59:59-23:59",
            "id": str(UUID(int=1)),
        },
    ],
)
def test_invalid_cursor_payloads_are_public_validation_errors(payload):
    import base64

    token = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    with pytest.raises(HistoryCursorError):
        decode_cursor(token, project_id="p", resource="a2a")


LEGACY_TABLE = """
CREATE TABLE a2a_delegations (
 id VARCHAR(36) PRIMARY KEY, message_id VARCHAR(128) NOT NULL UNIQUE,
 payload_hash VARCHAR(64) NOT NULL, project_id VARCHAR(36) NOT NULL REFERENCES projects(id),
 run_id VARCHAR(36) REFERENCES runs(id), remote VARCHAR(80) NOT NULL,
 endpoint VARCHAR(2048) NOT NULL, remote_project_id VARCHAR(128) NOT NULL,
 task JSON, status VARCHAR(40) NOT NULL
)
"""


async def test_legacy_database_upgrade_is_additive_reentrant_and_preserves_bindings(tmp_path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as legacy:
        legacy.execute(LEGACY_TABLE)
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{path}")
    repo = Repository(settings)
    await repo.initialize()
    own = await project(repo)
    old_id = str(uuid4())
    async with repo.engine.begin() as connection:
        await connection.exec_driver_sql(
            "INSERT INTO a2a_delegations (id,message_id,payload_hash,project_id,remote,endpoint,remote_project_id,status) VALUES (?,?,?,?,?,?,?,?)",
            (
                old_id,
                "legacy-message",
                "old-hash",
                own.id,
                "old-remote",
                "https://old.fixture/a2a/",
                "old-project",
                "PENDING",
            ),
        )
    reopened = Repository(settings)
    try:
        await asyncio.gather(
            A2ARequestStore(repo).initialize(), A2ARequestStore(reopened).initialize()
        )
        store = A2ARequestStore(reopened)
        old = await store.get_delegation(old_id)
        assert (
            old["created_at"] is None
            and old["message_id"] == "legacy-message"
            and old["payload_hash"] == "old-hash"
        )
        new = await store.prepare_delegation(
            project_id=own.id,
            run_id=None,
            remote="new",
            endpoint="https://new.fixture/a2a/",
            remote_project_id="new",
            message_id="new-message",
            mode="demo",
            demo_revision=1,
        )
        assert new["created_at"] is not None
        page = await store.page_delegations(own.id)
        assert [x["id"] for x in page["items"]] == [new["id"], old_id]
        async with repo.engine.connect() as connection:
            indexes = await connection.run_sync(lambda c: inspect(c).get_indexes("a2a_delegations"))
            assert any(x["name"] == "ix_a2a_delegations_history" for x in indexes)
    finally:
        await reopened.close()
        await repo.close()
