"""Report-free keyset history and startup recovery on independent SQLite data."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import event, inspect, select, text

from signaldesk.api import create_router
from signaldesk.config import Settings
from signaldesk.db import RUN_HISTORY_INDEX, Run
from signaldesk.history import history_page
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate, RunHistoryPage, RunSummaryRead

AUTH = {"Authorization": "Bearer history-fixture-token"}
SUMMARY_FIELDS = {
    "id",
    "project_id",
    "mode",
    "demo_revision",
    "status",
    "stage",
    "error",
    "created_at",
    "updated_at",
}
BASE_TIME = datetime(2026, 10, 22, tzinfo=UTC)


@pytest_asyncio.fixture
async def history_api(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/runs.db",
        api_token="history-fixture-token",
    )
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(project_input("History"))

    async def runner(*_args, **_kwargs):
        await asyncio.Event().wait()

    jobs = JobManager(repo, runner)
    app = FastAPI()
    app.include_router(create_router(repo, jobs, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        try:
            yield client, repo, jobs, project
        finally:
            await jobs.shutdown()
            await repo.close()


def project_input(name):
    return ProjectCreate(name=name, sources=[{"name": name, "url": "https://example.com/pricing"}])


async def seed_runs(repo, project_id, count, *, first_id=1, ties=False):
    rows = []
    for index in range(count):
        created = BASE_TIME if ties else BASE_TIME + timedelta(seconds=index)
        rows.append(
            Run(
                id=str(UUID(int=first_id + index)),
                project_id=project_id,
                mode="demo",
                demo_revision=1,
                status="completed",
                stage="completed",
                report={"title": f"Report {index}", "evidence": "private-fixture-body-" * 500},
                created_at=created,
                updated_at=created + timedelta(microseconds=123456),
            )
        )
    async with repo.sessions.begin() as session:
        session.add_all(rows)
    return rows


def assert_summary(value):
    assert set(value) == SUMMARY_FIELDS
    assert "report" not in value


async def test_empty_history_contract_auth_and_project_scope(history_api):
    client, _, _, project = history_api
    path = f"/api/projects/{project.id}/runs/page"
    assert (await client.get(path)).status_code == 401
    assert (await client.get(path, headers={"Authorization": "Bearer wrong"})).status_code == 401
    assert (await client.get("/api/projects/missing/runs/page", headers=AUTH)).status_code == 404
    response = await client.get(path, headers=AUTH)
    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "next_cursor": None,
        "has_more": False,
        "active_run": None,
    }
    assert "report" not in RunSummaryRead.model_fields
    assert "active_run" in RunHistoryPage.model_fields


async def test_pages_are_lightweight_and_legacy_details_remain_compatible(history_api):
    client, repo, _, project = history_api
    rows = await seed_runs(repo, project.id, 45)
    path = f"/api/projects/{project.id}/runs/page"
    seen = []
    cursor = None
    page_sizes = []
    while True:
        params = {} if cursor is None else {"cursor": cursor}
        response = await client.get(path, headers=AUTH, params=params)
        assert response.status_code == 200
        page = response.json()
        assert page["active_run"] is None
        assert "private-fixture-body" not in response.text
        assert len(response.content) < 20_000
        for item in page["items"]:
            assert_summary(item)
        page_sizes.append(len(page["items"]))
        seen.extend(item["id"] for item in page["items"])
        if not page["has_more"]:
            assert page["next_cursor"] is None
            break
        cursor = page["next_cursor"]
        assert isinstance(cursor, str) and len(cursor) <= 512
    assert page_sizes == [20, 20, 5]
    assert seen == [row.id for row in reversed(rows)]
    assert len(seen) == len(set(seen))
    detail = await client.get(f"/api/runs/{rows[0].id}", headers=AUTH)
    assert detail.status_code == 200
    assert detail.json()["project_id"] == project.id
    assert detail.json()["report"] == rows[0].report
    legacy = await client.get("/api/runs", headers=AUTH, params={"project_id": project.id})
    assert len(legacy.json()) == 45
    assert all("report" in item for item in legacy.json())


async def test_equal_creation_times_and_inserts_between_pages_are_stable(history_api):
    client, repo, _, project = history_api
    rows = await seed_runs(repo, project.id, 18, ties=True)
    path = f"/api/projects/{project.id}/runs/page"
    first = (await client.get(path, headers=AUTH, params={"limit": 6})).json()
    seen = [item["id"] for item in first["items"]]
    assert seen == [row.id for row in reversed(rows[-6:])]
    # A newly inserted run belongs to a refreshed first page, not the remaining
    # older-key traversal, even if its timestamp ties with existing records.
    inserted = (await seed_runs(repo, project.id, 1, first_id=9999, ties=True))[0]
    cursor = first["next_cursor"]
    while cursor:
        page = (await client.get(path, headers=AUTH, params={"limit": 6, "cursor": cursor})).json()
        seen.extend(item["id"] for item in page["items"])
        cursor = page["next_cursor"]
    assert seen == [row.id for row in reversed(rows)]
    refreshed = (await client.get(path, headers=AUTH, params={"limit": 6})).json()
    assert refreshed["items"][0]["id"] == inserted.id


async def test_active_old_run_is_returned_on_every_page_and_still_blocks_writes(history_api):
    client, repo, _, project = history_api
    rows = await seed_runs(repo, project.id, 25)
    original_first = await repo.page_runs(project.id)
    old = rows[0]
    await repo.update_run(old.id, status="queued", stage="queued")
    other = await repo.create_project(project_input("Other"))
    other_run = (await seed_runs(repo, other.id, 1, first_id=100))[0]
    await repo.update_run(other_run.id, status="running", stage="research")
    path = f"/api/projects/{project.id}/runs/page"
    first = (await client.get(path, headers=AUTH)).json()
    assert old.id not in {item["id"] for item in first["items"]}
    assert first["active_run"]["id"] == old.id
    assert first["active_run"]["status"] == "queued"
    assert_summary(first["active_run"])
    following = (
        await client.get(path, headers=AUTH, params={"cursor": original_first["next_cursor"]})
    ).json()
    assert following["active_run"]["id"] == old.id
    assert [item["id"] for item in following["items"]] == [row.id for row in reversed(rows[:5])]
    # The tiny activity refresh must remain independent of the visible page.
    activity = (await client.get(path, headers=AUTH, params={"limit": 1})).json()
    assert activity["active_run"]["id"] == old.id
    assert (
        await client.post(f"/api/projects/{project.id}/runs", headers=AUTH, json={})
    ).status_code == 409
    assert (
        await client.put(
            f"/api/projects/{project.id}",
            headers=AUTH,
            json=project_input("Edited").model_dump(mode="json"),
        )
    ).status_code == 409
    await repo.update_run(old.id, status="completed", stage="completed")
    terminal = (await client.get(path, headers=AUTH, params={"limit": 1})).json()
    assert terminal["active_run"] is None
    assert len(await repo.list_active_runs(other.id)) == 1


@pytest.mark.parametrize("limit", [0, -1, 101])
async def test_history_limit_validation(history_api, limit):
    client, _, _, project = history_api
    response = await client.get(
        f"/api/projects/{project.id}/runs/page", headers=AUTH, params={"limit": limit}
    )
    assert response.status_code == 422


@pytest.mark.parametrize("cursor", ["", "not-base64", "x" * 513])
async def test_history_cursor_validation(history_api, cursor):
    client, _, _, project = history_api
    response = await client.get(
        f"/api/projects/{project.id}/runs/page", headers=AUTH, params={"cursor": cursor}
    )
    assert response.status_code == 422


async def test_cursors_cannot_cross_project_or_resource(history_api):
    client, repo, _, project = history_api
    rows = await seed_runs(repo, project.id, 2)
    cursor = (await repo.page_runs(project.id, limit=1))["next_cursor"]
    other = await repo.create_project(project_input("Other"))
    assert (
        await client.get(
            f"/api/projects/{other.id}/runs/page", headers=AUTH, params={"cursor": cursor}
        )
    ).status_code == 422
    items = [{"id": row.id, "created_at": row.created_at} for row in reversed(rows)]
    for resource in ("sandbox", "a2a"):
        foreign = history_page(items, limit=1, project_id=project.id, resource=resource)[
            "next_cursor"
        ]
        response = await client.get(
            f"/api/projects/{project.id}/runs/page", headers=AUTH, params={"cursor": foreign}
        )
        assert response.status_code == 422


async def test_page_and_activity_selects_never_load_report_column(history_api):
    _, repo, _, project = history_api
    rows = await seed_runs(repo, project.id, 3)
    await repo.update_run(rows[0].id, status="queued")
    statements = []

    def capture(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT") and "FROM runs" in statement:
            statements.append(statement)

    event.listen(repo.engine.sync_engine, "before_cursor_execute", capture)
    try:
        page = await repo.page_runs(project.id, limit=1)
        summaries = await repo.list_active_runs()
    finally:
        event.remove(repo.engine.sync_engine, "before_cursor_execute", capture)
    assert len(statements) == 3
    assert all("runs.report" not in statement.lower() for statement in statements)
    assert len(page["items"]) == 1
    assert page["active_run"]["id"] == rows[0].id
    assert [summary.id for summary in summaries] == [rows[0].id]
    assert all("report" not in summary.model_dump() for summary in summaries)
    assert ".123456" in page["items"][0]["updated_at"]


async def test_old_database_gains_history_index_without_data_changes(history_api):
    _, repo, _, project = history_api
    rows = await seed_runs(repo, project.id, 2)
    async with repo.engine.begin() as conn:
        await conn.execute(text(f"DROP INDEX {RUN_HISTORY_INDEX.name}"))
        names = await conn.run_sync(
            lambda sync: {item["name"] for item in inspect(sync).get_indexes("runs")}
        )
        assert RUN_HISTORY_INDEX.name not in names
    reopened = Repository(repo.settings)
    try:
        await reopened.initialize()
        await reopened.initialize()
        async with reopened.engine.connect() as conn:
            indexes = await conn.run_sync(lambda sync: inspect(sync).get_indexes("runs"))
            match = next(item for item in indexes if item["name"] == RUN_HISTORY_INDEX.name)
            assert match["column_names"] == ["project_id", "created_at", "id"]
            ddl = await conn.scalar(
                text("SELECT sql FROM sqlite_master WHERE type='index' AND name=:name"),
                {"name": RUN_HISTORY_INDEX.name},
            )
            assert "created_at DESC" in ddl and "id DESC" in ddl
        assert (await reopened.get_project(project.id)).name == project.name
        assert (await reopened.get_run(rows[0].id)).report == rows[0].report
        assert len((await reopened.page_runs(project.id))["items"]) == 2
    finally:
        await reopened.close()


async def test_startup_recovery_uses_only_active_history_not_legacy_full_list(
    history_api, monkeypatch
):
    _, repo, jobs, project = history_api
    terminal = await seed_runs(repo, project.id, 30)
    await repo.update_run(terminal[0].id, status="queued")
    other = await repo.create_project(project_input("Other"))
    running = (await seed_runs(repo, other.id, 1, first_id=100))[0]
    await repo.update_run(running.id, status="running")

    async def forbidden_full_list(*_args, **_kwargs):
        pytest.fail("startup must not materialize every historical report")

    monkeypatch.setattr(repo, "list_runs", forbidden_full_list)
    await jobs.recover_interrupted()
    assert await repo.list_active_runs() == []
    assert (await repo.get_run(terminal[0].id)).status == "interrupted"
    assert (await repo.get_run(running.id)).status == "interrupted"
    assert (await repo.get_run(terminal[-1].id)).status == "completed"
    assert (await repo.list_events(terminal[0].id))[-1].kind == "interrupted"
    async with repo.sessions() as session:
        ids = (await session.scalars(select(Run.id).where(Run.status == "completed"))).all()
    assert len(ids) == 29
