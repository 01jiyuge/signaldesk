"""Bounded, run-isolated event seek pages and safe public REST projection."""

import asyncio
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import event, insert, inspect, select, text

from signaldesk.api import create_router
from signaldesk.config import Settings
from signaldesk.db import EVENT_HISTORY_INDEX, Event
from signaldesk.jobs import JobManager
from signaldesk.repository import MAX_EVENT_CURSOR, Repository
from signaldesk.schemas import EventRead, ProjectCreate, RunCreate

AUTH = {"Authorization": "Bearer event-fixture-token"}


@pytest_asyncio.fixture
async def event_api(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/events.db",
        api_token="event-fixture-token",
    )
    repo = Repository(settings)
    await repo.initialize()
    projects = [
        await repo.create_project(
            ProjectCreate(name=name, sources=[{"name": name, "url": "https://example.com"}])
        )
        for name in ("Events Alpha", "Events Beta")
    ]
    runs = [await repo.create_run(project.id, RunCreate()) for project in projects]

    async def unused_runner(*_args, **_kwargs):
        pytest.fail("Reading events must never start a research task")

    jobs = JobManager(repo, unused_runner)
    app = FastAPI()
    app.include_router(create_router(repo, jobs, settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://fixture"
    ) as client:
        try:
            yield client, repo, runs, jobs
        finally:
            await jobs.shutdown()
            await repo.close()


async def seed_events(repo, run_ids, count):
    at = datetime(2026, 10, 23, tzinfo=UTC)
    async with repo.engine.begin() as conn:
        await conn.execute(
            insert(Event),
            [
                {
                    "run_id": run_id,
                    "kind": "stage",
                    "message": f"Internal event {index}",
                    "data": {"stage": "collect", "raw": f"private-fixture-{run_id}-{index}"},
                    "created_at": at,
                }
                for index in range(count)
                for run_id in run_ids
            ],
        )


async def test_large_interleaved_history_is_bounded_and_strictly_run_isolated(event_api):
    _, repo, runs, _ = event_api
    await seed_events(repo, [run.id for run in runs], 1003)
    expected = await repo.list_events(runs[0].id)
    after, seen, batches = 0, [], []
    while True:
        page = await repo.list_event_page(runs[0].id, after=after)
        assert all(isinstance(item, EventRead) for item in page["items"])
        assert all(item.run_id == runs[0].id and item.id > after for item in page["items"])
        assert len(page["items"]) <= 100
        batches.append(len(page["items"]))
        seen.extend(item.id for item in page["items"])
        assert page["next_after"] == page["items"][-1].id
        after = page["next_after"]
        if not page["has_more"]:
            break
    assert seen == [item.id for item in expected]
    assert seen == sorted(set(seen))
    assert batches == [100] * 10 + [4]
    assert await repo.list_event_page(runs[0].id, after=after) == {
        "items": [],
        "next_after": after,
        "has_more": False,
    }
    maximum = await repo.list_event_page(runs[0].id, limit=200)
    assert len(maximum["items"]) == 200 and maximum["has_more"]


async def test_appends_between_pages_and_after_empty_tail_are_delivered_once(event_api):
    _, repo, runs, _ = event_api
    run = runs[0]
    await seed_events(repo, [run.id], 4)
    first = await repo.list_event_page(run.id, limit=2)
    appended = await repo.append_event(run.id, "warning", "Late append")
    seen = [item.id for item in first["items"]]
    after = first["next_after"]
    while True:
        page = await repo.list_event_page(run.id, after=after, limit=2)
        seen.extend(item.id for item in page["items"])
        after = page["next_after"]
        if not page["has_more"]:
            break
    assert seen == [item.id for item in await repo.list_events(run.id)]
    assert seen[-1] == appended.id and len(seen) == len(set(seen))
    assert (await repo.list_event_page(run.id, after=after))["next_after"] == after
    newest = await repo.append_event(run.id, "completed", "Finished later")
    tail = await repo.list_event_page(run.id, after=after, limit=1)
    assert [item.id for item in tail["items"]] == [newest.id]
    assert tail["next_after"] == newest.id and not tail["has_more"]


async def test_public_event_page_auth_404_bounds_and_empty_cursor(event_api):
    client, _, runs, _ = event_api
    path = f"/api/runs/{runs[0].id}/events/page"
    assert (await client.get(path)).status_code == 401
    assert (await client.get(path, headers={"Authorization": "Bearer wrong"})).status_code == 401
    assert (await client.get("/api/runs/missing/events/page", headers=AUTH)).status_code == 404
    response = await client.get(path, headers=AUTH, params={"after": MAX_EVENT_CURSOR})
    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "next_after": MAX_EVENT_CURSOR,
        "has_more": False,
        "producer_active": False,
    }


async def test_terminal_run_keeps_polling_until_its_producer_commits_final_event(event_api):
    client, repo, runs, jobs = event_api
    run = runs[0]
    after = (await repo.list_event_page(run.id))["next_after"]
    report_saved = asyncio.Event()
    finish = asyncio.Event()

    async def gated_runner(run_id, **_kwargs):
        await repo.update_run(run_id, status="completed", report={"summary": "Fixture report"})
        report_saved.set()
        await finish.wait()
        await repo.append_event(run_id, "completed", "Producer final event")

    jobs.runner = gated_runner
    jobs.submit(run.id)
    producer = jobs._tasks[run.id]
    try:
        async with asyncio.timeout(2):
            await report_saved.wait()
        path = f"/api/runs/{run.id}/events/page"
        waiting = await client.get(path, headers=AUTH, params={"after": after})
        assert waiting.status_code == 200
        assert waiting.json() == {
            "items": [],
            "next_after": after,
            "has_more": False,
            "producer_active": True,
        }
        finish.set()
        async with asyncio.timeout(2):
            await asyncio.shield(producer)
        assert not jobs.is_active(run.id)
        final = (await client.get(path, headers=AUTH, params={"after": after})).json()
        assert final["producer_active"] is False and final["has_more"] is False
        expected = await repo.list_events(run.id, after=after)
        assert len(expected) == 1 and expected[0].message == "Producer final event"
        assert [item["id"] for item in final["items"]] == [expected[0].id]
        assert final["items"][0]["kind"] == "completed"
        assert final["next_after"] == final["items"][-1]["id"] > after
    finally:
        finish.set()


async def test_producer_activity_is_sampled_before_query_even_if_it_finishes_after_read(
    event_api, monkeypatch
):
    client, repo, runs, jobs = event_api
    run = runs[0]
    after = (await repo.list_event_page(run.id))["next_after"]
    query = repo.list_event_page
    active, calls = True, []

    def is_active(run_id):
        assert run_id == run.id
        calls.append("active")
        return active

    async def finish_after_snapshot(run_id, **kwargs):
        nonlocal active
        calls.append("query")
        page = await query(run_id, **kwargs)
        if active:
            assert page["items"] == []
            await repo.append_event(run_id, "completed", "Committed after page snapshot")
            active = False
        return page

    monkeypatch.setattr(jobs, "is_active", is_active)
    monkeypatch.setattr(repo, "list_event_page", finish_after_snapshot)
    path = f"/api/runs/{run.id}/events/page"
    first = (await client.get(path, headers=AUTH, params={"after": after})).json()
    assert calls == ["active", "query"]
    assert first == {
        "items": [],
        "next_after": after,
        "has_more": False,
        "producer_active": True,
    }
    second = (await client.get(path, headers=AUTH, params={"after": after})).json()
    assert calls == ["active", "query", "active", "query"]
    assert second["producer_active"] is False and second["has_more"] is False
    expected = await repo.list_events(run.id, after=after)
    assert len(expected) == 1 and expected[0].message == "Committed after page snapshot"
    assert [item["id"] for item in second["items"]] == [expected[0].id]
    assert second["items"][0]["kind"] == "completed"


@pytest.mark.parametrize(
    "params",
    [
        {"after": -1},
        {"after": MAX_EVENT_CURSOR + 1},
        {"after": 2**80},
        {"after": "not-an-integer"},
        {"limit": 0},
        {"limit": 201},
        {"limit": "bad"},
    ],
)
async def test_public_event_page_rejects_invalid_query_before_database_bind(event_api, params):
    client, _, runs, _ = event_api
    response = await client.get(f"/api/runs/{runs[0].id}/events/page", headers=AUTH, params=params)
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("after", "limit"),
    [(-1, 100), (MAX_EVENT_CURSOR + 1, 100), (True, 100), (0, 0), (0, 201), (0, True)],
)
async def test_repository_event_limits_also_protect_internal_consumers(event_api, after, limit):
    _, repo, runs, _ = event_api
    with pytest.raises(ValueError):
        await repo.list_event_page(runs[0].id, after=after, limit=limit)


async def test_safe_projection_preserves_cursor_but_never_exposes_raw_configuration(event_api):
    client, repo, runs, _ = event_api
    run = runs[0]
    warning = await repo.append_event(
        run.id,
        "warning",
        "PRIVATE_RAW_EXCEPTION Authorization: Bearer fixture-secret-value",
        {
            "project": {"description": "PRIVATE_PROJECT_CONFIGURATION"},
            "memory_context": {"preference": "PRIVATE_MEMORY_CONTEXT"},
            "raw_exception": "PRIVATE_RAW_EXCEPTION",
            "code": "fixture_warning",
            "retryable": True,
            "stage": "collect",
            "model_call_skipped": False,
            "finding_count": 3,
            "snapshot_count": True,
            "evidence_count": 100001,
            "suggestion": "Check Authorization: Bearer fixture-secret-value",
        },
    )
    path = f"/api/runs/{run.id}/events"
    response = await client.get(path + "/page", headers=AUTH, params={"limit": 200})
    page = response.json()
    assert response.status_code == 200
    assert page["next_after"] == warning.id and not page["has_more"]
    assert set(page) == {"items", "next_after", "has_more", "producer_active"}
    configuration, item = page["items"]
    assert configuration["kind"] == "configuration" and configuration["data"] == {}
    assert item["id"] == warning.id and item["run_id"] == run.id
    suggestion = item["data"].pop("suggestion")
    assert "[redacted]" in suggestion and "fixture-secret-value" not in suggestion
    assert item["data"] == {
        "code": "fixture_warning",
        "retryable": True,
        "stage": "collect",
        "model_call_skipped": False,
        "finding_count": 3,
    }
    for private in (
        "PRIVATE_PROJECT_CONFIGURATION",
        "PRIVATE_MEMORY_CONTEXT",
        "PRIVATE_RAW_EXCEPTION",
        "fixture-secret-value",
    ):
        assert private not in response.text
    # The established endpoint remains an authenticated raw array for callers
    # that already depend on internal event payloads.
    legacy = await client.get(path, headers=AUTH)
    assert isinstance(legacy.json(), list)
    assert legacy.json()[-1]["data"]["raw_exception"] == "PRIVATE_RAW_EXCEPTION"


async def test_page_existence_check_never_loads_run_report_and_query_uses_id_seek(event_api):
    client, repo, runs, _ = event_api
    run = runs[0]
    await repo.update_run(run.id, report={"body": "REPORT_FIXTURE_MUST_NOT_LOAD" * 10000})
    await seed_events(repo, [run.id], 4)
    cursor = (await repo.list_event_page(run.id, limit=2))["next_after"]
    statements = []

    def capture(_conn, _cursor, statement, parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append((statement, parameters))

    event.listen(repo.engine.sync_engine, "before_cursor_execute", capture)
    try:
        response = await client.get(
            f"/api/runs/{run.id}/events/page", headers=AUTH, params={"after": cursor, "limit": 2}
        )
    finally:
        event.remove(repo.engine.sync_engine, "before_cursor_execute", capture)
    assert response.status_code == 200 and len(response.json()["items"]) == 2
    assert statements and all("runs.report" not in statement.lower() for statement, _ in statements)
    event_sql = [
        (statement, parameters)
        for statement, parameters in statements
        if "FROM events" in statement
    ]
    assert len(event_sql) == 1
    sql, parameters = event_sql[0]
    assert "events.id >" in sql and "ORDER BY events.id" in sql
    # SQLAlchemy's SQLite LIMIT clause adds a fixed OFFSET 0. No page number or
    # accumulated offset participates in traversal: the seek id is the cursor.
    assert "LIMIT" in sql and cursor in parameters


async def test_old_database_gets_event_index_without_changing_event_ids_or_data(event_api):
    _, repo, runs, _ = event_api
    saved = await repo.append_event(runs[0].id, "warning", "Keep event", {"fixture": "keep"})
    async with repo.engine.begin() as conn:
        await conn.execute(text(f"DROP INDEX {EVENT_HISTORY_INDEX.name}"))
    await repo.initialize()
    await repo.initialize()
    async with repo.engine.connect() as conn:
        indexes = await conn.run_sync(lambda sync: inspect(sync).get_indexes("events"))
        match = next(item for item in indexes if item["name"] == EVENT_HISTORY_INDEX.name)
        assert match["column_names"] == ["run_id", "id"]
        row = (await conn.execute(select(Event).where(Event.id == saved.id))).mappings().one()
        assert row["message"] == saved.message and row["data"] == saved.data
    page = await repo.list_event_page(runs[0].id, after=saved.id - 1)
    assert [item.id for item in page["items"]] == [saved.id]
    assert page["next_after"] == saved.id
