"""Bounded AG-UI replay, terminal draining and cursor-safe reconnection."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import insert, select

from signaldesk.agui import create_agui_router, stream_run
from signaldesk.config import Settings
from signaldesk.db import Event, utcnow
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository
from signaldesk.schemas import EventRead, ProjectCreate, RunCreate


def frames(body):
    result = []
    for block in body.split("\n\n"):
        lines = block.splitlines()
        data = [line[6:] for line in lines if line.startswith("data: ")]
        if data:
            cursor = next((line[4:] for line in lines if line.startswith("id: ")), None)
            result.append((cursor, json.loads("\n".join(data))))
    return result


def customs(body):
    return [event["value"] for _, event in frames(body) if event["type"] == "CUSTOM"]


async def seed(repo, run_id, count):
    rows = []
    for index in range(count):
        stage = {0: "collect", 100: "compare", 200: "research"}.get(index)
        rows.append(
            {
                "run_id": run_id,
                "kind": "stage" if stage else "collected",
                "message": "private-event-text-not-for-wire",
                "data": {"stage": stage} if stage else {"snapshot_count": 1},
                "created_at": utcnow(),
            }
        )
    async with repo.engine.begin() as connection:
        await connection.execute(insert(Event), rows)


async def event_ids(repo, run_id):
    async with repo.sessions() as session:
        return list(
            await session.scalars(select(Event.id).where(Event.run_id == run_id).order_by(Event.id))
        )


@pytest_asyncio.fixture
async def system(tmp_path, monkeypatch):
    monkeypatch.setattr("signaldesk.agui.POLL_INTERVAL_SECONDS", 0.001)
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/events.db",
        api_token="",
        model="",
        openai_api_key="",
        reranker_api_key="",
    )
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="Replay fixture", sources=[{"name": "Fixture", "url": "https://fixture.example/"}]
        )
    )
    run = await repo.create_run(project.id, RunCreate())
    app = FastAPI()
    app.include_router(create_agui_router(repo, settings))
    original = repo.list_event_page
    reads = []

    async def observe_page(run_id, after=0, limit=100):
        page = await original(run_id, after=after, limit=limit)
        reads.append((after, limit, len(page["items"]), page["has_more"]))
        return page

    async def forbidden(*args, **kwargs):
        raise AssertionError("SSE must not call the unbounded legacy event reader")

    monkeypatch.setattr(repo, "list_event_page", observe_page)
    monkeypatch.setattr(repo, "list_events", forbidden)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield SimpleNamespace(repo=repo, run=run, app=app, client=client, reads=reads)
    finally:
        await repo.close()


async def test_completed_run_drains_more_than_two_pages_before_terminal_boundary(
    system, monkeypatch
):
    await seed(system.repo, system.run.id, 305)
    await system.repo.update_run(
        system.run.id, status="completed", stage="completed", report={"title": "Ready"}
    )
    timeline = []
    original_page = system.repo.list_event_page

    async def observed_page(*args, **kwargs):
        page = await original_page(*args, **kwargs)
        timeline.append(("page", page["has_more"]))
        return page

    async def sleep(delay):
        timeline.append(("sleep", delay))
        await asyncio.sleep(0)

    monkeypatch.setattr(system.repo, "list_event_page", observed_page)
    monkeypatch.setattr(
        "signaldesk.agui.asyncio",
        SimpleNamespace(sleep=sleep, CancelledError=asyncio.CancelledError),
    )
    response = await system.client.get(f"/api/runs/{system.run.id}/ag-ui")
    assert response.status_code == 200
    expected = await event_ids(system.repo, system.run.id)
    assert [item["id"] for item in customs(response.text)] == expected
    decoded = frames(response.text)
    assert decoded[-1][1]["type"] == "RUN_FINISHED"
    assert decoded[-1][1]["metadata"]["cursor"] == expected[-1]
    assert all(cursor is None for cursor, event in decoded if event["type"] != "CUSTOM")
    assert all(limit == 100 and size <= 100 for _, limit, size, _ in system.reads)
    assert sum(more for _, _, _, more in system.reads) >= 3
    for index, (kind, more) in enumerate(timeline):
        if kind == "page" and more:
            assert timeline[index + 1] == ("sleep", 0)
    assert "private-event-text-not-for-wire" not in response.text


async def test_running_producer_appends_final_pages_after_report_saved(system, monkeypatch):
    await seed(system.repo, system.run.id, 305)
    last_initial = (await event_ids(system.repo, system.run.id))[-1]
    subscribed, saved, initial_drained, release = (asyncio.Event() for _ in range(4))
    original = system.repo.list_event_page

    async def observed_page(run_id, after=0, limit=100):
        subscribed.set()
        page = await original(run_id, after=after, limit=limit)
        if after >= last_initial:
            initial_drained.set()
        return page

    monkeypatch.setattr(system.repo, "list_event_page", observed_page)

    async def runner(run_id, resume=False):
        await subscribed.wait()
        await system.repo.update_run(
            run_id, status="completed", stage="completed", report={"title": "Saved"}
        )
        saved.set()
        await release.wait()
        await seed(system.repo, run_id, 215)
        await system.repo.append_event(
            run_id, "warning", "Store failed", {"code": "memory_write_failed"}
        )
        await system.repo.append_event(run_id, "completed", "Ready")

    jobs = JobManager(system.repo, runner)
    system.app.state.jobs = jobs
    jobs.submit(system.run.id)
    response_task = asyncio.create_task(system.client.get(f"/api/runs/{system.run.id}/ag-ui"))
    try:
        await asyncio.wait_for(saved.wait(), 2)
        await asyncio.wait_for(initial_drained.wait(), 2)
        assert not response_task.done() and jobs.is_active(system.run.id)
        release.set()
        response = await asyncio.wait_for(response_task, 3)
        business = customs(response.text)
        assert [item["id"] for item in business] == await event_ids(system.repo, system.run.id)
        assert [item["kind"] for item in business][-2:] == ["warning", "completed"]
        assert frames(response.text)[-1][1]["type"] == "RUN_FINISHED"
        assert not jobs.is_active(system.run.id)
        assert all(limit == 100 and size <= 100 for _, limit, size, _ in system.reads)
    finally:
        release.set()
        await jobs.shutdown()
        if not response_task.done():
            response_task.cancel()
        await asyncio.gather(response_task, return_exceptions=True)


class ConnectedRequest:
    disconnected = False

    async def is_disconnected(self):
        return self.disconnected


async def test_disconnect_mid_page_and_resume_from_last_custom_cursor(system):
    await seed(system.repo, system.run.id, 305)
    await system.repo.update_run(system.run.id, status="completed", stage="completed")
    request = ConnectedRequest()
    first, delivered = "", []
    observer = stream_run(system.repo, await system.repo.get_run(system.run.id), request)
    try:
        async for frame in observer:
            first += frame
            delivered = customs(first)
            if len(delivered) == 25:
                request.disconnected = True
    finally:
        await observer.aclose()
    assert len(delivered) == 25 and len(system.reads) == 1
    assert not any(event["type"] == "RUN_FINISHED" for _, event in frames(first))
    cursor = delivered[-1]["id"]
    response = await system.client.get(
        f"/api/runs/{system.run.id}/ag-ui", headers={"Last-Event-ID": str(cursor)}
    )
    resumed = customs(response.text)
    assert [item["id"] for item in delivered + resumed] == await event_ids(
        system.repo, system.run.id
    )
    assert all(item["id"] > cursor for item in resumed)
    assert frames(response.text)[-1][1]["metadata"]["cursor"] == resumed[-1]["id"]


async def test_page_failure_is_safe_and_reconnect_keeps_every_delivered_cursor(system, monkeypatch):
    await seed(system.repo, system.run.id, 305)
    await system.repo.update_run(system.run.id, status="completed", stage="completed")
    original = system.repo.list_event_page

    async def fail_second_page(run_id, after=0, limit=100):
        if after:
            raise RuntimeError("Authorization: Bearer fixture-secret private-database-detail")
        return await original(run_id, after=after, limit=limit)

    monkeypatch.setattr(system.repo, "list_event_page", fail_second_page)
    first = await system.client.get(f"/api/runs/{system.run.id}/ag-ui")
    delivered = customs(first.text)
    assert len(delivered) == 100
    terminal = frames(first.text)[-1][1]
    assert terminal["type"] == "RUN_ERROR" and terminal["code"] == "stream_unavailable"
    assert terminal["metadata"]["cursor"] == delivered[-1]["id"]
    assert "fixture-secret" not in first.text and "private-database-detail" not in first.text
    monkeypatch.setattr(system.repo, "list_event_page", original)
    resumed = await system.client.get(
        f"/api/runs/{system.run.id}/ag-ui", headers={"Last-Event-ID": str(delivered[-1]["id"])}
    )
    assert [item["id"] for item in delivered + customs(resumed.text)] == await event_ids(
        system.repo, system.run.id
    )


async def test_foreign_or_stale_page_items_never_advance_wire_cursor(system, monkeypatch):
    await system.repo.update_run(system.run.id, status="completed", stage="completed")
    valid = await system.repo.append_event(system.run.id, "collected", "Own")
    foreign = EventRead(
        id=valid.id + 100,
        run_id="another-run",
        kind="error",
        message="foreign-secret",
        data={},
        created_at=valid.created_at,
    )
    original = system.repo.list_event_page

    async def injected(run_id, after=0, limit=100):
        page = await original(run_id, after=after, limit=limit)
        return {
            **page,
            "items": [foreign, valid.model_copy(update={"id": 0}), *page["items"]],
            "next_after": foreign.id,
        }

    monkeypatch.setattr(system.repo, "list_event_page", injected)
    response = await system.client.get(f"/api/runs/{system.run.id}/ag-ui")
    business = customs(response.text)
    assert [item["id"] for item in business] == await event_ids(system.repo, system.run.id)
    assert all(item["run_id"] == system.run.id for item in business)
    assert "foreign-secret" not in response.text
    assert frames(response.text)[-1][1]["metadata"]["cursor"] == valid.id


async def test_nonprogressing_more_page_errors_once_instead_of_spinning(system, monkeypatch):
    called = []

    async def stuck(run_id, after=0, limit=100):
        called.append(after)
        return {"items": [], "next_after": 999, "has_more": True}

    monkeypatch.setattr(system.repo, "list_event_page", stuck)
    response = await system.client.get(f"/api/runs/{system.run.id}/ag-ui")
    terminal = frames(response.text)[-1][1]
    assert called == [0]
    assert terminal["type"] == "RUN_ERROR" and terminal["code"] == "stream_unavailable"
    assert terminal["metadata"]["cursor"] == 0
