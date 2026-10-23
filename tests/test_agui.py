import asyncio
import json

import httpx
import pytest
import pytest_asyncio
from ag_ui.core import Event
from fastapi import FastAPI
from pydantic import TypeAdapter

from signaldesk.agui import create_agui_router, public_snapshot, stream_run
from signaldesk.config import Settings
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate, RunCreate

AUTH = {"Authorization": "Bearer test-token"}


@pytest_asyncio.fixture
async def setup(tmp_path, monkeypatch):
    monkeypatch.setattr("signaldesk.agui.POLL_INTERVAL_SECONDS", 0.001)
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/agui.db",
        api_token="test-token",
    )
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="Alpha",
            description="private-configuration-do-not-stream",
            sources=[{"name": "Alpha", "url": "https://example.com"}],
        )
    )
    run = await repo.create_run(project.id, RunCreate())
    app = FastAPI()
    app.include_router(create_agui_router(repo, settings))
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, repo, project, run
    finally:
        await repo.close()


def parse_sse(body):
    frames = []
    for block in body.split("\n\n"):
        lines = block.splitlines()
        data = [line.removeprefix("data: ") for line in lines if line.startswith("data: ")]
        if not data:
            continue
        event = json.loads("\n".join(data))
        # Validate the actual wire payload against the SDK's discriminated union.
        TypeAdapter(Event).validate_python(event)
        cursor = next(
            (line.removeprefix("id: ") for line in lines if line.startswith("id: ")), None
        )
        frames.append((cursor, event))
    return frames


async def test_completed_stream_uses_official_events_steps_and_safe_state(setup):
    client, repo, project, run = setup
    await repo.append_event(run.id, "stage", "Collect", {"stage": "collect"})
    await repo.append_event(run.id, "stage", "Research", {"stage": "research"})
    await repo.update_run(run.id, status="completed", stage="completed", report={"title": "Ready"})
    await repo.append_event(run.id, "completed", "Ready", {"finding_count": 1})
    response = await client.get(f"/api/runs/{run.id}/ag-ui", headers=AUTH)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["x-accel-buffering"] == "no"
    assert "private-configuration-do-not-stream" not in response.text
    frames = parse_sse(response.text)
    events = [event for _, event in frames]
    assert events[0]["type"] == "RUN_STARTED"
    assert events[0]["threadId"] == project.id and events[0]["runId"] == run.id
    assert events[-1]["type"] == "RUN_FINISHED"
    assert events[-1]["outcome"] == {"type": "success"}
    starts = [event["stepName"] for event in events if event["type"] == "STEP_STARTED"]
    ends = [event["stepName"] for event in events if event["type"] == "STEP_FINISHED"]
    assert starts == ends == ["collect", "research"]
    snapshot = next(event["snapshot"] for event in events if event["type"] == "STATE_SNAPSHOT")
    assert snapshot["run"]["report"] == {"title": "Ready"}
    assert snapshot["reportAvailable"] is True and snapshot["reportOmitted"] is False
    customs = [(cursor, event["value"]) for cursor, event in frames if event["type"] == "CUSTOM"]
    assert all(
        cursor == str(event["id"]) and event["run_id"] == run.id for cursor, event in customs
    )
    configuration = next(event for _, event in customs if event["kind"] == "configuration")
    assert configuration["data"] == {}


async def test_auth_missing_run_and_invalid_cursors_fail_before_streaming(setup):
    client, _, _, run = setup
    path = f"/api/runs/{run.id}/ag-ui"
    assert (await client.get(path)).status_code == 401
    assert (await client.get("/api/runs/missing/ag-ui", headers=AUTH)).status_code == 404
    assert (await client.get(path + "?after=-1", headers=AUTH)).status_code == 422
    for invalid in ("not-an-id", "-1", "9" * 100):
        response = await client.get(path, headers={**AUTH, "Last-Event-ID": invalid})
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")


@pytest.mark.parametrize("status", ["failed", "cancelled", "interrupted"])
async def test_terminal_statuses_have_standard_boundary_and_redact_legacy_errors(setup, status):
    client, repo, _, run = setup
    await repo.update_run(run.id, status=status, error="Authorization: Bearer legacy-secret")
    await repo.append_event(
        run.id,
        "error",
        "raw provider error: secret-raw-text",
        {
            "code": "internal_error",
            "suggestion": "Authorization: Bearer suggestion-secret",
            "retryable": False,
            "error": "raw-secret-payload",
            "project": {"key": "raw-secret-config"},
        },
    )
    response = await client.get(f"/api/runs/{run.id}/ag-ui", headers=AUTH)
    events = [event for _, event in parse_sse(response.text)]
    for forbidden in (
        "legacy-secret",
        "secret-raw-text",
        "suggestion-secret",
        "raw-secret-payload",
        "raw-secret-config",
    ):
        assert forbidden not in response.text
    terminal = events[-1]
    if status == "cancelled":
        assert terminal["type"] == "RUN_FINISHED" and terminal["outcome"] == {"type": "cancelled"}
    else:
        assert terminal["type"] == "RUN_ERROR"
        assert terminal["code"] == f"run_{status}"
    safe_error = next(
        event["value"]
        for event in events
        if event["type"] == "CUSTOM" and event["value"]["kind"] == "error"
    )
    assert safe_error["data"]["code"] == "internal_error"
    assert safe_error["data"]["retryable"] is False


async def test_resume_cursor_skips_delivered_business_events_and_other_runs(setup):
    client, repo, _, run = setup
    first = await repo.append_event(run.id, "stage", "Collect", {"stage": "collect"})
    second = await repo.append_event(run.id, "stage", "Compare", {"stage": "compare"})
    third = await repo.append_event(run.id, "stage", "Verify", {"stage": "verify"})
    await repo.update_run(run.id, status="completed", stage="completed", report={"title": "Ready"})
    other_project = await repo.create_project(
        ProjectCreate(name="Beta", sources=[{"name": "Beta", "url": "https://example.org"}])
    )
    other_run = await repo.create_run(other_project.id, RunCreate())
    other_event = await repo.append_event(other_run.id, "stage", "Private", {"stage": "research"})
    response = await client.get(
        f"/api/runs/{run.id}/ag-ui?after={first.id}",
        headers={**AUTH, "Last-Event-ID": str(second.id)},
    )
    frames = parse_sse(response.text)
    customs = [event["value"] for _, event in frames if event["type"] == "CUSTOM"]
    assert [event["id"] for event in customs] == [third.id]
    assert all(event["run_id"] == run.id and event["id"] != other_event.id for event in customs)
    assert frames[0][1]["type"] == "RUN_STARTED"
    assert any(event["type"] == "STATE_SNAPSHOT" for _, event in frames)


async def test_large_report_uses_bounded_snapshot_with_http_fallback(setup):
    client, repo, _, run = setup
    await repo.update_run(
        run.id, status="completed", stage="completed", report={"summary": "界" * (1024 * 1024)}
    )
    snapshot = public_snapshot(await repo.get_run(run.id))
    assert snapshot["reportAvailable"] is True and snapshot["reportOmitted"] is True
    assert snapshot["run"]["report"] is None
    response = await client.get(f"/api/runs/{run.id}/ag-ui", headers=AUTH)
    assert len(response.content) < 10000
    parse_sse(response.text)


async def test_active_http_stream_observes_later_database_completion(setup, monkeypatch):
    client, repo, _, run = setup
    subscribed = asyncio.Event()
    original = repo.list_event_page

    async def observed_list_events(*args, **kwargs):
        subscribed.set()
        return await original(*args, **kwargs)

    monkeypatch.setattr(repo, "list_event_page", observed_list_events)

    async def complete_after_subscription():
        await subscribed.wait()
        await repo.update_run(run.id, status="running", stage="collect")
        await repo.append_event(run.id, "stage", "Collect", {"stage": "collect"})
        await repo.update_run(
            run.id, status="completed", stage="completed", report={"title": "Later"}
        )
        await repo.append_event(run.id, "completed", "Ready")

    producer = asyncio.create_task(complete_after_subscription())
    try:
        response = await asyncio.wait_for(
            client.get(f"/api/runs/{run.id}/ag-ui", headers=AUTH),
            timeout=3,
        )
        events = [event for _, event in parse_sse(response.text)]
        snapshots = [event["snapshot"] for event in events if event["type"] == "STATE_SNAPSHOT"]
        assert snapshots[0]["run"]["status"] == "queued"
        assert snapshots[-1]["run"]["status"] == "completed"
        assert snapshots[-1]["run"]["report"] == {"title": "Later"}
        assert events[-1]["type"] == "RUN_FINISHED"
    finally:
        await producer


class ConnectedRequest:
    async def is_disconnected(self):
        return False


async def test_heartbeats_and_disconnecting_observer_do_not_cancel_background_job(
    setup, monkeypatch
):
    _, repo, _, run = setup
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def runner(run_id, resume=False):
        started.set()
        await release.wait()
        await repo.update_run(run_id, status="completed", report={"title": "Still completed"})
        finished.set()

    jobs = JobManager(repo, runner)
    jobs.submit(run.id)
    await asyncio.wait_for(started.wait(), timeout=2)
    monkeypatch.setattr("signaldesk.agui.HEARTBEAT_INTERVAL_SECONDS", 0)
    observer = stream_run(repo, await repo.get_run(run.id), ConnectedRequest())
    try:
        for _ in range(10):
            frame = await asyncio.wait_for(anext(observer), timeout=2)
            if frame.startswith(": heartbeat"):
                break
        else:
            pytest.fail("Expected an SSE heartbeat")
        await observer.aclose()
        assert (await repo.get_run(run.id)).status == "running"
        release.set()
        await asyncio.wait_for(finished.wait(), timeout=2)
        assert (await repo.get_run(run.id)).status == "completed"
    finally:
        await observer.aclose()
        await jobs.shutdown()


async def test_terminal_stream_waits_for_store_and_final_events(setup, monkeypatch):
    from types import SimpleNamespace

    _, repo, _, run = setup
    saved, release = asyncio.Event(), asyncio.Event()

    async def runner(run_id, resume=False):
        await repo.update_run(run_id, status="completed", report={"title": "Saved"})
        saved.set()
        # Represents a slow Store write after the report has been committed.
        await release.wait()
        await repo.append_event(
            run_id,
            "warning",
            "Report saved but Store unavailable",
            {"code": "memory_write_failed", "retryable": True},
        )
        await repo.append_event(run_id, "completed", "Ready")

    jobs = JobManager(repo, runner)
    jobs.submit(run.id)
    await asyncio.wait_for(saved.wait(), timeout=2)
    request = ConnectedRequest()
    request.app = SimpleNamespace(state=SimpleNamespace(jobs=jobs))
    monkeypatch.setattr("signaldesk.agui.HEARTBEAT_INTERVAL_SECONDS", 0)
    observer = stream_run(repo, await repo.get_run(run.id), request)
    body = ""
    try:
        # Several polling passes after completed must still leave it open while
        # the producer is awaiting Store; the old two-pass heuristic ended here.
        heartbeats = 0
        while heartbeats < 4:
            frame = await asyncio.wait_for(anext(observer), timeout=2)
            body += frame
            heartbeats += frame.startswith(": heartbeat")
            assert '"type":"RUN_FINISHED"' not in frame
        release.set()

        async def drain():
            return "".join([frame async for frame in observer])

        body += await asyncio.wait_for(drain(), timeout=2)
        frames = parse_sse(body)
        customs = [event["value"] for _, event in frames if event["type"] == "CUSTOM"]
        assert [event["kind"] for event in customs][-2:] == ["warning", "completed"]
        warning = next(event for event in customs if event["kind"] == "warning")
        assert warning["message"] == "报告已保存，但写入项目记忆失败。"
        assert frames[-1][1]["type"] == "RUN_FINISHED"
        assert frames[-1][1]["metadata"]["cursor"] == customs[-1]["id"]
        assert not jobs.is_active(run.id)
    finally:
        release.set()
        await observer.aclose()
        await jobs.shutdown()


async def test_terminal_stream_waits_for_cancellation_audit_after_worker_exits(setup, monkeypatch):
    from types import SimpleNamespace

    _, repo, _, run = setup
    started, auditing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def runner(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    original = repo.append_event

    async def slow_audit(run_id, kind, *args, **kwargs):
        if kind == "cancelled":
            auditing.set()
            await release.wait()
        return await original(run_id, kind, *args, **kwargs)

    monkeypatch.setattr(repo, "append_event", slow_audit)
    monkeypatch.setattr("signaldesk.agui.HEARTBEAT_INTERVAL_SECONDS", 0)
    jobs = JobManager(repo, runner)
    jobs.submit(run.id)
    await asyncio.wait_for(started.wait(), 2)
    cancelling = asyncio.create_task(jobs.cancel(run.id))
    await asyncio.wait_for(auditing.wait(), 2)
    assert run.id not in jobs._tasks and jobs.is_active(run.id)
    request = ConnectedRequest()
    request.app = SimpleNamespace(state=SimpleNamespace(jobs=jobs))
    observer = stream_run(repo, await repo.get_run(run.id), request)
    body = ""
    try:
        heartbeats = 0
        while heartbeats < 4:
            frame = await asyncio.wait_for(anext(observer), 2)
            body += frame
            heartbeats += frame.startswith(": heartbeat")
            assert '"type":"RUN_FINISHED"' not in frame
        release.set()
        await asyncio.wait_for(cancelling, 2)

        async def drain():
            return "".join([frame async for frame in observer])

        body += await asyncio.wait_for(drain(), 2)
        frames = parse_sse(body)
        customs = [event["value"] for _, event in frames if event["type"] == "CUSTOM"]
        assert customs[-1]["kind"] == "cancelled"
        assert frames[-1][1]["type"] == "RUN_FINISHED"
        assert frames[-1][1]["metadata"]["cursor"] == customs[-1]["id"]
    finally:
        release.set()
        await cancelling
        await observer.aclose()
        await jobs.shutdown()
