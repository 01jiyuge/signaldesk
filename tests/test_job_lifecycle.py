"""Race regressions for request cancellation, resume and shutdown ownership."""

import asyncio

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from signaldesk.api import create_router
from signaldesk.config import Settings
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate, RunCreate


async def until(predicate):
    async with asyncio.timeout(5):
        while not predicate():  # noqa: ASYNC110 - observe lock waiters without changing production code
            await asyncio.sleep(0)


@pytest_asyncio.fixture
async def setup(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/jobs.db")
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(name="Lifecycle", sources=[{"name": "Test", "url": "https://example.com/"}])
    )
    run = await repo.create_run(project.id, RunCreate())
    started, stopping, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def runner(run_id, resume=False):
        calls.append((run_id, resume))
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if not resume:
                stopping.set()
                await release.wait()
            raise

    jobs = JobManager(repo, runner)
    app = FastAPI()
    app.include_router(create_router(repo, jobs, settings))
    jobs.submit(run.id)
    await asyncio.wait_for(started.wait(), 5)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        try:
            yield repo, jobs, run.id, client, stopping, release, calls
        finally:
            release.set()
            await jobs.shutdown()
            await repo.close()


async def test_two_cancels_cannot_drop_or_overwrite_a_resumed_producer(setup):
    repo, jobs, run_id, client, stopping, release, calls = setup
    first = asyncio.create_task(client.post(f"/api/runs/{run_id}/cancel"))
    await asyncio.wait_for(stopping.wait(), 5)
    second = asyncio.create_task(client.post(f"/api/runs/{run_id}/cancel"))
    await until(lambda: jobs._run_locks[run_id][1] == 2)
    resume = asyncio.create_task(client.post(f"/api/runs/{run_id}/resume"))
    await until(lambda: jobs._run_locks[run_id][1] == 3)
    release.set()
    results = await asyncio.wait_for(asyncio.gather(first, second, resume), 5)
    assert [result.status_code for result in results] == [200, 409, 202]
    await until(lambda: len(calls) == 2)
    assert (await repo.get_run(run_id)).status == "running" and jobs.is_active(run_id)
    assert not jobs._run_locks
    await jobs.shutdown()
    assert (await repo.get_run(run_id)).status == "interrupted"
    assert not jobs.is_active(run_id)
    events = await repo.list_events(run_id)
    assert [event.kind for event in events].count("cancelled") == 1


async def test_cancel_request_cancelled_twice_still_commits_final_state(setup):
    repo, jobs, run_id, _, stopping, release, _ = setup
    request = asyncio.create_task(jobs.cancel(run_id))
    await asyncio.wait_for(stopping.wait(), 5)
    request.cancel()
    await asyncio.sleep(0)
    request.cancel()
    await asyncio.sleep(0)
    assert not request.done() and jobs.is_active(run_id)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(request, 5)
    assert (await repo.get_run(run_id)).status == "cancelled"
    assert not jobs.is_active(run_id) and not jobs._operations
    assert (await repo.list_events(run_id))[-1].kind == "cancelled"


async def test_shutdown_joins_a_cancel_transition_even_after_workflow_exits(setup, monkeypatch):
    repo, jobs, run_id, _, stopping, release, _ = setup
    writing, commit = asyncio.Event(), asyncio.Event()
    original = repo.update_run

    async def slow_final_write(target, **kwargs):
        if target == run_id and kwargs.get("status") == "cancelled":
            writing.set()
            await commit.wait()
        return await original(target, **kwargs)

    monkeypatch.setattr(repo, "update_run", slow_final_write)
    cancel = asyncio.create_task(jobs.cancel(run_id))
    await asyncio.wait_for(stopping.wait(), 5)
    release.set()
    await asyncio.wait_for(writing.wait(), 5)
    assert run_id not in jobs._tasks and jobs.is_active(run_id)
    shutdown = asyncio.create_task(jobs.shutdown())
    await until(lambda: jobs._closing)
    shutdown.cancel()
    await asyncio.sleep(0)
    assert not shutdown.done()
    commit.set()
    await asyncio.wait_for(cancel, 5)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(shutdown, 5)
    assert (await repo.get_run(run_id)).status == "cancelled"
    assert not jobs._operations and not jobs._tasks


async def test_shutdown_during_resume_rolls_back_queued_record_before_return(setup, monkeypatch):
    repo, jobs, run_id, client, _, release, _ = setup
    release.set()
    await jobs.cancel(run_id)
    prepared, continue_resume = asyncio.Event(), asyncio.Event()
    original = repo.update_run

    async def pause_resume(target, **kwargs):
        result = await original(target, **kwargs)
        if target == run_id and kwargs.get("status") == "queued":
            prepared.set()
            await continue_resume.wait()
        return result

    monkeypatch.setattr(repo, "update_run", pause_resume)
    resume = asyncio.create_task(client.post(f"/api/runs/{run_id}/resume"))
    await asyncio.wait_for(prepared.wait(), 5)
    shutdown = asyncio.create_task(jobs.shutdown())
    await until(lambda: jobs._closing)
    assert not shutdown.done()
    continue_resume.set()
    response, _ = await asyncio.wait_for(asyncio.gather(resume, shutdown), 5)
    assert response.status_code == 409
    assert (await repo.get_run(run_id)).status == "cancelled"
    assert not jobs._tasks and not jobs._operations


async def test_disconnected_resume_finishes_submission_and_audit(setup):
    repo, jobs, run_id, client, _, release, calls = setup
    release.set()
    await jobs.cancel(run_id)
    entered, finish = asyncio.Event(), asyncio.Event()

    async def prepare():
        await repo.update_run(run_id, status="queued")
        entered.set()
        await finish.wait()
        jobs.submit(run_id, resume=True)
        await repo.append_event(run_id, "resumed", "Task requeued")

    request = asyncio.create_task(jobs.transition(run_id, prepare))
    await asyncio.wait_for(entered.wait(), 5)
    request.cancel()
    await asyncio.sleep(0)
    assert not request.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(request, 5)
    await until(lambda: len(calls) == 2)
    assert jobs.is_active(run_id) and (await repo.get_run(run_id)).status == "running"
    assert (await repo.list_events(run_id))[-1].kind == "resumed"


@pytest.mark.parametrize("closing", [False, True])
async def test_committed_create_is_submitted_or_marked_failed_after_disconnect(
    setup, monkeypatch, closing
):
    repo, jobs, old_run, client, _, release, calls = setup
    release.set()
    await jobs.cancel(old_run)
    project_id = (await repo.get_run(old_run)).project_id
    committed, return_created = asyncio.Event(), asyncio.Event()
    created = []
    original = repo.create_run

    async def pause_after_commit(*args, **kwargs):
        run = await original(*args, **kwargs)
        created.append(run.id)
        committed.set()
        await return_created.wait()
        return run

    monkeypatch.setattr(repo, "create_run", pause_after_commit)
    request = asyncio.create_task(client.post(f"/api/projects/{project_id}/runs", json={}))
    await asyncio.wait_for(committed.wait(), 5)
    request.cancel()
    await asyncio.sleep(0)
    assert not request.done() and jobs._operations
    shutdown = None
    if closing:
        shutdown = asyncio.create_task(jobs.shutdown())
        await until(lambda: jobs._closing)
        assert not shutdown.done()
    return_created.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(request, 5)
    if shutdown:
        await asyncio.wait_for(shutdown, 5)
        assert (await repo.get_run(created[0])).status == "failed"
        assert not jobs.is_active(created[0])
    else:
        await until(lambda: len(calls) == 2)
        assert (await repo.get_run(created[0])).status == "running"
        assert jobs.is_active(created[0])
    assert not jobs._operations and not jobs._run_locks
