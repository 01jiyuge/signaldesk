import asyncio

import pytest
import pytest_asyncio
from sqlalchemy import event, func, select

from signaldesk.a2a_store import A2ARequestStore, IdempotencyConflict, requests
from signaldesk.config import Settings
from signaldesk.db import Event, Run
from signaldesk.repository import ActiveRunError, Repository
from signaldesk.schemas import ProjectCreate, RunCreate


@pytest_asyncio.fixture
async def a2a_repository(tmp_path):
    repo = Repository(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/a2a.db")
    )
    await repo.initialize()
    try:
        yield repo
    finally:
        await repo.close()


async def project(repo, name="Fixture"):
    return await repo.create_project(
        ProjectCreate(
            name=name, sources=[{"name": name, "url": "https://northstar.example/pricing"}]
        )
    )


async def test_message_retry_reopen_conflict_and_active_run_share_atomic_configuration(
    a2a_repository,
):
    repo = a2a_repository
    first, second = await project(repo), await project(repo, "Other")
    store = A2ARequestStore(repo)
    run, created = await store.create_or_get(first.id, RunCreate(), "durable-message")
    assert created and await store.contains(run.id)
    assert (await repo.get_run_project(run.id)) == first
    same, created = await store.create_or_get(first.id, RunCreate(), "durable-message")
    assert same.id == run.id and not created
    with pytest.raises(ActiveRunError):
        await store.create_or_get(first.id, RunCreate(), "different-message")
    with pytest.raises(IdempotencyConflict):
        await store.create_or_get(first.id, RunCreate(demo_revision=2), "durable-message")
    with pytest.raises(IdempotencyConflict):
        await store.create_or_get(second.id, RunCreate(), "durable-message")
    await repo.update_run(run.id, status="completed")
    reopened = Repository(repo.settings)
    try:
        retry, created = await A2ARequestStore(reopened).create_or_get(
            first.id, RunCreate(), "durable-message"
        )
        assert retry.id == run.id and retry.status == "completed" and not created
    finally:
        await reopened.close()
    assert len(await repo.list_runs()) == 1


async def test_binding_failure_rolls_back_run_and_configuration(a2a_repository):
    repo = a2a_repository
    original = await project(repo)
    store = A2ARequestStore(repo)
    await store.initialize()

    def fail_binding(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO a2a_requests"):
            raise RuntimeError("fixture binding transaction failure")

    event.listen(repo.engine.sync_engine, "before_cursor_execute", fail_binding)
    try:
        with pytest.raises(RuntimeError, match="fixture binding"):
            await store.create_or_get(original.id, RunCreate(), "atomic-message")
    finally:
        event.remove(repo.engine.sync_engine, "before_cursor_execute", fail_binding)
    async with repo.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Run)) == 0
        assert await session.scalar(select(func.count()).select_from(Event)) == 0
        assert await session.scalar(select(func.count()).select_from(requests)) == 0
    assert (await store.create_or_get(original.id, RunCreate(), "atomic-message"))[1]


async def test_concurrent_retry_creates_exactly_one_task(a2a_repository):
    repo = a2a_repository
    original = await project(repo)
    store = A2ARequestStore(repo)
    responses = await asyncio.gather(
        *[store.create_or_get(original.id, RunCreate(), "concurrent-message") for _ in range(5)]
    )
    assert len({run.id for run, _ in responses}) == 1
    assert sum(created for _, created in responses) == 1
    assert len(await repo.list_runs()) == 1


async def test_task_update_rechecks_terminal_state_inside_database_lock(a2a_repository):
    repo = a2a_repository
    owner = await project(repo)
    store = A2ARequestStore(repo)
    record = await store.prepare_delegation(
        project_id=owner.id,
        run_id=None,
        remote="fixture",
        endpoint="https://fixture.example/a2a/",
        remote_project_id="remote-project",
        message_id="atomic-terminal",
        mode="demo",
        demo_revision=1,
    )
    working = {
        "id": "same-remote-task",
        "contextId": "remote-project",
        "status": {"state": "TASK_STATE_WORKING"},
    }
    await store.record_task(record["id"], working)
    other_repo = Repository(repo.settings)
    other = A2ARequestStore(other_repo)
    prior_read, release_update = asyncio.Event(), asyncio.Event()
    actual_get = other.get_delegation

    async def delayed_lookup(identifier):
        value = await actual_get(identifier)
        prior_read.set()
        await release_update.wait()
        return value

    other.get_delegation = delayed_lookup
    stale_update = asyncio.create_task(other.record_task(record["id"], working))
    try:
        await asyncio.wait_for(prior_read.wait(), 1)
        canceled = {**working, "status": {"state": "TASK_STATE_CANCELED"}}
        accepted = await store.record_task(record["id"], canceled)
        assert accepted["status"] == "TASK_STATE_CANCELED"
        release_update.set()
        assert (await stale_update)["status"] == "TASK_STATE_CANCELED"
        assert (await store.get_delegation(record["id"]))["status"] == "TASK_STATE_CANCELED"
    finally:
        release_update.set()
        await asyncio.gather(stale_update, return_exceptions=True)
        await other_repo.close()


@pytest.mark.parametrize("state", ["TASK_STATE_WORKING", "TASK_STATE_COMPLETED"])
async def test_delegation_cannot_rebind_to_another_remote_task_id(a2a_repository, state):
    from signaldesk.errors import PublicTaskError

    repo = a2a_repository
    owner = await project(repo)
    store = A2ARequestStore(repo)
    record = await store.prepare_delegation(
        project_id=owner.id,
        run_id=None,
        remote="fixture",
        endpoint="https://fixture.example/a2a/",
        remote_project_id="remote-project",
        message_id="fixed-task-id",
        mode="demo",
        demo_revision=1,
    )
    original = {
        "id": "first-remote-task",
        "contextId": "remote-project",
        "status": {"state": state},
    }
    await store.record_task(record["id"], original)
    with pytest.raises(PublicTaskError) as caught:
        await store.record_task(record["id"], {**original, "id": "different-after-restart"})
    assert caught.value.code == "a2a_task_mismatch"
    assert (await store.get_delegation(record["id"]))["task"] == original
