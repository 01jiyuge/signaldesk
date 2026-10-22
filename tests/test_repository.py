import asyncio
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from signaldesk.config import Settings
from signaldesk.jobs import JobManager
from signaldesk.repository import ActiveRunError, Repository
from signaldesk.schemas import ProjectCreate, RunCreate, SnapshotRecord


@pytest_asyncio.fixture
async def repo(tmp_path):
    repository = Repository(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/test.db")
    )
    await repository.initialize()
    yield repository
    await repository.close()


async def make_project(repo, name="Alpha"):
    return await repo.create_project(
        ProjectCreate(
            name=name,
            sources=[
                {"name": name, "url": "https://example.com/pricing"},
            ],
        )
    )


def snapshot(run, text="The Pro plan costs $25.", identifier="snapshot-1"):
    return SnapshotRecord(
        id=identifier,
        run_id=run.id,
        project_id=run.project_id,
        source_name="Alpha",
        url="https://example.com/pricing",
        title="Pricing",
        text=text,
        content_hash="hash",
        created_at=datetime.now(UTC).isoformat(),
    )


async def test_project_active_run_invariant_is_atomic(repo):
    project = await make_project(repo)
    results = await asyncio.gather(
        *[repo.create_run(project.id, RunCreate()) for _ in range(3)], return_exceptions=True
    )
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(isinstance(result, ActiveRunError) for result in results) == 2
    first = next(result for result in results if not isinstance(result, Exception))
    await repo.update_run(first.id, status="completed")
    assert (await repo.create_run(project.id, RunCreate())).status == "queued"


async def test_snapshots_only_use_verified_history_and_isolate_projects(repo):
    alpha = await make_project(repo)
    beta = await make_project(repo, "Beta")
    first = await repo.create_run(alpha.id, RunCreate())
    original = snapshot(first)
    await repo.save_snapshot(original)
    await repo.save_snapshot(original)
    assert len(await repo.get_snapshots(first.id)) == 1
    assert await repo.search_snapshots(alpha.id, "Pro") == []
    await repo.update_run(first.id, status="completed")
    assert len(await repo.search_snapshots(alpha.id, "Pro")) == 1
    assert await repo.search_snapshots(beta.id, "Pro") == []
    second = await repo.create_run(alpha.id, RunCreate(demo_revision=2))
    previous = await repo.previous_snapshot(alpha.id, original.url, second.id)
    assert previous.id == original.id
    await repo.save_snapshot(
        snapshot(second, text="Bad unverified pricing", identifier="snapshot-2")
    )
    await repo.update_run(second.id, status="failed")
    assert (await repo.search_snapshots(alpha.id, "Pro"))[0].id == original.id
    other = await repo.create_run(beta.id, RunCreate())
    cross_project = original.model_copy(update={"run_id": other.id})
    with pytest.raises(ValueError):
        await repo.save_snapshot(cross_project)


async def test_previous_snapshot_breaks_timestamp_ties_consistently(repo):
    project = await make_project(repo)
    timestamp = "2026-10-22T00:00:00Z"
    revisions = []
    for identifier, body in [
        ("snapshot-a-old", "Obsolete pricing"),
        ("snapshot-z-current", "Current pricing"),
    ]:
        run = await repo.create_run(project.id, RunCreate())
        revision = snapshot(run, text=body, identifier=identifier).model_copy(
            update={"created_at": timestamp}
        )
        await repo.save_snapshot(revision)
        await repo.update_run(run.id, status="completed")
        revisions.append(revision)

    current_run = await repo.create_run(project.id, RunCreate())
    previous = await repo.previous_snapshot(project.id, revisions[0].url, current_run.id)
    assert previous.id == revisions[1].id
    excluded = await repo.previous_snapshot(project.id, revisions[0].url, revisions[1].run_id)
    assert excluded.id == revisions[0].id


async def test_event_cursor_does_not_cross_run_boundary(repo):
    alpha = await make_project(repo)
    beta = await make_project(repo, "Beta")
    a_run = await repo.create_run(alpha.id, RunCreate())
    b_run = await repo.create_run(beta.id, RunCreate())
    first = await repo.append_event(a_run.id, "stage", "start")
    await repo.append_event(b_run.id, "stage", "private")
    last = await repo.append_event(a_run.id, "stage", "end")
    assert [event.id for event in await repo.list_events(a_run.id, after=first.id)] == [last.id]


async def test_recovery_shutdown_and_cancellation_are_durable(repo):
    project = await make_project(repo)
    stale = await repo.create_run(project.id, RunCreate())

    async def runner(run_id, resume=False):
        await asyncio.Event().wait()

    jobs = JobManager(repo, runner)
    await jobs.recover_interrupted()
    assert (await repo.get_run(stale.id)).status == "interrupted"
    current = await repo.create_run(project.id, RunCreate())
    jobs.submit(current.id)
    await jobs.cancel(current.id)
    assert (await repo.get_run(current.id)).status == "cancelled"
    # A late failure cannot overwrite a terminal cancellation.
    await repo.update_run(current.id, status="failed", error="late failure")
    assert (await repo.get_run(current.id)).status == "cancelled"
    pending = await repo.create_run(project.id, RunCreate())
    jobs.submit(pending.id)
    await jobs.shutdown()
    assert (await repo.get_run(pending.id)).status == "interrupted"


async def test_job_failure_is_persisted_and_other_project_continues(repo):
    alpha = await make_project(repo)
    beta = await make_project(repo, "Beta")
    a_run = await repo.create_run(alpha.id, RunCreate())
    b_run = await repo.create_run(beta.id, RunCreate())
    finished = asyncio.Event()

    async def runner(run_id, resume=False):
        if run_id == a_run.id:
            raise RuntimeError("collector unavailable; Authorization: Bearer secret-token")
        await repo.update_run(run_id, status="completed", report={"title": "Done"})
        finished.set()

    jobs = JobManager(repo, runner, max_concurrent_runs=1)
    jobs.submit(a_run.id)
    jobs.submit(b_run.id)
    await asyncio.wait_for(finished.wait(), timeout=3)
    assert (await repo.get_run(a_run.id)).status == "failed"
    assert "secret-token" not in (await repo.get_run(a_run.id)).error
    assert "secret-token" not in str(await repo.list_events(a_run.id))
    assert (await repo.get_run(b_run.id)).status == "completed"
    await jobs.shutdown()


async def test_edit_preserves_history_and_immutable_run_configuration(repo):
    original = await make_project(repo)
    run = await repo.create_run(original.id, RunCreate())
    await repo.save_snapshot(snapshot(run))
    with pytest.raises(ActiveRunError):
        await repo.update_project(
            original.id,
            ProjectCreate(
                name="Edited",
                sources=[{"name": "New source", "url": "https://example.org/new"}],
            ),
        )
    await repo.update_run(run.id, status="completed", report={"title": "Historical result"})
    edited = await repo.update_project(
        original.id,
        ProjectCreate(
            name="Edited",
            description="New focus",
            focus=["positioning"],
            sources=[{"name": "New source", "url": "https://example.org/new"}],
        ),
    )
    assert edited.name == "Edited"
    assert edited.id == original.id and edited.created_at == original.created_at
    assert await repo.get_run_project(run.id) == original
    assert (await repo.get_run(run.id)).report == {"title": "Historical result"}
    assert len(await repo.get_snapshots(run.id)) == 1
    next_run = await repo.create_run(original.id, RunCreate())
    assert await repo.get_run_project(next_run.id) == edited


async def test_edit_and_create_run_are_serialized_across_repository_instances(repo):
    project = await make_project(repo)
    other = Repository(repo.settings)
    payload = ProjectCreate(
        name="Edited", sources=[{"name": "Edited", "url": "https://example.org/new"}]
    )
    try:
        edited, run = await asyncio.gather(
            repo.update_project(project.id, payload),
            other.create_run(project.id, RunCreate()),
            return_exceptions=True,
        )
        assert not isinstance(run, Exception)
        configuration = await other.get_run_project(run.id)
        if isinstance(edited, ActiveRunError):
            assert configuration.name == project.name
        else:
            assert not isinstance(edited, Exception)
            assert configuration.name == "Edited"
            assert configuration.sources == edited.sources
    finally:
        await other.close()
