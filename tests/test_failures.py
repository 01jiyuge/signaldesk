import asyncio

import pytest

from signaldesk.collector import CollectionError, demo_page
from signaldesk.config import Settings
from signaldesk.errors import PublicTaskError, public_failure
from signaldesk.jobs import JobManager
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate, RunCreate


def test_failure_messages_are_actionable_and_do_not_copy_provider_secrets():
    private = RuntimeError("Authorization: Bearer secret-key-in-provider-response")
    result = public_failure(private)
    assert result.code == "internal_error"
    assert "secret-key" not in result.message and "secret-key" not in result.suggestion
    assert public_failure(TimeoutError()).code == "research_timeout"
    assert public_failure(TimeoutError()).retryable
    known = public_failure(
        PublicTaskError(
            "来源无法读取。", code="source_down", retryable=True, suggestion="稍后重试。"
        )
    )
    assert known.message == "来源无法读取。" and known.suggestion == "稍后重试。"


def test_unknown_demo_sources_fail_explicitly():
    with pytest.raises(CollectionError) as result:
        demo_page("https://example.com/", 1)
    assert result.value.code == "unsupported_demo_source"
    assert not result.value.retryable


async def test_task_failure_persists_safe_diagnostics(tmp_path):
    repo = Repository(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/safe.db")
    )
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(name="Errors", sources=[{"name": "Source", "url": "https://example.com/"}])
    )
    run = await repo.create_run(project.id, RunCreate())

    async def fail(*args, **kwargs):
        raise PublicTaskError(
            "来源解析被阻止。",
            code="source_address_blocked",
            retryable=False,
            suggestion="检查代理 DNS。",
        )

    jobs = JobManager(repo, fail)
    try:
        jobs.submit(run.id)
        for _ in range(100):
            saved = await repo.get_run(run.id)
            if saved.status == "failed":
                break
            await asyncio.sleep(0.01)
        assert saved.status == "failed" and saved.error == "来源解析被阻止。"
        await asyncio.gather(*list(jobs._tasks.values()), return_exceptions=True)
        events = await repo.list_events(run.id)
        error = next(item for item in events if item.kind == "error")
        assert error.data["code"] == "source_address_blocked"
        assert error.data["suggestion"] == "检查代理 DNS。"
        assert error.data["retryable"] is False
    finally:
        await jobs.shutdown()
        await repo.close()


async def test_completed_report_is_not_reclassified_when_final_event_fails(tmp_path):
    repo = Repository(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/done.db")
    )
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(name="Completed", sources=[{"name": "Source", "url": "https://example.com/"}])
    )
    run = await repo.create_run(project.id, RunCreate())
    finished = asyncio.Event()

    async def finish_then_fail(run_id, **kwargs):
        await repo.update_run(run_id, status="completed", report={"title": "Saved report"})
        finished.set()
        raise RuntimeError("An event write failed after committing the report")

    jobs = JobManager(repo, finish_then_fail)
    try:
        jobs.submit(run.id)
        await asyncio.wait_for(finished.wait(), timeout=5)
        await asyncio.gather(*list(jobs._tasks.values()), return_exceptions=True)
        saved = await repo.get_run(run.id)
        assert saved.status == "completed" and saved.report["title"] == "Saved report"
        with pytest.raises(ValueError):
            await jobs.cancel(run.id)
    finally:
        await jobs.shutdown()
        await repo.close()


def test_project_input_rejects_blank_names_and_duplicate_fragment_urls():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ProjectCreate(name="   ", sources=[{"name": "x", "url": "https://example.com/"}])
    with pytest.raises(ValidationError):
        ProjectCreate(
            name="x",
            sources=[
                {"name": "A", "url": "https://example.com/page#pricing"},
                {"name": "B", "url": "https://example.com/page#features"},
            ],
        )
    with pytest.raises(ValidationError):
        ProjectCreate(name="x", sources=[{"name": "A", "url": "https://user:pass@example.com/"}])
