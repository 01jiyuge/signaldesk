import json
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import delete

from signaldesk.checkpoints import open_checkpointer
from signaldesk.collector import CollectionError, content_hash, demo_page
from signaldesk.config import Settings
from signaldesk.db import Event
from signaldesk.errors import PublicTaskError
from signaldesk.repository import Repository
from signaldesk.research import ResearchEngine, demo_report, verify_report
from signaldesk.retrieval import Retriever
from signaldesk.schemas import (
    Evidence,
    ProjectCreate,
    ProjectRead,
    ResearchReport,
    RunCreate,
    SnapshotRecord,
)
from signaldesk.workflow import Workflow

SOURCES = [
    {"name": "Northstar", "url": "https://northstar.example/pricing"},
    {"name": "Orbit", "url": "https://orbit.example/changelog"},
]


def project_read():
    return ProjectRead(
        id="demo-project",
        name="Optimization",
        sources=SOURCES,
        created_at="2026-10-22T00:00:00Z",
    )


def fixture_evidence(revision=2, previous_revision=1):
    evidence = []
    for index, source in enumerate(SOURCES):
        title, body = demo_page(source["url"], revision)
        previous_id = f"previous-{index}" if previous_revision else None
        evidence.append(
            Evidence(
                id=f"current-{index}",
                source_name=source["name"],
                url=source["url"],
                title=title,
                text=body,
                change_type="baseline"
                if previous_revision is None
                else ("unchanged" if previous_revision == revision else "changed"),
                previous_id=previous_id,
            )
        )
        if previous_revision:
            old_title, old_body = demo_page(source["url"], previous_revision)
            evidence.append(
                Evidence(
                    id=previous_id,
                    source_name=source["name"],
                    url=source["url"],
                    title=old_title,
                    text=old_body,
                    change_type="baseline",
                    is_previous=True,
                )
            )
    return evidence


def test_demo_explains_exact_price_seat_api_and_sso_facts_with_original_quotes():
    evidence = fixture_evidence()
    report = demo_report(project_read(), evidence)
    assert len(report.findings) == 2
    northstar, orbit = report.findings
    assert "$29/月调整为 $39/月" in northstar.detail
    assert "5 个调整为 10 个" in northstar.detail
    assert "新增 API access" in northstar.detail
    assert "不可用变为仅 Enterprise 套餐可用" in orbit.detail
    assert "2026-10-15" in orbit.detail
    assert "--- previous" not in northstar.detail
    verify_report(report, evidence)
    assert len(northstar.citations) == 2
    assert "Price: $29 / month" in northstar.citations[0].quote
    assert "Price: $39 / month" in northstar.citations[1].quote
    assert "SSO: unavailable" == orbit.citations[0].quote
    assert "SSO: Enterprise only" in orbit.citations[1].quote


def test_demo_reverse_revision_does_not_claim_seat_growth_or_new_api():
    evidence = fixture_evidence(revision=1, previous_revision=2)
    report = demo_report(project_read(), evidence)
    detail = report.findings[0].detail
    assert "$39/月调整为 $29/月" in detail
    assert "10 个调整为 5 个" in detail
    assert "不再列出 API access" in detail
    assert "新增 API access" not in detail
    verify_report(report, evidence)


def test_demo_nonadjacent_changed_fields_still_quote_contiguous_original_text():
    evidence = fixture_evidence()
    evidence[0].text = evidence[0].text.replace("Seats: 10", "Seats: 5")
    report = demo_report(project_read(), evidence)
    verify_report(report, evidence)
    northstar = report.findings[0]
    assert "席位由" not in northstar.detail
    assert "Seats: 5" in northstar.citations[0].quote
    assert "Seats: 5" in northstar.citations[1].quote


def test_demo_baseline_and_unchanged_have_distinct_meanings():
    baseline = demo_report(project_read(), fixture_evidence(previous_revision=None))
    assert "首次基线不代表新品发布" in baseline.summary
    assert all("不代表产品刚刚发布" in finding.detail for finding in baseline.findings)
    unchanged = demo_report(project_read(), fixture_evidence(previous_revision=2))
    assert unchanged.findings == []
    assert "未发现变化" in unchanged.summary
    assert "未调用研究模型" in unchanged.summary


@pytest_asyncio.fixture
async def local_repo(tmp_path):
    settings = Settings(
        _env_file=None,
        model="openai:test-only",
        openai_api_key="test-only",
        database_url=f"sqlite+aiosqlite:///{tmp_path}/business.db",
        checkpoint_url=f"sqlite:///{tmp_path}/checkpoints.db",
        enable_vector_search=False,
    )
    repo = Repository(settings)
    await repo.initialize()
    try:
        yield repo, settings
    finally:
        await repo.close()


async def test_engine_skips_unchanged_but_still_researches_baselines_and_changes(
    local_repo, monkeypatch
):
    repo, settings = local_repo
    engine = ResearchEngine(settings, Retriever(repo, settings))
    calls = []

    async def local_research(project, evidence):
        calls.append(evidence)
        return ResearchReport(
            title="Local scripted report",
            summary="test",
            findings=[],
            recommendations=[],
            limitations=[],
        )

    monkeypatch.setattr(engine, "_live", local_research)
    unchanged = await engine.generate(project_read(), fixture_evidence(previous_revision=2), "live")
    assert unchanged.findings == []
    assert calls == []
    await engine.generate(project_read(), fixture_evidence(previous_revision=None), "live")
    await engine.generate(project_read(), fixture_evidence(), "live")
    assert len(calls) == 2


async def test_unchanged_does_not_hide_missing_live_configuration(local_repo):
    repo, _ = local_repo
    settings = Settings(_env_file=None, model="", openai_api_key="")
    engine = ResearchEngine(settings, Retriever(repo, settings))
    with pytest.raises(ValueError, match="MODEL"):
        await engine.generate(project_read(), fixture_evidence(previous_revision=2), "live")


async def seed_baseline(repo, project):
    previous = await repo.create_run(project.id, RunCreate(mode="live"))
    for index, source in enumerate(project.sources):
        title, body = demo_page(str(source.url), 1)
        await repo.save_snapshot(
            SnapshotRecord(
                id=f"baseline-{index}",
                run_id=previous.id,
                project_id=project.id,
                source_name=source.name,
                url=str(source.url),
                title=title,
                text=body,
                content_hash=content_hash(body),
                created_at="2026-10-22T00:00:00Z",
            )
        )
    await repo.update_run(previous.id, status="completed")


@pytest.mark.parametrize("fail_orbit", [False, True])
async def test_workflow_unchanged_skips_research_and_preserves_coverage_stats(
    local_repo,
    monkeypatch,
    fail_orbit,
):
    repo, settings = local_repo
    project = await repo.create_project(ProjectCreate(name="Source check", sources=SOURCES))
    await seed_baseline(repo, project)
    run = await repo.create_run(project.id, RunCreate(mode="live"))

    async def local_fetch(url, settings):
        if fail_orbit and "orbit.example" in url:
            raise CollectionError(
                "来源暂时不可用。",
                code="source_http_error",
                retryable=True,
                suggestion="稍后恢复任务。",
            )
        return demo_page(url, 1)

    async def forbidden_research(*args, **kwargs):
        raise AssertionError("Unchanged sources must not reach the research model")

    async def unchanged_embeddings(snapshots):
        assert snapshots == [], "Unchanged sources must not trigger document embeddings"

    monkeypatch.setattr("signaldesk.workflow.fetch_page", local_fetch)
    async with open_checkpointer(settings.checkpoint_url) as checkpointer:
        workflow = Workflow(repo, settings, checkpointer, Retriever(repo, settings))
        monkeypatch.setattr(workflow.research, "generate", forbidden_research)
        monkeypatch.setattr(workflow.retriever, "index_snapshots", unchanged_embeddings)
        await workflow.run(run.id)
    saved = await repo.get_run(run.id)
    assert saved.status == "completed"
    assert saved.report["findings"] == []
    assert saved.report["mode"] == "live"
    assert saved.report["stats"]["model_call_skipped"] is True
    assert saved.report["stats"]["unchanged_sources"] == (1 if fail_orbit else 2)
    assert saved.report["stats"]["failed_sources"] == int(fail_orbit)
    assert len([item for item in saved.report["evidence"] if not item["is_previous"]]) == (
        1 if fail_orbit else 2
    )
    events = await repo.list_events(run.id)
    assert any(event.kind == "research_skipped" for event in events)
    if fail_orbit:
        assert "1 个来源未成功读取，无法判断" in saved.report["summary"]
        assert any("Orbit" in limitation for limitation in saved.report["limitations"])
        warning = next(event for event in events if event.kind == "warning")
        assert warning.data["source_name"] == "Orbit"
        assert warning.data["code"] == "source_http_error"
        assert warning.data["retryable"] is True
        assert warning.data["suggestion"] == "稍后恢复任务。"


async def test_unexpected_source_errors_do_not_leak_provider_secrets(local_repo, monkeypatch):
    repo, settings = local_repo
    project = await repo.create_project(ProjectCreate(name="Safe failures", sources=SOURCES))
    run = await repo.create_run(project.id, RunCreate(mode="live"))

    async def private_error(*args):
        raise RuntimeError("Authorization: Bearer sk-private-secret; source body private")

    monkeypatch.setattr("signaldesk.workflow.fetch_page", private_error)
    workflow = Workflow(repo, settings, None, Retriever(repo, settings))
    with pytest.raises(CollectionError) as caught:
        await workflow._collect({"run_id": run.id})
    assert caught.value.code == "all_sources_failed"
    events = await repo.list_events(run.id)
    public = json.dumps([event.model_dump() for event in events], ensure_ascii=False)
    assert "sk-private-secret" not in public
    assert "source body private" not in public
    warnings = [event for event in events if event.kind == "warning"]
    assert len(warnings) == 2
    assert all(event.data["code"] and event.data["suggestion"] for event in warnings)


@pytest.mark.parametrize("context_change", ["focus", "description", "legacy"])
async def test_unchanged_content_still_researches_changed_or_unknown_context(
    local_repo,
    monkeypatch,
    context_change,
):
    repo, settings = local_repo
    project = await repo.create_project(ProjectCreate(name="Context check", sources=SOURCES))
    await seed_baseline(repo, project)
    if context_change == "legacy":
        previous = (await repo.list_runs(project.id))[0]
        async with repo.sessions.begin() as session:
            await session.execute(
                delete(Event).where(
                    Event.run_id == previous.id,
                    Event.kind == "configuration",
                )
            )
    else:
        payload = ProjectCreate.model_validate(project.model_dump())
        if context_change == "focus":
            payload.focus = ["positioning"]
        else:
            payload.description = "只检查 Enterprise 购买条件。"
        project = await repo.update_project(project.id, payload)
    run = await repo.create_run(project.id, RunCreate(mode="live"))
    calls = []

    async def local_fetch(url, settings):
        return demo_page(url, 1)

    async def local_research(project, evidence):
        calls.append(project)
        return ResearchReport(
            title="Context reviewed",
            summary="依据当前关注点分析相同正文。",
            findings=[],
            recommendations=[],
            limitations=[],
        )

    monkeypatch.setattr("signaldesk.workflow.fetch_page", local_fetch)
    async with open_checkpointer(settings.checkpoint_url) as checkpointer:
        workflow = Workflow(repo, settings, checkpointer, Retriever(repo, settings))
        monkeypatch.setattr(workflow.research, "_live", local_research)
        await workflow.run(run.id)
    assert len(calls) == 1
    if context_change == "focus":
        assert calls[0].focus == ["positioning"]
    saved = await repo.get_run(run.id)
    assert saved.status == "completed"
    assert saved.report["stats"]["unchanged_sources"] == 2
    assert saved.report["stats"]["model_call_skipped"] is False
    assert not any(event.kind == "research_skipped" for event in await repo.list_events(run.id))


async def test_resume_rejects_incomplete_checkpoint_report(local_repo, monkeypatch):
    repo, settings = local_repo
    project = await repo.create_project(ProjectCreate(name="Bad checkpoint", sources=SOURCES))
    run = await repo.create_run(project.id, RunCreate())
    workflow = Workflow(repo, settings, None, Retriever(repo, settings))
    draft = ResearchReport(
        title="Draft", summary="Not persisted", findings=[], recommendations=[], limitations=[]
    ).model_dump()

    async def invalid_state(*args):
        return SimpleNamespace(values={"report": draft}, next=())

    monkeypatch.setattr(workflow.graph, "aget_state", invalid_state)
    with pytest.raises(PublicTaskError) as caught:
        await workflow.run(run.id, resume=True)
    assert caught.value.code == "checkpoint_report_invalid"
    assert (await repo.get_run(run.id)).status != "completed"
