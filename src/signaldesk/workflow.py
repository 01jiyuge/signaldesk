"""Deterministic collection and verification around an autonomous research node."""

import asyncio
import difflib
from datetime import UTC, datetime
from typing import TypedDict
from uuid import NAMESPACE_URL, uuid5

from langgraph.graph import END, START, StateGraph

from signaldesk.collector import CollectionError, content_hash, demo_page, fetch_page
from signaldesk.config import Settings
from signaldesk.errors import PublicTaskError, public_failure
from signaldesk.memory import ProjectMemory
from signaldesk.repository import Repository
from signaldesk.research import (
    ResearchEngine,
    all_sources_unchanged,
    unchanged_report,
    verify_report,
)
from signaldesk.retrieval import Retriever
from signaldesk.schemas import Evidence, FinalReport, ProjectRead, ResearchReport, SnapshotRecord


class ResearchState(TypedDict, total=False):
    run_id: str
    snapshots: list[dict]
    evidence: list[dict]
    warnings: list[str]
    context_changed: bool
    report: dict
    memory_context: dict


def _research_context(project: ProjectRead) -> dict:
    return {
        "description": project.description,
        "focus": sorted(project.focus),
        "sources": sorted(str(source.url) for source in project.sources),
    }


class Workflow:
    def __init__(
        self,
        repo: Repository,
        settings: Settings,
        checkpointer,
        retriever: Retriever,
        *,
        store=None,
        memory: ProjectMemory | None = None,
        sandbox=None,
        delegate=None,
    ):
        self.repo = repo
        self.settings = settings
        self.retriever = retriever
        self.memory = memory
        self.research = ResearchEngine(
            settings, retriever, store=store, sandbox=sandbox, delegate=delegate
        )
        builder = StateGraph(ResearchState)
        for name in ("collect", "compare", "research", "verify", "persist"):
            builder.add_node(name, getattr(self, f"_{name}"))
        builder.add_edge(START, "collect")
        for left, right in zip(
            ["collect", "compare", "research", "verify"],
            ["compare", "research", "verify", "persist"],
            strict=True,
        ):
            builder.add_edge(left, right)
        builder.add_edge("persist", END)
        self.graph = builder.compile(checkpointer=checkpointer, store=store)

    async def run(self, run_id: str, resume: bool = False) -> None:
        run = await self.repo.get_run(run_id)
        if run is None:
            raise PublicTaskError("任务不存在。", code="run_not_found")
        config = {
            "configurable": {"thread_id": run_id},
            "metadata": {"run_id": run_id, "application": "signaldesk"},
        }
        state = await self.graph.aget_state(config) if resume else None
        if state and state.values and not state.next:
            # The process may stop after the final checkpoint but before the job state is seen.
            report = state.values.get("report")
            if report:
                try:
                    final = FinalReport.model_validate(report)
                    if final.mode != run.mode or not any(
                        not item.is_previous for item in final.evidence
                    ):
                        raise ValueError("Checkpoint report does not match the run")
                    verify_report(final, final.evidence)
                except ValueError as exc:
                    raise PublicTaskError(
                        "检查点中的报告不完整，不能标记为已完成。",
                        code="checkpoint_report_invalid",
                        suggestion="请新建研究任务，保留当前运行供排查。",
                    ) from exc
                await self.repo.update_run(
                    run_id,
                    status="completed",
                    stage="completed",
                    report=final.model_dump(),
                    error=None,
                )
                return
        payload = None if state and state.values and state.next else {"run_id": run_id}
        await self.graph.ainvoke(payload, config=config, durability="sync")

    async def _stage(self, run_id: str, stage: str, message: str) -> None:
        await self.repo.update_run(run_id, stage=stage)
        await self.repo.append_event(run_id, "stage", message, {"stage": stage})

    async def _collect(self, state: ResearchState) -> dict:
        run = await self.repo.get_run(state["run_id"])
        if run is None:
            raise ValueError("任务不存在。")
        project = await self.repo.get_run_project(run.id)
        if project is None:
            raise ValueError("项目不存在。")
        memory_context = {}
        if self.memory is not None:
            events = await self.repo.list_events(run.id)
            saved_memory = next((event for event in events if event.kind == "memory_context"), None)
            if saved_memory:
                memory_context = saved_memory.data.get("context", {})
            else:
                memory_context = await self.memory.load_context(project.id, limit=5)
                await self.repo.append_event(
                    run.id,
                    "memory_context",
                    "已固定本次任务的项目记忆上下文。",
                    {"context": memory_context},
                )
        await self._stage(
            run.id, "collect", "读取演示夹具。" if run.mode == "demo" else "采集公开来源。"
        )
        existing = {snapshot.url: snapshot for snapshot in await self.repo.get_snapshots(run.id)}
        semaphore = asyncio.Semaphore(3)

        async def collect(source):
            url = str(source.url)
            if url in existing:
                return existing[url], None
            try:
                async with semaphore:
                    if run.mode == "demo":
                        title, text = demo_page(url, run.demo_revision)
                    else:
                        title, text = await fetch_page(url, self.settings)
            except Exception as exc:
                # Only application-authored errors are public. A provider/client
                # exception may include secrets or source bodies in its message.
                failure = public_failure(exc)
                return None, {
                    "source_name": source.name,
                    "message": failure.message,
                    "code": failure.code,
                    "retryable": failure.retryable,
                    "suggestion": failure.suggestion,
                }
            snapshot = SnapshotRecord(
                id=str(uuid5(NAMESPACE_URL, f"{run.id}:{url}")),
                run_id=run.id,
                project_id=project.id,
                source_name=source.name,
                url=url,
                title=title,
                text=text,
                content_hash=content_hash(text),
                created_at=datetime.now(UTC).isoformat(),
            )
            # Keep persistence failures outside the source adapter exception
            # boundary so TaskGroup cancels and joins sibling database writes.
            await self.repo.save_snapshot(snapshot)
            return snapshot, None

        # Cancel and join sibling collectors on unexpected failure or cancellation.
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(collect(source)) for source in project.sources]
        results = [task.result() for task in tasks]
        snapshots = [snapshot.model_dump() for snapshot, _ in results if snapshot is not None]
        failures = [failure for _, failure in results if failure]
        warnings = [f"{failure['source_name']}：{failure['message']}" for failure in failures]
        for failure, warning in zip(failures, warnings, strict=True):
            await self.repo.append_event(run.id, "warning", warning, failure)
        if not snapshots:
            raise CollectionError(
                "所有来源均未成功读取。",
                code="all_sources_failed",
                retryable=any(failure["retryable"] for failure in failures),
                suggestion="查看逐来源错误与建议；修改来源后请新建任务，临时网络故障可稍后恢复。",
            )
        await self.repo.append_event(run.id, "collected", f"已保存 {len(snapshots)} 份来源快照。")
        return {"snapshots": snapshots, "warnings": warnings, "memory_context": memory_context}

    async def _compare(self, state: ResearchState) -> dict:
        await self._stage(state["run_id"], "compare", "对比上一次成功任务的来源内容。")
        run = await self.repo.get_run(state["run_id"])
        project = await self.repo.get_run_project(run.id)
        if project is None:
            raise PublicTaskError("任务的项目配置不存在。", code="project_not_found")
        current_events = await self.repo.list_events(run.id)
        context_changed = run.mode == "live" and not any(
            event.kind == "configuration" for event in current_events
        )
        previous_contexts: dict[str, dict | None] = {}
        evidence = []
        for data in state["snapshots"]:
            snapshot = SnapshotRecord.model_validate(data)
            previous = await self.repo.previous_snapshot(
                snapshot.project_id, snapshot.url, snapshot.run_id
            )
            if run.mode == "live" and previous:
                if previous.run_id not in previous_contexts:
                    events = await self.repo.list_events(previous.run_id)
                    previous_project = (
                        await self.repo.get_run_project(previous.run_id)
                        if any(event.kind == "configuration" for event in events)
                        else None
                    )
                    previous_contexts[previous.run_id] = (
                        _research_context(previous_project) if previous_project else None
                    )
                if previous_contexts[previous.run_id] != _research_context(project):
                    context_changed = True
                if self.memory is not None:
                    previous_events = await self.repo.list_events(previous.run_id)
                    previous_memory = next(
                        (
                            event.data.get("context", {})
                            for event in previous_events
                            if event.kind == "memory_context"
                        ),
                        {},
                    )

                    def preferences(context):
                        return sorted(
                            [
                                {"key": item["key"], "value": item["value"]}
                                for item in context.get("preferences", [])
                            ],
                            key=lambda item: item["key"],
                        )

                    if preferences(previous_memory) != preferences(state.get("memory_context", {})):
                        context_changed = True
            change = (
                "baseline"
                if previous is None
                else ("unchanged" if previous.content_hash == snapshot.content_hash else "changed")
            )
            diff = ""
            if previous and change == "changed":
                diff = "\n".join(
                    difflib.unified_diff(
                        previous.text.splitlines(),
                        snapshot.text.splitlines(),
                        fromfile="previous",
                        tofile="current",
                        lineterm="",
                    )
                )[:12000]
            evidence.append(
                Evidence(
                    id=snapshot.id,
                    source_name=snapshot.source_name,
                    url=snapshot.url,
                    title=snapshot.title,
                    text=snapshot.text,
                    change_type=change,
                    previous_id=previous.id if previous else None,
                    diff=diff,
                    observed_at=snapshot.created_at,
                ).model_dump()
            )
            if previous:
                evidence.append(
                    Evidence(
                        id=previous.id,
                        source_name=f"{previous.source_name}（历史快照）",
                        url=previous.url,
                        title=previous.title,
                        text=previous.text,
                        change_type="baseline",
                        observed_at=previous.created_at,
                        is_previous=True,
                    ).model_dump()
                )
        return {"evidence": evidence, "context_changed": context_changed}

    async def _research(self, state: ResearchState) -> dict:
        run = await self.repo.get_run(state["run_id"])
        project = await self.repo.get_run_project(run.id)
        if project is None:
            raise PublicTaskError("任务的项目配置不存在。", code="project_not_found")
        evidence = [Evidence.model_validate(item) for item in state["evidence"]]
        self.research.memory_contexts[project.id] = state.get("memory_context", {})
        context_changed = state.get("context_changed", run.mode == "live")
        skip_model = all_sources_unchanged(evidence) and not context_changed
        await self._stage(
            run.id,
            "research",
            "来源正文未变化，跳过模型研究。"
            if skip_model
            else ("生成演示分析。" if run.mode == "demo" else "Deep Agents 正在研究证据。"),
        )
        if skip_model:
            if run.mode == "live":
                self.settings.validate_live()
            report = unchanged_report(project, evidence, run.mode)
            await self.repo.append_event(
                run.id,
                "research_skipped",
                "已采集来源正文均与上次成功快照一致，本次未调用研究模型。",
                {"code": "sources_unchanged", "model_call_skipped": True},
            )
        else:
            origin = any(
                event.kind == "a2a_origin" for event in await self.repo.list_events(run.id)
            )
            token = self.research.execution_context.set(
                {"run_id": run.id, "allow_delegation": not origin}
            )
            try:
                if context_changed:
                    report = await self.research.generate(
                        project, evidence, run.mode, force_research=True
                    )
                else:
                    report = await self.research.generate(project, evidence, run.mode)
            finally:
                self.research.execution_context.reset(token)
        report.limitations.extend(state.get("warnings", []))
        return {"report": report.model_dump()}

    async def _verify(self, state: ResearchState) -> dict:
        await self._stage(state["run_id"], "verify", "检查引用 ID 与原文引句的一致性。")
        report = ResearchReport.model_validate(state["report"])
        evidence = [Evidence.model_validate(item) for item in state["evidence"]]
        verify_report(report, evidence)
        return {"report": report.model_dump()}

    async def _persist(self, state: ResearchState) -> dict:
        run = await self.repo.get_run(state["run_id"])
        await self._stage(run.id, "persist", "保存报告与可追溯证据。")
        evidence = [Evidence.model_validate(item) for item in state["evidence"]]
        current = [item for item in evidence if not item.is_previous]
        project = await self.repo.get_run_project(run.id)
        if project is None:
            raise PublicTaskError("任务的项目配置不存在。", code="project_not_found")
        report = FinalReport(
            **ResearchReport.model_validate(state["report"]).model_dump(),
            evidence=evidence,
            mode=run.mode,
            stats={
                "total_sources": len(project.sources),
                "collected_sources": len(current),
                "failed_sources": max(0, len(project.sources) - len(current)),
                "baseline_sources": sum(item.change_type == "baseline" for item in current),
                "changed_sources": sum(item.change_type == "changed" for item in current),
                "unchanged_sources": sum(item.change_type == "unchanged" for item in current),
                "model_call_skipped": run.mode == "demo"
                or (all_sources_unchanged(evidence) and not state.get("context_changed", True)),
            },
        ).model_dump()
        # Vector indexing is optional and never calls an embedding API in demo mode.
        if run.mode == "live":
            changed_ids = {item.id for item in current if item.change_type != "unchanged"}
            await self.retriever.index_snapshots(
                [
                    SnapshotRecord.model_validate(item)
                    for item in state["snapshots"]
                    if item["id"] in changed_ids
                ]
            )
        await self.repo.update_run(
            run.id, report=report, status="completed", stage="completed", error=None
        )
        if self.memory is not None:
            try:
                await self.memory.remember_run(
                    run.project_id,
                    run.id,
                    {
                        "title": report["title"][:200],
                        "summary": report["summary"][:600],
                        "finding_count": len(report["findings"]),
                        "mode": run.mode,
                        "citation_checked": True,
                        "facts_verified": False,
                    },
                )
            except Exception:
                await self.repo.append_event(
                    run.id,
                    "warning",
                    "报告已保存，但写入长期记忆失败。",
                    {
                        "code": "memory_write_failed",
                        "retryable": True,
                        "suggestion": "检查 Store 连接；报告和证据已保留。",
                    },
                )
        await self.repo.append_event(
            run.id, "completed", "报告已保存。", {"finding_count": len(report["findings"])}
        )
        return {"report": report}
