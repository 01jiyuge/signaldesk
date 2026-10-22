"""Deterministic fixture quality evaluation, with explicit opt-in metric uploads.

This is a benchmark runner with labeled expectations, not an LLM judge or a
measurement of live-model quality. Default execution never creates a cloud client.
"""

import asyncio
import copy
import json
import os
import re
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from langsmith import tracing_context

from signaldesk import __version__
from signaldesk.checkpoints import open_checkpointer
from signaldesk.collector import demo_page
from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.research import CitationError, demo_report, verify_report
from signaldesk.retrieval import Retriever
from signaldesk.schemas import Evidence, ProjectCreate, ProjectRead, ResearchReport, RunCreate
from signaldesk.workflow import Workflow

FIXTURE_SOURCES = [
    {"name": "Northstar", "url": "https://northstar.example/pricing"},
    {"name": "Orbit", "url": "https://orbit.example/changelog"},
]
SCENARIOS = {
    "baseline",
    "change",
    "nochange",
    "citation_invalid_id",
    "citation_invalid_quote",
    "coverage",
    "project_scope",
    "context_change",
}
LIMITATIONS = [
    "这是固定虚构夹具的确定性质量回归，不是远端真实模型效果评测。",
    "引用ID和原文匹配不等于事实真实性、证据蕴含关系或因果推断已被验证。",
    "事实字段采用固定夹具文本规则核对，不是通用语义评分；人工事实支持率尚未评测。",
    "context_change使用本地脚本研究函数，只验证工作流决策，不声称评估模型推理。",
]
METRIC_KEYS = {
    "finding_count",
    "baseline_sources",
    "changed_sources",
    "unchanged_sources",
    "collected_sources",
    "failed_sources",
    "total_sources",
    "model_call_skipped",
    "citation_ids_valid",
    "citation_quotes_valid",
    "citation_rejected",
    "citation_count",
    "valid_citation_id_count",
    "valid_quote_count",
    "baseline_is_initial",
    "no_change",
    "coverage_limited",
    "project_scope_ok",
    "search_result_count",
    "context_changed",
    "research_calls",
    "execution_succeeded",
}
FACT_KEYS = {
    "price_before",
    "price_after",
    "seats_before",
    "seats_after",
    "api_added",
    "sso_enterprise_added",
}


def load_cases(path: Path | None = None) -> dict:
    if path is None:
        source_path = Path(__file__).resolve().parents[2] / "evals" / "quality_cases.json"
        path = (
            source_path
            if source_path.is_file()
            else Path(__file__).parent / "evaluation_data" / "quality_cases.json"
        )
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1 or not document.get("cases"):
        raise ValueError("评测样本必须使用schema_version=1，且至少包含一个场景。")
    seen = set()
    for case in document["cases"]:
        if case.get("scenario") not in SCENARIOS or not isinstance(case.get("expected"), dict):
            raise ValueError("评测场景或期望结构无效。")
        if not re.fullmatch(r"[a-z0-9_]{1,80}", case.get("id", "")) or case["id"] in seen:
            raise ValueError("评测场景ID必须唯一，并仅包含小写字母、数字和下划线。")
        seen.add(case["id"])
    return document


def _citation_metrics(report: ResearchReport, evidence: list[Evidence]) -> dict:
    indexed = {item.id: item for item in evidence}
    citations = [citation for finding in report.findings for citation in finding.citations]
    valid_ids = sum(citation.evidence_id in indexed for citation in citations)
    valid_quotes = sum(
        citation.evidence_id in indexed
        and " ".join(citation.quote.split()) in " ".join(indexed[citation.evidence_id].text.split())
        for citation in citations
    )
    return {
        "citation_count": len(citations),
        "valid_citation_id_count": valid_ids,
        "valid_quote_count": valid_quotes,
        "citation_ids_valid": valid_ids == len(citations),
        "citation_quotes_valid": valid_quotes == len(citations),
    }


def _fact_metrics(report: ResearchReport) -> dict:
    detail = "\n".join(item.detail for item in report.findings)
    price = re.search(r"月付价格由 \$(\d+)/月调整为 \$(\d+)/月", detail)
    seats = re.search(r"包含席位由 (\d+) 个调整为 (\d+) 个", detail)
    return {
        "price_before": int(price.group(1)) if price else None,
        "price_after": int(price.group(2)) if price else None,
        "seats_before": int(seats.group(1)) if seats else None,
        "seats_after": int(seats.group(2)) if seats else None,
        "api_added": "新增 API access" in detail,
        "sso_enterprise_added": "SSO 从不可用变为仅 Enterprise 套餐可用" in detail,
    }


def _report_metrics(report_data: dict) -> dict:
    report = ResearchReport.model_validate(report_data)
    evidence = [Evidence.model_validate(item) for item in report_data.get("evidence", [])]
    current = [item for item in evidence if not item.is_previous]
    stats = report_data.get("stats") or {}
    failed = stats.get("failed_sources", 0)
    return {
        "finding_count": len(report.findings),
        **{
            key: stats.get(key, 0)
            for key in (
                "baseline_sources",
                "changed_sources",
                "unchanged_sources",
                "collected_sources",
                "failed_sources",
                "total_sources",
            )
        },
        "model_call_skipped": stats.get("model_call_skipped"),
        **_citation_metrics(report, evidence),
        "fact_fields": _fact_metrics(report),
        "baseline_is_initial": bool(report.findings)
        and all("不代表产品刚刚发布" in item.detail for item in report.findings),
        "no_change": not report.findings
        and bool(current)
        and all(item.change_type == "unchanged" for item in current)
        and "未发现变化" in report.summary,
        "coverage_limited": failed > 0
        and bool(report.limitations)
        and "未成功读取" in report.summary,
    }


def _fixture_project() -> ProjectRead:
    return ProjectRead(
        id="fixture-only",
        name="离线质量评测",
        sources=FIXTURE_SOURCES,
        created_at="2026-10-22T00:00:00Z",
    )


def _citation_case(scenario: str) -> dict:
    evidence = []
    for index, source in enumerate(FIXTURE_SOURCES):
        title, text = demo_page(source["url"], 1)
        evidence.append(
            Evidence(
                id=f"fixture-evidence-{index}",
                source_name=source["name"],
                url=source["url"],
                title=title,
                text=text,
                change_type="baseline",
            )
        )
    report = demo_report(_fixture_project(), evidence)
    citation = report.findings[0].citations[0]
    if scenario == "citation_invalid_id":
        citation.evidence_id = "nonexistent-fixture-id"
    else:
        citation.quote = "This invented quotation is absent from every fixture."
    rejected = False
    try:
        verify_report(report, evidence)
    except CitationError:
        rejected = True
    return {**_citation_metrics(report, evidence), "citation_rejected": rejected}


async def _new_demo_run(repo: Repository, workflow: Workflow, project_id: str, revision: int):
    run = await repo.create_run(project_id, RunCreate(mode="demo", demo_revision=revision))
    await workflow.run(run.id)
    return await repo.get_run(run.id)


async def _scripted_nodes(repo, workflow, project, baseline, scenario, parameters):
    if scenario == "context_change":
        payload = ProjectCreate.model_validate(project.model_dump())
        if parameters.get("field", "focus") == "description":
            payload.description = "只分析Enterprise购买条件（固定评测说明）。"
        else:
            payload.focus = ["positioning"]
        project = await repo.update_project(project.id, payload)
    run = await repo.create_run(project.id, RunCreate(mode="live"))
    snapshots = []
    for old in await repo.get_snapshots(baseline.id):
        current = old.model_copy(
            update={
                "id": str(uuid5(NAMESPACE_URL, f"{run.id}:{old.url}")),
                "run_id": run.id,
                "created_at": datetime.now(UTC).isoformat(),
            }
        )
        await repo.save_snapshot(current)
        snapshots.append(current.model_dump())
    calls = 0

    async def scripted_research(project, evidence):
        nonlocal calls
        calls += 1
        return ResearchReport(
            title="本地脚本分支结果",
            summary="固定研究替身，仅验证分支决策。",
            findings=[],
            recommendations=[],
            limitations=["未调用真实模型，不评价推理质量。"],
        )

    workflow.research._live = scripted_research
    state = {"run_id": run.id, "snapshots": snapshots, "warnings": []}
    for node in (workflow._compare, workflow._research, workflow._verify, workflow._persist):
        state.update(await node(state))
    saved = await repo.get_run(run.id)
    return {
        **_report_metrics(saved.report),
        "research_calls": calls,
        "context_changed": state["context_changed"],
    }


async def _workflow_case(case: dict) -> dict:
    scenario = case["scenario"]
    with tempfile.TemporaryDirectory(prefix="signaldesk-eval-") as directory:
        settings = Settings(
            _env_file=None,
            database_url=f"sqlite+aiosqlite:///{directory}/business.db",
            checkpoint_url=f"sqlite:///{directory}/checkpoints.db",
            data_dir=Path(directory),
            model="openai:local-scripted-fixture",
            openai_api_key="fixture-not-a-real-key",
            enable_vector_search=False,
            enable_fulltext_search=True,
            reranker_provider="none",
            mcp_urls=[],
            mcp_allowed_tools=[],
        )
        repo = Repository(settings)
        await repo.initialize()
        retriever = Retriever(repo, settings)
        try:
            await retriever.initialize()
            sources = copy.deepcopy(FIXTURE_SOURCES)
            if scenario == "coverage":
                sources.append(
                    {"name": "UnavailableFixture", "url": "https://missing.example/unknown"}
                )
            project = await repo.create_project(ProjectCreate(name="固定评测项目", sources=sources))
            async with open_checkpointer(settings.checkpoint_url) as checkpointer:
                workflow = Workflow(repo, settings, checkpointer, retriever)
                baseline = await _new_demo_run(repo, workflow, project.id, 1)
                if scenario in {"nochange", "context_change"}:
                    return await _scripted_nodes(
                        repo,
                        workflow,
                        project,
                        baseline,
                        scenario,
                        case.get("parameters", {}),
                    )
                if scenario == "project_scope":
                    other = await repo.create_project(
                        ProjectCreate(name="另一个评测项目", sources=sources)
                    )
                    other_run = await _new_demo_run(repo, workflow, other.id, 1)
                    allowed_ids = {item.id for item in await repo.get_snapshots(baseline.id)}
                    other_ids = {item.id for item in await repo.get_snapshots(other_run.id)}
                    hits = await workflow.retriever.search(project.id, "SSO", limit=10)
                    ids = {item.evidence_id for item in hits}
                    return {
                        "project_scope_ok": bool(ids)
                        and ids <= allowed_ids
                        and ids.isdisjoint(other_ids),
                        "search_result_count": len(hits),
                    }
                saved = baseline
                if scenario in {"change", "coverage"}:
                    saved = await _new_demo_run(
                        repo, workflow, project.id, 2 if scenario == "change" else 1
                    )
                return _report_metrics(saved.report)
        finally:
            await retriever.close()
            await repo.close()


async def observe_case(case: dict) -> dict:
    # A configured global LANGSMITH_TRACING must never turn offline fixture runs
    # into implicit uploads of source text, graph state, or settings.
    with tracing_context(enabled=False):
        if case["scenario"].startswith("citation_invalid_"):
            return _citation_case(case["scenario"])
        return await _workflow_case(case)


def _checks(expected: dict, observed: dict, prefix: str = "") -> list[dict]:
    checks = []
    for key, wanted in expected.items():
        name = f"{prefix}.{key}" if prefix else key
        actual = observed.get(key)
        if isinstance(wanted, dict):
            checks.extend(_checks(wanted, actual if isinstance(actual, dict) else {}, name))
        else:
            checks.append(
                {
                    "metric": name,
                    "expected": wanted,
                    "observed": actual,
                    "pass": actual == wanted
                    and isinstance(actual, bool) == isinstance(wanted, bool),
                }
            )
    return checks


async def evaluate_cases(document: dict | None = None) -> dict:
    document = document or await asyncio.to_thread(load_cases)
    started = time.perf_counter()
    results = []
    for case in document["cases"]:
        try:
            observed = await observe_case(case)
            observed["execution_succeeded"] = True
        except Exception as exc:
            # Never write raw provider errors or source bodies to an eval artifact.
            observed = {"execution_succeeded": False, "error_type": type(exc).__name__}
        expected = {"execution_succeeded": True, **case["expected"]}
        checks = _checks(expected, observed)
        results.append(
            {
                "id": case["id"],
                "scenario": case["scenario"],
                "description": case.get("description", ""),
                "expected": expected,
                "observed": observed,
                "checks": checks,
                "pass": all(item["pass"] for item in checks),
                "execution": "application_nodes_with_scripted_research"
                if case["scenario"] in {"nochange", "context_change"}
                else "application_citation_validator"
                if case["scenario"].startswith("citation_invalid_")
                else "full_demo_langgraph",
            }
        )
    checks = [check for result in results for check in result["checks"]]
    groups = {
        "citations": [item for item in checks if item["metric"].startswith("citation_")],
        "fixture_facts": [item for item in checks if item["metric"].startswith("fact_fields.")],
        "change_counts": [
            item
            for item in checks
            if item["metric"]
            in {
                "finding_count",
                "baseline_sources",
                "changed_sources",
                "unchanged_sources",
                "no_change",
            }
        ],
        "scope_and_coverage": [
            item
            for item in checks
            if item["metric"]
            in {
                "project_scope_ok",
                "total_sources",
                "collected_sources",
                "failed_sources",
                "coverage_limited",
            }
        ],
        "research_decisions": [
            item
            for item in checks
            if item["metric"]
            in {
                "context_changed",
                "model_call_skipped",
                "research_calls",
            }
        ],
    }
    return {
        "schema_version": 1,
        "suite": document.get("suite", "fixture-quality"),
        "evaluation_type": "offline_fixture_regression",
        "application_version": __version__,
        "created_at": datetime.now(UTC).isoformat(),
        "summary": {
            "scenarios": len(results),
            "passed": sum(result["pass"] for result in results),
            "failed": sum(not result["pass"] for result in results),
            "checks": len(checks),
            "checks_passed": sum(check["pass"] for check in checks),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "metric_groups": {
                name: {
                    "expected_checks": len(items),
                    "matched": sum(item["pass"] for item in items),
                }
                for name, items in groups.items()
            },
            "remote_model_quality_evaluated": False,
            "human_fact_support_rate": {"status": "not_evaluated", "value": None},
        },
        "scenarios": results,
        "limitations": LIMITATIONS.copy(),
        "langsmith": {"uploaded": False, "experiment_url": None},
    }


def _upload_metrics(metrics: dict) -> dict:
    # Only allow known numeric/boolean metrics. Descriptions, source text, URLs,
    # settings, prompts, exception messages, and arbitrary strings never upload.
    values = {}
    for key, value in metrics.items():
        if key == "fact_fields" and isinstance(value, dict):
            values[key] = {
                name: result
                for name, result in value.items()
                if name in FACT_KEYS and (result is None or isinstance(result, bool | int | float))
            }
        elif key in METRIC_KEYS and (value is None or isinstance(value, bool | int | float)):
            values[key] = value
    return values


def score_fixture_metrics(outputs: dict, reference_outputs: dict) -> dict:
    checks = _checks(reference_outputs, outputs.get("metrics", {}))
    return {
        "key": "fixture_expectations",
        "score": bool(outputs.get("passed"))
        and bool(checks)
        and all(check["pass"] for check in checks),
        "comment": "确定性夹具期望；不是人工事实支持率或真实模型评分。",
    }


async def upload_to_langsmith(
    report: dict, *, experiment_prefix: str = "signaldesk-fixture-quality"
) -> dict:
    """Upload metric-only results. Called exclusively after explicit user opt-in."""
    api_key = os.environ.get("LANGSMITH_API_KEY", "").strip()
    if not api_key:
        raise ValueError("显式上传需要配置 LANGSMITH_API_KEY。")
    from langsmith import Client, schemas
    from langsmith.evaluation import aevaluate

    client = Client(api_key=api_key, hide_metadata=True, omit_traced_runtime_info=True)
    try:
        dataset = await asyncio.to_thread(
            client.create_dataset,
            dataset_name=f"signaldesk-fixture-metrics-{uuid4().hex[:12]}",
            description="Numeric/boolean fixture expectations only; no source text or live-model scores.",
            metadata={"fixture_only": True},
        )
        examples = []
        outputs = {}
        for index, scenario in enumerate(report["scenarios"]):
            # The scenario enum and ordinal are controlled, so custom descriptions
            # or case identifiers cannot accidentally carry private content.
            kind = scenario["scenario"] if scenario["scenario"] in SCENARIOS else "fixture"
            inputs = {"case_index": index, "scenario": kind}
            expected = _upload_metrics(scenario["expected"])
            outputs[index] = {
                "passed": bool(scenario["pass"]),
                "metrics": _upload_metrics(scenario["observed"]),
            }
            examples.append(
                schemas.Example(
                    id=uuid4(),
                    dataset_id=dataset.id,
                    created_at=datetime.now(UTC),
                    inputs=inputs,
                    outputs=expected,
                )
            )
        await asyncio.to_thread(
            client.create_examples,
            dataset_id=dataset.id,
            examples=[
                {"id": example.id, "inputs": example.inputs, "outputs": example.outputs}
                for example in examples
            ],
        )

        async def recorded_fixture_result(inputs: dict) -> dict:
            return outputs[inputs["case_index"]]

        results = await aevaluate(
            recorded_fixture_result,
            data=examples,
            evaluators=[score_fixture_metrics],
            experiment_prefix=experiment_prefix,
            client=client,
            max_concurrency=1,
            blocking=True,
            upload_results=True,
            disable_evaluator_tracing=True,
            metadata={
                "fixture_only": True,
                "recorded_observations": True,
                "git": {},
                "revision_id": "offline-fixture-v1",
                "remote_model_quality_evaluated": False,
            },
            description="Precomputed local fixture outcomes. Metrics only; no live model or source uploads.",
        )
        await asyncio.to_thread(client.flush)
        return {
            "uploaded": True,
            "experiment_url": results.url,
            "experiment_name": results.experiment_name,
            "dataset_id": str(dataset.id),
            "payload": "fixture_metrics_only",
        }
    finally:
        await asyncio.to_thread(client.close)
