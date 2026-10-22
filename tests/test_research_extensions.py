import asyncio
import json

import pytest
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver

from signaldesk.config import Settings
from signaldesk.errors import PublicTaskError
from signaldesk.extensions import research_extension_tools
from signaldesk.repository import Repository
from signaldesk.research import ResearchEngine
from signaldesk.retrieval import Retriever
from signaldesk.sandbox import SandboxResult, SandboxUnavailableError
from signaldesk.schemas import Evidence, ProjectCreate, ResearchReport, RunCreate
from signaldesk.workflow import Workflow


class ScriptedTools(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


class RecordingSandbox:
    def __init__(self):
        self.calls = []

    async def execute(self, project_id, code, input_data=None, *, run_id):
        self.calls.append((project_id, run_id, code, input_data))
        return SandboxResult(execution_id="fixture-execution", status="completed", result=10)


class RecordingDelegate:
    def __init__(self, state="TASK_STATE_COMPLETED"):
        self.calls = []
        self.state = state

    async def delegate_wait(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return {"id": "fixture-delegation", "status": self.state, "task": {"artifacts": []}}


def configured(tmp_path):
    return Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/extensions.db",
        model="openai:scripted",
        openai_api_key="not-a-real-key",
        sandbox_enabled=True,
        enable_a2a=True,
        a2a_remote_agents={"analyst": "http://localhost:8001/a2a/"},
        a2a_remote_projects={"analyst": "remote-project"},
    )


def tool_map(settings, **kwargs):
    return {
        item.name: item
        for item in research_extension_tools(settings, project_id="local-project", **kwargs)
    }


async def test_tools_require_run_scope_and_explicitly_enabled_mapped_capabilities(tmp_path):
    settings = configured(tmp_path)
    sandbox, delegate = RecordingSandbox(), RecordingDelegate()
    assert tool_map(settings, run_id=None, sandbox=sandbox, delegate=delegate) == {}
    disabled = settings.model_copy(update={"sandbox_enabled": False, "enable_a2a": False})
    assert tool_map(disabled, run_id="run", sandbox=sandbox, delegate=delegate) == {}
    inbound = tool_map(
        settings, run_id="run", sandbox=sandbox, delegate=delegate, allow_delegation=False
    )
    assert set(inbound) == {"execute_python"}
    unmapped = settings.model_copy(update={"a2a_remote_projects": {"unknown": "project"}})
    assert tool_map(unmapped, run_id="run", delegate=delegate) == {}


async def test_tool_delegation_is_idempotent_and_noncompleted_is_never_a_success(tmp_path):
    settings = configured(tmp_path)
    delegate = RecordingDelegate()
    tool = tool_map(settings, run_id="run", delegate=delegate)["delegate_research"]
    denied = json.loads(await tool.ainvoke({"agent_alias": "https://unapproved.invalid"}))
    assert denied["error"] == "a2a_remote_not_allowed" and delegate.calls == []
    for _ in range(2):
        result = json.loads(await tool.ainvoke({"agent_alias": "analyst"}))
        assert result["status"] == "TASK_STATE_COMPLETED"
        assert "Current local evidence manifest only" in result["citation_scope"]
    assert delegate.calls[0][1]["message_id"] == delegate.calls[1][1]["message_id"]
    assert delegate.calls[0][0] == ("analyst", "remote-project")
    assert delegate.calls[0][1]["mode"] == "live"
    for state in ("TASK_STATE_FAILED", "TASK_STATE_INPUT_REQUIRED", "TASK_STATE_CANCELED"):
        delegate.state = state
        result = json.loads(await tool.ainvoke({"agent_alias": "analyst"}))
        assert result["error"] == "a2a_remote_incomplete" and result["status"] == state


async def test_safe_capability_errors_reach_model_and_cancellation_propagates(tmp_path):
    settings = configured(tmp_path)

    class Unavailable:
        async def execute(self, *args, **kwargs):
            raise SandboxUnavailableError("Docker 服务不可用。")

    tool = tool_map(settings, run_id="run", sandbox=Unavailable())["execute_python"]
    assert json.loads(await tool.ainvoke({"code": "print(1)"}))["error"] == "sandbox_unavailable"

    class Cancelled:
        async def execute(self, *args, **kwargs):
            raise asyncio.CancelledError

    tool = tool_map(settings, run_id="run", sandbox=Cancelled())["execute_python"]
    with pytest.raises(asyncio.CancelledError):
        await tool.ainvoke({"code": "print(1)"})

    class RemoteFailure:
        async def delegate_wait(self, *args, **kwargs):
            raise PublicTaskError("远端暂不可用。", code="a2a_remote_failed")

    tool = tool_map(settings, run_id="run", delegate=RemoteFailure())["delegate_research"]
    assert (
        json.loads(await tool.ainvoke({"agent_alias": "analyst"}))["error"] == "a2a_remote_failed"
    )


async def test_real_deepagent_graph_invokes_both_run_scoped_extension_tools(tmp_path, monkeypatch):
    settings = configured(tmp_path)
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(name="Tools", sources=[{"name": "Pricing", "url": "https://example.com"}])
    )
    run = await repo.create_run(project.id, RunCreate(mode="live"))
    report = ResearchReport(
        title="Scripted",
        summary="Integration test",
        findings=[],
        recommendations=[],
        limitations=[],
    )
    responses = [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "id": "calc",
                    "name": "execute_python",
                    "args": {
                        "code": "result=sum(input_data['values'])",
                        "input_data": {"values": [4, 6]},
                    },
                }
            ],
        ),
        AIMessage(
            content="",
            tool_calls=[
                {"id": "remote", "name": "delegate_research", "args": {"agent_alias": "analyst"}}
            ],
        ),
        AIMessage(
            content="",
            tool_calls=[{"id": "report", "name": "ResearchReport", "args": report.model_dump()}],
        ),
    ]
    model = ScriptedTools(responses=responses)
    monkeypatch.setattr("signaldesk.research.ChatOpenAI", lambda **kwargs: model)
    sandbox, delegate = RecordingSandbox(), RecordingDelegate()
    retriever = Retriever(repo, settings)
    engine = ResearchEngine(settings, retriever, sandbox=sandbox, delegate=delegate)
    token = engine.execution_context.set({"run_id": run.id, "allow_delegation": True})
    try:
        result = await engine.generate(project, [], "live", force_research=True)
        assert result.title == "Scripted"
        assert sandbox.calls[0][:2] == (project.id, run.id)
        assert sandbox.calls[0][3] == {"values": [4, 6]}
        assert delegate.calls[0][1]["local_run_id"] == run.id
        assert delegate.calls[0][1]["local_project_id"] == project.id
    finally:
        engine.execution_context.reset(token)
        await retriever.close()
        await repo.close()


async def test_workflow_extension_context_is_task_local_and_inbound_cannot_delegate(tmp_path):
    settings = configured(tmp_path)
    repo = Repository(settings)
    await repo.initialize()
    retriever = Retriever(repo, settings)
    workflow = Workflow(repo, settings, InMemorySaver(), retriever)
    seen = {}
    states = []
    for name in ("local", "inbound"):
        project = await repo.create_project(
            ProjectCreate(name=name, sources=[{"name": "Source", "url": "https://example.com"}])
        )
        run = await repo.create_run(project.id, RunCreate(mode="live"))
        if name == "inbound":
            await repo.append_event(run.id, "a2a_origin", "Remote task", {"delegation_depth": 1})
        evidence = Evidence(
            id="evidence",
            source_name="Source",
            url="https://example.com",
            title="Title",
            text="Source",
            change_type="baseline",
        )
        states.append(
            {"run_id": run.id, "evidence": [evidence.model_dump()], "context_changed": True}
        )

    async def generate(project, evidence, mode, **kwargs):
        before = workflow.research.execution_context.get()
        await asyncio.sleep(0.02)
        assert workflow.research.execution_context.get() is before
        seen[project.name] = before
        return ResearchReport(
            title=project.name, summary="Scripted", findings=[], recommendations=[], limitations=[]
        )

    workflow.research.generate = generate
    try:
        await asyncio.gather(*(workflow._research(state) for state in states))
        assert seen["local"] == {"run_id": states[0]["run_id"], "allow_delegation": True}
        assert seen["inbound"] == {"run_id": states[1]["run_id"], "allow_delegation": False}
        assert workflow.research.execution_context.get() is None
    finally:
        await retriever.close()
        await repo.close()
