import pytest
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.research import CitationError, ResearchEngine, verify_report
from signaldesk.retrieval import Retriever
from signaldesk.schemas import Citation, Evidence, Finding, ProjectRead, ResearchReport


def evidence():
    return Evidence(
        id="source-1",
        source_name="Source",
        url="https://public.test",
        title="Pricing",
        text="Pro costs $39 per month.",
        change_type="baseline",
    )


def report():
    return ResearchReport(
        title="Report",
        summary="Summary",
        findings=[
            Finding(
                title="Pricing",
                detail="Pro costs $39.",
                category="pricing",
                impact="low",
                citations=[Citation(evidence_id="source-1", quote="Pro costs $39 per month.")],
            )
        ],
        recommendations=[],
        limitations=[],
    )


def test_citation_validation_rejects_unknown_and_invented_quotes():
    valid = report()
    verify_report(valid, [evidence()])
    valid.findings[0].citations[0].quote = "Pro is free."
    with pytest.raises(CitationError):
        verify_report(valid, [evidence()])
    valid.findings[0].citations[0].evidence_id = "fabricated"
    with pytest.raises(CitationError):
        verify_report(valid, [evidence()])


class ToolCallingFake(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


async def test_real_deepagents_graph_with_scripted_model_and_skills(tmp_path, monkeypatch):
    settings = Settings(
        _env_file=None,
        model="openai:test-model",
        openai_api_key="test-only",
        database_url=f"sqlite+aiosqlite:///{tmp_path}/test.db",
    )
    model = ToolCallingFake(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": "skill",
                        "name": "read_file",
                        "args": {"file_path": "/skills/competitor-analysis/SKILL.md"},
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {"id": "evidence", "name": "read_evidence", "args": {"evidence_id": "source-1"}}
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {"id": "report", "name": "ResearchReport", "args": report().model_dump()}
                ],
            ),
        ]
    )
    monkeypatch.setattr("signaldesk.research.ChatOpenAI", lambda **kwargs: model)
    repo = Repository(settings)
    project = ProjectRead(
        id="p1",
        name="Test",
        created_at="2026-10-22T00:00:00Z",
        sources=[{"name": "Source", "url": "https://public.test"}],
    )
    try:
        engine = ResearchEngine(settings, Retriever(repo, settings))
        result = await engine.generate(project, [evidence()], "live")
        assert result.title == "Report"
        verify_report(result, [evidence()])
    finally:
        await repo.close()
