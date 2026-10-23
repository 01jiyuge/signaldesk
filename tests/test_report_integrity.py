"""Model/legacy report strings must survive storage, export and HTTP encoding."""

import asyncio

import httpx
import pytest

from signaldesk.app import create_app
from signaldesk.config import Settings
from signaldesk.exports import ExportUnavailable, export_json, export_markdown
from signaldesk.research import CitationError, verify_report
from signaldesk.schemas import Evidence, ResearchReport, RunRead


def report():
    return ResearchReport(
        title="中文 😀",
        summary="A local report",
        findings=[],
        recommendations=["Read the source."],
        limitations=[],
    )


@pytest.mark.parametrize("field", ["title", "summary", "recommendations", "limitations"])
async def test_invalid_model_text_fails_before_report_is_saved(tmp_path, monkeypatch, field):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/api.db",
        checkpoint_url=f"sqlite:///{tmp_path}/cp.db",
        memory_url=f"sqlite:///{tmp_path}/memory.db",
        model="openai:fixture-not-called",
        openai_api_key="fixture-not-used",
        api_token="",
        enable_a2a=False,
        sandbox_enabled=False,
    )
    app = create_app(settings)

    async def fetch(*args):
        return "Pricing", "Public fixture content"

    async def generate(*args, **kwargs):
        value = report()
        setattr(value, field, ["private-fixture-\ud800"] if field.endswith("s") else "\ud800")
        return value

    monkeypatch.setattr("signaldesk.workflow.fetch_page", fetch)
    async with app.router.lifespan_context(app):
        monkeypatch.setattr(app.state.workflow.research, "generate", generate)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://fixture",
        ) as client:
            project = (
                await client.post(
                    "/api/projects",
                    json={
                        "name": "Report check",
                        "sources": [{"name": "Source", "url": "https://fixture.example"}],
                    },
                )
            ).json()
            run = (
                await client.post(f"/api/projects/{project['id']}/runs", json={"mode": "live"})
            ).json()
            for _ in range(200):
                response = await client.get(f"/api/runs/{run['id']}")
                assert response.status_code == 200
                saved = response.json()
                if saved["status"] in {"failed", "completed"}:
                    break
                await asyncio.sleep(0.01)
            assert saved["status"] == "failed" and saved["report"] is None
            events = (await client.get(f"/api/runs/{run['id']}/events")).json()
            assert any(e["data"].get("code") == "citation_invalid" for e in events)
            assert "private-fixture" not in response.text


@pytest.mark.parametrize("extra", [{"legacy": "\udfff"}, {"legacy": float("nan")}])
def test_legacy_report_extra_values_cannot_break_either_export(extra):
    run = RunRead(
        id="run",
        project_id="project",
        mode="live",
        demo_revision=1,
        status="completed",
        stage="completed",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        report={**report().model_dump(), "evidence": [], "mode": "live", **extra},
    )
    for exporter in (export_json, export_markdown):
        with pytest.raises(ExportUnavailable):
            exporter(run)


def test_duplicate_evidence_cannot_silently_replace_its_original():
    item = Evidence(
        id="same",
        source_name="Source",
        url="https://fixture.example",
        title="Pricing",
        text="Original",
        change_type="baseline",
    )
    with pytest.raises(CitationError, match="不可重复"):
        verify_report(report(), [item, item.model_copy(update={"text": "Replacement"})])
    verify_report(report(), [item])
