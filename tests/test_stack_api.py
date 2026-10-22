import asyncio

import httpx

from signaldesk.app import create_app
from signaldesk.config import Settings
from signaldesk.schemas import ResearchReport


async def wait_run(client, run_id):
    for _ in range(200):
        run = (await client.get(f"/api/runs/{run_id}")).json()
        if run["status"] in {"completed", "failed", "cancelled"}:
            return run
        await asyncio.sleep(0.01)
    raise AssertionError("Run did not finish")


async def test_store_preferences_affect_research_decision_and_survive_restart(
    tmp_path, monkeypatch
):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/app.db",
        checkpoint_url=f"sqlite:///{tmp_path}/checkpoints.db",
        memory_url=f"sqlite:///{tmp_path}/memory.db",
        model="openai:scripted",
        openai_api_key="not-a-real-key",
        reranker_provider="none",
        mcp_urls=[],
    )
    calls = []

    async def collect(*args, **kwargs):
        return "Public pricing", "A stable public pricing page: Pro $39 per month."

    async def research(self, project, evidence):
        calls.append(self.memory_contexts[project.id])
        return ResearchReport(
            title="Scripted report",
            summary="Local integration only",
            findings=[],
            recommendations=[],
            limitations=[],
        )

    monkeypatch.setattr("signaldesk.workflow.fetch_page", collect)
    monkeypatch.setattr("signaldesk.research.ResearchEngine._live", research)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        assert app.state.workflow.graph.store is app.state.memory.store
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            capability = (await client.get("/api/capabilities")).json()
            assert capability["retrieval"]["fulltext_ready"]
            assert capability["memory"]["persistent_store_ready"]
            project = (
                await client.post(
                    "/api/projects",
                    json={
                        "name": "Store project",
                        "sources": [{"name": "Pricing", "url": "https://example.com/pricing"}],
                    },
                )
            ).json()
            endpoint = f"/api/projects/{project['id']}/memory/output"
            assert (
                await client.put(endpoint, json={"value": {"text": "private-preference-alpha"}})
            ).status_code == 200
            completed = []
            for change in (False, False, True):
                if change:
                    await client.put(endpoint, json={"value": {"text": "private-preference-beta"}})
                created = (
                    await client.post(f"/api/projects/{project['id']}/runs", json={"mode": "live"})
                ).json()
                run = await wait_run(client, created["id"])
                assert run["status"] == "completed", run
                completed.append(run)
            assert len(calls) == 2, (
                "Same content + same preferences skips research; changed preferences must research"
            )
            assert completed[1]["report"]["stats"]["model_call_skipped"] is True
            assert completed[2]["report"]["stats"]["model_call_skipped"] is False
            assert calls[0]["preferences"][0]["value"]["text"] == "private-preference-alpha"
            assert calls[1]["preferences"][0]["value"]["text"] == "private-preference-beta"
            memory = (await client.get(f"/api/projects/{project['id']}/memory")).json()
            assert len(memory["recent_runs"]) == 3
            assert all(not item["value"]["facts_verified"] for item in memory["recent_runs"])
            stream = await client.get(f"/api/runs/{completed[-1]['id']}/ag-ui")
            assert stream.status_code == 200 and "RUN_FINISHED" in stream.text
            assert "private-preference" not in stream.text and "not-a-real-key" not in stream.text
    restarted = create_app(settings)
    async with restarted.router.lifespan_context(restarted):
        context = await restarted.state.memory.load_context(project["id"])
        assert context["preferences"][0]["value"]["text"] == "private-preference-beta"
        assert len(context["recent_runs"]) == 3
