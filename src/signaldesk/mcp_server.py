"""Read-only stdio MCP interface. It never launches research or calls a model."""

from dotenv import load_dotenv
from fastmcp import FastMCP

from signaldesk.config import Settings, get_settings
from signaldesk.repository import Repository


def build_server(settings: Settings | None = None) -> FastMCP:
    settings = settings or get_settings()
    server = FastMCP(
        "SignalDesk", instructions="Read-only access to local competitive intelligence."
    )

    @server.tool(annotations={"readOnlyHint": True})
    async def list_projects() -> list[dict]:
        """List monitored projects. No model or external API is called."""
        repo = Repository(settings)
        try:
            return [item.model_dump(mode="json") for item in await repo.list_projects()]
        finally:
            await repo.close()

    @server.tool(annotations={"readOnlyHint": True})
    async def get_report(run_id: str) -> dict:
        """Read a completed report by run ID; incomplete runs have no public report."""
        repo = Repository(settings)
        try:
            run = await repo.get_run(run_id)
            if run is None or run.status != "completed":
                return {"error": "Completed report not found"}
            return {
                "run_id": run.id,
                "project_id": run.project_id,
                "mode": run.mode,
                "report": run.report,
            }
        finally:
            await repo.close()

    @server.tool(annotations={"readOnlyHint": True})
    async def search_evidence(project_id: str, query: str, limit: int = 5) -> list[dict]:
        """Lexically search one project's completed source snapshots without embedding API calls."""
        repo = Repository(settings)
        try:
            return [
                item.model_dump() for item in await repo.search_snapshots(project_id, query, limit)
            ]
        finally:
            await repo.close()

    return server


def main() -> None:
    load_dotenv(override=False)
    build_server().run(transport="stdio")


if __name__ == "__main__":
    main()
