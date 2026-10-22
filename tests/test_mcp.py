from fastmcp import Client

from signaldesk.config import Settings
from signaldesk.mcp_server import build_server
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate


async def test_readonly_mcp_tools(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/mcp.db")
    repo = Repository(settings)
    await repo.initialize()
    await repo.create_project(
        ProjectCreate(
            name="MCP project",
            sources=[{"name": "Source", "url": "https://northstar.example/pricing"}],
        )
    )
    await repo.close()
    async with Client(build_server(settings)) as client:
        tools = await client.list_tools()
        assert {item.name for item in tools} == {"list_projects", "get_report", "search_evidence"}
        assert all(item.annotations.read_only_hint for item in tools)
        result = await client.call_tool("list_projects", {})
        assert "MCP project" in str(result)
