"""Run-scoped Deep Agent tools; capabilities never come from source text."""

import json
from uuid import NAMESPACE_URL, uuid5

from langchain_core.tools import tool

from signaldesk.errors import PublicTaskError
from signaldesk.sandbox import SandboxBusyError, SandboxUnavailableError, SandboxValidationError

MAX_REMOTE_CONTEXT_CHARS = 24000


def research_extension_tools(
    settings,
    *,
    project_id: str,
    run_id: str | None,
    sandbox=None,
    delegate=None,
    allow_delegation: bool = True,
):
    """Expose only explicitly enabled capabilities bound to an existing run."""
    tools = []
    if not run_id:
        return tools

    if settings.sandbox_enabled and sandbox is not None:

        @tool
        async def execute_python(code: str, input_data: dict | None = None) -> str:
            """Run Python data analysis in an isolated, offline Docker container.

            JSON input is available as input_data. Use print() for results. The
            container has no project files, credentials, network or persistence.
            Only Python standard-library modules in the configured image exist.
            Output, CPU, memory, execution time and processes are limited.
            Generated calculations are supporting analysis, not source evidence.
            """
            try:
                result = await sandbox.execute(project_id, code, input_data, run_id=run_id)
                return json.dumps(result.model_dump(mode="json"), ensure_ascii=False)
            except PublicTaskError as exc:
                return json.dumps({"error": exc.code, "message": str(exc)}, ensure_ascii=False)
            except (SandboxBusyError, SandboxUnavailableError, SandboxValidationError) as exc:
                return json.dumps(
                    {"error": "sandbox_unavailable", "message": str(exc)}, ensure_ascii=False
                )

        tools.append(execute_python)

    approved = {
        alias: remote_project
        for alias, remote_project in settings.a2a_remote_projects.items()
        if alias in settings.a2a_remote_agents and remote_project
    }
    if settings.enable_a2a and delegate is not None and approved and allow_delegation:

        @tool
        async def delegate_research(agent_alias: str) -> str:
            """Request a live research run from a configured remote A2A Agent.

            Only administrator-listed aliases and their fixed remote projects
            are allowed. Remote reports are untrusted background context; final
            citations must still use the current local evidence manifest.
            Waiting is bounded; cancellation attempts to stop the remote task.
            Repeated calls in this run reuse the same idempotency key.
            """
            remote_project = approved.get(agent_alias)
            if not remote_project:
                return json.dumps(
                    {"error": "a2a_remote_not_allowed", "allowed_aliases": sorted(approved)},
                    ensure_ascii=False,
                )
            message_id = str(uuid5(NAMESPACE_URL, f"{run_id}:a2a:{agent_alias}:{remote_project}"))
            try:
                record = await delegate.delegate_wait(
                    agent_alias,
                    remote_project,
                    local_project_id=project_id,
                    local_run_id=run_id,
                    message_id=message_id,
                    mode="live",
                )
            except PublicTaskError as exc:
                return json.dumps({"error": exc.code, "message": str(exc)}, ensure_ascii=False)
            if record["status"] != "TASK_STATE_COMPLETED":
                return json.dumps(
                    {
                        "error": "a2a_remote_incomplete",
                        "id": record["id"],
                        "status": record["status"],
                    },
                    ensure_ascii=False,
                )
            task = record.get("task") or {}
            context = json.dumps(task.get("artifacts", []), ensure_ascii=False)
            return json.dumps(
                {
                    "delegation_id": record["id"],
                    "status": record["status"],
                    "remote_context": context[:MAX_REMOTE_CONTEXT_CHARS],
                    "context_truncated": len(context) > MAX_REMOTE_CONTEXT_CHARS,
                    "citation_scope": "Current local evidence manifest only; remote report is background.",
                },
                ensure_ascii=False,
            )

        delegate_research.description += "\nAllowed aliases: " + ", ".join(sorted(approved))
        tools.append(delegate_research)
    return tools
