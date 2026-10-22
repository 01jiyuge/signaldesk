"""HTTP routes. Progress is exposed as cursor-based JSON event polling."""

import secrets
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from signaldesk import __version__
from signaldesk.config import Settings
from signaldesk.exports import ExportUnavailable, attachment_filename, export_json, export_markdown
from signaldesk.jobs import JobManager
from signaldesk.repository import ActiveRunError, Repository, RunConfigurationError
from signaldesk.schemas import EventRead, ProjectCreate, ProjectRead, RunCreate, RunRead


def create_router(repo: Repository, jobs: JobManager, settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/api")
    bearer = HTTPBearer(auto_error=False)

    async def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> None:
        if not settings.api_token:
            return
        if (
            credentials is None
            or credentials.scheme.lower() != "bearer"
            or not secrets.compare_digest(credentials.credentials, settings.api_token)
        ):
            raise HTTPException(
                status_code=401,
                detail="需要有效的 Bearer API token。",
                headers={"WWW-Authenticate": "Bearer"},
            )

    protected = APIRouter(dependencies=[Depends(authenticate)])

    async def require_project(project_id: str) -> ProjectRead:
        project = await repo.get_project(project_id)
        if project is None:
            raise HTTPException(status_code=404, detail="项目不存在。")
        return project

    async def require_run(run_id: str) -> RunRead:
        run = await repo.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="任务不存在。")
        return run

    @router.get("/health")
    async def health():
        try:
            settings.validate_live()
            live_configured = True
        except ValueError:
            live_configured = False
        return {
            "status": "ok",
            "version": __version__,
            "mode": settings.research_mode,
            "live_configured": live_configured,
        }

    @protected.post("/projects", response_model=ProjectRead, status_code=status.HTTP_201_CREATED)
    async def create_project(payload: ProjectCreate):
        if len(payload.sources) > settings.max_sources:
            raise HTTPException(status_code=422, detail=f"最多允许 {settings.max_sources} 个来源。")
        return await repo.create_project(payload)

    @protected.get("/projects", response_model=list[ProjectRead])
    async def list_projects():
        return await repo.list_projects()

    @protected.get("/capabilities")
    async def capabilities(request: Request):
        retriever = getattr(request.app.state, "retriever", None)
        memory = getattr(request.app.state, "memory", None)
        return {
            "version": __version__,
            "orchestration": "langgraph",
            "http": {"max_request_bytes": settings.max_request_bytes},
            "research": {
                "harness": "deepagents",
                "framework": "langchain",
                "skills": True,
                "mcp_allowlist_configured": bool(settings.mcp_urls and settings.mcp_allowed_tools),
            },
            "retrieval": {
                "database": repo.engine.dialect.name,
                "fulltext_ready": bool(getattr(retriever, "fulltext_ready", False)),
                "vector_enabled": settings.enable_vector_search,
                "reranker_provider": settings.reranker_provider,
                "model_reranker_configured": bool(
                    settings.reranker_url and settings.reranker_model
                ),
            },
            "memory": {
                "persistent_store_ready": memory is not None,
                "backend": type(memory.store).__name__ if memory is not None else None,
            },
            "interaction": {
                "protocol": "ag-ui",
                "transport": "sse",
                "subscription": "/api/runs/{run_id}/ag-ui",
                "generic_run_agent_endpoint": False,
            },
            "evaluation": {"offline_command": "signaldesk-eval", "langsmith_opt_in": True},
            "extensions": {
                "sandbox": await request.app.state.sandbox.capabilities(),
                "a2a": {
                    "enabled": settings.enable_a2a,
                    "available": request.app.state.a2a_delegate is not None,
                    "reason": None if settings.enable_a2a else "A2A 尚未启用。",
                    "protocol_version": "1.0",
                    "v03_compatible": True,
                    "card_url": "/.well-known/agent-card.json",
                    "rpc_url": "/a2a/",
                    "remotes": [
                        {"name": alias, "project_id": settings.a2a_remote_projects.get(alias)}
                        for alias in sorted(settings.a2a_remote_agents)
                    ],
                },
            },
        }

    @protected.get("/projects/{project_id}", response_model=ProjectRead)
    async def get_project(project_id: str):
        return await require_project(project_id)

    @protected.put("/projects/{project_id}", response_model=ProjectRead)
    async def update_project(project_id: str, payload: ProjectCreate):
        if len(payload.sources) > settings.max_sources:
            raise HTTPException(status_code=422, detail=f"最多允许 {settings.max_sources} 个来源。")
        try:
            return await repo.update_project(project_id, payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="项目不存在。") from exc
        except ActiveRunError as exc:
            raise HTTPException(
                status_code=409, detail="项目存在等待或执行中的任务，结束后再编辑配置。"
            ) from exc

    @protected.post(
        "/projects/{project_id}/runs", response_model=RunRead, status_code=status.HTTP_202_ACCEPTED
    )
    async def create_run(project_id: str, payload: RunCreate):
        await require_project(project_id)
        if payload.mode == "live":
            try:
                settings.validate_live()
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

        async def create():
            try:
                run = await repo.create_run(project_id, payload)
            except ActiveRunError as exc:
                raise HTTPException(
                    status_code=409, detail="这个项目已有等待或执行中的任务。"
                ) from exc
            try:
                jobs.submit(run.id)
            except (RuntimeError, ValueError) as exc:
                await repo.update_run(run.id, status="failed", error="任务服务正在关闭，请重试。")
                raise HTTPException(status_code=409, detail="任务服务正在关闭，请重试。") from exc
            return run

        try:
            return await jobs.transition(f"project:{project_id}", create)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail="任务服务正在关闭，请稍后重试。") from exc

    @protected.get("/runs", response_model=list[RunRead])
    async def list_runs(project_id: str | None = None):
        if project_id is not None:
            await require_project(project_id)
        return await repo.list_runs(project_id)

    @protected.get("/runs/{run_id}", response_model=RunRead)
    async def get_run(run_id: str):
        return await require_run(run_id)

    @protected.get("/runs/{run_id}/export")
    async def export_report(run_id: str, format: Literal["markdown", "json"] = "markdown"):
        run = await require_run(run_id)
        try:
            content = export_markdown(run) if format == "markdown" else export_json(run)
        except ExportUnavailable as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return Response(
            content=content,
            media_type="text/markdown" if format == "markdown" else "application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{attachment_filename(run.id, format)}"',
                "Cache-Control": "private, no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )

    @protected.get("/runs/{run_id}/events", response_model=list[EventRead])
    async def get_events(run_id: str, after: int = Query(default=0, ge=0)):
        await require_run(run_id)
        return await repo.list_events(run_id, after)

    @protected.post(
        "/runs/{run_id}/resume", response_model=RunRead, status_code=status.HTTP_202_ACCEPTED
    )
    async def resume_run(run_id: str):
        async def resume():
            run = await require_run(run_id)
            if run.status not in ("interrupted", "failed", "cancelled"):
                raise HTTPException(status_code=409, detail="仅中断、失败或取消的任务可以恢复。")
            if run.mode == "live":
                try:
                    settings.validate_live()
                except ValueError as exc:
                    raise HTTPException(status_code=422, detail=str(exc)) from exc
            try:
                updated = await repo.update_run(run_id, status="queued", error=None)
            except RunConfigurationError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except ActiveRunError as exc:
                raise HTTPException(
                    status_code=409, detail="这个项目已有等待或执行中的任务。"
                ) from exc
            try:
                jobs.submit(run_id, resume=True)
            except (RuntimeError, ValueError) as exc:
                await repo.update_run(run_id, status=run.status, error=run.error)
                raise HTTPException(status_code=409, detail="任务尚未停止或服务正在关闭。") from exc
            await repo.append_event(run_id, "resumed", "任务已加入恢复队列。")
            return updated

        try:
            return await jobs.transition(run_id, resume)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail="任务服务正在关闭，请稍后重试。") from exc

    @protected.post("/runs/{run_id}/cancel", response_model=RunRead)
    async def cancel_run(run_id: str):
        await require_run(run_id)
        try:
            await jobs.cancel(run_id)
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail="仅等待或执行中的任务可以取消。") from exc
        return await require_run(run_id)

    @protected.get("/projects/{project_id}/search")
    async def search(
        project_id: str,
        request: Request,
        q: str = Query(min_length=1, max_length=1000),
        limit: int = Query(default=5, ge=1, le=20),
    ):
        await require_project(project_id)
        retriever = getattr(request.app.state, "retriever", None)
        if retriever is not None:
            return await retriever.search(project_id, q, limit=limit)
        return await repo.search_snapshots(project_id, q, limit)

    router.include_router(protected)
    return router
