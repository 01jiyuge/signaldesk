"""Bearer-authenticated, project-scoped asynchronous Docker execution HTTP API."""

import secrets
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.sandbox import (
    SandboxBusyError,
    SandboxConflictError,
    SandboxService,
    SandboxUnavailableError,
    SandboxValidationError,
)


class SandboxExecutionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str = Field(min_length=1)
    input_data: Any = None
    run_id: str | None = Field(default=None, min_length=1, max_length=100)


def create_sandbox_router(
    repo: Repository,
    service: SandboxService | None,
    settings: Settings,
) -> APIRouter:
    bearer = HTTPBearer(auto_error=False)

    async def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ):
        if settings.api_token and (
            credentials is None
            or credentials.scheme.lower() != "bearer"
            or not secrets.compare_digest(credentials.credentials, settings.api_token)
        ):
            raise HTTPException(
                401,
                "需要有效的 Bearer API token。",
                headers={"WWW-Authenticate": "Bearer"},
            )

    router = APIRouter(prefix="/api/projects", dependencies=[Depends(authenticate)])

    async def resolve(project_id: str, request: Request) -> SandboxService:
        if await repo.get_project(project_id) is None:
            raise HTTPException(404, "项目不存在。")
        current = service if service is not None else getattr(request.app.state, "sandbox", None)
        if current is None:
            raise HTTPException(503, "Sandbox 尚未就绪。")
        return current

    def api_error(exc):
        if isinstance(exc, KeyError):
            return HTTPException(404, str(exc).strip("'"))
        if isinstance(exc, SandboxValidationError):
            return HTTPException(422, str(exc))
        if isinstance(exc, SandboxConflictError):
            return HTTPException(409, str(exc))
        if isinstance(exc, SandboxBusyError):
            return HTTPException(429, str(exc), headers={"Retry-After": "5"})
        return HTTPException(503, str(exc))

    @router.get("/{project_id}/sandbox/capabilities")
    async def capabilities(project_id: str, request: Request):
        return await (await resolve(project_id, request)).capabilities()

    @router.post("/{project_id}/sandbox/executions", status_code=202)
    async def start(project_id: str, payload: SandboxExecutionInput, request: Request):
        current = await resolve(project_id, request)
        try:
            return await current.start(project_id, payload.code, payload.input_data, payload.run_id)
        except (KeyError, SandboxValidationError, SandboxUnavailableError, SandboxBusyError) as exc:
            raise api_error(exc) from exc

    @router.get("/{project_id}/sandbox/executions")
    async def list_executions(
        project_id: str,
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
    ):
        return await (await resolve(project_id, request)).list(project_id, limit)

    @router.get("/{project_id}/sandbox/executions/{execution_id}")
    async def get_execution(project_id: str, execution_id: str, request: Request):
        row = await (await resolve(project_id, request)).get(project_id, execution_id)
        if row is None:
            raise HTTPException(404, "此项目下的 Sandbox 执行不存在。")
        return row

    @router.post("/{project_id}/sandbox/executions/{execution_id}/cancel")
    async def cancel(project_id: str, execution_id: str, request: Request):
        current = await resolve(project_id, request)
        try:
            return await current.cancel(project_id, execution_id)
        except (KeyError, SandboxConflictError) as exc:
            raise api_error(exc) from exc

    return router
