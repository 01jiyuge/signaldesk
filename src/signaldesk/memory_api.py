"""Authenticated user-only preference editing; namespaces never come from HTTP."""

import secrets
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from signaldesk.config import Settings
from signaldesk.memory import MemoryLimitError, MemoryValidationError, ProjectMemory
from signaldesk.repository import Repository


class PreferenceInput(BaseModel):
    value: dict


def create_memory_router(
    repo: Repository, memory: ProjectMemory | None, settings: Settings
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
                401, "需要有效的 Bearer API token。", headers={"WWW-Authenticate": "Bearer"}
            )

    router = APIRouter(prefix="/api/projects", dependencies=[Depends(authenticate)])

    async def require_project(project_id: str):
        if await repo.get_project(project_id) is None:
            raise HTTPException(404, "项目不存在。")

    def resolve_memory(request: Request) -> ProjectMemory:
        current = memory if memory is not None else getattr(request.app.state, "memory", None)
        if current is None:
            raise HTTPException(503, "项目记忆尚未就绪，请稍后重试。")
        return current

    @router.get("/{project_id}/memory")
    async def list_memory(
        project_id: str, request: Request, limit: int = Query(default=10, ge=1, le=1000)
    ):
        await require_project(project_id)
        return await resolve_memory(request).load_context(project_id, limit=limit)

    @router.get("/{project_id}/memory/{key}")
    async def get_memory(project_id: str, key: str, request: Request):
        await require_project(project_id)
        try:
            item = await resolve_memory(request).get_preference(project_id, key)
        except MemoryValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        if item is None:
            raise HTTPException(404, "明确偏好不存在。")
        return item

    @router.put("/{project_id}/memory/{key}")
    async def set_memory(project_id: str, key: str, payload: PreferenceInput, request: Request):
        await require_project(project_id)
        try:
            return await resolve_memory(request).set_preference(project_id, key, payload.value)
        except MemoryValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        except MemoryLimitError as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.delete("/{project_id}/memory/{key}", status_code=204)
    async def delete_memory(project_id: str, key: str, request: Request):
        await require_project(project_id)
        try:
            deleted = await resolve_memory(request).delete_preference(project_id, key)
        except MemoryValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        if not deleted:
            raise HTTPException(404, "明确偏好不存在。")
        return Response(status_code=204)

    return router
