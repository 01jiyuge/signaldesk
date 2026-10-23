"""Authenticated user-only preference editing; namespaces never come from HTTP."""

from typing import Annotated
from urllib.parse import unquote_to_bytes

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from signaldesk.auth import authenticate_bearer
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
        authenticate_bearer(
            settings.api_token,
            credentials.scheme if credentials else "",
            credentials.credentials if credentials else "",
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

    def checked_key(request: Request, key: str) -> str:
        # ASGI servers can replace malformed percent-encoded UTF-8 with U+FFFD
        # in path parameters. Reject malformed wire text rather than silently
        # saving/deleting a different key. A valid encoded U+FFFD remains valid.
        raw_path = request.scope.get("raw_path")
        if raw_path is not None:
            try:
                unquote_to_bytes(raw_path).decode("utf-8", errors="strict")
            except UnicodeError as exc:
                raise HTTPException(422, "记忆 key 必须是有效 UTF-8 文本。") from exc
        return key

    @router.get("/{project_id}/memory")
    async def list_memory(
        project_id: str, request: Request, limit: int = Query(default=10, ge=1, le=1000)
    ):
        await require_project(project_id)
        try:
            return await resolve_memory(request).load_context(project_id, limit=limit)
        except MemoryLimitError as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.get("/{project_id}/memory/{key}")
    async def get_memory(project_id: str, key: str, request: Request):
        await require_project(project_id)
        try:
            item = await resolve_memory(request).get_preference(
                project_id, checked_key(request, key)
            )
        except MemoryValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        if item is None:
            raise HTTPException(404, "明确偏好不存在。")
        return item

    @router.put("/{project_id}/memory/{key}")
    async def set_memory(project_id: str, key: str, payload: PreferenceInput, request: Request):
        await require_project(project_id)
        try:
            return await resolve_memory(request).set_preference(
                project_id, checked_key(request, key), payload.value
            )
        except MemoryValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        except MemoryLimitError as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.delete("/{project_id}/memory/{key}", status_code=204)
    async def delete_memory(project_id: str, key: str, request: Request):
        await require_project(project_id)
        try:
            deleted = await resolve_memory(request).delete_preference(
                project_id, checked_key(request, key)
            )
        except MemoryValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        if not deleted:
            raise HTTPException(404, "明确偏好不存在。")
        return Response(status_code=204)

    return router
