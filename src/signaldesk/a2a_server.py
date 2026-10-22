"""Official A2A 1.0 JSON-RPC bridge; SDK-managed v0.3 compatibility is enabled.

Mount create_a2a_app at /a2a. It shares the existing repository/JobManager;
no second worker, model, arbitrary instruction executor, or project ACL exists.
"""

import asyncio
import secrets
from datetime import datetime
from urllib.parse import urlsplit

from a2a.server.request_handlers.request_handler import RequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    Artifact,
    HTTPAuthSecurityScheme,
    Message,
    Part,
    Role,
    SecurityRequirement,
    SecurityScheme,
    StringList,
    Task,
    TaskState,
    TaskStatus,
)
from a2a.utils.errors import (
    InternalError,
    InvalidParamsError,
    TaskNotCancelableError,
    TaskNotFoundError,
    UnsupportedOperationError,
)
from fastapi import APIRouter, HTTPException, Request
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct, Value
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import BaseModel, ConfigDict, Field
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from signaldesk import __version__
from signaldesk.a2a_store import A2ARequestStore, IdempotencyConflict
from signaldesk.repository import ActiveRunError
from signaldesk.schemas import RunCreate


class A2ACommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_id: str = Field(min_length=1, max_length=128)
    mode: str = "demo"
    demo_revision: int = 1

    def run_payload(self) -> RunCreate:
        return RunCreate.model_validate(self.model_dump(exclude={"project_id"}))


def authenticate_a2a(request: Request, settings) -> None:
    token = settings.api_token
    if token:
        scheme, _, supplied = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(
            supplied.encode(), token.encode()
        ):
            raise HTTPException(
                401, "需要有效的 Bearer API token。", headers={"WWW-Authenticate": "Bearer"}
            )


def validate_endpoint(url: str) -> str:
    parts = urlsplit(url)
    if (
        parts.scheme not in {"https", "http"}
        or not parts.hostname
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise ValueError("A2A 地址须为管理员配置的 HTTP(S) URL，不可含凭据、query 或 fragment。")
    _ = parts.port  # Reject malformed ports before opening a network connection.
    return url.rstrip("/") + "/"


def build_agent_card(settings) -> AgentCard:
    url = validate_endpoint(settings.a2a_public_url)
    card = AgentCard(
        name="SignalDesk",
        description="对已有项目执行证据优先的竞品监测；任务 ID 等于 run ID。",
        version=__version__,
        supported_interfaces=[
            AgentInterface(url=url, protocol_binding="JSONRPC", protocol_version="1.0")
        ],
        capabilities=AgentCapabilities(streaming=False, push_notifications=False),
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        skills=[
            AgentSkill(
                id="monitor-project",
                name="监测已有项目",
                description='单个 JSON data part：{"project_id":"…","mode":"demo|live","demo_revision":1}。message_id 为持久幂等键；不创建项目。',
                tags=["competitive-intelligence", "evidence", "monitoring"],
                input_modes=["application/json"],
                output_modes=["application/json"],
            )
        ],
    )
    if settings.api_token:
        card.security_schemes["bearer"].CopyFrom(
            SecurityScheme(http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="bearer"))
        )
        card.security_requirements.append(SecurityRequirement(schemes={"bearer": StringList()}))
    return card


STATES = {
    "queued": TaskState.TASK_STATE_SUBMITTED,
    "running": TaskState.TASK_STATE_WORKING,
    "completed": TaskState.TASK_STATE_COMPLETED,
    "cancelled": TaskState.TASK_STATE_CANCELED,
    "failed": TaskState.TASK_STATE_FAILED,
    "interrupted": TaskState.TASK_STATE_FAILED,
}


def run_to_task(run) -> Task:
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime.fromisoformat(run.updated_at))
    message = Message(
        message_id=f"status-{run.id}",
        task_id=run.id,
        context_id=run.project_id,
        role=Role.ROLE_AGENT,
        parts=[
            Part(
                text=(
                    "任务执行失败，请在本实例管理端核对。"
                    if run.error
                    else "服务中断，可通过本实例 API 显式恢复。"
                    if run.status == "interrupted"
                    else run.stage
                )
            )
        ],
    )
    task = Task(
        id=run.id,
        context_id=run.project_id,
        status=TaskStatus(state=STATES[run.status], message=message, timestamp=timestamp),
        metadata=ParseDict(
            {"project_id": run.project_id, "run_id": run.id, "mode": run.mode}, Struct()
        ),
    )
    if run.status == "completed" and run.report is not None:
        task.artifacts.append(
            Artifact(
                artifact_id=f"report-{run.id}",
                name="research-report.json",
                parts=[Part(data=ParseDict(run.report, Value()), media_type="application/json")],
            )
        )
    return task


class SignalDeskA2AHandler(RequestHandler):
    def __init__(self, repo, jobs, settings):
        self.repo, self.jobs, self.settings = repo, jobs, settings
        self.bindings = A2ARequestStore(repo)

    async def initialize(self):
        await self.bindings.initialize()

    async def _run(self, task_id, tenant=""):
        if tenant or not task_id or not await self.bindings.contains(task_id):
            raise TaskNotFoundError()
        run = await self.repo.get_run(task_id)
        if run is None:
            raise TaskNotFoundError()
        return run

    async def on_message_send(self, params, context):
        message = params.message
        if (
            params.tenant
            or message.task_id
            or message.role != Role.ROLE_USER
            or len(message.parts) != 1
            or not message.parts[0].HasField("data")
        ):
            raise InvalidParamsError(
                "仅支持用户向已有项目提交单个 JSON data part；不支持租户、任意指令或继续任务。"
            )
        if params.configuration.HasField("task_push_notification_config"):
            raise UnsupportedOperationError("本实例未启用推送通知。")
        try:
            command = A2ACommand.model_validate(MessageToDict(message.parts[0].data))
            payload = command.run_payload()
            if message.context_id and message.context_id != command.project_id:
                raise ValueError("context_id 必须等于 project_id。")
            if payload.mode == "live":
                self.settings.validate_live()

            async def create_and_submit():
                run, created = await self.bindings.create_or_get(
                    command.project_id, payload, message.message_id
                )
                if created:
                    try:
                        self.jobs.submit(run.id)
                    except (RuntimeError, ValueError) as exc:
                        await self.repo.update_run(
                            run.id,
                            status="failed",
                            error="任务服务正在关闭，请使用新的 message_id 重试。",
                        )
                        raise InternalError("任务服务暂不可用。") from exc
                return run

            # The owned operation spans commit, submission, and a failed-submit
            # write. A disconnected HTTP request cannot strand a committed run
            # outside the worker registry; shutdown waits for this operation.
            run = await self.jobs.transition(f"project:{command.project_id}", create_and_submit)
        except (ValueError, IdempotencyConflict) as exc:
            raise InvalidParamsError("项目研究参数、模型配置或 message_id 幂等绑定无效。") from exc
        except KeyError as exc:
            raise TaskNotFoundError("项目不存在。") from exc
        except ActiveRunError as exc:
            raise InvalidParamsError("项目已有其他等待或执行中的任务。") from exc
        except RuntimeError as exc:
            raise InternalError("任务服务正在关闭，暂不接受新任务。") from exc
        # Blocking result polling does not own a lifecycle transition. Holding
        # the project operation open here would make shutdown wait for research.
        if not params.configuration.return_immediately:
            deadline = asyncio.get_running_loop().time() + getattr(
                self.settings, "a2a_timeout_seconds", 180
            )
            while run.status in {"queued", "running"}:
                if asyncio.get_running_loop().time() >= deadline:
                    raise InternalError(
                        "等待任务结果超时；任务仍可通过 GetTask 查询，同 message_id 重试不会重复执行。"
                    )
                await asyncio.sleep(0.05)
                run = await self.repo.get_run(run.id)
        return run_to_task(run)

    async def on_get_task(self, params, context):
        return run_to_task(await self._run(params.id, params.tenant))

    async def on_cancel_task(self, params, context):
        run = await self._run(params.id, params.tenant)
        if run.status != "cancelled":
            try:
                await self.jobs.cancel(run.id)
            except ValueError as exc:
                raise TaskNotCancelableError() from exc
        return run_to_task(await self.repo.get_run(run.id))

    async def _unsupported(self, params, context):
        raise UnsupportedOperationError()

    async def _unsupported_stream(self, params, context):
        raise UnsupportedOperationError()
        yield  # pragma: no cover - SDK requires an async iterator interface.

    on_list_tasks = _unsupported
    on_create_task_push_notification_config = _unsupported
    on_get_task_push_notification_config = _unsupported
    on_list_task_push_notification_configs = _unsupported
    on_delete_task_push_notification_config = _unsupported
    on_get_extended_agent_card = _unsupported
    on_message_send_stream = _unsupported_stream
    on_subscribe_to_task = _unsupported_stream


class A2AAccessMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings):
        super().__init__(app)
        self.settings = settings

    async def dispatch(self, request, call_next):
        try:
            authenticate_a2a(request, self.settings)
        except HTTPException as exc:
            return JSONResponse(
                {"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers
            )
        if request.method == "POST":
            # Bound the entire JSON envelope, including unknown protobuf fields.
            data = bytearray()
            async for chunk in request.stream():
                data.extend(chunk)
                if len(data) > 65536:
                    return JSONResponse({"detail": "A2A 请求超过 64 KiB。"}, status_code=413)
            request._body = bytes(data)
        return await call_next(request)


def create_a2a_app(repo, jobs, settings) -> Starlette:
    bridge = SignalDeskA2AHandler(repo, jobs, settings)
    app = Starlette(
        routes=[
            *create_agent_card_routes(
                build_agent_card(settings), cache_control="private, no-store"
            ),
            *create_jsonrpc_routes(bridge, "/", enable_v0_3_compat=True),
        ]
    )
    app.add_middleware(A2AAccessMiddleware, settings=settings)
    app.state.bridge = bridge
    return app


def create_agent_card_router(settings) -> APIRouter:
    router = APIRouter()

    @router.get("/.well-known/agent-card.json", include_in_schema=False)
    async def agent_card(request: Request):
        authenticate_a2a(request, settings)
        return JSONResponse(
            MessageToDict(build_agent_card(settings)),
            headers={"Cache-Control": "private, no-store"},
        )

    return router
