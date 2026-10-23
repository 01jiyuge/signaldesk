"""Official SDK delegation to administrator-named, exact A2A endpoints only."""

import asyncio
from contextlib import asynccontextmanager
from typing import Literal

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import (
    CancelTaskRequest,
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
)
from a2a.utils.errors import TaskNotCancelableError, TaskNotFoundError
from fastapi import APIRouter, HTTPException, Query, Request
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Value
from pydantic import BaseModel, ConfigDict, Field

from signaldesk.a2a_server import A2ACommand, authenticate_a2a, validate_endpoint
from signaldesk.a2a_store import TERMINAL_TASK_STATES, A2ARequestStore, IdempotencyConflict
from signaldesk.errors import PublicTaskError
from signaldesk.history import HistoryCursorError, HistoryPage, utc_date

MAX_REMOTE_RESPONSE_BYTES = 2 * 1024 * 1024
SEND_ACK_GRACE_SECONDS = 5
REMOTE_CANCEL_GRACE_SECONDS = 5
SEND_DRAIN_GRACE_SECONDS = 5


class BoundedResponseStream(httpx.AsyncByteStream):
    """Limit bytes before httpx's automatic read and the SDK's JSON parsing."""

    def __init__(self, stream, max_bytes):
        self.stream, self.max_bytes = stream, max_bytes

    async def __aiter__(self):
        received = 0
        async for chunk in self.stream:
            received += len(chunk)
            if received > self.max_bytes:
                await self.aclose()
                raise PublicTaskError(
                    "远端 A2A 响应超过 2 MiB 限制。", code="a2a_response_too_large"
                )
            yield chunk

    async def aclose(self):
        await self.stream.aclose()


class BoundedResponseTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner=None, max_bytes=MAX_REMOTE_RESPONSE_BYTES):
        self.inner = inner if inner is not None else httpx.AsyncHTTPTransport(trust_env=False)
        self.max_bytes = max_bytes

    async def handle_async_request(self, request):
        response = await self.inner.handle_async_request(request)
        try:
            # Identity encoding prevents a compressed response from expanding
            # beyond the wire limit inside httpx's decoder before we can count it.
            if response.headers.get("content-encoding", "identity").lower() != "identity":
                raise PublicTaskError(
                    "远端 A2A 响应未使用请求的 identity 编码。",
                    code="a2a_response_encoding_unsupported",
                )
            declared = response.headers.get("content-length")
            if declared is not None and int(declared) > self.max_bytes:
                raise PublicTaskError(
                    "远端 A2A 响应超过 2 MiB 限制。", code="a2a_response_too_large"
                )
        except BaseException:
            await response.aclose()
            raise
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            stream=BoundedResponseStream(response.stream, self.max_bytes),
            extensions=response.extensions,
        )

    async def aclose(self):
        await self.inner.aclose()


class A2ADelegate:
    def __init__(self, settings, repo):
        self.settings = settings
        self.repo = repo
        self.bindings = A2ARequestStore(repo)

    @asynccontextmanager
    async def _client(self, remote: str):
        remotes = getattr(self.settings, "a2a_remote_agents", {})
        if remote not in remotes:
            raise PublicTaskError(
                "远端 Agent 不在管理员白名单中。",
                code="a2a_remote_not_allowed",
                suggestion="请管理员配置 A2A_REMOTE_AGENTS 中的名称与固定地址。",
            )
        try:
            endpoint = validate_endpoint(remotes[remote])
            token = getattr(self.settings, "a2a_remote_tokens", {}).get(remote, "")
            headers = {"Accept-Encoding": "identity"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            async with httpx.AsyncClient(
                headers=headers,
                timeout=getattr(self.settings, "a2a_timeout_seconds", 180),
                follow_redirects=False,
                trust_env=False,
                transport=BoundedResponseTransport(),
            ) as http:
                card = await A2ACardResolver(http, endpoint).get_agent_card()
                interfaces = [
                    item
                    for item in card.supported_interfaces
                    if item.protocol_binding == "JSONRPC"
                    and item.protocol_version in {"1.0", "1.0.0"}
                    and validate_endpoint(item.url) == endpoint
                ]
                if not interfaces:
                    raise ValueError("Agent Card 未提供白名单地址的协议1.0 JSONRPC接口。")
                # Never let the SDK select a card-advertised different origin,
                # path, protocol, tenant, or extended-card authentication flow.
                approved = type(interfaces[0])()
                approved.CopyFrom(interfaces[0])
                if approved.tenant:
                    raise ValueError("This adapter does not support multi-tenant remote interfaces")
                del card.supported_interfaces[:]
                card.supported_interfaces.append(approved)
                client = ClientFactory(
                    ClientConfig(
                        streaming=False,
                        polling=False,
                        httpx_client=http,
                        supported_protocol_bindings=["JSONRPC"],
                    )
                ).create(card)
                try:
                    yield client
                finally:
                    await client.close()
        except PublicTaskError:
            raise
        except TaskNotFoundError as exc:
            raise PublicTaskError("远端项目或任务不存在。", code="a2a_remote_not_found") from exc
        except TaskNotCancelableError as exc:
            raise PublicTaskError(
                "远端任务已结束，无法取消。", code="a2a_task_not_cancelable"
            ) from exc
        except Exception as exc:
            raise PublicTaskError(
                "远端 A2A 调用失败或响应不符合协议。",
                code="a2a_remote_failed",
                retryable=True,
                suggestion="检查管理员白名单、远端 token、协议版本和任务 ID；同 message_id 重试不会在支持幂等的 SignalDesk 远端重复创建任务。",
            ) from exc

    @staticmethod
    def _check_project(task, project_id):
        if task.context_id != project_id:
            raise PublicTaskError(
                "远端任务不属于所请求项目。",
                code="a2a_project_mismatch",
                suggestion="检查远端项目 ID 与任务 ID 的对应关系。",
            )

    @staticmethod
    def _public(record):
        result = {
            key: record[key]
            for key in (
                "id",
                "project_id",
                "run_id",
                "remote",
                "remote_project_id",
                "status",
                "task",
            )
        }
        created = record.get("created_at")
        if created is not None:
            created = utc_date(created).isoformat()
        return {**result, "created_at": created}

    async def _registered(self, delegation_id):
        record = await self.bindings.get_delegation(delegation_id)
        if record is None:
            raise KeyError(delegation_id)
        endpoint = getattr(self.settings, "a2a_remote_agents", {}).get(record["remote"])
        if not endpoint or validate_endpoint(endpoint) != record["endpoint"]:
            raise PublicTaskError(
                "登记委托的远端地址已不在当前白名单中。", code="a2a_remote_not_allowed"
            )
        if record["task"] is None:
            raise PublicTaskError(
                "委托尚未取得远端任务 ID；请用原 message_id 重试提交。",
                code="a2a_task_pending",
                retryable=True,
            )
        return record

    async def send(
        self,
        remote: str,
        remote_project_id: str,
        *,
        local_project_id: str,
        local_run_id: str | None = None,
        mode="demo",
        demo_revision=1,
        message_id: str,
        _acknowledged: asyncio.Future | None = None,
    ) -> dict:
        if not isinstance(message_id, str) or not 1 <= len(message_id) <= 128:
            raise PublicTaskError("message_id 须为 1–128 字符。", code="a2a_invalid_message_id")
        async with self.bindings.serialize_send(message_id) as (gate, waiting):
            return await self._send_locked(
                remote,
                remote_project_id,
                local_project_id=local_project_id,
                local_run_id=local_run_id,
                mode=mode,
                demo_revision=demo_revision,
                message_id=message_id,
                _acknowledged=_acknowledged,
                _send_gate=gate,
                _waited=waiting,
            )

    async def _send_locked(
        self,
        remote: str,
        remote_project_id: str,
        *,
        local_project_id: str,
        local_run_id: str | None,
        mode,
        demo_revision,
        message_id: str,
        _acknowledged: asyncio.Future | None,
        _send_gate,
        _waited: bool,
    ) -> dict:
        command = A2ACommand(project_id=remote_project_id, mode=mode, demo_revision=demo_revision)
        command.run_payload()
        if not 1 <= len(message_id) <= 128:
            raise PublicTaskError("message_id 须为 1–128 字符。", code="a2a_invalid_message_id")
        endpoint = getattr(self.settings, "a2a_remote_agents", {}).get(remote)
        if endpoint is None:
            raise PublicTaskError("远端 Agent 不在管理员白名单中。", code="a2a_remote_not_allowed")
        record = await self.bindings.prepare_delegation(
            project_id=local_project_id,
            run_id=local_run_id,
            remote=remote,
            endpoint=validate_endpoint(endpoint),
            remote_project_id=remote_project_id,
            mode=mode,
            demo_revision=demo_revision,
            message_id=message_id,
        )
        if record["task"] is not None:
            result = self._public(record)
            if _acknowledged is not None and not _acknowledged.done():
                _acknowledged.set_result(result)
            return result
        if _waited and _send_gate.failed:
            raise PublicTaskError(
                "相同消息的并发发送尚未确认远端任务，请核对 PENDING 登记后显式重试。",
                code="a2a_task_pending",
                retryable=True,
                suggestion="远端可能已接受任务；缺失 ACK 时无法保证外部恰好执行一次。",
            )
        request = SendMessageRequest(
            message=Message(
                message_id=message_id,
                context_id=remote_project_id,
                role=Role.ROLE_USER,
                parts=[
                    Part(
                        data=ParseDict(command.model_dump(), Value()), media_type="application/json"
                    )
                ],
            ),
            configuration=SendMessageConfiguration(return_immediately=True),
        )
        try:
            async with self._client(remote) as client:
                async for response in client.send_message(request):
                    if not response.HasField("task"):
                        raise ValueError("Remote response did not contain a task")
                    self._check_project(response.task, remote_project_id)
                    task = MessageToDict(response.task)
                    accepted = await self.bindings.record_task(record["id"], task)
                    result = self._public(accepted)
                    if _acknowledged is not None and not _acknowledged.done():
                        _acknowledged.set_result(result)
                    return result
                raise ValueError("Remote did not return a task")
        except BaseException:
            _send_gate.failed = True
            raise

    async def get(self, delegation_id: str) -> dict:
        record = await self._registered(delegation_id)
        async with self._client(record["remote"]) as client:
            task = await client.get_task(GetTaskRequest(id=record["task"]["id"]))
            self._check_project(task, record["remote_project_id"])
            data = MessageToDict(task)
            accepted = await self.bindings.record_task(record["id"], data)
            return self._public(accepted)

    async def list(self, project_id: str, *, limit: int = 100, offset: int = 0) -> list[dict]:
        return [
            self._public(record)
            for record in await self.bindings.list_delegations(
                project_id, limit=limit, offset=offset
            )
        ]

    async def local_record(self, project_id: str, delegation_id: str) -> dict:
        if await self.repo.get_project(project_id) is None:
            raise KeyError(project_id)
        record = await self.bindings.get_delegation(delegation_id)
        if record is None or record["project_id"] != project_id:
            raise KeyError(delegation_id)
        return self._public(record)

    async def page(self, project_id: str, *, limit: int = 20, cursor: str | None = None) -> dict:
        page = await self.bindings.page_delegations(project_id, limit=limit, cursor=cursor)
        return {**page, "items": [self._public(record) for record in page["items"]]}

    async def cancel(self, delegation_id: str) -> dict:
        record = await self._registered(delegation_id)
        task_id = record["task"]["id"]
        async with self._client(record["remote"]) as client:
            current = await client.get_task(GetTaskRequest(id=task_id))
            self._check_project(current, record["remote_project_id"])
            accepted = await self.bindings.record_task(record["id"], MessageToDict(current))
            state = accepted["status"]
            if state == "TASK_STATE_CANCELED":
                return self._public(accepted)
            if state in TERMINAL_TASK_STATES:
                raise TaskNotCancelableError()
            task = await client.cancel_task(CancelTaskRequest(id=task_id))
            self._check_project(task, record["remote_project_id"])
            data = MessageToDict(task)
            accepted = await self.bindings.record_task(record["id"], data)
            if (
                accepted["status"] in TERMINAL_TASK_STATES
                and accepted["status"] != "TASK_STATE_CANCELED"
            ):
                raise TaskNotCancelableError()
            return self._public(accepted)

    async def delegate_wait(
        self,
        remote: str,
        remote_project_id: str,
        *,
        local_project_id: str,
        local_run_id: str,
        message_id: str,
        mode="live",
        demo_revision=1,
        timeout_seconds: float | None = None,
    ) -> dict:
        if getattr(self.settings, "a2a_remote_projects", {}).get(remote) != remote_project_id:
            raise PublicTaskError(
                "自动委托目标未在管理员项目映射中授权。",
                code="a2a_remote_project_not_allowed",
            )
        local_run = await self.repo.get_run(local_run_id)
        if local_run is None or local_run.project_id != local_project_id:
            raise KeyError("Local run does not belong to the project")
        if any(event.kind == "a2a_origin" for event in await self.repo.list_events(local_run_id)):
            raise PublicTaskError("入站 A2A 任务不可继续自动委托。", code="a2a_recursion_blocked")
        record = None
        acknowledged = asyncio.get_running_loop().create_future()
        # Own this single attempt, including its HTTP client closure and local
        # record_task commit. Cancellation of the waiter must not destroy an
        # acknowledgement for a task the remote has already accepted.
        send_task = asyncio.create_task(
            self.send(
                remote,
                remote_project_id,
                local_project_id=local_project_id,
                local_run_id=local_run_id,
                message_id=message_id,
                mode=mode,
                demo_revision=demo_revision,
                _acknowledged=acknowledged,
            ),
            name=f"a2a-send-{local_run_id}",
        )

        async def cleanup():
            nonlocal record
            try:
                if record is None:
                    try:
                        done, _ = await asyncio.wait(
                            {send_task, acknowledged},
                            timeout=SEND_ACK_GRACE_SECONDS,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        # The durable acknowledgement can precede the SDK
                        # context's HTTP-client close. Do not lose that known
                        # task ID just because closing the send is still pending.
                        if acknowledged in done:
                            record = acknowledged.result()
                        elif send_task in done:
                            record = send_task.result()
                    except (Exception, asyncio.CancelledError):
                        # No second POST: an absent acknowledgement remains
                        # unconfirmed, and the original attempt is drained below.
                        pass
                if record is not None:
                    async with asyncio.timeout(REMOTE_CANCEL_GRACE_SECONDS):
                        await self.cancel(record["id"])
            except Exception:
                pass  # Best effort only: network failure cannot prove remote cancellation.
            finally:
                if not send_task.done():
                    send_task.cancel()
                # Drain the owned, cancellation-cooperative SDK/HTTP request,
                # including its finally/close. This gets its own finite budget
                # rather than an unbounded await after the ACK deadline expires.
                try:
                    async with asyncio.timeout(SEND_DRAIN_GRACE_SECONDS):
                        await asyncio.gather(send_task, return_exceptions=True)
                except TimeoutError:
                    pass
                if not acknowledged.done():
                    acknowledged.cancel()

        async def finish_cleanup():
            cleanup_task = asyncio.create_task(cleanup(), name=f"a2a-cleanup-{local_run_id}")
            cancelled_again = False
            # A caller can cancel more than once, including while a timeout is
            # being handled. Preserve cleanup ownership until the same cleanup
            # finishes; a bare shield await would leave it in the background.
            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError:
                    cancelled_again = True
            cleanup_task.result()
            return cancelled_again

        try:
            async with asyncio.timeout(
                timeout_seconds or getattr(self.settings, "a2a_timeout_seconds", 180)
            ):
                record = await asyncio.shield(send_task)
                while record["status"] in {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"}:
                    await asyncio.sleep(0.2)
                    record = await self.get(record["id"])
                if record["status"] != "TASK_STATE_COMPLETED":
                    raise PublicTaskError(
                        "远端 A2A 研究未成功完成，请查看登记委托状态。",
                        code="a2a_remote_task_failed",
                        suggestion=f"登记委托 {record['id']} 的状态为 {record['status']}。",
                    )
                return record
        except TimeoutError as exc:
            if await finish_cleanup():
                raise asyncio.CancelledError() from None
            raise PublicTaskError(
                "等待远端 A2A 任务超时；已尽力请求取消，请核对登记委托状态。",
                code="a2a_timeout",
                retryable=True,
            ) from exc
        except asyncio.CancelledError:
            await finish_cleanup()
            raise


class DelegateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    remote: str = Field(min_length=1, max_length=80)
    remote_project_id: str = Field(min_length=1, max_length=128)
    message_id: str = Field(min_length=1, max_length=128)
    mode: Literal["demo", "live"] = "demo"
    demo_revision: Literal[1, 2] = 1


class DelegationRead(BaseModel):
    id: str
    project_id: str
    run_id: str | None
    remote: str
    remote_project_id: str
    status: str
    task: dict | None
    created_at: str | None


def create_a2a_delegation_router(repo, settings) -> APIRouter:
    router = APIRouter(prefix="/api")
    delegate = A2ADelegate(settings, repo)

    async def call(operation):
        try:
            return await operation
        except KeyError as exc:
            raise HTTPException(404, "项目或登记委托不存在。") from exc
        except IdempotencyConflict as exc:
            raise HTTPException(409, "message_id 已绑定不同委托参数。") from exc
        except HistoryCursorError as exc:
            raise HTTPException(422, str(exc)) from exc
        except PublicTaskError as exc:
            if exc.code == "a2a_remote_not_found":
                raise HTTPException(404, str(exc)) from exc
            if exc.code == "a2a_task_not_cancelable":
                raise HTTPException(409, str(exc)) from exc
            raise

    @router.post("/projects/{project_id}/a2a/delegations", status_code=202)
    async def send(project_id: str, payload: DelegateRequest, request: Request):
        authenticate_a2a(request, settings)
        return await call(
            delegate.send(
                payload.remote,
                payload.remote_project_id,
                local_project_id=project_id,
                mode=payload.mode,
                demo_revision=payload.demo_revision,
                message_id=payload.message_id,
            )
        )

    @router.get("/a2a/delegations/{delegation_id}")
    async def get(delegation_id: str, request: Request):
        authenticate_a2a(request, settings)
        return await call(delegate.get(delegation_id))

    @router.get("/projects/{project_id}/a2a/delegations")
    async def list_delegations(
        project_id: str,
        request: Request,
        limit: int = Query(default=100, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ):
        authenticate_a2a(request, settings)
        return await call(delegate.list(project_id, limit=limit, offset=offset))

    @router.post("/a2a/delegations/{delegation_id}/cancel")
    async def cancel(delegation_id: str, request: Request):
        authenticate_a2a(request, settings)
        return await call(delegate.cancel(delegation_id))

    @router.get(
        "/projects/{project_id}/a2a/delegations/page", response_model=HistoryPage[DelegationRead]
    )
    async def page_delegations(
        project_id: str,
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
        cursor: str | None = Query(default=None, min_length=1, max_length=512),
    ):
        authenticate_a2a(request, settings)
        return await call(delegate.page(project_id, limit=limit, cursor=cursor))

    @router.get(
        "/projects/{project_id}/a2a/delegations/{delegation_id}", response_model=DelegationRead
    )
    async def local_record(project_id: str, delegation_id: str, request: Request):
        authenticate_a2a(request, settings)
        return await call(delegate.local_record(project_id, delegation_id))

    return router
