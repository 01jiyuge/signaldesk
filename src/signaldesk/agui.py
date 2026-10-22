"""AG-UI event projection of existing runs over reconnectable SSE.

This read-only transport observes background jobs. It is not a RunAgentInput
runner, and closing a subscriber never cancels the underlying research task.
"""

import asyncio
import json
import re
import secrets
import time
from collections.abc import AsyncIterator
from typing import Annotated

from ag_ui.core import (
    BaseEvent,
    CustomEvent,
    RunErrorEvent,
    RunFinishedEvent,
    RunStartedEvent,
    StateSnapshotEvent,
    StepFinishedEvent,
    StepStartedEvent,
)
from ag_ui.encoder import EventEncoder
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.schemas import EventRead, RunRead

POLL_INTERVAL_SECONDS = 0.25
HEARTBEAT_INTERVAL_SECONDS = 15.0
MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024 - 4096
MAX_CURSOR = 2**53 - 1
TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
STAGE_MESSAGES = {
    "queued": "任务正在等待执行。",
    "collect": "正在采集来源。",
    "compare": "正在比较来源快照。",
    "research": "正在研究证据。",
    "verify": "正在核验报告引用。",
    "persist": "正在保存报告。",
    "completed": "报告已保存。",
    "approval": "报告正在等待人工审批。",
    "awaiting_approval": "报告正在等待人工审批。",
}


def _redact(value: str, limit: int = 600) -> str:
    value = re.sub(r"(?i)\bbearer\s+[^\s,;]+", "Bearer [redacted]", value)
    value = re.sub(r"\bsk-[A-Za-z0-9_-]+", "[redacted]", value)
    value = re.sub(
        r"(?i)\b(api[_-]?key|access[_-]?token|authorization|password|secret)\b\s*[:=]\s*[^\s,;]+",
        r"\1=[redacted]",
        value,
    )
    return value[:limit]


def public_event(event: EventRead) -> dict:
    """Keep the EventRead shape while excluding configuration and raw diagnostics."""
    data = {}
    for key in ("retryable", "model_call_skipped", "context_changed"):
        if type(event.data.get(key)) is bool:
            data[key] = event.data[key]
    for key in ("finding_count", "snapshot_count", "evidence_count"):
        value = event.data.get(key)
        if type(value) is int and 0 <= value <= 100000:
            data[key] = value
    code = event.data.get("code")
    if isinstance(code, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,79}", code):
        data["code"] = code
    stage = event.data.get("stage")
    if isinstance(stage, str) and stage in STAGE_MESSAGES:
        data["stage"] = stage
    suggestion = event.data.get("suggestion")
    if isinstance(suggestion, str) and "code" in data:
        data["suggestion"] = _redact(suggestion)
    messages = {
        "configuration": "已保存本次研究配置。",
        "configuration_invalidated": "项目配置已修改，请新建研究任务。",
        "error": "任务执行失败，请查看错误分类与操作建议。",
        "warning": "部分来源采集出现问题，请查看错误分类与操作建议。",
        "collected": "来源快照已保存。",
        "completed": "报告已保存。",
        "cancelled": "任务已取消。",
        "interrupted": "任务已中断，可以恢复。",
        "resumed": "任务已加入恢复队列。",
        "research_skipped": "来源正文未变化，本次已跳过研究模型。",
        "memory_saved": "研究摘要已加入项目记忆。",
        "sandbox_started": "正在隔离容器中运行 Python 分析。",
        "sandbox_finished": "隔离分析已结束，可在扩展能力中查看执行结果。",
        "sandbox_cancelled": "隔离分析已取消。",
        "a2a_origin": "该研究由 A2A 委托创建。",
    }
    if event.kind == "stage":
        message = STAGE_MESSAGES.get(data.get("stage"), "研究阶段已更新。")
    elif event.kind == "warning" and data.get("code") == "memory_write_failed":
        message = "报告已保存，但写入项目记忆失败。"
    else:
        message = messages.get(event.kind, "任务状态已更新。")
    return {
        "id": event.id,
        "run_id": event.run_id,
        "kind": event.kind if re.fullmatch(r"[a-z][a-z0-9_]{0,79}", event.kind) else "event",
        "message": message,
        "data": data,
        "created_at": event.created_at,
    }


def public_snapshot(run: RunRead) -> dict:
    data = run.model_dump(mode="json")
    if data["error"] is not None:
        data["error"] = "任务执行失败，请查看任务错误提示。" if run.status == "failed" else None
    available = run.status == "completed" and run.report is not None
    snapshot = {"run": data, "reportAvailable": available, "reportOmitted": False}
    if run.report is not None and (
        not available
        or len(json.dumps(snapshot, ensure_ascii=False).encode("utf-8")) > MAX_SNAPSHOT_BYTES
    ):
        data["report"] = None
        snapshot["reportOmitted"] = True
    return snapshot


def _frame(encoder: EventEncoder, event: BaseEvent, *, cursor: int | None = None) -> str:
    encoded = encoder.encode(event)
    # Only business-event frames advance the resumable database cursor. Step and
    # state projections must not skip an undelivered CUSTOM frame on reconnect.
    return (f"id: {cursor}\n" if cursor is not None else "") + encoded


async def stream_run(
    repo: Repository,
    initial: RunRead,
    request: Request,
    after: int = 0,
) -> AsyncIterator[str]:
    encoder = EventEncoder(accept="text/event-stream")
    cursor = after
    metadata = {"projection": "signaldesk.run", "cursor": cursor}
    yield _frame(
        encoder,
        RunStartedEvent(
            thread_id=initial.project_id,
            run_id=initial.id,
            timestamp=int(time.time() * 1000),
            metadata=metadata,
        ),
    )
    yield _frame(encoder, StateSnapshotEvent(snapshot=public_snapshot(initial)))
    fingerprint = (initial.updated_at, initial.status, initial.stage)
    active_step = None
    terminal_seen = None
    last_write = time.monotonic()
    try:
        while not await request.is_disconnected():
            events = await repo.list_events(initial.id, after=cursor)
            for event in events:
                if event.run_id != initial.id or event.id <= cursor:
                    continue
                safe = public_event(event)
                stage = safe["data"].get("stage") if event.kind == "stage" else None
                if stage and stage != active_step:
                    if active_step:
                        yield _frame(encoder, StepFinishedEvent(step_name=active_step))
                    active_step = stage
                    yield _frame(encoder, StepStartedEvent(step_name=active_step))
                yield _frame(
                    encoder, CustomEvent(name="signaldesk.event", value=safe), cursor=event.id
                )
                cursor = event.id
                last_write = time.monotonic()
            current = await repo.get_run(initial.id)
            if current is None:
                yield _frame(encoder, RunErrorEvent(message="任务不存在。", code="run_not_found"))
                return
            current_fingerprint = (current.updated_at, current.status, current.stage)
            if current_fingerprint != fingerprint:
                yield _frame(encoder, StateSnapshotEvent(snapshot=public_snapshot(current)))
                fingerprint = current_fingerprint
                last_write = time.monotonic()
            if current.status in TERMINAL_STATUSES:
                # A report can be saved before Store and final event writes.
                # Wait for the local producer, then drain once after it exits.
                # The extra pass also supports legacy runs with no producer.
                app = getattr(request, "app", None)
                jobs = getattr(getattr(app, "state", None), "jobs", None)
                producer_active = jobs is not None and jobs.is_active(initial.id)
                if terminal_seen == current.status and not producer_active:
                    if active_step:
                        yield _frame(
                            encoder,
                            StepFinishedEvent(
                                step_name=active_step,
                                metadata={"status": current.status},
                            ),
                        )
                    if current.status in {"completed", "cancelled"}:
                        yield _frame(
                            encoder,
                            RunFinishedEvent(
                                thread_id=current.project_id,
                                run_id=current.id,
                                result={
                                    "status": current.status,
                                    "reportAvailable": current.report is not None,
                                },
                                outcome={
                                    "type": "cancelled"
                                    if current.status == "cancelled"
                                    else "success"
                                },
                                metadata={"cursor": cursor},
                            ),
                        )
                    else:
                        yield _frame(
                            encoder,
                            RunErrorEvent(
                                message="任务执行失败，请查看错误分类与操作建议。"
                                if current.status == "failed"
                                else "任务已中断，可以恢复。",
                                code="run_failed"
                                if current.status == "failed"
                                else "run_interrupted",
                                metadata={
                                    "runId": current.id,
                                    "threadId": current.project_id,
                                    "cursor": cursor,
                                },
                            ),
                        )
                    return
                terminal_seen = None if producer_active else current.status
            else:
                terminal_seen = None
                if (
                    active_step is None
                    and current.stage in STAGE_MESSAGES
                    and current.stage != "queued"
                ):
                    active_step = current.stage
                    yield _frame(encoder, StepStartedEvent(step_name=active_step))
            if time.monotonic() - last_write >= HEARTBEAT_INTERVAL_SECONDS:
                yield ": heartbeat\n\n"
                last_write = time.monotonic()
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        # StreamingResponse cancels this observer when the client disconnects.
        # There is deliberately no access to JobManager.cancel here.
        raise
    except Exception:
        yield _frame(
            encoder,
            RunErrorEvent(
                message="事件订阅暂时不可用，请重新连接；后台任务继续执行。",
                code="stream_unavailable",
                metadata={"runId": initial.id, "threadId": initial.project_id, "cursor": cursor},
            ),
        )


def create_agui_router(repo: Repository, settings: Settings) -> APIRouter:
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

    router = APIRouter(prefix="/api/runs", dependencies=[Depends(authenticate)])

    @router.get("/{run_id}/ag-ui")
    async def subscribe(
        run_id: str,
        request: Request,
        after: int = Query(default=0, ge=0, le=MAX_CURSOR),
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ):
        if last_event_id is not None:
            if (
                not last_event_id.isascii()
                or not last_event_id.isdigit()
                or len(last_event_id) > 16
                or not 0 <= int(last_event_id) <= MAX_CURSOR
            ):
                raise HTTPException(422, "Last-Event-ID 必须为有效的非负数据库事件游标。")
            after = max(after, int(last_event_id))
        run = await repo.get_run(run_id)
        if run is None:
            raise HTTPException(404, "任务不存在。")
        return StreamingResponse(
            stream_run(repo, run, request, after),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
        )

    return router
