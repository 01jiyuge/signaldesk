"""Disposable, offline Docker execution with bounded output and durable audit rows.

Only Docker's fixed CLI is started on the host. Submitted Python is passed through
stdin to a restricted container; it is never evaluated by the application process.
"""

import asyncio
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict
from sqlalchemy import JSON, DateTime, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from signaldesk.config import Settings
from signaldesk.repository import Repository

SandboxStatus = Literal[
    "queued",
    "running",
    "completed",
    "failed",
    "timed_out",
    "cancelled",
    "output_limited",
    "interrupted",
]
ACTIVE_STATUSES = ("queued", "running")


class SandboxUnavailableError(RuntimeError):
    """A safe, user-facing explanation of unavailable Docker execution."""


class SandboxValidationError(ValueError):
    pass


class SandboxBusyError(RuntimeError):
    pass


class SandboxConflictError(RuntimeError):
    pass


class SandboxResult(BaseModel):
    execution_id: str
    status: SandboxStatus
    exit_code: int | None = None
    result: Any = None
    stdout: str = ""
    stderr: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    duration_seconds: float = 0
    error: str | None = None
    cleanup_confirmed: bool = True


class SandboxExecutionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    project_id: str
    run_id: str | None
    status: SandboxStatus
    image: str
    code_hash: str
    result: SandboxResult | None
    created_at: str
    updated_at: str


@dataclass
class CommandOutput:
    exit_code: int | None
    stdout: bytes = b""
    stderr: bytes = b""
    timed_out: bool = False
    output_limited: bool = False
    stdout_truncated: bool = False
    stderr_truncated: bool = False


class DockerCLI:
    """Bounded Docker subprocess transport; injectable for offline tests."""

    def __init__(self, binary: str = "docker", *, spawn=None):
        self.binary = binary
        self.spawn = spawn or asyncio.create_subprocess_exec

    async def run(
        self,
        args: list[str],
        *,
        stdin: bytes = b"",
        timeout: float = 3,  # noqa: ASYNC109
        max_output_bytes: int = 4096,
    ) -> CommandOutput:
        # Docker context configuration is needed on Desktop. No model/API secrets
        # are inherited by the CLI, and no host environment is passed to Docker.
        env = {
            key: value
            for key, value in os.environ.items()
            if key
            in {
                "PATH",
                "HOME",
                "DOCKER_HOST",
                "DOCKER_CONTEXT",
                "DOCKER_TLS_VERIFY",
                "DOCKER_CERT_PATH",
                "XDG_RUNTIME_DIR",
            }
        }
        process = await self.spawn(
            self.binary,
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            limit=8192,
        )
        exceeded = asyncio.Event()
        chunks: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
        truncated = {"stdout": False, "stderr": False}

        async def read(name: str):
            stream = getattr(process, name)
            while data := await stream.read(4096):
                remaining = max_output_bytes - len(chunks[name])
                chunks[name].extend(data[: max(remaining, 0)])
                if len(data) > remaining:
                    truncated[name] = True
                    exceeded.set()

        async def write():
            try:
                process.stdin.write(stdin)
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                process.stdin.close()

        readers = [asyncio.create_task(read(name)) for name in chunks]
        writer = asyncio.create_task(write())
        waiter = asyncio.create_task(process.wait())
        limiter = asyncio.create_task(exceeded.wait())
        timed_out = False

        async def stop():
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            try:
                await asyncio.wait_for(asyncio.shield(waiter), timeout=2)
            except TimeoutError:
                waiter.cancel()

        try:
            done, _ = await asyncio.wait(
                [waiter, limiter], timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            timed_out = not done
            if timed_out or exceeded.is_set():
                await stop()
            # Closing pipes can lag process exit. Never let a descendant holding
            # a pipe open keep the host request alive indefinitely.
            try:
                await asyncio.wait_for(asyncio.gather(*readers, writer), timeout=1)
            except TimeoutError:
                for task in [*readers, writer]:
                    task.cancel()
        except BaseException:
            await _finish_cleanup(stop())
            raise
        finally:
            for task in [*readers, writer, waiter, limiter]:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*readers, writer, waiter, limiter, return_exceptions=True)
        return CommandOutput(
            exit_code=process.returncode,
            stdout=bytes(chunks["stdout"]),
            stderr=bytes(chunks["stderr"]),
            timed_out=timed_out,
            output_limited=exceeded.is_set(),
            **{
                "stdout_truncated": truncated["stdout"],
                "stderr_truncated": truncated["stderr"],
            },
        )


async def _finish_cleanup(awaitable):
    """Finish bounded cleanup even if a second shutdown/cancel arrives."""
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            continue
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


_HARNESS = r"""
import json, sys, traceback
payload = json.load(sys.stdin)
namespace = {"__name__": "__main__", "input_data": payload["input_data"]}
try:
    exec(compile(payload["code"], "<sandbox>", "exec"), namespace, namespace)
    value = json.dumps({"result": namespace.get("result")}, ensure_ascii=False, allow_nan=False)
    print("\n" + payload["marker"] + value, flush=True)
except BaseException:
    traceback.print_exc()
    sys.exit(1)
""".strip()


def _execution_id(value: str | None = None) -> str:
    try:
        return str(UUID(value)) if value else str(uuid4())
    except (ValueError, AttributeError, TypeError) as exc:
        raise SandboxValidationError("执行 ID 必须是 UUID。") from exc


def container_name(execution_id: str) -> str:
    return "signaldesk-sandbox-" + UUID(_execution_id(execution_id)).hex


def _validate_payload(settings: Settings, code: str, input_data: Any) -> bytes:
    if not isinstance(code, str) or not code.strip():
        raise SandboxValidationError("Python 代码不能为空。")
    try:
        code_bytes = code.encode("utf-8")
        input_bytes = json.dumps(input_data, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise SandboxValidationError("输入必须是有效 JSON，代码必须是有效 UTF-8。") from exc
    if len(code_bytes) > settings.sandbox_max_code_bytes:
        raise SandboxValidationError(f"代码不能超过 {settings.sandbox_max_code_bytes} 字节。")
    if len(input_bytes) > settings.sandbox_max_input_bytes:
        raise SandboxValidationError(f"JSON 输入不能超过 {settings.sandbox_max_input_bytes} 字节。")
    return input_bytes


class DockerSandbox:
    def __init__(self, settings: Settings, *, cli: DockerCLI | None = None):
        self.settings = settings
        self.cli = cli or DockerCLI(settings.sandbox_docker_binary)
        self._slots = asyncio.Semaphore(settings.sandbox_max_concurrent)
        self._probe_lock = asyncio.Lock()
        self._cached: tuple[float, dict] | None = None
        self._active: set[asyncio.Task] = set()
        self._closing = False
        # A failed cleanup blocks new executions until the container is removed.
        self._pending_cleanup: set[str] = set()

    def limits(self) -> dict:
        return {
            "timeout_seconds": self.settings.sandbox_timeout_seconds,
            "memory_mb": self.settings.sandbox_memory_mb,
            "cpus": self.settings.sandbox_cpus,
            "pids": self.settings.sandbox_pids_limit,
            "max_concurrent": self.settings.sandbox_max_concurrent,
            "max_output_bytes": self.settings.sandbox_max_output_bytes,
            "max_code_bytes": self.settings.sandbox_max_code_bytes,
            "max_input_bytes": self.settings.sandbox_max_input_bytes,
        }

    async def capabilities(self) -> dict:
        info = {
            "enabled": self.settings.sandbox_enabled,
            "available": False,
            "reason": "Sandbox 尚未启用。",
            "image": self.settings.sandbox_image,
            "limits": self.limits(),
        }
        if not self.settings.sandbox_enabled:
            return info
        async with self._probe_lock:
            if self._cached and time.monotonic() - self._cached[0] < 5:
                return dict(self._cached[1])
            image = self.settings.sandbox_image
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}", image):
                info["reason"] = "管理员配置的 Sandbox 镜像格式无效。"
            else:
                try:
                    daemon = await self.cli.run(["info", "--format", "{{.ServerVersion}}"])
                    if daemon.exit_code != 0 or daemon.timed_out or daemon.output_limited:
                        info["reason"] = "Docker 服务不可用，请启动 Docker。"
                    else:
                        inspected = await self.cli.run(
                            ["image", "inspect", "--format", "{{.Id}}", image]
                        )
                        if (
                            inspected.exit_code != 0
                            or inspected.timed_out
                            or inspected.output_limited
                        ):
                            info["reason"] = "Sandbox 固定镜像尚未就绪，请管理员预先拉取。"
                        else:
                            for execution_id in tuple(self._pending_cleanup):
                                if await self.cleanup(execution_id):
                                    self._pending_cleanup.discard(execution_id)
                            if self._pending_cleanup:
                                info["reason"] = "上次容器清理未确认，暂时停止新执行。"
                            else:
                                info.update(available=True, reason=None)
                except (OSError, RuntimeError):
                    info["reason"] = "Docker 命令不可用，请检查 Docker 安装。"
            self._cached = (time.monotonic(), info)
            return dict(info)

    async def cleanup(self, execution_id: str) -> bool:
        name = container_name(execution_id)
        try:
            removed = await self.cli.run(["rm", "--force", name], timeout=3)
            if removed.exit_code == 0 and not removed.timed_out:
                return True
            inspected = await self.cli.run(
                ["container", "inspect", "--format", "{{.Id}}", name], timeout=3
            )
            # Non-zero alone is insufficient: Docker itself may be unreachable.
            diagnostic = inspected.stderr.lower()
            return (
                inspected.exit_code != 0
                and not inspected.timed_out
                and (b"no such object" in diagnostic or b"no such container" in diagnostic)
            )
        except (OSError, RuntimeError):
            return False

    def _arguments(self, execution_id: str) -> list[str]:
        s = self.settings
        return [
            "create",
            "--rm",
            "--interactive",
            "--name",
            container_name(execution_id),
            "--label=app=signaldesk-sandbox",
            f"--label=signaldesk.execution={execution_id}",
            "--pull=never",
            "--network=none",
            "--read-only",
            "--user=65534:65534",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges=true",
            f"--cpus={s.sandbox_cpus}",
            f"--memory={s.sandbox_memory_mb}m",
            f"--memory-swap={s.sandbox_memory_mb}m",
            f"--pids-limit={s.sandbox_pids_limit}",
            "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777",
            "--workdir=/tmp",
            "--ipc=none",
            "--init",
            "--log-driver=none",
            "--ulimit=nofile=128:128",
            "--ulimit=core=0:0",
            "--hostname=sandbox",
            "--entrypoint=python",
            s.sandbox_image,
            "-I",
            "-u",
            "-c",
            _HARNESS,
        ]

    async def execute(
        self,
        code: str,
        input_data: Any = None,
        *,
        execution_id: str | None = None,
    ) -> SandboxResult:
        _validate_payload(self.settings, code, input_data)
        execution_id = _execution_id(execution_id)
        if self._closing:
            raise SandboxUnavailableError("Sandbox 正在关闭。")
        available = await self.capabilities()
        if not available["available"]:
            raise SandboxUnavailableError(available["reason"])
        task = asyncio.current_task()
        self._active.add(task)
        try:
            async with self._slots:
                if self._pending_cleanup:
                    available = await self.capabilities()
                    if not available["available"]:
                        raise SandboxUnavailableError(available["reason"])
                marker = "__SIGNALDESK_RESULT_" + uuid4().hex + "__"
                stdin = json.dumps(
                    {"code": code, "input_data": input_data, "marker": marker},
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
                started = time.monotonic()
                result = None
                try:
                    # Separate creation from starting: cancelling a CLI while the
                    # daemon creates a container can otherwise race rm/noSuch.
                    # An unacknowledged create can leave only a dormant container;
                    # untrusted code starts solely after a successful response.
                    created = await self.cli.run(self._arguments(execution_id), timeout=3)
                    if created.exit_code != 0 or created.timed_out or created.output_limited:
                        output = CommandOutput(125)
                    else:
                        output = await self.cli.run(
                            ["start", "--attach", "--interactive", container_name(execution_id)],
                            stdin=stdin,
                            timeout=self.settings.sandbox_timeout_seconds,
                            max_output_bytes=self.settings.sandbox_max_output_bytes,
                        )
                    stdout = output.stdout.decode("utf-8", errors="replace")
                    stderr = output.stderr.decode("utf-8", errors="replace")
                    status: SandboxStatus = "failed"
                    error = "Python 执行失败。" if output.exit_code else None
                    value = None
                    if output.timed_out:
                        status, error = "timed_out", "执行超过时间上限，已停止容器。"
                    elif output.output_limited:
                        status, error = "output_limited", "执行输出超过上限，已停止容器。"
                    elif output.exit_code in (125, 126, 127):
                        # Startup diagnostics can contain local context paths.
                        stderr = ""
                        error = "Docker 容器未能启动，请检查管理员配置。"
                    elif output.exit_code == 0:
                        prefix, found, encoded = stdout.rpartition("\n" + marker)
                        try:
                            if not found:
                                raise ValueError("missing result frame")
                            value = json.loads(encoded)["result"]
                            stdout, status = prefix, "completed"
                        except (ValueError, TypeError, KeyError):
                            error = "Python 未返回有效 JSON 结果，请为 result 赋可序列化值。"
                    result = SandboxResult(
                        execution_id=execution_id,
                        status=status,
                        exit_code=output.exit_code,
                        result=value,
                        stdout=stdout,
                        stderr=stderr,
                        stdout_truncated=output.stdout_truncated,
                        stderr_truncated=output.stderr_truncated,
                        duration_seconds=round(time.monotonic() - started, 4),
                        error=error,
                    )
                except OSError as exc:
                    raise SandboxUnavailableError("Docker 执行不可用，请稍后重试。") from exc
                finally:

                    async def cleanup_and_record():
                        confirmed = await self.cleanup(execution_id)
                        if not confirmed:
                            self._pending_cleanup.add(execution_id)
                            self._cached = None
                        if result is not None:
                            result.cleanup_confirmed = confirmed
                            if not confirmed:
                                result.error = (
                                    "容器清理尚未确认，请管理员检查 Docker；新执行暂时停止。"
                                )

                    await _finish_cleanup(cleanup_and_record())
                return result
        finally:
            self._active.discard(task)

    async def shutdown(self):
        self._closing = True
        current = asyncio.current_task()
        tasks = [task for task in self._active if task is not current]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.settings.sandbox_enabled:
            for execution_id in tuple(self._pending_cleanup):
                if await self.cleanup(execution_id):
                    self._pending_cleanup.discard(execution_id)


class SandboxBase(DeclarativeBase):
    pass


class SandboxExecution(SandboxBase):
    __tablename__ = "sandbox_executions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    project_id: Mapped[str] = mapped_column(String(36), index=True)
    run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    image: Mapped[str] = mapped_column(String(255))
    code_hash: Mapped[str] = mapped_column(String(64))
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return (value.replace(tzinfo=UTC) if value.tzinfo is None else value).isoformat()


def _read(row: SandboxExecution) -> SandboxExecutionRead:
    return SandboxExecutionRead(
        id=row.id,
        project_id=row.project_id,
        run_id=row.run_id,
        status=row.status,
        image=row.image,
        code_hash=row.code_hash,
        result=SandboxResult.model_validate(row.result) if row.result else None,
        created_at=_iso(row.created_at),
        updated_at=_iso(row.updated_at),
    )


class SandboxService:
    """Single-process durable execution queue; owns neither repo nor Docker daemon."""

    def __init__(self, repo: Repository, settings: Settings, executor: DockerSandbox | None = None):
        self.repo = repo
        self.settings = settings
        self.executor = executor or DockerSandbox(settings)
        self._tasks: dict[str, asyncio.Task] = {}
        self._slots = asyncio.Semaphore(settings.sandbox_max_concurrent)
        self._start_lock = asyncio.Lock()
        self._closing = False

    async def initialize(self):
        async with self.repo.engine.begin() as connection:
            await connection.run_sync(SandboxBase.metadata.create_all)
        async with self.repo.sessions() as session:
            rows = (await session.scalars(select(SandboxExecution))).all()
            stale = [
                (row.id, row.project_id, row.status, row.result)
                for row in rows
                if row.status in ACTIVE_STATUSES
                or (row.result and row.result.get("cleanup_confirmed") is False)
            ]
        for execution_id, project_id, status, previous_result in stale:
            confirmed = (
                await self.executor.cleanup(execution_id)
                if self.settings.sandbox_enabled
                else False
            )
            if not confirmed and self.settings.sandbox_enabled:
                self.executor._pending_cleanup.add(execution_id)
            if status in ACTIVE_STATUSES:
                result = SandboxResult(
                    execution_id=execution_id,
                    status="interrupted",
                    cleanup_confirmed=confirmed,
                    error="服务重启中断了执行，请新建任务。",
                )
            else:
                result = SandboxResult.model_validate(previous_result)
                result.cleanup_confirmed = confirmed
            await self._store(project_id, execution_id, result)

    async def capabilities(self) -> dict:
        return await self.executor.capabilities()

    async def _scope(self, project_id: str, run_id: str | None = None):
        if await self.repo.get_project(project_id) is None:
            raise KeyError("项目不存在。")
        if run_id is not None:
            run = await self.repo.get_run(run_id)
            if run is None or run.project_id != project_id:
                raise KeyError("此项目下的研究任务不存在。")

    async def start(
        self,
        project_id: str,
        code: str,
        input_data: Any = None,
        run_id: str | None = None,
    ) -> SandboxExecutionRead:
        await self._scope(project_id, run_id)
        _validate_payload(self.settings, code, input_data)
        capability = await self.capabilities()
        if not capability["available"]:
            raise SandboxUnavailableError(capability["reason"])
        async with self._start_lock:
            if self._closing:
                raise SandboxUnavailableError("Sandbox 正在关闭。")
            if len(self._tasks) >= max(8, self.settings.sandbox_max_concurrent * 8):
                raise SandboxBusyError("Sandbox 队列已满，请稍后重试。")
            execution_id = str(uuid4())
            row = SandboxExecution(
                id=execution_id,
                project_id=project_id,
                run_id=run_id,
                status="queued",
                image=self.settings.sandbox_image,
                code_hash=hashlib.sha256(code.encode()).hexdigest(),
                result=None,
                created_at=_now(),
                updated_at=_now(),
            )
            async with self.repo.sessions.begin() as session:
                session.add(row)
            task = asyncio.create_task(
                self._run(project_id, execution_id, code, input_data, run_id),
                name=f"sandbox:{execution_id}",
            )
            self._tasks[execution_id] = task
            task.add_done_callback(lambda _: self._tasks.pop(execution_id, None))
            return _read(row)

    async def list(self, project_id: str, limit: int = 20) -> list[SandboxExecutionRead]:
        await self._scope(project_id)
        async with self.repo.sessions() as session:
            rows = (
                await session.scalars(
                    select(SandboxExecution)
                    .where(SandboxExecution.project_id == project_id)
                    .order_by(SandboxExecution.created_at.desc(), SandboxExecution.id.desc())
                    .limit(max(1, min(limit, 100)))
                )
            ).all()
            return [_read(row) for row in rows]

    async def get(self, project_id: str, execution_id: str) -> SandboxExecutionRead | None:
        async with self.repo.sessions() as session:
            row = await session.get(SandboxExecution, execution_id)
            return _read(row) if row is not None and row.project_id == project_id else None

    async def _store(self, project_id: str, execution_id: str, result: SandboxResult):
        async with self.repo.sessions.begin() as session:
            row = await session.get(SandboxExecution, execution_id)
            if row is not None and row.project_id == project_id:
                row.status, row.result, row.updated_at = (
                    result.status,
                    result.model_dump(mode="json"),
                    _now(),
                )

    async def _audit(self, run_id: str | None, kind: str, execution_id: str, **data):
        if run_id:
            await self.repo.append_event(
                run_id,
                kind,
                "Sandbox 隔离分析执行状态已更新。",
                {"execution_id": execution_id, **data},
            )

    async def _run(self, project_id, execution_id, code, input_data, run_id):
        try:
            async with self._slots:
                async with self.repo.sessions.begin() as session:
                    row = await session.get(SandboxExecution, execution_id)
                    row.status, row.updated_at = "running", _now()
                await self._audit(run_id, "sandbox_started", execution_id)
                result = await self.executor.execute(code, input_data, execution_id=execution_id)
                await self._store(project_id, execution_id, result)
                await self._audit(run_id, "sandbox_finished", execution_id, status=result.status)
        except asyncio.CancelledError:

            async def store_cancelled():
                current = await self.get(project_id, execution_id)
                if current and current.status in ACTIVE_STATUSES:
                    await self._store(
                        project_id,
                        execution_id,
                        SandboxResult(
                            execution_id=execution_id,
                            status="cancelled",
                            cleanup_confirmed=execution_id not in self.executor._pending_cleanup,
                            error="执行已取消。",
                        ),
                    )

            await _finish_cleanup(store_cancelled())
            await _finish_cleanup(self._audit(run_id, "sandbox_cancelled", execution_id))
            raise
        except Exception:
            # Driver/model/daemon messages may contain credentials or local paths.
            # Persist a stable error without exception repr or traceback.
            await self._store(
                project_id,
                execution_id,
                SandboxResult(
                    execution_id=execution_id,
                    status="failed",
                    cleanup_confirmed=execution_id not in self.executor._pending_cleanup,
                    error="Sandbox 执行失败，请检查 Docker 状态后重试。",
                ),
            )
            await self._audit(run_id, "sandbox_finished", execution_id, status="failed")

    async def cancel(self, project_id: str, execution_id: str) -> SandboxExecutionRead:
        current = await self.get(project_id, execution_id)
        if current is None:
            raise KeyError("此项目下的 Sandbox 执行不存在。")
        if current.status not in ACTIVE_STATUSES:
            raise SandboxConflictError("执行已结束，无法取消。")
        task = self._tasks.get(execution_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        current = await self.get(project_id, execution_id)
        # A task cancelled before its first coroutine instruction cannot enter
        # _run's finally/cancel handler; persist cancellation here as well.
        if current.status in ACTIVE_STATUSES:
            confirmed = await self.executor.cleanup(execution_id)
            if not confirmed:
                self.executor._pending_cleanup.add(execution_id)
                self.executor._cached = None
            await self._store(
                project_id,
                execution_id,
                SandboxResult(
                    execution_id=execution_id,
                    status="cancelled",
                    cleanup_confirmed=confirmed,
                    error="执行已取消。",
                ),
            )
        return await self.get(project_id, execution_id)

    async def execute(
        self,
        project_id: str,
        code: str,
        input_data: Any = None,
        run_id: str | None = None,
    ) -> SandboxResult:
        row = await self.start(project_id, code, input_data, run_id)
        task = self._tasks.get(row.id)
        try:
            if task:
                await asyncio.shield(task)
        except asyncio.CancelledError:
            current = await self.get(project_id, row.id)
            if current and current.status in ACTIVE_STATUSES:
                await _finish_cleanup(self.cancel(project_id, row.id))
            raise
        finished = await self.get(project_id, row.id)
        return finished.result

    async def shutdown(self):
        self._closing = True
        # Use scoped cancel so queued tasks cancelled before first execution also
        # acquire durable terminal records.
        tasks = list(self._tasks)
        for execution_id in tasks:
            async with self.repo.sessions() as session:
                row = await session.get(SandboxExecution, execution_id)
                project_id = row.project_id if row else None
            if project_id:
                current = await self.get(project_id, execution_id)
                if current and current.status in ACTIVE_STATUSES:
                    await self.cancel(project_id, execution_id)
        await self.executor.shutdown()
