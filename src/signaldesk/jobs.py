"""Bounded in-process execution with explicit interruption and cancellation states."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from signaldesk.errors import public_failure
from signaldesk.repository import Repository

logger = logging.getLogger(__name__)


class JobManager:
    def __init__(
        self, repo: Repository, runner: Callable[..., Awaitable[None]], max_concurrent_runs: int = 2
    ):
        self.repo = repo
        self.runner = runner
        self._semaphore = asyncio.Semaphore(max(1, max_concurrent_runs))
        self._tasks: dict[str, asyncio.Task] = {}
        self._closing = False

    def is_active(self, run_id: str) -> bool:
        """Keep observers open until the producer has finished all final writes."""
        task = self._tasks.get(run_id)
        return task is not None and not task.done()

    def submit(self, run_id: str, resume: bool = False) -> None:
        if self._closing:
            raise RuntimeError("Job manager is shutting down")
        task = self._tasks.get(run_id)
        if task and not task.done():
            raise ValueError("This run is already executing")
        self._tasks[run_id] = asyncio.create_task(
            self._execute(run_id, resume), name=f"research-{run_id}"
        )

    async def _execute(self, run_id: str, resume: bool) -> None:
        try:
            async with self._semaphore:
                run = await self.repo.get_run(run_id)
                if run is None or run.status == "cancelled":
                    return
                await self.repo.update_run(run_id, status="running", error=None)
                await self.runner(run_id, resume=resume)
        except asyncio.CancelledError:
            if self._closing:
                run = await self.repo.get_run(run_id)
                if run and run.status in ("queued", "running"):
                    await self.repo.update_run(run_id, status="interrupted")
                    await self.repo.append_event(
                        run_id, "interrupted", "Service stopped; this run can be resumed"
                    )
            raise
        except Exception as exc:
            # Provider exceptions can contain request headers or private source
            # content. Keep raw messages out of both API state and default logs.
            logger.error("Research run %s failed (%s)", run_id, type(exc).__name__)
            run = await self.repo.get_run(run_id)
            if run and run.status not in {"cancelled", "completed"}:
                detail = public_failure(exc)
                await self.repo.update_run(run_id, status="failed", error=detail.message)
                await self.repo.append_event(
                    run_id,
                    "error",
                    detail.message,
                    {
                        "error_type": type(exc).__name__,
                        "code": detail.code,
                        "retryable": detail.retryable,
                        "suggestion": detail.suggestion,
                    },
                )
        finally:
            current = asyncio.current_task()
            if self._tasks.get(run_id) is current:
                self._tasks.pop(run_id, None)

    async def cancel(self, run_id: str) -> None:
        run = await self.repo.get_run(run_id)
        if run is None:
            raise KeyError(run_id)
        if run.status not in ("queued", "running"):
            raise ValueError("Only queued or running runs can be cancelled")
        # Cancel and join before persisting cancellation so no workflow write can
        # race after the final state has been recorded.
        task = self._tasks.get(run_id)
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._tasks.pop(run_id, None)
        current = await self.repo.get_run(run_id)
        if current and current.status == "completed":
            raise ValueError("The report finished before cancellation")
        await self.repo.update_run(run_id, status="cancelled", error=None)
        await self.repo.append_event(run_id, "cancelled", "Run cancelled by the user")

    async def recover_interrupted(self) -> None:
        for run in await self.repo.list_runs():
            if run.status in ("queued", "running"):
                await self.repo.update_run(run.id, status="interrupted")
                await self.repo.append_event(
                    run.id, "interrupted", "Previous process stopped; resume this run explicitly"
                )

    async def shutdown(self) -> None:
        self._closing = True
        run_ids = list(self._tasks)
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # A task cancelled before its first scheduling never enters _execute.
        for run_id in run_ids:
            run = await self.repo.get_run(run_id)
            if run and run.status in ("queued", "running"):
                await self.repo.update_run(run_id, status="interrupted")
                await self.repo.append_event(
                    run_id, "interrupted", "Service stopped; this run can be resumed"
                )
        self._tasks.clear()
