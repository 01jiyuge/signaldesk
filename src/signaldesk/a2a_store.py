"""Atomic A2A message-to-run bindings in the application's database."""

import asyncio
import hashlib
import json
from uuid import uuid4

from sqlalchemy import JSON, Column, ForeignKey, String, Table, insert, select, update
from sqlalchemy.exc import IntegrityError

from signaldesk.db import Base, Event, Run, utcnow
from signaldesk.repository import ActiveRunError, Repository, _project, _run
from signaldesk.schemas import RunCreate, RunRead

requests = Table(
    "a2a_requests",
    Base.metadata,
    Column("message_id", String(128), primary_key=True),
    Column("payload_hash", String(64), nullable=False),
    Column("run_id", ForeignKey("runs.id"), nullable=False, unique=True),
)
delegations = Table(
    "a2a_delegations",
    Base.metadata,
    Column("id", String(36), primary_key=True),
    Column("message_id", String(128), nullable=False, unique=True),
    Column("payload_hash", String(64), nullable=False),
    Column("project_id", ForeignKey("projects.id"), nullable=False),
    Column("run_id", ForeignKey("runs.id"), nullable=True),
    Column("remote", String(80), nullable=False),
    Column("endpoint", String(2048), nullable=False),
    Column("remote_project_id", String(128), nullable=False),
    Column("task", JSON, nullable=True),
    Column("status", String(40), nullable=False),
)


class IdempotencyConflict(ValueError):
    pass


class A2ARequestStore:
    def __init__(self, repo: Repository):
        self.repo = repo
        self._initialized = False
        self._initialize_lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self._initialize_lock:
            if not self._initialized:
                async with self.repo.engine.begin() as connection:
                    await connection.run_sync(lambda conn: requests.create(conn, checkfirst=True))
                    await connection.run_sync(
                        lambda conn: delegations.create(conn, checkfirst=True)
                    )
                self._initialized = True

    async def create_or_get(
        self, project_id: str, payload: RunCreate, message_id: str
    ) -> tuple[RunRead, bool]:
        if not isinstance(message_id, str) or not 1 <= len(message_id) <= 128:
            raise ValueError("message_id 须为 1–128 字符。")
        await self.initialize()
        digest = hashlib.sha256(
            json.dumps(
                {"project_id": project_id, **payload.model_dump(mode="json")},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        try:
            # Reuse the repository's database/Python project locks so project
            # edits, ordinary runs, and A2A requests obey the same invariant.
            async with self.repo._locked_project_session(project_id) as (session, project):
                existing = (
                    (
                        await session.execute(
                            select(requests).where(requests.c.message_id == message_id)
                        )
                    )
                    .mappings()
                    .first()
                )
                if existing:
                    if existing["payload_hash"] != digest:
                        raise IdempotencyConflict("message_id 已绑定不同项目或参数。")
                    return _run(await session.get(Run, existing["run_id"])), False
                if await session.scalar(
                    select(Run.id).where(
                        Run.project_id == project_id, Run.status.in_(("queued", "running"))
                    )
                ):
                    raise ActiveRunError("This project already has an active run")
                row = Run(
                    id=str(uuid4()),
                    project_id=project_id,
                    mode=payload.mode,
                    demo_revision=payload.demo_revision,
                    status="queued",
                    stage="queued",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                )
                session.add(row)
                await session.flush()
                session.add(
                    Event(
                        run_id=row.id,
                        kind="configuration",
                        message="已保存本次研究配置。",
                        data={"project": _project(project).model_dump(mode="json")},
                        created_at=utcnow(),
                    )
                )
                session.add(
                    Event(
                        run_id=row.id,
                        kind="a2a_origin",
                        message="该任务由远端 A2A 调用创建；禁用继续委托。",
                        data={"delegation_depth": 1},
                        created_at=utcnow(),
                    )
                )
                await session.execute(
                    insert(requests).values(
                        message_id=message_id,
                        payload_hash=digest,
                        run_id=row.id,
                    )
                )
                return _run(row), True
        except IntegrityError as exc:
            # A concurrent different-project request can race on the globally
            # unique message ID. The losing run/configuration were rolled back.
            async with self.repo.sessions() as session:
                existing = (
                    (
                        await session.execute(
                            select(requests).where(requests.c.message_id == message_id)
                        )
                    )
                    .mappings()
                    .first()
                )
                if existing:
                    if existing["payload_hash"] != digest:
                        raise IdempotencyConflict("message_id 已绑定不同项目或参数。") from exc
                    return _run(await session.get(Run, existing["run_id"])), False
            raise ActiveRunError("This project already has an active run") from exc

    async def contains(self, run_id: str) -> bool:
        await self.initialize()
        async with self.repo.sessions() as session:
            return bool(
                await session.scalar(select(requests.c.run_id).where(requests.c.run_id == run_id))
            )

    async def prepare_delegation(
        self,
        *,
        project_id,
        run_id,
        remote,
        endpoint,
        remote_project_id,
        message_id,
        mode,
        demo_revision,
    ) -> dict:
        await self.initialize()
        if not isinstance(message_id, str) or not 1 <= len(message_id) <= 128:
            raise ValueError("message_id 须为 1–128 字符。")
        payload = dict(
            project_id=project_id,
            run_id=run_id,
            remote=remote,
            endpoint=endpoint,
            remote_project_id=remote_project_id,
            mode=mode,
            demo_revision=demo_revision,
        )
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        try:
            async with self.repo._locked_project_session(project_id) as (session, _project_row):
                if run_id:
                    run = await session.get(Run, run_id)
                    if run is None or run.project_id != project_id:
                        raise KeyError("Local run does not belong to the project")
                existing = (
                    (
                        await session.execute(
                            select(delegations).where(delegations.c.message_id == message_id)
                        )
                    )
                    .mappings()
                    .first()
                )
                if existing:
                    if existing["payload_hash"] != digest:
                        raise IdempotencyConflict("message_id 已绑定不同委托参数。")
                    return dict(existing)
                values = dict(
                    id=str(uuid4()),
                    message_id=message_id,
                    payload_hash=digest,
                    **{
                        key: payload[key]
                        for key in (
                            "project_id",
                            "run_id",
                            "remote",
                            "endpoint",
                            "remote_project_id",
                        )
                    },
                    task=None,
                    status="PENDING",
                )
                await session.execute(insert(delegations).values(**values))
                return values
        except IntegrityError as exc:
            async with self.repo.sessions() as session:
                existing = (
                    (
                        await session.execute(
                            select(delegations).where(delegations.c.message_id == message_id)
                        )
                    )
                    .mappings()
                    .first()
                )
                if existing and existing["payload_hash"] == digest:
                    return dict(existing)
            raise IdempotencyConflict("message_id 已绑定不同委托参数。") from exc

    async def get_delegation(self, delegation_id) -> dict | None:
        await self.initialize()
        async with self.repo.sessions() as session:
            row = (
                (
                    await session.execute(
                        select(delegations).where(delegations.c.id == delegation_id)
                    )
                )
                .mappings()
                .first()
            )
            return dict(row) if row else None

    async def list_delegations(self, project_id, *, limit=100, offset=0) -> list[dict]:
        if await self.repo.get_project(project_id) is None:
            raise KeyError(project_id)
        await self.initialize()
        async with self.repo.sessions() as session:
            rows = (
                (
                    await session.execute(
                        select(delegations)
                        .where(delegations.c.project_id == project_id)
                        .order_by(delegations.c.id)
                        .limit(max(1, min(limit, 200)))
                        .offset(max(0, offset))
                    )
                )
                .mappings()
                .all()
            )
            return [dict(row) for row in rows]

    async def record_task(self, delegation_id, task: dict) -> None:
        async with self.repo.engine.begin() as connection:
            await connection.execute(
                update(delegations)
                .where(delegations.c.id == delegation_id)
                .values(
                    task=task,
                    status=task["status"]["state"],
                )
            )
