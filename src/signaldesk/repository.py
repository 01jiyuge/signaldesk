"""Async persistence boundary shared by the API and research workflows."""

import asyncio
import re
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import event, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from signaldesk.config import Settings
from signaldesk.db import Base, Event, Project, Run, Snapshot, utcnow
from signaldesk.schemas import (
    EventRead,
    ProjectCreate,
    ProjectRead,
    RunCreate,
    RunRead,
    SnapshotRecord,
)


class ActiveRunError(ValueError):
    """A project already has an active research run."""


class RunConfigurationError(ValueError):
    """An edited legacy project has no recoverable run configuration."""


def _iso(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


def _date(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _project(row: Project) -> ProjectRead:
    return ProjectRead(
        id=row.id,
        name=row.name,
        description=row.description,
        sources=row.sources,
        focus=row.focus,
        created_at=_iso(row.created_at),
    )


def _run(row: Run) -> RunRead:
    return RunRead(
        id=row.id,
        project_id=row.project_id,
        mode=row.mode,
        demo_revision=row.demo_revision,
        status=row.status,
        stage=row.stage,
        error=row.error,
        report=row.report,
        created_at=_iso(row.created_at),
        updated_at=_iso(row.updated_at),
    )


def _snapshot(row: Snapshot) -> SnapshotRecord:
    return SnapshotRecord(
        id=row.id,
        run_id=row.run_id,
        project_id=row.project_id,
        source_name=row.source_name,
        url=row.url,
        title=row.title,
        text=row.text,
        content_hash=row.content_hash,
        created_at=_iso(row.created_at),
    )


class Repository:
    def __init__(self, settings: Settings):
        self.settings = settings
        db_url = make_url(settings.database_url)
        if db_url.get_backend_name() == "sqlite" and db_url.database not in (None, "", ":memory:"):
            Path(db_url.database).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_async_engine(settings.database_url)
        if db_url.get_backend_name() == "sqlite":

            @event.listens_for(self.engine.sync_engine, "connect")
            def _sqlite_pragmas(connection, _):
                cursor = connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute("PRAGMA busy_timeout=10000")
                cursor.close()

        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self._project_locks: dict[str, asyncio.Lock] = {}

    async def initialize(self) -> None:
        if self.engine.dialect.name == "sqlite":
            async with self.engine.connect() as connection:
                await connection.execution_options(isolation_level="AUTOCOMMIT")
                await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self.engine.dispose()

    async def create_project(self, payload: ProjectCreate) -> ProjectRead:
        row = Project(
            id=str(uuid4()),
            name=payload.name,
            description=payload.description,
            sources=[source.model_dump(mode="json") for source in payload.sources],
            focus=list(payload.focus),
            created_at=utcnow(),
        )
        async with self.sessions.begin() as session:
            session.add(row)
        return _project(row)

    async def list_projects(self) -> list[ProjectRead]:
        async with self.sessions() as session:
            rows = (
                await session.scalars(select(Project).order_by(Project.created_at.desc()))
            ).all()
            return [_project(row) for row in rows]

    async def get_project(self, project_id: str) -> ProjectRead | None:
        async with self.sessions() as session:
            row = await session.get(Project, project_id)
            return _project(row) if row else None

    @asynccontextmanager
    async def _locked_project_session(self, project_id: str):
        """Serialize project edits and run transitions in Python and the database."""
        lock = self._project_locks.setdefault(project_id, asyncio.Lock())
        async with lock:
            async with self.sessions.begin() as session:
                if self.engine.dialect.name == "sqlite":
                    # SQLite has no FOR UPDATE. Acquire its writer reservation
                    # before the active-run read, including across processes.
                    await session.execute(text("BEGIN IMMEDIATE"))
                project = await session.scalar(
                    select(Project).where(Project.id == project_id).with_for_update()
                )
                if project is None:
                    raise KeyError(project_id)
                yield session, project

    async def update_project(self, project_id: str, payload: ProjectCreate) -> ProjectRead:
        async with self._locked_project_session(project_id) as (session, project):
            active = await session.scalar(
                select(Run.id)
                .where(Run.project_id == project_id, Run.status.in_(("queued", "running")))
                .limit(1)
            )
            if active:
                raise ActiveRunError("This project already has an active run")
            legacy = (
                await session.scalars(
                    select(Run).where(
                        Run.project_id == project_id,
                        Run.status.in_(("failed", "interrupted", "cancelled")),
                    )
                )
            ).all()
            for run in legacy:
                configured = await session.scalar(
                    select(Event.id)
                    .where(
                        Event.run_id == run.id,
                        Event.kind.in_(("configuration", "configuration_invalidated")),
                    )
                    .limit(1)
                )
                if not configured:
                    session.add(
                        Event(
                            run_id=run.id,
                            kind="configuration_invalidated",
                            message="项目配置已修改；此旧任务缺少原始配置，请新建任务。",
                            data={},
                            created_at=utcnow(),
                        )
                    )
            project.name = payload.name
            project.description = payload.description
            project.sources = [source.model_dump(mode="json") for source in payload.sources]
            project.focus = list(payload.focus)
            await session.flush()
            return _project(project)

    async def get_run_project(self, run_id: str) -> ProjectRead | None:
        """Read immutable configuration; legacy runs fall back until invalidated."""
        async with self.sessions() as session:
            run = await session.get(Run, run_id)
            if run is None:
                return None
            configuration = await session.scalar(
                select(Event)
                .where(
                    Event.run_id == run_id,
                    Event.kind == "configuration",
                )
                .order_by(Event.id)
                .limit(1)
            )
            if configuration:
                return ProjectRead.model_validate(configuration.data["project"])
            invalidated = await session.scalar(
                select(Event.id)
                .where(
                    Event.run_id == run_id,
                    Event.kind == "configuration_invalidated",
                )
                .limit(1)
            )
            if invalidated:
                return None
            project = await session.get(Project, run.project_id)
            return _project(project) if project else None

    async def create_run(self, project_id: str, payload: RunCreate) -> RunRead:
        async with self._locked_project_session(project_id) as (session, project):
            active = await session.scalar(
                select(Run.id)
                .where(
                    Run.project_id == project_id,
                    Run.status.in_(("queued", "running")),
                )
                .limit(1)
            )
            if active:
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
            try:
                await session.flush()
            except IntegrityError as exc:
                raise ActiveRunError("This project already has an active run") from exc
            session.add(
                Event(
                    run_id=row.id,
                    kind="configuration",
                    message="已保存本次研究配置。",
                    data={"project": _project(project).model_dump(mode="json")},
                    created_at=utcnow(),
                )
            )
            return _run(row)

    async def get_run(self, run_id: str) -> RunRead | None:
        async with self.sessions() as session:
            row = await session.get(Run, run_id)
            return _run(row) if row else None

    async def list_runs(self, project_id: str | None = None) -> list[RunRead]:
        query = select(Run).order_by(Run.created_at.desc())
        if project_id is not None:
            query = query.where(Run.project_id == project_id)
        async with self.sessions() as session:
            return [_run(row) for row in (await session.scalars(query)).all()]

    async def update_run(self, run_id: str, **fields) -> RunRead | None:
        # Joining the transaction on cancellation matters for aiosqlite: its
        # worker thread can finish BEGIN IMMEDIATE after the awaiting task was
        # cancelled. Never leave that writer reservation behind in the pool.
        mutation = asyncio.create_task(self._update_run(run_id, **fields))
        try:
            return await asyncio.shield(mutation)
        except asyncio.CancelledError:
            await asyncio.gather(mutation, return_exceptions=True)
            raise

    async def _update_run(self, run_id: str, **fields) -> RunRead | None:
        allowed = {"status", "stage", "error", "report"}
        unexpected = set(fields) - allowed
        if unexpected:
            raise ValueError(f"Unsupported run fields: {sorted(unexpected)}")
        async with self.sessions() as lookup:
            project_id = await lookup.scalar(select(Run.project_id).where(Run.id == run_id))
            if project_id is None:
                return None
        async with self._locked_project_session(project_id) as (session, _):
            row = await session.get(Run, run_id)
            if fields.get("status") == "queued":
                invalidated = await session.scalar(
                    select(Event.id)
                    .where(
                        Event.run_id == run_id,
                        Event.kind == "configuration_invalidated",
                    )
                    .limit(1)
                )
                if invalidated:
                    raise RunConfigurationError("项目已修改，旧任务缺少原始配置，请新建任务。")
            # Cancellation is terminal until an explicit resume requests queued.
            if row.status == "cancelled" and fields.get("status") not in (
                None,
                "queued",
                "cancelled",
            ):
                return _run(row)
            for key, value in fields.items():
                setattr(row, key, value)
            row.updated_at = utcnow()
            try:
                await session.flush()
            except IntegrityError as exc:
                raise ActiveRunError("This project already has an active run") from exc
            return _run(row)

    async def append_event(
        self, run_id: str, kind: str, message: str, data: dict | None = None
    ) -> EventRead:
        row = Event(run_id=run_id, kind=kind, message=message, data=data or {}, created_at=utcnow())
        async with self.sessions.begin() as session:
            session.add(row)
            await session.flush()
        return EventRead(
            id=row.id,
            run_id=row.run_id,
            kind=row.kind,
            message=row.message,
            data=row.data,
            created_at=_iso(row.created_at),
        )

    async def list_events(self, run_id: str, after: int = 0) -> list[EventRead]:
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(Event)
                    .where(
                        Event.run_id == run_id,
                        Event.id > after,
                    )
                    .order_by(Event.id)
                )
            ).all()
            return [
                EventRead(
                    id=row.id,
                    run_id=row.run_id,
                    kind=row.kind,
                    message=row.message,
                    data=row.data,
                    created_at=_iso(row.created_at),
                )
                for row in rows
            ]

    async def save_snapshot(self, snapshot: SnapshotRecord) -> None:
        values = snapshot.model_dump(mode="python")
        values["created_at"] = _date(values["created_at"])
        async with self.sessions.begin() as session:
            run = await session.get(Run, snapshot.run_id)
            if run is None or run.project_id != snapshot.project_id:
                raise ValueError("Snapshot run and project must belong together")
            existing = await session.get(Snapshot, snapshot.id)
            if existing and (
                existing.run_id != snapshot.run_id or existing.project_id != snapshot.project_id
            ):
                raise ValueError("A snapshot ID cannot be reused across runs or projects")
            # Merge keeps workflow retries idempotent without database-specific SQL.
            await session.merge(Snapshot(**values))

    async def get_snapshots(self, run_id: str) -> list[SnapshotRecord]:
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(Snapshot)
                    .where(
                        Snapshot.run_id == run_id,
                    )
                    .order_by(Snapshot.created_at)
                )
            ).all()
            return [_snapshot(row) for row in rows]

    async def previous_snapshot(
        self, project_id: str, url: str, exclude_run_id: str
    ) -> SnapshotRecord | None:
        async with self.sessions() as session:
            row = await session.scalar(
                select(Snapshot)
                .join(Run, Snapshot.run_id == Run.id)
                .where(
                    Snapshot.project_id == project_id,
                    Snapshot.url == url,
                    Snapshot.run_id != exclude_run_id,
                    Run.status == "completed",
                )
                .order_by(Snapshot.created_at.desc(), Snapshot.id.desc())
                .limit(1)
            )
            return _snapshot(row) if row else None

    async def search_snapshots(
        self, project_id: str, query: str, limit: int = 5
    ) -> list[SnapshotRecord]:
        """Score each source's latest successful revision with a portable baseline."""
        terms = set(re.findall(r"[\w]+", query.casefold()))
        if not terms:
            return []
        latest = (
            select(
                Snapshot.id,
                func.row_number()
                .over(
                    partition_by=Snapshot.url,
                    order_by=(Snapshot.created_at.desc(), Snapshot.id.desc()),
                )
                .label("revision"),
            )
            .join(Run, Snapshot.run_id == Run.id)
            .where(Snapshot.project_id == project_id, Run.status == "completed")
            .subquery()
        )
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(Snapshot)
                    .join(latest, Snapshot.id == latest.c.id)
                    .where(latest.c.revision == 1)
                    .order_by(Snapshot.created_at.desc(), Snapshot.id.desc())
                    .limit(1000)
                )
            ).all()
        scored = []
        for row in rows:
            haystack = f"{row.title}\n{row.text}".casefold()
            score = sum(haystack.count(term) for term in terms)
            if score:
                scored.append((score, row))
        scored.sort(key=lambda item: (item[0], item[1].created_at, item[1].id), reverse=True)
        return [_snapshot(row) for _, row in scored[: max(1, min(limit, 50))]]
