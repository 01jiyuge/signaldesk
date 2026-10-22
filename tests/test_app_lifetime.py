"""Application ownership tests: failures must not abandon other resources."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from test_sandbox import FakeCLI

import signaldesk.app as application
from signaldesk.config import Settings
from signaldesk.memory import ProjectMemory, open_memory_store
from signaldesk.repository import Repository
from signaldesk.sandbox import DockerSandbox
from signaldesk.schemas import ProjectCreate, RunCreate


def composed_fakes(monkeypatch, *, fail=None, close_failures=()):
    calls, active = [], set()

    async def initialize(name):
        calls.append(name + ".initialize")
        active.add(name)
        if fail == name + ".initialize":
            raise RuntimeError("controlled partial initialize failure")

    async def close(name, operation="close"):
        calls.append(name + "." + operation)
        active.discard(name)
        if name + "." + operation in close_failures:
            raise RuntimeError("controlled closer failure")

    repo = SimpleNamespace(
        initialize=lambda: initialize("repo"),
        close=lambda: close("repo"),
    )
    retriever = SimpleNamespace(
        initialize=lambda: initialize("retriever"),
        close=lambda: close("retriever"),
    )
    sandbox = SimpleNamespace(
        initialize=lambda: initialize("sandbox"),
        shutdown=lambda: close("sandbox", "shutdown"),
    )

    async def recover():
        calls.append("jobs.recover")
        active.add("jobs")
        if fail == "jobs.recover":
            raise RuntimeError("controlled recovery failure")

    jobs = SimpleNamespace(recover_interrupted=recover, shutdown=lambda: close("jobs", "shutdown"))

    @asynccontextmanager
    async def resource(name):
        calls.append(name + ".open")
        if fail == name + ".open":
            raise RuntimeError("controlled context open failure")
        active.add(name)
        try:
            yield SimpleNamespace(name=name)
        finally:
            await close(name)

    monkeypatch.setattr(application, "Repository", lambda _: repo)
    monkeypatch.setattr(application, "Retriever", lambda *_: retriever)
    monkeypatch.setattr(application, "SandboxService", lambda *_: sandbox)
    monkeypatch.setattr(application, "JobManager", lambda *_: jobs)
    monkeypatch.setattr(application, "Workflow", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(application, "open_checkpointer", lambda _: resource("checkpointer"))
    monkeypatch.setattr(application, "open_memory_store", lambda _: resource("store"))
    app = application.create_app(Settings(_env_file=None, enable_a2a=False))
    return app, calls, active


@pytest.mark.parametrize(
    "failure",
    [
        "repo.initialize",
        "retriever.initialize",
        "sandbox.initialize",
        "checkpointer.open",
        "store.open",
        "jobs.recover",
    ],
)
async def test_partial_startup_failure_closes_pre_registered_resources(monkeypatch, failure):
    app, calls, active = composed_fakes(monkeypatch, fail=failure)
    with pytest.raises(RuntimeError, match="controlled"):
        async with app.router.lifespan_context(app):
            raise AssertionError("A failed startup must not yield a running app")
    assert calls[-3:] == ["sandbox.shutdown", "retriever.close", "repo.close"]
    assert (
        calls.count("sandbox.shutdown")
        == calls.count("retriever.close")
        == calls.count("repo.close")
        == 1
    )
    assert not active
    if failure == "store.open":
        assert "checkpointer.close" in calls
    if failure == "jobs.recover":
        assert (
            calls.index("jobs.shutdown")
            < calls.index("store.close")
            < calls.index("checkpointer.close")
        )


@pytest.mark.parametrize(
    "failures",
    [
        ("sandbox.shutdown",),
        ("retriever.close",),
        ("sandbox.shutdown", "retriever.close"),
        ("store.close",),
        ("checkpointer.close",),
        ("jobs.shutdown",),
    ],
)
async def test_one_closer_failure_does_not_skip_other_owners(monkeypatch, failures):
    app, calls, active = composed_fakes(monkeypatch, close_failures=failures)
    with pytest.raises(RuntimeError, match="controlled closer failure"):
        async with app.router.lifespan_context(app):
            assert active == {"repo", "retriever", "sandbox", "checkpointer", "store", "jobs"}
    expected = [
        "jobs.shutdown",
        "store.close",
        "checkpointer.close",
        "sandbox.shutdown",
        "retriever.close",
        "repo.close",
    ]
    assert calls[-6:] == expected
    assert all(calls.count(name) == 1 for name in expected)
    assert not active


async def test_real_persistence_stays_open_until_its_producers_are_joined(tmp_path, monkeypatch):
    # No external model, network, Docker or tracing is used. Only Docker's
    # transport is controlled; repository, Store, checkpoints and owners are real.
    monkeypatch.setenv("LANGSMITH_TRACING", "false")
    monkeypatch.setenv("LANGCHAIN_TRACING_V2", "false")
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/business.db",
        checkpoint_url=f"sqlite:///{tmp_path}/checkpoints.db",
        memory_url=f"sqlite:///{tmp_path}/memory.db",
        sandbox_enabled=True,
        enable_a2a=False,
        enable_vector_search=False,
        model="",
        openai_api_key="",
    )
    app = application.create_app(settings)
    repo, sandbox, retriever = app.state.repo, app.state.sandbox, app.state.retriever
    cli = FakeCLI()
    cli.hold = True
    sandbox.executor = DockerSandbox(settings, cli=cli)
    closed, calls, late_writes = set(), [], []
    original_checkpointer = application.open_checkpointer
    original_memory = application.open_memory_store

    @asynccontextmanager
    async def tracked_checkpointer(url):
        async with original_checkpointer(url) as checkpointer:
            yield checkpointer
        closed.add("checkpointer")
        calls.append("checkpointer.closed")

    @asynccontextmanager
    async def tracked_memory(current_settings):
        async with original_memory(current_settings) as store:
            yield store
        closed.add("store")
        calls.append("store.closed")

    monkeypatch.setattr(application, "open_checkpointer", tracked_checkpointer)
    monkeypatch.setattr(application, "open_memory_store", tracked_memory)
    original_append, original_repo_close = repo.append_event, repo.close
    original_sandbox_close, original_retriever_close = sandbox.shutdown, retriever.close
    stopped = asyncio.Event()
    producers = []

    async def guarded_append(*args, **kwargs):
        if "repo" in closed:
            late_writes.append("event")
            raise AssertionError("A producer wrote after repository closure")
        return await original_append(*args, **kwargs)

    async def sandbox_close():
        assert stopped.is_set(), "Research must finish while Store/checkpoints are open"
        assert {"store", "checkpointer"}.issubset(closed)
        assert "repo" not in closed
        await original_sandbox_close()
        assert all(task.done() for task in producers)
        closed.add("sandbox")
        calls.append("sandbox.closed")

    async def retriever_close():
        assert "sandbox" in closed and "repo" not in closed
        await original_retriever_close()
        closed.add("retriever")
        calls.append("retriever.closed")

    async def repo_close():
        assert {"sandbox", "retriever", "store", "checkpointer"}.issubset(closed)
        assert all(task.done() for task in producers)
        await original_repo_close()
        closed.add("repo")
        calls.append("repo.closed")

    monkeypatch.setattr(repo, "append_event", guarded_append)
    monkeypatch.setattr(repo, "close", repo_close)
    monkeypatch.setattr(sandbox, "shutdown", sandbox_close)
    monkeypatch.setattr(retriever, "close", retriever_close)
    started = asyncio.Event()
    async with app.router.lifespan_context(app):
        project = await repo.create_project(
            ProjectCreate(
                name="Lifetime fixture",
                sources=[{"name": "Example", "url": "https://example.com"}],
            )
        )
        run = await repo.create_run(project.id, RunCreate())
        config = {"configurable": {"thread_id": run.id}}

        async def running_workflow(run_id, resume=False):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                assert not {"store", "checkpointer", "repo"}.intersection(closed)
                await app.state.workflow.graph.aupdate_state(
                    config, {"run_id": run_id}, as_node="collect"
                )
                await app.state.memory.set_preference(
                    project.id, "shutdown", {"text": "persisted while open"}
                )
                await repo.append_event(run_id, "shutdown_flush", "Controlled shutdown flush")
                calls.append("research.flushed")
                stopped.set()

        monkeypatch.setattr(app.state.workflow, "run", running_workflow)
        app.state.jobs.submit(run.id)
        await asyncio.wait_for(started.wait(), 1)
        producers.append(app.state.jobs._tasks[run.id])
        execution = await sandbox.start(project.id, "result=42", run_id=run.id)
        await asyncio.wait_for(cli.started.wait(), 1)
        producers.append(sandbox._tasks[execution.id])
    assert stopped.is_set() and cli.active == 0
    assert not late_writes and all(task.done() for task in producers)
    assert calls == [
        "research.flushed",
        "store.closed",
        "checkpointer.closed",
        "sandbox.closed",
        "retriever.closed",
        "repo.closed",
    ]

    # Reopening independent adapters verifies that the shutdown writes committed
    # to real persistent stores rather than merely updating in-memory flags.
    reopened = Repository(settings)
    await reopened.initialize()
    try:
        assert (await reopened.get_run(run.id)).status == "interrupted"
        kinds = [event.kind for event in await reopened.list_events(run.id)]
        assert "shutdown_flush" in kinds and "sandbox_cancelled" in kinds
        async with open_memory_store(settings) as store:
            preference = await ProjectMemory(store).get_preference(project.id, "shutdown")
            assert preference["value"] == {"text": "persisted while open"}
        async with original_checkpointer(settings.checkpoint_url) as checkpointer:
            checkpoint = await checkpointer.aget_tuple(config)
            assert checkpoint.checkpoint["channel_values"]["run_id"] == run.id
    finally:
        await reopened.close()
