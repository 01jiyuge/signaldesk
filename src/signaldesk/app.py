"""Single-process application composition and resource lifetime."""

from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from signaldesk import __version__
from signaldesk.a2a_client import A2ADelegate, create_a2a_delegation_router
from signaldesk.a2a_server import create_a2a_app, create_agent_card_router
from signaldesk.agui import create_agui_router
from signaldesk.api import create_router
from signaldesk.checkpoints import open_checkpointer
from signaldesk.config import Settings, get_settings
from signaldesk.errors import PublicTaskError
from signaldesk.jobs import JobManager
from signaldesk.memory import ProjectMemory, open_memory_store
from signaldesk.memory_api import create_memory_router
from signaldesk.repository import Repository
from signaldesk.retrieval import Retriever
from signaldesk.sandbox import SandboxService
from signaldesk.sandbox_api import create_sandbox_router
from signaldesk.workflow import Workflow


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    repo = Repository(settings)
    retriever = Retriever(repo, settings)
    sandbox = SandboxService(repo, settings)
    delegate = A2ADelegate(settings, repo) if settings.enable_a2a else None
    workflow = None
    a2a_app = None

    async def run_workflow(run_id: str, resume: bool = False):
        if workflow is None:
            raise RuntimeError("Workflow has not started")
        await workflow.run(run_id, resume=resume)

    jobs = JobManager(repo, run_workflow, settings.max_concurrent_runs)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal workflow
        try:
            await repo.initialize()
            await retriever.initialize()
            await sandbox.initialize()
            if a2a_app is not None:
                await a2a_app.state.bridge.initialize()
            async with AsyncExitStack() as stack:
                checkpointer = await stack.enter_async_context(
                    open_checkpointer(settings.checkpoint_url)
                )
                store = await stack.enter_async_context(open_memory_store(settings))
                memory = ProjectMemory(store, settings.memory_max_items)
                app.state.memory = memory
                workflow = Workflow(
                    repo,
                    settings,
                    checkpointer,
                    retriever,
                    store=store,
                    memory=memory,
                    sandbox=sandbox,
                    delegate=delegate,
                )
                app.state.workflow = workflow
                await jobs.recover_interrupted()
                try:
                    yield
                finally:
                    await jobs.shutdown()
        finally:
            await sandbox.shutdown()
            await retriever.close()
            await repo.close()

    app = FastAPI(title="SignalDesk", version=__version__, lifespan=lifespan)
    app.state.repo = repo
    app.state.jobs = jobs
    app.state.retriever = retriever
    app.state.sandbox = sandbox
    app.state.a2a_delegate = delegate
    app.include_router(create_router(repo, jobs, settings))
    app.include_router(create_memory_router(repo, None, settings))
    app.include_router(create_agui_router(repo, settings))
    app.include_router(create_sandbox_router(repo, None, settings))
    if settings.enable_a2a:
        a2a_app = create_a2a_app(repo, jobs, settings)
        app.mount("/a2a", a2a_app)
        app.include_router(create_agent_card_router(settings))
        app.include_router(create_a2a_delegation_router(repo, settings))

    @app.exception_handler(PublicTaskError)
    async def public_error(request, exc: PublicTaskError):
        return JSONResponse(
            status_code=503 if exc.retryable else 422,
            content={
                "detail": str(exc),
                "code": exc.code,
                "retryable": exc.retryable,
                "suggestion": exc.suggestion,
            },
        )

    static = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static, check_dir=False), name="static")

    @app.get("/", include_in_schema=False)
    async def dashboard():
        return FileResponse(static / "index.html")

    return app
