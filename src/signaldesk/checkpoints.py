"""Durable graph checkpoints for local SQLite and PostgreSQL deployments."""

from contextlib import asynccontextmanager

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from sqlalchemy.engine import make_url

from signaldesk.local_storage import sqlite_file_path


@asynccontextmanager
async def open_checkpointer(url: str):
    database = sqlite_file_path(url)
    parsed = make_url(url)
    if parsed.get_backend_name() == "sqlite":
        if database is not None:
            database.parent.mkdir(parents=True, exist_ok=True)
        database = str(database) if database is not None else ":memory:"
        async with AsyncSqliteSaver.from_conn_string(database) as saver:
            yield saver
    elif url.startswith(("postgres://", "postgresql://")):
        async with AsyncPostgresSaver.from_conn_string(url) as saver:
            await saver.setup()
            yield saver
    else:
        raise ValueError("CHECKPOINT_URL 必须使用 sqlite:/// 或 postgresql://。")
