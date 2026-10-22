"""Durable graph checkpoints for local SQLite and PostgreSQL deployments."""

from contextlib import asynccontextmanager
from pathlib import Path

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver


@asynccontextmanager
async def open_checkpointer(url: str):
    if url.startswith("sqlite:///"):
        database = url.removeprefix("sqlite:///")
        if database != ":memory:":
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        async with AsyncSqliteSaver.from_conn_string(database) as saver:
            yield saver
    elif url.startswith(("postgres://", "postgresql://")):
        async with AsyncPostgresSaver.from_conn_string(url) as saver:
            await saver.setup()
            yield saver
    else:
        raise ValueError("CHECKPOINT_URL 必须使用 sqlite:/// 或 postgresql://。")
