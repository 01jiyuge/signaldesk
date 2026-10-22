"""Persistent LangGraph Store adapters and project-scoped, explicit user memory.

SQLiteStore implements the official BaseStore operation protocol without an
embedding provider. TTL and semantic-query search are deliberately unsupported.
ProjectMemory never infers preferences from collected pages or model output.
"""

import asyncio
import json
import sqlite3
import threading
from collections.abc import Iterable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from operator import ge, gt, le, lt
from pathlib import Path
from typing import Any

from langgraph.store.base import (
    BaseStore,
    GetOp,
    InvalidNamespaceError,
    Item,
    ListNamespacesOp,
    Op,
    PutOp,
    Result,
    SearchItem,
    SearchOp,
)
from sqlalchemy.engine import make_url

from signaldesk.config import Settings


def _namespace(namespace: tuple[str, ...], *, write: bool = False) -> None:
    if not isinstance(namespace, tuple):
        raise InvalidNamespaceError("Namespace must be a tuple of string labels")
    if write and (not namespace or namespace[0] == "langgraph"):
        raise InvalidNamespaceError("A nonempty, non-reserved namespace is required")
    if any(not isinstance(label, str) or not label or "." in label for label in namespace):
        raise InvalidNamespaceError("Namespace labels must be nonempty strings without periods")


def _encoded(namespace: tuple[str, ...]) -> str:
    return json.dumps(namespace, ensure_ascii=False, separators=(",", ":"))


def _matches(value: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        if any(key.startswith("$") for key in expected):
            for operator, operand in expected.items():
                if operator == "$eq":
                    matched = value == operand
                elif operator == "$ne":
                    matched = value != operand
                elif operator in {"$in", "$nin"}:
                    if not isinstance(operand, list):
                        raise ValueError(f"{operator} requires a list")
                    matched = value in operand
                    if operator == "$nin":
                        matched = not matched
                elif operator in {"$gt", "$gte", "$lt", "$lte"}:
                    try:
                        matched = {
                            "$gt": gt,
                            "$gte": ge,
                            "$lt": lt,
                            "$lte": le,
                        }[operator](value, operand)
                    except TypeError:
                        matched = False
                else:
                    raise NotImplementedError(
                        f"SQLiteStore does not support filter operator {operator}"
                    )
                if not matched:
                    return False
            return True
        return isinstance(value, dict) and all(
            _matches(value.get(key), item) for key, item in expected.items()
        )
    return value == expected


def _validate_filter(value: Any) -> None:
    if not isinstance(value, dict):
        return
    supported = {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin"}
    for key, operand in value.items():
        if key.startswith("$") and key not in supported:
            raise NotImplementedError(f"SQLiteStore does not support filter operator {key}")
        if key in {"$in", "$nin"} and not isinstance(operand, list):
            raise ValueError(f"{key} requires a list")
        if not key.startswith("$"):
            _validate_filter(operand)


class SQLiteStore(BaseStore):
    """A file-backed BaseStore with atomic ordered batches and JSON filtering.

    Supported search filters: nested object equality and $eq/$ne/$gt/$gte/$lt/
    $lte/$in/$nin. Search results are ordered by most recently updated first.
    There is no semantic index: query, TTL, and explicit field indexes raise
    NotImplementedError rather than silently claiming those capabilities.
    """

    supports_ttl = False

    def __init__(self, path: str | Path):
        if str(path) == ":memory:":
            raise ValueError("SQLiteStore requires a file-backed database")
        path = Path(path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=10000")
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS store_items ("
            "namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, "
            "created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "PRIMARY KEY(namespace, key))"
        )
        self._connection.commit()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        operations = list(ops)
        with self._lock:
            if self._closed:
                raise RuntimeError("SQLiteStore is closed")
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                results = [self._execute(operation) for operation in operations]
                self._connection.commit()
                return results
            except BaseException:
                self._connection.rollback()
                raise

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        # The worker retains the connection lock until commit/rollback. close()
        # therefore joins any cancelled async operation before closing SQLite.
        return await asyncio.to_thread(self.batch, list(ops))

    @staticmethod
    def _item(row: sqlite3.Row, *, search: bool = False) -> Item:
        cls = SearchItem if search else Item
        return cls(
            namespace=tuple(json.loads(row["namespace"])),
            key=row["key"],
            value=json.loads(row["value"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _execute(self, op: Op) -> Result:
        if isinstance(op, GetOp):
            _namespace(op.namespace)
            row = self._connection.execute(
                "SELECT * FROM store_items WHERE namespace=? AND key=?",
                (_encoded(op.namespace), op.key),
            ).fetchone()
            return self._item(row) if row else None
        if isinstance(op, PutOp):
            _namespace(op.namespace, write=op.value is not None)
            if op.ttl is not None:
                raise NotImplementedError("SQLiteStore does not support TTL")
            if op.index not in (None, False):
                raise NotImplementedError("SQLiteStore does not support semantic field indexes")
            if op.value is None:
                self._connection.execute(
                    "DELETE FROM store_items WHERE namespace=? AND key=?",
                    (_encoded(op.namespace), op.key),
                )
                return None
            if not isinstance(op.value, Mapping) or any(
                not isinstance(key, str) for key in op.value
            ):
                raise ValueError("Store values must be JSON objects with string keys")
            encoded = json.dumps(dict(op.value), ensure_ascii=False, allow_nan=False)
            now = datetime.now(UTC).isoformat()
            self._connection.execute(
                "INSERT INTO store_items(namespace,key,value,created_at,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (_encoded(op.namespace), op.key, encoded, now, now),
            )
            return None
        if isinstance(op, SearchOp):
            _namespace(op.namespace_prefix)
            if op.query is not None:
                raise NotImplementedError("SQLiteStore has no semantic query search; omit query")
            if op.limit < 0 or op.offset < 0:
                raise ValueError("Search limit and offset must be nonnegative")
            _validate_filter(op.filter)
            query = "SELECT * FROM store_items"
            params = []
            if op.namespace_prefix:
                exact = _encoded(op.namespace_prefix)
                prefix = exact[:-1] + ","
                query += " WHERE namespace=? OR substr(namespace,1,?)=?"
                params = [exact, len(prefix), prefix]
            rows = self._connection.execute(
                query + " ORDER BY updated_at DESC, namespace ASC, key ASC", params
            ).fetchall()
            matched = [
                row
                for row in rows
                if all(
                    _matches(json.loads(row["value"]).get(key), value)
                    for key, value in (op.filter or {}).items()
                )
            ]
            return [
                self._item(row, search=True) for row in matched[op.offset : op.offset + op.limit]
            ]
        if isinstance(op, ListNamespacesOp):
            if op.limit < 0 or op.offset < 0 or (op.max_depth is not None and op.max_depth < 1):
                raise ValueError("Invalid namespace pagination or max_depth")
            conditions = op.match_conditions or ()
            for condition in conditions:
                _namespace(condition.path)
                if condition.match_type not in {"prefix", "suffix"}:
                    raise NotImplementedError("Only namespace prefix/suffix matching is supported")
            rows = self._connection.execute("SELECT DISTINCT namespace FROM store_items").fetchall()
            namespaces = []
            for row in rows:
                namespace = tuple(json.loads(row[0]))
                matched = True
                for condition in conditions:
                    path = condition.path
                    candidate = (
                        namespace[: len(path)]
                        if condition.match_type == "prefix"
                        else (namespace[-len(path) :] if path else ())
                    )
                    if len(candidate) != len(path) or not all(
                        pattern == "*" or pattern == label
                        for pattern, label in zip(path, candidate, strict=True)
                    ):
                        matched = False
                        break
                if matched:
                    namespaces.append(namespace[: op.max_depth] if op.max_depth else namespace)
            return sorted(set(namespaces))[op.offset : op.offset + op.limit]
        raise NotImplementedError(f"Unsupported Store operation: {type(op).__name__}")


@asynccontextmanager
async def open_memory_store(settings: Settings):
    url = make_url(settings.memory_url)
    if url.get_backend_name() in {"postgres", "postgresql"}:
        from langgraph.store.postgres import AsyncPostgresStore

        async with AsyncPostgresStore.from_conn_string(settings.memory_url) as store:
            await store.setup()
            yield store
    elif url.get_backend_name() == "sqlite" and url.database:
        if url.query:
            raise ValueError("SQLite memory URL query parameters are unsupported")
        store = await asyncio.to_thread(SQLiteStore, url.database)
        try:
            yield store
        finally:
            await asyncio.to_thread(store.close)
    else:
        raise ValueError("MEMORY_URL must use sqlite:/// or postgresql://")


class MemoryValidationError(ValueError):
    pass


class MemoryLimitError(ValueError):
    pass


class ProjectMemory:
    """Explicit preferences and idempotent run summaries in fixed namespaces."""

    def __init__(self, store: BaseStore, max_items: int = 50):
        if not 1 <= max_items <= 1000:
            raise ValueError("Memory item limit must be between 1 and 1000")
        self.store = store
        self.max_items = max_items
        self._locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def _namespace(project_id: str, category: str) -> tuple[str, ...]:
        if (
            not isinstance(project_id, str)
            or not project_id
            or len(project_id) > 128
            or "." in project_id
        ):
            raise MemoryValidationError("无效的项目 ID。")
        return ("projects", project_id, category)

    @staticmethod
    def _key(key: str) -> str:
        if (
            not isinstance(key, str)
            or not 1 <= len(key) <= 80
            or key.strip() != key
            or any(ord(char) < 32 or char in "/\\" for char in key)
        ):
            raise MemoryValidationError(
                "记忆 key 须为 1–80 字符，且不可包含控制字符、路径分隔符或首尾空格。"
            )
        return key

    @staticmethod
    def _value(value: dict) -> dict:
        if not isinstance(value, dict):
            raise MemoryValidationError("记忆内容必须是 JSON 对象。")
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise MemoryValidationError("记忆内容必须可序列化为标准 JSON。") from exc
        if len(encoded.encode("utf-8")) > 8000:
            raise MemoryValidationError("单条记忆内容不可超过 8000 字节。")
        return json.loads(encoded)

    @staticmethod
    def _record(item: Item) -> dict:
        return {
            "key": item.key,
            "value": item.value["content"],
            "source": item.value["source"],
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat(),
        }

    async def list_preferences(self, project_id: str) -> list[dict]:
        namespace = self._namespace(project_id, "preferences")
        items = await self.store.asearch(
            namespace,
            filter={"kind": "preference", "source": "user"},
            limit=self.max_items,
        )
        return [self._record(item) for item in items if item.namespace == namespace]

    async def get_preference(self, project_id: str, key: str) -> dict | None:
        item = await self.store.aget(self._namespace(project_id, "preferences"), self._key(key))
        if (
            item is None
            or item.value.get("kind") != "preference"
            or item.value.get("source") != "user"
        ):
            return None
        return self._record(item)

    async def set_preference(self, project_id: str, key: str, value: dict) -> dict:
        namespace = self._namespace(project_id, "preferences")
        key, value = self._key(key), self._value(value)
        async with self._locks.setdefault(project_id, asyncio.Lock()):
            existing = await self.store.aget(namespace, key)
            if existing is None:
                items = await self.store.asearch(namespace, limit=self.max_items + 1)
                if len(items) >= self.max_items:
                    raise MemoryLimitError(
                        f"每个项目最多保存 {self.max_items} 条明确偏好，请先删除旧条目。"
                    )
            await self.store.aput(
                namespace, key, {"kind": "preference", "source": "user", "content": value}
            )
            return self._record(await self.store.aget(namespace, key))

    async def delete_preference(self, project_id: str, key: str) -> bool:
        namespace, key = self._namespace(project_id, "preferences"), self._key(key)
        async with self._locks.setdefault(project_id, asyncio.Lock()):
            existing = await self.store.aget(namespace, key)
            if existing is None:
                return False
            await self.store.adelete(namespace, key)
            return True

    async def remember_run(self, project_id: str, run_id: str, summary: dict) -> dict:
        namespace = self._namespace(project_id, "runs")
        key, summary = self._key(run_id), self._value(summary)
        async with self._locks.setdefault(project_id, asyncio.Lock()):
            existing = await self.store.aget(namespace, key)
            if existing is not None:
                return self._record(existing)
            await self.store.aput(
                namespace,
                key,
                {"kind": "run_summary", "source": "verified_run", "content": summary},
            )
            items = await self.store.asearch(namespace, limit=self.max_items + 1)
            items = sorted(items, key=lambda item: item.updated_at, reverse=True)
            for item in items[self.max_items :]:
                await self.store.adelete(namespace, item.key)
            return self._record(await self.store.aget(namespace, key))

    async def load_context(self, project_id: str, limit: int = 10) -> dict:
        if not 1 <= limit <= 1000:
            raise MemoryValidationError("上下文条数须介于 1 和 1000 之间。")
        limit = min(limit, self.max_items)
        namespace = self._namespace(project_id, "runs")
        items = await self.store.asearch(
            namespace,
            filter={"kind": "run_summary", "source": "verified_run"},
            limit=self.max_items,
        )
        items = sorted(
            (item for item in items if item.namespace == namespace),
            key=lambda item: item.updated_at,
            reverse=True,
        )[:limit]
        return {
            "preferences": await self.list_preferences(project_id),
            "recent_runs": [self._record(item) for item in items],
        }
