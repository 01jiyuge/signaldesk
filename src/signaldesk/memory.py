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
from signaldesk.local_storage import sqlite_file_path


async def _finish_owned(operation):
    """Keep resource/mutation ownership until cooperative work really stops."""
    task = asyncio.ensure_future(operation)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    if cancelled:
        # Retrieve a failed result without allowing a cleanup error to replace
        # the caller's cancellation or become an unhandled task exception.
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


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
        try:
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
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

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
            if op.limit == 0:
                return []
            query = "SELECT * FROM store_items"
            params = []
            if op.namespace_prefix:
                exact = _encoded(op.namespace_prefix)
                prefix = exact[:-1] + ","
                query += " WHERE namespace=? OR substr(namespace,1,?)=?"
                params = [exact, len(prefix), prefix]
            cursor = self._connection.execute(
                query + " ORDER BY updated_at DESC, namespace ASC, key ASC", params
            )
            matched, skipped = [], 0
            try:
                # Filters include Python-only nested comparisons. Stream bounded
                # batches instead of materializing every namespace record before
                # applying offset/limit, including during overflow repair.
                while rows := cursor.fetchmany(64):
                    for row in rows:
                        value = json.loads(row["value"])
                        if not all(
                            _matches(value.get(key), expected)
                            for key, expected in (op.filter or {}).items()
                        ):
                            continue
                        if skipped < op.offset:
                            skipped += 1
                            continue
                        matched.append(self._item(row, search=True))
                        if len(matched) == op.limit:
                            return matched
                return matched
            finally:
                cursor.close()
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
    database = sqlite_file_path(settings.memory_url)
    url = make_url(settings.memory_url)
    if url.get_backend_name() in {"postgres", "postgresql"}:
        from langgraph.store.postgres import AsyncPostgresStore

        async with AsyncPostgresStore.from_conn_string(settings.memory_url) as store:
            await store.setup()
            yield store
    elif url.get_backend_name() == "sqlite" and url.database:
        opening = asyncio.create_task(asyncio.to_thread(SQLiteStore, database or ":memory:"))
        try:
            store = await asyncio.shield(opening)
        except asyncio.CancelledError:
            # __aenter__ has not yielded yet, so an outer ExitStack cannot own
            # this connection. The constructor thread cannot be cancelled.
            async def close_opening():
                try:
                    constructed = await opening
                except Exception:
                    return
                await asyncio.to_thread(constructed.close)

            await _finish_owned(close_opening())
            raise
        try:
            yield store
        finally:
            await _finish_owned(asyncio.to_thread(store.close))
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
        try:
            key.encode("utf-8")
        except UnicodeError as exc:
            raise MemoryValidationError("记忆 key 必须是有效 UTF-8 文本。") from exc
        return key

    @staticmethod
    def _value(value: dict) -> dict:
        if not isinstance(value, dict):
            raise MemoryValidationError("记忆内容必须是 JSON 对象。")
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
            size = len(encoded.encode("utf-8"))
            decoded = json.loads(encoded)
        except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
            raise MemoryValidationError("记忆内容必须可序列化为标准 JSON。") from exc
        if size > 8000:
            raise MemoryValidationError("单条记忆内容不可超过 8000 字节。")
        return decoded

    @staticmethod
    def _record(item: Item) -> dict:
        return {
            "key": item.key,
            "value": item.value["content"],
            "source": item.value["source"],
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat(),
        }

    @staticmethod
    def _owns(item: Item, namespace: tuple[str, ...], kind: str, source: str) -> bool:
        return (
            item.namespace == namespace
            and item.value.get("kind") == kind
            and item.value.get("source") == source
        )

    async def _scan_exact(self, namespace: tuple[str, ...], kind: str, source: str) -> list[Item]:
        # BaseStore search is a prefix operation. Fetch a complete bounded
        # window before filtering: descendants must not consume our quota, and
        # an overflow/reduced limit must still return the newest owned records.
        # Official PG Store does not break updated_at ties by key, so OFFSET
        # pages can overlap. Grow the first window instead of trusting tie order.
        base = self.max_items + 64
        maximum = 8 * base + 1
        window = base
        while True:
            page = await self.store.asearch(
                namespace,
                filter={"kind": kind, "source": source},
                limit=window,
                offset=0,
            )
            identities = {(item.namespace, item.key) for item in page}
            if len(identities) != len(page) or len(page) > window:
                raise MemoryLimitError("记忆命名空间读取未取得稳定进展，请检查 Store 状态后重试。")
            if len(page) < window:
                return sorted(
                    (item for item in page if self._owns(item, namespace, kind, source)),
                    key=lambda item: (item.updated_at, item.key),
                    reverse=True,
                )
            if window == maximum:
                raise MemoryLimitError(
                    "记忆命名空间包含过多外来记录或历史溢出，无法完整读取；请检查 Store 命名空间。"
                )
            window = min(window * 2 + 1, maximum)

    async def list_preferences(self, project_id: str) -> list[dict]:
        namespace = self._namespace(project_id, "preferences")
        items = await self._scan_exact(namespace, "preference", "user")
        return [self._record(item) for item in items[: self.max_items]]

    async def get_preference(self, project_id: str, key: str) -> dict | None:
        namespace = self._namespace(project_id, "preferences")
        item = await self.store.aget(namespace, self._key(key))
        if item is None or not self._owns(item, namespace, "preference", "user"):
            return None
        return self._record(item)

    async def set_preference(self, project_id: str, key: str, value: dict) -> dict:
        namespace = self._namespace(project_id, "preferences")
        key, value = self._key(key), self._value(value)
        async with self._locks.setdefault(project_id, asyncio.Lock()):
            existing = await self.store.aget(namespace, key)
            if existing is not None and not self._owns(existing, namespace, "preference", "user"):
                raise MemoryValidationError("该记忆 key 已由其他类别记录使用，请改用新的 key。")
            if existing is None:
                items = await self._scan_exact(namespace, "preference", "user")
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
            if existing is None or not self._owns(existing, namespace, "preference", "user"):
                return False
            await self.store.adelete(namespace, key)
            return True

    async def remember_run(self, project_id: str, run_id: str, summary: dict) -> dict:
        namespace = self._namespace(project_id, "runs")
        key, summary = self._key(run_id), self._value(summary)
        async with self._locks.setdefault(project_id, asyncio.Lock()):

            async def remember_and_prune():
                existing = await self.store.aget(namespace, key)
                if existing is None:
                    await self.store.aput(
                        namespace,
                        key,
                        {"kind": "run_summary", "source": "verified_run", "content": summary},
                    )
                    existing = await self.store.aget(namespace, key)
                # Idempotent retries also repair an older overflow or a reduced
                # configuration; a successful insert alone is not completion.
                await self._prune_runs(namespace)
                return self._record(existing)

            return await _finish_owned(remember_and_prune())

    async def _prune_runs(self, namespace: tuple[str, ...]) -> None:
        window, offset, retained = self.max_items + 64, 0, {}
        foreign_pages, previous_page, previous_expired = 0, None, None
        while True:
            page = await self.store.asearch(
                namespace,
                filter={"kind": "run_summary", "source": "verified_run"},
                limit=window,
                offset=offset,
            )
            if not page:
                return
            # Store search is a namespace-prefix operation. Only records in
            # this exact run namespace are ours to evict, never preferences or
            # a descendant namespace populated by another Store consumer.
            candidates = dict(retained)
            candidates.update(
                {(item.namespace, item.key): item for item in page if item.namespace == namespace}
            )
            ranked = sorted(
                candidates.values(), key=lambda item: (item.updated_at, item.key), reverse=True
            )
            retained = {(item.namespace, item.key): item for item in ranked[: self.max_items]}
            expired = ranked[self.max_items :]
            if expired:
                fingerprint = frozenset(
                    (item.namespace, item.key, item.updated_at) for item in expired
                )
                if fingerprint == previous_expired:
                    raise MemoryLimitError("运行摘要清理未取得进展，请检查 Store 状态后重试。")
                await self.store.abatch([PutOp(namespace, item.key, None) for item in expired])
                # The official PostgreSQL Store does not order timestamp ties by
                # key. Restart after every deletion instead of assuming OFFSET
                # preserves the order of tied rows. At most max_items are kept,
                # so an exact-namespace window larger than that must progress.
                offset, foreign_pages, previous_page = 0, 0, None
                previous_expired = fingerprint
                continue
            if len(page) < window:
                return
            # Prefix searches can be dominated by another consumer's descendant
            # namespaces. Read through a finite number of foreign-only windows,
            # never evicting those records or spinning on a non-progressing store.
            identity = frozenset((item.namespace, item.key) for item in page)
            foreign_pages += 1
            if identity == previous_page or foreign_pages >= 8:
                raise MemoryLimitError(
                    "运行摘要命名空间包含过多外来记录，无法安全清理；请检查 Store 命名空间。"
                )
            previous_page = identity
            offset += len(page)

    async def load_context(self, project_id: str, limit: int = 10) -> dict:
        if not 1 <= limit <= 1000:
            raise MemoryValidationError("上下文条数须介于 1 和 1000 之间。")
        limit = min(limit, self.max_items)
        namespace = self._namespace(project_id, "runs")
        items = (await self._scan_exact(namespace, "run_summary", "verified_run"))[:limit]
        return {
            "preferences": await self.list_preferences(project_id),
            "recent_runs": [self._record(item) for item in items],
        }
