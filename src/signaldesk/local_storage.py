"""Canonical local SQLite paths and cooperative process ownership.

Locks protect SignalDesk applications and maintenance commands that use this
module. They are advisory: external SQLite clients must also be stopped before
an offline backup. Persistent sidecars must never be unlinked after unlocking.
"""

import errno
import os
import stat
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class StorageError(ValueError):
    """A safe operational error; neither URLs nor credentials enter messages."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def _canonical_path(path: str | Path) -> Path:
    try:
        canonical = Path(path).expanduser().resolve()
        if canonical.exists():
            info = canonical.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink > 1:
                raise StorageError(
                    "storage_invalid_path", "SQLite 数据库必须是普通文件，且不能使用硬链接。"
                )
        return canonical
    except StorageError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise StorageError("storage_invalid_path", "无法解析本地 SQLite 文件路径。") from exc


def sqlite_file_path(url: str) -> Path | None:
    """Resolve ordinary SQLite URLs exactly as all three local adapters do.

    Non-SQLite URLs and SQLite in-memory databases have no file to lock. SQLite
    URI/query/host variants are rejected instead of guessing their identities.
    """
    try:
        parsed = make_url(url)
    except (ArgumentError, TypeError, ValueError) as exc:
        raise StorageError("storage_invalid_config", "数据库 URL 配置无效。") from exc
    if parsed.get_backend_name() != "sqlite":
        return None
    if (
        "\x00" in url
        or "?" in url
        or parsed.query
        or parsed.host
        or parsed.username
        or parsed.password
        or parsed.port is not None
    ):
        raise StorageError(
            "storage_invalid_config", "SQLite 仅支持不含主机、URI 和查询参数的本地文件 URL。"
        )
    database = parsed.database
    if database in (None, "", ":memory:"):
        return None
    if database.lower().startswith("file:"):
        raise StorageError("storage_invalid_config", "SQLite 文件 URI 不受支持。")
    path = _canonical_path(database)
    if "?" in str(path):
        raise StorageError("storage_invalid_path", "SQLite 文件路径不能包含问号。")
    return path


def normalized_storage_settings(settings):
    """Freeze file identities before constructing resources or changing cwd.

    The copy also prevents a caller mutating its Settings instance from moving
    an adapter or its ownership lock after create_app has already returned.
    """
    updates = {}
    for field in ("database_url", "checkpoint_url", "memory_url"):
        url = getattr(settings, field)
        path = sqlite_file_path(url)
        if path is not None:
            updates[field] = (
                make_url(url).set(database=str(path)).render_as_string(hide_password=False)
            )
    return settings.model_copy(deep=True, update=updates)


def sqlite_paths(settings, require_all: bool = False) -> dict[str, Path]:
    """Return file-backed local roles; maintenance may require all three."""
    result = {}
    for role, url in (
        ("business", settings.database_url),
        ("checkpoints", settings.checkpoint_url),
        ("memory", settings.memory_url),
    ):
        path = sqlite_file_path(url)
        if path is None:
            if require_all:
                raise StorageError(
                    "storage_sqlite_required",
                    "此操作要求业务库、检查点和记忆库均为本地 SQLite 文件。",
                )
        else:
            result[role] = path
    return result


@contextmanager
def storage_locks(paths: Mapping[str, Path]) -> Iterator[None]:
    """Own every distinct local database until all dependent resources close.

    Sorted, nonblocking acquisition avoids deadlocks and fails before opening
    databases. An exit or a failed partial acquisition releases every owned fd.
    """
    if not paths:
        yield
        return
    if sys.platform not in {"linux", "darwin"}:
        raise StorageError("storage_unsupported", "本地 SQLite 文件锁目前仅支持 Linux 和 macOS。")

    import fcntl

    descriptors = []
    try:
        canonical = sorted({_canonical_path(path) for path in paths.values()}, key=str)
        for path in canonical:
            lock_path = path.with_name(path.name + ".signaldesk.lock")
            try:
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                if lock_path.is_symlink():
                    raise StorageError("storage_invalid_path", "数据库锁文件不能是符号链接。")
                if lock_path.exists() and not lock_path.is_file():
                    raise StorageError("storage_invalid_path", "数据库锁必须使用普通文件。")
                descriptor = os.open(
                    lock_path,
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                    0o600,
                )
                descriptors.append(descriptor)
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise StorageError("storage_invalid_path", "数据库锁必须是无硬链接的普通文件。")
                os.fchmod(descriptor, 0o600)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    if exc.errno in {errno.EACCES, errno.EAGAIN}:
                        raise StorageError(
                            "storage_busy", "数据库正被另一个服务或维护命令使用，请先停止该进程。"
                        ) from exc
                    raise
            except OSError as exc:
                raise StorageError("storage_io", "无法打开或锁定本地数据库锁文件。") from exc
        yield
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
