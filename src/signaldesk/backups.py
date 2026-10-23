"""Offline SQLite bundles, verification, and restoration to a new directory.

The three databases are copied while holding SignalDesk's cooperative file
locks. External SQLite clients must also be stopped. Publication is serialized
with the same advisory locking protocol; no existing destination is replaced.
"""

import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.engine import make_url

from signaldesk import __version__
from signaldesk.local_storage import StorageError, sqlite_file_path, sqlite_paths, storage_locks

DATABASE_FILES = {
    "business": "business.sqlite3",
    "checkpoints": "checkpoints.sqlite3",
    "memory": "memory.sqlite3",
}
MANIFEST_LIMIT = 65_536
COPY_CHUNK = 1_048_576


class _Deadline:
    def __init__(self, seconds: float):
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise StorageError("backup_invalid", "操作时限必须为有效的正数秒。")
        if not 0 < seconds <= 600 or not math.isfinite(seconds):
            raise StorageError("backup_invalid", "操作时限必须大于 0 且不超过 600 秒。")
        self.expires = time.monotonic() + seconds

    def check(self) -> None:
        if time.monotonic() >= self.expires:
            raise StorageError("backup_timeout", "数据库操作超过时限，请停止其他写入后重试。")

    def busy_timeout(self) -> float:
        self.check()
        return min(0.25, max(0.001, self.expires - time.monotonic()))


def _absolute(path: Path) -> Path:
    # Resolve the parent, preserving a final symlink for explicit rejection.
    path = Path(path).expanduser().absolute()
    return path.parent.resolve() / path.name


def _regular(path: Path, *, missing: str = "backup_invalid") -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise StorageError(missing, "所需数据库或备份文件不存在。") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise StorageError("backup_invalid", "数据库与备份文件必须为普通文件，不能使用链接。")
    return info


def _directory(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise StorageError("backup_invalid", "备份目录不存在。") from exc
    if not stat.S_ISDIR(info.st_mode):
        raise StorageError("backup_invalid", "备份必须是普通目录，不能使用符号链接。")


def _check_absent(path: Path) -> None:
    if os.path.lexists(path):
        raise StorageError("target_exists", "目标已存在，请选择一个尚不存在的新目录。")


def _fsync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def _publication(target: Path, deadline: _Deadline):
    """Stage under the same parent and serialize cooperating publishers."""
    _check_absent(target)
    if not target.parent.is_dir():
        raise StorageError("io_error", "目标父目录不存在或不可用。")
    lock_identity = target.parent / ("." + target.name + ".publish")
    with storage_locks({"publication": lock_identity}):
        _check_absent(target)
        stage = Path(tempfile.mkdtemp(prefix=".signaldesk-stage-", dir=target.parent))
        try:
            os.chmod(stage, 0o700)
            yield stage
            deadline.check()
            _fsync_directory(stage)
            _check_absent(target)
            os.rename(stage, target)
            try:
                _fsync_directory(target.parent)
            except OSError as exc:
                raise StorageError(
                    "publication_uncertain",
                    "目标目录已发布，但持久化确认失败；请先校验该目录，不要直接重试。",
                ) from exc
        finally:
            # Only this operation's unpublished staging tree is ever removed.
            if stage.exists():
                shutil.rmtree(stage)


@contextmanager
def _read_database(path: Path, deadline: _Deadline):
    _regular(path)
    connection = sqlite3.connect(
        path.as_uri() + "?mode=ro", uri=True, timeout=deadline.busy_timeout()
    )
    try:
        connection.execute("PRAGMA query_only=ON")
        yield connection
    finally:
        connection.close()


def _validate_database(path: Path, deadline: _Deadline) -> None:
    # Even mode=ro can create WAL shared-memory sidecars. Inspect an archive's
    # header before connecting; only standalone rollback-journal copies belong
    # in this format. Source backup connections intentionally still allow WAL.
    _regular(path)
    with path.open("rb") as stream:
        header = stream.read(20)
    deadline.check()
    if header[:16] != b"SQLite format 3\0" or header[18:20] != b"\x01\x01":
        raise StorageError("backup_invalid", "备份数据库格式无效，或仍使用 WAL 格式。")
    with _read_database(path, deadline) as connection:
        connection.set_progress_handler(lambda: int(time.monotonic() >= deadline.expires), 1000)
        try:
            cursor = connection.execute("PRAGMA integrity_check")
            first = cursor.fetchone()
            if first != ("ok",) or cursor.fetchone() is not None:
                raise StorageError("backup_invalid", "备份数据库完整性检查失败。")
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise StorageError("backup_invalid", "备份数据库存在外键约束错误。")
            deadline.check()
        except sqlite3.Error as exc:
            deadline.check()
            raise StorageError("backup_invalid", "备份数据库无法通过 SQLite 校验。") from exc
        finally:
            connection.set_progress_handler(None, 0)


def _hash_file(path: Path, deadline: _Deadline) -> tuple[int, str]:
    _regular(path)
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while block := source.read(COPY_CHUNK):
            deadline.check()
            size += len(block)
            digest.update(block)
    deadline.check()
    return size, digest.hexdigest()


def _backup_database(source: Path, target: Path, deadline: _Deadline) -> None:
    with _read_database(source, deadline) as connection:
        destination = sqlite3.connect(target, timeout=deadline.busy_timeout())
        try:
            os.chmod(target, 0o600)
            connection.backup(
                destination,
                pages=64,
                progress=lambda *_: deadline.check(),
                sleep=0.05,
            )
            deadline.check()
            # Backup preserves a WAL source's journal-mode header. Normalize the
            # copy while it is exclusively ours so the archive is self-contained.
            mode = destination.execute("PRAGMA journal_mode=DELETE").fetchone()
            if mode != ("delete",):
                raise StorageError("backup_invalid", "无法生成独立的 SQLite 备份文件。")
            destination.commit()
        finally:
            destination.close()
    _validate_database(target, deadline)
    _fsync_file(target)


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate manifest field")
        result[key] = value
    return result


def _invalid_constant(_value):
    raise ValueError("Nonfinite manifest value")


def _read_manifest(bundle: Path, deadline: _Deadline) -> dict:
    path = bundle / "manifest.json"
    if _regular(path).st_size > MANIFEST_LIMIT:
        raise StorageError("backup_invalid", "备份清单过大。")
    with path.open("rb") as stream:
        raw = stream.read(MANIFEST_LIMIT + 1)
    deadline.check()
    if len(raw) > MANIFEST_LIMIT:
        raise StorageError("backup_invalid", "备份清单过大。")
    try:
        result = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_json_object,
            parse_constant=_invalid_constant,
        )
        if not isinstance(result, dict) or set(result) != {
            "format_version",
            "app_version",
            "created_at",
            "databases",
        }:
            raise ValueError("Manifest fields")
        if type(result["format_version"]) is not int or result["format_version"] != 1:
            raise ValueError("Unknown bundle format")
        version = result["app_version"]
        if not isinstance(version, str) or not re.fullmatch(r"[0-9A-Za-z.+_-]{1,80}", version):
            raise ValueError("Invalid app version")
        created = result["created_at"]
        if not isinstance(created, str) or len(created) > 64:
            raise ValueError("Invalid timestamp")
        if datetime.fromisoformat(created).utcoffset() is None:
            raise ValueError("Timestamp requires a timezone")
        rows = result["databases"]
        if not isinstance(rows, list) or len(rows) != len(DATABASE_FILES):
            raise ValueError("Database roles")
        roles = set()
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"role", "file", "size_bytes", "sha256"}:
                raise ValueError("Database fields")
            role = row["role"]
            if not isinstance(role, str) or role not in DATABASE_FILES or role in roles:
                raise ValueError("Database role")
            roles.add(role)
            if row["file"] != DATABASE_FILES[role]:
                raise ValueError("Database filename")
            if type(row["size_bytes"]) is not int or row["size_bytes"] <= 0:
                raise ValueError("Database size")
            if not isinstance(row["sha256"], str) or not re.fullmatch(
                r"[0-9a-f]{64}", row["sha256"]
            ):
                raise ValueError("Database hash")
        return result
    except (UnicodeError, ValueError, TypeError, OverflowError, RecursionError) as exc:
        raise StorageError("backup_invalid", "备份清单格式无效或版本不受支持。") from exc


def _verify(bundle: Path, deadline: _Deadline, expected: dict | None = None) -> dict:
    deadline.check()
    _directory(bundle)
    required = {"manifest.json", *DATABASE_FILES.values()}

    def contents():
        names = set()
        for item in bundle.iterdir():
            if item.name not in required:
                raise StorageError(
                    "backup_invalid", "备份文件不完整或包含额外文件，不能含 WAL 等旁路文件。"
                )
            names.add(item.name)
        return names

    if contents() != required:
        raise StorageError(
            "backup_invalid", "备份文件不完整或包含额外文件，不能含 WAL 等旁路文件。"
        )
    manifest = _read_manifest(bundle, deadline)
    if expected is not None and manifest != expected:
        raise StorageError("backup_invalid", "备份在恢复过程中发生变化，请重新生成备份。")
    for row in manifest["databases"]:
        path = bundle / row["file"]
        if _regular(path).st_size != row["size_bytes"]:
            raise StorageError("backup_invalid", "备份数据库大小与清单不符。")
        if _hash_file(path, deadline) != (row["size_bytes"], row["sha256"]):
            raise StorageError("backup_invalid", "备份数据库校验和与清单不符。")
        _validate_database(path, deadline)
    # A WAL-mode database may create sidecars when read. Archives produced by
    # this module are DELETE-mode and must remain standalone after validation.
    if contents() != required:
        raise StorageError("backup_invalid", "备份数据库不是独立文件，不能依赖 WAL 旁路文件。")
    return manifest


def _write_manifest(stage: Path, manifest: dict) -> None:
    path = stage / "manifest.json"
    encoded = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    with path.open("xb") as stream:
        os.chmod(path, 0o600)
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _copy_file(source: Path, target: Path, deadline: _Deadline) -> None:
    _regular(source)
    with source.open("rb") as reader, target.open("xb") as writer:
        os.chmod(target, 0o600)
        while block := reader.read(COPY_CHUNK):
            deadline.check()
            writer.write(block)
        writer.flush()
        os.fsync(writer.fileno())
    deadline.check()


def backup_sqlite(settings, output: Path, *, timeout_seconds: float = 60) -> dict:
    """Copy all configured SQLite roles to one newly published offline bundle."""
    deadline = _Deadline(timeout_seconds)
    try:
        paths = sqlite_paths(settings, require_all=True)
        # Resolve/URL policy remains centralized in sqlite_paths. Preserve its
        # canonical identities, but reject a configured final symlink before
        # opening a backup source (applications may use the canonical alias).
        for url in (settings.database_url, settings.checkpoint_url, settings.memory_url):
            if Path(make_url(url).database).expanduser().is_symlink():
                raise StorageError("backup_invalid", "备份源数据库不能使用符号链接。")
        output = _absolute(output)
        _check_absent(output)
        for path in paths.values():
            _regular(path, missing="source_missing")
        with storage_locks(paths), _publication(output, deadline) as stage:
            rows = []
            for role, filename in DATABASE_FILES.items():
                deadline.check()
                _backup_database(paths[role], stage / filename, deadline)
                size, digest = _hash_file(stage / filename, deadline)
                rows.append({"role": role, "file": filename, "size_bytes": size, "sha256": digest})
            manifest = {
                "format_version": 1,
                "app_version": __version__,
                "created_at": datetime.now(UTC).isoformat(),
                "databases": rows,
            }
            _write_manifest(stage, manifest)
            _verify(stage, deadline, expected=manifest)
        return {"bundle": str(output), "manifest": manifest, "verified": True}
    except StorageError:
        raise
    except sqlite3.Error as exc:
        deadline.check()
        raise StorageError(
            "backup_invalid", "源数据库无法完成 SQLite 备份，请检查其完整性。"
        ) from exc
    except (OSError, ValueError, RuntimeError) as exc:
        raise StorageError("io_error", "无法完成数据库备份，请检查文件权限与可用空间。") from exc


def verify_backup(bundle: Path, *, timeout_seconds: float = 60) -> dict:
    """Validate a bundle without opening settings, migrating, or calling a model."""
    deadline = _Deadline(timeout_seconds)
    try:
        bundle = _absolute(bundle)
        manifest = _verify(bundle, deadline)
        return {"bundle": str(bundle), "manifest": manifest, "verified": True}
    except StorageError:
        raise
    except sqlite3.Error as exc:
        deadline.check()
        raise StorageError("backup_invalid", "备份数据库无法通过 SQLite 校验。") from exc
    except (OSError, ValueError, RuntimeError) as exc:
        raise StorageError("io_error", "无法读取备份，请检查目录与文件权限。") from exc


def _restore_urls(target_dir: Path) -> dict[str, str]:
    fields = {
        "database_url": ("business", "sqlite+aiosqlite:///"),
        "checkpoint_url": ("checkpoints", "sqlite:///"),
        "memory_url": ("memory", "sqlite:///"),
    }
    urls = {}
    try:
        for name, (role, prefix) in fields.items():
            path = target_dir / DATABASE_FILES[role]
            url = prefix + str(path)
            if sqlite_file_path(url) != path:
                raise StorageError("backup_invalid", "恢复目录不能准确表示为 SQLite 文件 URL。")
            urls[name] = url
    except StorageError as exc:
        raise StorageError(
            "backup_invalid", "恢复目录含 SQLite URL 不支持的字符，请选择不含问号的新路径。"
        ) from exc
    return urls


def restore_backup(bundle: Path, target_dir: Path, *, timeout_seconds: float = 60) -> dict:
    """Restore to a new directory only; never replace an existing database."""
    deadline = _Deadline(timeout_seconds)
    try:
        bundle, target_dir = _absolute(bundle), _absolute(target_dir)
        _check_absent(target_dir)
        if target_dir.is_relative_to(bundle):
            raise StorageError(
                "backup_invalid", "恢复目标不能位于备份目录内部，请选择独立的新目录。"
            )
        urls = _restore_urls(target_dir)
        manifest = _verify(bundle, deadline)
        with _publication(target_dir, deadline) as stage:
            for filename in DATABASE_FILES.values():
                _copy_file(bundle / filename, stage / filename, deadline)
            _write_manifest(stage, manifest)
            # Rehash and revalidate copied bytes: a successful source preflight
            # does not establish ownership of a directory modified externally.
            _verify(stage, deadline, expected=manifest)
        return {"target_dir": str(target_dir), "urls": urls, "verified": True}
    except StorageError:
        raise
    except sqlite3.Error as exc:
        deadline.check()
        raise StorageError("backup_invalid", "备份数据库无法通过 SQLite 校验。") from exc
    except (OSError, ValueError, RuntimeError) as exc:
        raise StorageError("io_error", "无法恢复数据库，请检查文件权限与可用空间。") from exc
