"""Local SQLite identities, process locks and adapter path agreement."""

import json
import os
import select
import signal
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from signaldesk.app import create_app
from signaldesk.checkpoints import open_checkpointer
from signaldesk.config import Settings
from signaldesk.local_storage import (
    StorageError,
    normalized_storage_settings,
    sqlite_file_path,
    sqlite_paths,
    storage_locks,
)
from signaldesk.memory import open_memory_store
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"}, reason="Local ownership supports Linux and macOS"
)

PROBE = """
import json, sys
from pathlib import Path
from signaldesk.local_storage import StorageError, storage_locks
try:
    with storage_locks({key: Path(value) for key, value in json.loads(sys.argv[1]).items()}):
        print('locked', flush=True)
        if len(sys.argv) > 2:
            sys.stdin.readline()
except StorageError as exc:
    print(exc.code, flush=True)
    sys.exit(2)
"""


def probe(paths):
    return subprocess.run(
        [
            sys.executable,
            "-c",
            PROBE,
            json.dumps({key: str(value) for key, value in paths.items()}),
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def local_settings(tmp_path):
    return Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/business.db",
        checkpoint_url=f"sqlite:///{tmp_path}/checkpoints.db",
        memory_url=f"sqlite:///{tmp_path}/memory.db",
    )


def test_common_parser_canonicalizes_relative_absolute_tilde_and_symlink(tmp_path, monkeypatch):
    target = tmp_path / "database #@% 中文.db"
    target.touch()
    alias = tmp_path / "alias.db"
    alias.symlink_to(target)
    monkeypatch.chdir(tmp_path)
    assert sqlite_file_path("sqlite+aiosqlite:///./database #@% 中文.db") == target
    assert sqlite_file_path(f"sqlite:///{target}") == target
    assert sqlite_file_path(f"sqlite:///{alias}") == target
    assert sqlite_file_path("sqlite:///~/signaldesk-path-fixture.db") == (
        Path("~/signaldesk-path-fixture.db").expanduser().resolve()
    )


@pytest.mark.parametrize(
    "url",
    [
        "sqlite://user:fixture-password@host/database",
        "sqlite:///database?mode=ro",
        "sqlite:///database?ignored",
        "sqlite:///database?",
        "sqlite:///file:memory?mode=memory&uri=true",
        "sqlite:///FILE:database",
        "sqlite:///path\x00suffix",
        "not-a-url-fixture-password",
    ],
)
def test_unsupported_sqlite_spellings_fail_with_safe_errors(url):
    with pytest.raises(StorageError) as raised:
        sqlite_file_path(url)
    assert raised.value.code == "storage_invalid_config"
    assert "fixture-password" not in raised.value.message
    assert str(raised.value) == raised.value.message


def test_roles_skip_nonfiles_and_can_require_all_three(tmp_path):
    settings = local_settings(tmp_path)
    expected = {
        "business": tmp_path / "business.db",
        "checkpoints": tmp_path / "checkpoints.db",
        "memory": tmp_path / "memory.db",
    }
    assert sqlite_paths(settings, require_all=True) == expected
    mixed = settings.model_copy(
        update={"checkpoint_url": "sqlite:///:memory:", "memory_url": "postgresql://fixture/db"}
    )
    assert sqlite_paths(mixed) == {"business": expected["business"]}
    with pytest.raises(StorageError) as raised:
        sqlite_paths(mixed, require_all=True)
    assert raised.value.code == "storage_sqlite_required"
    assert sqlite_file_path("sqlite://") is None
    assert sqlite_file_path("sqlite+aiosqlite:///:memory:") is None
    assert sqlite_file_path("postgresql://fixture:fixture-password@host/db") is None


def test_normalization_preserves_driver_and_copies_other_config_without_changing_input(tmp_path):
    original = local_settings(tmp_path)
    original.checkpoint_url = "sqlite:///:memory:"
    original.memory_url = "postgresql://fixture:fixture-password@host/db"
    normalized = normalized_storage_settings(original)
    assert normalized is not original
    assert normalized.database_url.startswith("sqlite+aiosqlite:///")
    assert sqlite_file_path(normalized.database_url) == tmp_path / "business.db"
    assert normalized.checkpoint_url == original.checkpoint_url
    assert normalized.memory_url == original.memory_url
    original.database_url = "sqlite+aiosqlite:///:memory:"
    original.mcp_urls.append("https://fixture.invalid")
    assert sqlite_file_path(normalized.database_url) == tmp_path / "business.db"
    assert normalized.mcp_urls == []


def test_alias_to_question_mark_filename_cannot_create_an_unparseable_frozen_url(tmp_path):
    target = tmp_path / "database?ignored"
    target.touch()
    alias = tmp_path / "alias.db"
    alias.symlink_to(target)
    with pytest.raises(StorageError) as raised:
        sqlite_file_path(f"sqlite:///{alias}")
    assert raised.value.code == "storage_invalid_path"


def test_database_hardlinks_and_special_paths_are_rejected(tmp_path):
    original, hardlink = tmp_path / "original.db", tmp_path / "hardlink.db"
    original.touch()
    os.link(original, hardlink)
    for path in (original, hardlink, tmp_path):
        with pytest.raises(StorageError) as raised:
            sqlite_file_path(f"sqlite:///{path}")
        assert raised.value.code == "storage_invalid_path"


def test_duplicate_roles_acquire_one_persistent_private_lock(tmp_path):
    path = tmp_path / "database.db"
    paths = {"business": path, "checkpoints": path, "memory": path}
    sidecar = path.with_name(path.name + ".signaldesk.lock")
    with storage_locks(paths):
        info = sidecar.stat()
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert probe(paths).stdout.strip() == "storage_busy"
    assert sidecar.exists()
    with storage_locks(paths):
        assert sidecar.stat().st_ino == info.st_ino
    assert probe(paths).stdout.strip() == "locked"


def test_processes_conflict_on_any_shared_database_but_not_disjoint_sets(tmp_path):
    paths = sqlite_paths(local_settings(tmp_path))
    with storage_locks(paths):
        same = probe(paths)
        shared = probe({"business": tmp_path / "other.db", "memory": paths["memory"]})
        separate = probe({"business": tmp_path / "separate.db"})
        assert same.returncode == shared.returncode == 2
        assert same.stdout.strip() == shared.stdout.strip() == "storage_busy"
        assert separate.returncode == 0 and separate.stdout.strip() == "locked"


def test_relative_absolute_and_symlink_paths_cannot_bypass_process_lock(tmp_path, monkeypatch):
    database = tmp_path / "database.db"
    database.touch()
    alias = tmp_path / "alias.db"
    alias.symlink_to(database)
    monkeypatch.chdir(tmp_path)
    with storage_locks({"business": database}):
        for path in (Path("database.db"), database, alias):
            result = probe({"business": path})
            assert result.returncode == 2 and result.stdout.strip() == "storage_busy"


def test_partial_acquisition_and_body_failures_release_previous_locks(tmp_path):
    first, blocked = tmp_path / "a.db", tmp_path / "z.db"
    with storage_locks({"business": blocked}):
        with pytest.raises(StorageError) as raised:
            with storage_locks({"business": first, "memory": blocked}):
                pytest.fail("Shared database must prevent entering the operation")
        assert raised.value.code == "storage_busy"
        with storage_locks({"business": first}):
            pass
    with pytest.raises(RuntimeError, match="controlled"):
        with storage_locks({"business": first}):
            raise RuntimeError("controlled operation failure")
    with storage_locks({"business": first}):
        pass


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory", "hardlink"])
def test_lock_sidecars_reject_links_and_special_files_without_modifying_target(tmp_path, kind):
    database = tmp_path / "database.db"
    sidecar = tmp_path / "database.db.signaldesk.lock"
    target = tmp_path / "fixture.txt"
    target.write_text("unchanged", encoding="utf-8")
    if kind == "symlink":
        sidecar.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(sidecar)
    elif kind == "directory":
        sidecar.mkdir()
    else:
        os.link(target, sidecar)
    with pytest.raises(StorageError) as raised:
        with storage_locks({"business": database}):
            pytest.fail("Unsafe sidecar must fail before operation starts")
    assert raised.value.code == "storage_invalid_path"
    assert target.read_text(encoding="utf-8") == "unchanged"


def test_sigkill_releases_lock_without_deleting_sidecar(tmp_path):
    path = tmp_path / "database.db"
    process = subprocess.Popen(
        [sys.executable, "-c", PROBE, json.dumps({"business": str(path)}), "hold"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready, _, _ = select.select([process.stdout], [], [], 10)
        assert ready and process.stdout.readline().strip() == "locked"
        assert probe({"business": path}).stdout.strip() == "storage_busy"
        process.send_signal(signal.SIGKILL)
        process.wait(timeout=5)
        assert (tmp_path / "database.db.signaldesk.lock").is_file()
        assert probe({"business": path}).stdout.strip() == "locked"
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def test_unsupported_platform_fails_closed_only_for_file_databases(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    with storage_locks({}):
        pass
    with pytest.raises(StorageError) as raised:
        with storage_locks({"business": tmp_path / "database.db"}):
            pytest.fail("Unsupported platform cannot silently skip file ownership")
    assert raised.value.code == "storage_unsupported"


async def test_three_adapters_open_the_same_canonical_paths_as_their_locks(tmp_path, monkeypatch):
    directory = tmp_path / "canonical"
    directory.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(directory, target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        _env_file=None,
        database_url="sqlite+aiosqlite:///./alias/business #@% 中文.db",
        checkpoint_url="sqlite:///./alias/checkpoints #@% 中文.db",
        memory_url="sqlite:///./alias/memory #@% 中文.db",
    )
    paths = sqlite_paths(settings, require_all=True)
    with storage_locks(paths):
        repo = Repository(settings)
        try:
            await repo.initialize()
            project = await repo.create_project(
                ProjectCreate(
                    name="Path fixture", sources=[{"name": "Fixture", "url": "https://example.com"}]
                )
            )
            assert await repo.get_project(project.id)
            assert repo.engine.url.database == str(paths["business"])
            async with open_checkpointer(settings.checkpoint_url) as saver:
                await saver.setup()
                async with saver.conn.execute("PRAGMA database_list") as cursor:
                    assert (await cursor.fetchone())[2] == str(paths["checkpoints"])
            async with open_memory_store(settings) as store:
                await store.aput(("fixture",), "key", {"text": "persisted"})
        finally:
            await repo.close()
    for role, expected in (
        ("business", "projects"),
        ("checkpoints", "checkpoints"),
        ("memory", "store_items"),
    ):
        with sqlite3.connect(paths[role]) as connection:
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (expected,)
            ).fetchone()
    assert len(list(directory.glob("*.db"))) == 3


async def test_repository_and_checkpoints_keep_in_memory_support():
    settings = Settings(_env_file=None, database_url="sqlite+aiosqlite:///:memory:")
    repo = Repository(settings)
    try:
        await repo.initialize()
        project = await repo.create_project(
            ProjectCreate(
                name="Memory fixture", sources=[{"name": "Fixture", "url": "https://example.com"}]
            )
        )
        assert await repo.get_project(project.id)
        async with open_checkpointer("sqlite:///:memory:") as saver:
            await saver.setup()
            assert await saver.aget_tuple({"configurable": {"thread_id": "empty"}}) is None
    finally:
        await repo.close()


@pytest.mark.parametrize("mutation", ["symlink", "cwd", "original_settings"])
async def test_production_app_freezes_storage_identities_before_lifespan(
    tmp_path, monkeypatch, mutation
):
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(first, target_is_directory=True)
    if mutation == "cwd":
        monkeypatch.chdir(first)
        source = Path(".")
    else:
        source = alias if mutation == "symlink" else first
    original = local_settings(source)
    original.enable_a2a = False
    original.sandbox_enabled = False
    original.enable_vector_search = False
    app = create_app(original)
    if mutation == "symlink":
        alias.unlink()
        alias.symlink_to(second, target_is_directory=True)
    elif mutation == "cwd":
        monkeypatch.chdir(second)
    else:
        replacement = local_settings(second)
        original.database_url = replacement.database_url
        original.checkpoint_url = replacement.checkpoint_url
        original.memory_url = replacement.memory_url
    expected = sqlite_paths(local_settings(first), require_all=True)
    async with app.router.lifespan_context(app):
        repo = app.state.repo
        assert repo.engine.url.database == str(expected["business"])
        assert sqlite_paths(repo.settings, require_all=True) == expected
        project = await repo.create_project(
            ProjectCreate(
                name="Frozen fixture", sources=[{"name": "Fixture", "url": "https://example.com"}]
            )
        )
        await app.state.memory.set_preference(project.id, "frozen", {"text": "stored"})
        for role, path in expected.items():
            with pytest.raises(StorageError) as raised:
                with storage_locks({role: path}):
                    pytest.fail("Every actual database must remain owned")
            assert raised.value.code == "storage_busy"
        assert (await app.state.memory.list_preferences(project.id))[0]["key"] == "frozen"
        assert not list(second.iterdir())
    with storage_locks(expected):
        assert all(path.is_file() for path in expected.values())
