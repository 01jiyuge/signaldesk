"""Offline bundles preserve real SQLite data and never replace live targets."""

import hashlib
import json
import os
import sqlite3
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

import signaldesk.backups as module
from signaldesk.app import create_app
from signaldesk.backups import backup_sqlite, restore_backup, verify_backup
from signaldesk.checkpoints import open_checkpointer
from signaldesk.config import Settings
from signaldesk.local_storage import StorageError, sqlite_paths, storage_locks
from signaldesk.memory import ProjectMemory, open_memory_store
from signaldesk.repository import Repository
from signaldesk.schemas import ProjectCreate, RunCreate


def settings_for(directory):
    return Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{directory}/business.db",
        checkpoint_url=f"sqlite:///{directory}/checkpoints.db",
        memory_url=f"sqlite:///{directory}/memory.db",
        model="",
        openai_api_key="",
        enable_a2a=False,
        sandbox_enabled=False,
        enable_vector_search=False,
        reranker_provider="none",
    )


def seed_databases(directory):
    directory.mkdir(parents=True)
    settings = settings_for(directory)
    for role, path in sqlite_paths(settings, require_all=True).items():
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE fixture(role TEXT, value TEXT)")
            connection.execute("INSERT INTO fixture VALUES (?, ?)", (role, "中文 fixture"))
    return settings


@pytest.fixture
def sources(tmp_path):
    return seed_databases(tmp_path / "sources")


def load_manifest(bundle):
    return json.loads((bundle / "manifest.json").read_text())


def save_manifest(bundle, manifest):
    (bundle / "manifest.json").write_text(json.dumps(manifest))


def rehash(bundle, filename):
    manifest = load_manifest(bundle)
    content = (bundle / filename).read_bytes()
    item = next(row for row in manifest["databases"] if row["file"] == filename)
    item.update(size_bytes=len(content), sha256=hashlib.sha256(content).hexdigest())
    save_manifest(bundle, manifest)


def assert_no_staging(parent):
    assert list(parent.glob(".signaldesk-stage-*")) == []


def test_wal_commit_not_checkpointed_is_included_in_a_standalone_private_bundle(sources, tmp_path):
    source = sqlite_paths(sources)["business"]
    connection = sqlite3.connect(source)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("INSERT INTO fixture VALUES ('business', 'WAL committed fixture')")
        connection.commit()
        assert source.with_name(source.name + "-wal").stat().st_size > 0
        bundle = tmp_path / "bundle"
        result = backup_sqlite(sources, bundle)
        assert result["verified"] is True and result["bundle"] == str(bundle)
        assert set(item.name for item in bundle.iterdir()) == {
            "manifest.json",
            "business.sqlite3",
            "checkpoints.sqlite3",
            "memory.sqlite3",
        }
        with sqlite3.connect(bundle / "business.sqlite3") as copied:
            assert copied.execute("PRAGMA journal_mode").fetchone() == ("delete",)
            assert copied.execute("SELECT value FROM fixture ORDER BY rowid").fetchall() == [
                ("中文 fixture",),
                ("WAL committed fixture",),
            ]
        manifest_text = (bundle / "manifest.json").read_text()
        assert str(source) not in manifest_text and sources.database_url not in manifest_text
        assert "openai_api_key" not in manifest_text and ".env" not in manifest_text
        assert stat.S_IMODE(bundle.stat().st_mode) == 0o700
        assert all(stat.S_IMODE(item.stat().st_mode) == 0o600 for item in bundle.iterdir())
        assert verify_backup(bundle)["manifest"] == result["manifest"]
        assert_no_staging(tmp_path)
    finally:
        connection.close()


async def test_real_business_checkpoint_and_memory_restore_without_running_research(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("LANGSMITH_TRACING", "false")
    monkeypatch.setenv("LANGCHAIN_TRACING_V2", "false")
    settings = settings_for(tmp_path / "live")
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        repo = app.state.repo
        project = await repo.create_project(
            ProjectCreate(
                name="离线恢复 fixture",
                sources=[{"name": "Fixture", "url": "https://example.com"}],
            )
        )
        run = await repo.create_run(project.id, RunCreate())
        await repo.update_run(run.id, status="completed", report={"summary": "真实数据库 fixture"})
        await repo.append_event(run.id, "completed", "离线 fixture 完成")
        await app.state.memory.set_preference(project.id, "language", {"text": "中文"})
        config = {"configurable": {"thread_id": run.id}}
        await app.state.workflow.graph.aupdate_state(config, {"run_id": run.id}, as_node="collect")
        with pytest.raises(StorageError) as blocked:
            backup_sqlite(settings, tmp_path / "while-running")
        assert blocked.value.code == "storage_busy"
        assert not (tmp_path / "while-running").exists()

    bundle, target = tmp_path / "backup", tmp_path / "恢复 目录"
    backup_sqlite(settings, bundle)
    result = restore_backup(bundle, target)
    assert result["verified"] is True and result["target_dir"] == str(target)
    restored = settings.model_copy(update=result["urls"])
    restored_repo = Repository(restored)
    try:
        await restored_repo.initialize()
        saved = await restored_repo.get_run(run.id)
        assert saved.status == "completed" and saved.report == {"summary": "真实数据库 fixture"}
        assert (await restored_repo.get_project(project.id)).name == "离线恢复 fixture"
        assert (await restored_repo.list_events(run.id))[-1].message == "离线 fixture 完成"
        async with open_memory_store(restored) as store:
            preference = await ProjectMemory(store).get_preference(project.id, "language")
            assert preference["value"]["text"] == "中文"
        async with open_checkpointer(restored.checkpoint_url) as checkpointer:
            checkpoint = await checkpointer.aget_tuple(config)
            assert checkpoint.checkpoint["channel_values"]["run_id"] == run.id
    finally:
        await restored_repo.close()
    assert_no_staging(tmp_path)


@pytest.mark.parametrize("operation", ["backup", "restore"])
def test_existing_target_and_symlink_are_never_replaced(sources, tmp_path, operation):
    bundle = tmp_path / "bundle"
    backup_sqlite(sources, bundle)
    target = tmp_path / "target"
    target.mkdir()
    sentinel = target / "keep.txt"
    sentinel.write_text("another owner's directory")

    def action(destination):
        if operation == "backup":
            return backup_sqlite(sources, destination)
        return restore_backup(bundle, destination)

    for destination in (target, tmp_path / "target-link"):
        if destination != target:
            destination.symlink_to(target, target_is_directory=True)
        with pytest.raises(StorageError) as error:
            action(destination)
        assert error.value.code == "target_exists"
        assert sentinel.read_text() == "another owner's directory"
    assert_no_staging(tmp_path)


def test_missing_source_is_not_created_and_configured_symlink_is_rejected(sources, tmp_path):
    paths = sqlite_paths(sources)
    paths["checkpoints"].unlink()
    with pytest.raises(StorageError) as error:
        backup_sqlite(sources, tmp_path / "missing")
    assert error.value.code == "source_missing"
    assert not paths["checkpoints"].exists() and not (tmp_path / "missing").exists()
    paths["checkpoints"].symlink_to(paths["business"])
    with pytest.raises(StorageError) as error:
        backup_sqlite(sources, tmp_path / "symlink")
    assert error.value.code == "backup_invalid"
    assert not (tmp_path / "symlink").exists()


@pytest.mark.parametrize(
    "updates",
    [
        {"memory_url": "sqlite:///:memory:"},
        {"checkpoint_url": "postgresql://fixture-secret@localhost/fixture"},
        {"database_url": "sqlite+aiosqlite:///file:fixture?uri=true"},
    ],
)
def test_non_file_sqlite_configuration_fails_without_exposing_credentials(
    sources, tmp_path, updates
):
    with pytest.raises(StorageError) as error:
        backup_sqlite(sources.model_copy(update=updates), tmp_path / "invalid")
    assert "fixture-secret" not in str(error.value)
    assert not (tmp_path / "invalid").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown_version",
        "duplicate_role",
        "path_traversal",
        "extra_field",
        "missing_file",
        "symlink_file",
        "hardlink_file",
        "extra_wal",
        "extra_directory",
        "wrong_size",
        "wrong_hash",
        "large_manifest",
        "duplicate_json_key",
        "nonfinite_json",
    ],
)
def test_invalid_bundles_are_rejected_before_creating_a_restore_target(sources, tmp_path, mutation):
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    backup_sqlite(sources, bundle)
    manifest = load_manifest(bundle)
    first = manifest["databases"][0]
    if mutation == "unknown_version":
        manifest["format_version"] = 2
    elif mutation == "duplicate_role":
        manifest["databases"][1] = first.copy()
    elif mutation == "path_traversal":
        first["file"] = "../business.db"
    elif mutation == "extra_field":
        first["source_url"] = "PRIVATE_CONFIGURATION"
    elif mutation == "missing_file":
        (bundle / first["file"]).unlink()
    elif mutation == "symlink_file":
        (bundle / first["file"]).unlink()
        (bundle / first["file"]).symlink_to(sqlite_paths(sources)["business"])
    elif mutation == "hardlink_file":
        (bundle / first["file"]).unlink()
        os.link(sqlite_paths(sources)["business"], bundle / first["file"])
    elif mutation == "extra_wal":
        (bundle / "business.sqlite3-wal").write_bytes(b"not a valid archive sidecar")
    elif mutation == "extra_directory":
        (bundle / "nested").mkdir()
    elif mutation == "wrong_size":
        first["size_bytes"] += 1
    elif mutation == "wrong_hash":
        first["sha256"] = "0" * 64
    if mutation == "large_manifest":
        (bundle / "manifest.json").write_bytes(b" " * (65_536 + 1))
    elif mutation == "duplicate_json_key":
        (bundle / "manifest.json").write_text('{"format_version":1,"format_version":1}')
    elif mutation == "nonfinite_json":
        (bundle / "manifest.json").write_text('{"format_version":NaN}')
    else:
        save_manifest(bundle, manifest)
    for action in (lambda: verify_backup(bundle), lambda: restore_backup(bundle, target)):
        with pytest.raises(StorageError) as error:
            action()
        assert error.value.code == "backup_invalid"
    assert not target.exists()
    assert_no_staging(tmp_path)


def test_corrupt_database_with_updated_manifest_still_fails_sqlite_integrity(sources, tmp_path):
    bundle = tmp_path / "bundle"
    backup_sqlite(sources, bundle)
    (bundle / "business.sqlite3").write_bytes(b"SQLite format 3\0" + b"broken" * 100)
    rehash(bundle, "business.sqlite3")
    with pytest.raises(StorageError) as error:
        verify_backup(bundle)
    assert error.value.code == "backup_invalid"


def test_wal_header_without_sidecars_is_rejected_before_connecting_and_never_mutates_bundle(
    sources, tmp_path, monkeypatch
):
    bundle = tmp_path / "bundle"
    backup_sqlite(sources, bundle)
    path = bundle / "business.sqlite3"
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    finally:
        connection.close()
    assert path.read_bytes()[18:20] == b"\x02\x02"
    assert not path.with_name(path.name + "-wal").exists()
    assert not path.with_name(path.name + "-shm").exists()
    rehash(bundle, path.name)
    before = {item.name: item.read_bytes() for item in bundle.iterdir()}

    def forbidden_connect(*_args, **_kwargs):
        pytest.fail("WAL archive header must be refused before SQLite can create sidecars")

    monkeypatch.setattr(module.sqlite3, "connect", forbidden_connect)
    with pytest.raises(StorageError) as error:
        verify_backup(bundle)
    assert error.value.code == "backup_invalid"
    assert {item.name: item.read_bytes() for item in bundle.iterdir()} == before


@pytest.mark.parametrize("name", ["恢复 目录", "hash#name", "at@name", "percent%name"])
def test_restored_urls_preserve_supported_filename_characters(sources, tmp_path, name):
    bundle, target = tmp_path / "bundle", tmp_path / name
    backup_sqlite(sources, bundle)
    result = restore_backup(bundle, target)
    paths = sqlite_paths(SimpleNamespace(**result["urls"]), require_all=True)
    assert paths == {role: target / filename for role, filename in module.DATABASE_FILES.items()}
    for role, path in paths.items():
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT role, value FROM fixture").fetchall() == [
                (role, "中文 fixture")
            ]


@pytest.mark.parametrize("name", ["invalid?name", "invalid?", "?token=fixture-secret"])
def test_unrepresentable_restore_url_is_rejected_before_creating_any_output(
    sources, tmp_path, name
):
    bundle, target = tmp_path / "bundle", tmp_path / name
    backup_sqlite(sources, bundle)
    before = set(tmp_path.iterdir())
    with pytest.raises(StorageError) as error:
        restore_backup(bundle, target)
    assert error.value.code == "backup_invalid"
    assert "fixture-secret" not in str(error.value)
    assert not target.exists() and set(tmp_path.iterdir()) == before
    assert verify_backup(bundle)["verified"] is True


def test_foreign_key_violations_are_detected_even_when_sqlite_integrity_is_ok(sources, tmp_path):
    bundle = tmp_path / "bundle"
    backup_sqlite(sources, bundle)
    with sqlite3.connect(bundle / "business.sqlite3") as connection:
        connection.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        connection.execute("CREATE TABLE child(parent_id INTEGER REFERENCES parent(id))")
        connection.execute("INSERT INTO child VALUES (999)")
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    rehash(bundle, "business.sqlite3")
    with pytest.raises(StorageError) as error:
        verify_backup(bundle)
    assert error.value.code == "backup_invalid"


@pytest.mark.parametrize("operation", ["backup", "restore"])
def test_second_database_failure_removes_only_owned_staging(
    sources, tmp_path, monkeypatch, operation
):
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    backup_sqlite(sources, bundle)
    name = "_backup_database" if operation == "backup" else "_copy_file"
    original = getattr(module, name)
    count = 0

    def fail_second(*args):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError("PRIVATE_FAILURE token=fixture-secret")
        return original(*args)

    monkeypatch.setattr(module, name, fail_second)
    with pytest.raises(StorageError) as error:
        if operation == "backup":
            backup_sqlite(sources, target)
        else:
            restore_backup(bundle, target)
    assert error.value.code == "io_error" and "fixture-secret" not in str(error.value)
    assert not target.exists()
    assert verify_backup(bundle)["verified"]
    assert_no_staging(tmp_path)


@pytest.mark.parametrize("operation", ["backup", "restore"])
def test_parent_fsync_failure_reports_uncertain_publication_without_removing_valid_output(
    sources, tmp_path, monkeypatch, operation
):
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    backup_sqlite(sources, bundle)
    original_files = {
        path: path.read_bytes() for path in [*sqlite_paths(sources).values(), *bundle.iterdir()]
    }
    fsync = module._fsync_directory

    def fail_publication_confirmation(path):
        if path == target.parent:
            raise OSError("PRIVATE_FSYNC_FAILURE token=fixture-secret")
        return fsync(path)

    monkeypatch.setattr(module, "_fsync_directory", fail_publication_confirmation)
    with pytest.raises(StorageError) as error:
        if operation == "backup":
            backup_sqlite(sources, target)
        else:
            restore_backup(bundle, target)
    assert error.value.code == "publication_uncertain"
    assert str(error.value) == "目标目录已发布，但持久化确认失败；请先校验该目录，不要直接重试。"
    assert target.is_dir() and verify_backup(target)["verified"] is True
    assert {path: path.read_bytes() for path in original_files} == original_files
    assert_no_staging(tmp_path)


def test_restore_rechecks_copied_bytes_after_source_preflight(sources, tmp_path, monkeypatch):
    bundle, target = tmp_path / "bundle", tmp_path / "target"
    backup_sqlite(sources, bundle)
    copy = module._copy_file
    changed = False

    def mutate_after_preflight(source, destination, deadline):
        nonlocal changed
        if not changed:
            changed = True
            with sqlite3.connect(source) as connection:
                connection.execute(
                    "INSERT INTO fixture VALUES ('external', 'changed after verify')"
                )
        return copy(source, destination, deadline)

    monkeypatch.setattr(module, "_copy_file", mutate_after_preflight)
    with pytest.raises(StorageError) as error:
        restore_backup(bundle, target)
    assert error.value.code == "backup_invalid"
    assert changed and not target.exists()
    assert_no_staging(tmp_path)


def test_concurrent_same_destination_publishers_cannot_replace_each_other(
    sources, tmp_path, monkeypatch
):
    other = seed_databases(tmp_path / "other")
    target = tmp_path / "bundle"
    entered, release = threading.Event(), threading.Event()
    backup = module._backup_database

    def gated_backup(source, destination, deadline):
        if not entered.is_set():
            entered.set()
            assert release.wait(3), "Controlled publisher was not released"
        return backup(source, destination, deadline)

    monkeypatch.setattr(module, "_backup_database", gated_backup)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(backup_sqlite, sources, target)
        try:
            assert entered.wait(3)
            with pytest.raises(StorageError) as error:
                backup_sqlite(other, target)
            assert error.value.code == "storage_busy"
        finally:
            release.set()
        assert first.result(timeout=3)["verified"] is True
    assert verify_backup(target)["verified"]
    assert_no_staging(tmp_path)


def test_backup_requires_ownership_of_every_source_role(sources, tmp_path):
    with storage_locks({"memory": sqlite_paths(sources)["memory"]}):
        with pytest.raises(StorageError) as error:
            backup_sqlite(sources, tmp_path / "bundle")
    assert error.value.code == "storage_busy"
    assert not (tmp_path / "bundle").exists()


def test_restore_never_adds_staging_or_lock_files_inside_its_source_bundle(sources, tmp_path):
    bundle = tmp_path / "bundle"
    backup_sqlite(sources, bundle)
    before = sorted(item.name for item in bundle.iterdir())
    with pytest.raises(StorageError) as error:
        restore_backup(bundle, bundle / "restored")
    assert error.value.code == "backup_invalid"
    assert sorted(item.name for item in bundle.iterdir()) == before
    assert verify_backup(bundle)["verified"] is True


@pytest.mark.parametrize("operation", ["backup", "verify", "restore"])
def test_total_deadline_raises_safe_error_and_does_not_publish(sources, tmp_path, operation):
    bundle, target = tmp_path / "bundle", tmp_path / "timed-out"
    backup_sqlite(sources, bundle)
    with pytest.raises(StorageError) as error:
        if operation == "backup":
            backup_sqlite(sources, target, timeout_seconds=1e-9)
        elif operation == "verify":
            verify_backup(bundle, timeout_seconds=1e-9)
        else:
            restore_backup(bundle, target, timeout_seconds=1e-9)
    assert error.value.code == "backup_timeout"
    assert not target.exists()
    assert_no_staging(tmp_path)


@pytest.mark.parametrize("timeout", [True, 0, -1, float("nan"), float("inf"), 601, 10**1000, "60"])
def test_invalid_timeout_is_rejected_before_io(tmp_path, timeout):
    with pytest.raises(StorageError) as error:
        verify_backup(tmp_path / "missing", timeout_seconds=timeout)
    assert error.value.code == "backup_invalid"
