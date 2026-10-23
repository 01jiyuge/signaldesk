import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from pydantic import ValidationError

from signaldesk import database_cli
from signaldesk.config import Settings
from signaldesk.local_storage import StorageError

SECRET = "must-not-print-private-token"


def test_backup_dispatch_loads_only_the_current_directory_dotenv(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    observed = {}
    settings = object()

    def dotenv(**kwargs):
        observed["dotenv"] = kwargs

    def backup(actual_settings, output, *, timeout_seconds):
        observed["backup"] = (actual_settings, output, timeout_seconds)
        return {"bundle": str(output), "verified": True}

    monkeypatch.setattr(database_cli, "load_dotenv", dotenv)
    monkeypatch.setattr(database_cli, "Settings", lambda: settings)
    monkeypatch.setattr(database_cli, "backup_sqlite", backup)
    assert database_cli.main(["backup", "--output", "new-bundle", "--timeout", "1"]) == 0
    assert observed == {
        "dotenv": {"dotenv_path": tmp_path / ".env", "override": False},
        "backup": (settings, Path("new-bundle"), 1.0),
    }
    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "status": "ok",
        "command": "backup",
        "bundle": "new-bundle",
        "verified": True,
    }
    assert output.err == ""


@pytest.mark.parametrize("command", ["verify", "restore"])
def test_verify_and_restore_never_load_configuration(command, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("Offline bundle operations must not load .env or Settings")

    observed = []

    def verify(bundle, *, timeout_seconds):
        observed.append((bundle, timeout_seconds))
        return {"verified": True}

    def restore(bundle, target, *, timeout_seconds):
        observed.append((bundle, target, timeout_seconds))
        return {"target_dir": str(target), "verified": True}

    monkeypatch.setattr(database_cli, "load_dotenv", forbidden)
    monkeypatch.setattr(database_cli, "Settings", forbidden)
    monkeypatch.setattr(database_cli, "verify_backup", verify)
    monkeypatch.setattr(database_cli, "restore_backup", restore)
    argv = [command, "bundle"]
    if command == "restore":
        argv += ["--target-dir", "new-target"]
    argv += ["--timeout", "600"]
    assert database_cli.main(argv) == 0
    expected = (
        [(Path("bundle"), 600.0)]
        if command == "verify"
        else [(Path("bundle"), Path("new-target"), 600.0)]
    )
    assert observed == expected
    output = capsys.readouterr()
    assert json.loads(output.out)["command"] == command
    assert output.err == ""


@pytest.mark.parametrize(
    "error,expected",
    [
        (StorageError("storage_busy", "数据库正在使用，请停止服务后重试。"), "storage_busy"),
        (OSError(SECRET), "io_error"),
        (sqlite3.DatabaseError(SECRET), "sqlite_error"),
        (RuntimeError(SECRET), "operation_failed"),
        (
            ValidationError.from_exception_data(
                "Settings",
                [{"type": "int_parsing", "loc": ("max_concurrent_runs",), "input": SECRET}],
            ),
            "configuration_invalid",
        ),
    ],
)
def test_errors_are_single_safe_json_documents(error, expected, monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(database_cli, "verify_backup", fail)
    assert database_cli.main(["verify", "bundle"]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    result = json.loads(output.err)
    assert set(result) == {"status", "code", "message"}
    assert result["status"] == "error" and result["code"] == expected
    assert SECRET not in output.err and "Traceback" not in output.err


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "0.99", "601", SECRET])
def test_invalid_timeout_is_exit_two_without_echoing_input(value, capsys):
    with pytest.raises(SystemExit) as raised:
        database_cli.main(["verify", "bundle", f"--timeout={value}"])
    assert raised.value.code == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err)["code"] == "invalid_arguments"
    assert SECRET not in output.err


@pytest.mark.parametrize(
    "argv", [[], ["backup"], ["restore", "bundle"], ["verify", "bundle", "--force", SECRET]]
)
def test_missing_or_unsupported_arguments_are_safe_exit_two(argv, capsys):
    with pytest.raises(SystemExit) as raised:
        database_cli.main(argv)
    assert raised.value.code == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err)["code"] == "invalid_arguments"
    assert SECRET not in output.err


def test_keyboard_interrupt_is_not_converted_to_an_operational_error(monkeypatch, capsys):
    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(database_cli, "verify_backup", interrupt)
    with pytest.raises(KeyboardInterrupt):
        database_cli.main(["verify", "bundle"])
    output = capsys.readouterr()
    assert output.out == output.err == ""


def _environment():
    environment = os.environ.copy()
    for field in Settings.model_fields:
        environment.pop(field.upper(), None)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONIOENCODING"] = "utf-8"
    return environment


def _invoke(cwd, *args, environment=None):
    result = subprocess.run(
        [sys.executable, "-m", "signaldesk.database_cli", *map(str, args)],
        cwd=cwd,
        env=_environment() if environment is None else environment,
        capture_output=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert "Traceback" not in result.stderr
    assert SECRET not in result.stdout + result.stderr
    return result


def _source_databases(directory):
    paths = {}
    for role in ("business", "checkpoints", "memory"):
        path = directory / f"{role}.db"
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("CREATE TABLE fixture (id TEXT PRIMARY KEY, value TEXT NOT NULL)")
            connection.execute("INSERT INTO fixture VALUES (?, ?)", (role, f"saved-{role}"))
            connection.commit()
        paths[role] = path
    (directory / ".env").write_text(
        f"DATABASE_URL=sqlite+aiosqlite:///{paths['business']}\n"
        f"CHECKPOINT_URL=sqlite:///{paths['checkpoints']}\n"
        f"MEMORY_URL=sqlite:///{paths['memory']}\n"
        f"API_TOKEN={SECRET}\n",
        encoding="utf-8",
    )
    return paths


def test_real_module_backup_verify_restore_without_current_application_configuration(tmp_path):
    original = _source_databases(tmp_path)
    bundle = tmp_path / "offline-bundle"
    result = _invoke(tmp_path, "backup", "--output", bundle)
    assert result.returncode == 0 and result.stderr == ""
    backup = json.loads(result.stdout)
    assert backup["status"] == "ok" and backup["command"] == "backup" and backup["verified"]
    assert Path(backup["bundle"]) == bundle
    manifest_text = json.dumps(backup["manifest"])
    assert all(str(path) not in manifest_text for path in original.values())

    # These settings would reject a backup. Neither verify nor restore may parse
    # the unrelated current workspace's .env or consult its PostgreSQL URLs.
    another = tmp_path / "unrelated-workspace"
    another.mkdir()
    (another / ".env").write_text(f"MAX_CONCURRENT_RUNS={SECRET}\n", encoding="utf-8")
    environment = _environment()
    environment["DATABASE_URL"] = f"postgresql+asyncpg://user:{SECRET}@invalid.example/db"
    environment["CHECKPOINT_URL"] = "postgresql://invalid.example/db"
    environment["MEMORY_URL"] = "postgresql://invalid.example/db"
    checked = _invoke(another, "verify", bundle, environment=environment)
    assert checked.returncode == 0 and checked.stderr == ""
    assert json.loads(checked.stdout)["verified"] is True
    target = tmp_path / "restored"
    restored = _invoke(another, "restore", bundle, "--target-dir", target, environment=environment)
    assert restored.returncode == 0 and restored.stderr == ""
    payload = json.loads(restored.stdout)
    assert payload["command"] == "restore" and payload["verified"]
    assert Path(payload["target_dir"]) == target
    for role in original:
        with closing(sqlite3.connect(target / f"{role}.sqlite3")) as connection:
            assert connection.execute("SELECT id, value FROM fixture").fetchall() == [
                (role, f"saved-{role}")
            ]
    assert set(payload["urls"]) == {"database_url", "checkpoint_url", "memory_url"}

    preserved = {path.name: path.read_bytes() for path in target.iterdir() if path.is_file()}
    duplicate = _invoke(another, "restore", bundle, "--target-dir", target, environment=environment)
    assert duplicate.returncode == 1 and duplicate.stdout == ""
    assert json.loads(duplicate.stderr)["status"] == "error"
    assert preserved == {
        path.name: path.read_bytes() for path in target.iterdir() if path.is_file()
    }


def test_real_module_missing_inputs_and_invalid_settings_remain_safe_json(tmp_path):
    missing = _invoke(tmp_path, "verify", tmp_path / SECRET)
    assert missing.returncode == 1 and missing.stdout == ""
    assert json.loads(missing.stderr)["status"] == "error"
    (tmp_path / ".env").write_text(f"MAX_CONCURRENT_RUNS={SECRET}\n", encoding="utf-8")
    output = tmp_path / "new-bundle"
    invalid = _invoke(tmp_path, "backup", "--output", output)
    assert invalid.returncode == 1 and invalid.stdout == ""
    assert json.loads(invalid.stderr)["code"] == "configuration_invalid"
    assert not output.exists()
