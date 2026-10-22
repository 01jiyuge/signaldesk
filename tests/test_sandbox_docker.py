"""Opt-in real Docker checks. Images must already exist; never pull in tests."""

import asyncio
import json
import os

import pytest

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.sandbox import DockerSandbox, SandboxService, container_name
from signaldesk.schemas import ProjectCreate, RunCreate

pytestmark = pytest.mark.skipif(
    os.getenv("SIGNALDESK_TEST_DOCKER") != "1",
    reason="Set SIGNALDESK_TEST_DOCKER=1 for real Docker isolation checks",
)


async def test_real_docker_json_stdin_offline_nonroot_readonly_and_cleanup():
    sandbox = DockerSandbox(Settings(_env_file=None, sandbox_enabled=True))
    result = await sandbox.execute(
        """
import os, pathlib, socket
root_write_denied = False
try:
    pathlib.Path('/signaldesk-untrusted').write_text('denied')
except OSError:
    root_write_denied = True
offline = False
try:
    socket.create_connection(('1.1.1.1', 80), timeout=0.2)
except OSError:
    offline = True
status = pathlib.Path('/proc/self/status').read_text()
result = {'total': sum(input_data['values']), 'uid': os.getuid(),
          'root_write_denied': root_write_denied, 'offline': offline,
          'no_new_privs': 'NoNewPrivs:\\t1' in status,
          'capabilities_dropped': 'CapEff:\\t0000000000000000' in status,
          'secret_absent': 'OPENAI_API_KEY' not in os.environ}
print('container output')
""",
        {"values": [2, 3, 5]},
    )
    assert result.status == "completed", result.model_dump()
    assert result.result == {
        "total": 10,
        "uid": 65534,
        "root_write_denied": True,
        "offline": True,
        "no_new_privs": True,
        "capabilities_dropped": True,
        "secret_absent": True,
    }
    assert result.stdout == "container output\n" and result.cleanup_confirmed
    inspected = await sandbox.cli.run(["container", "inspect", container_name(result.execution_id)])
    assert inspected.exit_code != 0 and b"No such" in inspected.stderr
    await sandbox.shutdown()


async def test_real_docker_timeout_output_limit_and_cancellation_cleanup():
    from uuid import uuid4

    sandbox = DockerSandbox(
        Settings(_env_file=None, sandbox_enabled=True, sandbox_timeout_seconds=1)
    )
    timed = await sandbox.execute("while True: pass")
    assert timed.status == "timed_out" and timed.cleanup_confirmed
    limited = await sandbox.execute("print('x' * 200000)")
    assert (
        limited.status == "output_limited"
        and limited.stdout_truncated
        and limited.cleanup_confirmed
    )
    assert len(limited.stdout.encode()) <= sandbox.settings.sandbox_max_output_bytes
    execution_id = str(uuid4())
    task = asyncio.create_task(sandbox.execute("while True: pass", execution_id=execution_id))
    # Wait until the container actually exists, then cancel a running execution.
    found = False
    for _ in range(40):
        inspected = await sandbox.cli.run(
            [
                "container",
                "inspect",
                "--format",
                "{{.State.Running}}",
                container_name(execution_id),
            ]
        )
        if inspected.exit_code == 0 and inspected.stdout.strip() == b"true":
            found = True
            break
        await asyncio.sleep(0.02)
    assert found
    configured = await sandbox.cli.run(
        [
            "container",
            "inspect",
            "--format",
            "{{json .HostConfig}}",
            container_name(execution_id),
        ],
        max_output_bytes=8192,
    )
    limits = json.loads(configured.stdout)
    assert limits["Memory"] == limits["MemorySwap"] == 128 * 1024 * 1024
    assert limits["NanoCpus"] == 500_000_000 and limits["PidsLimit"] == 32
    assert limits["ReadonlyRootfs"] and limits["Init"]
    assert limits["NetworkMode"] == "none" and limits["CapDrop"] == ["ALL"]
    assert "no-new-privileges=true" in limits["SecurityOpt"]
    assert not limits["Binds"] and not limits.get("Mounts")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    inspected = await sandbox.cli.run(["container", "inspect", container_name(execution_id)])
    assert inspected.exit_code != 0 and b"No such" in inspected.stderr
    await sandbox.shutdown()


async def test_real_docker_service_tool_result_is_durable_and_audited(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/real-sandbox.db",
        sandbox_enabled=True,
    )
    repo = Repository(settings)
    await repo.initialize()
    project = await repo.create_project(
        ProjectCreate(
            name="Docker integration test",
            sources=[{"name": "Example", "url": "https://example.com"}],
        )
    )
    run = await repo.create_run(project.id, RunCreate())
    service = SandboxService(repo, settings)
    await service.initialize()
    try:
        result = await service.execute(
            project.id,
            "result = {'sum': sum(input_data)}",
            [4, 7],
            run_id=run.id,
        )
        assert result.status == "completed" and result.result == {"sum": 11}
        assert result.cleanup_confirmed
        events = [event.kind for event in await repo.list_events(run.id)]
        assert events[-2:] == ["sandbox_started", "sandbox_finished"]
    finally:
        await service.shutdown()
        await repo.close()
    reopened = Repository(settings)
    await reopened.initialize()
    recovered = SandboxService(reopened, settings)
    await recovered.initialize()
    try:
        persisted = await recovered.get(project.id, result.execution_id)
        assert persisted.status == "completed" and persisted.result.result == {"sum": 11}
        assert persisted.run_id == run.id
    finally:
        await recovered.shutdown()
        await reopened.close()
