import asyncio
import json

import pytest

from signaldesk.config import Settings
from signaldesk.sandbox import (
    CommandOutput,
    DockerCLI,
    DockerSandbox,
    SandboxUnavailableError,
    SandboxValidationError,
    container_name,
)


class FakeCLI:
    def __init__(self):
        self.calls = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.hold = False
        self.run_output = None
        self.daemon = True
        self.image = True
        self.cleanup_ok = True
        self.hold_create = False
        self.create_started = asyncio.Event()
        self.active = 0
        self.peak = 0

    async def run(self, args, **kwargs):
        self.calls.append((args, kwargs))
        if args[0] == "info":
            return CommandOutput(0 if self.daemon else 1, b"29.6.1", b"SECRET /host/path")
        if args[:2] == ["image", "inspect"]:
            return CommandOutput(0 if self.image else 1, b"sha256:test", b"SECRET /host/path")
        if args[0] == "rm":
            return CommandOutput(0 if self.cleanup_ok else 1)
        if args[:2] == ["container", "inspect"]:
            return CommandOutput(
                1, stderr=b"No such object" if self.cleanup_ok else b"daemon unavailable"
            )
        if args[0] == "create":
            self.create_started.set()
            if self.hold_create:
                await self.release.wait()
            return CommandOutput(0, b"f" * 64)
        assert args[0] == "start"
        self.active += 1
        self.peak = max(self.active, self.peak)
        self.started.set()
        try:
            if self.hold:
                await self.release.wait()
            if self.run_output is not None:
                return self.run_output
            payload = json.loads(kwargs["stdin"])
            # Test transport never evaluates submitted Python on the host.
            stdout = (
                "printed\n\n" + payload["marker"] + json.dumps({"result": {"answer": 42}}) + "\n"
            )
            return CommandOutput(0, stdout.encode())
        finally:
            self.active -= 1


def sandbox_settings(**kwargs):
    return Settings(_env_file=None, sandbox_enabled=True, **kwargs)


async def test_fixed_isolation_flags_stdin_only_and_result_frame():
    cli = FakeCLI()
    sandbox = DockerSandbox(sandbox_settings(), cli=cli)
    result = await sandbox.execute("result = input_data['private']", {"private": "test-only"})
    assert result.status == "completed"
    assert result.result == {"answer": 42}
    assert result.stdout == "printed\n"
    assert result.cleanup_confirmed
    args, _ = next(item for item in cli.calls if item[0][0] == "create")
    _, options = next(item for item in cli.calls if item[0][0] == "start")
    assert {
        "--interactive",
        "--pull=never",
        "--network=none",
        "--read-only",
        "--user=65534:65534",
        "--cap-drop=ALL",
        "--init",
        "--security-opt=no-new-privileges=true",
        "--memory=128m",
        "--memory-swap=128m",
        "--cpus=0.5",
        "--pids-limit=32",
        "--log-driver=none",
        "--ipc=none",
    }.issubset(args)
    assert "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777" in args
    assert not any(a.startswith(("--mount", "--volume", "--env", "--privileged")) for a in args)
    assert "result = input_data['private']" not in args
    assert "test-only" not in " ".join(args)
    assert json.loads(options["stdin"])["input_data"] == {"private": "test-only"}
    assert cli.calls[-1][0] == ["rm", "--force", container_name(result.execution_id)]


async def test_disabled_never_probes_and_availability_is_cached_redacted():
    cli = FakeCLI()
    disabled = DockerSandbox(Settings(_env_file=None), cli=cli)
    assert (await disabled.capabilities())["available"] is False
    with pytest.raises(SandboxUnavailableError):
        await disabled.execute("result = 1")
    assert cli.calls == []
    sandbox = DockerSandbox(sandbox_settings(), cli=cli)
    cli.daemon = False
    capability = await sandbox.capabilities()
    assert capability["enabled"] and not capability["available"]
    assert "SECRET" not in str(capability)
    assert await sandbox.capabilities() == capability
    assert len(cli.calls) == 1
    missing_image = FakeCLI()
    missing_image.image = False
    capability = await DockerSandbox(sandbox_settings(), cli=missing_image).capabilities()
    assert not capability["available"]
    assert "预先拉取" in capability["reason"]
    assert all(args[0] != "pull" for args, _ in missing_image.calls)


async def test_validation_before_docker_and_unsafe_image_rejected():
    cli = FakeCLI()
    sandbox = DockerSandbox(
        sandbox_settings(sandbox_max_code_bytes=4, sandbox_max_input_bytes=4), cli=cli
    )
    for code, data in [
        ("12345", None),
        ("x", "12345"),
        ("", {}),
        ("x", float("nan")),
        ("x", {1, 2}),
    ]:
        with pytest.raises(SandboxValidationError):
            await sandbox.execute(code, data)
    assert cli.calls == []
    unsafe = DockerSandbox(sandbox_settings(sandbox_image="--privileged"), cli=cli)
    assert not (await unsafe.capabilities())["available"]
    assert cli.calls == []
    with pytest.raises(SandboxValidationError):
        await sandbox.execute("x", None, execution_id="../../another-container")


@pytest.mark.parametrize(
    "output,expected",
    [
        (CommandOutput(-9, timed_out=True), "timed_out"),
        (
            CommandOutput(-9, stdout=b"bounded", output_limited=True, stdout_truncated=True),
            "output_limited",
        ),
        (CommandOutput(1, stderr=b"ValueError: calculation failed"), "failed"),
        (CommandOutput(125, stderr=b"SECRET /host/context"), "failed"),
        (CommandOutput(0, stdout=b"missing frame"), "failed"),
    ],
)
async def test_terminal_outputs_always_force_cleanup(output, expected):
    cli = FakeCLI()
    cli.run_output = output
    result = await DockerSandbox(sandbox_settings(), cli=cli).execute("result = 0")
    assert result.status == expected
    assert result.cleanup_confirmed
    assert cli.calls[-1][0][0] == "rm"
    if output.exit_code == 125:
        assert result.stderr == "" and "SECRET" not in result.model_dump_json()


async def test_cancellation_cleans_and_parallelism_is_bounded():
    cli = FakeCLI()
    cli.hold = True
    sandbox = DockerSandbox(sandbox_settings(sandbox_max_concurrent=1), cli=cli)
    first = asyncio.create_task(sandbox.execute("result = 1"))
    await asyncio.wait_for(cli.started.wait(), 1)
    second = asyncio.create_task(sandbox.execute("result = 2"))
    await asyncio.sleep(0)
    assert cli.active == cli.peak == 1
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    cli.release.set()
    assert (await second).status == "completed"
    assert cli.peak == 1
    assert sum(args[0] == "rm" for args, _ in cli.calls) == 2
    await sandbox.shutdown()


async def test_unconfirmed_cleanup_blocks_new_runs_until_recovered():
    cli = FakeCLI()
    cli.cleanup_ok = False
    sandbox = DockerSandbox(sandbox_settings(), cli=cli)
    result = await sandbox.execute("result = 1")
    assert not result.cleanup_confirmed
    assert "清理" in result.error
    assert not (await sandbox.capabilities())["available"]
    with pytest.raises(SandboxUnavailableError):
        await sandbox.execute("result = 2")
    cli.cleanup_ok = True
    sandbox._cached = None
    assert (await sandbox.capabilities())["available"]


async def test_cancellation_during_create_cannot_start_user_code():
    cli = FakeCLI()
    cli.hold_create = True
    sandbox = DockerSandbox(sandbox_settings(), cli=cli)
    task = asyncio.create_task(sandbox.execute("result = input_data", {"private": "not-argv"}))
    await asyncio.wait_for(cli.create_started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not any(args[0] == "start" for args, _ in cli.calls)
    assert cli.calls[-1][0][0] == "rm"


class FakeInput:
    def __init__(self):
        self.data = bytearray()
        self.closed = False

    def write(self, value):
        self.data.extend(value)

    async def drain(self):
        pass

    def close(self):
        self.closed = True


class FakeProcess:
    def __init__(self):
        self.stdin = FakeInput()
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode = None
        self.done = asyncio.Event()
        self.killed = False

    def kill(self):
        self.killed = True
        self.returncode = -9
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        self.done.set()

    async def wait(self):
        await self.done.wait()
        return self.returncode


async def test_transport_reads_both_streams_bounded_and_sanitizes_cli_environment(monkeypatch):
    process = FakeProcess()
    captured = {}

    async def spawn(*args, **kwargs):
        captured.update(args=args, **kwargs)
        process.stdout.feed_data(b"o" * 20000)
        process.stderr.feed_data(b"e" * 20000)
        return process

    monkeypatch.setenv("OPENAI_API_KEY", "do-not-inherit")
    output = await DockerCLI(spawn=spawn).run(
        ["run", "fixed-image"], stdin=b"{}", max_output_bytes=1024
    )
    assert output.output_limited and process.killed
    assert len(output.stdout) == len(output.stderr) == 1024
    assert output.stdout_truncated and output.stderr_truncated
    assert "OPENAI_API_KEY" not in captured["env"]
    assert captured["args"] == ("docker", "run", "fixed-image")
    assert captured["stdin"] == asyncio.subprocess.PIPE
    assert process.stdin.closed


async def test_transport_timeout_and_external_cancellation_kill_child():
    processes = []

    async def spawn(*args, **kwargs):
        process = FakeProcess()
        processes.append(process)
        return process

    cli = DockerCLI(spawn=spawn)
    output = await cli.run(["run", "fixed-image"], timeout=0.01)
    assert output.timed_out and processes[0].killed
    task = asyncio.create_task(cli.run(["run", "fixed-image"]))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert processes[1].killed


async def test_missing_docker_binary_is_a_safe_unavailable_error():
    async def missing(*args, **kwargs):
        raise FileNotFoundError("SECRET /host/path")

    sandbox = DockerSandbox(sandbox_settings(), cli=DockerCLI(spawn=missing))
    with pytest.raises(SandboxUnavailableError) as error:
        await sandbox.execute("result = 1")
    assert "SECRET" not in str(error.value)
