# Python Sandbox（v0.4）

SignalDesk 使用本机 Docker Engine/ Docker Desktop 创建短生命周期 Linux 容器。
用户提交的 Python 只在容器内执行；应用服务器不会调用宿主 Python 执行这些代码。
本模块不依赖 Cloudflare 账号，也没有接入 Cloudflare Sandbox SDK。

## 管理员准备

先安装并启动 Docker，手动准备镜像，再开启 `.env` 中的配置：

```dotenv
SANDBOX_ENABLED=true
SANDBOX_IMAGE=python:3.14-slim
SANDBOX_TIMEOUT_SECONDS=10
SANDBOX_MEMORY_MB=128
SANDBOX_CPUS=0.5
SANDBOX_PIDS_LIMIT=32
SANDBOX_MAX_CONCURRENT=2
SANDBOX_MAX_OUTPUT_BYTES=65536
SANDBOX_MAX_CODE_BYTES=20000
SANDBOX_MAX_INPUT_BYTES=65536
```

应用仅使用 `--pull=never`，不会替管理员安装 Docker 或拉取镜像。
生产部署建议将 `SANDBOX_IMAGE` 固定为已审核的 digest，例如本次验证使用的官方多架构镜像
`python@sha256:0514bc76712d64f8bb48adcb95b1ab0d88c69fee95d62fe4c18ee948bee9b1e1`。
镜像由管理员选择，HTTP 请求不接受镜像、环境变量、卷、网络或启动参数。
默认关闭时能力查询与服务启动均不探测 Docker。

## 输入与结果

代码直接读取 JSON 变量 `input_data`，将需要返回的 JSON 结构赋给变量 `result`：

```python
values = input_data["values"]
print(f"已分析 {len(values)} 个数据")
result = {"total": sum(values), "average": sum(values) / len(values)}
```

只支持固定镜像内已安装的模块；默认 Python slim 镜像提供标准库。
`result` 必须可序列化为 JSON；未赋值时返回 `null`。
`print` 输出作为 `stdout`，异常回溯作为 `stderr`，两者分别按字节限制读取。
超限立即终止执行并保留有界前缀。返回文本中的无效 UTF-8 会替换为替代字符。
代码、输出和结果均属于不可信数据；它们不能自动成为来源证据、系统指令或项目明确偏好。

## 隔离与生命周期

- 先 `docker create`，收到成功响应后才执行 `docker start --attach --interactive`。
  创建阶段中断最多留下未启动容器，避免 `docker run` 被中断后延迟启动用户代码的竞态。
- 代码与 JSON 输入通过标准输入进入容器，不写宿主文件，不放入进程参数，不创建宿主绑定挂载。
- 容器使用独立 PID/IPC/网络命名空间、`--network=none`、非 root UID/GID 65534、只读根文件系统。
- `--cap-drop=ALL`、`no-new-privileges` 保留 Docker 默认 seccomp；不启用 privileged、设备或 Docker socket。
- CPU、内存及同额 memory-swap、进程数、文件描述符均设上限；`/tmp` 是 16 MiB 的 `noexec,nosuid,nodev` tmpfs。
- `--init` 负责转发信号和回收子进程。正常完成、超时、输出超限、取消与应用关闭都会额外执行强制删除；不只依赖 `--rm`。
- 执行时限覆盖启动后 attach 阶段；预检、创建和强制清理分别有额外的有界管理超时。
- Docker 不可达时不能保证即时删除。结果显式记录 `cleanup_confirmed=false`，并停止新执行，直到清理得到确认。
  重启会重新处理所有未确认清理记录，包括关闭 Sandbox 期间标记为 `interrupted` 的任务。

Docker 与宿主共享内核；这是应用的容器执行隔离边界。
面向公开多租户生产服务时应把 Docker 执行器部署到独立工作节点，按组织要求增加系统级隔离与监控。

## HTTP 与工具调用

所有接口沿用业务 API 的 Bearer token 配置，并校验项目与可选 `run_id` 的归属。

| 方法 | 路径 | 行为 |
| --- | --- | --- |
| GET | `/api/projects/{id}/sandbox/capabilities` | 返回 enabled、available、安全 reason、image、limits；探测缓存 5 秒 |
| POST | `/api/projects/{id}/sandbox/executions` | `{code,input_data,run_id?}`，返回 202 与完整执行记录 |
| GET | `/api/projects/{id}/sandbox/executions` | 返回执行记录数组，`limit=1..100` |
| GET | `/api/projects/{id}/sandbox/executions/{execution_id}` | 项目范围内的执行记录 |
| POST | `/api/projects/{id}/sandbox/executions/{execution_id}/cancel` | 停止 queued/running，返回完整执行记录 |

HTTP POST 创建后台任务，断开 HTTP 请求不会取消该任务；需要使用 cancel 接口。
Python 工具 `await service.execute(project_id, code, input_data, run_id=...)` 则等待结果，调用任务被取消时会连带取消执行。

执行状态为 `queued/running/completed/failed/timed_out/cancelled/output_limited/interrupted`。
外层执行记录 `result` 是 `SandboxResult` 对象，其中的 `result` 才是代码返回的 JSON。
`SandboxResult` 同时包含 stdout、stderr、exit_code、duration_seconds、截断标志、error、cleanup_confirmed。
Docker 关闭或镜像未准备返回 503；配置/输入不合法 422；跨项目或缺失记录 404；终态取消 409；队列满 429。

应用数据库使用独立 `sandbox_executions` 表，保存项目/run 归属、镜像、代码 SHA-256、状态与有界结果。
不持久化代码原文和 JSON 输入。关联研究 run 时追加 `sandbox_started/sandbox_finished/sandbox_cancelled` 审计事件，
事件只含执行 ID 与状态，不复制代码、输入或输出。
当前后台队列由单个应用进程拥有；部署时仍使用一个应用 worker。未来多 worker 需要租约与专用任务队列。

## 验证

离线测试以假 Docker transport 验证参数、并发流读取、取消、输出上限、认证、归属与数据库恢复，完全不在宿主执行提交代码。
真实容器测试需已启动 Docker、准备固定镜像后显式开启：

```bash
SIGNALDESK_TEST_DOCKER=1 python -m pytest tests/test_sandbox*.py
```

官方依据：[create/start 生命周期](https://docs.docker.com/reference/cli/docker/container/create/)、
[start attach](https://docs.docker.com/reference/cli/docker/container/start/)、
[隔离与运行参数](https://docs.docker.com/reference/cli/docker/container/run/)、
[资源约束](https://docs.docker.com/engine/containers/resource_constraints/)、
[默认 seccomp](https://docs.docker.com/engine/security/seccomp/)。
