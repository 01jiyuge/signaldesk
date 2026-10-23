# SignalDesk · 竞品情报 Agent

[![Checks](https://github.com/01jiyuge/signaldesk/actions/workflows/ci.yml/badge.svg)](https://github.com/01jiyuge/signaldesk/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

使用 Python、FastAPI、LangGraph、Deep Agents、LangChain、Agent Skills 和 MCP 构建的持续型竞品情报项目。

Evidence-first competitive intelligence agents with durable workflows, project memory, hybrid retrieval, Docker Sandbox and A2A interoperability.

当前版本 **0.4.3**：页面采集与 HTTP 重排补充原始响应限长、未压缩响应要求和整体网络超时；检索修复最新快照筛选与嵌入错误处理，API 拒绝非法 Unicode 和非有限 JSON 数值，控制台完善鉴权、连接和表单竞态保护。详见 [本轮说明](docs/V0.4.3_UPGRADE.md)。

## 本地启动

需要 Python 3.11–3.14 和 [uv](https://docs.astral.sh/uv/)。在项目目录运行：

```bash
git clone https://github.com/01jiyuge/signaldesk.git
cd signaldesk
cp .env.example .env
uv sync --frozen --extra dev
uv run signaldesk
```

打开 <http://127.0.0.1:8000>，创建演示项目，依次运行“基线 v1 → 变化 v2 → 相同 v2”。
Northstar / Orbit 是虚构夹具，demo 不访问外网、不调用模型。
真实调研需配置 `MODEL=openai:你的模型名`、`OPENAI_API_KEY`，使用真实公网 HTML 来源并选择 live；可用 `OPENAI_BASE_URL` 接入兼容服务。缺配置会报错。

## 已实现

- 中文控制台：项目与来源编辑、后台运行、取消/恢复、报告历史、引用与 Markdown/JSON 导出。
- LangGraph 五阶段图：采集 → 对比 → Deep Agents 研究 → 引用一致性校验 → 保存。
- LangChain 工具与应用 Skills；可接入显式白名单的 MCP，只读 stdio MCP 服务可独立启动。
- 默认 SQLite；PostgreSQL 业务库、checkpointer、官方 AsyncPostgresStore 与 pgvector 已在独立本地库验证。
- Checkpointer 保存节点进度；Store 按项目保存明确偏好与运行摘要，重启后保留。偏好变化会重新触发研究。
- 词法 + PostgreSQL 全文/GIN（本地 SQLite 使用 FTS5）+ 可选 pgvector，RRF 融合后重排。
- Reranker 默认 `lexical` 确定性基线，可改为 `http` 接入模型重排服务；默认没有加载神经重排模型。
- AG-UI 标准事件经 SSE 订阅已有任务，支持游标续传、断线重连和 Bearer；JSON 轮询作为明确标识的兼容回退。
- `signaldesk-eval` 默认离线运行固定夹具；`--langsmith` 显式上传指标并创建实验。真实任务追踪可另行开启。
- Sandbox：容器内执行 Python/JSON，离线、非 root、只读根目录、资源/输出限制、超时/取消清理、持久执行审计。
- A2A：官方 SDK 1.2.2、协议 1.0 JSON-RPC 及 SDK 0.3 兼容，Agent Card、任务委托/查询/取消、持久幂等和远端白名单。
- Deep Agents 可按需调用 `execute_python` 与已授权映射的 `delegate_research`；每次绑定当前项目/run，入站 A2A 研究禁止继续自动委托。
- 前端「扩展能力」提供实际执行与委托表单，展示真实 stdout/stderr/JSON、任务状态与远端报告；历史首屏 20 条，支持加载更多、恢复页外选择和本地缓存详情。
- `GET /api/capabilities` 查看本实例的检索、记忆、交互与扩展状态。
- `/api` 与 `/a2a` 下的 `POST`、`PUT`、`PATCH` 在解析前限制请求体实际字节数；`MAX_REQUEST_BYTES` 默认 `1048576`（1 MiB），超限返回 413，非 `identity` 内容编码返回 415。

当前执行器面向本地单用户、单进程。Sandbox 和 A2A 默认关闭，按下面文档启用；自动调度、团队权限和分布式队列仍在后续阶段。
A2A 已确认的重试复用本地委托映射，不重复发送创建请求；远端已接受但 ACK 丢失时仍可能留下 `PENDING`，外部执行的恰好一次语义依赖远端幂等支持。
AG-UI 当前是业务任务的标准事件投影，未实现通用 `RunAgentInput` 运行端点。
引用校验只检查证据 ID 和引文是否存在于原文，不证明事实真实或推理成立。

## 方案与验证

- [完整项目方案](docs/PROJECT_PLAN.md)
- [架构、API 与项目目录](docs/ARCHITECTURE.md)
- [分阶段实施与当前状态](docs/ROADMAP.md)
- [启动、配置和评测](docs/OPERATIONS.md)
- [V0.4.3 采集、检索与交互边界](docs/V0.4.3_UPGRADE.md)
- [V0.4.2 历史分页与生命周期](docs/V0.4.2_UPGRADE.md)
- [V0.4.1 可靠性与运行边界](docs/V0.4.1_UPGRADE.md)
- [V0.4 Sandbox 与 A2A](docs/V0.4_UPGRADE.md)
- [Sandbox 配置与隔离边界](docs/SANDBOX.md)
- [A2A 双实例接入](docs/A2A.md)
- [V0.3 历史实现说明](docs/V0.3_UPGRADE.md)
- [V0.2 历史变更说明](docs/V0.2_UPGRADE.md)

```bash
uv run pytest -q
uv run ruff check .
uv run signaldesk-eval --output data/evaluation.json
uv run signaldesk-mcp
docker compose up --build
```

V0.4.3 完整 Python 回归 **344 项通过**（含真实 PostgreSQL 的 7 项与真实 Docker 的 3 项集成测试），前端行为测试 **12/12 通过**；独立 HTTP 服务与浏览器已验证错误 Token、操作限制、正确重连和 AG-UI 研究恢复。离线评测 **9/9 场景、55/55 项期望匹配**，不衡量真实模型效果。历史验证保留在 [V0.4.2 说明](docs/V0.4.2_UPGRADE.md)。
GitHub Actions 已新增 PostgreSQL 与 Docker 真实集成任务；fork PR 保留离线检查。各次提交的实际结果见 [Actions](https://github.com/01jiyuge/signaldesk/actions/workflows/ci.yml)。
没有使用真实模型、嵌入或远端 Reranker 密钥，也未上传 LangSmith；这些服务的效果仍需配置后验收。
Docker 隔离与双实例 A2A 已实际运行。Compose 默认不开放 Docker socket，容器内 app 不自动获得宿主执行器权限；Sandbox 可先按原生 Python + Docker 路径运行。

## 参与与许可

欢迎提交 Issue 和 Pull Request，开发流程见 [CONTRIBUTING.md](CONTRIBUTING.md)。项目采用 [MIT License](LICENSE)。
