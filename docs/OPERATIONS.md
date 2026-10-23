# 启动、配置与验证

更新：2026-10-23。当前 V0.4.2 为本地单用户、单进程可运行版本；历史分页和旧库兼容升级见 [V0.4.2_UPGRADE.md](./V0.4.2_UPGRADE.md)，MAX_REQUEST_BYTES 配置见 [V0.4.1_UPGRADE.md](./V0.4.1_UPGRADE.md)，Sandbox/A2A 基础见 [V0.4_UPGRADE.md](./V0.4_UPGRADE.md)。

## 1. 本地启动

```bash
cd signaldesk
cp .env.example .env
uv sync --extra dev
uv run signaldesk
```

打开 <http://127.0.0.1:8000>；OpenAPI 为 <http://127.0.0.1:8000/docs>。
端口被占用时使用 `uv run signaldesk --port 8765`。需要 Python 3.11–3.14。
机器没有 uv 时，可以在虚拟环境中运行 `python -m pip install uv`，也可以直接
`python -m pip install -e '.[dev]'` 后使用 `signaldesk`。不要把密钥提交到版本控制。

### 演示验收

1. 控制台选择创建演示项目，来源为 Northstar `/pricing` 和 Orbit `/changelog`。
2. 选择“基线 v1”并运行：得到两项来源基线。
3. 选择“变化 v2”并运行：Northstar 月价从 $29 变为 $39、席位从 5 变为 10，Orbit 增加 Enterprise SSO。
4. 再运行一次 v2：没有内容变化，findings 为空。
5. 打开引用和历史快照，查看新旧引文与 diff。搜索 `SSO` 可返回本项目证据。
6. 重启服务，项目、报告、快照及检查点仍保留。

V0.2 可在项目标题旁编辑来源与关注规则，活动任务结束后才允许保存。旧报告继续使用原证据。
已完成报告可点击 Markdown / JSON 导出；新旧快照和引用随报告一起保留。

以上产品、日期与价格全部为虚构测试夹具；不访问示例域名，不调用模型 API。
演示与真实模式都写入 mode 字段。用户应为真实来源另建项目，避免示例与业务材料混用。

## 2. 真实模型与采集

`.env` 中配置：

```dotenv
MODEL=openai:你实际可调用的模型名
OPENAI_API_KEY=填写真实密钥
# OPENAI_BASE_URL=https://你的兼容提供商/v1
```

V0.1 提供 OpenAI 兼容适配器，模型必须支持工具调用。只设置模型名称不代表账号有访问权限。
然后在控制台创建真实来源项目，使用公共 HTTP(S) HTML 或纯文本 URL，选择“真实模式”。
当前没有联网搜索引擎和动态浏览器采集器；真实模式在配置的来源与项目历史内定向研究。
JavaScript 动态页面、PDF、付费内容或登录页面可能无法提取，需后续采集器适配。

来源限制：最多 8 个 URL；默认最多 1 MB/页、24,000 字符/页，3 个抓取并发，20 秒请求超时，
最多 5 次重定向。每次重定向都会重新检查地址。生产环境还需网络层出站策略处理 DNS 重绑定；
应用 DNS 校验本身不是完整的网络隔离。不要把本服务部署为开放代理。

模型限制：默认 180 秒总研究超时、24 个图步骤 recursion limit、模型请求最多重试 1 次。
这些是执行约束，当前没有人民币或美元额度的硬性拦截。

Deep Agents 只得到证据查询工具、项目历史检索和内存中的技能文件副本。
它无法通过该虚拟文件系统读取宿主 `.env` 或执行 shell。引用检查只核对 ID 与引句原文一致性，
不能替代对事实、时效性、证据支持程度和影响判断的人工复核。

## 3. 任务中断、取消与恢复

- 同一项目最多一个 queued/running 任务；不同项目按 `MAX_CONCURRENT_RUNS` 限制并发。
- 服务启动把旧 queued/running 记录标记为 interrupted，需要用户手动点“恢复”。
- failed/interrupted/cancelled 可手动恢复，completed 或仍在执行的任务返回冲突。
- 恢复沿用 run ID 作为 LangGraph thread ID；已完成的图节点从数据库检查点加载。
- 若尚无检查点，重新进入图，已存在的本次快照按 ID 复用，避免重复采集入库。
- 恢复粒度为顶层图节点。research 节点内部的模型循环没有独立检查点，中断后可能重新调用模型并产生费用。
- 新任务使用创建时保存的配置快照。旧任务缺少快照且项目已修改时不可恢复，需要新建任务。
- 快照 ID 按 run ID 与 URL 确定；报告存放在同一运行记录中。事件日志可记录重试尝试。
- 不要同时启动多个 app worker，也不要让多个本地实例共同管理同一套任务数据库。

## 4. PostgreSQL 与 pgvector

本地有 Docker daemon 时：

```bash
docker compose up --build
```

Compose 提供 PostgreSQL 17 + pgvector 镜像、应用服务与持久卷，默认仍为无密钥演示。
本地数据库密码默认 `signaldesk-local-only`，仅适合开发；可在 `.env` 设置 `POSTGRES_PASSWORD`。
数据库与应用映射端口均绑定 127.0.0.1。换成含特殊字符密码时，连接 URL 中应使用正确的 URL 编码。
`docker compose down` 会停止容器；不加 `-v` 会保留数据库卷。

也可直接设置：

```dotenv
DATABASE_URL=postgresql+asyncpg://用户:密码@localhost:5432/signaldesk
CHECKPOINT_URL=postgresql://用户:密码@localhost:5432/signaldesk
MEMORY_URL=postgresql://用户:密码@localhost:5432/signaldesk
```

业务库使用 SQLAlchemy/asyncpg，图检查点与官方 AsyncPostgresStore 使用 Psycopg。首次启动会创建初始表；
现有数据库的结构升级需要迁移脚本，V0.1 尚未引入 Alembic。升级前先备份。

可选向量检索：

```dotenv
ENABLE_VECTOR_SEARCH=true
EMBEDDING_MODEL=你可用的嵌入模型名
EMBEDDING_DIMENSIONS=1536
```

需要提供商支持配置的 dimensions 参数、有效 `OPENAI_API_KEY`，以及数据库 vector 扩展权限。
应用创建 `evidence_embeddings` 表；只为真实任务中首次或发生变化的来源建立向量，嵌入和查询会产生服务费用。
默认开启 `ENABLE_FULLTEXT_SEARCH=true`：PostgreSQL 使用 simple 全文检索与 GIN 索引，SQLite 使用 FTS5。
词法、全文和可选向量候选按 RRF 融合。默认 `RERANKER_PROVIDER=lexical` 是确定性基线，
不是神经重排模型；设为 none 可直接保留 RRF 顺序。中文分词与大规模性能尚未专项评测。
改变嵌入模型或维度时需要重建索引和嵌入，不能直接复用旧向量。
向量索引异常会使任务失败并保留可恢复状态，不会默默丢弃结果。

### 模型重排服务

```dotenv
RERANKER_PROVIDER=http
RERANKER_URL=https://你的重排服务/v2/rerank
RERANKER_MODEL=服务支持的模型名
RERANKER_API_KEY=填写服务密钥
RERANKER_TIMEOUT_SECONDS=20
```

接口采用 Cohere 风格 JSON：发送 model、query、documents、top_n，接收 results[].index/relevance_score。
应用校验索引范围、重复项和有限数字；配置不足或服务失败返回明确错误。URL 必须指向可信服务，
候选原文会发给该服务。当前已用受控 HTTP transport 验证协议，尚未评测真实服务的相关性提升。

### 项目记忆

默认 `MEMORY_URL=sqlite:///./data/memory.db`，PostgreSQL 可与业务库和检查点共用服务。
界面“项目记忆”可新增、编辑、移除明确偏好；系统独立保存最近运行摘要。
collect 开始时冻结记忆，运行期间修改的偏好用于下一次运行，恢复沿用原上下文。
默认每类保留 50 项（`MEMORY_MAX_ITEMS`），单个偏好值不超过 8KB；摘要不是事实认证。
升级备份需同时保留业务库、checkpointer 与 Store；SQLite 三个文件应在服务停止后一起备份。

## 5. MCP

### 对外提供只读查询

先启动一次应用创建数据库，再从同一项目目录运行：

```bash
uv run signaldesk-mcp
```

这是 stdio 服务，提供 `list_projects`、`get_report`、`search_evidence`。
查询不会启动调研、不调用模型；MCP 历史搜索使用词法基线。连接者具有本地库的读取能力，
只连接可信客户端。当前没有远程 HTTP MCP 公开端点。

### 接入外部工具

```dotenv
MCP_URLS=["https://你信任的工具服务/mcp"]
MCP_ALLOWED_TOOLS=["search_documents"]
```

真实模式仅将显式允许的工具交给 Agent；白名单应仅包含只读工具。工具名将加 `mcp_0_` 等前缀。
远端工具的权限语义由运营者确认，当前应用不能证明第三方工具真的无副作用。
当前 LangChain 的 `langchain.mcp` 为 Beta，项目通过 uv.lock 锁定依赖并隔离接入代码。

## 6. 认证与追踪

配置 `API_TOKEN` 后，除健康检查外的业务 API 要求 `Authorization: Bearer ...`。
控制台可在本次浏览器会话中填写 Token。静态资源和 API 文档不受 Token 保护，但不包含业务数据。
这属于单用户共享 Token 机制，不是团队权限或多租户系统。

LangSmith 可选：

```dotenv
LANGSMITH_TRACING=true
LANGSMITH_API_KEY=填写你的密钥
LANGSMITH_PROJECT=signaldesk
```

仅在明确同意向该服务发送来源内容、模型消息和运行元数据时开启。
未开启也能运行。追踪 SDK 提供能力，云端接收与展示需要账号和实际验收。

## 7. 验证命令与边界

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
node --check src/signaldesk/static/app.js
uv lock --check
uv run signaldesk-eval --output data/evaluation.json
```

测试覆盖本地 SQLite、API、demo 全流程、变更与无变更、应用重启、节点级检查点恢复、
来源限制、引用拒绝、Deep Agents 实图与脚本模型、Skills 读取、只读 MCP 以及项目历史隔离。
脚本模型只用于验证真实框架的调用接口，不代表真实模型效果。

完整 Python 回归265项通过，前端行为测试4项通过。实际PostgreSQL测试6项、实际Docker测试3项，其中一项PG测试结合Docker；按环境变量明确启用，缺少条件会跳过。
实际数据库测试覆盖 GIN 全文、向量、Store、checkpointer 和应用整体；嵌入输入是确定性夹具，不调用供应商。
Chrome 已验证 SSE 续传、项目切换、记忆 CRUD 和服务重启持久化。

```bash
# 仅对你授权的测试库执行；测试创建随机 schema 并只清理自己的 schema
SIGNALDESK_TEST_POSTGRES_URL=postgresql://用户:密码@localhost:5432/测试库 uv run pytest tests/test_postgres_integration.py -q
```

本次已实际运行Docker隔离及双实例A2A，已构建应用镜像并在镜像内跑通离线评测。未使用真实模型、嵌入、模型重排、外部MCP或LangSmith密钥；真实质量与云端实验/追踪仍需验收。

V0.4 公网采集冒烟遇到过代理 fake-IP DNS：example.com 被解析到 198.18.0.0/15 保留段，
采集器按非公网地址拒绝。若你的代理也使用 fake-IP DNS，应在受控环境中改用真实公网解析，
再验证采集；不要通过允许内网/保留地址来绕过限制。


## 8. 离线评测与 LangSmith

```bash
uv run signaldesk-eval --output data/evaluation.json
# 仅在显式需要上传夹具指标时执行；密钥需导出到环境变量
export LANGSMITH_API_KEY=你的密钥
uv run signaldesk-eval --langsmith --output data/evaluation-langsmith.json
```

默认 CLI 不构造云端 client、不上传，即使环境中开启了 tracing，离线场景仍关闭追踪。
当前 9 个固定场景、55 项期望；失败返回非零退出码。评测不使用真实模型，不能代替事实支持率人工评审。
`--langsmith` 创建固定场景数据集/实验，只上传允许的数值/布尔指标与夹具 ID，不上传业务正文或密钥。
真实研究的 LangSmith trace 则可能包含模型消息与来源内容，需单独确认数据授权。

## 9. AG-UI 与能力状态

任务创建保持原 REST API；订阅 `GET /api/runs/{run_id}/ag-ui?after=0`，Content-Type 为 text/event-stream。
配置 token 后需发送 Bearer，原生 EventSource 不能设请求头，前端使用 fetch 流并支持 AbortController。
`GET /api/capabilities` 可查看本实例 Store、全文、向量与 Reranker 状态，不做付费外部探测。
断开订阅不取消任务；后台任务取消需专门调用 cancel。当前不是通用 AG-UI RunAgentInput 执行端点。


## 10. Sandbox 与 A2A

新能力默认关闭。按 [SANDBOX.md](./SANDBOX.md) 预先启动Docker、拉取固定镜像，然后设置SANDBOX_ENABLED=true。
按 [A2A.md](./A2A.md) 设置ENABLE_A2A、准确的A2A_PUBLIC_URL及远端alias/token/project映射。
控制台「扩展能力」显示实际可用性，Python代码可读取input_data、赋result返回JSON；远端委托只面向已有项目。

```bash
# 完整外部依赖验证，仅使用自己授权的测试库；测试自动清理随机schema和自己的容器
SIGNALDESK_TEST_DOCKER=1 SIGNALDESK_TEST_POSTGRES_URL=postgresql://用户:密码@localhost:5432/测试库 uv run pytest -q
```

Compose不挂宿主Docker socket。应用容器内默认不会获得本机Docker执行权限，不能仅传SANDBOX_ENABLED=true便声称已配置完成。
双实例 A2A 可按 [A2A.md](./A2A.md) 分别启动本地服务与远端研究服务，使用不同端口和独立的业务库、checkpointer、Store。
在远端创建项目后，将其地址、token 和项目 ID 配置到本地 alias 映射；两端的配置分别保存，不共享同一套任务数据库。

若源码目录含 AppleDouble `._*` 文件或不兼容的扩展属性，Docker context sender 可能在加载 `.dockerignore` 前报 xattr 错误。
可从干净的 Git checkout 构建，或使用不含这些附加文件与扩展属性的源码副本；不要将 `data/` 或 `.env` 复制到构建上下文。
