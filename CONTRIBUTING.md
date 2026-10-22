# 参与开发

欢迎提交可复现的问题和聚焦单一改动的 Pull Request。先说明问题、预期行为与实际结果；
涉及真实模型时注明模型和运行模式，日志中移除 API key、token、私人来源正文和数据库连接凭据。

## 开发环境

需要 Python 3.11–3.14 和 uv。在克隆的项目目录运行：

```bash
cp .env.example .env
uv sync --frozen --extra dev
uv run signaldesk
```

默认 demo 不联网、不调用模型。不要提交 `.env`、运行数据库或业务报告。
Sandbox 和 A2A 的准备步骤见 [Sandbox](docs/SANDBOX.md) 与 [A2A](docs/A2A.md)。

## 提交前验证

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
uv run signaldesk-eval
node --check src/signaldesk/static/app.js
uv lock --check
```

默认测试会明确跳过需要真实 PostgreSQL / Docker 的场景。具备独立测试库、Docker 和预拉取镜像后，
可按 [运行文档](docs/OPERATIONS.md) 显式启用集成测试；不要使用生产数据库。
新增测试应验证实际行为与边界，不要以模拟供应商响应宣称真实模型质量通过。

修改 API、配置、权限范围、任务恢复或协议语义时，请同步对应文档；更新依赖时提交 `uv.lock`。
PR 说明包含问题、行为变化与实际验证，区分已经运行和未运行的检查。

## 许可证

项目采用 [MIT License](LICENSE)。提交贡献表示你同意按该许可证提供贡献。
