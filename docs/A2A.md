# A2A · 跨实例项目研究委托（V0.4）

使用官方 `a2a-sdk[http-server]==1.2.2`。原生协议为 A2A 1.0 JSON-RPC，启用 SDK 官方 0.3 兼容层。
当前能力是对**远端已经存在的项目**发起研究、查询和取消，不接收任意 prompt、来源 URL 或宿主命令。

## 1. 启用服务

```dotenv
ENABLE_A2A=true
A2A_PUBLIC_URL=http://127.0.0.1:8000/a2a/
API_TOKEN=为本实例设置共享token
A2A_TIMEOUT_SECONDS=180
```

Card 发布 `A2A_PUBLIC_URL`，不会根据不可信 Host 头拼接地址。它必须与客户端白名单中的实际地址一致。

| 入口 | 行为 |
| --- | --- |
| `GET /.well-known/agent-card.json` | 根路径 Agent Card |
| `GET /a2a/.well-known/agent-card.json` | 挂载路径 Agent Card |
| `POST /a2a/` | 官方 SDK JSON-RPC；1.0 的 `SendMessage`、`GetTask`、`CancelTask` |
| 同一 RPC | SDK 0.3 兼容的 `message/send`、`tasks/get`、`tasks/cancel` |

配置 token 后 Card 和 RPC 都要求 `Authorization: Bearer ...`。当前是单用户共享 token，不提供多租户身份。
不支持 streaming、push notifications、租户、任务追加消息或通用跨 Agent 规划；Card 如实声明。

## 2. 管理员允许远端

先在另一个实例创建项目并复制其 project ID，再在本地配置：

```dotenv
ENABLE_A2A=true
A2A_REMOTE_AGENTS={"researcher":"http://127.0.0.1:8001/a2a/"}
A2A_REMOTE_TOKENS={"researcher":"远端实例的token"}
A2A_REMOTE_PROJECTS={"researcher":"远端项目ID"}
```

本地 API_TOKEN 与 remote token 分开。前端只选 alias，不接受任意网络 URL。
客户端只接受 Card 中与已配置地址完全一致的 JSON-RPC 1.0 接口；禁止重定向和跨地址选择。
HTTP 响应在 SDK JSON 解析前限制为 2 MiB，并要求 identity 编码，过大报告明确报错。
同一应用内的 LangGraph 节点不走 A2A；只有实际 HTTP 委托才属于这条路径。

`A2A_REMOTE_PROJECTS` 是 Deep Agent 自动委托的固定目标映射。未映射 alias 不会获得自动委托工具；
手动控制台可以填写远端项目 ID，远端仍须验证项目存在与其 token 权限。

## 3. 本地业务 API

这些接口与原有业务 API 共用本地 Bearer 认证：

| 方法 | 路径 | 内容 |
| --- | --- | --- |
| POST | `/api/projects/{local_project_id}/a2a/delegations` | `{remote,remote_project_id,message_id,mode,demo_revision}`，成功 202 |
| GET | `/api/projects/{local_project_id}/a2a/delegations` | 本地登记历史；limit 1–200、offset；不访问远端 |
| GET | `/api/a2a/delegations/{delegation_id}` | 读取登记并查询对应远端 Task |
| POST | `/api/a2a/delegations/{delegation_id}/cancel` | 查询对应 Task 后请求取消；已结束返回 409 |

返回 `id/project_id/run_id/remote/remote_project_id/status/task`。
本地 delegation ID 与远端 task ID 不同；不能拿任意远端 task ID 调用本地查询/取消。
`task` 是官方 1.0 Task JSON，完成报告位于 `artifacts[].parts[].data`。
历史列表可以包含 `PENDING/task=null`，表示请求登记了但未拿到远端任务 ID。

前端路径：选择项目 → 展开「扩展能力」→ A2A 选择 alias、远端项目和模式。
JSON 轮询展示远端状态；关闭页面或切换项目只中止观察，不自动取消远端任务。

## 4. 原始 1.0 请求样例

```json
{
  "jsonrpc": "2.0",
  "id": "rpc-1",
  "method": "SendMessage",
  "params": {
    "message": {
      "messageId": "为本次请求生成的UUID",
      "contextId": "远端项目ID",
      "role": "ROLE_USER",
      "parts": [{
        "mediaType": "application/json",
        "data": {"project_id": "远端项目ID", "mode": "demo", "demo_revision": 1}
      }]
    },
    "configuration": {"returnImmediately": true}
  }
}
```

向远端 `/a2a/` POST，带 `Content-Type: application/json`、`A2A-Version: 1.0` 和所需 Bearer。
任务 ID 等于远端业务 run ID，contextId 等于远端 project ID。只能查询 A2A 创建的任务。

## 5. 幂等、取消与恢复边界

- `message_id` 在实例内持久唯一，参数指纹必须一致；复用不同参数被拒绝。同参数重试映射原 run。
- 新 run、配置快照、`a2a_origin` 与幂等绑定在同一事务内创建，随后才提交本地 worker。
- 本地委托登记也持久化；刷新后可查看历史。PENDING 需要用原 message_id 和参数重试确认，不能编造远端 Task ID。
- 断网/响应丢失不能保证远端已取消。高层 `delegate_wait` 超时或被取消会尝试有界清理；发送响应完全丢失时可能只留下 PENDING，需核对远端。
- 只有 `TASK_STATE_COMPLETED` 是研究成功；FAILED/CANCELED/REJECTED/INPUT_REQUIRED/AUTH_REQUIRED 等不会当成成功报告。
- 完成任务不可取消；GetTask 与 CancelTask 之间完成的竞态也返回安全 409。
- 远端进程重启后运行转 interrupted，可经远端业务 API 手动恢复；不会自动重新创建任务。
- 入站任务保存 `a2a_origin`，Deep Agent 和高层委托器都禁止继续自动委托，防止递归链。
- 远端报告仅作背景，最终发现仍须引用当前本地证据清单。研究工具会截断过长远端上下文并标明截断。

## 6. 实测与官方资料

真实两个 Uvicorn 实例、独立数据库、官方客户端已完成 demo 五节点图并返回报告 artifact；另覆盖鉴权、
重试、事务回滚、跨项目/跨地址拒绝、响应限长、legacy 错误脱敏、超时、取消与已完成取消冲突。
这证明协议和执行路径，不证明真实模型研究质量；演示报告仍标为虚构夹具。

- [官方 SDK v1.2.2](https://github.com/a2aproject/a2a-python/releases/tag/v1.2.2)
- [官方 1.0 迁移说明](https://github.com/a2aproject/a2a-python/blob/v1.2.2/docs/migrations/v1_0/README.md)
