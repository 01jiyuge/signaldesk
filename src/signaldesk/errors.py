"""Safe, actionable task failures. Raw provider responses never reach the UI."""

from dataclasses import dataclass


class PublicTaskError(ValueError):
    """An application-authored message that is safe to show to the operator."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "task_error",
        retryable: bool = False,
        suggestion: str = "",
    ):
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.suggestion = suggestion


@dataclass(frozen=True)
class FailureDetail:
    message: str
    code: str
    retryable: bool
    suggestion: str


def public_failure(exc: Exception) -> FailureDetail:
    if isinstance(exc, PublicTaskError):
        return FailureDetail(str(exc), exc.code, exc.retryable, exc.suggestion)
    if isinstance(exc, TimeoutError):
        return FailureDetail(
            "研究超过本次运行时间限制。",
            "research_timeout",
            True,
            "可减少来源数量，或调整 AGENT_TIMEOUT_SECONDS 后恢复任务。",
        )
    # Match provider exception types, never copy messages containing credentials,
    # request headers, source bodies, or provider-internal diagnostics.
    name = type(exc).__name__
    if name in {"AuthenticationError", "PermissionDeniedError"}:
        return FailureDetail(
            "模型服务拒绝访问。",
            "model_access_denied",
            False,
            "检查服务端模型密钥、模型访问权限与 OPENAI_BASE_URL，再恢复任务。",
        )
    if name == "RateLimitError":
        return FailureDetail(
            "模型服务达到速率或额度限制。",
            "model_rate_limit",
            True,
            "检查账号额度并稍后恢复，避免连续重复提交。",
        )
    if name in {"APIConnectionError", "APITimeoutError"}:
        return FailureDetail(
            "暂时无法连接模型服务。",
            "model_connection_failed",
            True,
            "检查模型服务地址和网络连接后恢复任务。",
        )
    if name == "GraphRecursionError":
        return FailureDetail(
            "调研达到工具调用步骤上限。",
            "research_step_limit",
            False,
            "缩小研究范围，或检查工具返回后调整 AGENT_RECURSION_LIMIT。",
        )
    if name in {"OperationalError", "InterfaceError"}:
        return FailureDetail(
            "暂时无法访问数据库。",
            "database_unavailable",
            True,
            "检查数据库服务和连接配置；恢复连接后再继续任务。",
        )
    if name == "CitationError":
        return FailureDetail(
            "报告引用未通过原文一致性检查。",
            "citation_invalid",
            False,
            "核对来源原文后新建研究任务；直接恢复会再次校验同一份报告。",
        )
    return FailureDetail(
        "任务执行失败，请检查服务配置后重试。",
        "internal_error",
        False,
        "查看对应运行 ID 的服务端日志，排查后再恢复任务。",
    )
