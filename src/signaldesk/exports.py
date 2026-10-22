"""Downloadable reports with inert Markdown text and safe attachment names."""

import html
import json
import re
from urllib.parse import quote, urlsplit

from pydantic import ValidationError

from signaldesk.schemas import FinalReport, RunRead


class ExportUnavailable(ValueError):
    """The run has no complete, verified report to export."""


def _text(value: object) -> str:
    # Escape HTML and Markdown syntax together so external page/model content
    # stays text even in renderers that permit raw HTML, images, and links.
    escaped = html.escape(str(value), quote=True)
    return re.sub(r"([\\`*_{}\[\]()#+.!|>~\-])", r"\\\1", escaped)


def _link(label: str, url: str) -> str:
    try:
        parts = urlsplit(url)
        valid = (
            parts.scheme.lower() in {"http", "https"}
            and parts.hostname
            and parts.username is None
            and parts.password is None
            and not any(ord(char) < 33 or ord(char) == 127 for char in url)
        )
        # Accessing port also validates malformed port declarations.
        _ = parts.port
    except ValueError:
        valid = False
    if not valid:
        return f"{_text(label)}（已省略无效或非 HTTP(S) 链接）"
    encoded = quote(url, safe=":/?#@!$&'+,;=%-._~")
    return f"[{_text(label)}]({encoded})"


def _quote(value: str) -> str:
    return "\n".join(f"> {_text(line)}" for line in value.splitlines())


def _ready(run: RunRead) -> FinalReport:
    if run.status != "completed" or run.report is None:
        raise ExportUnavailable("仅已完成且包含完整报告的任务可以导出。")
    try:
        report = FinalReport.model_validate(run.report)
    except ValidationError as exc:
        raise ExportUnavailable("任务报告不完整，无法导出。") from exc
    evidence = {item.id: item for item in report.evidence}
    if report.mode != run.mode or len(evidence) != len(report.evidence):
        raise ExportUnavailable("任务报告的模式或证据不一致，无法导出。")
    for finding in report.findings:
        for citation in finding.citations:
            item = evidence.get(citation.evidence_id)
            normalized = " ".join(citation.quote.split())
            if item is None or not normalized or normalized not in " ".join(item.text.split()):
                raise ExportUnavailable("任务报告缺少一致的引用证据，无法导出。")
    return report


def attachment_filename(run_id: str, extension: str) -> str:
    identifier = re.sub(r"[^a-zA-Z0-9_-]", "", run_id)[:80] or "report"
    suffix = "md" if extension == "markdown" else "json"
    return f"signaldesk-{identifier}.{suffix}"


def export_json(run: RunRead) -> str:
    """Preserve the report structure inside its run metadata envelope."""
    _ready(run)
    return json.dumps(run.model_dump(mode="json"), ensure_ascii=False, indent=2)


def export_markdown(run: RunRead) -> str:
    report = _ready(run)
    indexed = {item.id: item for item in report.evidence}
    lines = [
        f"# {_text(report.title)}",
        "",
        f"- 任务：{_text(run.id)}",
        f"- 模式：{'演示（demo）' if run.mode == 'demo' else '真实调研（live）'}",
        f"- 创建时间：{_text(run.created_at)}",
        f"- 完成更新时间：{_text(run.updated_at)}",
        "",
        "## 摘要",
        "",
        _text(report.summary),
        "",
        "## 发现",
        "",
    ]
    if not report.findings:
        lines.extend(["本次未形成新的发现。", ""])
    for number, finding in enumerate(report.findings, start=1):
        lines.extend(
            [
                f"### {number}. {_text(finding.title)}",
                "",
                f"分类：{_text(finding.category)}；影响：{_text(finding.impact)}",
                "",
                _text(finding.detail),
                "",
                "引用：",
                "",
            ]
        )
        for citation in finding.citations:
            item = indexed[citation.evidence_id]
            lines.extend(
                [
                    f"- {_link(item.source_name, item.url)} · {'历史' if item.is_previous else '当前'}证据：{_text(item.id)}",
                    "",
                    _quote(citation.quote),
                    "",
                ]
            )
    for title, values in (("建议", report.recommendations), ("局限", report.limitations)):
        lines.extend([f"## {title}", ""])
        lines.extend([f"- {_text(value)}" for value in values] or ["无。"])
        lines.append("")
    lines.extend(["## 来源证据与差异", ""])
    for item in report.evidence:
        lines.extend(
            [
                f"### {_text(item.source_name)}{'（历史快照）' if item.is_previous else ''}",
                "",
                f"- 来源：{_link(item.title, item.url)}",
                f"- 证据 ID：{_text(item.id)}",
                f"- 采集时间：{_text(item.observed_at or '未记录')}",
                f"- 变化类型：{_text(item.change_type)}",
            ]
        )
        if item.previous_id:
            lines.append(f"- 上次证据 ID：{_text(item.previous_id)}")
        lines.extend(["", "原文：", "", _quote(item.text), ""])
        if item.diff:
            lines.extend(["原文差异：", "", _quote(item.diff), ""])
    return "\n".join(lines).rstrip() + "\n"
