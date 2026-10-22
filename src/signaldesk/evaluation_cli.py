"""CLI for local fixture quality evaluation and explicit optional experiment upload."""

import argparse
import asyncio
import contextlib
import json
import os
import sys
from pathlib import Path

from signaldesk.evaluation import evaluate_cases, upload_to_langsmith


def _write_report(output: str, report: dict) -> str:
    path = Path(output).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(path.resolve())


async def _run(args: argparse.Namespace) -> int:
    if args.langsmith and not os.environ.get("LANGSMITH_API_KEY", "").strip():
        print("显式上传需要先设置 LANGSMITH_API_KEY；未执行上传。", file=sys.stderr)
        return 2
    report = await evaluate_cases()
    upload_failed = False
    if args.langsmith:
        try:
            # SDK progress belongs on stderr; stdout remains one concise summary.
            with contextlib.redirect_stdout(sys.stderr):
                report["langsmith"] = await upload_to_langsmith(report)
        except Exception as exc:
            upload_failed = True
            report["langsmith"]["error_type"] = type(exc).__name__
            print("LangSmith上传未完成；离线结果仍有效，请检查连接与账号权限。", file=sys.stderr)
    output_path = (
        await asyncio.to_thread(_write_report, args.output, report) if args.output else None
    )
    summary = report["summary"]
    print(
        f"离线夹具质量评测：{summary['passed']}/{summary['scenarios']} 场景通过，"
        f"{summary['checks_passed']}/{summary['checks']} 项期望匹配。"
    )
    print("未评测远端模型效果；人工事实支持率尚未评测。")
    if output_path:
        print(f"JSON：{output_path}")
    if report["langsmith"].get("experiment_url"):
        print(f"LangSmith实验：{report['langsmith']['experiment_url']}")
    return 2 if upload_failed else (1 if summary["failed"] else 0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行固定夹具质量评测，默认离线且不上传。")
    parser.add_argument("--output", help="将完整场景、期望、观测与统计写入指定JSON路径。")
    parser.add_argument(
        "--langsmith", action="store_true", help="明确上传夹具指标并创建LangSmith实验；需API key。"
    )
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
