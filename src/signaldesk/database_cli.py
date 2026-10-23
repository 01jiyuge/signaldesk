"""Offline SQLite backup, verification, and restore commands."""

import argparse
import json
import math
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv
from pydantic import ValidationError

from signaldesk.backups import backup_sqlite, restore_backup, verify_backup
from signaldesk.config import Settings
from signaldesk.local_storage import StorageError


def _error(code: str, message: str, *, exit_code: int = 1) -> int:
    print(
        json.dumps({"status": "error", "code": code, "message": message}, ensure_ascii=False),
        file=sys.stderr,
    )
    return exit_code


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # argparse normally includes supplied values in its errors. Database
        # management errors must not echo an accidental URL or credential.
        self.exit(
            _error(
                "invalid_arguments",
                "命令参数无效；请使用 signaldesk-db --help 查看用法。",
                exit_code=2,
            )
        )


def _timeout(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout 必须是 1–600 秒的有限数值。") from exc
    if not math.isfinite(seconds) or not 1 <= seconds <= 600:
        raise argparse.ArgumentTypeError("timeout 必须是 1–600 秒的有限数值。")
    return seconds


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="signaldesk-db",
        description="离线备份、校验和恢复 SignalDesk 的三个本地 SQLite 数据库。",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    backup = commands.add_parser("backup", help="停服后将三个数据库备份到不存在的新目录。")
    backup.add_argument("--output", required=True, type=Path, help="备份包目标目录，必须不存在。")
    verify = commands.add_parser("verify", help="只校验备份包，不读取当前应用配置。")
    verify.add_argument("bundle", type=Path, help="需要校验的备份包目录。")
    restore = commands.add_parser("restore", help="校验后恢复到不存在的新目录，不覆盖当前数据库。")
    restore.add_argument("bundle", type=Path, help="需要恢复的备份包目录。")
    restore.add_argument(
        "--target-dir", required=True, type=Path, help="恢复目标目录，必须不存在。"
    )
    for command in (backup, verify, restore):
        command.add_argument(
            "--timeout", type=_timeout, default=60.0, help="操作时间预算（1–600 秒，默认 60）。"
        )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "backup":
            load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)
            result = backup_sqlite(Settings(), args.output, timeout_seconds=args.timeout)
        elif args.command == "verify":
            result = verify_backup(args.bundle, timeout_seconds=args.timeout)
        else:
            result = restore_backup(args.bundle, args.target_dir, timeout_seconds=args.timeout)
        print(
            json.dumps(
                {**result, "status": "ok", "command": args.command},
                ensure_ascii=False,
                allow_nan=False,
            )
        )
        return 0
    except StorageError as exc:
        return _error(exc.code, exc.message)
    except ValidationError:
        return _error(
            "configuration_invalid", "应用配置无效；请检查当前目录 .env 和环境变量中的配置项。"
        )
    except OSError:
        return _error("io_error", "无法完成文件操作；请检查文件权限、可用磁盘空间和目标目录。")
    except sqlite3.Error:
        return _error(
            "sqlite_error", "无法读取或写入 SQLite 数据库；请检查数据库完整性并确认服务已停止。"
        )
    except Exception:
        return _error("operation_failed", "数据库操作未完成；请检查备份包、配置和文件权限后重试。")


if __name__ == "__main__":
    raise SystemExit(main())
