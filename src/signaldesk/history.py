"""Project-bound keyset cursors for immutable extension creation order."""

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import and_, or_

T = TypeVar("T")


class HistoryPage(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None
    has_more: bool


class HistoryCursorError(ValueError):
    pass


@dataclass(frozen=True)
class HistoryPosition:
    created_at: datetime | None
    id: str


def utc_date(value: str | datetime) -> datetime:
    date = datetime.fromisoformat(value) if isinstance(value, str) else value
    if date.tzinfo is None:
        date = date.replace(tzinfo=UTC)
    return date.astimezone(UTC)


def decode_cursor(token: str | None, *, project_id: str, resource: str) -> HistoryPosition | None:
    if token is None:
        return None
    try:
        if not 1 <= len(token) <= 512:
            raise ValueError
        raw = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
        value = json.loads(raw)
        if (
            not isinstance(value, dict)
            or set(value) != {"v", "project", "resource", "at", "id"}
            or type(value["v"]) is not int
            or value["v"] != 1
            or value["project"] != project_id
            or value["resource"] != resource
            or not isinstance(value["id"], str)
            or str(UUID(value["id"])) != value["id"]
        ):
            raise ValueError
        at = value["at"]
        if at is not None:
            if not isinstance(at, str) or len(at) > 64:
                raise ValueError
            date = datetime.fromisoformat(at)
            if date.tzinfo is None:
                raise ValueError
            at = date.astimezone(UTC)
        return HistoryPosition(at, value["id"])
    except (
        ValueError,
        TypeError,
        KeyError,
        binascii.Error,
        UnicodeError,
        RecursionError,
        OverflowError,
    ) as exc:
        raise HistoryCursorError("历史游标无效，或不属于当前项目及记录类型；请刷新历史。") from exc


def before_position(created_at, identifier, position: HistoryPosition):
    if position.created_at is None:
        return and_(created_at.is_(None), identifier < position.id)
    return or_(
        created_at < position.created_at,
        and_(created_at == position.created_at, identifier < position.id),
        created_at.is_(None),
    )


def history_page(items: list[dict], *, limit: int, project_id: str, resource: str) -> dict:
    more = len(items) > limit
    items = items[:limit]
    cursor = None
    if more:
        last = items[-1]
        value = {
            "v": 1,
            "project": project_id,
            "resource": resource,
            "at": utc_date(last["created_at"]).isoformat() if last["created_at"] else None,
            "id": last["id"],
        }
        cursor = (
            base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode())
            .decode()
            .rstrip("=")
        )
    return {"items": items, "next_cursor": cursor, "has_more": more}
