"""Public contracts shared by HTTP, workflow, MCP, and the dashboard."""

from typing import Annotated, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from signaldesk.history import HistoryPage

Mode = Literal["demo", "live"]
Status = Literal["queued", "running", "interrupted", "completed", "failed", "cancelled"]


class SourceInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    name: str = Field(min_length=1, max_length=80)
    url: HttpUrl

    @field_validator("url")
    @classmethod
    def canonical_url(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password:
            raise ValueError("来源 URL 不可包含用户名或密码。")
        if value.port not in (80, 443):
            raise ValueError("来源仅支持标准 HTTP(S) 端口。")
        parts = urlsplit(str(value))
        return HttpUrl(urlunsplit(parts._replace(fragment="")))


class ProjectCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=2000)
    sources: list[SourceInput] = Field(min_length=1, max_length=8)
    focus: list[Annotated[str, Field(min_length=1, max_length=100)]] = Field(
        default_factory=lambda: ["pricing", "features"], min_length=1, max_length=8
    )

    @field_validator("sources")
    @classmethod
    def distinct_urls(cls, values: list[SourceInput]) -> list[SourceInput]:
        if len({str(value.url) for value in values}) != len(values):
            raise ValueError("来源 URL 不可重复。")
        return values


class ProjectRead(ProjectCreate):
    id: str
    created_at: str


class RunCreate(BaseModel):
    mode: Mode = "demo"
    demo_revision: Literal[1, 2] = 1


class RunRead(BaseModel):
    id: str
    project_id: str
    mode: Mode
    demo_revision: int
    status: Status
    stage: str
    error: str | None = None
    report: dict | None = None
    created_at: str
    updated_at: str


class RunSummaryRead(BaseModel):
    id: str
    project_id: str
    mode: Mode
    demo_revision: int
    status: Status
    stage: str
    error: str | None = None
    created_at: str
    updated_at: str


class RunHistoryPage(HistoryPage[RunSummaryRead]):
    active_run: RunSummaryRead | None


class EventRead(BaseModel):
    id: int
    run_id: str
    kind: str
    message: str
    data: dict = Field(default_factory=dict)
    created_at: str


class SnapshotRecord(BaseModel):
    id: str
    run_id: str
    project_id: str
    source_name: str
    url: str
    title: str
    text: str
    content_hash: str
    created_at: str


class Citation(BaseModel):
    evidence_id: str
    quote: str = Field(min_length=1, max_length=1000)


class Finding(BaseModel):
    title: str
    detail: str
    category: Literal["pricing", "features", "positioning", "other"] = "other"
    impact: Literal["high", "medium", "low"] = "low"
    citations: list[Citation] = Field(min_length=1)


class ResearchReport(BaseModel):
    title: str
    summary: str
    findings: list[Finding]
    recommendations: list[str]
    limitations: list[str]


class Evidence(BaseModel):
    id: str
    source_name: str
    url: str
    title: str
    text: str
    change_type: Literal["baseline", "changed", "unchanged"]
    previous_id: str | None = None
    diff: str = ""
    observed_at: str = ""
    is_previous: bool = False


class ReportStats(BaseModel):
    total_sources: int = Field(ge=0)
    collected_sources: int = Field(ge=0)
    failed_sources: int = Field(ge=0)
    baseline_sources: int = Field(ge=0)
    changed_sources: int = Field(ge=0)
    unchanged_sources: int = Field(ge=0)
    model_call_skipped: bool = True


class FinalReport(ResearchReport):
    evidence: list[Evidence]
    mode: Mode
    stats: ReportStats | None = None


class SearchHit(BaseModel):
    evidence_id: str
    source_name: str
    url: str
    title: str
    text: str
    score: float
    strategy: str
    retrieval_methods: list[str] = Field(default_factory=list)
    retrieval_score: float | None = None
    rerank_score: float | None = None
    created_at: str
