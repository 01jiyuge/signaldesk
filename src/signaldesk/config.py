"""Environment configuration. No model calls occur when running in demo mode."""

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SKILLS = PROJECT_ROOT / "skills"
if not DEFAULT_SKILLS.is_dir():
    DEFAULT_SKILLS = Path(__file__).parent / "application_skills"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./data/signaldesk.db"
    checkpoint_url: str = "sqlite:///./data/checkpoints.db"
    memory_url: str = "sqlite:///./data/memory.db"
    memory_max_items: int = Field(default=50, ge=1, le=500)
    api_token: str = ""
    research_mode: str = "demo"
    model: str = ""
    openai_api_key: str = ""
    openai_base_url: str | None = None
    embedding_model: str = ""
    embedding_dimensions: int = Field(default=1536, ge=1, le=4096)
    enable_vector_search: bool = False
    enable_fulltext_search: bool = True
    retrieval_candidates: int = Field(default=30, ge=5, le=100)
    reranker_provider: Literal["none", "lexical", "http"] = "lexical"
    reranker_url: str = ""
    reranker_api_key: str = ""
    reranker_model: str = ""
    reranker_timeout_seconds: float = Field(default=20, ge=1, le=120)
    max_concurrent_runs: int = Field(default=2, ge=1, le=8)
    max_sources: int = Field(default=8, ge=1, le=20)
    max_source_bytes: int = Field(default=1_000_000, ge=1000, le=5_000_000)
    max_source_chars: int = Field(default=24_000, ge=1000, le=100_000)
    request_timeout_seconds: float = Field(default=20, ge=1, le=60)
    agent_timeout_seconds: float = Field(default=180, ge=10, le=600)
    agent_recursion_limit: int = Field(default=24, ge=4, le=80)
    mcp_urls: list[str] = Field(default_factory=list)
    mcp_allowed_tools: list[str] = Field(default_factory=list)
    sandbox_enabled: bool = False
    sandbox_image: str = "python:3.14-slim"
    sandbox_docker_binary: str = "docker"
    sandbox_timeout_seconds: float = Field(default=10.0, ge=0.1, le=60)
    sandbox_memory_mb: int = Field(default=128, ge=32, le=1024)
    sandbox_cpus: float = Field(default=0.5, ge=0.1, le=2)
    sandbox_pids_limit: int = Field(default=32, ge=16, le=256)
    sandbox_max_concurrent: int = Field(default=2, ge=1, le=8)
    sandbox_max_output_bytes: int = Field(default=65536, ge=1024, le=262144)
    sandbox_max_code_bytes: int = Field(default=20000, ge=1, le=100000)
    sandbox_max_input_bytes: int = Field(default=65536, ge=1, le=262144)
    enable_a2a: bool = False
    a2a_public_url: str = "http://127.0.0.1:8000/a2a/"
    a2a_remote_agents: dict[str, str] = Field(default_factory=dict)
    a2a_remote_tokens: dict[str, str] = Field(default_factory=dict)
    a2a_remote_projects: dict[str, str] = Field(default_factory=dict)
    a2a_timeout_seconds: float = Field(default=180, ge=1, le=600)
    skills_dir: Path = DEFAULT_SKILLS
    data_dir: Path = Path("data")

    @field_validator("openai_base_url", mode="before")
    @classmethod
    def empty_base_url_is_default(cls, value):
        return value or None

    def validate_live(self) -> None:
        if not self.model:
            raise ValueError("真实调研需要在 .env 设置 MODEL（模型提供商:模型名）。")
        # V0.1 ships an OpenAI-compatible provider. Other providers are future adapters.
        if not self.model.startswith("openai:"):
            raise ValueError("V0.1 真实模式支持 openai: 模型标识；其他提供商需安装适配器。")
        if not self.openai_api_key:
            raise ValueError("真实调研需要在 .env 设置 OPENAI_API_KEY。")


def get_settings() -> Settings:
    return Settings()
