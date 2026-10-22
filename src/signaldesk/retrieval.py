"""Scoped lexical + full-text + pgvector recall, RRF fusion, then reranking."""

import json
import re
from datetime import UTC, datetime

import httpx
from langchain_openai import OpenAIEmbeddings
from sqlalchemy import text

from signaldesk.config import Settings
from signaldesk.repository import Repository
from signaldesk.reranking import Reranker
from signaldesk.schemas import SearchHit, SnapshotRecord

LATEST = """SELECT s.*, row_number() OVER (
    PARTITION BY s.url ORDER BY s.created_at DESC, s.id DESC) AS rn
    FROM snapshots s JOIN runs r ON r.id=s.run_id
    WHERE s.project_id=:project_id AND r.status='completed'"""


def _snapshot(row) -> SnapshotRecord:
    data = dict(row)
    value = data["created_at"]
    if isinstance(value, datetime):
        value = value.replace(tzinfo=UTC) if value.tzinfo is None else value
        data["created_at"] = value.isoformat()
    return SnapshotRecord.model_validate(data)


class Retriever:
    def __init__(self, repo: Repository, settings: Settings):
        self.repo, self.settings = repo, settings
        self.embeddings = None
        self.fulltext_ready = False
        self.reranker = Reranker(settings)
        self._http_client = self._http_async_client = None

    async def initialize(self) -> None:
        if self.settings.enable_fulltext_search:
            await self._initialize_fulltext()
        if not self.settings.enable_vector_search:
            return
        if self.repo.engine.dialect.name != "postgresql":
            raise ValueError("ENABLE_VECTOR_SEARCH 需要 PostgreSQL/pgvector。")
        if not self.settings.embedding_model or not self.settings.openai_api_key:
            raise ValueError("向量检索需要 EMBEDDING_MODEL 和 OPENAI_API_KEY。")
        self._http_client = httpx.Client(timeout=self.settings.request_timeout_seconds)
        self._http_async_client = httpx.AsyncClient(timeout=self.settings.request_timeout_seconds)
        self.embeddings = OpenAIEmbeddings(
            model=self.settings.embedding_model,
            dimensions=self.settings.embedding_dimensions,
            api_key=self.settings.openai_api_key,
            base_url=self.settings.openai_base_url,
            http_client=self._http_client,
            http_async_client=self._http_async_client,
        )
        async with self.repo.engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            await conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS evidence_embeddings ("
                    "snapshot_id VARCHAR(120) PRIMARY KEY REFERENCES snapshots(id) ON DELETE CASCADE, "
                    "project_id VARCHAR(36) NOT NULL, "
                    f"embedding vector({self.settings.embedding_dimensions}) NOT NULL)"
                )
            )

    async def _initialize_fulltext(self):
        async with self.repo.engine.begin() as conn:
            if self.repo.engine.dialect.name == "postgresql":
                await conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_snapshots_fulltext ON snapshots "
                        "USING GIN (to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(text,'')))"
                    )
                )
            elif self.repo.engine.dialect.name == "sqlite":
                await conn.execute(
                    text(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS snapshot_fts "
                        "USING fts5(snapshot_id UNINDEXED, title, body, tokenize='unicode61')"
                    )
                )
                for command in [
                    "CREATE TRIGGER IF NOT EXISTS snapshots_fts_insert AFTER INSERT ON snapshots BEGIN "
                    "INSERT INTO snapshot_fts(snapshot_id,title,body) VALUES(new.id,new.title,new.text); END",
                    "CREATE TRIGGER IF NOT EXISTS snapshots_fts_delete AFTER DELETE ON snapshots BEGIN "
                    "DELETE FROM snapshot_fts WHERE snapshot_id=old.id; END",
                    "CREATE TRIGGER IF NOT EXISTS snapshots_fts_update AFTER UPDATE ON snapshots BEGIN "
                    "DELETE FROM snapshot_fts WHERE snapshot_id=old.id; "
                    "INSERT INTO snapshot_fts(snapshot_id,title,body) VALUES(new.id,new.title,new.text); END",
                    "INSERT INTO snapshot_fts(snapshot_id,title,body) SELECT s.id,s.title,s.text FROM snapshots s "
                    "WHERE NOT EXISTS(SELECT 1 FROM snapshot_fts f WHERE f.snapshot_id=s.id)",
                ]:
                    await conn.execute(text(command))
            else:
                raise ValueError("全文检索仅支持 SQLite FTS5 或 PostgreSQL。")
        self.fulltext_ready = True

    async def close(self) -> None:
        await self.reranker.close()
        if self._http_async_client is not None:
            await self._http_async_client.aclose()
        if self._http_client is not None:
            self._http_client.close()

    async def index_snapshots(self, snapshots: list[SnapshotRecord]) -> None:
        if self.embeddings is None or not snapshots:
            return
        vectors = await self.embeddings.aembed_documents(
            [f"{s.title}\n{s.text[:12000]}" for s in snapshots]
        )
        async with self.repo.engine.begin() as conn:
            for snapshot, vector in zip(snapshots, vectors, strict=True):
                await conn.execute(
                    text(
                        "INSERT INTO evidence_embeddings(snapshot_id, project_id, embedding) "
                        "VALUES (:id,:project_id,CAST(:embedding AS vector)) "
                        "ON CONFLICT(snapshot_id) DO UPDATE SET embedding=EXCLUDED.embedding"
                    ),
                    {
                        "id": snapshot.id,
                        "project_id": snapshot.project_id,
                        "embedding": json.dumps(vector),
                    },
                )

    async def _fulltext(self, project_id: str, query: str, limit: int) -> list[SnapshotRecord]:
        if not self.fulltext_ready:
            return []
        if self.repo.engine.dialect.name == "sqlite":
            terms = re.findall(r"\w+", query, re.UNICODE)[:20]
            if not terms:
                return []
            query = " OR ".join('"' + term.replace('"', '""') + '"' for term in terms)
            sql = f"""WITH latest AS ({LATEST}) SELECT l.* FROM latest l
                JOIN snapshot_fts ON snapshot_fts.snapshot_id=l.id
                WHERE l.rn=1 AND snapshot_fts MATCH :query
                ORDER BY bm25(snapshot_fts), l.created_at DESC LIMIT :limit"""
        else:
            sql = f"""WITH latest AS ({LATEST}) SELECT l.* FROM latest l
                WHERE l.rn=1 AND to_tsvector('simple',coalesce(l.title,'') || ' ' || coalesce(l.text,''))
                @@ websearch_to_tsquery('simple',:query)
                ORDER BY ts_rank_cd(to_tsvector('simple',coalesce(l.title,'') || ' ' || coalesce(l.text,'')),
                websearch_to_tsquery('simple',:query)) DESC,l.created_at DESC LIMIT :limit"""
        async with self.repo.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(sql), {"project_id": project_id, "query": query, "limit": limit}
                    )
                )
                .mappings()
                .all()
            )
        return [_snapshot(row) for row in rows]

    async def _vector(self, project_id: str, query: str, limit: int) -> list[SnapshotRecord]:
        if self.embeddings is None:
            return []
        async with self.repo.engine.connect() as conn:
            count = await conn.scalar(
                text(
                    "SELECT count(*) FROM evidence_embeddings e JOIN snapshots s ON s.id=e.snapshot_id "
                    "JOIN runs r ON r.id=s.run_id WHERE e.project_id=:project_id AND r.status='completed'"
                ),
                {"project_id": project_id},
            )
        if not count:
            return []
        vector = await self.embeddings.aembed_query(query)
        # Reuse an older embedding only when its content hash still matches the
        # latest successful snapshot. Never return superseded source contents.
        sql = f"""WITH latest AS ({LATEST}) SELECT l.* FROM latest l
            JOIN LATERAL (SELECT e.embedding FROM evidence_embeddings e
                JOIN snapshots old ON old.id=e.snapshot_id JOIN runs r ON r.id=old.run_id
                WHERE e.project_id=l.project_id AND old.url=l.url AND old.content_hash=l.content_hash
                AND r.status='completed' ORDER BY old.created_at DESC LIMIT 1) v ON true
            WHERE l.rn=1 ORDER BY v.embedding <=> CAST(:embedding AS vector) LIMIT :limit"""
        async with self.repo.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(sql),
                        {"project_id": project_id, "embedding": json.dumps(vector), "limit": limit},
                    )
                )
                .mappings()
                .all()
            )
        return [_snapshot(row) for row in rows]

    async def search(self, project_id: str, query: str, limit: int = 5) -> list[SearchHit]:
        limit = max(1, min(limit, 20))
        count = self.settings.retrieval_candidates
        recalls = {"lexical": await self.repo.search_snapshots(project_id, query, limit=count)}
        if self.fulltext_ready:
            recalls["fulltext"] = await self._fulltext(project_id, query, count)
        if self.embeddings is not None:
            recalls["vector"] = await self._vector(project_id, query, count)
        candidates, scores, methods = {}, {}, {}
        for method, values in recalls.items():
            for rank, item in enumerate(values, start=1):
                candidates[item.id] = item
                scores[item.id] = scores.get(item.id, 0) + 1 / (60 + rank)
                methods.setdefault(item.id, []).append(method)
        strategy = "hybrid_rrf" if len(recalls) > 1 else "lexical"
        hits = []
        for item_id in sorted(scores, key=lambda key: scores[key], reverse=True)[:count]:
            item = candidates[item_id]
            hits.append(
                SearchHit(
                    evidence_id=item.id,
                    source_name=item.source_name,
                    url=item.url,
                    title=item.title,
                    text=item.text[:4000],
                    score=scores[item_id],
                    strategy=strategy,
                    retrieval_methods=methods[item_id],
                    created_at=item.created_at,
                )
            )
        return await self.reranker.rank(query, hits, limit)
