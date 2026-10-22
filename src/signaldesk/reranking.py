"""Explicit lexical baseline and Cohere-compatible neural reranking."""

import math
import re

import httpx

from signaldesk.config import Settings
from signaldesk.errors import PublicTaskError
from signaldesk.schemas import SearchHit


class Reranker:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self.settings, self.client = settings, client
        self._owns_client = client is None

    async def close(self):
        if self._owns_client and self.client is not None:
            await self.client.aclose()

    async def rank(self, query: str, hits: list[SearchHit], limit: int) -> list[SearchHit]:
        provider = self.settings.reranker_provider
        if not hits or provider == "none":
            return hits[:limit]
        if provider == "lexical":
            # A transparent baseline, not a semantic cross-encoder.
            terms = set(re.findall(r"\w+", query.casefold()))
            scored = []
            for hit in hits:
                body, title = hit.text.casefold(), hit.title.casefold()
                score = sum(2 * (term in title) + (term in body) for term in terms)
                score += 2 * (query.casefold() in body) if query.strip() else 0
                scored.append(
                    hit.model_copy(
                        update={
                            "retrieval_score": hit.score,
                            "rerank_score": float(score),
                            "score": float(score),
                            "strategy": hit.strategy + "+lexical_rerank",
                        }
                    )
                )
            return sorted(
                scored, key=lambda hit: (hit.score, hit.retrieval_score or 0), reverse=True
            )[:limit]
        if not self.settings.reranker_url or not self.settings.reranker_model:
            raise PublicTaskError(
                "模型重排缺少服务地址或模型配置。",
                code="reranker_not_configured",
                suggestion="配置 RERANKER_URL 与 RERANKER_MODEL。",
            )
        if self.client is None:
            self.client = httpx.AsyncClient(
                timeout=self.settings.reranker_timeout_seconds, follow_redirects=False
            )
        headers = (
            {"Authorization": f"Bearer {self.settings.reranker_api_key}"}
            if self.settings.reranker_api_key
            else {}
        )
        try:
            response = await self.client.post(
                self.settings.reranker_url,
                headers=headers,
                json={
                    "model": self.settings.reranker_model,
                    "query": query,
                    "documents": [f"{hit.title}\n{hit.text}" for hit in hits],
                    "top_n": min(limit, len(hits)),
                },
            )
            response.raise_for_status()
            results = response.json()["results"]
            if not isinstance(results, list) or len(results) != min(limit, len(hits)):
                raise ValueError("Incomplete reranker response")
            seen, ranked = set(), []
            for item in results:
                index, score = item["index"], item["relevance_score"]
                if (
                    not isinstance(index, int)
                    or isinstance(index, bool)
                    or index in seen
                    or index < 0
                    or index >= len(hits)
                    or isinstance(score, bool)
                    or not isinstance(score, (float, int))
                    or not math.isfinite(score)
                ):
                    raise ValueError("Invalid reranker response")
                seen.add(index)
                hit = hits[index]
                ranked.append(
                    hit.model_copy(
                        update={
                            "retrieval_score": hit.score,
                            "rerank_score": float(score),
                            "score": float(score),
                            "strategy": hit.strategy + "+model_rerank",
                        }
                    )
                )
            return sorted(ranked, key=lambda hit: hit.score, reverse=True)
        except (httpx.HTTPError, ValueError, KeyError, TypeError, OverflowError) as exc:
            raise PublicTaskError(
                "模型重排服务调用失败或返回了无效结果。",
                code="reranker_failed",
                retryable=True,
                suggestion="检查重排服务、模型和凭据后重试。",
            ) from exc
