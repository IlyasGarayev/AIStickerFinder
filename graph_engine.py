"""
graph_engine.py — LangGraph StateGraph, pure aiohttp edition.

Flow:
  route_and_translate  (Node A — Gemini 3.1 Pro Preview)
       ↓
  vectorize_vibe       (Node B — text-embedding-004)
       ↓
  query_chromadb       (Node C — Cosine similarity, top-5)
       ↓
  validate_and_rerank  (Node D — Gemini 3.1 Pro Preview)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, TypedDict

import aiohttp
from langgraph.graph import END, StateGraph

import config
import database

logger = logging.getLogger(__name__)

# ─── API Clients (aiohttp) ────────────────────────────────────────────────────

_session: aiohttp.ClientSession | None = None

def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(limit=10, enable_cleanup_closed=True)
        _session = aiohttp.ClientSession(connector=connector)
    return _session


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()


# ─── State ────────────────────────────────────────────────────────────────────

class StickerSearchState(TypedDict, total=False):
    query: str
    english_vibe: str
    embedding: list[float]
    raw_results: list[dict[str, Any]]
    validated_results: list[dict[str, Any]]
    error: str | None


# ─── Translation Cache ────────────────────────────────────────────────────────

class TranslationCache:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._redis: Any | None = None
        if config.USE_REDIS_CACHE:
            try:
                import redis.asyncio as aioredis  # type: ignore
                self._redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
                logger.info("TranslationCache: Redis @ %s", config.REDIS_URL)
            except ImportError:
                logger.warning("USE_REDIS_CACHE=true but 'redis' not installed. Using in-memory cache.")

    @staticmethod
    def _key(query: str) -> str:
        return hashlib.sha256(query.strip().lower().encode()).hexdigest()

    async def get(self, query: str) -> str | None:
        k = self._key(query)
        return (await self._redis.get(k)) if self._redis else self._store.get(k)

    async def set(self, query: str, vibe: str) -> None:
        k = self._key(query)
        if self._redis:
            await self._redis.set(k, vibe, ex=86400)
        else:
            self._store[k] = vibe


_translation_cache = TranslationCache()


# ─── Node A: Language Router & Translator ────────────────────────────────────

async def route_and_translate(state: StickerSearchState) -> StickerSearchState:
    query = state["query"]

    cached = await _translation_cache.get(query)
    if cached:
        logger.info("[Node A] Cache hit.")
        return {**state, "english_vibe": cached}

    prompt = f"""
You are a multilingual semantic search expert.

USER QUERY: "{query}"

1. Detect the language.
2. Understand the emotional "vibe" or situation the user is expressing.
3. Expand it into a rich English paragraph (4-8 sentences) capturing:
   - Core emotion and mood
   - Implied real-world situation
   - Related feelings, synonyms, cultural nuances
   - What kind of sticker would perfectly match

Respond ONLY with the English paragraph — no labels, no quotes.
"""
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_PRO_MODEL}:generateContent?key={config.GOOGLE_API_KEY}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        
        session = get_session()
        async with session.post(url, json=payload, timeout=20) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status}: {text}")
            data = json.loads(text)
            english_vibe = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            
        await _translation_cache.set(query, english_vibe)
        logger.info("[Node A] Translated (%d chars).", len(english_vibe))
        return {**state, "english_vibe": english_vibe, "error": None}
    except Exception as exc:
        logger.error("[Node A] Failed: %s", exc)
        return {**state, "english_vibe": query, "error": str(exc)}


# ─── Node B: Vectorize ────────────────────────────────────────────────────────

async def vectorize_vibe(state: StickerSearchState) -> StickerSearchState:
    english_vibe = state.get("english_vibe", state["query"])
    try:
        # For single text embedding, we can still use batchEmbedContents
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_EMBEDDING_MODEL}:batchEmbedContents?key={config.GOOGLE_API_KEY}"
        payload = {
            "requests": [{
                "model": f"models/{config.GEMINI_EMBEDDING_MODEL}",
                "content": {"parts": [{"text": english_vibe}]}
            }]
        }
        
        session = get_session()
        async with session.post(url, json=payload, timeout=15) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status}: {text}")
            data = json.loads(text)
            embedding = data["embeddings"][0]["values"]
            
        logger.info("[Node B] Embedded → %d-dim vector.", len(embedding))
        return {**state, "embedding": embedding}
    except Exception as exc:
        logger.error("[Node B] Embedding failed: %s", exc)
        return {**state, "embedding": [], "error": str(exc)}


# ─── Node C: ChromaDB Query ───────────────────────────────────────────────────

async def query_chromadb(state: StickerSearchState) -> StickerSearchState:
    embedding = state.get("embedding", [])
    if not embedding:
        return {**state, "raw_results": [], "error": "No embedding from Node B."}
    try:
        raw = await asyncio.to_thread(database.cosine_search, embedding, config.TOP_K)
        logger.info("[Node C] %d raw hits.", len(raw))
        return {**state, "raw_results": raw}
    except Exception as exc:
        logger.error("[Node C] ChromaDB query failed: %s", exc)
        return {**state, "raw_results": [], "error": str(exc)}


# ─── Node D: Validate & Re-rank ───────────────────────────────────────────────

_RESULT_SCHEMA = """[
  {
    "sticker_id": "<uuid>",
    "file_path": "<path>",
    "match_explanation": "<1-2 sentence explanation>",
    "confidence_score": <0.0-1.0>
  }
]"""


async def validate_and_rerank(state: StickerSearchState) -> StickerSearchState:
    raw_results = state.get("raw_results", [])
    if not raw_results:
        return {**state, "validated_results": []}

    candidates = json.dumps(
        [
            {
                "sticker_id":        r["sticker_id"],
                "file_path":         r["file_path"],
                "visual_description": r["metadata"].get("visual_description", ""),
                "emotional_vibe":    r["metadata"].get("emotional_vibe", ""),
                "implied_situation": r["metadata"].get("implied_situation", ""),
                "action_description": r["metadata"].get("action_description", "none"),
                "vibe_category":     r["metadata"].get("vibe_category", ""),
                "cosine_score":      r["score"],
            }
            for r in raw_results
        ],
        ensure_ascii=False,
        indent=2,
    )

    prompt = f"""You are a sticker recommendation expert.

USER QUERY: "{state['query']}"
ENGLISH VIBE: "{state.get('english_vibe', state['query'])}"

CANDIDATES:
{candidates}

Tasks:
1. Re-rank by vibe match (best first).
2. Remove stickers with confidence < 0.3.
3. Write a 1-2 sentence match explanation for each kept sticker.
4. Assign a confidence_score 0.0–1.0.

Respond ONLY with a JSON array (no markdown fences):
{_RESULT_SCHEMA}"""

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_PRO_MODEL}:generateContent?key={config.GOOGLE_API_KEY}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        
        session = get_session()
        async with session.post(url, json=payload, timeout=30) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status}: {text}")
            data = json.loads(text)
            raw_text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            
        if raw_text.startswith("```"):
            raw_text = raw_text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        validated: list[dict[str, Any]] = json.loads(raw_text)
        logger.info("[Node D] Re-ranked → %d results.", len(validated))
        return {**state, "validated_results": validated}
    except Exception as exc:
        logger.error("[Node D] Re-ranking failed (%s). Using raw results.", exc)
        fallback = [
            {
                "sticker_id":        r["sticker_id"],
                "file_path":         r["file_path"],
                "match_explanation": r["metadata"].get("emotional_vibe", ""),
                "confidence_score":  r["score"],
            }
            for r in raw_results
        ]
        return {**state, "validated_results": fallback}


# ─── Graph Builder ────────────────────────────────────────────────────────────

_compiled_graph = None


def build_graph():
    global _compiled_graph
    if _compiled_graph is not None:
        return _compiled_graph

    builder = StateGraph(StickerSearchState)
    builder.add_node("route_and_translate", route_and_translate)
    builder.add_node("vectorize_vibe",      vectorize_vibe)
    builder.add_node("query_chromadb",      query_chromadb)
    builder.add_node("validate_and_rerank", validate_and_rerank)

    builder.set_entry_point("route_and_translate")
    builder.add_edge("route_and_translate", "vectorize_vibe")
    builder.add_edge("vectorize_vibe",      "query_chromadb")
    builder.add_edge("query_chromadb",      "validate_and_rerank")
    builder.add_edge("validate_and_rerank", END)

    _compiled_graph = builder.compile()
    logger.info("LangGraph compiled.")
    return _compiled_graph


# ─── Mermaid Export ───────────────────────────────────────────────────────────

_MERMAID = """\
# Sticker Vibe Search — LangGraph Flow

```mermaid
flowchart TD
    START([🚀 User Query]) --> A
    A["🌐 Node A\\nLanguage Router\\n(Gemini 3.1 Pro Preview)"]
    A --> B
    B["🧮 Node B\\nVectorize Vibe\\n(text-embedding-004)"]
    B --> C
    C["🔍 Node C\\nChromaDB Cosine Search\\n(Top-5)"]
    C --> D
    D["✅ Node D\\nValidate & Re-rank\\n(Gemini 3.1 Pro Preview)"]
    D --> END_NODE([🎯 Results])
    style A fill:#4A90D9,color:#fff
    style B fill:#7B68EE,color:#fff
    style C fill:#50C878,color:#fff
    style D fill:#FF8C00,color:#fff
```
"""


def export_mermaid(output_path: str = config.MERMAID_OUTPUT_FILE) -> str:
    Path(output_path).write_text(_MERMAID, encoding="utf-8")
    logger.info("Mermaid diagram written to '%s'.", output_path)
    return _MERMAID


# ─── Search Entrypoint ────────────────────────────────────────────────────────

async def run_search(query: str) -> list[dict[str, Any]]:
    graph = build_graph()
    final: StickerSearchState = await graph.ainvoke(StickerSearchState(query=query))
    if final.get("error") and not final.get("validated_results"):
        raise RuntimeError(f"Pipeline error: {final['error']}")
    return final.get("validated_results", [])
