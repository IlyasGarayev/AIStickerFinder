"""
indexer.py — Vision indexer using pure aiohttp to avoid Google SDK freeze issues.

Pipeline:
  1. Scan /storage/stickers/ for .webp files not yet in ChromaDB.
  2. Describe each sticker with Gemini 3-flash-preview (static OR animated prompt).
  3. Validate the 8-field JSON schema.
  4. Build a "Super-Context String" combining all fields.
  5. Batch-embed with text-embedding-004 (rate-limited token bucket).
  6. Upsert vectors + metadata into ChromaDB in chunks of 50.

Run directly:
    python indexer.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
import base64
from pathlib import Path
from typing import TypedDict, Any

import aiohttp
import config
import database

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ─── Sticker Metadata Schema ──────────────────────────────────────────────────

class StickerMetadata(TypedDict):
    visual_description: str
    emotional_vibe: str
    implied_situation: str
    action_description: str   # Motion for animated stickers; "none" for static
    text_content: str
    semantic_tags: list[str]
    vibe_category: str
    predictive_queries: list[str]
    is_animated: bool


# ─── Token-Bucket Rate Limiter ────────────────────────────────────────────────

class AsyncRateLimiter:
    """Token-bucket limiter to respect RPM."""
    def __init__(self, max_calls: int, period: float) -> None:
        self._max_calls = max_calls
        self._period = period
        self._tokens = float(max_calls)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                float(self._max_calls),
                self._tokens + elapsed * (self._max_calls / self._period),
            )
            self._last_refill = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) * (self._period / self._max_calls)
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


# Vision: 30 RPM
_vision_limiter = AsyncRateLimiter(max_calls=30, period=60.0)
_vision_semaphore = asyncio.Semaphore(5)

# Embedding: 40 RPM
_embed_limiter = AsyncRateLimiter(max_calls=40, period=60.0)


# ─── Retry Helper ─────────────────────────────────────────────────────────────

def _is_retryable(status: int, text: str) -> bool:
    """Retry on 429 (Rate Limit) and 5xx (Server Error)."""
    return status == 429 or status >= 500 or "quota" in text.lower()


async def _with_retry(fn, max_attempts: int = 5, base_delay: float = 2.0, label: str = "call"):
    """Full-jitter exponential backoff for aiohttp calls."""
    for attempt in range(1, max_attempts + 1):
        try:
            return await fn()
        except Exception as exc:
            # We raise an Exception with special format "STATUS|TEXT" if it's an HTTP error
            status = 500
            text = str(exc)
            if "|" in text:
                try:
                    status_str, text = text.split("|", 1)
                    status = int(status_str)
                except ValueError:
                    pass

            if attempt == max_attempts or not _is_retryable(status, text):
                logger.error("[Retry] '%s' gave up after %d attempt(s) (Status %s): %s", label, attempt, status, text)
                raise Exception(text)
            
            cap = base_delay * (2 ** (attempt - 1))
            sleep_for = random.uniform(0.5, cap)
            logger.warning("[Retry] '%s' attempt %d/%d — Status %s. Sleeping %.1fs…", label, attempt, max_attempts, status, sleep_for)
            await asyncio.sleep(sleep_for)


# ─── Animated Detection ───────────────────────────────────────────────────────

def _detect_animated(path: Path) -> bool:
    try:
        return b"ANIM" in path.read_bytes()[:64]
    except OSError:
        return False


# ─── Vision Prompts ───────────────────────────────────────────────────────────

_STATIC_PROMPT = """You are an expert sticker analyst. Analyse this STATIC sticker image.
Respond ONLY with a single valid JSON object — no markdown fences, no extra keys:

{
  "visual_description": "<Vivid 2-3 sentence description of colors, characters, art style>",
  "emotional_vibe": "<3-6 comma-separated emotions/vibes>",
  "implied_situation": "<1-2 sentences: real-world scenario for sending this sticker>",
  "action_description": "none",
  "text_content": "<Exact text on sticker, or 'none'>",
  "semantic_tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"],
  "vibe_category": "<snake_case_category>",
  "predictive_queries": ["<query1>", "<query2>", "<query3>", "<query4>"],
  "is_animated": false
}"""

_ANIMATED_PROMPT = """You are an expert sticker analyst specialised in ANIMATED stickers.
Analyse the full animation. Respond ONLY with a single valid JSON object — no markdown fences:

{
  "visual_description": "<Vivid 2-3 sentence description of character(s) and visual style>",
  "emotional_vibe": "<3-6 comma-separated emotions conveyed by the animation>",
  "implied_situation": "<1-2 sentences: real-world scenario for sending this sticker>",
  "action_description": "<Key motion in 1-2 sentences: WHAT IS HAPPENING, e.g. 'A cat repeatedly slams a keyboard then collapses face-down'>",
  "text_content": "<Exact text on sticker, or 'none'>",
  "semantic_tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"],
  "vibe_category": "<snake_case_category>",
  "predictive_queries": ["<query1>", "<query2>", "<query3>", "<query4>"],
  "is_animated": true
}"""


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


# ─── Describe a Single Sticker ────────────────────────────────────────────────

async def describe_sticker(sticker_path: Path) -> StickerMetadata | None:
    is_animated = await asyncio.to_thread(_detect_animated, sticker_path)
    prompt = _ANIMATED_PROMPT if is_animated else _STATIC_PROMPT

    async def _call() -> StickerMetadata:
        await _vision_limiter.acquire()
        image_bytes = await asyncio.to_thread(sticker_path.read_bytes)
        b64_image = base64.b64encode(image_bytes).decode("utf-8")

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_VISION_MODEL}:generateContent?key={config.GOOGLE_API_KEY}"
        payload = {
            "contents": [{
                "parts": [
                    {"inline_data": {"mime_type": "image/webp", "data": b64_image}},
                    {"text": prompt}
                ]
            }],
            "generationConfig": {"temperature": 0.2}
        }

        session = get_session()
        async with session.post(url, json=payload, timeout=30) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise Exception(f"{resp.status}|{text}")
            
            data = json.loads(text)
            
            try:
                raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            except (KeyError, IndexError):
                raise Exception(f"500|Malformed Google Vision response: {text[:200]}")

            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

            metadata: StickerMetadata = json.loads(raw)
            required = set(StickerMetadata.__annotations__.keys())
            missing = required - metadata.keys()
            if missing:
                raise ValueError(f"Missing keys: {missing}")

            metadata["is_animated"] = is_animated
            return metadata

    try:
        async with _vision_semaphore:
            return await _with_retry(_call, max_attempts=5, base_delay=2.0, label=sticker_path.name)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.error("Schema error on '%s': %s", sticker_path.name, exc)
        return None
    except Exception as exc:
        logger.error("Giving up on '%s': %s", sticker_path.name, exc)
        return None


# ─── Super-Context String ─────────────────────────────────────────────────────

def build_super_context(metadata: StickerMetadata, sticker_id: str) -> str:
    tags    = ", ".join(metadata["semantic_tags"])
    queries = " | ".join(metadata["predictive_queries"])
    action  = (
        f" Action: {metadata['action_description']}."
        if metadata.get("is_animated") and metadata.get("action_description") != "none"
        else ""
    )
    return (
        f"Sticker ID: {sticker_id}. "
        f"Visual: {metadata['visual_description']}{action} "
        f"Emotional vibe: {metadata['emotional_vibe']}. "
        f"Situation: {metadata['implied_situation']} "
        f"Text: {metadata['text_content']}. "
        f"Tags: {tags}. "
        f"Category: {metadata['vibe_category']}. "
        f"Predictive queries: {queries}."
    )


# ─── Batch Embedding ──────────────────────────────────────────────────────────

async def batch_embed(texts: list[str]) -> list[list[float]]:
    all_embeddings: list[list[float]] = []
    batch_size = config.EMBEDDING_BATCH_SIZE
    n_batches  = math.ceil(len(texts) / batch_size)

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_EMBEDDING_MODEL}:batchEmbedContents?key={config.GOOGLE_API_KEY}"

    for i in range(0, len(texts), batch_size):
        chunk = texts[i : i + batch_size]
        batch_num = i // batch_size + 1
        logger.info("Embedding batch %d/%d (%d texts)…", batch_num, n_batches, len(chunk))

        async def _embed(chunk=chunk):
            await _embed_limiter.acquire()
            payload = {
                "requests": [
                    {
                        "model": f"models/{config.GEMINI_EMBEDDING_MODEL}",
                        "content": {"parts": [{"text": txt}]}
                    }
                    for txt in chunk
                ]
            }
            
            session = get_session()
            async with session.post(url, json=payload, timeout=20) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise Exception(f"{resp.status}|{text}")
                
                data = json.loads(text)
                try:
                    return [item["values"] for item in data["embeddings"]]
                except (KeyError, TypeError):
                    raise Exception(f"500|Malformed Embed response: {text[:200]}")

        try:
            vecs = await _with_retry(_embed, max_attempts=5, base_delay=3.0, label=f"embed-batch-{batch_num}")
            all_embeddings.extend(vecs)
        except Exception as exc:
            logger.error("Embedding batch %d permanently failed: %s. Using zero vectors.", batch_num, exc)
            all_embeddings.extend([[0.0] * 768] * len(chunk))

        if i + batch_size < len(texts):
            await asyncio.sleep(config.EMBEDDING_BATCH_DELAY_S)

    return all_embeddings


# ─── Main Indexer ─────────────────────────────────────────────────────────────

async def run_indexer(storage_dir: Path | None = None) -> dict[str, int]:
    storage_dir = storage_dir or config.STORAGE_DIR
    webp_files  = sorted(storage_dir.glob("*.webp"))

    if not webp_files:
        logger.warning("No .webp files found in '%s'.", storage_dir)
        return {"total": 0, "skipped": 0, "indexed": 0, "failed": 0}

    logger.info("Found %d .webp file(s) in '%s'.", len(webp_files), storage_dir)

    already_indexed = database.get_indexed_ids()
    to_index = [f for f in webp_files if f.stem not in already_indexed]
    skipped  = len(webp_files) - len(to_index)
    logger.info("Skipping %d already-indexed. Processing %d new sticker(s).", skipped, len(to_index))

    if not to_index:
        return {"total": len(webp_files), "skipped": skipped, "indexed": 0, "failed": 0}

    chunk_size = 50
    n_chunks = math.ceil(len(to_index) / chunk_size)
    total_indexed = 0
    total_failed = 0

    try:
        for chunk_idx in range(n_chunks):
            chunk_start = chunk_idx * chunk_size
            chunk_to_index = to_index[chunk_start : chunk_start + chunk_size]
            
            logger.info("--- Processing chunk %d/%d (%d stickers) ---", chunk_idx + 1, n_chunks, len(chunk_to_index))

            described = await asyncio.gather(
                *[_describe_logged(f, chunk_start + i, len(to_index)) for i, f in enumerate(chunk_to_index, 1)]
            )

            valid = [(p, m) for p, m in described if m is not None]
            failed_count = len(described) - len(valid)
            total_failed += failed_count
            
            if not valid:
                continue

            ids_list   = [p.stem for p, _ in valid]
            metas_list = [m for _, m in valid]
            contexts   = [build_super_context(m, sid) for m, sid in zip(metas_list, ids_list)]

            embeddings = await batch_embed(contexts)

            db_metadatas = [
                {
                    "file_path":         str((config.STORAGE_DIR / f"{sid}.webp").resolve()),
                    "visual_description": m["visual_description"],
                    "emotional_vibe":     m["emotional_vibe"],
                    "implied_situation":  m["implied_situation"],
                    "action_description": m.get("action_description", "none"),
                    "text_content":       m["text_content"],
                    "semantic_tags":      ", ".join(m["semantic_tags"]),
                    "vibe_category":      m["vibe_category"],
                    "predictive_queries": " | ".join(m["predictive_queries"]),
                    "is_animated":        str(m.get("is_animated", False)),
                }
                for m, sid in zip(metas_list, ids_list)
            ]

            database.batch_upsert_stickers(
                sticker_ids=ids_list,
                embeddings=embeddings,
                metadatas=db_metadatas,
                documents=contexts,
            )
            total_indexed += len(valid)

        logger.info(
            "✅ Done. Total: %d | Skipped: %d | Indexed: %d | Failed: %d",
            len(webp_files), skipped, total_indexed, total_failed,
        )
        return {"total": len(webp_files), "skipped": skipped, "indexed": total_indexed, "failed": total_failed}
    
    finally:
        await close_session()


async def _describe_logged(path: Path, idx: int, total: int) -> tuple[Path, StickerMetadata | None]:
    logger.info("Describing sticker %d/%d: %s", idx, total, path.name)
    return path, await describe_sticker(path)


if __name__ == "__main__":
    asyncio.run(run_indexer())
