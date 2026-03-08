"""
config.py — Central configuration for the Semantic Sticker Vibe Search Engine.
Loads all environment variables and defines every constant used across modules.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ─── Load .env ────────────────────────────────────────────────────────────────
load_dotenv()

# ─── Google API ───────────────────────────────────────────────────────────────
GOOGLE_API_KEY: str = os.environ.get("GOOGLE_API_KEY", "")
if not GOOGLE_API_KEY:
    raise EnvironmentError(
        "GOOGLE_API_KEY is not set. "
        "Create a .env file with: GOOGLE_API_KEY=<your-key>"
    )

# ─── Telegram API ─────────────────────────────────────────────────────────────
TELEGRAM_API_ID: int = int(os.environ.get("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH: str = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION_FILE: str = os.environ.get("TELEGRAM_SESSION_FILE", "telegram_session")

# ─── Model Configuration (Google Gen AI 2026 Stack) ───────────────────────────
# Vision model: high RPM/RPD for bulk sticker processing
GEMINI_VISION_MODEL: str = "gemini-3-flash-preview"

# Pro model: complex semantic reasoning, translation, re-ranking
GEMINI_PRO_MODEL: str = "gemini-2.5-flash"

# Embedding model: 768-dimensional dense vectors
GEMINI_EMBEDDING_MODEL: str = "text-embedding-004"

# ─── ChromaDB ─────────────────────────────────────────────────────────────────
CHROMA_PERSIST_DIR: str = os.environ.get("CHROMA_PERSIST_DIR", "./chroma_db")
COLLECTION_NAME: str = "stickers"
TOP_K: int = 5  # Top results returned from cosine search

# ─── Storage ──────────────────────────────────────────────────────────────────
STORAGE_DIR: Path = Path(os.environ.get("STORAGE_DIR", "./storage/stickers"))
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# Sticker image standard (WhatsApp/Telegram compatible)
STICKER_SIZE: tuple[int, int] = (512, 512)

# ─── Rate Limiting & Batching ─────────────────────────────────────────────────
# Gemini Embedding: max 100 texts per batch request
EMBEDDING_BATCH_SIZE: int = 100
# Delay between embedding batches to stay within 3,000 RPM
EMBEDDING_BATCH_DELAY_S: float = float(os.environ.get("EMBEDDING_BATCH_DELAY_S", "2.0"))

# ─── TranslationCache ─────────────────────────────────────────────────────────
# Set USE_REDIS_CACHE=true in .env to switch from in-memory to Redis
USE_REDIS_CACHE: bool = os.environ.get("USE_REDIS_CACHE", "false").lower() == "true"
REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379")

# ─── FastAPI App Metadata ─────────────────────────────────────────────────────
APP_TITLE: str = "Semantic Sticker Vibe Search Engine"
APP_VERSION: str = "2.0.0"
APP_DESCRIPTION: str = (
    "Search your Telegram sticker library by 'vibe' — in any language — "
    "powered by Gemini 3 Flash (vision), Gemini 3.1 Pro (reasoning), "
    "text-embedding-004, LangGraph, and ChromaDB."
)

# ─── LangGraph Diagram Output ─────────────────────────────────────────────────
MERMAID_OUTPUT_FILE: str = "graph_flow.md"
