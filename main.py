"""
main.py — FastAPI application for the Semantic Sticker Vibe Search Engine.

Endpoints:
  POST /sync-pack          Download a Telegram sticker pack
  POST /index              Run the full Gemini 3-flash indexing pipeline
  POST /search             Search stickers by vibe (any language)
  GET  /sticker/{id}       Serve a sticker .webp file
  GET  /graph/mermaid      Return the LangGraph flow as a Mermaid diagram
  GET  /health             Health check

Run with:
    uvicorn main:app --reload
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import aiofiles
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel, HttpUrl, field_validator

import config
import database
import graph_engine
import indexer
from downloader import download_sticker_pack

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ─── Lifespan (startup / shutdown) ───────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    On startup:
      - Initialise ChromaDB collection (creates it if missing).
      - Compile the LangGraph (validates node wiring).
      - Export the Mermaid diagram to graph_flow.md.
    """
    logger.info("=== Sticker Vibe Search Engine — Starting up ===")

    database.get_collection()  # warm up ChromaDB
    graph_engine.build_graph()  # compile LangGraph
    graph_engine.export_mermaid()  # write graph_flow.md

    logger.info(
        "ChromaDB ready with %d indexed stickers.", database.collection_count()
    )
    logger.info("=== Startup complete. API is live. ===")

    yield  # ── Server is running ──

    logger.info("=== Shutting down. Goodbye. ===")


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title=config.APP_TITLE,
    version=config.APP_VERSION,
    description=config.APP_DESCRIPTION,
    lifespan=lifespan,
)

# ─── CORS ───────────────────────────────────────────────────────────────────

origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "https://stickerfinder.ilyasgarayev.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request / Response Models ────────────────────────────────────────────────


class SyncPackRequest(BaseModel):
    pack_url: str

    @field_validator("pack_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("pack_url must not be empty.")
        return v


class SyncPackResponse(BaseModel):
    message: str
    downloaded: int
    stickers: list[dict]


class IndexResponse(BaseModel):
    message: str
    total: int
    skipped: int
    indexed: int
    failed: int


class SearchRequest(BaseModel):
    query: str

    @field_validator("query")
    @classmethod
    def validate_query(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Query must not be empty.")
        if len(v) > 2000:
            raise ValueError("Query must be 2000 characters or fewer.")
        return v


class StickerResult(BaseModel):
    sticker_id: str
    file_path: str
    image_url: str
    match_explanation: str
    confidence_score: float


class SearchResponse(BaseModel):
    query: str
    results: list[StickerResult]
    total_indexed: int


# ─── Endpoints ────────────────────────────────────────────────────────────────


@app.get("/health", tags=["System"])
async def health_check():
    """Quick health check — confirms the API is up and ChromaDB is reachable."""
    return {
        "status": "ok",
        "total_indexed_stickers": database.collection_count(),
        "storage_dir": str(config.STORAGE_DIR),
        "vision_model": config.GEMINI_VISION_MODEL,
        "pro_model": config.GEMINI_PRO_MODEL,
        "embedding_model": config.GEMINI_EMBEDDING_MODEL,
    }


@app.post("/sync-pack", response_model=SyncPackResponse, tags=["Ingestion"])
async def sync_sticker_pack(body: SyncPackRequest, background: BackgroundTasks):
    """
    Download all stickers from a Telegram sticker pack URL.

    After downloading, the stickers are automatically queued for indexing
    in the background.

    Example body:
        {"pack_url": "https://t.me/addstickers/SomePack"}
    """
    try:
        stickers = await download_sticker_pack(body.pack_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Telegram download failed for URL: %s", body.pack_url)
        raise HTTPException(status_code=502, detail=f"Telegram download failed: {exc}")

    # Kick off the indexer in the background so the response is immediate
    background.add_task(_run_indexer_background)

    return SyncPackResponse(
        message=f"Downloaded {len(stickers)} stickers. Indexing started in background.",
        downloaded=len(stickers),
        stickers=stickers,
    )


@app.post("/index", response_model=IndexResponse, tags=["Ingestion"])
async def trigger_indexing():
    """
    Scan the sticker storage directory and index any new stickers with
    Gemini 3-flash. Already-indexed stickers are skipped automatically.
    """
    try:
        summary = await indexer.run_indexer()
    except Exception as exc:
        logger.exception("Indexer raised an unexpected error.")
        raise HTTPException(status_code=500, detail=f"Indexing failed: {exc}")

    return IndexResponse(
        message="Indexing complete.",
        **summary,
    )


@app.post("/search", response_model=SearchResponse, tags=["Search"])
async def search_stickers(body: SearchRequest):
    """
    Search the sticker library by 'vibe' — in any language.

    The query is:
      1. Translated and semantically expanded (Gemini 3.1 Pro).
      2. Embedded (text-embedding-004).
      3. Matched via cosine similarity (ChromaDB, top-5).
      4. Re-ranked and validated (Gemini 3.1 Pro).

    Returns up to 5 sticker results with explanations and confidence scores.
    """
    if database.collection_count() == 0:
        raise HTTPException(
            status_code=404,
            detail="No stickers indexed yet. Run POST /index first.",
        )

    try:
        results = await graph_engine.run_search(body.query)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.exception("Search pipeline failed for query: %s", body.query[:80])
        raise HTTPException(status_code=500, detail=f"Search failed: {exc}")

    sticker_results = [
        StickerResult(
            sticker_id=r["sticker_id"],
            file_path=r["file_path"],
            image_url=f"/sticker/{r['sticker_id']}",
            match_explanation=r.get("match_explanation", ""),
            confidence_score=float(r.get("confidence_score", 0.0)),
        )
        for r in results
    ]

    return SearchResponse(
        query=body.query,
        results=sticker_results,
        total_indexed=database.collection_count(),
    )


@app.get("/sticker/{sticker_id}", tags=["Assets"])
async def serve_sticker(sticker_id: str):
    """
    Serve a sticker .webp file by its UUID.

    The sticker_id is the filename without the `.webp` extension.
    """
    # Basic validation — no path traversal
    if "/" in sticker_id or "\\" in sticker_id or ".." in sticker_id:
        raise HTTPException(status_code=400, detail="Invalid sticker_id.")

    file_path = config.STORAGE_DIR / f"{sticker_id}.webp"

    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Sticker '{sticker_id}' not found in storage.",
        )

    return FileResponse(
        path=str(file_path),
        media_type="image/webp",
        filename=f"{sticker_id}.webp",
    )


@app.get("/graph/mermaid", response_class=PlainTextResponse, tags=["System"])
async def get_graph_mermaid():
    """
    Return the LangGraph search-flow as a Mermaid diagram (Markdown).
    Paste the contents into any Mermaid renderer to visualise the pipeline.
    """
    mermaid_file = Path(config.MERMAID_OUTPUT_FILE)

    if not mermaid_file.exists():
        # Regenerate on demand if the file was deleted
        return PlainTextResponse(graph_engine.export_mermaid())

    async with aiofiles.open(mermaid_file, "r", encoding="utf-8") as f:
        content = await f.read()

    return PlainTextResponse(content, media_type="text/markdown")


# ─── Background tasks ─────────────────────────────────────────────────────────


async def _run_indexer_background():
    """Fire-and-forget wrapper for the indexer, used by /sync-pack."""
    try:
        summary = await indexer.run_indexer()
        logger.info("Background indexing finished: %s", summary)
    except Exception as exc:
        logger.error("Background indexing error: %s", exc)
