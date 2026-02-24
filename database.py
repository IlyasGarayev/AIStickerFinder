"""
database.py — ChromaDB client, collection management, upsert, and cosine search.
This module is the single source of truth for all vector store operations.
"""

from __future__ import annotations

import logging
from typing import Any

import chromadb
from chromadb.config import Settings

import config

logger = logging.getLogger(__name__)

# ─── Singleton ChromaDB client ────────────────────────────────────────────────

_client: chromadb.ClientAPI | None = None
_collection: chromadb.Collection | None = None


def get_client() -> chromadb.ClientAPI:
    """Return (or lazily create) the persistent ChromaDB client."""
    global _client
    if _client is None:
        _client = chromadb.PersistentClient(
            path=config.CHROMA_PERSIST_DIR,
            settings=Settings(anonymized_telemetry=False),
        )
        logger.info("ChromaDB client initialised at '%s'", config.CHROMA_PERSIST_DIR)
    return _client


def get_collection() -> chromadb.Collection:
    """
    Return (or lazily create) the sticker collection.
    Uses COSINE distance so we can treat distances as (1 - similarity).
    """
    global _collection
    if _collection is None:
        client = get_client()
        _collection = client.get_or_create_collection(
            name=config.COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info(
            "ChromaDB collection '%s' ready (%d items)",
            config.COLLECTION_NAME,
            _collection.count(),
        )
    return _collection


# ─── Write operations ─────────────────────────────────────────────────────────


def upsert_sticker(
    sticker_id: str,
    embedding: list[float],
    metadata: dict[str, Any],
    document: str,
) -> None:
    """
    Upsert a single sticker into ChromaDB.

    Args:
        sticker_id: UUID filename (without extension) — used as the ChromaDB ID.
        embedding:  Dense vector from text-embedding-004.
        metadata:   Dict with sticker JSON fields + file_path.
        document:   The "Super-Context String" used to generate the embedding.
    """
    collection = get_collection()
    collection.upsert(
        ids=[sticker_id],
        embeddings=[embedding],
        metadatas=[metadata],
        documents=[document],
    )
    logger.debug("Upserted sticker '%s' into ChromaDB.", sticker_id)


def batch_upsert_stickers(
    sticker_ids: list[str],
    embeddings: list[list[float]],
    metadatas: list[dict[str, Any]],
    documents: list[str],
) -> None:
    """
    Upsert a batch of stickers in a single ChromaDB call.
    All lists must have the same length.
    """
    if not sticker_ids:
        return
    collection = get_collection()
    collection.upsert(
        ids=sticker_ids,
        embeddings=embeddings,
        metadatas=metadatas,
        documents=documents,
    )
    logger.info("Batch-upserted %d stickers into ChromaDB.", len(sticker_ids))


# ─── Read operations ──────────────────────────────────────────────────────────


def cosine_search(
    query_vector: list[float],
    top_k: int = config.TOP_K,
) -> list[dict[str, Any]]:
    """
    Perform cosine similarity search against the sticker collection.

    Returns a list of dicts, each with:
        - sticker_id   : str
        - file_path    : str
        - metadata     : dict  (all stored fields)
        - distance     : float (0 = identical, 2 = opposite)
        - score        : float (1 - distance/2, range 0-1, higher = better)
    """
    collection = get_collection()
    results = collection.query(
        query_embeddings=[query_vector],
        n_results=min(top_k, collection.count() or 1),
        include=["metadatas", "distances", "documents"],
    )

    hits: list[dict[str, Any]] = []
    ids = results.get("ids", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    for sticker_id, meta, distance in zip(ids, metadatas, distances):
        # Convert cosine distance → similarity score [0, 1]
        score = round(max(0.0, 1.0 - distance / 2.0), 4)
        hits.append(
            {
                "sticker_id": sticker_id,
                "file_path": meta.get("file_path", ""),
                "metadata": meta,
                "distance": round(distance, 6),
                "score": score,
            }
        )

    return hits


def get_indexed_ids() -> set[str]:
    """Return the set of all sticker_ids already stored in ChromaDB."""
    collection = get_collection()
    result = collection.get(include=[])  # IDs only
    return set(result.get("ids", []))


def collection_count() -> int:
    """Return total number of vectors in the collection."""
    return get_collection().count()
