"""
downloader.py — Async Telegram sticker pack downloader using Telethon.

Supports both static and animated stickers:
  • Static stickers  → .webp files → resized to 512×512 lossless WebP
  • Animated stickers → .webm (VP9) files → converted to animated .webp via ffmpeg
    (required for WhatsApp compatibility — WhatsApp only supports animated WebP)

Prerequisites:
  - ffmpeg must be installed and available in PATH.
    Ubuntu/Debian:  sudo apt install ffmpeg
    macOS (brew):   brew install ffmpeg
    Windows:        https://ffmpeg.org/download.html
"""

from __future__ import annotations

import asyncio
import io
import logging
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import TypedDict

from PIL import Image
from telethon import TelegramClient
from telethon.tl.functions.messages import GetStickerSetRequest
from telethon.tl.types import (
    Document,
    DocumentAttributeSticker,
    DocumentAttributeImageSize,
    InputStickerSetShortName,
)

import config

logger = logging.getLogger(__name__)


# ─── Types ────────────────────────────────────────────────────────────────────


class StickerFile(TypedDict):
    sticker_id: str      # UUID (no extension)
    file_path: str       # Absolute path to the saved file
    is_animated: bool    # True if this is an animated WebP


# ─── ffmpeg availability check ────────────────────────────────────────────────


def _check_ffmpeg() -> None:
    """Raise RuntimeError if ffmpeg is not installed."""
    if shutil.which("ffmpeg") is None:
        raise RuntimeError(
            "ffmpeg is not installed or not in PATH. "
            "Animated sticker conversion requires ffmpeg.\n"
            "  Ubuntu: sudo apt install ffmpeg\n"
            "  macOS:  brew install ffmpeg"
        )


# ─── URL Parsing ──────────────────────────────────────────────────────────────


def _extract_short_name(pack_url: str) -> str:
    """
    Parse short_name from a Telegram sticker pack URL.
    Accepts:
      https://t.me/addstickers/<short_name>
      t.me/addstickers/<short_name>
      <short_name>  (bare)
    """
    url = pack_url.strip().rstrip("/")
    if "addstickers/" in url:
        return url.split("addstickers/")[-1]
    return url


# ─── Image Processing ─────────────────────────────────────────────────────────


def _static_webp_bytes(raw: bytes) -> bytes:
    """Resize image bytes to 512×512 and return as lossless static WebP."""
    img = Image.open(io.BytesIO(raw)).convert("RGBA")
    img = img.resize(config.STICKER_SIZE, Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="WEBP", lossless=True, quality=100)
    return buf.getvalue()


def _convert_webm_to_animated_webp(webm_bytes: bytes) -> bytes:
    """
    Convert a VP9-encoded .webm animated sticker to an animated .webp via ffmpeg.

    FFmpeg pipeline:
      input: pipe:0 (webm bytes)
      -vf: scale 512:512, preserve aspect ratio (pad to square with transparency)
      -loop 0: loop the animation infinitely (WhatsApp requirement)
      output: pipe:1 (animated webp bytes)

    Raises subprocess.CalledProcessError if ffmpeg fails.
    """
    cmd = [
        "ffmpeg",
        "-y",                         # Overwrite output without asking
        "-i", "pipe:0",               # Read from stdin
        "-vf", (
            "scale='if(gt(iw,ih),512,-2)':'if(gt(iw,ih),-2,512)',"
            "pad=512:512:(512-iw)/2:(512-ih)/2:color=0x00000000,"
            "format=rgba"
        ),
        "-loop", "0",                 # Infinite loop (WhatsApp requirement)
        "-lossless", "1",             # Lossless WebP quality
        "-compression_level", "6",   # Balanced encode speed vs size
        "-preset", "picture",
        "-f", "webp",
        "pipe:1",                     # Write to stdout
    ]

    result = subprocess.run(
        cmd,
        input=webm_bytes,
        capture_output=True,
        check=True,
    )

    if not result.stdout:
        raise RuntimeError("ffmpeg produced empty output for animated sticker.")

    return result.stdout


# ─── Per-sticker document helpers ─────────────────────────────────────────────


def _is_animated_sticker(document: Document) -> bool:
    """
    Return True if this Telegram document is an animated (video) sticker.
    Animated stickers are typically video/webm-anything-or-mp4 mime types.
    """
    if not document.mime_type:
        return False
    return document.mime_type.startswith("video/")

def _is_lottie_sticker(document: Document) -> bool:
    """Return True if it is a .tgs (Lottie) vector sticker which PIL cannot parse."""
    if not document.mime_type:
        return False
    return document.mime_type == "application/x-tgsticker"


# ─── Core Downloader ──────────────────────────────────────────────────────────


async def download_sticker_pack(pack_url: str) -> list[StickerFile]:
    """
    Download all stickers from a Telegram sticker pack URL.

    - Static stickers  → saved as 512×512 lossless WebP.
    - Animated stickers → converted from .webm to animated .webp via ffmpeg.

    Args:
        pack_url: Full t.me/addstickers URL or bare short name.

    Returns:
        List of StickerFile dicts (sticker_id, file_path, is_animated).

    Raises:
        ValueError: Missing Telegram credentials.
        RuntimeError: ffmpeg not installed (when animated stickers are present).
    """
    if not config.TELEGRAM_API_ID or not config.TELEGRAM_API_HASH:
        raise ValueError(
            "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env."
        )

    short_name = _extract_short_name(pack_url)
    logger.info("Downloading sticker pack: '%s'", short_name)

    async with TelegramClient(
        config.TELEGRAM_SESSION_FILE,
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH,
    ) as client:
        sticker_set = await client(
            GetStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=short_name),
                hash=0,
            )
        )
        documents: list[Document] = sticker_set.documents
        logger.info(
            "Pack '%s': %d sticker(s) found. Starting download…",
            short_name, len(documents),
        )

        # Check ffmpeg availability before hitting any animated sticker
        has_animated = any(_is_animated_sticker(d) for d in documents)
        if has_animated:
            _check_ffmpeg()
            logger.info("Animated stickers detected — ffmpeg conversion enabled.")

        # Limit concurrency: 8 downloads at a time to avoid Telegram rate limits
        semaphore = asyncio.Semaphore(8)

        async def bounded_download(doc: Document) -> StickerFile:
            async with semaphore:
                return await _download_single(client, doc, config.STORAGE_DIR)

        results = await asyncio.gather(
            *[bounded_download(d) for d in documents],
            return_exceptions=True,
        )

        valid_results = []
        for r in results:
            if isinstance(r, Exception):
                logger.warning("Skipping individual sticker due to error: %s", r)
                continue
            valid_results.append(r)

    animated_count = sum(1 for r in valid_results if r["is_animated"])
    logger.info(
        "Pack '%s' downloaded: %d static, %d animated.",
        short_name, len(valid_results) - animated_count, animated_count,
    )
    return valid_results


async def _download_single(
    client: TelegramClient,
    document: Document,
    storage_dir: Path,
) -> StickerFile:
    """Download and process a single sticker document."""
    if _is_lottie_sticker(document):
        raise ValueError("Lottie (.tgs) vector stickers are not currently supported.")

    sticker_id = str(uuid.uuid4())
    file_path = storage_dir / f"{sticker_id}.webp"

    raw: bytes = await client.download_media(document, file=bytes)  # type: ignore[arg-type]
    if raw is None:
        raise RuntimeError(f"Download returned None for document {document.id}")

    is_animated = _is_animated_sticker(document)

    if is_animated:
        # Run ffmpeg in a thread — it's CPU/subprocess bound
        final_bytes = await asyncio.to_thread(_convert_webm_to_animated_webp, raw)
        logger.debug("Converted animated sticker → %s", file_path.name)
    else:
        final_bytes = await asyncio.to_thread(_static_webp_bytes, raw)
        logger.debug("Saved static sticker → %s", file_path.name)

    await asyncio.to_thread(file_path.write_bytes, final_bytes)

    return StickerFile(
        sticker_id=sticker_id,
        file_path=str(file_path.resolve()),
        is_animated=is_animated,
    )
