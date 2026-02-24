"""
sync_all.py — Download ALL sticker packs → index → vector DB.

Packs are downloaded sequentially (one at a time) to avoid Telegram flood limits.
A failed pack is logged and the script continues. After all downloads, the full
indexer pipeline runs automatically.

Usage:
    python sync_all.py                # Full run: download + index
    python sync_all.py --skip-download  # Only index (use after a crash)
    python sync_all.py --skip-index     # Only download
    python sync_all.py --dry-run        # Print pack list and exit
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass

# ─── Logging (stdout + file) ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sync_all.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ─── Master Pack List ─────────────────────────────────────────────────────────
# All short-names from t.me/addstickers/<name>
# Packs with known animation are marked; the downloader auto-detects via MIME type.
STICKER_PACKS: list[dict] = [

    # ── Original packs ────────────────────────────────────────────────────────
    {"name": "MemePack1",  "animated": False},
    {"name": "MemePack2",  "animated": False},
    {"name": "MemePack3",  "animated": False},
    {"name": "MemePack4",  "animated": False},
    {"name": "MemePack5",  "animated": False},
    {"name": "MemePack6",  "animated": False},
    {"name": "MemePack7",  "animated": False},
    {"name": "MemePack8",  "animated": False},
    {"name": "MemePack9",  "animated": False},
    {"name": "MemePack10", "animated": False},
    {"name": "GreatMindsAnimated",  "animated": True},
    {"name": "GreatMindsAnimated2", "animated": True},
    {"name": "ClassicMemesAnimated", "animated": True},

    # ── Memes & Reactions ─────────────────────────────────────────────────────
    {"name": "VideoMemes",  "animated": True},
    {"name": "VideoMemes2", "animated": True},
    {"name": "VideoMemes3", "animated": True},
    {"name": "VideoMemes4", "animated": True},
    {"name": "VideoMemes5", "animated": True},
    {"name": "ClassicMemes",     "animated": False},
    {"name": "Memes_Pack",       "animated": False},
    {"name": "Meme_Collection",  "animated": False},
    {"name": "Meme_Hub",         "animated": False},
    {"name": "Meme_Hub_2",       "animated": False},
    {"name": "Meme_Hub_3",       "animated": False},
    {"name": "TrollFace_HD",     "animated": False},
    {"name": "RageComics",       "animated": False},

    # ── Pepe & Feels ──────────────────────────────────────────────────────────
    {"name": "Pepe_Stickers",          "animated": False},
    {"name": "Pepe_Animated",          "animated": True},
    {"name": "PepeFullPack",           "animated": False},
    {"name": "PepeHighQuality",        "animated": False},
    {"name": "Pepe_The_Frog_Animated", "animated": True},
    {"name": "Pepe_Emotions",          "animated": False},
    {"name": "RarePepes",              "animated": False},
    {"name": "ApuApustaja",            "animated": False},

    # ── Animals & Pets ────────────────────────────────────────────────────────
    {"name": "OfficeCat",           "animated": False},
    {"name": "Broken_Cats",         "animated": False},
    {"name": "Funny_Cats_Animated", "animated": True},
    {"name": "Dog_Memes",           "animated": False},
    {"name": "Doge_Pack",           "animated": False},
    {"name": "Hamster_Vibe",        "animated": False},
    {"name": "UtyaDuck",            "animated": False},
    {"name": "Pusheen",             "animated": True},
    {"name": "Cat_Emotions",        "animated": False},
    {"name": "Angry_Animals",       "animated": False},

    # ── Movie & TV Shows ──────────────────────────────────────────────────────
    {"name": "RickAndMorty",          "animated": False},
    {"name": "RickAndMorty_Animated", "animated": True},
    {"name": "Simpsons_Animated",     "animated": True},
    {"name": "TheSimpsons",           "animated": False},
    {"name": "MrBean_Animated",       "animated": True},
    {"name": "Marvel_Stickers",       "animated": False},
    {"name": "DC_Heroes",             "animated": False},
    {"name": "HarryPotter_Animated",  "animated": True},
    {"name": "StarWars_Pack",         "animated": False},
    {"name": "BreakingBad_Stickers",  "animated": False},
    {"name": "TheOffice_Memes",       "animated": False},
    {"name": "Friends_TV_Show",       "animated": False},

    # ── Cartoon & Anime ───────────────────────────────────────────────────────
    {"name": "TomAndJerry",      "animated": False},
    {"name": "TomAndJerry_Animated", "animated": True},
    {"name": "SpongeBob_Animated", "animated": True},
    {"name": "SpongeBob_Memes",  "animated": False},
    {"name": "Shrek_Pack",       "animated": False},
    {"name": "Minions_Animated", "animated": True},
    {"name": "Anime_Reactions",  "animated": False},
    {"name": "Naruto_Stickers",  "animated": False},

    # ── Misc & Random Vibes ───────────────────────────────────────────────────
    {"name": "HotCherry",           "animated": False},
    {"name": "AnimatedHotCherry",   "animated": True},
    {"name": "MilkAndMocha",        "animated": False},
    {"name": "Sticky_Business",     "animated": False},
    {"name": "Blob_Animated",       "animated": True},
    {"name": "Telegram_Best_Memes", "animated": False},
    {"name": "Sticker_Mix",         "animated": False},
    {"name": "Global_Reactions",    "animated": False},
]

TELEGRAM_BASE_URL = "https://t.me/addstickers/{name}"

# Delay between packs to avoid Telegram flood-wait errors
INTER_PACK_DELAY_S: float = 3.0


# ─── Result Tracking ──────────────────────────────────────────────────────────


@dataclass
class PackResult:
    name: str
    success: bool
    downloaded: int = 0
    error: str = ""
    duration_s: float = 0.0


# ─── Download Loop ────────────────────────────────────────────────────────────


async def download_all_packs(packs: list[dict]) -> list[PackResult]:
    """Download each pack sequentially with full error isolation."""
    from downloader import download_sticker_pack

    results: list[PackResult] = []
    total = len(packs)

    for idx, pack in enumerate(packs, start=1):
        name = pack["name"]
        url = TELEGRAM_BASE_URL.format(name=name)
        logger.info("━━━ [%d/%d] %s ━━━", idx, total, name)

        t0 = time.monotonic()
        try:
            stickers = await download_sticker_pack(url)
            dur = round(time.monotonic() - t0, 1)
            results.append(PackResult(name=name, success=True, downloaded=len(stickers), duration_s=dur))
            logger.info("✅ %s → %d stickers in %.1fs", name, len(stickers), dur)
        except Exception as exc:
            dur = round(time.monotonic() - t0, 1)
            results.append(PackResult(name=name, success=False, error=str(exc), duration_s=dur))
            logger.error("❌ %s failed: %s", name, exc)

        if idx < total:
            await asyncio.sleep(INTER_PACK_DELAY_S)

    return results


# ─── Summary ──────────────────────────────────────────────────────────────────


def print_summary(results: list[PackResult], index_summary: dict | None) -> None:
    ok  = [r for r in results if r.success]
    bad = [r for r in results if not r.success]

    print("\n" + "═" * 62)
    print("  SYNC COMPLETE")
    print("═" * 62)
    if results:
        print(f"  Packs attempted  : {len(results)}")
        print(f"  Packs succeeded  : {len(ok)}")
        print(f"  Packs failed     : {len(bad)}")
        print(f"  Total downloaded : {sum(r.downloaded for r in ok)} stickers")
    if bad:
        print("\n  ── Failed packs ──")
        for r in bad:
            print(f"    ✗ {r.name:<30} {r.error[:50]}")
    if index_summary:
        print("\n  ── Indexing ──")
        print(f"    Total    : {index_summary.get('total', '?')}")
        print(f"    Indexed  : {index_summary.get('indexed', '?')}")
        print(f"    Skipped  : {index_summary.get('skipped', '?')}")
        print(f"    Failed   : {index_summary.get('failed', '?')}")
    print("═" * 62 + "\n")


# ─── Main ─────────────────────────────────────────────────────────────────────


async def main(skip_download: bool, skip_index: bool, dry_run: bool) -> None:
    if dry_run:
        static_count   = sum(1 for p in STICKER_PACKS if not p["animated"])
        animated_count = sum(1 for p in STICKER_PACKS if p["animated"])
        print(f"\nDry run — {len(STICKER_PACKS)} packs ({static_count} static, {animated_count} animated):\n")
        for p in STICKER_PACKS:
            icon = "🎞 " if p["animated"] else "🖼 "
            print(f"  {icon} {p['name']}")
        print()
        return

    dl_results: list[PackResult] = []
    idx_summary: dict | None = None

    # ── Phase 1: Download ─────────────────────────────────────────────────────
    if not skip_download:
        static   = sum(1 for p in STICKER_PACKS if not p["animated"])
        animated = sum(1 for p in STICKER_PACKS if p["animated"])
        logger.info(
            "Starting download: %d packs total (%d static, %d animated).",
            len(STICKER_PACKS), static, animated,
        )
        dl_results = await download_all_packs(STICKER_PACKS)
    else:
        logger.info("Skipping download phase (--skip-download).")

    # ── Phase 2: Index ────────────────────────────────────────────────────────
    if not skip_index:
        logger.info("Starting indexer pipeline…")
        from indexer import run_indexer
        idx_summary = await run_indexer()
    else:
        logger.info("Skipping indexing phase (--skip-index).")

    print_summary(dl_results, idx_summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download all Telegram sticker packs and index them into ChromaDB."
    )
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip downloading; only run the indexer.")
    parser.add_argument("--skip-index", action="store_true",
                        help="Only download; do not run the indexer.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the pack list and exit without doing anything.")
    args = parser.parse_args()

    asyncio.run(main(
        skip_download=args.skip_download,
        skip_index=args.skip_index,
        dry_run=args.dry_run,
    ))
