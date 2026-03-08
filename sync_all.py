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
    {"name": "KamikazeCat", "animated": False},
    {"name": "RedLinx", "animated": False},
    {"name": "Animals", "animated": False},
    {"name": "MuriSecretary", "animated": False},
    {"name": "NeonDemon", "animated": False},
    {"name": "CupidCat", "animated": False},
    {"name": "LegoUnikitty", "animated": False},
    {"name": "SiameseKitty", "animated": False},
    {"name": "MokaDog", "animated": False},
    {"name": "ChristmasDoggie", "animated": False},
    {"name": "CupidDog", "animated": False},
    {"name": "ReginaldPawson", "animated": False},
    {"name": "GoodBoyCody", "animated": False},
    {"name": "HuskyBoy", "animated": False},
    {"name": "TopDog", "animated": False},
    {"name": "JessAndJack", "animated": False},
    {"name": "LazyPanda", "animated": False},
    {"name": "MrCage", "animated": False},
    {"name": "WhiteHenDarkSoul", "animated": False},
    {"name": "CriminalRaccoon", "animated": False},
    {"name": "KeanuReeves", "animated": False},
    {"name": "Spider_meme", "animated": False},
    {"name": "SuperGreenFrog", "animated": False},
    {"name": "MemePolice", "animated": False},
    {"name": "DachshundJones", "animated": False},
    {"name": "ArmBirds", "animated": False},
    {"name": "TomJerryFun", "animated": False},
    {"name": "TarantinoMovie", "animated": False},
    {"name": "KittyMeme", "animated": False},
    {"name": "MoarKittyMeme", "animated": False},
    {"name": "Homer_Jay_Simpson", "animated": False},
    {"name": "Totoro", "animated": False},
    {"name": "PikachuDetective", "animated": False},
    {"name": "MomokoStar", "animated": False},
    {"name": "PrettySailorMoon", "animated": False},
    {"name": "Mesozoic", "animated": False},
    {"name": "Jason_Funderburker", "animated": False},
    {"name": "MrRibbit", "animated": False},
    {"name": "KermitTheMuppetShow", "animated": False},
    {"name": "BankerFrog", "animated": False},
    {"name": "Oliver_the_Frog", "animated": False},
    {"name": "MisterFrogo", "animated": False},
    {"name": "ToddBear", "animated": False},
    {"name": "SarcasticPolarBear", "animated": False},
    {"name": "Little_Bear", "animated": False},
    {"name": "BullAndBear", "animated": False},
    {"name": "TedTheBear", "animated": False},
    {"name": "YourALF", "animated": False},
    {"name": "BuddyBear", "animated": False},
    {"name": "TeddyBrown", "animated": False},
    {"name": "BearCub", "animated": False},
    {"name": "ColdAffairs", "animated": False},
    {"name": "GusTheDuck", "animated": False},
    {"name": "DuckHuntDog", "animated": False},
    {"name": "DonaldAndDaisyDuck", "animated": False},
    {"name": "CowboyFox", "animated": False},
    {"name": "AliceFox", "animated": False},
    {"name": "FunnyFox", "animated": False},
    {"name": "ComradeFox", "animated": False},
    {"name": "TheFoxSays", "animated": False},
    {"name": "DocAndMarty", "animated": False},
    {"name": "FerdinandFox", "animated": False},
    {"name": "GoFox", "animated": False},
    {"name": "MillionaireFox", "animated": False},
    {"name": "Lisushka", "animated": False},
    {"name": "CuteBunnyGirl", "animated": False},
    {"name": "SadBlobby", "animated": False},
    {"name": "Born_To_Die", "animated": False},
    {"name": "LoveBirdsLife", "animated": False},
    {"name": "StoryOfLove", "animated": False},
    {"name": "Cupida", "animated": False},
    {"name": "ShakespearesTragedy", "animated": False},
    {"name": "CupidValentin", "animated": False},
    {"name": "TheWitnessGirl", "animated": False},
    {"name": "Girl_in_Love", "animated": False},
    {"name": "Aphrodite", "animated": False},
    {"name": "LoveDove", "animated": False},
    {"name": "VaultBoySet", "animated": False},
    {"name": "IHateValentinesDay", "animated": False},
    {"name": "HotsyTotsyBoy", "animated": False},
    {"name": "BoyWhoLived", "animated": False},
    {"name": "Karisha", "animated": False},
    {"name": "TheMuse", "animated": False},
    {"name": "FairyKyute", "animated": False},
    {"name": "MissAlena", "animated": False},
    {"name": "BillieEilishFan", "animated": False},
    {"name": "WildWoman", "animated": False},
    {"name": "RabbitJessica", "animated": False},
    {"name": "ChristmasAngel", "animated": False},
    {"name": "TheMatrixMovie", "animated": False},
    {"name": "ConneryBond", "animated": False},
    {"name": "ToyStory", "animated": False},
    {"name": "Belfort", "animated": False},
    {"name": "Hellboy", "animated": False},
    {"name": "TheBestDeadpool", "animated": False},
    {"name": "TheJoker", "animated": False},
    {"name": "AntonioMontana", "animated": False},
    {"name": "SpiderVerse", "animated": False},
    {"name": "LokiTheGod", "animated": False},
    {"name": "T_800", "animated": False},
    {"name": "PrincessLeiaOrgana", "animated": False},
    {"name": "MyNiffler", "animated": False},
    {"name": "VeryNiceBorat", "animated": False},
    {"name": "DrEvil", "animated": False},
    {"name": "Rich_Uncle", "animated": False},
    {"name": "Daenerys", "animated": False},
    {"name": "RDR2Pack", "animated": False},
    {"name": "GameofThrones", "animated": False},
    {"name": "DiabloGames", "animated": False},
    {"name": "GameOfThronesColor", "animated": False},
    {"name": "KratosAndBoi", "animated": False},
    {"name": "Friday_The_13th", "animated": False},
    {"name": "PokemonMasters", "animated": False},
    {"name": "SansaStark", "animated": False},
    {"name": "GameZelda", "animated": False},
    {"name": "Kolibri", "animated": False},
    {"name": "PeachyChick", "animated": False},
    {"name": "JeanJacques", "animated": False},
    {"name": "PeteThePig", "animated": False},
    {"name": "Piggy2019", "animated": False},
    {"name": "RickyPanda", "animated": False},
    {"name": "PandaChan", "animated": False},
    {"name": "CryptoHamster", "animated": False},
    {"name": "MrHamster", "animated": False},
    {"name": "Cheshire_Smile", "animated": False},
    {"name": "MrBeanShow", "animated": False},
    {"name": "MemeDoggie", "animated": False},
    {"name": "DonutAndCoffee", "animated": False},
    {"name": "Vicky", "animated": False},
    {"name": "LovelyBanana", "animated": False},
    {"name": "AstroKitty", "animated": False},
    {"name": "StarmanMusk", "animated": False},
    {"name": "SpaceJamLola", "animated": False},
    {"name": "LoneDeadSpaceman", "animated": False},
    {"name": "MarcustheMagician", "animated": False},
    {"name": "ZForZombie", "animated": False},
    {"name": "ZombieWolf", "animated": False},
    {"name": "Roger_Smith", "animated": False},
    {"name": "AlienGuy", "animated": False},
    {"name": "MenInBlack", "animated": False},
    {"name": "GoRobot", "animated": False},
    {"name": "OppyTheRover", "animated": False},
    {"name": "Bender", "animated": False},
    {"name": "MeWantCookie", "animated": False},
    {"name": "LaraCroftTombRaider", "animated": False},
    {"name": "EvilFairy", "animated": False},
    {"name": "MissDevil", "animated": False},
    {"name": "DevilInYou", "animated": False},
    {"name": "AvengersHeroes", "animated": False},
    {"name": "BatmanComics", "animated": False},
    {"name": "WonderWomanDC", "animated": False},
    {"name": "ClassicCaptainAmerica", "animated": False},
    {"name": "PartyDog", "animated": False},
    {"name": "VitaminParty", "animated": True},
    {"name": "ReindeerParty", "animated": False},
    {"name": "StayFit", "animated": False},
    {"name": "ElvisKing", "animated": False},
    {"name": "VisserYolandi", "animated": False},
    {"name": "WithTheBeatles", "animated": False},
    {"name": "DavidStarman", "animated": False},
    {"name": "SportEquip", "animated": True},
    {"name": "SportGuy", "animated": False},
    {"name": "TravelingSalesman", "animated": False},
    {"name": "BlondieVacation", "animated": False},
    {"name": "MrBlanket", "animated": False},
    {"name": "MrOlaf", "animated": False},
    {"name": "Winter_Is_Coming", "animated": False},
    {"name": "SnowQ", "animated": False},
    {"name": "TropicalHolidays", "animated": False},
    {"name": "Blahaj", "animated": False},
    {"name": "MrShark", "animated": False},
    {"name": "KoiAndOctopussy", "animated": False},
    {"name": "MexicanAxolotl", "animated": False},
    {"name": "GoldamnFish", "animated": False},
    {"name": "BadassDisney", "animated": False},
    {"name": "SirenInLove", "animated": False},
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
