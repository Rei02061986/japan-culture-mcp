"""
Phase 17 Step 3: Match DB entities against local AniList JSON files.

Loads data/anilist/anime.json (19,703 items, 14,223 with seasonYear)
and matches DB entities by exact title.native -> label_ja.
Sets release_year (from seasonYear) and anilist_id.

Source tag: anilist_json
"""
import sqlite3
import json
import time
import shutil
import os
import re
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p17_step3.db"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "anilist")
BATCH_SIZE = 5000


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA cache_size=-64000")
    return db


def db_commit_retry(db, retries=5):
    for i in range(retries):
        try:
            db.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {i+1}: {e}", flush=True)
            time.sleep(3)
    return False


def normalize_title(title):
    """Normalize title for matching: strip whitespace, fullwidth->halfwidth."""
    if not title:
        return None
    t = title.strip()
    # Fullwidth alphanumeric -> halfwidth
    result = []
    for ch in t:
        cp = ord(ch)
        if 0xFF01 <= cp <= 0xFF5E:
            result.append(chr(cp - 0xFEE0))
        elif ch == "\u3000":  # fullwidth space
            result.append(" ")
        else:
            result.append(ch)
    return "".join(result)


def load_anilist_json(filename):
    """Load AniList JSON and build lookup dicts."""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"  WARNING: {path} not found", flush=True)
        return {}, {}

    with open(path, "r", encoding="utf-8") as f:
        items = json.load(f)

    print(f"  Loaded {len(items):,} items from {filename}", flush=True)

    # Build native title -> item lookup
    native_lookup = {}
    normalized_lookup = {}
    with_year = 0

    for item in items:
        titles = item.get("title", {})
        native = titles.get("native")
        season_year = item.get("seasonYear")
        start_date = item.get("startDate", {})
        start_year = start_date.get("year") if isinstance(start_date, dict) else None
        year = season_year or start_year
        anilist_id = item.get("id")

        if year:
            with_year += 1

        entry = {
            "anilist_id": anilist_id,
            "year": year,
            "format": item.get("format"),
            "native": native,
            "romaji": titles.get("romaji"),
            "english": titles.get("english"),
        }

        if native:
            native_lookup[native] = entry
            norm = normalize_title(native)
            if norm and norm != native:
                normalized_lookup[norm] = entry

    print(f"  Native titles: {len(native_lookup):,}", flush=True)
    print(f"  Normalized titles: {len(normalized_lookup):,}", flush=True)
    print(f"  With year: {with_year:,}", flush=True)

    return native_lookup, normalized_lookup


def strip_disambiguation(label):
    """Strip Wikidata disambiguation suffix like (1973年の映画)."""
    return re.sub(r"\s*[（(][^）)]*[）)]$", "", label).strip()


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 17 Step 3: AniList Local JSON Matching", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    # Load AniList data
    print("\nLoading AniList JSON files...", flush=True)
    anime_native, anime_norm = load_anilist_json("anime.json")
    manga_native, manga_norm = load_anilist_json("manga.json")

    # Merge all lookups (anime has priority for year data)
    all_native = {}
    all_norm = {}
    # manga first (lower priority)
    all_native.update(manga_native)
    all_norm.update(manga_norm)
    # anime overwrites (higher priority -- has seasonYear)
    all_native.update(anime_native)
    all_norm.update(anime_norm)

    print(f"\nCombined lookup: {len(all_native):,} native, {len(all_norm):,} normalized",
          flush=True)

    # Copy DB
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = open_db()

    # Get counts before
    already_set = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]
    anilist_set = db.execute(
        "SELECT COUNT(*) FROM entities WHERE anilist_id IS NOT NULL AND is_dormant=0"
    ).fetchone()[0]
    print(f"\nBefore: release_year set: {already_set:,}, anilist_id set: {anilist_set:,}",
          flush=True)

    # Query candidate entities (creative work sources)
    print("\nQuerying candidate entities...", flush=True)
    cursor = db.execute("""
        SELECT id, label_ja, label_en FROM entities
        WHERE is_dormant = 0
          AND entity_type IN ('work', 'film', 'music', 'game', 'character')
          AND (source LIKE 'madb%'
               OR source LIKE 'wikidata_media%'
               OR source LIKE 'wd_tv%'
               OR source LIKE 'wd_film%'
               OR source LIKE 'wd_anime%'
               OR source LIKE 'wd_manga%'
               OR source LIKE 'wd_game%'
               OR source LIKE 'wd_music%'
               OR source LIKE 'wd_phase9%'
               OR source LIKE 'aozora%'
               OR source LIKE 'colbase%')
    """)

    updates_year = []  # (year, source_tag, anilist_id, entity_id)
    updates_id_only = []  # (anilist_id, entity_id) -- matched but no year
    matched = 0
    scanned = 0

    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for eid, label_ja, label_en in rows:
            scanned += 1

            if not label_ja:
                continue

            entry = None
            # Try exact native match
            if label_ja in all_native:
                entry = all_native[label_ja]
            else:
                # Try normalized match
                norm = normalize_title(label_ja)
                if norm in all_norm:
                    entry = all_norm[norm]
                elif norm in all_native:
                    entry = all_native[norm]
                else:
                    # Try stripping disambiguation
                    stripped = strip_disambiguation(label_ja)
                    if stripped != label_ja:
                        if stripped in all_native:
                            entry = all_native[stripped]
                        else:
                            norm_s = normalize_title(stripped)
                            if norm_s in all_norm:
                                entry = all_norm[norm_s]

            if entry:
                matched += 1
                anilist_id = entry["anilist_id"]
                year = entry["year"]
                if year and 1900 <= year <= 2026:
                    updates_year.append((year, "anilist_json", anilist_id, eid))
                else:
                    updates_id_only.append((anilist_id, eid))

        if scanned % 100000 == 0:
            print(f"  Scanned: {scanned:,}, matched: {matched:,}", flush=True)

    print(f"\nTotal scanned: {scanned:,}", flush=True)
    print(f"Matched: {matched:,}", flush=True)
    print(f"  With year: {len(updates_year):,}", flush=True)
    print(f"  ID only (no year): {len(updates_id_only):,}", flush=True)

    # Write updates
    print("\nWriting updates...", flush=True)

    # Year + anilist_id updates
    for i in range(0, len(updates_year), BATCH_SIZE):
        batch = updates_year[i:i + BATCH_SIZE]
        db.executemany("""
            UPDATE entities
            SET release_year = ?, release_year_source = ?, anilist_id = ?
            WHERE id = ?
        """, batch)
        db_commit_retry(db)

    # ID-only updates (anilist_id but no year)
    for i in range(0, len(updates_id_only), BATCH_SIZE):
        batch = updates_id_only[i:i + BATCH_SIZE]
        db.executemany("""
            UPDATE entities SET anilist_id = ? WHERE id = ?
        """, batch)
        db_commit_retry(db)

    # Counts after
    year_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]
    anilist_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE anilist_id IS NOT NULL AND is_dormant=0"
    ).fetchone()[0]

    # Breakdown by source type
    anime_count = sum(1 for _, _, aid, _ in updates_year if aid in
                      {e["anilist_id"] for e in anime_native.values()})
    manga_count = len(updates_year) - anime_count
    print(f"\n  Anime matches with year: ~{anime_count:,}", flush=True)
    print(f"  Manga matches with year: ~{manga_count:,}", flush=True)

    elapsed = time.time() - t0

    print(f"\n{'='*70}", flush=True)
    print("PHASE 17 STEP 3 SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  Candidates scanned:   {scanned:,}", flush=True)
    print(f"  Matched (total):      {matched:,}", flush=True)
    print(f"  With release_year:    +{len(updates_year):,}", flush=True)
    print(f"  anilist_id only:      +{len(updates_id_only):,}", flush=True)
    print(f"  release_year: {already_set:,} -> {year_after:,} (+{year_after - already_set:,})",
          flush=True)
    print(f"  anilist_id:   {anilist_set:,} -> {anilist_after:,} (+{anilist_after - anilist_set:,})",
          flush=True)
    print(f"  Duration: {elapsed:.1f}s", flush=True)

    db.close()

    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)
    print("Phase 17 Step 3 complete.", flush=True)


if __name__ == "__main__":
    main()
