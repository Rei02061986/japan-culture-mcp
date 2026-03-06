"""
Phase 17 Step 4: Re-fetch AniList manga data with startDate.year field.

The original data/anilist/manga.json has 0% seasonYear coverage for manga.
This script re-fetches with startDate included, then matches to DB entities.

Rate limit: 90 req/min (0.7s interval). ~760 pages for manga.
Saves to data/anilist/manga_v2.json.

Source tag: anilist_manga_api
"""
import sqlite3
import json
import time
import shutil
import os
import re
import requests
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p17_step4.db"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "anilist")
MANGA_V2_PATH = os.path.join(DATA_DIR, "manga_v2.json")
BATCH_SIZE = 5000
ANILIST_URL = "https://graphql.anilist.co"

QUERY = """
query ($page: Int, $perPage: Int, $type: MediaType) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    media(type: $type, countryOfOrigin: JP, sort: POPULARITY_DESC) {
      id
      title {
        romaji
        english
        native
      }
      format
      status
      seasonYear
      season
      startDate {
        year
        month
        day
      }
      genres
      averageScore
      popularity
      source
    }
  }
}
"""


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


def fetch_page(page, per_page=50):
    """Fetch one page of manga from AniList."""
    for attempt in range(5):
        try:
            resp = requests.post(
                ANILIST_URL,
                json={
                    "query": QUERY,
                    "variables": {
                        "page": page,
                        "perPage": per_page,
                        "type": "MANGA",
                    },
                },
                timeout=30,
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                print(f"    429 rate limited, waiting {retry_after}s...", flush=True)
                time.sleep(retry_after)
                continue

            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"    HTTP {resp.status_code}, attempt {attempt+1}", flush=True)
                time.sleep(5 * (attempt + 1))
        except Exception as e:
            print(f"    Error page {page}: {e}", flush=True)
            time.sleep(5 * (attempt + 1))
    return None


def fetch_all_manga():
    """Fetch all manga from AniList with startDate."""
    all_media = []
    page = 1

    while True:
        if page % 50 == 1:
            print(f"  Fetching page {page}, total so far: {len(all_media):,}...", flush=True)

        data = fetch_page(page)
        if not data:
            print(f"  Failed at page {page}, stopping.", flush=True)
            break

        page_data = data.get("data", {}).get("Page", {})
        page_info = page_data.get("pageInfo", {})
        media_list = page_data.get("media", [])

        if not media_list:
            break

        all_media.extend(media_list)

        if page % 100 == 0:
            total_est = page_info.get("total", "?")
            print(f"    Progress: {len(all_media):,}/{total_est}", flush=True)

        if not page_info.get("hasNextPage", False):
            break

        page += 1
        time.sleep(0.7)  # Rate limit: 90 req/min

    return all_media


def normalize_title(title):
    """Normalize title for matching."""
    if not title:
        return None
    t = title.strip()
    result = []
    for ch in t:
        cp = ord(ch)
        if 0xFF01 <= cp <= 0xFF5E:
            result.append(chr(cp - 0xFEE0))
        elif ch == "\u3000":
            result.append(" ")
        else:
            result.append(ch)
    return "".join(result)


def strip_disambiguation(label):
    """Strip Wikidata disambiguation suffix."""
    return re.sub(r"\s*[（(][^）)]*[）)]$", "", label).strip()


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 17 Step 4: AniList Manga Re-fetch with startDate", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    # === Phase 1: Fetch manga data ===
    if os.path.exists(MANGA_V2_PATH):
        print(f"\nmanga_v2.json already exists ({os.path.getsize(MANGA_V2_PATH):,} bytes).", flush=True)
        print("Loading existing file...", flush=True)
        with open(MANGA_V2_PATH, "r", encoding="utf-8") as f:
            manga_data = json.load(f)
        print(f"  Loaded {len(manga_data):,} items.", flush=True)
    else:
        print("\nFetching manga from AniList API...", flush=True)
        manga_data = fetch_all_manga()
        print(f"\nFetched {len(manga_data):,} manga items.", flush=True)

        # Save to file
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(MANGA_V2_PATH, "w", encoding="utf-8") as f:
            json.dump(manga_data, f, ensure_ascii=False)
        print(f"Saved to {MANGA_V2_PATH} ({os.path.getsize(MANGA_V2_PATH):,} bytes)", flush=True)

    # Analyze startDate coverage
    with_start_year = 0
    for item in manga_data:
        sd = item.get("startDate")
        if isinstance(sd, dict) and sd.get("year"):
            with_start_year += 1
    print(f"\nManga with startDate.year: {with_start_year:,}/{len(manga_data):,} "
          f"({100*with_start_year/max(len(manga_data),1):.1f}%)", flush=True)

    # Build lookup dicts
    print("\nBuilding lookup dicts...", flush=True)
    native_lookup = {}
    norm_lookup = {}
    for item in manga_data:
        titles = item.get("title", {})
        native = titles.get("native")
        sd = item.get("startDate")
        year = sd.get("year") if isinstance(sd, dict) else None
        anilist_id = item.get("id")

        entry = {
            "anilist_id": anilist_id,
            "year": year,
            "format": item.get("format"),
        }

        if native:
            native_lookup[native] = entry
            norm = normalize_title(native)
            if norm and norm != native:
                norm_lookup[norm] = entry

    print(f"  Native titles: {len(native_lookup):,}", flush=True)
    print(f"  Normalized: {len(norm_lookup):,}", flush=True)

    # === Phase 2: Match to DB ===
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = open_db()

    year_before = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]
    print(f"\nrelease_year before: {year_before:,}", flush=True)

    # Query candidate entities -- focus on sources likely to have manga
    print("\nQuerying candidate entities...", flush=True)
    cursor = db.execute("""
        SELECT id, label_ja FROM entities
        WHERE is_dormant = 0
          AND entity_type = 'work'
          AND (source LIKE 'madb%'
               OR source LIKE 'wikidata_media%'
               OR source LIKE 'wd_manga%'
               OR source LIKE 'wd_phase9%')
    """)

    updates = []
    updates_id = []
    matched = 0
    scanned = 0

    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for eid, label_ja in rows:
            if not label_ja:
                continue
            scanned += 1

            entry = None
            if label_ja in native_lookup:
                entry = native_lookup[label_ja]
            else:
                norm = normalize_title(label_ja)
                if norm in norm_lookup:
                    entry = norm_lookup[norm]
                elif norm in native_lookup:
                    entry = native_lookup[norm]
                else:
                    stripped = strip_disambiguation(label_ja)
                    if stripped != label_ja and stripped in native_lookup:
                        entry = native_lookup[stripped]

            if entry:
                matched += 1
                anilist_id = entry["anilist_id"]
                year = entry["year"]
                if year and 1900 <= year <= 2026:
                    updates.append((year, "anilist_manga_api", anilist_id, eid))
                else:
                    updates_id.append((anilist_id, eid))

        if scanned % 50000 == 0:
            print(f"  Scanned: {scanned:,}, matched: {matched:,}", flush=True)

    print(f"\nTotal scanned: {scanned:,}", flush=True)
    print(f"Matched: {matched:,}", flush=True)
    print(f"  With year: {len(updates):,}", flush=True)
    print(f"  ID only: {len(updates_id):,}", flush=True)

    # Write updates (only where release_year IS NULL to avoid overwriting Step 3)
    print("\nWriting updates (release_year IS NULL only)...", flush=True)
    written = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        db.executemany("""
            UPDATE entities
            SET release_year = ?, release_year_source = ?, anilist_id = ?
            WHERE id = ? AND release_year IS NULL
        """, batch)
        db_commit_retry(db)

    for i in range(0, len(updates_id), BATCH_SIZE):
        batch = updates_id[i:i + BATCH_SIZE]
        db.executemany("""
            UPDATE entities SET anilist_id = ?
            WHERE id = ? AND anilist_id IS NULL
        """, batch)
        db_commit_retry(db)

    year_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]

    elapsed = time.time() - t0

    print(f"\n{'='*70}", flush=True)
    print("PHASE 17 STEP 4 SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  Manga fetched from API:   {len(manga_data):,}", flush=True)
    print(f"  With startDate.year:      {with_start_year:,}", flush=True)
    print(f"  DB candidates scanned:    {scanned:,}", flush=True)
    print(f"  Matched:                  {matched:,}", flush=True)
    print(f"  release_year written:     +{len(updates):,}", flush=True)
    print(f"  release_year: {year_before:,} -> {year_after:,} (+{year_after - year_before:,})",
          flush=True)
    print(f"  Duration: {elapsed:.1f}s", flush=True)

    db.close()

    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)
    print("Phase 17 Step 4 complete.", flush=True)


if __name__ == "__main__":
    main()
