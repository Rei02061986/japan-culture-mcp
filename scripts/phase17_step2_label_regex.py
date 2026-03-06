"""
Phase 17 Step 2: Extract release_year from label_ja using source-aware regex.

Tiers (processed low->high priority so higher overwrites):
  T1: Semicolon full date  ;YYYY-MM-DD  -> label_semicolon_date
  T2: Japanese era          昭和49年      -> label_jp_era
  T3: Wikidata disambig     (YYYY年の映画) -> label_wd_disambiguation
  T4: Generic YYYY年 (non-JPS only)       -> label_generic_year
"""
import sqlite3
import re
import time
import shutil
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p17_step2.db"
BATCH_SIZE = 10000
YEAR_MIN = 1400
YEAR_MAX = 2026

# Japanese era conversion table
JP_ERA = {
    "明治": 1868,
    "大正": 1912,
    "昭和": 1926,
    "平成": 1989,
    "令和": 2019,
}

# Kanji numeral mapping
KANJI_NUMS = {
    "〇": 0, "一": 1, "二": 2, "三": 3, "四": 4,
    "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
    "十": 10, "百": 100, "元": 1,
}

# JPS source prefixes (newspaper/academic dominated)
JPS_SOURCES = (
    "jps_", "japansearch_", "jps_phase", "jps_date", "jps_p14",
    "jps_broad", "jps_animal", "jps_plant",
)


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


def kanji_to_int(s):
    """Convert kanji numeral string to integer. E.g. 四十九->49, 元->1."""
    if s == "元":
        return 1
    result = 0
    current = 0
    for ch in s:
        if ch in KANJI_NUMS:
            val = KANJI_NUMS[ch]
            if val >= 10:
                if current == 0:
                    current = 1
                result += current * val
                current = 0
            else:
                current = current * 10 + val
        else:
            break
    result += current
    return result if result > 0 else None


def extract_jp_era_year(label):
    """Extract year from Japanese era notation like 昭和49年 or 昭和四十九年."""
    for era_name, base_year in JP_ERA.items():
        # Arabic numeral: 昭和49年
        m = re.search(era_name + r"(\d{1,2})年", label)
        if m:
            era_num = int(m.group(1))
            year = base_year + era_num - 1
            if YEAR_MIN <= year <= YEAR_MAX:
                return year
        # Kanji numeral: 昭和四十九年
        m = re.search(era_name + r"([〇一二三四五六七八九十百元]+)年", label)
        if m:
            era_num = kanji_to_int(m.group(1))
            if era_num:
                year = base_year + era_num - 1
                if YEAR_MIN <= year <= YEAR_MAX:
                    return year
    return None


def extract_semicolon_date(label):
    """Extract year from ;YYYY-MM-DD suffix (newspaper format)."""
    m = re.search(r";(\d{4})-\d{2}-\d{2}", label)
    if m:
        year = int(m.group(1))
        if YEAR_MIN <= year <= YEAR_MAX:
            return year
    # Also try ;YYYY (no month/day)
    m = re.search(r";(\d{4})(?:\s|$|[^\d])", label)
    if m:
        year = int(m.group(1))
        if YEAR_MIN <= year <= YEAR_MAX:
            return year
    return None


def extract_wd_disambiguation(label):
    """Extract year from Wikidata disambiguation like (1973年の映画)."""
    m = re.search(r"[（(](\d{4})年の", label)
    if m:
        year = int(m.group(1))
        if YEAR_MIN <= year <= YEAR_MAX:
            return year
    # Also try [YYYY] bracket format
    m = re.search(r"\[(\d{4})\]", label)
    if m:
        year = int(m.group(1))
        if YEAR_MIN <= year <= YEAR_MAX:
            return year
    return None


def extract_generic_year(label):
    """Extract year from generic YYYY年 pattern."""
    m = re.search(r"(\d{4})年", label)
    if m:
        year = int(m.group(1))
        if YEAR_MIN <= year <= YEAR_MAX:
            return year
    return None


def is_jps_source(source):
    """Check if source is a JapanSearch source."""
    if not source:
        return False
    return any(source.startswith(p) for p in JPS_SOURCES)


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 17 Step 2: Label-based release_year extraction", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = open_db()

    # Counts before
    total_entities = db.execute(
        "SELECT COUNT(*) FROM entities WHERE is_dormant = 0"
    ).fetchone()[0]
    already_set = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]

    print(f"\nActive entities: {total_entities:,}", flush=True)
    print(f"Already with release_year: {already_set:,}", flush=True)

    # Process in tiers (low priority first, high priority overwrites)
    tier_counts = {}

    # === TIER T1: Semicolon dates (lowest priority, highest volume) ===
    print("\n--- Tier T1: Semicolon dates (;YYYY-MM-DD) ---", flush=True)
    cursor = db.execute("""
        SELECT id, label_ja FROM entities
        WHERE is_dormant = 0
          AND release_year IS NULL
          AND label_ja LIKE '%;%'
          AND entity_type = 'work'
    """)
    updates = []
    scanned = 0
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for eid, label in rows:
            scanned += 1
            year = extract_semicolon_date(label)
            if year:
                updates.append((year, "label_semicolon_date", eid))
        if scanned % 200000 == 0:
            print(f"  Scanned: {scanned:,}, found: {len(updates):,}", flush=True)

    print(f"  Total scanned: {scanned:,}, found: {len(updates):,}", flush=True)
    if updates:
        for i in range(0, len(updates), BATCH_SIZE):
            batch = updates[i:i + BATCH_SIZE]
            db.executemany(
                "UPDATE entities SET release_year = ?, release_year_source = ? WHERE id = ?",
                batch,
            )
            db_commit_retry(db)
    tier_counts["T1_semicolon"] = len(updates)
    print(f"  T1 written: {len(updates):,}", flush=True)

    # === TIER T2: Japanese era (medium priority) ===
    print("\n--- Tier T2: Japanese era (明治/大正/昭和/平成/令和) ---", flush=True)
    cursor = db.execute("""
        SELECT id, label_ja FROM entities
        WHERE is_dormant = 0
          AND release_year IS NULL
          AND entity_type = 'work'
    """)
    updates = []
    scanned = 0
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for eid, label in rows:
            if not label:
                continue
            scanned += 1
            # Quick pre-check: does label contain any era name?
            has_era = False
            for era_name in JP_ERA:
                if era_name in label:
                    has_era = True
                    break
            if not has_era:
                continue
            year = extract_jp_era_year(label)
            if year:
                updates.append((year, "label_jp_era", eid))
        if scanned % 500000 == 0:
            print(f"  Scanned: {scanned:,}, found: {len(updates):,}", flush=True)

    print(f"  Total scanned: {scanned:,}, found: {len(updates):,}", flush=True)
    if updates:
        for i in range(0, len(updates), BATCH_SIZE):
            batch = updates[i:i + BATCH_SIZE]
            db.executemany(
                "UPDATE entities SET release_year = ?, release_year_source = ? WHERE id = ?",
                batch,
            )
            db_commit_retry(db)
    tier_counts["T2_jp_era"] = len(updates)
    print(f"  T2 written: {len(updates):,}", flush=True)

    # === TIER T3: Wikidata disambiguation (high priority) ===
    print("\n--- Tier T3: Wikidata disambiguation (YYYY年の映画) ---", flush=True)
    cursor = db.execute("""
        SELECT id, label_ja FROM entities
        WHERE is_dormant = 0
          AND release_year IS NULL
          AND (source LIKE 'wikidata%' OR source LIKE 'wd_%')
    """)
    updates = []
    scanned = 0
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for eid, label in rows:
            if not label:
                continue
            scanned += 1
            year = extract_wd_disambiguation(label)
            if year:
                updates.append((year, "label_wd_disambiguation", eid))

    print(f"  Total scanned: {scanned:,}, found: {len(updates):,}", flush=True)
    if updates:
        db.executemany(
            "UPDATE entities SET release_year = ?, release_year_source = ? WHERE id = ?",
            updates,
        )
        db_commit_retry(db)
    tier_counts["T3_wd_disambig"] = len(updates)
    print(f"  T3 written: {len(updates):,}", flush=True)

    # === TIER T4: Generic YYYY年 for non-JPS sources (highest priority) ===
    print("\n--- Tier T4: Generic YYYY年 (non-JPS only) ---", flush=True)
    cursor = db.execute("""
        SELECT id, label_ja, source FROM entities
        WHERE is_dormant = 0
          AND release_year IS NULL
          AND entity_type IN ('work', 'artifact', 'event', 'film', 'music', 'game')
    """)
    updates = []
    scanned = 0
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for eid, label, source in rows:
            if not label:
                continue
            scanned += 1
            # Skip JPS sources for generic pattern (too noisy)
            if is_jps_source(source):
                continue
            year = extract_generic_year(label)
            if year:
                updates.append((year, "label_generic_year", eid))

    print(f"  Total scanned: {scanned:,}, found: {len(updates):,}", flush=True)
    if updates:
        for i in range(0, len(updates), BATCH_SIZE):
            batch = updates[i:i + BATCH_SIZE]
            db.executemany(
                "UPDATE entities SET release_year = ?, release_year_source = ? WHERE id = ?",
                batch,
            )
            db_commit_retry(db)
    tier_counts["T4_generic"] = len(updates)
    print(f"  T4 written: {len(updates):,}", flush=True)

    # === SUMMARY ===
    total_set = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]

    # Distribution by source
    print("\n--- Source breakdown ---", flush=True)
    rows = db.execute("""
        SELECT release_year_source, COUNT(*) FROM entities
        WHERE release_year IS NOT NULL
        GROUP BY release_year_source ORDER BY COUNT(*) DESC
    """).fetchall()
    for src, cnt in rows:
        print(f"  {str(src):30s} {cnt:>10,}", flush=True)

    # Year distribution (decades)
    print("\n--- Year distribution (decades) ---", flush=True)
    rows = db.execute("""
        SELECT (release_year / 10) * 10 as decade, COUNT(*)
        FROM entities WHERE release_year IS NOT NULL
        GROUP BY decade ORDER BY decade
    """).fetchall()
    for dec, cnt in rows:
        print(f"  {dec}s: {cnt:>10,}", flush=True)

    elapsed = time.time() - t0

    print(f"\n{'='*70}", flush=True)
    print("PHASE 17 STEP 2 SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    for tier, cnt in tier_counts.items():
        print(f"  {tier:25s}: +{cnt:,}", flush=True)
    print(f"  {'Total with release_year':25s}: {total_set:,}", flush=True)
    print(f"  {'Active entities':25s}: {total_entities:,}", flush=True)
    print(f"  {'Coverage':25s}: {100*total_set/max(total_entities,1):.2f}%", flush=True)
    print(f"  Duration: {elapsed:.1f}s", flush=True)

    db.close()

    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)
    print("Phase 17 Step 2 complete.", flush=True)


if __name__ == "__main__":
    main()
