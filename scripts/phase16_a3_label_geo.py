"""
Phase 16 A3: Geo-enrich entities via label_ja place-name matching.

Strategy: Entities whose label_ja contains a modern prefecture name
(with suffix) or an old province name get approximate coordinates for
that region. Longer prefecture names are matched first to avoid
partial-match collisions (e.g. "神奈川" before "神"). Each matched
entity gets a unique grid-offset coordinate so they spread across the
region rather than stacking on one point.

Source: p16_label_geo
"""
import sqlite3
import time
import shutil
import os
import math
from datetime import datetime

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p16.db"
BATCH_SIZE = 5000

# Modern prefecture names -> (lat, lon)
# Suffixes (県/都/府/道) are appended during search.
PREFECTURE_MAP = {
    '北海道': (43.0642, 141.3469),
    '青森': (40.8244, 140.7400),
    '岩手': (39.7036, 141.1527),
    '宮城': (38.2688, 140.8721),
    '秋田': (39.7186, 140.1024),
    '山形': (38.2405, 140.3634),
    '福島': (37.7503, 140.4676),
    '茨城': (36.3418, 140.4468),
    '栃木': (36.5657, 139.8836),
    '群馬': (36.3911, 139.0608),
    '埼玉': (35.8569, 139.6489),
    '千葉': (35.6047, 140.1233),
    '東京': (35.6762, 139.6503),
    '神奈川': (35.4478, 139.6425),
    '新潟': (37.9026, 139.0236),
    '富山': (36.6953, 137.2113),
    '石川': (36.5946, 136.6256),
    '福井': (36.0652, 136.2219),
    '山梨': (35.6642, 138.5684),
    '長野': (36.2325, 138.1812),
    '岐阜': (35.3912, 136.7223),
    '静岡': (34.9769, 138.3831),
    '愛知': (35.1802, 136.9066),
    '三重': (34.7303, 136.5086),
    '滋賀': (35.0045, 135.8686),
    '京都': (35.0116, 135.7681),
    '大阪': (34.6937, 135.5023),
    '兵庫': (34.6913, 135.1830),
    '奈良': (34.6851, 135.8049),
    '和歌山': (34.2261, 135.1675),
    '鳥取': (35.5039, 134.2383),
    '島根': (35.4723, 133.0505),
    '岡山': (34.6617, 133.9350),
    '広島': (34.3966, 132.4596),
    '山口': (34.1861, 131.4714),
    '徳島': (34.0658, 134.5593),
    '香川': (34.3402, 134.0434),
    '愛媛': (33.8416, 132.7657),
    '高知': (33.5597, 133.5311),
    '福岡': (33.6064, 130.4183),
    '佐賀': (33.2494, 130.2988),
    '長崎': (32.7503, 129.8779),
    '熊本': (32.7898, 130.7417),
    '大分': (33.2382, 131.6126),
    '宮崎': (31.9111, 131.4239),
    '鹿児島': (31.5602, 130.5581),
    '沖縄': (26.3344, 127.8056),
}

# Old province names -> (lat, lon)
OLD_PROVINCE_MAP = {
    '蝦夷': (43.0, 141.3),
    '陸奥': (40.8, 140.7),
    '出羽': (38.2, 140.3),
    '下野': (36.6, 139.9),
    '上野': (36.4, 139.1),
    '武蔵': (35.7, 139.7),
    '相模': (35.4, 139.3),
    '越後': (37.9, 139.0),
    '越中': (36.7, 137.2),
    '能登': (37.0, 136.8),
    '加賀': (36.6, 136.6),
    '越前': (36.1, 136.2),
    '甲斐': (35.7, 138.6),
    '信濃': (36.2, 138.2),
    '飛騨': (36.1, 137.3),
    '美濃': (35.4, 136.8),
    '駿河': (35.0, 138.4),
    '遠江': (34.8, 137.8),
    '三河': (34.8, 137.2),
    '尾張': (35.2, 136.9),
    '伊勢': (34.7, 136.5),
    '近江': (35.0, 135.9),
    '山城': (35.0, 135.8),
    '大和': (34.7, 135.8),
    '摂津': (34.7, 135.5),
    '河内': (34.6, 135.6),
    '和泉': (34.5, 135.5),
    '播磨': (34.8, 134.7),
    '紀伊': (34.2, 135.2),
    '因幡': (35.5, 134.2),
    '伯耆': (35.4, 133.5),
    '出雲': (35.4, 132.8),
    '備前': (34.7, 134.0),
    '備中': (34.7, 133.6),
    '安芸': (34.4, 132.5),
    '周防': (34.2, 131.5),
    '長門': (34.4, 131.2),
    '阿波': (34.1, 134.6),
    '讃岐': (34.3, 134.0),
    '伊予': (33.8, 132.8),
    '土佐': (33.6, 133.5),
    '筑前': (33.6, 130.4),
    '筑後': (33.3, 130.5),
    '肥前': (33.2, 130.3),
    '肥後': (32.8, 130.7),
    '豊前': (33.6, 131.0),
    '豊後': (33.2, 131.6),
    '日向': (31.9, 131.4),
    '大隅': (31.4, 130.7),
    '薩摩': (31.6, 130.6),
    '琉球': (26.3, 127.8),
}

# Special suffix mapping for prefectures
SPECIAL_SUFFIX = {
    '北海道': '道',
    '東京': '都',
    '大阪': '府',
    '京都': '府',
}


def open_db():
    db = sqlite3.connect(TMP_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
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


def compute_grid_coords(base_lat, base_lon, count):
    """Return list of (lat, lon) spread on a grid centred on base point.

    For N entities, creates a ceil(sqrt(N)) x ceil(sqrt(N)) grid with
    0.01 degree spacing (~1 km) centred on (base_lat, base_lon).
    """
    if count == 0:
        return []

    grid_size = max(1, math.ceil(math.sqrt(count)))
    half = grid_size / 2.0
    spacing = 0.01

    coords = []
    for i in range(count):
        row = i % grid_size
        col = i // grid_size
        lat = base_lat + (row - half) * spacing
        lon = base_lon + (col - half) * spacing
        coords.append((lat, lon))
    return coords


def build_prefecture_patterns():
    """Build LIKE patterns for modern prefectures, sorted longest name first.

    Returns list of (pattern_string, prefecture_name, lat, lon).
    Sorted by name length descending so "神奈川" matches before "神",
    "和歌山" before "和", "鹿児島" before "鹿", etc.
    """
    patterns = []
    for name, (lat, lon) in PREFECTURE_MAP.items():
        suffix = SPECIAL_SUFFIX.get(name, '県')
        if name == '北海道':
            # Hokkaido: match "北海道" directly (already includes suffix)
            pattern = '%北海道%'
        else:
            pattern = '%' + name + suffix + '%'
        patterns.append((pattern, name + suffix, lat, lon))

    # Sort by pattern specificity: longest name first
    patterns.sort(key=lambda x: len(x[1]), reverse=True)
    return patterns


def build_old_province_patterns():
    """Build LIKE patterns for old province names, sorted longest name first.

    Returns list of (pattern_string, province_name, lat, lon).
    """
    patterns = []
    for name, (lat, lon) in OLD_PROVINCE_MAP.items():
        pattern = '%' + name + '%'
        patterns.append((pattern, name, lat, lon))

    patterns.sort(key=lambda x: len(x[1]), reverse=True)
    return patterns


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 16 A3: Geo-Enrich Entities via Label Place-Name Matching", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    # --- Copy DB to /tmp ---
    print(f"\nCopying DB to {TMP_DB} ...", flush=True)
    shutil.copy2(SRC_DB, TMP_DB)
    print("  Done.", flush=True)

    db = open_db()

    # --- Counts before ---
    entity_count = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    geo_before = db.execute(
        "SELECT COUNT(*) FROM entities WHERE lat IS NOT NULL"
    ).fetchone()[0]
    no_geo_active = db.execute(
        "SELECT COUNT(*) FROM entities WHERE lat IS NULL AND is_dormant = 0"
    ).fetchone()[0]
    print(f"\nTotal entities:              {entity_count:,}", flush=True)
    print(f"Entities with coords:        {geo_before:,}", flush=True)
    print(f"Active without coords:       {no_geo_active:,}", flush=True)

    # Track already-matched entity IDs to avoid double-matching
    matched_ids = set()
    total_updated = 0

    # =====================================================================
    # Strategy A: Modern prefecture names in label_ja
    # =====================================================================
    print(f"\n{'=' * 60}", flush=True)
    print("Strategy A: Modern prefecture names (label_ja LIKE '%XX県%')", flush=True)
    print("=" * 60, flush=True)

    pref_patterns = build_prefecture_patterns()
    strategy_a_total = 0

    for pattern, display_name, base_lat, base_lon in pref_patterns:
        # Find entity IDs matching this pattern, no coords, not dormant
        rows = db.execute("""
            SELECT id FROM entities
            WHERE label_ja LIKE ?
              AND lat IS NULL
              AND is_dormant = 0
        """, (pattern,)).fetchall()

        # Filter out already-matched
        entity_ids = [row[0] for row in rows if row[0] not in matched_ids]

        if not entity_ids:
            continue

        # Compute grid coordinates
        grid_coords = compute_grid_coords(base_lat, base_lon, len(entity_ids))

        # Build batch
        batch = []
        for idx, eid in enumerate(entity_ids):
            lat, lon = grid_coords[idx]
            batch.append((lat, lon, eid))
            matched_ids.add(eid)

        # Execute in chunks
        for chunk_start in range(0, len(batch), BATCH_SIZE):
            chunk = batch[chunk_start:chunk_start + BATCH_SIZE]
            db.executemany(
                "UPDATE entities SET lat = ?, lon = ? WHERE id = ?",
                chunk
            )
            db_commit_retry(db)

        strategy_a_total += len(entity_ids)
        print(f"  {display_name:6s}  -> {len(entity_ids):>7,} entities "
              f"(base: {base_lat:.4f}, {base_lon:.4f})", flush=True)

    total_updated += strategy_a_total
    print(f"\n  Strategy A total: +{strategy_a_total:,} entities", flush=True)

    # =====================================================================
    # Strategy B: Old province names in label_ja
    # =====================================================================
    print(f"\n{'=' * 60}", flush=True)
    print("Strategy B: Old province names (label_ja LIKE '%旧国名%')", flush=True)
    print("=" * 60, flush=True)

    old_patterns = build_old_province_patterns()
    strategy_b_total = 0

    for pattern, display_name, base_lat, base_lon in old_patterns:
        # Find entity IDs matching this pattern, no coords, not dormant
        rows = db.execute("""
            SELECT id FROM entities
            WHERE label_ja LIKE ?
              AND lat IS NULL
              AND is_dormant = 0
        """, (pattern,)).fetchall()

        # Filter out already-matched (from Strategy A or earlier in B)
        entity_ids = [row[0] for row in rows if row[0] not in matched_ids]

        if not entity_ids:
            continue

        # Compute grid coordinates
        grid_coords = compute_grid_coords(base_lat, base_lon, len(entity_ids))

        # Build batch
        batch = []
        for idx, eid in enumerate(entity_ids):
            lat, lon = grid_coords[idx]
            batch.append((lat, lon, eid))
            matched_ids.add(eid)

        # Execute in chunks
        for chunk_start in range(0, len(batch), BATCH_SIZE):
            chunk = batch[chunk_start:chunk_start + BATCH_SIZE]
            db.executemany(
                "UPDATE entities SET lat = ?, lon = ? WHERE id = ?",
                chunk
            )
            db_commit_retry(db)

        strategy_b_total += len(entity_ids)
        print(f"  {display_name:4s}  -> {len(entity_ids):>7,} entities "
              f"(base: {base_lat:.4f}, {base_lon:.4f})", flush=True)

    total_updated += strategy_b_total
    print(f"\n  Strategy B total: +{strategy_b_total:,} entities", flush=True)

    # --- Overlap report ---
    # (No overlaps possible since matched_ids prevents double-matching,
    # but report how many Strategy B candidates were already matched by A.)
    print(f"\n  Overlap prevention: matched_ids set size = {len(matched_ids):,}", flush=True)

    # --- Counts after ---
    geo_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE lat IS NOT NULL"
    ).fetchone()[0]
    elapsed = time.time() - t0

    # --- Summary ---
    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 16 A3 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Total entities:            {entity_count:,}", flush=True)
    print(f"  Geo-entities before:       {geo_before:,}", flush=True)
    print(f"  Geo-entities after:        {geo_after:,}", flush=True)
    print(f"  Strategy A (prefectures): +{strategy_a_total:,}", flush=True)
    print(f"  Strategy B (old provinces):+{strategy_b_total:,}", flush=True)
    print(f"  Total newly geo-enriched: +{total_updated:,}", flush=True)
    print(f"  Duration:                  {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 16 A3 complete.", flush=True)


if __name__ == "__main__":
    main()
