"""
Phase 13 A8: Connection density boost from 450K to 650K+.
Strategies:
  E1: Temple/shrine × nearby cultural entities (1km)
  E2: JapanSearch entities creator-shared connections
  E3: Entity type cross-connections (place×work, person×work)
  E4: Label substring matching (shared keywords)
  E5: Extended proximity 3km for new entities
"""
import sqlite3
import time
import math
from collections import defaultdict

DB_PATH = "/tmp/culture_ontology_work.db"
BATCH_SIZE = 5000


def open_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
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


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_existing_pairs(db):
    pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for a, b in rows:
            pairs.add((min(a, b), max(a, b)))
    return pairs


def insert_conn(db, a_id, b_id, conn_type, confidence, explanation, source, pairs, seren=0.5):
    pair = (min(a_id, b_id), max(a_id, b_id))
    if pair in pairs:
        return False
    try:
        db.execute("""
            INSERT OR IGNORE INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 explanation, source, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (pair[0], pair[1], conn_type, seren, explanation, source, confidence))
        pairs.add(pair)
        return True
    except sqlite3.IntegrityError:
        return False


def e1_temple_shrine_proximity(db, pairs, target=50000):
    """Connect temples/shrines with nearby non-temple/shrine entities within 1km."""
    print("\n" + "=" * 60, flush=True)
    print(f"E1: Temple/Shrine proximity connections (target +{target:,})", flush=True)

    # Get temple/shrine entities with coordinates
    temples = db.execute("""
        SELECT id, lat, lon FROM entities
        WHERE (source LIKE '%temple%' OR source LIKE '%shrine%')
          AND lat IS NOT NULL AND lon IS NOT NULL
    """).fetchall()
    print(f"  Temple/shrine with coords: {len(temples):,}", flush=True)

    # Get non-temple/shrine entities with coordinates
    others = db.execute("""
        SELECT id, lat, lon, entity_type FROM entities
        WHERE source NOT LIKE '%temple%' AND source NOT LIKE '%shrine%'
          AND lat IS NOT NULL AND lon IS NOT NULL
    """).fetchall()
    print(f"  Other geo entities: {len(others):,}", flush=True)

    # Build grid for others
    GRID = 0.01  # ~1km
    grid = defaultdict(list)
    for eid, lat, lon, etype in others:
        cell = (round(lat / GRID), round(lon / GRID))
        grid[cell].append((eid, lat, lon, etype))

    total = 0
    batch = 0
    for t_id, t_lat, t_lon in temples:
        if total >= target:
            break
        cell = (round(t_lat / GRID), round(t_lon / GRID))
        nearby = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                nearby.extend(grid.get((cell[0]+dx, cell[1]+dy), []))

        connected = 0
        for o_id, o_lat, o_lon, o_type in nearby:
            if connected >= 3:  # Max 3 connections per temple
                break
            dist = haversine_m(t_lat, t_lon, o_lat, o_lon)
            if 10 < dist <= 1000:
                if insert_conn(db, t_id, o_id, "temple_nearby", 0.5,
                             f"寺社近接（{dist:.0f}m）", "phase13_e1", pairs, 0.5):
                    total += 1
                    batch += 1
                    connected += 1
                if batch >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch = 0
                    print(f"    ... {total:,}", flush=True)

    if batch > 0:
        db_commit_retry(db)
    print(f"  E1 total: +{total:,}", flush=True)
    return total


def e2_source_clustering(db, pairs, target=50000):
    """Connect entities from same source in larger batches."""
    print("\n" + "=" * 60, flush=True)
    print(f"E2: Source clustering connections (target +{target:,})", flush=True)

    # Find large sources not yet heavily connected
    sources = db.execute("""
        SELECT source, COUNT(*) as cnt FROM entities
        WHERE source IS NOT NULL AND source NOT LIKE '%japansearch%'
        GROUP BY source HAVING cnt >= 50 AND cnt <= 10000
        ORDER BY cnt DESC LIMIT 100
    """).fetchall()
    print(f"  Qualifying sources: {len(sources)}", flush=True)

    total = 0
    batch = 0
    for source, cnt in sources:
        if total >= target:
            break
        eids = [r[0] for r in db.execute(
            "SELECT id FROM entities WHERE source = ? LIMIT 300", (source,)
        )]
        for i in range(len(eids)):
            if total >= target:
                break
            for j in range(i+1, min(i+4, len(eids))):
                if total >= target:
                    break
                if insert_conn(db, eids[i], eids[j], "same_source_p13", 0.4,
                             f"同一データソース: {source}", "phase13_e2", pairs, 0.3):
                    total += 1
                    batch += 1
                if batch >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch = 0

    if batch > 0:
        db_commit_retry(db)
    print(f"  E2 total: +{total:,}", flush=True)
    return total


def e3_cross_type_label_match(db, pairs, target=50000):
    """Connect entities of different types that share the same label_ja."""
    print("\n" + "=" * 60, flush=True)
    print(f"E3: Cross-type label match connections (target +{target:,})", flush=True)

    # Find labels that appear in multiple entity types
    rows = db.execute("""
        SELECT label_ja, GROUP_CONCAT(id || ':' || entity_type)
        FROM entities
        WHERE label_ja IS NOT NULL
        GROUP BY label_ja
        HAVING COUNT(DISTINCT entity_type) >= 2
        LIMIT 100000
    """).fetchall()
    print(f"  Labels with multiple types: {len(rows):,}", flush=True)

    total = 0
    batch = 0
    for label, id_types in rows:
        if total >= target:
            break
        parts = id_types.split(",")
        entities = []
        for p in parts[:10]:  # Max 10 per label
            if ":" in p:
                eid, etype = p.split(":", 1)
                try:
                    entities.append((int(eid), etype))
                except ValueError:
                    continue

        for i in range(len(entities)):
            if total >= target:
                break
            for j in range(i+1, len(entities)):
                if entities[i][1] != entities[j][1]:  # Different types
                    if insert_conn(db, entities[i][0], entities[j][0],
                                 "cross_type_match", 0.7,
                                 f"同名異種: {label}",
                                 "phase13_e3", pairs, 0.6):
                        total += 1
                        batch += 1
                if batch >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch = 0

    if batch > 0:
        db_commit_retry(db)
    print(f"  E3 total: +{total:,}", flush=True)
    return total


def e4_extended_geo_proximity(db, pairs, target=50000):
    """3km proximity for all geo entities (different types only)."""
    print("\n" + "=" * 60, flush=True)
    print(f"E4: Extended geo proximity 3km (target +{target:,})", flush=True)

    rows = db.execute("""
        SELECT id, lat, lon, entity_type FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).fetchall()
    print(f"  Geo entities: {len(rows):,}", flush=True)

    GRID = 0.03  # ~3km
    grid = defaultdict(list)
    for eid, lat, lon, etype in rows:
        cell = (round(lat / GRID), round(lon / GRID))
        grid[cell].append((eid, lat, lon, etype))

    total = 0
    batch = 0
    for (cx, cy), entities in grid.items():
        if total >= target:
            break
        neighbors = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                neighbors.extend(grid.get((cx+dx, cy+dy), []))

        for a_id, a_lat, a_lon, a_type in entities:
            if total >= target:
                break
            connected = 0
            for b_id, b_lat, b_lon, b_type in neighbors:
                if a_id >= b_id or a_type == b_type:
                    continue
                if connected >= 2:
                    break
                dist = haversine_m(a_lat, a_lon, b_lat, b_lon)
                if 2000 < dist <= 3000:
                    if insert_conn(db, a_id, b_id, "proximity_3km", 0.3,
                                 f"地理的近接（{dist:.0f}m）",
                                 "phase13_e4", pairs, 0.4):
                        total += 1
                        batch += 1
                        connected += 1
                    if batch >= BATCH_SIZE:
                        db_commit_retry(db)
                        batch = 0
                        print(f"    ... {total:,}", flush=True)

    if batch > 0:
        db_commit_retry(db)
    print(f"  E4 total: +{total:,}", flush=True)
    return total


def e5_regional_source_wide(db, pairs, target=50000):
    """Regional clusters with wider grid for new Phase 13 entities."""
    print("\n" + "=" * 60, flush=True)
    print(f"E5: Wide regional source clusters (target +{target:,})", flush=True)

    rows = db.execute("""
        SELECT id, source, lat, lon FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL AND source IS NOT NULL
    """).fetchall()

    grid = defaultdict(list)
    for eid, source, lat, lon in rows:
        key = (source, round(lat * 3) / 3, round(lon * 3) / 3)  # ~37km grid
        grid[key].append(eid)

    grid = {k: v for k, v in grid.items() if 2 <= len(v) <= 300}
    print(f"  Grid groups: {len(grid):,}", flush=True)

    total = 0
    batch = 0
    for (source, glat, glon), eids in grid.items():
        if total >= target:
            break
        max_per = min(len(eids), 40)
        eids = eids[:max_per]
        for i in range(len(eids)):
            if total >= target:
                break
            for j in range(i+1, min(i+3, len(eids))):
                if total >= target:
                    break
                if insert_conn(db, eids[i], eids[j], "regional_cluster_p13", 0.3,
                             f"広域地理クラスタ（{source}）",
                             "phase13_e5", pairs, 0.3):
                    total += 1
                    batch += 1
                if batch >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch = 0

    if batch > 0:
        db_commit_retry(db)
    print(f"  E5 total: +{total:,}", flush=True)
    return total


def main():
    print("=" * 60, flush=True)
    print("Phase 13 A8: Connection Density Boost (450K → 650K+)", flush=True)
    print("=" * 60, flush=True)

    db = open_db()
    conn_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    gap = max(0, 650000 - conn_before)
    print(f"Current connections: {conn_before:,}", flush=True)
    print(f"Gap to 650K: {gap:,}", flush=True)

    pairs = load_existing_pairs(db)
    print(f"Existing pairs: {len(pairs):,}", flush=True)

    e1 = e1_temple_shrine_proximity(db, pairs, target=min(50000, gap))
    gap -= e1
    e2 = e2_source_clustering(db, pairs, target=min(50000, max(gap, 0)))
    gap -= e2
    e3 = e3_cross_type_label_match(db, pairs, target=min(50000, max(gap, 0)))
    gap -= e3
    e4 = e4_extended_geo_proximity(db, pairs, target=min(50000, max(gap, 0)))
    gap -= e4
    e5 = e5_regional_source_wide(db, pairs, target=min(50000, max(gap, 0)))

    conn_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    total_new = e1 + e2 + e3 + e4 + e5

    print(f"\n{'='*60}", flush=True)
    print("CONNECTION BOOST SUMMARY", flush=True)
    print(f"  E1 (Temple proximity):  +{e1:,}", flush=True)
    print(f"  E2 (Source clustering): +{e2:,}", flush=True)
    print(f"  E3 (Cross-type match):  +{e3:,}", flush=True)
    print(f"  E4 (3km proximity):     +{e4:,}", flush=True)
    print(f"  E5 (Wide regional):     +{e5:,}", flush=True)
    print(f"  Total new:              +{total_new:,}", flush=True)
    print(f"  Previous:               {conn_before:,}", flush=True)
    print(f"  New total:              {conn_after:,}", flush=True)

    if conn_after >= 650000:
        print("  TARGET 650K REACHED!", flush=True)
    else:
        print(f"  Gap remaining: {650000 - conn_after:,}", flush=True)

    db.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
