"""
Phase 15 Step 3: Geo-proximity connections for isolated entities.
Connect geo-isolated entities to their nearest neighbors within 1km
using the R-Tree spatial index. Prefers cross-type connections.
"""
import sqlite3
import time
import shutil
import math
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p15.db"
SOURCE = "p15_proximity"
BATCH_INSERT_SIZE = 1000
MAX_NEIGHBORS = 3
RADIUS_DEG_LAT = 0.009   # ~1km in latitude
RADIUS_DEG_LON = 0.011   # ~1km in longitude (at ~35N)


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
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
    """Calculate distance in meters between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_existing_pairs(db):
    """Load all existing connection pairs for deduplication."""
    print("  Loading existing connection pairs...", flush=True)
    pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for a, b in rows:
            pairs.add((min(a, b), max(a, b)))
    return pairs


def get_isolated_geo_entities(db):
    """Get all isolated entities that have lat/lon coordinates."""
    print("  Querying isolated geo entities...", flush=True)
    rows = db.execute("""
        SELECT e.id, e.lat, e.lon, e.entity_type
        FROM entities e
        WHERE e.lat IS NOT NULL AND e.lon IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
          )
    """).fetchall()
    return rows


def find_nearby_rtree(db, entity_id, lat, lon):
    """Find nearby entities using R-Tree index within ~1km."""
    rows = db.execute("""
        SELECT r.id, e.lat, e.lon, e.entity_type
        FROM entities_rtree r
        JOIN entities e ON e.id = r.id
        WHERE r.min_lat <= ? + ? AND r.max_lat >= ? - ?
          AND r.min_lon <= ? + ? AND r.max_lon >= ? - ?
          AND r.id != ?
    """, (lat, RADIUS_DEG_LAT, lat, RADIUS_DEG_LAT,
          lon, RADIUS_DEG_LON, lon, RADIUS_DEG_LON,
          entity_id)).fetchall()
    return rows


def main():
    print("=" * 60, flush=True)
    print("Phase 15 Step 3: Geo-Proximity Connections", flush=True)
    print("  for Isolated Entities (1km R-Tree)", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()

    # Counts before
    entities_total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conns_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    isolated_total = db.execute("""
        SELECT COUNT(*) FROM entities e
        WHERE NOT EXISTS (
            SELECT 1 FROM connections c
            WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
        )
    """).fetchone()[0]
    rtree_count = db.execute("SELECT COUNT(*) FROM entities_rtree").fetchone()[0]

    print(f"\nTotal entities: {entities_total:,}", flush=True)
    print(f"Connections before: {conns_before:,}", flush=True)
    print(f"Isolated entities: {isolated_total:,}", flush=True)
    print(f"R-Tree entries: {rtree_count:,}", flush=True)

    # Get isolated geo entities
    isolated_geo = get_isolated_geo_entities(db)
    print(f"Isolated entities with lat/lon: {len(isolated_geo):,}", flush=True)

    # Load existing pairs
    existing_pairs = load_existing_pairs(db)
    print(f"Existing connection pairs: {len(existing_pairs):,}", flush=True)

    # Process each isolated geo entity
    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    total_connections = 0
    batch_pending = 0
    entities_connected = 0
    entities_no_neighbor = 0

    print(f"\nProcessing {len(isolated_geo):,} isolated geo entities...\n", flush=True)

    for idx, (eid, lat, lon, etype) in enumerate(isolated_geo):
        # Progress every 5000 entities
        if (idx + 1) % 5000 == 0 or idx == 0:
            print(f"  Progress: {idx+1:,}/{len(isolated_geo):,} "
                  f"| connections: {total_connections:,} "
                  f"| connected: {entities_connected:,} "
                  f"| no_neighbor: {entities_no_neighbor:,}",
                  flush=True)

        # Find nearby entities via R-Tree
        nearby = find_nearby_rtree(db, eid, lat, lon)

        if not nearby:
            entities_no_neighbor += 1
            continue

        # Calculate distances and sort
        candidates = []
        for n_id, n_lat, n_lon, n_type in nearby:
            dist = haversine_m(lat, lon, n_lat, n_lon)
            if dist <= 1000:  # Within 1km
                # Prefer different entity_type (sort cross-type first)
                is_cross_type = 1 if n_type != etype else 0
                candidates.append((is_cross_type, dist, n_id, n_type))

        if not candidates:
            entities_no_neighbor += 1
            continue

        # Sort: cross-type first, then by distance
        candidates.sort(key=lambda x: (-x[0], x[1]))

        connected_count = 0
        for is_cross, dist, n_id, n_type in candidates:
            if connected_count >= MAX_NEIGHBORS:
                break

            pair = (min(eid, n_id), max(eid, n_id))
            if pair in existing_pairs:
                continue

            # geography dimension: distance ratio (closer = lower value = more related)
            geography_val = min(dist / 1000.0, 1.0)
            dist_m = dist
            explanation = f"{dist_m:.0f}m"

            try:
                db.execute("""
                    INSERT OR IGNORE INTO connections
                        (entity_a_id, entity_b_id, connection_type,
                         geography_distance, serendipity_score,
                         explanation, source, confidence, created_at)
                    VALUES (?, ?, 'geo_proximity', ?, 0.5, ?, ?, 0.7, ?)
                """, (pair[0], pair[1], geography_val,
                      explanation, SOURCE, now_str))
                existing_pairs.add(pair)
                total_connections += 1
                batch_pending += 1
                connected_count += 1
            except sqlite3.IntegrityError:
                pass

            # Batch commit
            if batch_pending >= BATCH_INSERT_SIZE:
                db_commit_retry(db)
                batch_pending = 0

        if connected_count > 0:
            entities_connected += 1
        else:
            entities_no_neighbor += 1

    # Final commit
    if batch_pending > 0:
        db_commit_retry(db)

    # Counts after
    conns_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    isolated_after = db.execute("""
        SELECT COUNT(*) FROM entities e
        WHERE NOT EXISTS (
            SELECT 1 FROM connections c
            WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
        )
    """).fetchone()[0]

    db.close()

    # Copy DB back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("DB copied back.", flush=True)

    # Summary
    duration = datetime.now() - start
    print(f"\n{'='*60}", flush=True)
    print("PHASE 15 STEP 3 SUMMARY", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Isolated geo entities:        {len(isolated_geo):,}", flush=True)
    print(f"  Entities connected:           {entities_connected:,}", flush=True)
    print(f"  Entities with no neighbor:    {entities_no_neighbor:,}", flush=True)
    print(f"  New connections created:      +{total_connections:,}", flush=True)
    print(f"  Connections: {conns_before:,} -> {conns_after:,}", flush=True)
    print(f"  Isolated entities: {isolated_total:,} -> {isolated_after:,}", flush=True)
    print(f"  Duration: {duration}", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
