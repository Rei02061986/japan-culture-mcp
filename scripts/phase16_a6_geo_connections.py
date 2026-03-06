"""
Phase 16 A6: Geo-proximity connections after coordinate enrichment.
Rebuild R-Tree for newly geo-enabled entities and create proximity
connections (within 2km) for entities that lack geo_proximity connections.
Prefers cross-type connections, at most 5 neighbors per entity.
"""
import sqlite3
import time
import shutil
import math
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p16.db"
SOURCE = "p16_proximity"
BATCH_INSERT_SIZE = 1000
MAX_NEIGHBORS = 5
RADIUS_KM = 2.0
RADIUS_DEG_LAT = 0.018    # ~2km in latitude
RADIUS_DEG_LON = 0.022    # ~2km in longitude (at ~35N)
PROGRESS_EVERY = 10000


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


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance in kilometers between two lat/lon points."""
    R = 6371.0
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


def get_entities_without_geo_connections(db):
    """Get all non-dormant entities with coords that have no geo_proximity connections."""
    print("  Querying entities with coords but no geo_proximity connections...", flush=True)
    rows = db.execute("""
        SELECT e.id, e.lat, e.lon, e.entity_type
        FROM entities e
        WHERE e.lat IS NOT NULL AND e.lon IS NOT NULL
          AND e.is_dormant = 0
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE (c.entity_a_id = e.id OR c.entity_b_id = e.id)
                AND c.connection_type = 'geo_proximity'
          )
    """).fetchall()
    return rows


def find_nearby_rtree(db, entity_id, lat, lon):
    """Find nearby entities using R-Tree index within ~2km bounding box."""
    rows = db.execute("""
        SELECT r.id, e.lat, e.lon, e.entity_type
        FROM entities_rtree r
        JOIN entities e ON e.id = r.id
        WHERE r.min_lat <= ? + ?
          AND r.max_lat >= ? - ?
          AND r.min_lon <= ? + ?
          AND r.max_lon >= ? - ?
          AND r.id != ?
          AND e.is_dormant = 0
    """, (lat, RADIUS_DEG_LAT, lat, RADIUS_DEG_LAT,
          lon, RADIUS_DEG_LON, lon, RADIUS_DEG_LON,
          entity_id)).fetchall()
    return rows


def rebuild_rtree(db):
    """Rebuild R-Tree spatial index from scratch to ensure consistency."""
    print("\n  Rebuilding R-Tree spatial index...", flush=True)

    # Count before
    rtree_before = db.execute("SELECT COUNT(*) FROM entities_rtree").fetchone()[0]
    print(f"    R-Tree entries before rebuild: {rtree_before:,}", flush=True)

    # Count entities with coordinates
    geo_entities = db.execute(
        "SELECT COUNT(*) FROM entities WHERE lat IS NOT NULL AND lon IS NOT NULL"
    ).fetchone()[0]
    print(f"    Entities with coordinates: {geo_entities:,}", flush=True)

    # Full rebuild
    db.execute("DELETE FROM entities_rtree")
    db.execute("""
        INSERT INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
        SELECT id, lat, lat, lon, lon
        FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)
    db_commit_retry(db)

    # Count after
    rtree_after = db.execute("SELECT COUNT(*) FROM entities_rtree").fetchone()[0]
    print(f"    R-Tree entries after rebuild: {rtree_after:,}", flush=True)
    print(f"    R-Tree delta: +{rtree_after - rtree_before:,}", flush=True)

    return rtree_before, rtree_after


def main():
    print("=" * 60, flush=True)
    print("Phase 16 A6: Geo-Proximity Connections", flush=True)
    print("  (2km R-Tree, max 5 neighbors, cross-type preferred)", flush=True)
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
    geo_conns_before = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type = 'geo_proximity'"
    ).fetchone()[0]

    print(f"\nTotal entities: {entities_total:,}", flush=True)
    print(f"Connections before: {conns_before:,}", flush=True)
    print(f"Geo-proximity connections before: {geo_conns_before:,}", flush=True)

    # Rebuild R-Tree
    rtree_before, rtree_after = rebuild_rtree(db)

    # Get target entities (have coords, no geo_proximity connections)
    targets = get_entities_without_geo_connections(db)
    print(f"\nEntities with coords but no geo_proximity connections: {len(targets):,}", flush=True)

    if not targets:
        print("No entities need geo-proximity connections. Done.", flush=True)
        db.close()
        return

    # Load existing pairs for dedup
    existing_pairs = load_existing_pairs(db)
    print(f"Existing connection pairs: {len(existing_pairs):,}", flush=True)

    # Process each target entity
    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    total_connections = 0
    batch_pending = 0
    entities_connected = 0
    entities_no_neighbor = 0

    print(f"\nProcessing {len(targets):,} entities for proximity connections...\n", flush=True)

    for idx, (eid, lat, lon, etype) in enumerate(targets):
        # Progress
        if (idx + 1) % PROGRESS_EVERY == 0 or idx == 0:
            print(f"  Progress: {idx+1:,}/{len(targets):,} "
                  f"| new connections: {total_connections:,} "
                  f"| connected: {entities_connected:,} "
                  f"| no_neighbor: {entities_no_neighbor:,}",
                  flush=True)

        # Find nearby entities via R-Tree bounding box
        nearby = find_nearby_rtree(db, eid, lat, lon)

        if not nearby:
            entities_no_neighbor += 1
            continue

        # Calculate actual Haversine distances and filter to <= 2km
        candidates = []
        for n_id, n_lat, n_lon, n_type in nearby:
            dist_km = haversine_km(lat, lon, n_lat, n_lon)
            if dist_km <= RADIUS_KM:
                is_cross_type = 1 if n_type != etype else 0
                candidates.append((is_cross_type, dist_km, n_id, n_type))

        if not candidates:
            entities_no_neighbor += 1
            continue

        # Sort: cross-type first, then by distance (ascending)
        candidates.sort(key=lambda x: (-x[0], x[1]))

        connected_count = 0
        for is_cross, dist_km, n_id, n_type in candidates:
            if connected_count >= MAX_NEIGHBORS:
                break

            pair = (min(eid, n_id), max(eid, n_id))
            if pair in existing_pairs:
                continue

            # geography_distance: ratio of dist to max radius (closer = lower = more related)
            geography_val = min(dist_km / RADIUS_KM, 1.0)
            explanation = f"{dist_km*1000:.0f}m"

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
    geo_conns_after = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type = 'geo_proximity'"
    ).fetchone()[0]

    db.close()

    # Copy DB back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("DB copied back.", flush=True)

    # Summary
    duration = datetime.now() - start
    print(f"\n{'='*60}", flush=True)
    print("PHASE 16 A6 SUMMARY", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Target entities (coords, no geo_proximity): {len(targets):,}", flush=True)
    print(f"  Entities connected:                         {entities_connected:,}", flush=True)
    print(f"  Entities with no neighbor:                  {entities_no_neighbor:,}", flush=True)
    print(f"  New geo_proximity connections:               +{total_connections:,}", flush=True)
    print(f"  R-Tree entries: {rtree_before:,} -> {rtree_after:,} (+{rtree_after - rtree_before:,})", flush=True)
    print(f"  Total connections: {conns_before:,} -> {conns_after:,} (+{conns_after - conns_before:,})", flush=True)
    print(f"  Geo connections: {geo_conns_before:,} -> {geo_conns_after:,} (+{geo_conns_after - geo_conns_before:,})", flush=True)
    print(f"  Duration: {duration}", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
