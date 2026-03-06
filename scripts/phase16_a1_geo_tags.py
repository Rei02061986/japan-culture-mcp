"""
Phase 16 A1: Geo-enrich entities via geography tags.

Strategy: Entities that have geography tags in entity_tags but no lat/lon
get approximate prefecture/region-level coordinates. A systematic grid
offset is applied so entities spread across the region rather than all
landing on a single point.

Source: p16_geo_tags
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

# Region / prefecture tag value_codes -> (lat, lon)
COORDS = {
    'hokkaido': (43.0642, 141.3469),
    'tohoku': (39.7, 140.1),
    'kanto': (35.7, 139.7),
    'chubu': (36.2, 137.9),
    'kinki': (34.7, 135.5),
    'chugoku': (34.4, 132.5),
    'shikoku': (33.8, 133.5),
    'kyushu': (33.0, 131.0),
    'tokyo': (35.6762, 139.6503),
    'kyoto': (35.0116, 135.7681),
    'osaka': (34.6937, 135.5023),
    'nara': (34.6851, 135.8049),
    'okinawa': (26.3344, 127.8056),
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
    This spreads entities across roughly a 50 km area for large sets.
    """
    if count == 0:
        return []

    grid_size = max(1, math.ceil(math.sqrt(count)))
    half = grid_size / 2.0
    spacing = 0.01  # ~1.1 km at mid-latitudes

    coords = []
    for i in range(count):
        row = i % grid_size
        col = i // grid_size
        lat = base_lat + (row - half) * spacing
        lon = base_lon + (col - half) * spacing
        coords.append((lat, lon))
    return coords


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 16 A1: Geo-Enrich Entities via Geography Tags", flush=True)
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
    print(f"\nTotal entities:          {entity_count:,}", flush=True)
    print(f"Entities with coords:    {geo_before:,}", flush=True)
    print(f"Entities without coords: {entity_count - geo_before:,}", flush=True)

    # --- Check which geography value_codes exist in entity_tags ---
    existing_codes = db.execute("""
        SELECT DISTINCT value_code FROM entity_tags WHERE axis = 'geography'
    """).fetchall()
    existing_codes = {row[0] for row in existing_codes}
    print(f"\nGeography tag value_codes in DB: {len(existing_codes):,}", flush=True)
    matched_codes = sorted(set(COORDS.keys()) & existing_codes)
    unmatched_codes = sorted(set(COORDS.keys()) - existing_codes)
    print(f"Matched to COORDS dict:          {len(matched_codes)}", flush=True)
    if unmatched_codes:
        print(f"Not found in DB (skipped):       {unmatched_codes}", flush=True)

    # --- Process each value_code ---
    total_updated = 0

    for value_code in matched_codes:
        base_lat, base_lon = COORDS[value_code]

        # Find entity IDs with this geography tag, no lat/lon, not dormant
        rows = db.execute("""
            SELECT et.entity_id
            FROM entity_tags et
            JOIN entities e ON et.entity_id = e.id
            WHERE et.axis = 'geography'
              AND et.value_code = ?
              AND e.lat IS NULL
              AND e.is_dormant = 0
        """, (value_code,)).fetchall()

        entity_ids = [row[0] for row in rows]

        if not entity_ids:
            print(f"  {value_code:12s}  -> 0 entities (skip)", flush=True)
            continue

        # Compute grid coordinates
        grid_coords = compute_grid_coords(base_lat, base_lon, len(entity_ids))

        # Build batch of (lat, lon, entity_id)
        batch = []
        for idx, eid in enumerate(entity_ids):
            lat, lon = grid_coords[idx]
            batch.append((lat, lon, eid))

        # Execute in chunks
        for chunk_start in range(0, len(batch), BATCH_SIZE):
            chunk = batch[chunk_start:chunk_start + BATCH_SIZE]
            db.executemany(
                "UPDATE entities SET lat = ?, lon = ? WHERE id = ?",
                chunk
            )
            db_commit_retry(db)

        total_updated += len(entity_ids)
        print(f"  {value_code:12s}  -> {len(entity_ids):,} entities updated "
              f"(base: {base_lat:.4f}, {base_lon:.4f})", flush=True)

    # --- Counts after ---
    geo_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE lat IS NOT NULL"
    ).fetchone()[0]
    elapsed = time.time() - t0

    # --- Summary ---
    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 16 A1 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Total entities:            {entity_count:,}", flush=True)
    print(f"  Geo-entities before:       {geo_before:,}", flush=True)
    print(f"  Geo-entities after:        {geo_after:,}", flush=True)
    print(f"  Newly geo-enriched:       +{total_updated:,}", flush=True)
    print(f"  Value codes processed:     {len(matched_codes)}", flush=True)
    print(f"  Duration:                  {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 16 A1 complete.", flush=True)


if __name__ == "__main__":
    main()
