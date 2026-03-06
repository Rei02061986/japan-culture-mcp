"""
Phase 15 Step 5: Cross entity-type connections via shared geography tags.

Strategy A: For each geography value_code, find isolated entities of different
entity_types and connect one from type A to one from type B for every type pair.

Strategy B: For isolated entities sharing BOTH a geography AND era tag
(same value_code for each axis), create a stronger era+geo cross-connection.

Target: ~50K connections
Sources: p15_type_geo, p15_era_geo
"""
import sqlite3
import time
import shutil
import os
from collections import defaultdict
from itertools import combinations
from datetime import datetime

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p15.db"
BATCH_SIZE = 5000


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


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 15 Step 5: Type x Geography Cross-Connections", flush=True)
    print("=" * 70, flush=True)

    # --- Copy DB to /tmp ---
    print(f"\nCopying DB to {TMP_DB} ...", flush=True)
    shutil.copy2(SRC_DB, TMP_DB)
    print("  Done.", flush=True)

    db = open_db()

    # --- Counts before ---
    conn_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    entity_count = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Entities:            {entity_count:,}", flush=True)
    print(f"Connections before:  {conn_before:,}", flush=True)

    # --- Build temp table of isolated entity IDs ---
    print("\nBuilding isolated entities temp table...", flush=True)
    db.execute("DROP TABLE IF EXISTS isolated_entities")
    db.execute("""
        CREATE TEMP TABLE isolated_entities AS
        SELECT id FROM entities
        WHERE NOT EXISTS (
            SELECT 1 FROM connections
            WHERE entity_a_id = entities.id OR entity_b_id = entities.id
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_isolated_id ON isolated_entities(id)")
    db_commit_retry(db)
    iso_count = db.execute("SELECT COUNT(*) FROM isolated_entities").fetchone()[0]
    print(f"Isolated entities:   {iso_count:,}", flush=True)

    # --- Load existing pairs for dedup ---
    print("Loading existing connection pairs...", flush=True)
    existing_pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for a, b in rows:
            existing_pairs.add((min(a, b), max(a, b)))
    print(f"Existing pairs:      {len(existing_pairs):,}", flush=True)

    # =====================================================================
    # Strategy A: Cross entity-type via shared geography tag
    # =====================================================================
    print(f"\n{'=' * 60}", flush=True)
    print("Strategy A: Cross entity-type via shared geography tag", flush=True)
    print("=" * 60, flush=True)

    # Query: geography-tagged isolated entities with their entity_type
    print("  Querying geography-tagged isolated entities...", flush=True)
    geo_rows = db.execute("""
        SELECT et.value_code, e.entity_type, e.id
        FROM entity_tags et
        JOIN entities e ON et.entity_id = e.id
        WHERE et.axis = 'geography'
          AND e.id IN (SELECT id FROM isolated_entities)
    """).fetchall()
    print(f"  Geography-tagged isolated rows: {len(geo_rows):,}", flush=True)

    # Group by value_code -> {entity_type: [entity_ids]}
    geo_groups = defaultdict(lambda: defaultdict(list))
    for value_code, entity_type, eid in geo_rows:
        geo_groups[value_code][entity_type].append(eid)

    # For each geo group, connect across different entity_types
    total_a = 0
    batch_buf = []
    groups_with_cross = 0

    for value_code, type_dict in geo_groups.items():
        if len(type_dict) < 2:
            continue  # Need at least 2 different entity_types

        groups_with_cross += 1
        type_names = list(type_dict.keys())

        for type_a, type_b in combinations(type_names, 2):
            eids_a = type_dict[type_a]
            eids_b = type_dict[type_b]

            # Connect one from A to one from B
            a_id = eids_a[0]
            b_id = eids_b[0]
            pair = (min(a_id, b_id), max(a_id, b_id))
            if pair in existing_pairs:
                continue

            explanation = f"geo:{value_code} {type_a}<>{type_b}"
            batch_buf.append((
                pair[0], pair[1], "type_geo_cross",
                0.0,   # theme
                0.0,   # era
                0.0,   # medium
                0.2,   # geography
                0.0,   # experience_distance
                0.6,   # serendipity_score
                explanation, "p15_type_geo", 0.6, now,
            ))
            existing_pairs.add(pair)
            total_a += 1

        if len(batch_buf) >= BATCH_SIZE:
            db.executemany("""
                INSERT OR IGNORE INTO connections
                    (entity_a_id, entity_b_id, connection_type,
                     theme_distance, era_distance, medium_distance, geography_distance, experience_distance,
                     serendipity_score, explanation, source, confidence, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_buf)
            db_commit_retry(db)
            batch_buf = []
            print(f"    ... Strategy A: {total_a:,} connections", flush=True)

    # Flush Strategy A
    if batch_buf:
        db.executemany("""
            INSERT OR IGNORE INTO connections
                (entity_a_id, entity_b_id, connection_type,
                 theme_distance, era_distance, medium_distance, geography_distance, experience_distance,
                 serendipity_score, explanation, source, confidence, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_buf)
        db_commit_retry(db)
        batch_buf = []

    print(f"  Strategy A total:  +{total_a:,} connections", flush=True)
    print(f"  Geo groups with cross-type: {groups_with_cross:,}", flush=True)

    # =====================================================================
    # Strategy B: Shared geography AND era tags (stronger connection)
    # =====================================================================
    print(f"\n{'=' * 60}", flush=True)
    print("Strategy B: Shared geography + era tags (stronger)", flush=True)
    print("=" * 60, flush=True)

    # Get geography tags for isolated entities: entity_id -> set of value_codes
    print("  Loading geography tags for isolated entities...", flush=True)
    geo_tags = defaultdict(set)
    for row in db.execute("""
        SELECT et.entity_id, et.value_code
        FROM entity_tags et
        WHERE et.axis = 'geography'
          AND et.entity_id IN (SELECT id FROM isolated_entities)
    """):
        geo_tags[row[0]].add(row[1])

    # Get era tags for isolated entities: entity_id -> set of value_codes
    print("  Loading era tags for isolated entities...", flush=True)
    era_tags = defaultdict(set)
    for row in db.execute("""
        SELECT et.entity_id, et.value_code
        FROM entity_tags et
        WHERE et.axis = 'era'
          AND et.entity_id IN (SELECT id FROM isolated_entities)
    """):
        era_tags[row[0]].add(row[1])

    # Find entities that have both geography and era tags
    both_entities = set(geo_tags.keys()) & set(era_tags.keys())
    print(f"  Entities with both geo + era tags: {len(both_entities):,}", flush=True)

    # Group by (geo_value, era_value) -> {entity_type: [entity_ids]}
    # For each entity, for each combo of its geo and era values
    geo_era_groups = defaultdict(lambda: defaultdict(list))

    # We also need entity_type info
    if both_entities:
        # Load entity_type for these entities
        eid_list = list(both_entities)
        entity_types = {}
        # Query in chunks to avoid huge IN clauses
        chunk_size = 10000
        for ci in range(0, len(eid_list), chunk_size):
            chunk = eid_list[ci:ci + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            for row in db.execute(
                f"SELECT id, entity_type FROM entities WHERE id IN ({placeholders})",
                chunk
            ):
                entity_types[row[0]] = row[1]

        for eid in both_entities:
            etype = entity_types.get(eid)
            if not etype:
                continue
            for gv in geo_tags[eid]:
                for ev in era_tags[eid]:
                    geo_era_groups[(gv, ev)][etype].append(eid)

    print(f"  (geo, era) groups: {len(geo_era_groups):,}", flush=True)

    total_b = 0

    for (geo_val, era_val), type_dict in geo_era_groups.items():
        if len(type_dict) < 2:
            continue  # Need multiple types

        type_names = list(type_dict.keys())

        for type_a, type_b in combinations(type_names, 2):
            eids_a = type_dict[type_a]
            eids_b = type_dict[type_b]

            a_id = eids_a[0]
            b_id = eids_b[0]
            pair = (min(a_id, b_id), max(a_id, b_id))
            if pair in existing_pairs:
                continue

            explanation = f"geo:{geo_val} era:{era_val} {type_a}<>{type_b}"
            batch_buf.append((
                pair[0], pair[1], "era_geo_cross",
                0.0,   # theme
                0.1,   # era
                0.0,   # medium
                0.1,   # geography
                0.0,   # experience_distance
                0.4,   # serendipity_score
                explanation, "p15_era_geo", 0.8, now,
            ))
            existing_pairs.add(pair)
            total_b += 1

        if len(batch_buf) >= BATCH_SIZE:
            db.executemany("""
                INSERT OR IGNORE INTO connections
                    (entity_a_id, entity_b_id, connection_type,
                     theme_distance, era_distance, medium_distance, geography_distance, experience_distance,
                     serendipity_score, explanation, source, confidence, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_buf)
            db_commit_retry(db)
            batch_buf = []
            print(f"    ... Strategy B: {total_b:,} connections", flush=True)

    # Flush Strategy B
    if batch_buf:
        db.executemany("""
            INSERT OR IGNORE INTO connections
                (entity_a_id, entity_b_id, connection_type,
                 theme_distance, era_distance, medium_distance, geography_distance, experience_distance,
                 serendipity_score, explanation, source, confidence, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_buf)
        db_commit_retry(db)

    print(f"  Strategy B total:  +{total_b:,} connections", flush=True)

    # --- Counts after ---
    conn_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    elapsed = time.time() - t0

    # --- Summary ---
    total_new = total_a + total_b
    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 15 STEP 5 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Strategy A (type x geo):   +{total_a:,}", flush=True)
    print(f"  Strategy B (era x geo):    +{total_b:,}", flush=True)
    print(f"  Total new connections:     +{total_new:,}", flush=True)
    print(f"  Connections before:         {conn_before:,}", flush=True)
    print(f"  Connections after:          {conn_after:,}", flush=True)
    print(f"  Isolated entities (was):    {iso_count:,}", flush=True)
    print(f"  Duration:                   {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 15 Step 5 complete.", flush=True)


if __name__ == "__main__":
    main()
