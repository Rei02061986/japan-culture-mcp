"""
Phase 15 Step 2: Shared-tag connections for isolated entities.

Strategy: Connect isolated entities that share the same (axis, value_code) tag.
For each tag group, sample entities and chain-connect them.
Only 135 unique tag values exist, but groups are massive (up to 1.6M entities).
We sample up to 2000 entities per group → ~200K total connections.

Target: ~200K connections
Source: p15_shared_tag
"""
import sqlite3
import time
import shutil
import random
import os
from datetime import datetime

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p15.db"
BATCH_SIZE = 5000
SAMPLE_PER_GROUP = 2000

AXIS_TO_CONN_TYPE = {
    "theme": "shared_theme",
    "era": "shared_era",
    "medium": "shared_medium",
    "geography": "shared_geography",
    "experience": "shared_experience",
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


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 15 Step 2: Shared-Tag Connections for Isolated Entities", flush=True)
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

    # --- Get distinct (axis, value_code) pairs with counts ---
    print("\nQuerying tag groups...", flush=True)
    tag_groups = db.execute("""
        SELECT et.axis, et.value_code, COUNT(*) as cnt
        FROM entity_tags et
        INNER JOIN isolated_entities ie ON et.entity_id = ie.id
        GROUP BY et.axis, et.value_code
        HAVING COUNT(*) >= 2
        ORDER BY cnt DESC
    """).fetchall()
    print(f"Tag groups with 2+ isolated entities: {len(tag_groups):,}", flush=True)

    # --- Process each group: sample entities, chain-connect ---
    total_new = 0
    batch_buf = []
    groups_processed = 0

    for axis, value_code, cnt in tag_groups:
        # Determine sample size
        sample_size = min(cnt, SAMPLE_PER_GROUP)

        # Query entity IDs for this group (random sample via ORDER BY RANDOM())
        if cnt <= SAMPLE_PER_GROUP:
            rows = db.execute("""
                SELECT et.entity_id FROM entity_tags et
                INNER JOIN isolated_entities ie ON et.entity_id = ie.id
                WHERE et.axis = ? AND et.value_code = ?
            """, (axis, value_code)).fetchall()
        else:
            rows = db.execute("""
                SELECT et.entity_id FROM entity_tags et
                INNER JOIN isolated_entities ie ON et.entity_id = ie.id
                WHERE et.axis = ? AND et.value_code = ?
                ORDER BY RANDOM() LIMIT ?
            """, (axis, value_code, SAMPLE_PER_GROUP)).fetchall()

        eids = [r[0] for r in rows]
        if len(eids) < 2:
            continue

        conn_type = AXIS_TO_CONN_TYPE.get(axis, "shared_tag")
        explanation = f"{axis}:{value_code}"

        # Build chain: 0->1, 1->2, 2->3, ...
        for i in range(len(eids) - 1):
            a_id, b_id = eids[i], eids[i + 1]
            pair = (min(a_id, b_id), max(a_id, b_id))
            if pair in existing_pairs:
                continue

            theme_val = 0.2 if axis == "theme" else 0.0
            era_val = 0.2 if axis == "era" else 0.0
            medium_val = 0.2 if axis == "medium" else 0.0
            geography_val = 0.2 if axis == "geography" else 0.0
            experience_val = 0.2 if axis == "experience" else 0.0

            batch_buf.append((
                pair[0], pair[1], conn_type,
                theme_val, era_val, medium_val, geography_val, experience_val,
                0.3, explanation, "p15_shared_tag", 0.7, now,
            ))
            existing_pairs.add(pair)
            total_new += 1

        groups_processed += 1

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

        print(f"  [{groups_processed}/{len(tag_groups)}] {axis}:{value_code} "
              f"({cnt:,} isolated, sampled {len(eids)}) -> +{len(eids)-1} chain links  |  total: {total_new:,}",
              flush=True)

    # Flush remaining
    if batch_buf:
        db.executemany("""
            INSERT OR IGNORE INTO connections
                (entity_a_id, entity_b_id, connection_type,
                 theme_distance, era_distance, medium_distance, geography_distance, experience_distance,
                 serendipity_score, explanation, source, confidence, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_buf)
        db_commit_retry(db)

    # --- Counts after ---
    conn_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    elapsed = time.time() - t0

    # --- Summary ---
    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 15 STEP 2 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Tag groups processed:     {groups_processed:,}", flush=True)
    print(f"  New connections:          +{total_new:,}", flush=True)
    print(f"  Connections before:        {conn_before:,}", flush=True)
    print(f"  Connections after:         {conn_after:,}", flush=True)
    print(f"  Isolated entities (was):   {iso_count:,}", flush=True)
    print(f"  Duration:                  {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 15 Step 2 complete.", flush=True)


if __name__ == "__main__":
    main()
