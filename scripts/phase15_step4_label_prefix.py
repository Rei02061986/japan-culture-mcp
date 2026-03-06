"""
Phase 15 Step 4: Label-prefix connections for isolated JapanSearch entities.

Strategy: Group isolated JapanSearch entities by the first 6 characters of
label_ja, then chain-connect entities within each group. Skips groups with
>500 entities (too generic a prefix) or <2 entities (nothing to connect).

Target: ~150K connections
Source: p15_label_prefix
"""
import sqlite3
import time
import shutil
import os
from collections import defaultdict
from datetime import datetime

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p15.db"
BATCH_SIZE = 5000
PREFIX_LEN = 6
MAX_GROUP_SIZE = 500


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
    print("Phase 15 Step 4: Label-Prefix Connections for Isolated Entities", flush=True)
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

    # --- Query isolated JapanSearch entities ---
    print("\nQuerying isolated JapanSearch entities with label_ja...", flush=True)
    rows = db.execute("""
        SELECT e.id, e.label_ja
        FROM entities e
        INNER JOIN isolated_entities ie ON e.id = ie.id
        WHERE (e.source LIKE 'jps%' OR e.source LIKE 'japansearch%')
          AND e.label_ja IS NOT NULL
          AND LENGTH(e.label_ja) >= ?
    """, (PREFIX_LEN,)).fetchall()
    print(f"Matching entities:   {len(rows):,}", flush=True)

    # --- Group by first PREFIX_LEN characters of label_ja ---
    print(f"Grouping by first {PREFIX_LEN} characters of label_ja...", flush=True)
    prefix_groups = defaultdict(list)
    for eid, label_ja in rows:
        prefix = label_ja[:PREFIX_LEN]
        prefix_groups[prefix].append(eid)

    # Filter: keep groups with 2-500 entities
    valid_groups = {k: v for k, v in prefix_groups.items() if 2 <= len(v) <= MAX_GROUP_SIZE}
    skipped_too_large = sum(1 for v in prefix_groups.values() if len(v) > MAX_GROUP_SIZE)
    skipped_too_small = sum(1 for v in prefix_groups.values() if len(v) < 2)
    total_in_valid = sum(len(v) for v in valid_groups.values())

    print(f"Total prefix groups: {len(prefix_groups):,}", flush=True)
    print(f"Valid groups (2-{MAX_GROUP_SIZE}): {len(valid_groups):,}  ({total_in_valid:,} entities)", flush=True)
    print(f"Skipped (>500):      {skipped_too_large:,}", flush=True)
    print(f"Skipped (<2):        {skipped_too_small:,}", flush=True)

    # --- Chain-connect within each valid group ---
    total_new = 0
    batch_buf = []
    groups_processed = 0

    for prefix, eids in valid_groups.items():
        # Chain: 0->1, 1->2, 2->3, ...
        for i in range(len(eids) - 1):
            a_id, b_id = eids[i], eids[i + 1]
            pair = (min(a_id, b_id), max(a_id, b_id))
            if pair in existing_pairs:
                continue

            batch_buf.append((
                pair[0], pair[1], "label_similarity",
                0.3,   # theme
                0.0,   # era
                0.3,   # medium
                0.0,   # geography
                0.0,   # experience_distance
                0.5,   # serendipity_score
                f"prefix:{prefix}", "p15_label_prefix", 0.5, now,
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

        if groups_processed % 1000 == 0:
            print(f"  Groups processed: {groups_processed:,}  |  New connections: {total_new:,}", flush=True)

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
    print("PHASE 15 STEP 4 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Prefix groups processed:  {groups_processed:,}", flush=True)
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
    print("Phase 15 Step 4 complete.", flush=True)


if __name__ == "__main__":
    main()
