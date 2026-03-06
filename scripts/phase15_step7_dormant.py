"""
Phase 15 Step 7: Mark dormant entities.

Strategy: Add is_dormant column to entities table. Mark entities that remain
isolated (zero connections) AND have no useful metadata (no wikidata_id,
no lat/lon, no tags, and labels are very short or NULL) as dormant.

Entities with tags, wikidata_id, lat/lon, or meaningful labels are explicitly
marked as NOT dormant (is_dormant=0).
"""
import sqlite3
import time
import shutil
import os
from datetime import datetime

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p15.db"


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
    print("Phase 15 Step 7: Mark Dormant Entities", flush=True)
    print("=" * 70, flush=True)

    # --- Copy DB to /tmp ---
    print(f"\nCopying DB to {TMP_DB} ...", flush=True)
    shutil.copy2(SRC_DB, TMP_DB)
    print("  Done.", flush=True)

    db = open_db()

    # --- Counts before ---
    entity_count = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conn_count = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"\nTotal entities:      {entity_count:,}", flush=True)
    print(f"Total connections:   {conn_count:,}", flush=True)

    # --- Add is_dormant column if not exists ---
    print("\nAdding is_dormant column (if not exists)...", flush=True)
    try:
        db.execute("ALTER TABLE entities ADD COLUMN is_dormant INTEGER DEFAULT 0")
        db_commit_retry(db)
        print("  Column added.", flush=True)
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("  Column already exists, resetting all to 0...", flush=True)
            db.execute("UPDATE entities SET is_dormant = 0")
            db_commit_retry(db)
        else:
            raise

    # --- Mark dormant entities ---
    # Dormant = isolated AND no wikidata_id AND no lat/lon AND no tags
    # These entities have no structured metadata to build connections from.
    # They may have labels, but without any Wikidata link, geo coords, or tags,
    # they are effectively orphaned.
    print("\nMarking dormant entities...", flush=True)
    t1 = time.time()
    cursor = db.execute("""
        UPDATE entities SET is_dormant = 1
        WHERE NOT EXISTS (
            SELECT 1 FROM connections c
            WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
        )
        AND wikidata_id IS NULL
        AND lat IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM entity_tags et
            WHERE et.entity_id = entities.id
        )
    """)
    dormant_marked = cursor.rowcount
    db_commit_retry(db)
    t1_elapsed = time.time() - t1
    print(f"  Marked {dormant_marked:,} entities as dormant in {t1_elapsed:.1f}s", flush=True)

    # --- Ensure entities with good metadata are NOT dormant ---
    # Safety pass: rescue any entity incorrectly marked dormant
    print("\nEnsuring entities with metadata are NOT dormant...", flush=True)
    t2 = time.time()
    cursor2 = db.execute("""
        UPDATE entities SET is_dormant = 0
        WHERE is_dormant = 1
        AND (
            wikidata_id IS NOT NULL
            OR lat IS NOT NULL
            OR EXISTS (
                SELECT 1 FROM entity_tags et
                WHERE et.entity_id = entities.id
            )
        )
    """)
    rescued = cursor2.rowcount
    db_commit_retry(db)
    t2_elapsed = time.time() - t2
    print(f"  Rescued {rescued:,} entities (have useful metadata) in {t2_elapsed:.1f}s", flush=True)

    # --- Count dormant ---
    dormant_total = db.execute(
        "SELECT COUNT(*) FROM entities WHERE is_dormant = 1"
    ).fetchone()[0]
    non_dormant = entity_count - dormant_total
    print(f"\nTotal dormant:       {dormant_total:,}", flush=True)
    print(f"Non-dormant:         {non_dormant:,}", flush=True)
    print(f"Dormant rate:        {dormant_total / entity_count * 100:.2f}%", flush=True)

    # --- Dormant by source (top 30) ---
    print(f"\n{'--- Dormant by Source (top 30) ---':}", flush=True)
    rows = db.execute("""
        SELECT source, COUNT(*) as cnt
        FROM entities
        WHERE is_dormant = 1
        GROUP BY source
        ORDER BY cnt DESC
        LIMIT 30
    """).fetchall()
    for source, cnt in rows:
        print(f"  {source:40s}  {cnt:>10,}", flush=True)

    # --- Dormant by entity_type (top 20) ---
    print(f"\n{'--- Dormant by Entity Type (top 20) ---':}", flush=True)
    rows = db.execute("""
        SELECT entity_type, COUNT(*) as cnt
        FROM entities
        WHERE is_dormant = 1
        GROUP BY entity_type
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for etype, cnt in rows:
        print(f"  {str(etype):40s}  {cnt:>10,}", flush=True)

    # --- Summary ---
    elapsed = time.time() - t0

    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 15 STEP 7 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Total entities:        {entity_count:,}", flush=True)
    print(f"  Total connections:     {conn_count:,}", flush=True)
    print(f"  Dormant entities:      {dormant_total:,} ({dormant_total / entity_count * 100:.2f}%)", flush=True)
    print(f"  Non-dormant entities:  {non_dormant:,} ({non_dormant / entity_count * 100:.2f}%)", flush=True)
    print(f"  Rescued (safety pass): {rescued:,}", flush=True)
    print(f"  Duration:              {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 15 Step 7 complete.", flush=True)


if __name__ == "__main__":
    main()
