"""
Phase 17 Step 1: Add release_year and release_year_source columns to entities.

Adds:
  - release_year INTEGER DEFAULT NULL
  - release_year_source TEXT DEFAULT NULL
  - Partial index on release_year WHERE NOT NULL
"""
import sqlite3
import time
import shutil
import os

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p17_step1.db"


def main():
    t0 = time.time()

    print("=" * 70, flush=True)
    print("Phase 17 Step 1: Add release_year columns", flush=True)
    print("=" * 70, flush=True)

    # Copy DB to /tmp
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    # Check if columns already exist
    cols = {row[1] for row in db.execute("PRAGMA table_info(entities)").fetchall()}
    print(f"\nExisting columns: {len(cols)}", flush=True)

    if "release_year" in cols:
        print("  release_year already exists — skipping ALTER.", flush=True)
    else:
        print("  Adding release_year INTEGER...", flush=True)
        db.execute("ALTER TABLE entities ADD COLUMN release_year INTEGER DEFAULT NULL")
        print("  Done.", flush=True)

    if "release_year_source" in cols:
        print("  release_year_source already exists — skipping ALTER.", flush=True)
    else:
        print("  Adding release_year_source TEXT...", flush=True)
        db.execute("ALTER TABLE entities ADD COLUMN release_year_source TEXT DEFAULT NULL")
        print("  Done.", flush=True)

    db.commit()

    # Create partial index
    print("\nCreating partial index on release_year...", flush=True)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_entities_release_year
        ON entities(release_year) WHERE release_year IS NOT NULL
    """)
    db.commit()
    print("  Done.", flush=True)

    # Verify
    cols_after = {row[1] for row in db.execute("PRAGMA table_info(entities)").fetchall()}
    print(f"\nColumns after: {len(cols_after)}", flush=True)
    assert "release_year" in cols_after, "release_year not found!"
    assert "release_year_source" in cols_after, "release_year_source not found!"

    # Count
    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Total entities: {total:,}", flush=True)

    db.close()

    # Copy back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)

    elapsed = time.time() - t0
    print(f"\nPhase 17 Step 1 complete. Duration: {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
