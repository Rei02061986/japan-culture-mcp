"""
Phase 18 A1: Build FTS5 trigram index for CJK substring matching.

Adds entities_fts_trigram virtual table alongside existing unicode61-based
entities_fts table. The trigram tokenizer enables substring matching for
Japanese/CJK text, solving the search quality issues identified in Phase 17
Codex user testing (1.83/5 satisfaction).

Auto-sync triggers keep the trigram index in sync with the entities table.
"""
import os
import shutil
import sqlite3
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ORIG_DB = os.path.join(SCRIPT_DIR, "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p18_fts5_trigram.db"


def main():
    t0 = time.time()

    print("=" * 70, flush=True)
    print("Phase 18 A1: FTS5 Trigram Index Build", flush=True)
    print("=" * 70, flush=True)

    if not os.path.exists(ORIG_DB):
        print(f"ERROR: DB not found at {ORIG_DB}", flush=True)
        return

    # Copy DB to /tmp
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    sz = os.path.getsize(WORK_DB)
    print(f"  Done. Size: {sz / 1e9:.2f} GB", flush=True)

    db = sqlite3.connect(WORK_DB, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=60000")

    # Check SQLite version supports trigram
    sqlite_ver = db.execute("SELECT sqlite_version()").fetchone()[0]
    print(f"\nSQLite version: {sqlite_ver}", flush=True)

    # Check entity count
    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Total entities: {total:,}", flush=True)

    # --- 1. Check if trigram table already exists ---
    existing = db.execute(
        "SELECT name FROM sqlite_master WHERE name='entities_fts_trigram'"
    ).fetchone()

    if existing:
        print("\n  entities_fts_trigram already exists, dropping for rebuild...", flush=True)
        db.execute("DROP TABLE IF EXISTS entities_fts_trigram")
        db.commit()

    # --- 2. Create FTS5 trigram table ---
    print("\n--- Creating FTS5 Trigram Table ---", flush=True)
    try:
        db.execute("""
            CREATE VIRTUAL TABLE entities_fts_trigram USING fts5(
                label_ja, label_en,
                content='entities',
                content_rowid='id',
                tokenize='trigram'
            )
        """)
        db.commit()
        print("  Table created successfully.", flush=True)
    except Exception as e:
        print(f"  ERROR creating trigram table: {e}", flush=True)
        print("  Your SQLite version may not support the trigram tokenizer.", flush=True)
        db.close()
        return

    # --- 3. Populate from entities ---
    print("\n--- Populating FTS5 Trigram Index ---", flush=True)
    print("  (This may take 10-20 minutes for 8M+ entities...)", flush=True)
    start = time.time()
    db.execute("""
        INSERT INTO entities_fts_trigram(rowid, label_ja, label_en)
        SELECT id, label_ja, label_en FROM entities
    """)
    db.commit()
    elapsed = time.time() - start
    print(f"  Populated in {elapsed:.1f}s", flush=True)

    # Verify count
    fts_count = db.execute(
        "SELECT COUNT(*) FROM entities_fts_trigram"
    ).fetchone()[0]
    print(f"  FTS5 trigram rows: {fts_count:,}", flush=True)

    # --- 4. Test CJK substring matching ---
    print("\n--- Testing CJK Substring Matching ---", flush=True)
    test_queries = [
        ("鬼滅", "CJK substring"),
        ("進撃", "CJK substring"),
        ("北斎", "CJK artist name"),
        ("金閣寺", "CJK full name"),
        ("anime", "English keyword"),
        ("Hokusai", "English name"),
        ("新潟", "Prefecture name"),
        ("聖地", "Pilgrimage keyword"),
    ]
    for query, desc in test_queries:
        count = db.execute(
            "SELECT COUNT(*) FROM entities_fts_trigram WHERE entities_fts_trigram MATCH ?",
            (query,),
        ).fetchone()[0]
        print(f"  '{query}' ({desc}): {count:,} results", flush=True)

    # --- 5. Create Auto-Sync Triggers ---
    print("\n--- Creating Auto-Sync Triggers ---", flush=True)

    # Drop existing trigram triggers if any
    db.execute("DROP TRIGGER IF EXISTS fts_trigram_insert_trigger")
    db.execute("DROP TRIGGER IF EXISTS fts_trigram_delete_trigger")
    db.execute("DROP TRIGGER IF EXISTS fts_trigram_update_trigger")

    db.execute("""
        CREATE TRIGGER fts_trigram_insert_trigger AFTER INSERT ON entities BEGIN
            INSERT INTO entities_fts_trigram(rowid, label_ja, label_en)
            VALUES (new.id, new.label_ja, new.label_en);
        END
    """)
    db.execute("""
        CREATE TRIGGER fts_trigram_delete_trigger AFTER DELETE ON entities BEGIN
            INSERT INTO entities_fts_trigram(entities_fts_trigram, rowid, label_ja, label_en)
            VALUES ('delete', old.id, old.label_ja, old.label_en);
        END
    """)
    db.execute("""
        CREATE TRIGGER fts_trigram_update_trigger AFTER UPDATE ON entities BEGIN
            INSERT INTO entities_fts_trigram(entities_fts_trigram, rowid, label_ja, label_en)
            VALUES ('delete', old.id, old.label_ja, old.label_en);
            INSERT INTO entities_fts_trigram(rowid, label_ja, label_en)
            VALUES (new.id, new.label_ja, new.label_en);
        END
    """)
    db.commit()
    print("  Triggers created (insert/delete/update).", flush=True)

    # Verify triggers
    triggers = db.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'fts_trigram%'"
    ).fetchall()
    print(f"  Trigger count: {len(triggers)}", flush=True)

    # --- 6. Final DB size ---
    db.close()
    new_sz = os.path.getsize(WORK_DB)
    print(f"\n--- Summary ---", flush=True)
    print(f"  Original DB: {sz / 1e9:.2f} GB", flush=True)
    print(f"  With trigram: {new_sz / 1e9:.2f} GB (+{(new_sz - sz) / 1e6:.0f} MB)", flush=True)

    # --- 7. Copy back ---
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)

    # Cleanup
    try:
        os.unlink(WORK_DB)
    except OSError:
        pass

    elapsed_total = time.time() - t0
    print(f"\nTotal duration: {elapsed_total:.1f}s", flush=True)
    print("Phase 18 A1 complete.", flush=True)


if __name__ == "__main__":
    main()
