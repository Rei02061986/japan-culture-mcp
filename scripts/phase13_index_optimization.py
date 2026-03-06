"""
Phase 13 B2: Index optimization for 5.5M+ entity DB.
Creates FTS5, R-Tree, and composite indexes.
Then runs VACUUM + ANALYZE.
"""
import sqlite3
import time

DB_PATH = "/tmp/culture_ontology_work.db"


def open_db():
    db = sqlite3.connect(DB_PATH, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=60000")
    db.execute("PRAGMA cache_size=-64000")  # 64MB cache
    return db


def timed(label, func):
    start = time.time()
    func()
    elapsed = time.time() - start
    print(f"  {label}: {elapsed:.1f}s", flush=True)
    return elapsed


def main():
    print("=" * 60, flush=True)
    print("Phase 13 B2: Index Optimization", flush=True)
    print("=" * 60, flush=True)

    db = open_db()
    entity_count = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conn_count = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"Entities: {entity_count:,}", flush=True)
    print(f"Connections: {conn_count:,}", flush=True)

    # --- 1. Basic B-Tree Indexes ---
    print("\n--- B-Tree Indexes ---", flush=True)

    btree_indexes = [
        ("idx_entities_label_ja", "CREATE INDEX IF NOT EXISTS idx_entities_label_ja ON entities(label_ja)"),
        ("idx_entities_label_en", "CREATE INDEX IF NOT EXISTS idx_entities_label_en ON entities(label_en)"),
        ("idx_entities_source", "CREATE INDEX IF NOT EXISTS idx_entities_source ON entities(source)"),
        ("idx_entities_lat_lon", "CREATE INDEX IF NOT EXISTS idx_entities_lat_lon ON entities(lat, lon) WHERE lat IS NOT NULL"),
        ("idx_conn_ab", "CREATE INDEX IF NOT EXISTS idx_conn_ab ON connections(entity_a_id, entity_b_id)"),
        ("idx_conn_ba", "CREATE INDEX IF NOT EXISTS idx_conn_ba ON connections(entity_b_id, entity_a_id)"),
        ("idx_conn_type", "CREATE INDEX IF NOT EXISTS idx_conn_type ON connections(connection_type)"),
        ("idx_conn_source", "CREATE INDEX IF NOT EXISTS idx_conn_source ON connections(source)"),
    ]

    for name, sql in btree_indexes:
        timed(name, lambda s=sql: db.execute(s))
        db.commit()

    # --- 2. FTS5 Full-Text Search ---
    print("\n--- FTS5 Full-Text Search ---", flush=True)

    # Check if FTS5 table already exists
    existing = db.execute(
        "SELECT name FROM sqlite_master WHERE name='entities_fts'"
    ).fetchone()

    if existing:
        print("  entities_fts already exists, dropping for rebuild...", flush=True)
        db.execute("DROP TABLE IF EXISTS entities_fts")
        db.commit()

    # Create FTS5 table (content-less with external content for efficiency)
    print("  Creating FTS5 table...", flush=True)
    db.execute("""
        CREATE VIRTUAL TABLE entities_fts USING fts5(
            label_ja, label_en,
            content='entities',
            content_rowid='id',
            tokenize='unicode61 remove_diacritics 2'
        )
    """)
    db.commit()

    # Populate FTS5 from entities table
    print("  Populating FTS5 (this may take a while)...", flush=True)
    start = time.time()
    db.execute("""
        INSERT INTO entities_fts(rowid, label_ja, label_en)
        SELECT id, label_ja, label_en FROM entities
    """)
    db.commit()
    elapsed = time.time() - start
    print(f"  FTS5 populated in {elapsed:.1f}s", flush=True)

    # Verify FTS5 works
    test = db.execute(
        "SELECT COUNT(*) FROM entities_fts WHERE entities_fts MATCH '北斎'"
    ).fetchone()[0]
    print(f"  FTS5 test '北斎': {test} results", flush=True)

    # --- 3. FTS5 Sync Triggers ---
    print("\n--- FTS5 Triggers ---", flush=True)

    db.execute("DROP TRIGGER IF EXISTS entities_fts_ai")
    db.execute("DROP TRIGGER IF EXISTS entities_fts_ad")
    db.execute("DROP TRIGGER IF EXISTS entities_fts_au")

    db.execute("""
        CREATE TRIGGER entities_fts_ai AFTER INSERT ON entities BEGIN
            INSERT INTO entities_fts(rowid, label_ja, label_en)
            VALUES (new.id, new.label_ja, new.label_en);
        END
    """)
    db.execute("""
        CREATE TRIGGER entities_fts_ad AFTER DELETE ON entities BEGIN
            INSERT INTO entities_fts(entities_fts, rowid, label_ja, label_en)
            VALUES ('delete', old.id, old.label_ja, old.label_en);
        END
    """)
    db.execute("""
        CREATE TRIGGER entities_fts_au AFTER UPDATE ON entities BEGIN
            INSERT INTO entities_fts(entities_fts, rowid, label_ja, label_en)
            VALUES ('delete', old.id, old.label_ja, old.label_en);
            INSERT INTO entities_fts(rowid, label_ja, label_en)
            VALUES (new.id, new.label_ja, new.label_en);
        END
    """)
    db.commit()
    print("  FTS5 triggers created (insert/delete/update)", flush=True)

    # --- 4. R-Tree Spatial Index ---
    print("\n--- R-Tree Spatial Index ---", flush=True)

    existing_rt = db.execute(
        "SELECT name FROM sqlite_master WHERE name='entities_rtree'"
    ).fetchone()

    if existing_rt:
        print("  entities_rtree already exists, dropping for rebuild...", flush=True)
        db.execute("DROP TABLE IF EXISTS entities_rtree")
        db.commit()

    db.execute("""
        CREATE VIRTUAL TABLE entities_rtree USING rtree(
            id,
            min_lat, max_lat,
            min_lon, max_lon
        )
    """)
    db.commit()

    print("  Populating R-Tree...", flush=True)
    start = time.time()
    db.execute("""
        INSERT INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
        SELECT id, lat, lat, lon, lon
        FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)
    db.commit()
    elapsed = time.time() - start
    geo_count = db.execute("SELECT COUNT(*) FROM entities_rtree").fetchone()[0]
    print(f"  R-Tree populated: {geo_count:,} entries in {elapsed:.1f}s", flush=True)

    # R-Tree sync triggers
    db.execute("DROP TRIGGER IF EXISTS entities_rtree_ai")
    db.execute("DROP TRIGGER IF EXISTS entities_rtree_ad")
    db.execute("DROP TRIGGER IF EXISTS entities_rtree_au")

    db.execute("""
        CREATE TRIGGER entities_rtree_ai AFTER INSERT ON entities
        WHEN new.lat IS NOT NULL AND new.lon IS NOT NULL BEGIN
            INSERT OR REPLACE INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
            VALUES (new.id, new.lat, new.lat, new.lon, new.lon);
        END
    """)
    db.execute("""
        CREATE TRIGGER entities_rtree_ad AFTER DELETE ON entities
        WHEN old.lat IS NOT NULL BEGIN
            DELETE FROM entities_rtree WHERE id = old.id;
        END
    """)
    db.execute("""
        CREATE TRIGGER entities_rtree_au AFTER UPDATE OF lat, lon ON entities
        WHEN new.lat IS NOT NULL AND new.lon IS NOT NULL BEGIN
            DELETE FROM entities_rtree WHERE id = old.id;
            INSERT OR REPLACE INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
            VALUES (new.id, new.lat, new.lat, new.lon, new.lon);
        END
    """)
    db.commit()
    print("  R-Tree triggers created", flush=True)

    # Verify R-Tree works (Tokyo area)
    test_rt = db.execute("""
        SELECT COUNT(*) FROM entities_rtree
        WHERE min_lat BETWEEN 35.5 AND 35.8
          AND min_lon BETWEEN 139.5 AND 139.9
    """).fetchone()[0]
    print(f"  R-Tree test (Tokyo area): {test_rt} results", flush=True)

    # --- 5. ANALYZE ---
    print("\n--- ANALYZE ---", flush=True)
    start = time.time()
    db.execute("ANALYZE")
    db.commit()
    elapsed = time.time() - start
    print(f"  ANALYZE completed in {elapsed:.1f}s", flush=True)

    # --- 6. Benchmark ---
    print("\n--- Benchmark ---", flush=True)

    benchmarks = [
        ("LIKE search '北斎'",
         "SELECT COUNT(*) FROM entities WHERE label_ja LIKE '%北斎%'"),
        ("FTS5 search '北斎'",
         "SELECT COUNT(*) FROM entities e JOIN entities_fts f ON e.id = f.rowid WHERE entities_fts MATCH '北斎'"),
        ("LIKE search '寺院'",
         "SELECT COUNT(*) FROM entities WHERE label_ja LIKE '%寺院%'"),
        ("FTS5 search '寺院'",
         "SELECT COUNT(*) FROM entities e JOIN entities_fts f ON e.id = f.rowid WHERE entities_fts MATCH '寺院'"),
        ("Geo bbox (Tokyo, no R-Tree)",
         "SELECT COUNT(*) FROM entities WHERE lat BETWEEN 35.5 AND 35.8 AND lon BETWEEN 139.5 AND 139.9"),
        ("Geo R-Tree (Tokyo)",
         "SELECT COUNT(*) FROM entities e JOIN entities_rtree rt ON e.id = rt.id WHERE rt.min_lat BETWEEN 35.5 AND 35.8 AND rt.min_lon BETWEEN 139.5 AND 139.9"),
        ("Connection lookup (id=1000)",
         "SELECT COUNT(*) FROM connections WHERE entity_a_id = 1000 OR entity_b_id = 1000"),
        ("Connection lookup composite",
         "SELECT COUNT(*) FROM connections WHERE entity_a_id = 1000 UNION ALL SELECT COUNT(*) FROM connections WHERE entity_b_id = 1000"),
    ]

    for name, sql in benchmarks:
        start = time.time()
        result = db.execute(sql).fetchone()
        elapsed_ms = (time.time() - start) * 1000
        print(f"  {name}: {elapsed_ms:.0f}ms (result: {result[0]})", flush=True)

    # --- Summary ---
    print(f"\n{'='*60}", flush=True)
    print("INDEX OPTIMIZATION SUMMARY", flush=True)
    print(f"  B-Tree indexes: {len(btree_indexes)}", flush=True)
    print(f"  FTS5: entities_fts (label_ja, label_en) + 3 triggers", flush=True)
    print(f"  R-Tree: entities_rtree ({geo_count:,} geo entries) + 3 triggers", flush=True)
    print(f"  ANALYZE: completed", flush=True)

    # Check total DB size
    page_count = db.execute("PRAGMA page_count").fetchone()[0]
    page_size = db.execute("PRAGMA page_size").fetchone()[0]
    db_size_mb = (page_count * page_size) / (1024 * 1024)
    print(f"  DB size: {db_size_mb:.0f} MB", flush=True)

    db.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
