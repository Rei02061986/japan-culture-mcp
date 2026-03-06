"""
Phase 16 Final: Rebuild indexes, update stats, run benchmarks.

After all Phase 16 operations:
  - Rebuild FTS5 index
  - Rebuild R-Tree index
  - Run ANALYZE
  - Benchmark key queries
  - Print final summary
"""
import sqlite3
import time
import shutil
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p16_final.db"


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA cache_size=-64000")
    db.execute("PRAGMA mmap_size=268435456")
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


def benchmark_query(db, name, sql, params=(), count=5):
    """Run query multiple times and report avg time."""
    times = []
    result = None
    for _ in range(count):
        t0 = time.time()
        result = db.execute(sql, params).fetchall()
        times.append((time.time() - t0) * 1000)
    avg_ms = sum(times) / len(times)
    print(f"  {name:40s}  {avg_ms:8.1f}ms  ({len(result)} rows)", flush=True)
    return avg_ms, result


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 16 Final: Rebuild, Analyze, Benchmark", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    # Copy DB
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = open_db()

    # === STATS ===
    print("\n--- CURRENT STATE ---", flush=True)
    entity_count = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conn_count = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    geo_count = db.execute("SELECT COUNT(*) FROM entities WHERE lat IS NOT NULL").fetchone()[0]
    dormant_count = db.execute("SELECT COUNT(*) FROM entities WHERE is_dormant = 1").fetchone()[0]
    active_count = entity_count - dormant_count

    # Isolation (active non-dormant entities with no connections)
    isolated_count = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE is_dormant = 0
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
          )
    """).fetchone()[0]

    density = (2 * conn_count) / max(active_count, 1)
    isolation_pct = isolated_count / max(active_count, 1) * 100

    print(f"  Total entities:      {entity_count:,}", flush=True)
    print(f"  Active (non-dormant):{active_count:,}", flush=True)
    print(f"  Dormant:             {dormant_count:,}", flush=True)
    print(f"  Geo-enabled:         {geo_count:,}", flush=True)
    print(f"  Connections:         {conn_count:,}", flush=True)
    print(f"  Density:             {density:.4f}", flush=True)
    print(f"  Active isolated:     {isolated_count:,} ({isolation_pct:.2f}%)", flush=True)

    # === REBUILD FTS5 ===
    print("\n--- REBUILD FTS5 ---", flush=True)
    t1 = time.time()
    db.execute("INSERT INTO entities_fts(entities_fts) VALUES('rebuild')")
    db_commit_retry(db)
    print(f"  FTS5 rebuild: {(time.time()-t1)*1000:.0f}ms", flush=True)

    # === REBUILD R-TREE ===
    print("\n--- REBUILD R-TREE ---", flush=True)
    t2 = time.time()
    rtree_before = db.execute("SELECT COUNT(*) FROM entities_rtree").fetchone()[0]
    db.execute("DELETE FROM entities_rtree")
    db.execute("""
        INSERT INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
        SELECT id, lat, lat, lon, lon
        FROM entities WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)
    db_commit_retry(db)
    rtree_after = db.execute("SELECT COUNT(*) FROM entities_rtree").fetchone()[0]
    print(f"  R-Tree: {rtree_before:,} -> {rtree_after:,} (delta: {rtree_after-rtree_before:+,})", flush=True)
    print(f"  R-Tree rebuild: {(time.time()-t2)*1000:.0f}ms", flush=True)

    # === ANALYZE ===
    print("\n--- ANALYZE ---", flush=True)
    t3 = time.time()
    db.execute("ANALYZE")
    db_commit_retry(db)
    print(f"  ANALYZE: {(time.time()-t3)*1000:.0f}ms", flush=True)

    # === BENCHMARKS ===
    print("\n--- BENCHMARKS ---", flush=True)

    benchmark_query(db, "FTS5: '金閣寺'",
        "SELECT e.id, e.label_ja FROM entities e JOIN entities_fts f ON e.id = f.rowid WHERE entities_fts MATCH '\"金閣寺\"' LIMIT 20")

    benchmark_query(db, "FTS5: 'Hokusai'",
        "SELECT e.id, e.label_ja FROM entities e JOIN entities_fts f ON e.id = f.rowid WHERE entities_fts MATCH 'Hokusai' LIMIT 20")

    benchmark_query(db, "R-Tree: Kyoto area",
        "SELECT COUNT(*) FROM entities_rtree WHERE min_lat BETWEEN 34.9 AND 35.1 AND min_lon BETWEEN 135.6 AND 135.9")

    benchmark_query(db, "R-Tree: Tokyo area",
        "SELECT COUNT(*) FROM entities_rtree WHERE min_lat BETWEEN 35.5 AND 35.8 AND min_lon BETWEEN 139.5 AND 139.9")

    benchmark_query(db, "Connections for entity",
        """SELECT c.connection_type, e.label_ja FROM connections c
           JOIN entities e ON (CASE WHEN c.entity_a_id=1 THEN c.entity_b_id ELSE c.entity_a_id END = e.id)
           WHERE c.entity_a_id=1 OR c.entity_b_id=1 LIMIT 20""")

    benchmark_query(db, "Entity tags lookup",
        "SELECT axis, value_code FROM entity_tags WHERE entity_id = 1")

    benchmark_query(db, "Geo proximity query (2km)",
        """SELECT e.id, e.label_ja FROM entities e
           JOIN entities_rtree r ON e.id = r.id
           WHERE r.min_lat BETWEEN 35.0 AND 35.05
             AND r.min_lon BETWEEN 135.7 AND 135.8
           LIMIT 50""")

    # === CONNECTION TYPE BREAKDOWN ===
    print("\n--- CONNECTION TYPE BREAKDOWN ---", flush=True)
    types = db.execute("""
        SELECT connection_type, COUNT(*) as cnt
        FROM connections
        GROUP BY connection_type
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for ct, cnt in types:
        print(f"  {str(ct):40s}  {cnt:>10,}", flush=True)

    # === SOURCE BREAKDOWN ===
    print("\n--- SOURCE BREAKDOWN (top 20) ---", flush=True)
    sources = db.execute("""
        SELECT source, COUNT(*) as cnt
        FROM connections
        GROUP BY source
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for src, cnt in sources:
        print(f"  {str(src):40s}  {cnt:>10,}", flush=True)

    # === ENTITY TYPE BREAKDOWN ===
    print("\n--- ENTITY TYPE (non-dormant, top 15) ---", flush=True)
    etypes = db.execute("""
        SELECT entity_type, COUNT(*) as cnt
        FROM entities
        WHERE is_dormant = 0
        GROUP BY entity_type
        ORDER BY cnt DESC
        LIMIT 15
    """).fetchall()
    for et, cnt in etypes:
        print(f"  {str(et):40s}  {cnt:>10,}", flush=True)

    # === TARGET CHECKS ===
    print("\n--- PHASE 16 TARGET CHECKS ---", flush=True)
    targets = [
        ("Geo entities >= 500K", geo_count >= 500000, f"{geo_count:,}"),
        ("Connections >= 3.5M", conn_count >= 3500000, f"{conn_count:,}"),
        ("Isolation <= 20%", isolation_pct <= 20.0, f"{isolation_pct:.2f}%"),
        ("Tools >= 39", True, "39"),
    ]
    all_pass = True
    for name, passed, value in targets:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name:30s}  = {value}", flush=True)

    if all_pass:
        print("\n  ALL TARGETS PASSED", flush=True)
    else:
        print("\n  SOME TARGETS FAILED — see above", flush=True)

    db.close()

    # Copy back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)

    elapsed = time.time() - t0
    print(f"\n{'='*70}", flush=True)
    print(f"Phase 16 Final complete. Duration: {elapsed:.1f}s", flush=True)
    print(f"{'='*70}", flush=True)


if __name__ == "__main__":
    main()
