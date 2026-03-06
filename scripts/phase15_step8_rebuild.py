"""
Phase 15 Step 8: Rebuild indexes, ANALYZE, benchmark, and final statistics.

Strategy: Rebuild FTS5 index, run ANALYZE for query planner optimization,
benchmark key query patterns, and print comprehensive database statistics
with PASS/FAIL targets.

Does NOT run VACUUM (too slow for 3GB DB on Google Drive).
"""
import sqlite3
import time
import shutil
import os
from datetime import datetime

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p15.db"

# --- Targets ---
TARGET_CONNECTIONS = 1_200_000
TARGET_DENSITY = 0.24
TARGET_ACTIVE_ISOLATION_RATE = 0.25  # <= 25%


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


def benchmark(db, label, sql):
    """Run a query and return (elapsed_ms, result_value)."""
    start = time.time()
    result = db.execute(sql).fetchone()
    elapsed_ms = (time.time() - start) * 1000
    val = result[0] if result else None
    print(f"  {label:50s}  {elapsed_ms:8.1f}ms  (result: {val})", flush=True)
    return elapsed_ms, val


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 15 Step 8: Rebuild Indexes, ANALYZE, Benchmark & Statistics", flush=True)
    print("=" * 70, flush=True)

    # --- Copy DB to /tmp ---
    print(f"\nCopying DB to {TMP_DB} ...", flush=True)
    shutil.copy2(SRC_DB, TMP_DB)
    print("  Done.", flush=True)

    db = open_db()

    # =====================================================================
    # 1. Rebuild FTS5
    # =====================================================================
    print("\n--- Rebuilding FTS5 Index ---", flush=True)
    t1 = time.time()
    db.execute("INSERT INTO entities_fts(entities_fts) VALUES('rebuild')")
    db_commit_retry(db)
    t1_elapsed = time.time() - t1
    print(f"  FTS5 rebuild completed in {t1_elapsed:.1f}s", flush=True)

    # =====================================================================
    # 2. Run ANALYZE
    # =====================================================================
    print("\n--- Running ANALYZE ---", flush=True)
    t2 = time.time()
    db.execute("ANALYZE")
    db_commit_retry(db)
    t2_elapsed = time.time() - t2
    print(f"  ANALYZE completed in {t2_elapsed:.1f}s", flush=True)

    # =====================================================================
    # 3. Final Statistics
    # =====================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("FINAL DATABASE STATISTICS", flush=True)
    print(f"{'=' * 70}", flush=True)

    # Core counts
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_connections = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]

    # Isolated entities
    isolated_count = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE NOT EXISTS (
            SELECT 1 FROM connections c
            WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
        )
    """).fetchone()[0]
    connected_count = total_entities - isolated_count
    isolation_rate = isolated_count / total_entities if total_entities > 0 else 0
    connection_rate = connected_count / total_entities if total_entities > 0 else 0
    density = total_connections / total_entities if total_entities > 0 else 0

    print(f"\n  Total entities:          {total_entities:>12,}", flush=True)
    print(f"  Total connections:       {total_connections:>12,}", flush=True)
    print(f"  Connected entities:      {connected_count:>12,}  ({connection_rate * 100:.2f}%)", flush=True)
    print(f"  Isolated entities:       {isolated_count:>12,}  ({isolation_rate * 100:.2f}%)", flush=True)
    print(f"  Connection density:      {density:>12.4f}  (connections / entities)", flush=True)

    # Dormant count (may not exist if step 7 hasn't run yet)
    dormant_count = 0
    has_dormant = False
    try:
        dormant_count = db.execute(
            "SELECT COUNT(*) FROM entities WHERE is_dormant = 1"
        ).fetchone()[0]
        has_dormant = True
    except sqlite3.OperationalError:
        print("\n  [Note] is_dormant column not found; skipping dormant stats.", flush=True)

    if has_dormant:
        active_entities = total_entities - dormant_count
        active_isolated = db.execute("""
            SELECT COUNT(*) FROM entities
            WHERE is_dormant = 0
            AND NOT EXISTS (
                SELECT 1 FROM connections c
                WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
            )
        """).fetchone()[0]
        active_connected = active_entities - active_isolated
        active_isolation_rate = active_isolated / active_entities if active_entities > 0 else 0

        print(f"\n  Dormant entities:        {dormant_count:>12,}", flush=True)
        print(f"  Active entities:         {active_entities:>12,}", flush=True)
        print(f"  Active connected:        {active_connected:>12,}  ({active_connected / active_entities * 100:.2f}%)", flush=True)
        print(f"  Active isolated:         {active_isolated:>12,}  ({active_isolation_rate * 100:.2f}%)", flush=True)
    else:
        active_entities = total_entities
        active_isolated = isolated_count
        active_isolation_rate = isolation_rate

    # --- Connections by source (top 20) ---
    print(f"\n{'--- Connections by Source (top 20) ---':}", flush=True)
    rows = db.execute("""
        SELECT source, COUNT(*) as cnt
        FROM connections
        GROUP BY source
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for source, cnt in rows:
        print(f"    {str(source):40s}  {cnt:>10,}", flush=True)

    # --- Connections by connection_type (top 20) ---
    print(f"\n{'--- Connections by Type (top 20) ---':}", flush=True)
    rows = db.execute("""
        SELECT connection_type, COUNT(*) as cnt
        FROM connections
        GROUP BY connection_type
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for ctype, cnt in rows:
        print(f"    {str(ctype):40s}  {cnt:>10,}", flush=True)

    # --- DB size ---
    page_count = db.execute("PRAGMA page_count").fetchone()[0]
    page_size = db.execute("PRAGMA page_size").fetchone()[0]
    db_size_mb = (page_count * page_size) / (1024 * 1024)
    print(f"\n  DB size:                 {db_size_mb:>10.0f} MB", flush=True)

    # =====================================================================
    # 4. Benchmark Queries
    # =====================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("BENCHMARK QUERIES", flush=True)
    print(f"{'=' * 70}", flush=True)

    bm_fts_ms, bm_fts_val = benchmark(
        db,
        "FTS5 search (Tokyo)",
        """SELECT * FROM entities
           WHERE id IN (
               SELECT rowid FROM entities_fts
               WHERE entities_fts MATCH '"東京"'
           ) LIMIT 10""",
    )

    bm_rtree_ms, bm_rtree_val = benchmark(
        db,
        "R-Tree search (Tokyo bbox)",
        """SELECT COUNT(*) FROM entities_rtree
           WHERE min_lat >= 35.6 AND max_lat <= 35.8
           AND min_lon >= 139.6 AND max_lon <= 139.9""",
    )

    bm_conn_ms, bm_conn_val = benchmark(
        db,
        "Connection lookup (first entity)",
        """SELECT COUNT(*) FROM connections
           WHERE entity_a_id = (SELECT id FROM entities LIMIT 1)""",
    )

    bm_iso_ms, bm_iso_val = benchmark(
        db,
        "Isolation check (full scan)",
        """SELECT COUNT(*) FROM entities
           WHERE NOT EXISTS (
               SELECT 1 FROM connections c
               WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
           )""",
    )

    # =====================================================================
    # 5. PASS / FAIL Targets
    # =====================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("TARGETS", flush=True)
    print(f"{'=' * 70}", flush=True)

    results = []

    # Target 1: connections >= 1,200,000
    pass1 = total_connections >= TARGET_CONNECTIONS
    status1 = "PASS" if pass1 else "FAIL"
    results.append(pass1)
    print(
        f"  [{status1}] Connections >= {TARGET_CONNECTIONS:,}: "
        f"{total_connections:,}",
        flush=True,
    )

    # Target 2: density >= 0.24
    pass2 = density >= TARGET_DENSITY
    status2 = "PASS" if pass2 else "FAIL"
    results.append(pass2)
    print(
        f"  [{status2}] Density >= {TARGET_DENSITY}: "
        f"{density:.4f}",
        flush=True,
    )

    # Target 3: active isolation rate <= 25%
    pass3 = active_isolation_rate <= TARGET_ACTIVE_ISOLATION_RATE
    status3 = "PASS" if pass3 else "FAIL"
    results.append(pass3)
    print(
        f"  [{status3}] Active isolation rate <= {TARGET_ACTIVE_ISOLATION_RATE * 100:.0f}%: "
        f"{active_isolation_rate * 100:.2f}%",
        flush=True,
    )

    all_pass = all(results)
    overall = "ALL TARGETS PASSED" if all_pass else "SOME TARGETS FAILED"

    # =====================================================================
    # 6. Summary
    # =====================================================================
    elapsed = time.time() - t0

    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 15 STEP 8 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  FTS5 rebuild:          {t1_elapsed:.1f}s", flush=True)
    print(f"  ANALYZE:               {t2_elapsed:.1f}s", flush=True)
    print(f"  Benchmark FTS5:        {bm_fts_ms:.1f}ms", flush=True)
    print(f"  Benchmark R-Tree:      {bm_rtree_ms:.1f}ms", flush=True)
    print(f"  Benchmark connection:  {bm_conn_ms:.1f}ms", flush=True)
    print(f"  Benchmark isolation:   {bm_iso_ms:.1f}ms", flush=True)
    print(f"  Targets:               {overall}", flush=True)
    print(f"  Duration:              {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 15 Step 8 complete.", flush=True)


if __name__ == "__main__":
    main()
