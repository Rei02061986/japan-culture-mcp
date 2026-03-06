"""
Phase 16 D2: Connect remaining isolated entities via shared tags.

Strategy: For isolated entities that have entity_tags, pair them with
another entity sharing the same (axis, value_code) tag. Use random
sampling per tag group, chain-connect to maximize unique entities connected.

Target: reduce active isolation from 21.46% to ≤20% (need ~77K new connections)
Source: p16_tag_connect
"""
import sqlite3
import time
import shutil
import os
import random
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p16d2.db"
SOURCE = "p16_tag_connect"
TARGET_SAMPLE = 500  # entities per tag group


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
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
    print("Phase 16 D2: Connect Isolated Entities via Shared Tags", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    # Copy DB
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = open_db()

    # Counts before
    conn_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    isolated_before = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE is_dormant = 0
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
          )
    """).fetchone()[0]
    active_count = db.execute("SELECT COUNT(*) FROM entities WHERE is_dormant = 0").fetchone()[0]
    iso_pct_before = isolated_before / max(active_count, 1) * 100

    print(f"\nConnections before:  {conn_before:,}", flush=True)
    print(f"Active isolated:     {isolated_before:,} ({iso_pct_before:.2f}%)", flush=True)

    # Get all unique (axis, value_code) pairs
    print("\nLoading tag groups...", flush=True)
    tag_groups = db.execute("""
        SELECT DISTINCT axis, value_code FROM entity_tags
    """).fetchall()
    print(f"  Tag groups: {len(tag_groups)}", flush=True)

    # Load existing pairs
    print("Loading existing connection pairs...", flush=True)
    existing_pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for a, b in rows:
            existing_pairs.add((min(a, b), max(a, b)))
    print(f"  Existing pairs: {len(existing_pairs):,}", flush=True)

    # Track which entities get connected
    total_connections = 0
    entities_connected = set()
    batch_pending = 0

    print(f"\nProcessing {len(tag_groups)} tag groups...\n", flush=True)

    for idx, (axis, value_code) in enumerate(tag_groups):
        # Get isolated entities in this tag group
        isolated_in_group = db.execute("""
            SELECT et.entity_id
            FROM entity_tags et
            JOIN entities e ON et.entity_id = e.id
            WHERE et.axis = ? AND et.value_code = ?
              AND e.is_dormant = 0
              AND NOT EXISTS (
                  SELECT 1 FROM connections c
                  WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
              )
            ORDER BY RANDOM()
            LIMIT ?
        """, (axis, value_code, TARGET_SAMPLE)).fetchall()

        if len(isolated_in_group) < 2:
            continue

        eids = [r[0] for r in isolated_in_group]

        # Chain-connect: 0-1, 1-2, 2-3, ...
        group_conns = 0
        for i in range(len(eids) - 1):
            pair = (min(eids[i], eids[i+1]), max(eids[i], eids[i+1]))
            if pair in existing_pairs:
                continue

            try:
                db.execute("""
                    INSERT OR IGNORE INTO connections
                        (entity_a_id, entity_b_id, connection_type,
                         theme_distance, serendipity_score,
                         explanation, source, confidence, created_at)
                    VALUES (?, ?, 'shared_theme', 0.3, 0.4,
                            ?, ?, 0.6, ?)
                """, (pair[0], pair[1],
                      f"tag:{axis}:{value_code}", SOURCE, now))
                existing_pairs.add(pair)
                total_connections += 1
                group_conns += 1
                batch_pending += 1
                entities_connected.add(eids[i])
                entities_connected.add(eids[i+1])
            except sqlite3.IntegrityError:
                pass

        if batch_pending >= 5000:
            db_commit_retry(db)
            batch_pending = 0

        if (idx + 1) % 20 == 0 or idx == 0:
            print(f"  Group {idx+1}/{len(tag_groups)} ({axis}:{value_code}) "
                  f"| conns: {total_connections:,} | unique entities: {len(entities_connected):,}",
                  flush=True)

    # Final commit
    if batch_pending > 0:
        db_commit_retry(db)

    # Counts after
    conn_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    isolated_after = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE is_dormant = 0
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
          )
    """).fetchone()[0]
    iso_pct_after = isolated_after / max(active_count, 1) * 100

    elapsed = time.time() - t0

    print(f"\n{'='*70}", flush=True)
    print("PHASE 16 D2 SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  New connections:          +{total_connections:,}", flush=True)
    print(f"  Unique entities connected:{len(entities_connected):,}", flush=True)
    print(f"  Connections: {conn_before:,} -> {conn_after:,}", flush=True)
    print(f"  Active isolated: {isolated_before:,} ({iso_pct_before:.2f}%) -> "
          f"{isolated_after:,} ({iso_pct_after:.2f}%)", flush=True)
    print(f"  Target: ≤20%             {'PASS' if iso_pct_after <= 20.0 else 'FAIL'}", flush=True)
    print(f"  Duration: {elapsed:.1f}s", flush=True)

    db.close()

    # Copy back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)
    print("Phase 16 D2 complete.", flush=True)


if __name__ == "__main__":
    main()
