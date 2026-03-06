"""
Phase 16 D3: Label prefix chain for isolated entities.

Strategy: Group isolated JapanSearch entities by first 4 chars of label_ja
(shorter prefix = more groups hit). Chain-connect within each group.
Target: reduce isolation from 21.12% to ≤20%.

Source: p16_label_chain
"""
import sqlite3
import time
import shutil
import os
from datetime import datetime
from collections import defaultdict

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p16d3.db"
SOURCE = "p16_label_chain"
PREFIX_LEN = 4
MAX_GROUP_SIZE = 200  # cap chain length per group
MIN_GROUP_SIZE = 2


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
    print("Phase 16 D3: Label Prefix Chain for Isolated Entities", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = open_db()

    # Counts before
    conn_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    active_count = db.execute("SELECT COUNT(*) FROM entities WHERE is_dormant = 0").fetchone()[0]
    isolated_before = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE is_dormant = 0
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
          )
    """).fetchone()[0]
    iso_pct_before = isolated_before / max(active_count, 1) * 100

    print(f"\nConnections before: {conn_before:,}", flush=True)
    print(f"Active isolated:   {isolated_before:,} ({iso_pct_before:.2f}%)", flush=True)
    print(f"Target:            ≤{int(active_count * 0.20):,} ({active_count:,} * 20%)", flush=True)
    print(f"Need to connect:   {isolated_before - int(active_count * 0.20):,} entities", flush=True)

    # Get isolated entities with label_ja
    print(f"\nQuerying isolated entities with label_ja...", flush=True)
    rows = db.execute("""
        SELECT id, label_ja FROM entities
        WHERE is_dormant = 0
          AND label_ja IS NOT NULL
          AND LENGTH(label_ja) >= ?
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE c.entity_a_id = entities.id OR c.entity_b_id = entities.id
          )
    """, (PREFIX_LEN,)).fetchall()
    print(f"  Isolated with label_ja (len>={PREFIX_LEN}): {len(rows):,}", flush=True)

    # Group by prefix
    print(f"  Grouping by first {PREFIX_LEN} chars...", flush=True)
    groups = defaultdict(list)
    for eid, label in rows:
        prefix = label[:PREFIX_LEN]
        groups[prefix].append(eid)

    valid_groups = {p: eids for p, eids in groups.items() if len(eids) >= MIN_GROUP_SIZE}
    total_in_valid = sum(len(eids) for eids in valid_groups.values())
    print(f"  Total prefix groups: {len(groups):,}", flush=True)
    print(f"  Valid groups (size >= {MIN_GROUP_SIZE}): {len(valid_groups):,}", flush=True)
    print(f"  Entities in valid groups: {total_in_valid:,}", flush=True)

    # Load existing pairs
    print("  Loading existing connection pairs...", flush=True)
    existing_pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        batch = cursor.fetchmany(100000)
        if not batch:
            break
        for a, b in batch:
            existing_pairs.add((min(a, b), max(a, b)))
    print(f"  Existing pairs: {len(existing_pairs):,}", flush=True)

    # Chain-connect within groups
    print(f"\nChain-connecting {len(valid_groups):,} groups...\n", flush=True)
    total_connections = 0
    entities_connected = set()
    batch_pending = 0
    groups_processed = 0

    for prefix, eids in sorted(valid_groups.items(), key=lambda x: -len(x[1])):
        # Cap group size
        if len(eids) > MAX_GROUP_SIZE:
            eids = eids[:MAX_GROUP_SIZE]

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
                    VALUES (?, ?, 'label_similarity', 0.3, 0.4,
                            ?, ?, 0.5, ?)
                """, (pair[0], pair[1], f"prefix:{prefix}", SOURCE, now))
                existing_pairs.add(pair)
                total_connections += 1
                group_conns += 1
                batch_pending += 1
                entities_connected.add(eids[i])
                entities_connected.add(eids[i+1])
            except sqlite3.IntegrityError:
                pass

        if batch_pending >= 10000:
            db_commit_retry(db)
            batch_pending = 0

        groups_processed += 1
        if groups_processed % 10000 == 0:
            print(f"  Groups: {groups_processed:,}/{len(valid_groups):,} "
                  f"| conns: {total_connections:,} "
                  f"| entities: {len(entities_connected):,}", flush=True)

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
    print("PHASE 16 D3 SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  Groups processed:         {groups_processed:,}", flush=True)
    print(f"  New connections:          +{total_connections:,}", flush=True)
    print(f"  Unique entities connected:{len(entities_connected):,}", flush=True)
    print(f"  Connections: {conn_before:,} -> {conn_after:,}", flush=True)
    print(f"  Active isolated: {isolated_before:,} ({iso_pct_before:.2f}%) -> "
          f"{isolated_after:,} ({iso_pct_after:.2f}%)", flush=True)
    print(f"  Target: ≤20%             {'PASS' if iso_pct_after <= 20.0 else 'FAIL'}", flush=True)
    print(f"  Duration: {elapsed:.1f}s", flush=True)

    db.close()

    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)
    print("Phase 16 D3 complete.", flush=True)


if __name__ == "__main__":
    main()
