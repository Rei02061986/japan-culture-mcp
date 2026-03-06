#!/usr/bin/env python3
"""
Phase 18 D1: Heritage rank enrichment from Wikidata P1435.

Fetches heritage designation data (国宝, 重要文化財, etc.) for entities
that already exist in the DB via their wikidata_id.

Adds heritage_rank and designation_year columns to entities table.

Expected yield: ~3,500 heritage designations matched to existing entities.
"""
import json
import re
import shutil
import sqlite3
import time
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
DB_SRC = SCRIPT_DIR.parent / "ontology" / "culture_ontology.db"
DB_TMP = Path("/tmp/culture_ontology_p18_heritage.db")

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
BATCH_SIZE = 200
RATE_LIMIT_SEC = 10

# Wikidata heritage designation QIDs → rank codes
DESIGNATION_MAP = {
    "Q1139795": "kokuho",              # 国宝
    "Q1188622": "juyo_bunkazai",       # 重要文化財
    "Q10857476": "registered_tangible", # 登録有形文化財
    "Q1120982": "historic_site",       # 史跡
    "Q2319498": "scenic_beauty",       # 名勝
    "Q2753153": "natural_monument",    # 天然記念物
    "Q9259": "world_heritage",         # 世界遺産
}

# Priority order (lower = higher rank)
RANK_PRIORITY = {
    "world_heritage": 0,
    "kokuho": 1,
    "juyo_bunkazai": 2,
    "historic_site": 3,
    "scenic_beauty": 4,
    "natural_monument": 5,
    "registered_tangible": 6,
}


def fetch_batch(wikidata_ids: list) -> dict:
    """Fetch heritage designations for a batch of Wikidata IDs."""
    values = " ".join(f"wd:{qid}" for qid in wikidata_ids)
    query = f"""
    SELECT ?item ?designation ?inception WHERE {{
      VALUES ?item {{ {values} }}
      ?item wdt:P1435 ?designation .
      OPTIONAL {{ ?item wdt:P571 ?inception }}
    }}
    """
    try:
        r = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            headers={"Accept": "application/sparql-results+json"},
            timeout=60,
        )
        if r.status_code == 429:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
            return fetch_batch(wikidata_ids)
        if r.status_code == 504:
            print("    Timeout, retrying in 30s...")
            time.sleep(30)
            return fetch_batch(wikidata_ids)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"    SPARQL error: {e}")
        return {}

    results = {}
    for binding in data.get("results", {}).get("bindings", []):
        qid = binding["item"]["value"].split("/")[-1]
        desig_qid = binding["designation"]["value"].split("/")[-1]
        rank = DESIGNATION_MAP.get(desig_qid)
        if not rank:
            continue

        year = None
        if "inception" in binding:
            m = re.search(r"(\d{4})", binding["inception"]["value"])
            if m:
                y = int(m.group(1))
                if 600 <= y <= 2026:
                    year = y

        # Keep highest priority rank
        if qid not in results or RANK_PRIORITY.get(rank, 99) < RANK_PRIORITY.get(results[qid][0], 99):
            results[qid] = (rank, year)

    return results


def main():
    print("=" * 70)
    print("Phase 18 D1: Heritage Rank Enrichment (Wikidata P1435)")
    print("=" * 70)

    # Copy DB to /tmp/
    print(f"\nCopying DB to {DB_TMP}...")
    shutil.copy2(DB_SRC, DB_TMP)
    print(f"  Done. Size: {DB_TMP.stat().st_size / (1024**3):.2f} GB")

    con = sqlite3.connect(str(DB_TMP))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.row_factory = sqlite3.Row

    # Add columns if missing
    cols = [r[1] for r in con.execute("PRAGMA table_info(entities)").fetchall()]
    if "heritage_rank" not in cols:
        con.execute("ALTER TABLE entities ADD COLUMN heritage_rank TEXT")
        print("Added heritage_rank column")
    if "designation_year" not in cols:
        con.execute("ALTER TABLE entities ADD COLUMN designation_year INTEGER")
        print("Added designation_year column")
    con.commit()

    # Get entities with wikidata_id
    rows = con.execute("""
        SELECT id, wikidata_id FROM entities
        WHERE wikidata_id IS NOT NULL AND wikidata_id != ''
    """).fetchall()
    print(f"\nEntities with wikidata_id: {len(rows):,}")

    # Build wikidata_id → entity_id map
    qid_to_ids = {}
    for r in rows:
        qid = r["wikidata_id"]
        if qid.startswith("Q"):
            if qid not in qid_to_ids:
                qid_to_ids[qid] = []
            qid_to_ids[qid].append(r["id"])

    all_qids = list(qid_to_ids.keys())
    print(f"Unique Wikidata QIDs: {len(all_qids):,}")

    # Batch query
    total_found = 0
    total_updated = 0
    num_batches = (len(all_qids) + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"\n--- Querying Wikidata ({num_batches} batches of {BATCH_SIZE}) ---")

    for i in range(0, len(all_qids), BATCH_SIZE):
        batch_num = i // BATCH_SIZE + 1
        batch = all_qids[i:i + BATCH_SIZE]

        results = fetch_batch(batch)
        total_found += len(results)

        # Update DB
        for qid, (rank, year) in results.items():
            for entity_id in qid_to_ids.get(qid, []):
                con.execute("""
                    UPDATE entities
                    SET heritage_rank = ?, designation_year = ?
                    WHERE id = ? AND (heritage_rank IS NULL OR ? < COALESCE(
                        CASE heritage_rank
                            WHEN 'world_heritage' THEN 0
                            WHEN 'kokuho' THEN 1
                            WHEN 'juyo_bunkazai' THEN 2
                            WHEN 'historic_site' THEN 3
                            WHEN 'scenic_beauty' THEN 4
                            WHEN 'natural_monument' THEN 5
                            WHEN 'registered_tangible' THEN 6
                            ELSE 99
                        END, 99))
                """, (rank, year, entity_id, RANK_PRIORITY.get(rank, 99)))
                total_updated += 1

        if batch_num % 10 == 0 or batch_num == num_batches:
            con.commit()
            print(f"  Batch {batch_num}/{num_batches} ({i + len(batch):,}/{len(all_qids):,})"
                  f" — Found: {total_found:,} / Updated: {total_updated:,}")

        if i + BATCH_SIZE < len(all_qids):
            time.sleep(RATE_LIMIT_SEC)

    con.commit()

    # Verify
    print("\n--- Verification ---")
    rank_dist = con.execute("""
        SELECT heritage_rank, COUNT(*) as cnt
        FROM entities WHERE heritage_rank IS NOT NULL
        GROUP BY heritage_rank ORDER BY cnt DESC
    """).fetchall()
    for r in rank_dist:
        print(f"  {r['heritage_rank']}: {r['cnt']:,}")

    with_year = con.execute(
        "SELECT COUNT(*) FROM entities WHERE designation_year IS NOT NULL"
    ).fetchone()[0]
    print(f"  With designation_year: {with_year:,}")

    con.close()

    # Copy back
    print(f"\nCopying DB back to {DB_SRC}...")
    shutil.copy2(DB_TMP, DB_SRC)
    print("  Done.")

    print(f"\nTotal duration: Heritage rank enrichment complete.")
    print(f"  Found: {total_found:,}")
    print(f"  Updated: {total_updated:,}")


if __name__ == "__main__":
    main()
