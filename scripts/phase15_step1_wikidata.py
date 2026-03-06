"""
Phase 15 Step 1: Wikidata structural connections for isolated entities.
Query Wikidata SPARQL for isolated entities that have wikidata_id,
find relationships that map to other entities already in the DB.
Creates connections between previously-isolated entities and existing ones.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import shutil
import math
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p15.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
SOURCE = "p15_wikidata_struct"
BATCH_INSERT_SIZE = 1000
SPARQL_BATCH_SIZE = 50


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


# Property -> (connection_type, dimension_values)
# dimension_values is a dict of column_name -> float value
PROP_MAP = {
    "http://www.wikidata.org/prop/direct/P31":  ("instance_of",          {"theme": 0.2}),
    "http://www.wikidata.org/prop/direct/P279": ("instance_of",          {"theme": 0.2}),
    "http://www.wikidata.org/prop/direct/P361": ("part_of",              {"theme": 0.1}),
    "http://www.wikidata.org/prop/direct/P527": ("part_of",              {"theme": 0.1}),
    "http://www.wikidata.org/prop/direct/P131": ("located_in",           {"geography": 0.1}),
    "http://www.wikidata.org/prop/direct/P17":  ("located_in",           {"geography": 0.1}),
    "http://www.wikidata.org/prop/direct/P495": ("country_origin",       {"geography": 0.2}),
    "http://www.wikidata.org/prop/direct/P136": ("genre",                {"medium": 0.2}),
    "http://www.wikidata.org/prop/direct/P840": ("narrative_location",   {"geography": 0.2, "theme": 0.3}),
    "http://www.wikidata.org/prop/direct/P170": ("creator",              {"theme": 0.1, "medium": 0.2}),
    "http://www.wikidata.org/prop/direct/P50":  ("creator",              {"theme": 0.1, "medium": 0.2}),
    "http://www.wikidata.org/prop/direct/P175": ("performer_composer",   {"medium": 0.1}),
    "http://www.wikidata.org/prop/direct/P86":  ("performer_composer",   {"medium": 0.1}),
    "http://www.wikidata.org/prop/direct/P57":  ("director_cast",        {"medium": 0.1}),
    "http://www.wikidata.org/prop/direct/P161": ("director_cast",        {"medium": 0.1}),
    "http://www.wikidata.org/prop/direct/P449": ("publisher_distributor", {"medium": 0.3}),
    "http://www.wikidata.org/prop/direct/P750": ("publisher_distributor", {"medium": 0.3}),
}


def sparql_query(query, retries=5):
    """Execute a SPARQL query against Wikidata with retries."""
    for attempt in range(retries):
        try:
            params = urllib.parse.urlencode({
                "query": query,
                "format": "json",
            })
            url = WIKIDATA_URL + "?" + params
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("results", {}).get("bindings", [])
        except Exception as e:
            status = ""
            if hasattr(e, "code"):
                status = f" (HTTP {e.code})"
            print(f"    SPARQL error{status} (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = min(10 * (2 ** attempt), 120)
                if hasattr(e, "code") and e.code in (429, 503, 504):
                    wait = max(wait, 30)
                print(f"    Waiting {wait}s...", flush=True)
                time.sleep(wait)
    return []


def extract_wikidata_id(uri):
    """Extract Q-id from a Wikidata entity URI."""
    if uri and "/entity/" in uri:
        qid = uri.split("/entity/")[-1]
        if qid.startswith("Q"):
            return qid
    return None


def get_isolated_entities_with_wikidata(db):
    """Get all isolated entities (zero connections) that have a wikidata_id."""
    print("  Querying isolated entities with wikidata_id...", flush=True)
    rows = db.execute("""
        SELECT e.id, e.wikidata_id
        FROM entities e
        WHERE e.wikidata_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM connections c
              WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
          )
    """).fetchall()
    return rows


def get_all_wikidata_ids(db):
    """Get all wikidata_id values in the DB mapped to entity id."""
    print("  Loading all wikidata_ids in DB...", flush=True)
    wid_to_id = {}
    cursor = db.execute("SELECT id, wikidata_id FROM entities WHERE wikidata_id IS NOT NULL")
    while True:
        rows = cursor.fetchmany(200000)
        if not rows:
            break
        for eid, wid in rows:
            wid_to_id[wid] = eid
    return wid_to_id


def load_existing_pairs(db):
    """Load all existing connection pairs for deduplication."""
    print("  Loading existing connection pairs...", flush=True)
    pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for a, b in rows:
            pairs.add((min(a, b), max(a, b)))
    return pairs


def build_sparql_batch(wikidata_ids):
    """Build a SPARQL query for a batch of wikidata_ids."""
    values_str = " ".join(f"wd:{wid}" for wid in wikidata_ids)
    query = f"""
SELECT ?item ?prop ?target WHERE {{
  VALUES ?item {{ {values_str} }}
  ?item ?prop ?target .
  VALUES ?prop {{
    wdt:P31 wdt:P279 wdt:P361 wdt:P527
    wdt:P131 wdt:P17 wdt:P495
    wdt:P136 wdt:P840
    wdt:P170 wdt:P50
    wdt:P175 wdt:P86
    wdt:P57 wdt:P161
    wdt:P449 wdt:P750
  }}
  FILTER(STRSTARTS(STR(?target), "http://www.wikidata.org/entity/Q"))
}}
"""
    return query


def main():
    print("=" * 60, flush=True)
    print("Phase 15 Step 1: Wikidata Structural Connections", flush=True)
    print("  for Isolated Entities", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()

    # Counts before
    entities_total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conns_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    isolated_total = db.execute("""
        SELECT COUNT(*) FROM entities e
        WHERE NOT EXISTS (
            SELECT 1 FROM connections c
            WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
        )
    """).fetchone()[0]

    print(f"\nTotal entities: {entities_total:,}", flush=True)
    print(f"Connections before: {conns_before:,}", flush=True)
    print(f"Isolated entities: {isolated_total:,}", flush=True)

    # Step 1: Get isolated entities with wikidata_id
    isolated = get_isolated_entities_with_wikidata(db)
    print(f"Isolated with wikidata_id: {len(isolated):,}", flush=True)

    # Step 2: Get all wikidata_ids in DB for target matching
    wid_to_id = get_all_wikidata_ids(db)
    print(f"Total wikidata_ids in DB: {len(wid_to_id):,}", flush=True)

    # Step 3: Load existing pairs
    existing_pairs = load_existing_pairs(db)
    print(f"Existing connection pairs: {len(existing_pairs):,}", flush=True)

    # Step 4: Query SPARQL in batches
    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    total_connections = 0
    batch_pending = 0
    skipped_no_target = 0
    skipped_duplicate = 0
    skipped_self = 0
    sparql_errors = 0

    # Build list of wikidata_ids from isolated entities
    isolated_wids = [wid for _, wid in isolated]
    total_batches = math.ceil(len(isolated_wids) / SPARQL_BATCH_SIZE)
    print(f"\nSPARQL batches to process: {total_batches:,}", flush=True)
    print(f"(batch size: {SPARQL_BATCH_SIZE})\n", flush=True)

    for batch_idx in range(0, len(isolated_wids), SPARQL_BATCH_SIZE):
        batch_wids = isolated_wids[batch_idx:batch_idx + SPARQL_BATCH_SIZE]
        batch_num = batch_idx // SPARQL_BATCH_SIZE + 1

        # Progress every 50 batches
        if batch_num % 50 == 0 or batch_num == 1:
            print(f"  Batch {batch_num:,}/{total_batches:,} "
                  f"| connections so far: {total_connections:,} "
                  f"| skipped(no_target={skipped_no_target:,}, "
                  f"dup={skipped_duplicate:,}, self={skipped_self:,})",
                  flush=True)

        query = build_sparql_batch(batch_wids)
        bindings = sparql_query(query)

        if bindings is None or len(bindings) == 0:
            sparql_errors += 1
            time.sleep(2)
            continue

        for b in bindings:
            item_uri = b.get("item", {}).get("value", "")
            prop_uri = b.get("prop", {}).get("value", "")
            target_uri = b.get("target", {}).get("value", "")

            source_wid = extract_wikidata_id(item_uri)
            target_wid = extract_wikidata_id(target_uri)

            if not source_wid or not target_wid:
                continue

            # Check if target exists in our DB
            target_eid = wid_to_id.get(target_wid)
            if target_eid is None:
                skipped_no_target += 1
                continue

            source_eid = wid_to_id.get(source_wid)
            if source_eid is None:
                skipped_no_target += 1
                continue

            if source_eid == target_eid:
                skipped_self += 1
                continue

            # Deduplicate
            pair = (min(source_eid, target_eid), max(source_eid, target_eid))
            if pair in existing_pairs:
                skipped_duplicate += 1
                continue

            # Map property to connection type and dimensions
            prop_info = PROP_MAP.get(prop_uri)
            if prop_info is None:
                continue

            conn_type, dimensions = prop_info
            theme_val = dimensions.get("theme", 0.0)
            medium_val = dimensions.get("medium", 0.0)
            geography_val = dimensions.get("geography", 0.0)

            # Extract property code for explanation
            prop_code = prop_uri.split("/")[-1] if "/" in prop_uri else prop_uri
            explanation = f"Wikidata {prop_code}: {source_wid} -> {target_wid}"

            try:
                db.execute("""
                    INSERT OR IGNORE INTO connections
                        (entity_a_id, entity_b_id, connection_type,
                         theme_distance, medium_distance, geography_distance,
                         serendipity_score, explanation, source, confidence, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 0.3, ?, ?, 0.8, ?)
                """, (pair[0], pair[1], conn_type,
                      theme_val, medium_val, geography_val,
                      explanation, SOURCE, now_str))
                existing_pairs.add(pair)
                total_connections += 1
                batch_pending += 1
            except sqlite3.IntegrityError:
                skipped_duplicate += 1

            # Batch commit
            if batch_pending >= BATCH_INSERT_SIZE:
                db_commit_retry(db)
                batch_pending = 0

        # Rate limiting: 2 seconds between SPARQL queries
        time.sleep(2)

    # Final commit
    if batch_pending > 0:
        db_commit_retry(db)

    # Counts after
    conns_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    isolated_after = db.execute("""
        SELECT COUNT(*) FROM entities e
        WHERE NOT EXISTS (
            SELECT 1 FROM connections c
            WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
        )
    """).fetchone()[0]

    db.close()

    # Copy DB back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("DB copied back.", flush=True)

    # Summary
    duration = datetime.now() - start
    print(f"\n{'='*60}", flush=True)
    print("PHASE 15 STEP 1 SUMMARY", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Isolated entities queried:    {len(isolated):,}", flush=True)
    print(f"  SPARQL batches processed:     {total_batches:,}", flush=True)
    print(f"  SPARQL empty/error batches:   {sparql_errors:,}", flush=True)
    print(f"  New connections created:      +{total_connections:,}", flush=True)
    print(f"  Skipped (no target in DB):    {skipped_no_target:,}", flush=True)
    print(f"  Skipped (duplicate pair):     {skipped_duplicate:,}", flush=True)
    print(f"  Skipped (self-connection):    {skipped_self:,}", flush=True)
    print(f"  Connections: {conns_before:,} -> {conns_after:,}", flush=True)
    print(f"  Isolated entities: {isolated_total:,} -> {isolated_after:,}", flush=True)
    print(f"  Duration: {duration}", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
