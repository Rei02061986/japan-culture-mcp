"""
Phase 16 A2: Wikidata coordinate enrichment (P625).
Query Wikidata SPARQL for entities that have a wikidata_id but no lat/lon.
Batch-fetch P625 (coordinate location) and update the entities table.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import shutil
import math
import os
import re
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p16.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
SPARQL_BATCH_SIZE = 100
COMMIT_EVERY = 500
PROGRESS_EVERY = 50       # print progress every N batches
SPARQL_DELAY = 1.5        # seconds between SPARQL requests


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


def sparql_query(query, retries=5):
    """Execute a SPARQL query against Wikidata with retries and exponential backoff."""
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


def parse_point(wkt_literal):
    """Parse Wikidata Point(lon lat) WKT literal into (lat, lon) tuple.

    Wikidata returns coordinates as 'Point(lon lat)' in the ?coords value.
    """
    if not wkt_literal:
        return None, None
    m = re.search(r"Point\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", wkt_literal)
    if m:
        lon = float(m.group(1))
        lat = float(m.group(2))
        # Sanity check: reasonable coordinate ranges
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
    return None, None


def get_entities_needing_coords(db):
    """Get entities that have wikidata_id but no lat/lon and are not dormant."""
    print("  Querying entities with wikidata_id but no coordinates...", flush=True)
    rows = db.execute("""
        SELECT id, wikidata_id
        FROM entities
        WHERE wikidata_id IS NOT NULL
          AND lat IS NULL
          AND is_dormant = 0
    """).fetchall()
    return rows


def build_sparql_batch(wikidata_ids):
    """Build a SPARQL query to fetch P625 coordinates for a batch of wikidata_ids."""
    values_str = " ".join(f"wd:{wid}" for wid in wikidata_ids)
    query = f"""SELECT ?item ?coords WHERE {{
  VALUES ?item {{ {values_str} }}
  ?item wdt:P625 ?coords .
}}"""
    return query


def main():
    print("=" * 60, flush=True)
    print("Phase 16 A2: Wikidata Coordinate Enrichment (P625)", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()

    # Counts before
    entities_total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    entities_with_coords_before = db.execute(
        "SELECT COUNT(*) FROM entities WHERE lat IS NOT NULL AND lon IS NOT NULL"
    ).fetchone()[0]

    print(f"\nTotal entities: {entities_total:,}", flush=True)
    print(f"Entities with coords (before): {entities_with_coords_before:,}", flush=True)

    # Get entities needing coordinates
    targets = get_entities_needing_coords(db)
    print(f"Entities with wikidata_id but no coords: {len(targets):,}", flush=True)

    if not targets:
        print("No entities need coordinate enrichment. Done.", flush=True)
        db.close()
        return

    # Build lookup: wikidata_id -> entity id
    wid_to_eid = {}
    for eid, wid in targets:
        wid_to_eid[wid] = eid

    # Deduplicate wikidata_ids (in case multiple entities share same wikidata_id)
    unique_wids = list(wid_to_eid.keys())
    total_batches = math.ceil(len(unique_wids) / SPARQL_BATCH_SIZE)

    print(f"Unique wikidata_ids to query: {len(unique_wids):,}", flush=True)
    print(f"SPARQL batches ({SPARQL_BATCH_SIZE}/batch): {total_batches:,}", flush=True)

    # Process batches
    total_updated = 0
    total_sparql_results = 0
    total_failed_parse = 0
    updates_pending = 0

    print(f"\nStarting SPARQL queries...\n", flush=True)

    for batch_idx in range(total_batches):
        batch_start = batch_idx * SPARQL_BATCH_SIZE
        batch_end = min(batch_start + SPARQL_BATCH_SIZE, len(unique_wids))
        batch_wids = unique_wids[batch_start:batch_end]

        # Progress every PROGRESS_EVERY batches
        if (batch_idx + 1) % PROGRESS_EVERY == 0 or batch_idx == 0:
            print(f"  Batch {batch_idx+1}/{total_batches} "
                  f"| SPARQL results: {total_sparql_results:,} "
                  f"| updated: {total_updated:,} "
                  f"| parse_fail: {total_failed_parse:,}",
                  flush=True)

        # Build and execute SPARQL query
        sparql = build_sparql_batch(batch_wids)
        bindings = sparql_query(sparql)
        total_sparql_results += len(bindings)

        # Parse results and update
        for binding in bindings:
            item_uri = binding.get("item", {}).get("value", "")
            coords_val = binding.get("coords", {}).get("value", "")

            # Extract Q-id from URI
            qid = None
            if "/entity/" in item_uri:
                qid = item_uri.split("/entity/")[-1]

            if not qid or qid not in wid_to_eid:
                continue

            lat, lon = parse_point(coords_val)
            if lat is None or lon is None:
                total_failed_parse += 1
                continue

            eid = wid_to_eid[qid]
            try:
                db.execute(
                    "UPDATE entities SET lat = ?, lon = ? WHERE id = ?",
                    (lat, lon, eid)
                )
                total_updated += 1
                updates_pending += 1
            except sqlite3.Error as e:
                print(f"    DB error updating entity {eid}: {e}", flush=True)

        # Batch commit every COMMIT_EVERY updates
        if updates_pending >= COMMIT_EVERY:
            db_commit_retry(db)
            updates_pending = 0

        # Rate limiting
        if batch_idx < total_batches - 1:
            time.sleep(SPARQL_DELAY)

    # Final commit
    if updates_pending > 0:
        db_commit_retry(db)

    # Counts after
    entities_with_coords_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE lat IS NOT NULL AND lon IS NOT NULL"
    ).fetchone()[0]

    db.close()

    # Copy DB back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("DB copied back.", flush=True)

    # Summary
    duration = datetime.now() - start
    new_coords = entities_with_coords_after - entities_with_coords_before
    print(f"\n{'='*60}", flush=True)
    print("PHASE 16 A2 SUMMARY", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Entities queried (wikidata_id, no coords):  {len(targets):,}", flush=True)
    print(f"  Unique wikidata_ids:                        {len(unique_wids):,}", flush=True)
    print(f"  SPARQL batches executed:                    {total_batches:,}", flush=True)
    print(f"  SPARQL results received:                    {total_sparql_results:,}", flush=True)
    print(f"  Failed coordinate parses:                   {total_failed_parse:,}", flush=True)
    print(f"  Entities updated with coords:               +{total_updated:,}", flush=True)
    print(f"  Entities with coords: {entities_with_coords_before:,} -> {entities_with_coords_after:,} (+{new_coords:,})", flush=True)
    print(f"  Duration: {duration}", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
