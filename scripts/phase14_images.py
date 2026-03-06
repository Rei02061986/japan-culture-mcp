"""
Phase 14 A6: Add image_url column and populate from Wikidata P18.
Queries existing entities with wikidata_id in batches, fetches P18 image
filenames, converts to Wikimedia Commons URLs, and updates entities.
Target: image URLs for ~50,000+ entities.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import shutil
import os
import hashlib
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p14.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
BATCH_SIZE = 100  # Wikidata VALUES batch size for SPARQL
COMMIT_SIZE = 1000


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def sparql_query(query, retries=5):
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
                return json.loads(resp.read().decode("utf-8")).get("results", {}).get("bindings", [])
        except Exception as e:
            status = ""
            if hasattr(e, "code"):
                status = f" (HTTP {e.code})"
            print(f"    SPARQL error{status} (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = min(10 * (2 ** attempt), 120)
                if hasattr(e, "code") and e.code in (429, 504):
                    wait = max(wait, 30)
                print(f"    Waiting {wait}s...", flush=True)
                time.sleep(wait)
    return []


def extract_wikidata_id(uri):
    if uri and "/entity/" in uri:
        return uri.split("/entity/")[-1]
    return None


def commons_url_from_filename(filename):
    """Convert a Wikimedia Commons filename to a direct URL.

    The URL scheme is:
    https://upload.wikimedia.org/wikipedia/commons/{hash[0]}/{hash[0:2]}/{filename}
    where hash = md5(filename_with_spaces_replaced_by_underscores)
    """
    if not filename:
        return None

    # If it is already a full URL, return as-is
    if filename.startswith("http"):
        # Extract filename from commons URL if needed
        if "commons.wikimedia.org" in filename or "upload.wikimedia.org" in filename:
            return filename
        return filename

    # Clean the filename
    fname = filename.replace(" ", "_")

    # Compute MD5 hash
    md5 = hashlib.md5(fname.encode("utf-8")).hexdigest()

    # Build URL
    encoded_fname = urllib.parse.quote(fname)
    url = f"https://upload.wikimedia.org/wikipedia/commons/{md5[0]}/{md5[0:2]}/{encoded_fname}"
    return url


def ensure_image_column(db):
    """Add image_url column if it does not exist."""
    # Check if column exists
    cursor = db.execute("PRAGMA table_info(entities)")
    columns = [row[1] for row in cursor.fetchall()]
    if "image_url" not in columns:
        print("  Adding image_url column to entities table...", flush=True)
        db.execute("ALTER TABLE entities ADD COLUMN image_url TEXT")
        db.commit()
        print("  Column added.", flush=True)
    else:
        print("  image_url column already exists.", flush=True)


def load_entities_with_wikidata_id(db):
    """Load all entities that have a wikidata_id but no image_url yet."""
    entities = []  # (id, wikidata_id)
    cursor = db.execute("""
        SELECT id, wikidata_id FROM entities
        WHERE wikidata_id IS NOT NULL
          AND (image_url IS NULL OR image_url = '')
        ORDER BY id
    """)
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        entities.extend(rows)
    return entities


def fetch_images_batch(wikidata_ids):
    """Fetch P18 image for a batch of Wikidata IDs."""
    if not wikidata_ids:
        return {}

    values_str = " ".join(f"wd:{wid}" for wid in wikidata_ids)
    query = f"""
SELECT ?item ?image WHERE {{
  VALUES ?item {{ {values_str} }}
  ?item wdt:P18 ?image .
}}
"""
    bindings = sparql_query(query)
    results = {}  # wikidata_id -> image_url

    for b in bindings:
        wid = extract_wikidata_id(b.get("item", {}).get("value", ""))
        image_value = b.get("image", {}).get("value", "")
        if wid and image_value:
            # Convert the commons file reference to a URL
            if image_value.startswith("http://commons.wikimedia.org/wiki/Special:FilePath/"):
                # Extract filename from the FilePath URL
                fname = image_value.split("Special:FilePath/")[-1]
                fname = urllib.parse.unquote(fname)
                image_url = commons_url_from_filename(fname)
            elif "upload.wikimedia.org" in image_value:
                image_url = image_value
            else:
                # Try to convert as filename
                image_url = commons_url_from_filename(image_value)

            if image_url and wid not in results:
                results[wid] = image_url

    return results


def main():
    print("=" * 60, flush=True)
    print("Phase 14 A6: Image URL Population from Wikidata P18", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp
    print(f"Copying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_with_wdid = db.execute(
        "SELECT COUNT(*) FROM entities WHERE wikidata_id IS NOT NULL"
    ).fetchone()[0]
    print(f"Total entities: {total_entities:,}", flush=True)
    print(f"Entities with wikidata_id: {total_with_wdid:,}", flush=True)

    # Ensure image_url column exists
    ensure_image_column(db)

    # Check how many already have images
    existing_images = db.execute(
        "SELECT COUNT(*) FROM entities WHERE image_url IS NOT NULL AND image_url != ''"
    ).fetchone()[0]
    print(f"Entities already with image_url: {existing_images:,}", flush=True)

    # Load entities needing images
    print("Loading entities with wikidata_id needing images...", flush=True)
    entities = load_entities_with_wikidata_id(db)
    print(f"Entities to process: {len(entities):,}", flush=True)

    if not entities:
        print("No entities to process. Done.", flush=True)
        db.close()
        return

    total_updated = 0
    total_errors = 0
    commit_pending = 0

    # Process in batches of BATCH_SIZE
    for i in range(0, len(entities), BATCH_SIZE):
        batch = entities[i:i + BATCH_SIZE]
        batch_wids = [wid for _, wid in batch]
        batch_id_map = {wid: eid for eid, wid in batch}

        try:
            image_map = fetch_images_batch(batch_wids)

            for wid, image_url in image_map.items():
                eid = batch_id_map.get(wid)
                if not eid:
                    continue

                try:
                    db.execute(
                        "UPDATE entities SET image_url = ? WHERE id = ?",
                        (image_url, eid)
                    )
                    total_updated += 1
                    commit_pending += 1
                except sqlite3.Error as e:
                    total_errors += 1

            if commit_pending >= COMMIT_SIZE:
                db.commit()
                commit_pending = 0

        except Exception as e:
            print(f"    Batch error at offset {i}: {e}", flush=True)
            total_errors += 1

        # Progress reporting
        processed = min(i + BATCH_SIZE, len(entities))
        if processed % 10000 < BATCH_SIZE:
            pct = processed / len(entities) * 100
            print(f"    Progress: {processed:,}/{len(entities):,} ({pct:.1f}%), updated: {total_updated:,}, errors: {total_errors}", flush=True)

        # Rate limit: 1 request per second
        time.sleep(1)

    if commit_pending > 0:
        db.commit()

    final_images = db.execute(
        "SELECT COUNT(*) FROM entities WHERE image_url IS NOT NULL AND image_url != ''"
    ).fetchone()[0]

    # Sample some results
    print("\n  Sample image URLs:", flush=True)
    samples = db.execute("""
        SELECT label_ja, label_en, image_url FROM entities
        WHERE image_url IS NOT NULL AND image_url != ''
        ORDER BY RANDOM() LIMIT 5
    """).fetchall()
    for label_ja, label_en, img_url in samples:
        name = label_ja or label_en or "?"
        short_url = img_url[:80] + "..." if len(img_url) > 80 else img_url
        print(f"    {name}: {short_url}", flush=True)

    print(f"\n{'='*60}", flush=True)
    print("SUMMARY", flush=True)
    print(f"  Entities processed: {len(entities):,}", flush=True)
    print(f"  Images found and updated: {total_updated:,}", flush=True)
    print(f"  Errors: {total_errors}", flush=True)
    print(f"  Image coverage: {existing_images:,} -> {final_images:,}", flush=True)
    print(f"  Image coverage %: {final_images/total_with_wdid*100:.1f}% of entities with wikidata_id", flush=True)
    print(f"  Duration: {datetime.now() - start}", flush=True)
    db.close()

    # Copy back
    print(f"Copying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
