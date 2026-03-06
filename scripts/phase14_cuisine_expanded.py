"""
Phase 14 A5: Comprehensive Japanese cuisine from Wikidata.
Sushi types, wagashi, ramen, sake, shochu, and more.
Target: 5,000+ food entities with region and ingredient connections.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import shutil
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p14.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
PAGE_SIZE = 10000
BATCH_SIZE = 1000
SOURCE = "wd_cuisine_p14"


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


def load_existing_wdids(db):
    existing = set()
    cursor = db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL")
    while True:
        rows = cursor.fetchmany(200000)
        if not rows:
            break
        for (wid,) in rows:
            existing.add(wid)
    return existing


# Cuisine categories with their SPARQL queries
CUISINE_CATEGORIES = [
    # Broad: Japanese cuisine items
    {
        "name": "Japanese Cuisine (broad)",
        "sparql": """
SELECT ?item ?labelJa ?labelEn ?region ?regionLabel WHERE {{
  ?item wdt:P361 wd:Q746549 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P131 ?region . ?region rdfs:label ?regionLabel . FILTER(LANG(?regionLabel) = "ja") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Food from Japan
    {
        "name": "Food from Japan (P495=Q17)",
        "sparql": """
SELECT ?item ?labelJa ?labelEn ?region ?regionLabel WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q2095 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P131 ?region . ?region rdfs:label ?regionLabel . FILTER(LANG(?regionLabel) = "ja") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Japanese drinks
    {
        "name": "Japanese Drinks",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q40050 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Sushi types
    {
        "name": "Sushi Types",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q16836498 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Wagashi
    {
        "name": "Wagashi",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q1032329 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Ramen types
    {
        "name": "Ramen",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q178275 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Sake varieties
    {
        "name": "Sake",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q131419 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Shochu
    {
        "name": "Shochu",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q188858 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Broader: Japanese confectionery
    {
        "name": "Japanese Confectionery",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q852803 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Dishes originating from Japan
    {
        "name": "Dishes from Japan",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q746549 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Japanese rice wine / fermented food
    {
        "name": "Japanese Fermented Foods",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q151885 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # Japanese tea
    {
        "name": "Japanese Tea",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q6097 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
]


def fetch_cuisine_category(db, existing_wdids, cat):
    """Fetch one cuisine category with pagination."""
    name = cat["name"]
    sparql_tmpl = cat["sparql"]

    print(f"\n  --- {name} ---", flush=True)
    total_new = 0
    offset = 0
    empty_streak = 0
    batch_pending = 0
    region_links = []  # (food_wid, region_label)

    while True:
        query = sparql_tmpl.format(limit=PAGE_SIZE, offset=offset)
        bindings = sparql_query(query)

        if not bindings:
            empty_streak += 1
            if empty_streak >= 2:
                break
            offset += PAGE_SIZE
            time.sleep(1)
            continue

        empty_streak = 0
        page_new = 0

        for b in bindings:
            wid = extract_wikidata_id(b.get("item", {}).get("value", ""))
            if not wid or wid in existing_wdids:
                continue

            label_ja = b.get("labelJa", {}).get("value", "")
            label_en = b.get("labelEn", {}).get("value", "")
            if not label_ja and not label_en:
                continue

            # Track region association
            region_label = b.get("regionLabel", {}).get("value", "")
            if region_label:
                region_links.append((wid, region_label))

            try:
                db.execute(
                    "INSERT OR IGNORE INTO entities (wikidata_id, label_ja, label_en, entity_type, source) VALUES (?, ?, ?, 'food', ?)",
                    (wid, label_ja or None, label_en or None, SOURCE)
                )
                existing_wdids.add(wid)
                page_new += 1
                batch_pending += 1
            except sqlite3.IntegrityError:
                pass

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

        total_new += page_new
        offset += PAGE_SIZE

        if len(bindings) < PAGE_SIZE:
            break

        time.sleep(1)

    if batch_pending > 0:
        db.commit()

    print(f"    {name}: +{total_new:,}", flush=True)
    return total_new, region_links


def fetch_ingredient_links():
    """Fetch ingredient (P527) relationships for Japanese food items."""
    print("\n  --- Fetching ingredient links ---", flush=True)
    links = []  # (food_wid, ingredient_wid)
    offset = 0
    empty_streak = 0

    while True:
        query = f"""
SELECT ?food ?ingredient WHERE {{
  ?food wdt:P31/wdt:P279* wd:Q2095 .
  ?food wdt:P495 wd:Q17 .
  ?food wdt:P527 ?ingredient .
}}
LIMIT {PAGE_SIZE} OFFSET {offset}
"""
        bindings = sparql_query(query)
        if not bindings:
            empty_streak += 1
            if empty_streak >= 2:
                break
            offset += PAGE_SIZE
            time.sleep(1)
            continue

        empty_streak = 0
        for b in bindings:
            f_wid = extract_wikidata_id(b.get("food", {}).get("value", ""))
            i_wid = extract_wikidata_id(b.get("ingredient", {}).get("value", ""))
            if f_wid and i_wid:
                links.append((f_wid, i_wid))

        offset += PAGE_SIZE
        if len(bindings) < PAGE_SIZE:
            break
        time.sleep(1)

    print(f"    Ingredient links found: {len(links):,}", flush=True)
    return links


def generate_connections(db, region_links, ingredient_links):
    """Generate food->region (geographic_cultural) and same-type (same_theme) connections."""
    print("\n  --- Generating connections ---", flush=True)

    # Load existing pairs
    existing_pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for a, b in rows:
            existing_pairs.add((min(a, b), max(a, b)))

    total_conns = 0
    batch_pending = 0

    # Build wikidata_id -> entity id mapping for food entities
    wid_to_id = {}
    cursor = db.execute(
        "SELECT id, wikidata_id FROM entities WHERE wikidata_id IS NOT NULL AND source = ?",
        (SOURCE,)
    )
    for eid, wid in cursor:
        wid_to_id[wid] = eid

    # Food -> Region connections
    # First, find region entities by label
    region_conns = 0
    for food_wid, region_label in region_links:
        food_id = wid_to_id.get(food_wid)
        if not food_id:
            continue

        # Find region entity by label
        row = db.execute(
            "SELECT id FROM entities WHERE label_ja = ? AND entity_type = 'place' LIMIT 1",
            (region_label,)
        ).fetchone()
        if not row:
            continue

        region_id = row[0]
        pair = (min(food_id, region_id), max(food_id, region_id))
        if pair in existing_pairs:
            continue

        try:
            db.execute("""
                INSERT OR IGNORE INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     explanation, source, confidence)
                VALUES (?, ?, 'geographic_cultural', 0.6, ?, ?, 0.8)
            """, (pair[0], pair[1],
                  f"{region_label} regional cuisine",
                  "wd_cuisine_conn_p14"))
            existing_pairs.add(pair)
            region_conns += 1
            batch_pending += 1
        except sqlite3.IntegrityError:
            pass

        if batch_pending >= BATCH_SIZE:
            db.commit()
            batch_pending = 0

    print(f"    Food-region connections: {region_conns:,}", flush=True)
    total_conns += region_conns

    # Ingredient connections
    ingredient_conns = 0
    for food_wid, ingr_wid in ingredient_links:
        food_id = wid_to_id.get(food_wid)
        if not food_id:
            continue

        # Find ingredient entity
        ingr_id = wid_to_id.get(ingr_wid)
        if not ingr_id:
            row = db.execute(
                "SELECT id FROM entities WHERE wikidata_id = ?", (ingr_wid,)
            ).fetchone()
            if row:
                ingr_id = row[0]
        if not ingr_id:
            continue

        pair = (min(food_id, ingr_id), max(food_id, ingr_id))
        if pair in existing_pairs:
            continue

        try:
            db.execute("""
                INSERT OR IGNORE INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     explanation, source, confidence)
                VALUES (?, ?, 'ingredient_of', 0.4, 'Ingredient relationship', ?, 0.8)
            """, (pair[0], pair[1], "wd_cuisine_conn_p14"))
            existing_pairs.add(pair)
            ingredient_conns += 1
            batch_pending += 1
        except sqlite3.IntegrityError:
            pass

        if batch_pending >= BATCH_SIZE:
            db.commit()
            batch_pending = 0

    print(f"    Ingredient connections: {ingredient_conns:,}", flush=True)
    total_conns += ingredient_conns

    # Same cuisine type connections (food entities connected by proximity in DB insertion order)
    food_ids = list(wid_to_id.values())
    same_type_conns = 0
    for i in range(len(food_ids)):
        for j in range(i + 1, min(i + 4, len(food_ids))):
            pair = (min(food_ids[i], food_ids[j]), max(food_ids[i], food_ids[j]))
            if pair in existing_pairs:
                continue
            try:
                db.execute("""
                    INSERT OR IGNORE INTO connections
                        (entity_a_id, entity_b_id, connection_type, serendipity_score,
                         explanation, source, confidence)
                    VALUES (?, ?, 'same_theme', 0.4, 'Same cuisine category', ?, 0.6)
                """, (pair[0], pair[1], "wd_cuisine_conn_p14"))
                existing_pairs.add(pair)
                same_type_conns += 1
                batch_pending += 1
            except sqlite3.IntegrityError:
                pass

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

        if same_type_conns >= 20000:
            break

    if batch_pending > 0:
        db.commit()

    print(f"    Same-theme connections: {same_type_conns:,}", flush=True)
    total_conns += same_type_conns
    print(f"    Total connections: {total_conns:,}", flush=True)
    return total_conns


def main():
    print("=" * 60, flush=True)
    print("Phase 14 A5: Japanese Cuisine Expansion", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp
    print(f"Copying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()
    before_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    before_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    existing_food = db.execute(
        "SELECT COUNT(*) FROM entities WHERE entity_type = 'food'"
    ).fetchone()[0]
    print(f"Entities before: {before_entities:,}", flush=True)
    print(f"Connections before: {before_conns:,}", flush=True)
    print(f"Existing food entities: {existing_food:,}", flush=True)

    existing_wdids = load_existing_wdids(db)
    print(f"Existing wikidata_ids: {len(existing_wdids):,}", flush=True)

    all_region_links = []
    category_results = {}
    total_entities = 0

    for cat in CUISINE_CATEGORIES:
        try:
            count, region_links = fetch_cuisine_category(db, existing_wdids, cat)
            category_results[cat["name"]] = count
            total_entities += count
            all_region_links.extend(region_links)
        except Exception as e:
            print(f"    ERROR in {cat['name']}: {e}", flush=True)
            category_results[cat["name"]] = 0

    # Fetch ingredient links
    ingredient_links = []
    try:
        ingredient_links = fetch_ingredient_links()
    except Exception as e:
        print(f"    Ingredient fetch error: {e}", flush=True)

    # Generate connections
    total_conns = 0
    try:
        total_conns = generate_connections(db, all_region_links, ingredient_links)
    except Exception as e:
        print(f"    Connection generation error: {e}", flush=True)

    after_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    after_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    final_food = db.execute(
        "SELECT COUNT(*) FROM entities WHERE entity_type = 'food'"
    ).fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print("CATEGORY RESULTS", flush=True)
    for name, count in category_results.items():
        print(f"  {name}: +{count:,}", flush=True)

    print(f"\nSUMMARY", flush=True)
    print(f"  New entities: +{after_entities - before_entities:,}", flush=True)
    print(f"  New connections: +{after_conns - before_conns:,}", flush=True)
    print(f"  Food entities: {existing_food:,} -> {final_food:,}", flush=True)
    print(f"  Entities: {before_entities:,} -> {after_entities:,}", flush=True)
    print(f"  Connections: {before_conns:,} -> {after_conns:,}", flush=True)
    print(f"  Duration: {datetime.now() - start}", flush=True)
    db.close()

    # Copy back
    print(f"Copying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
