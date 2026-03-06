"""
Phase 12 Stream A2-A9: 弱い領域のWikidata一括取得
日本料理、TV番組、近代建築、日本庭園、方言、ファッション
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/0.8 (teddykmk@gmail.com)"


def run_sparql(query, retries=3):
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({"query": query, "format": "json"}).encode()
            req = urllib.request.Request(WIKIDATA_ENDPOINT, data=data, headers={
                "User-Agent": UA, "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  SPARQL error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
    return None


def parse_coord(wkt):
    m = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', wkt)
    if m:
        return float(m.group(2)), float(m.group(1))
    return None, None


def db_commit_retry(db, retries=5):
    for i in range(retries):
        try:
            db.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {i+1}: {e}", flush=True)
            time.sleep(3)
    return False


QUERIES = [
    # ── A3: 日本料理 ──
    ("cuisine_dish", "food", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  { ?item wdt:P31 wd:Q746549 }
  UNION { ?item wdt:P361 wd:Q182323 }
  UNION { ?item wdt:P279 wd:Q746549 }
  UNION { ?item wdt:P31 wd:Q16836498 }
  UNION { ?item wdt:P31 wd:Q1032329 }
  UNION { ?item wdt:P31 wd:Q178275 }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    ("cuisine_food_japan", "food", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  ?item wdt:P17 wd:Q17 .
  ?item wdt:P31/wdt:P279* wd:Q2095 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 10000
"""),

    ("sake_shochu", "food", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  { ?item wdt:P31/wdt:P279* wd:Q170238 }
  UNION { ?item wdt:P31/wdt:P279* wd:Q15091 }
  UNION { ?item wdt:P31 wd:Q272511 }
  UNION { ?item wdt:P31 wd:Q182929 }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    ("wagashi_sweets", "food", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  { ?item wdt:P31/wdt:P279* wd:Q1032329 }
  UNION { ?item wdt:P31/wdt:P279* wd:Q1377126 }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 3000
"""),

    # ── A4: TV番組/ドラマ ──
    ("tv_series_japan", "work", """
SELECT DISTINCT ?item ?itemLabel ?date WHERE {
  ?item wdt:P31/wdt:P279* wd:Q5398426 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL { ?item wdt:P580 ?date }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 20000
"""),

    ("taiga_drama", "work", """
SELECT DISTINCT ?item ?itemLabel ?date WHERE {
  ?item wdt:P31 wd:Q21191270 .
  OPTIONAL { ?item wdt:P580 ?date }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 500
"""),

    ("asadora", "work", """
SELECT DISTINCT ?item ?itemLabel ?date WHERE {
  ?item wdt:P31 wd:Q15205936 .
  OPTIONAL { ?item wdt:P580 ?date }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 500
"""),

    ("variety_show", "work", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  ?item wdt:P31/wdt:P279* wd:Q670914 .
  ?item wdt:P495 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 10000
"""),

    # ── A6: 近代建築 ──
    ("buildings_architect", "building", """
SELECT DISTINCT ?item ?itemLabel ?coords ?architectLabel WHERE {
  ?item wdt:P17 wd:Q17 .
  ?item wdt:P84 ?architect .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 15000
"""),

    ("registered_cultural", "building", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  ?item wdt:P31 wd:Q3395498 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 15000
"""),

    ("bridges_japan", "building", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  ?item wdt:P31/wdt:P279* wd:Q12280 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    ("castles_japan", "building", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  ?item wdt:P31/wdt:P279* wd:Q23413 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # ── A7: 日本庭園 ──
    ("japanese_gardens", "place", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  ?item wdt:P31/wdt:P279* wd:Q15773439 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 3000
"""),

    ("scenic_beauty", "place", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  { ?item wdt:P31 wd:Q2319498 }
  UNION { ?item wdt:P31 wd:Q5816381 }
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 2000
"""),

    # ── A8: 方言 ──
    ("dialects", "place", """
SELECT DISTINCT ?item ?itemLabel ?regionLabel WHERE {
  ?item wdt:P31/wdt:P279* wd:Q33273 .
  { ?item wdt:P17 wd:Q17 } UNION { ?item wdt:P495 wd:Q17 }
  OPTIONAL { ?item wdt:P131 ?region }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 1000
"""),

    ("japanese_languages", "place", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  ?item wdt:P31/wdt:P279* wd:Q34770 .
  ?item wdt:P17 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 500
"""),

    # ── A9: ファッション ──
    ("fashion_designers", "person", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106 wd:Q3501317 .
  ?item wdt:P27 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 2000
"""),

    ("fashion_brands", "organization", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  { ?item wdt:P31/wdt:P279* wd:Q1070990 . ?item wdt:P17 wd:Q17 }
  UNION { ?item wdt:P31/wdt:P279* wd:Q1618899 . ?item wdt:P17 wd:Q17 }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 3000
"""),

    ("fashion_magazines", "work", """
SELECT DISTINCT ?item ?itemLabel WHERE {
  ?item wdt:P31/wdt:P279* wd:Q5272157 .
  ?item wdt:P495 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 500
"""),

    # ── Extra: Museums, Parks, Hot springs ──
    ("museums_japan", "place", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  ?item wdt:P31/wdt:P279* wd:Q33506 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 10000
"""),

    ("onsen_japan", "place", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  ?item wdt:P31/wdt:P279* wd:Q41207 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    ("national_parks", "place", """
SELECT DISTINCT ?item ?itemLabel ?coords WHERE {
  { ?item wdt:P31 wd:Q46169 . ?item wdt:P17 wd:Q17 }
  UNION { ?item wdt:P31 wd:Q2600878 . ?item wdt:P17 wd:Q17 }
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 2000
"""),
]


def main():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    existing = set()
    existing_wdid = set()
    for row in db.execute("SELECT label_ja, wikidata_id FROM entities"):
        existing.add(row[0])
        if row[1]:
            existing_wdid.add(row[1])
    print(f"Existing entities: {len(existing):,} (wikidata_ids: {len(existing_wdid):,})", flush=True)

    grand_total = 0

    for query_name, entity_type, query in QUERIES:
        print(f"\n{'='*50}", flush=True)
        print(f"Query: {query_name} (type={entity_type})", flush=True)

        result = run_sparql(query)
        if not result or "results" not in result:
            print(f"  No results or error", flush=True)
            time.sleep(10)
            continue

        bindings = result["results"]["bindings"]
        print(f"  Raw: {len(bindings):,}", flush=True)

        new = 0
        for b in bindings:
            label = b.get("itemLabel", {}).get("value", "")
            if not label or label in existing or label.startswith("Q") or len(label) < 2:
                continue

            lat, lon = None, None
            coord = b.get("coords", {}).get("value", "")
            if coord:
                lat, lon = parse_coord(coord)

            wikidata_id = b.get("item", {}).get("value", "").split("/")[-1]
            if wikidata_id in existing_wdid:
                continue

            db.execute("""
                INSERT OR IGNORE INTO entities (label_ja, label_en, entity_type, lat, lon, wikidata_id, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (label, label, entity_type, lat, lon, wikidata_id, f"wikidata_{query_name}"))
            existing.add(label)
            existing_wdid.add(wikidata_id)
            new += 1

        db_commit_retry(db)
        grand_total += new
        print(f"  New: {new:,} (running total: {grand_total:,})", flush=True)
        time.sleep(5)

    # Final stats
    total_ent = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print(f"=== Phase 12 A2-A9 Results ===", flush=True)
    print(f"Grand total new entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_ent:,}", flush=True)

    # Category breakdown
    print(f"\nBy source:", flush=True)
    for row in db.execute("""
        SELECT source, COUNT(*) FROM entities
        WHERE source LIKE 'wikidata_%'
        GROUP BY source ORDER BY COUNT(*) DESC LIMIT 30
    """):
        print(f"  {row[0]}: {row[1]:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
