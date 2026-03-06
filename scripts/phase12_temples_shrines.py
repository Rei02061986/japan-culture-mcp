"""
Phase 12 Stream A1: 寺院・神社 大量取得
Wikidata SPARQL + OSM Overpass API
目標: 寺院40,000+ 神社30,000+
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
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


def run_overpass(query, retries=3):
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({"data": query}).encode()
            req = urllib.request.Request(OVERPASS_ENDPOINT, data=data, headers={
                "User-Agent": UA,
            })
            with urllib.request.urlopen(req, timeout=300) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  Overpass error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(30 * (attempt + 1))
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


def main():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    # Get existing labels AND wikidata_ids for dedup
    existing = set()
    existing_wdid = set()
    for row in db.execute("SELECT label_ja, wikidata_id FROM entities"):
        existing.add(row[0])
        if row[1]:
            existing_wdid.add(row[1])
    print(f"Existing entities: {len(existing):,} (wikidata_ids: {len(existing_wdid):,})", flush=True)

    total_new = 0

    # ── 1. Wikidata: Buddhist temples ──
    print("\n=== Wikidata: Buddhist temples ===", flush=True)
    result = run_sparql("""
SELECT ?item ?itemLabel ?coords ?inception ?adminLabel WHERE {
  ?item wdt:P31/wdt:P279* wd:Q160742 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  OPTIONAL { ?item wdt:P571 ?inception }
  OPTIONAL { ?item wdt:P131 ?admin }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 50000
""")
    if result:
        bindings = result["results"]["bindings"]
        print(f"  Raw: {len(bindings):,}", flush=True)
        new = 0
        for b in bindings:
            label = b.get("itemLabel", {}).get("value", "")
            if not label or label in existing:
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
                VALUES (?, ?, 'place', ?, ?, ?, 'wikidata_temple')
            """, (label, label, lat, lon, wikidata_id))
            existing.add(label)
            existing_wdid.add(wikidata_id)
            new += 1
        db_commit_retry(db)
        total_new += new
        print(f"  New temples: {new:,}", flush=True)
    time.sleep(5)

    # ── 2. Wikidata: Shinto shrines ──
    print("\n=== Wikidata: Shinto shrines ===", flush=True)
    result = run_sparql("""
SELECT ?item ?itemLabel ?coords ?inception ?adminLabel WHERE {
  ?item wdt:P31/wdt:P279* wd:Q845945 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  OPTIONAL { ?item wdt:P571 ?inception }
  OPTIONAL { ?item wdt:P131 ?admin }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 50000
""")
    if result:
        bindings = result["results"]["bindings"]
        print(f"  Raw: {len(bindings):,}", flush=True)
        new = 0
        for b in bindings:
            label = b.get("itemLabel", {}).get("value", "")
            if not label or label in existing:
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
                VALUES (?, ?, 'place', ?, ?, ?, 'wikidata_shrine')
            """, (label, label, lat, lon, wikidata_id))
            existing.add(label)
            existing_wdid.add(wikidata_id)
            new += 1
        db_commit_retry(db)
        total_new += new
        print(f"  New shrines: {new:,}", flush=True)
    time.sleep(5)

    # ── 3. OSM Overpass: Buddhist temples ──
    print("\n=== OSM Overpass: Buddhist temples ===", flush=True)
    osm_result = run_overpass("""
[out:json][timeout:300];
area["ISO3166-1"="JP"]->.japan;
(
  node["amenity"="place_of_worship"]["religion"="buddhist"](area.japan);
  way["amenity"="place_of_worship"]["religion"="buddhist"](area.japan);
  relation["amenity"="place_of_worship"]["religion"="buddhist"](area.japan);
);
out center;
""")
    if osm_result:
        elements = osm_result.get("elements", [])
        print(f"  Raw OSM temples: {len(elements):,}", flush=True)
        new = 0
        for el in elements:
            name = el.get("tags", {}).get("name", "")
            if not name or name in existing:
                continue
            lat = el.get("lat") or (el.get("center", {}) or {}).get("lat")
            lon = el.get("lon") or (el.get("center", {}) or {}).get("lon")
            if not lat or not lon:
                continue
            db.execute("""
                INSERT OR IGNORE INTO entities (label_ja, label_en, entity_type, lat, lon, source)
                VALUES (?, ?, 'place', ?, ?, 'osm_temple')
            """, (name, name, lat, lon))
            existing.add(name)
            new += 1
            if new % 5000 == 0:
                db_commit_retry(db)
                print(f"    ... {new:,} imported", flush=True)
        db_commit_retry(db)
        total_new += new
        print(f"  New OSM temples: {new:,}", flush=True)
    time.sleep(15)

    # ── 4. OSM Overpass: Shinto shrines ──
    print("\n=== OSM Overpass: Shinto shrines ===", flush=True)
    osm_result = run_overpass("""
[out:json][timeout:300];
area["ISO3166-1"="JP"]->.japan;
(
  node["amenity"="place_of_worship"]["religion"="shinto"](area.japan);
  way["amenity"="place_of_worship"]["religion"="shinto"](area.japan);
  relation["amenity"="place_of_worship"]["religion"="shinto"](area.japan);
);
out center;
""")
    if osm_result:
        elements = osm_result.get("elements", [])
        print(f"  Raw OSM shrines: {len(elements):,}", flush=True)
        new = 0
        for el in elements:
            name = el.get("tags", {}).get("name", "")
            if not name or name in existing:
                continue
            lat = el.get("lat") or (el.get("center", {}) or {}).get("lat")
            lon = el.get("lon") or (el.get("center", {}) or {}).get("lon")
            if not lat or not lon:
                continue
            db.execute("""
                INSERT OR IGNORE INTO entities (label_ja, label_en, entity_type, lat, lon, source)
                VALUES (?, ?, 'place', ?, ?, 'osm_shrine')
            """, (name, name, lat, lon))
            existing.add(name)
            new += 1
            if new % 5000 == 0:
                db_commit_retry(db)
                print(f"    ... {new:,} imported", flush=True)
        db_commit_retry(db)
        total_new += new
        print(f"  New OSM shrines: {new:,}", flush=True)

    # ── Final stats ──
    total_ent = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    temples = db.execute("SELECT COUNT(*) FROM entities WHERE source LIKE '%temple%'").fetchone()[0]
    shrines = db.execute("SELECT COUNT(*) FROM entities WHERE source LIKE '%shrine%'").fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print(f"=== Phase 12 A1: Temple/Shrine Results ===", flush=True)
    print(f"Total new entities: {total_new:,}", flush=True)
    print(f"Temple entities: {temples:,}", flush=True)
    print(f"Shrine entities: {shrines:,}", flush=True)
    print(f"Total entities: {total_ent:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
