"""
Phase 13 A2: OSM Overpass full temple/shrine expansion.
Split by region to avoid timeouts. Coordinate-based dedup.
Target: 60K+ temples, 50K+ shrines total.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import math

DB_PATH = "/tmp/culture_ontology_work.db"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
UA = "japan-culture-mcp/0.9 (teddykmk@gmail.com)"
BATCH_SIZE = 5000

# Split Japan into regional bboxes for reliable queries
REGIONS = {
    "hokkaido": [41.3, 139.3, 45.6, 145.8],
    "tohoku": [37.7, 139.0, 41.6, 142.1],
    "kanto": [34.8, 138.4, 37.0, 141.0],
    "chubu": [34.7, 136.0, 38.0, 140.0],
    "kinki": [33.4, 134.0, 35.8, 137.0],
    "chugoku": [33.5, 130.7, 35.7, 134.5],
    "shikoku": [32.7, 132.0, 34.5, 134.8],
    "kyushu": [30.9, 129.4, 34.0, 132.2],
    "okinawa": [24.0, 122.9, 27.9, 131.3],
}


def open_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def overpass_query(query, retries=3):
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({"data": query}).encode("utf-8")
            req = urllib.request.Request(OVERPASS_URL, data=data, headers={
                "User-Agent": UA,
            })
            with urllib.request.urlopen(req, timeout=300) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  Overpass error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f"  Waiting {wait}s...", flush=True)
                time.sleep(wait)
    return None


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_existing(db):
    """Load existing wikidata_ids and name+coord pairs for dedup."""
    existing_wdid = set()
    existing_coords = {}  # label -> [(lat, lon), ...]

    print("  Loading existing entities for dedup...", flush=True)
    cursor = db.execute("SELECT label_ja, wikidata_id, lat, lon FROM entities")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for label, wid, lat, lon in rows:
            if wid:
                existing_wdid.add(wid)
            if label and lat is not None and lon is not None:
                if label not in existing_coords:
                    existing_coords[label] = []
                existing_coords[label].append((lat, lon))

    print(f"  Existing wikidata_ids: {len(existing_wdid):,}", flush=True)
    print(f"  Existing labeled coords: {len(existing_coords):,}", flush=True)
    return existing_wdid, existing_coords


def is_duplicate(label, lat, lon, existing_wdid, existing_coords, wikidata_id=None):
    """Check if entity is duplicate by wikidata_id or name+proximity."""
    if wikidata_id and wikidata_id in existing_wdid:
        return True
    if label in existing_coords:
        for ex_lat, ex_lon in existing_coords[label]:
            if haversine_m(lat, lon, ex_lat, ex_lon) < 500:
                return True
    return False


def fetch_and_insert(db, religion, entity_source, existing_wdid, existing_coords):
    """Fetch all temples or shrines from OSM by region."""
    total_fetched = 0
    total_inserted = 0
    batch = 0

    for region_name, bbox in REGIONS.items():
        s, w, n, e = bbox
        query = f"""
[out:json][timeout:180][bbox:{s},{w},{n},{e}];
(
  node["amenity"="place_of_worship"]["religion"="{religion}"];
  way["amenity"="place_of_worship"]["religion"="{religion}"];
  relation["amenity"="place_of_worship"]["religion"="{religion}"];
);
out center tags;
"""
        print(f"  {region_name}: querying {religion}...", flush=True)
        result = overpass_query(query)

        if not result:
            print(f"  {region_name}: FAILED, skipping", flush=True)
            time.sleep(15)
            continue

        elements = result.get("elements", [])
        total_fetched += len(elements)
        region_inserted = 0

        for elem in elements:
            tags = elem.get("tags", {})
            # Get coordinates
            lat = elem.get("lat") or elem.get("center", {}).get("lat")
            lon = elem.get("lon") or elem.get("center", {}).get("lon")
            if not lat or not lon:
                continue

            # Get name
            name = tags.get("name:ja") or tags.get("name") or tags.get("name:en")
            if not name:
                continue

            name_en = tags.get("name:en") or tags.get("name:ja_rm") or name
            wikidata_id = tags.get("wikidata")

            # Dedup check
            if is_duplicate(name, lat, lon, existing_wdid, existing_coords, wikidata_id):
                continue

            # Insert
            try:
                db.execute("""
                    INSERT OR IGNORE INTO entities
                        (label_ja, label_en, entity_type, lat, lon, wikidata_id, source)
                    VALUES (?, ?, 'place', ?, ?, ?, ?)
                """, (name, name_en, lat, lon, wikidata_id, entity_source))

                # Update dedup structures
                if wikidata_id:
                    existing_wdid.add(wikidata_id)
                if name not in existing_coords:
                    existing_coords[name] = []
                existing_coords[name].append((lat, lon))

                total_inserted += 1
                region_inserted += 1
                batch += 1

                if batch >= BATCH_SIZE:
                    db.commit()
                    batch = 0
            except sqlite3.IntegrityError:
                pass

        if batch > 0:
            db.commit()
            batch = 0

        print(f"  {region_name}: fetched={len(elements):,}, inserted={region_inserted:,}", flush=True)
        time.sleep(12)  # Rate limit

    return total_fetched, total_inserted


def main():
    print("=" * 60, flush=True)
    print("Phase 13 A2: OSM Temple/Shrine Full Expansion", flush=True)
    print("=" * 60, flush=True)

    db = open_db()
    before = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Entities before: {before:,}", flush=True)

    existing_wdid, existing_coords = load_existing(db)

    # Temples (Buddhist)
    print("\n--- TEMPLES (Buddhist) ---", flush=True)
    t_fetched, t_inserted = fetch_and_insert(
        db, "buddhist", "osm_temple_p13", existing_wdid, existing_coords
    )
    print(f"Temples: fetched={t_fetched:,}, inserted={t_inserted:,}", flush=True)

    # Shrines (Shinto)
    print("\n--- SHRINES (Shinto) ---", flush=True)
    s_fetched, s_inserted = fetch_and_insert(
        db, "shinto", "osm_shrine_p13", existing_wdid, existing_coords
    )
    print(f"Shrines: fetched={s_fetched:,}, inserted={s_inserted:,}", flush=True)

    after = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"SUMMARY", flush=True)
    print(f"  Temples: fetched={t_fetched:,}, new={t_inserted:,}", flush=True)
    print(f"  Shrines: fetched={s_fetched:,}, new={s_inserted:,}", flush=True)
    print(f"  Total new: +{t_inserted + s_inserted:,}", flush=True)
    print(f"  Entities: {before:,} -> {after:,}", flush=True)
    db.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
