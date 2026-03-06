"""
Phase 14 A2: Complete OSM temple/shrine acquisition for all 47 prefectures.
Uses Overpass API with administrative area filters per prefecture.
Deduplicates against existing osm_temple_p13/osm_shrine_p13 entries.
Target: reach 100,000+ temples+shrines total.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import math
import shutil
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p14.db"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
BATCH_SIZE = 1000

# All 47 prefectures with OSM relation IDs
PREFECTURES = {
    "北海道": 3795658,
    "青森": 1831051,
    "岩手": 1831052,
    "宮城": 1831053,
    "秋田": 1831054,
    "山形": 1831055,
    "福島": 1831056,
    "茨城": 1831057,
    "栃木": 1831058,
    "群馬": 1831059,
    "埼玉": 1831060,
    "千葉": 1831061,
    "東京": 1543125,
    "神奈川": 1831063,
    "新潟": 1831064,
    "富山": 1831065,
    "石川": 1831066,
    "福井": 1831067,
    "山梨": 1831068,
    "長野": 1831069,
    "岐阜": 1831070,
    "静岡": 1831071,
    "愛知": 1831072,
    "三重": 1831073,
    "滋賀": 1831074,
    "京都": 1831075,
    "大阪": 1831076,
    "兵庫": 1831077,
    "奈良": 1831078,
    "和歌山": 1831079,
    "鳥取": 1831080,
    "島根": 1831081,
    "岡山": 1831082,
    "広島": 1831083,
    "山口": 1831084,
    "徳島": 1831085,
    "香川": 1831086,
    "愛媛": 1831087,
    "高知": 1831088,
    "福岡": 1831089,
    "佐賀": 1831090,
    "長崎": 1831091,
    "熊本": 1831092,
    "大分": 1831093,
    "宮崎": 1831094,
    "鹿児島": 1831095,
    "沖縄": 1831096,
}


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
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
            print(f"    Overpass error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f"    Waiting {wait}s...", flush=True)
                time.sleep(wait)
    return None


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_existing(db):
    """Load existing wikidata_ids and name+coord pairs for dedup."""
    existing_wdid = set()
    existing_coords = {}  # label -> [(lat, lon), ...]

    print("  Loading existing entities for dedup...", flush=True)
    cursor = db.execute("SELECT label_ja, wikidata_id, lat, lon FROM entities")
    count = 0
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
        count += len(rows)
        if count % 1000000 == 0:
            print(f"    Loaded {count:,} entities...", flush=True)

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


def query_prefecture(pref_name, rel_id, religion):
    """Query Overpass for temples or shrines in a specific prefecture."""
    area_id = rel_id + 3600000000
    query = f"""
[out:json][timeout:300];
area(id:{area_id})->.searchArea;
(
  node["amenity"="place_of_worship"]["religion"="{religion}"](area.searchArea);
  way["amenity"="place_of_worship"]["religion"="{religion}"](area.searchArea);
  relation["amenity"="place_of_worship"]["religion"="{religion}"](area.searchArea);
);
out center tags;
"""
    return overpass_query(query)


def process_prefecture(db, pref_name, rel_id, religion, source_tag,
                       existing_wdid, existing_coords):
    """Process one prefecture for one religion type."""
    result = query_prefecture(pref_name, rel_id, religion)
    if not result:
        print(f"    {pref_name} ({religion}): FAILED", flush=True)
        return 0, 0, 0

    elements = result.get("elements", [])
    fetched = len(elements)
    inserted = 0
    skipped = 0
    batch_pending = 0

    for elem in elements:
        tags = elem.get("tags", {})

        # Get coordinates
        lat = elem.get("lat") or elem.get("center", {}).get("lat")
        lon = elem.get("lon") or elem.get("center", {}).get("lon")
        if not lat or not lon:
            skipped += 1
            continue

        # Get name
        name = tags.get("name:ja") or tags.get("name") or tags.get("name:en")
        if not name:
            skipped += 1
            continue

        name_en = tags.get("name:en") or tags.get("name:ja_rm") or name
        wikidata_id = tags.get("wikidata")
        denomination = tags.get("denomination", "")

        # Dedup check
        if is_duplicate(name, lat, lon, existing_wdid, existing_coords, wikidata_id):
            skipped += 1
            continue

        try:
            db.execute("""
                INSERT OR IGNORE INTO entities
                    (label_ja, label_en, entity_type, lat, lon, wikidata_id, source)
                VALUES (?, ?, 'place', ?, ?, ?, ?)
            """, (name, name_en, lat, lon, wikidata_id, source_tag))

            # Update dedup structures
            if wikidata_id:
                existing_wdid.add(wikidata_id)
            if name not in existing_coords:
                existing_coords[name] = []
            existing_coords[name].append((lat, lon))

            inserted += 1
            batch_pending += 1
        except sqlite3.IntegrityError:
            pass

        if batch_pending >= BATCH_SIZE:
            db.commit()
            batch_pending = 0

    if batch_pending > 0:
        db.commit()

    return fetched, inserted, skipped


def main():
    print("=" * 60, flush=True)
    print("Phase 14 A2: OSM Complete Prefecture Temple/Shrine Expansion", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp for heavy writes
    print(f"Copying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()
    before = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    existing_temples = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source LIKE '%temple%'"
    ).fetchone()[0]
    existing_shrines = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source LIKE '%shrine%'"
    ).fetchone()[0]
    print(f"Entities before: {before:,}", flush=True)
    print(f"Existing temples: {existing_temples:,}", flush=True)
    print(f"Existing shrines: {existing_shrines:,}", flush=True)

    existing_wdid, existing_coords = load_existing(db)

    total_temple_fetched = 0
    total_temple_inserted = 0
    total_shrine_fetched = 0
    total_shrine_inserted = 0
    errors = 0

    for pref_name, rel_id in PREFECTURES.items():
        print(f"\n  [{pref_name}]", flush=True)

        # Buddhist temples
        try:
            tf, ti, ts = process_prefecture(
                db, pref_name, rel_id, "buddhist", "osm_temple_p14",
                existing_wdid, existing_coords
            )
            total_temple_fetched += tf
            total_temple_inserted += ti
            print(f"    Temples: fetched={tf:,}, new={ti:,}, skipped={ts:,}", flush=True)
        except Exception as e:
            print(f"    Temples ERROR: {e}", flush=True)
            errors += 1

        time.sleep(10)

        # Shinto shrines
        try:
            sf, si, ss = process_prefecture(
                db, pref_name, rel_id, "shinto", "osm_shrine_p14",
                existing_wdid, existing_coords
            )
            total_shrine_fetched += sf
            total_shrine_inserted += si
            print(f"    Shrines: fetched={sf:,}, new={si:,}, skipped={ss:,}", flush=True)
        except Exception as e:
            print(f"    Shrines ERROR: {e}", flush=True)
            errors += 1

        time.sleep(10)

    after = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    final_temples = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source LIKE '%temple%'"
    ).fetchone()[0]
    final_shrines = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source LIKE '%shrine%'"
    ).fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print("SUMMARY", flush=True)
    print(f"  Temples: fetched={total_temple_fetched:,}, new={total_temple_inserted:,}", flush=True)
    print(f"  Shrines: fetched={total_shrine_fetched:,}, new={total_shrine_inserted:,}", flush=True)
    print(f"  Total new: +{total_temple_inserted + total_shrine_inserted:,}", flush=True)
    print(f"  Entities: {before:,} -> {after:,}", flush=True)
    print(f"  Temple total: {existing_temples:,} -> {final_temples:,}", flush=True)
    print(f"  Shrine total: {existing_shrines:,} -> {final_shrines:,}", flush=True)
    print(f"  Combined temples+shrines: {final_temples + final_shrines:,}", flush=True)
    print(f"  Errors: {errors}", flush=True)
    print(f"  Duration: {datetime.now() - start}", flush=True)
    db.close()

    # Copy back
    print(f"Copying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
