"""
Phase 8A-2: Wikidata cultural properties of Japan.
Uses P1435 (heritage designation) + P17=Japan, and P195 (collection in Japanese museums).
Also fetches Japanese paintings and traditional crafts.
Target: >= 1,000 entities with coordinates.
"""
import requests
import json
import time
import sqlite3
import os
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.3 (contact@example.com)'
}

CATEGORIES = {
    # All heritage-designated sites in Japan (P1435 = any, P17 = Japan)
    "heritage_sites": """
SELECT ?item ?itemLabel ?itemDescription ?coord ?status ?statusLabel WHERE {{
  ?item wdt:P1435 ?status .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    # Items in Japanese museum collections (P195)
    "museum_collections": """
SELECT ?item ?itemLabel ?itemDescription ?coord ?collection ?collectionLabel WHERE {{
  ?item wdt:P195 ?collection .
  ?collection wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    # Japanese paintings
    "paintings": """
SELECT ?item ?itemLabel ?itemDescription ?coord WHERE {{
  ?item wdt:P31 wd:Q3305213 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    # Traditional Japanese performing arts venues
    "performing_arts_venues": """
SELECT ?item ?itemLabel ?itemDescription ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q24354 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    # Japanese cultural landscapes
    "cultural_landscapes": """
SELECT ?item ?itemLabel ?itemDescription ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q210272 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    # Sculptures/statues in Japan
    "sculptures": """
SELECT ?item ?itemLabel ?itemDescription ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q860861 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
}

# Auto-tagging rules
LABEL_THEMES = {
    '神社': 'shrine_temple', '寺': 'shrine_temple', '院': 'shrine_temple',
    '城': 'samurai', '武': 'samurai',
    '庭園': 'nature_communion', '公園': 'nature_communion',
    '古墳': 'historical_event', '遺跡': 'historical_event',
    '橋': 'architecture', '塔': 'architecture', '門': 'architecture',
    '仏': 'sacred_profane', '像': 'sacred_profane',
    '祭': 'matsuri', '踊': 'performing_arts',
    '絵': 'visual_arts', '画': 'visual_arts',
    '彫刻': 'visual_arts', '屏風': 'visual_arts',
    '刀': 'samurai', '鎧': 'samurai',
    '焼': 'traditional_craft', '織': 'traditional_craft',
    '茶': 'wabi_sabi',
}

LABEL_MEDIUMS = {
    '神社': 'architecture', '寺': 'architecture', '城': 'architecture',
    '橋': 'architecture', '塔': 'architecture', '門': 'architecture',
    '庭園': 'architecture', '堂': 'architecture',
    '絵': 'painting', '画': 'painting', '屏風': 'painting',
    '彫刻': 'sculpture', '像': 'sculpture',
    '焼': 'craft', '織': 'craft', '刀': 'craft',
}

COORD_REGEX = re.compile(r'Point\(([-\d.]+)\s+([-\d.]+)\)')

def coord_to_geo(lat, lon):
    if lat > 41.0: return 'hokkaido'
    if lat > 38.0: return 'tohoku'
    if lat > 36.0 and lon > 138.5: return 'kanto'
    if lat > 35.0 and lon < 137.0: return 'kinki'
    if lat > 34.0 and lon > 137.0: return 'chubu'
    if lat > 33.5 and lon < 134.0: return 'chugoku'
    if lat > 33.0 and lon > 133.0: return 'shikoku'
    return 'kyushu'

def guess_era(label):
    era_kw = {
        '縄文': 'ancient', '弥生': 'ancient', '古墳': 'ancient',
        '飛鳥': 'ancient', '奈良': 'ancient', '平安': 'ancient',
        '鎌倉': 'medieval', '室町': 'medieval', '南北朝': 'medieval',
        '戦国': 'medieval', '安土': 'medieval', '桃山': 'medieval',
        '江戸': 'edo_early', '元禄': 'edo_early',
        '幕末': 'edo_late',
        '明治': 'meiji_taisho', '大正': 'meiji_taisho',
        '昭和': 'showa_postwar',
    }
    for kw, era in era_kw.items():
        if kw in label:
            return era
    return None


def sparql_fetch(query_template, offset=0):
    q = query_template.format(offset=offset)
    for attempt in range(3):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={'query': q},
                headers=HEADERS,
                timeout=90
            )
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"    429, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            else:
                print(f"    HTTP {resp.status_code}, attempt {attempt+1}", flush=True)
                time.sleep(30)
        except Exception as e:
            print(f"    ERROR: {e}, attempt {attempt+1}", flush=True)
            time.sleep(30)
    return []


def main():
    db = sqlite3.connect(DB_PATH)
    os.makedirs('data/wikidata', exist_ok=True)

    existing_qids = set()
    for row in db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        existing_qids.add(row[0])
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])

    print(f"Existing QIDs: {len(existing_qids):,}", flush=True)
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    total_new = 0

    for cat_name, query_template in CATEGORIES.items():
        print(f"\n=== {cat_name} ===", flush=True)
        all_results = []
        offset = 0

        while True:
            print(f"  offset={offset}...", flush=True)
            bindings = sparql_fetch(query_template, offset)
            if not bindings:
                break
            all_results.extend(bindings)
            print(f"    Got {len(bindings)}, total: {len(all_results)}", flush=True)
            if len(bindings) < 500:
                break
            offset += 500
            time.sleep(10)

        with open(f'data/wikidata/cultural_{cat_name}.json', 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False)

        cat_new = 0
        for rec in all_results:
            label = rec.get('itemLabel', {}).get('value', '')
            if not label or label.startswith('Q'):
                continue

            wikidata_uri = rec.get('item', {}).get('value', '')
            wikidata_id = wikidata_uri.split('/')[-1] if wikidata_uri else None

            if wikidata_id and wikidata_id in existing_qids:
                continue
            if label in existing_labels:
                continue

            # Parse coordinates
            lat, lon = None, None
            coord_str = rec.get('coord', {}).get('value', '')
            if coord_str:
                m = COORD_REGEX.search(coord_str)
                if m:
                    lon = float(m.group(1))
                    lat = float(m.group(2))

            label_en = rec.get('itemDescription', {}).get('value', '')
            if label_en and (len(label_en) > 80 or ',' in label_en):
                label_en = None

            # Determine entity_type
            if lat and lon:
                entity_type = 'place'
            else:
                entity_type = 'artifact'

            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, wikidata_id, lat, lon, source)
                VALUES (?, ?, ?, ?, ?, ?, 'wikidata_cultural_phase8')
            """, (label, label_en, entity_type, wikidata_id, lat, lon))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]

            if wikidata_id:
                existing_qids.add(wikidata_id)
            existing_labels.add(label)

            # Auto-tag from label
            theme_set = False
            for kw, theme in LABEL_THEMES.items():
                if kw in label:
                    db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'cultural_label', 0.7)", (eid, theme))
                    theme_set = True
                    break
            if not theme_set:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', 'craft_mastery', 'cultural_default', 0.5)", (eid,))

            for kw, medium in LABEL_MEDIUMS.items():
                if kw in label:
                    db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'cultural_label', 0.7)", (eid, medium))
                    break

            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'aesthetic', 'cultural_default', 0.7)", (eid,))

            if lat and lon:
                geo = coord_to_geo(lat, lon)
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', ?, 'coord_mapping', 0.9)", (eid, geo))

            era = guess_era(label)
            if era:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'label_era', 0.6)", (eid, era))

            cat_new += 1

        db.commit()
        total_new += cat_new
        print(f"  New entities: {cat_new:,}", flush=True)

    # Final stats
    cultural_count = db.execute("SELECT COUNT(*) FROM entities WHERE source='wikidata_cultural_phase8'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    coord_count = db.execute("SELECT COUNT(*) FROM entities WHERE source='wikidata_cultural_phase8' AND lat IS NOT NULL").fetchone()[0]

    print(f"\n=== Cultural Properties Import Complete ===", flush=True)
    print(f"Total new entities: {total_new:,}", flush=True)
    print(f"Cultural entities in DB: {cultural_count:,}", flush=True)
    print(f"With coordinates: {coord_count:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
