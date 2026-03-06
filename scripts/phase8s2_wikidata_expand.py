"""
Phase 8 Stage 1: Wikidata expansion.
Fetch anime studios, traditional crafts, festivals, art movements, world heritage.
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
    "anime_studios": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?coord ?inception WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q210167 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  OPTIONAL {{ ?item wdt:P571 ?inception . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "organization",
        "theme": "visual_arts",
        "medium": "anime",
        "experience": "aesthetic",
    },
    "traditional_crafts": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?coord ?image WHERE {{
  ?item wdt:P31 wd:Q15893266 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  OPTIONAL {{ ?item wdt:P18 ?image . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "cultural_practice",
        "theme": "traditional_craft",
        "medium": "craft",
        "experience": "aesthetic",
    },
    "festivals_japan": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?coord ?date WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q132241 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  OPTIONAL {{ ?item wdt:P585 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "festival",
        "theme": "matsuri",
        "experience": "social",
    },
    "art_movements_japan": {
        "query": """SELECT ?item ?itemLabel ?itemDescription WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q968159 .
  ?item wdt:P17 wd:Q17 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "art_movement",
        "theme": "visual_arts",
        "experience": "intellectual",
    },
    "world_heritage_japan": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?coord WHERE {{
  ?item wdt:P1435 wd:Q9259 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "place",
        "theme": "sacred_profane",
        "experience": "aesthetic",
    },
    # Living national treasures - try different approach
    "LNT_ceramics": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?birth WHERE {{
  ?item wdt:P106 wd:Q1209498 .
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P166 ?award .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "person",
        "theme": "traditional_craft",
        "medium": "craft",
        "experience": "aesthetic",
    },
    # Japanese music groups/bands
    "music_groups": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?inception WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q215380 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P571 ?inception . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "organization",
        "theme": "musical_arts",
        "medium": "music",
        "experience": "aesthetic",
    },
    # Japanese TV drama series
    "tv_drama": {
        "query": """SELECT ?item ?itemLabel ?date WHERE {{
  ?item wdt:P31 wd:Q5398426 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P580 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "work",
        "theme": "literary_arts",
        "medium": "film",
        "experience": "aesthetic",
    },
    # Japanese literature prizes/awards
    "literature_works": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?date WHERE {{
  ?item wdt:P31 wd:Q7725634 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "work",
        "theme": "literary_arts",
        "medium": "literature",
        "experience": "intellectual",
    },
    # Hot springs (onsen) - expanded
    "onsen_expanded": {
        "query": """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q27185 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "place",
        "theme": "nature_communion",
        "experience": "physical",
    },
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

def year_to_era(year):
    if year < 1185: return 'ancient'
    if year < 1573: return 'medieval'
    if year < 1700: return 'edo_early'
    if year < 1868: return 'edo_late'
    if year < 1926: return 'meiji_taisho'
    if year < 1945: return 'showa_prewar'
    if year < 1989: return 'showa_postwar'
    if year < 2019: return 'heisei'
    return 'reiwa'

def sparql_fetch(query_template, offset=0):
    q = query_template.format(offset=offset)
    for attempt in range(3):
        try:
            resp = requests.get(WIKIDATA_SPARQL, params={'query': q}, headers=HEADERS, timeout=90)
            if resp.status_code == 429:
                time.sleep(60 * (attempt + 1))
                continue
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            print(f"    HTTP {resp.status_code}", flush=True)
            time.sleep(30)
        except Exception as e:
            print(f"    ERROR: {e}", flush=True)
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
    total_new = 0

    for cat_name, config in CATEGORIES.items():
        print(f"\n=== {cat_name} ===", flush=True)
        all_results = []
        offset = 0

        while True:
            print(f"  offset={offset}...", flush=True)
            bindings = sparql_fetch(config['query'], offset)
            if not bindings:
                break
            all_results.extend(bindings)
            print(f"    Got {len(bindings)}, total: {len(all_results)}", flush=True)
            if len(bindings) < 500:
                break
            offset += 500
            time.sleep(10)

        with open(f'data/wikidata/expand_{cat_name}.json', 'w', encoding='utf-8') as f:
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

            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, wikidata_id, lat, lon, source)
                VALUES (?, ?, ?, ?, ?, ?, 'wikidata_expand_phase8')
            """, (label, label_en, config['entity_type'], wikidata_id, lat, lon))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]

            if wikidata_id:
                existing_qids.add(wikidata_id)
            existing_labels.add(label)

            # Auto-tag
            if 'theme' in config:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'wd_expand', 0.8)", (eid, config['theme']))
            if 'medium' in config:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'wd_expand', 0.7)", (eid, config['medium']))
            if 'experience' in config:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', ?, 'wd_expand', 0.7)", (eid, config['experience']))

            if lat and lon:
                geo = coord_to_geo(lat, lon)
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', ?, 'coord', 0.9)", (eid, geo))

            # Era from date fields
            for date_field in ['inception', 'date', 'birth']:
                date_val = rec.get(date_field, {}).get('value', '')
                if date_val and len(date_val) >= 4:
                    try:
                        year = int(date_val[:4])
                        era = year_to_era(year)
                        db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'wd_date', 0.8)", (eid, era))
                        break
                    except ValueError:
                        pass

            cat_new += 1

        db.commit()
        total_new += cat_new
        print(f"  New: {cat_new:,}", flush=True)

    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    expand = db.execute("SELECT COUNT(*) FROM entities WHERE source='wikidata_expand_phase8'").fetchone()[0]
    print(f"\n=== Wikidata Expansion Complete ===", flush=True)
    print(f"New entities: {total_new:,}", flush=True)
    print(f"Expansion entities: {expand:,}", flush=True)
    print(f"Total entities: {total:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
