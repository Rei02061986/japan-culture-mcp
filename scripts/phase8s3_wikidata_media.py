"""
Phase 8 Stage 3b: Wikidata media expansion.
Fetch Japanese video games, films, manga series from Wikidata.
Target: ~20,000 new entities to reach 250K total.
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
    "video_games": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?date WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q7889 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "work",
        "theme": "game_culture",
        "medium": "game",
        "experience": "aesthetic",
    },
    "films": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?date WHERE {{
  ?item wdt:P31 wd:Q11424 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "work",
        "theme": "visual_arts",
        "medium": "film",
        "experience": "aesthetic",
    },
    "manga_series": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?date WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q21198342 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "work",
        "theme": "literary_arts",
        "medium": "manga",
        "experience": "aesthetic",
    },
    "jp_novelists": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?birth WHERE {{
  ?item wdt:P106 wd:Q36180 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "person",
        "theme": "literary_arts",
        "medium": "literature",
        "experience": "intellectual",
    },
    "jp_painters": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?birth WHERE {{
  ?item wdt:P106 wd:Q1028181 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "person",
        "theme": "visual_arts",
        "medium": "painting",
        "experience": "aesthetic",
    },
    "jp_voice_actors": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?birth WHERE {{
  ?item wdt:P106 wd:Q622807 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "person",
        "theme": "visual_arts",
        "medium": "anime",
        "experience": "aesthetic",
    },
    "jp_composers": {
        "query": """SELECT ?item ?itemLabel ?itemDescription ?birth WHERE {{
  ?item wdt:P106 wd:Q36834 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
        "entity_type": "person",
        "theme": "musical_arts",
        "medium": "music",
        "experience": "aesthetic",
    },
}

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

    existing_qids = set()
    for row in db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        existing_qids.add(row[0])
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])

    print(f"Existing QIDs: {len(existing_qids):,}", flush=True)
    print(f"Existing labels: {len(existing_labels):,}", flush=True)
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

        with open(f'data/wikidata/media_{cat_name}.json', 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False)

        cat_new = 0
        for rec in all_results:
            label = rec.get('itemLabel', {}).get('value', '')
            if not label or label.startswith('Q') or len(label) < 2:
                continue

            wikidata_uri = rec.get('item', {}).get('value', '')
            wikidata_id = wikidata_uri.split('/')[-1] if wikidata_uri else None

            if wikidata_id and wikidata_id in existing_qids:
                continue
            if label in existing_labels:
                continue

            label_en = rec.get('itemDescription', {}).get('value', '')
            if label_en and (len(label_en) > 80 or ',' in label_en):
                label_en = None

            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, wikidata_id, source)
                VALUES (?, ?, ?, ?, 'wikidata_media_phase8')
            """, (label, label_en, config['entity_type'], wikidata_id))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]

            if wikidata_id:
                existing_qids.add(wikidata_id)
            existing_labels.add(label)

            # Auto-tag
            if 'theme' in config:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'wd_media', 0.8)", (eid, config['theme']))
            if 'medium' in config:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'wd_media', 0.7)", (eid, config['medium']))
            if 'experience' in config:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', ?, 'wd_media', 0.7)", (eid, config['experience']))

            # Era from date
            for date_field in ['date', 'birth']:
                date_val = rec.get(date_field, {}).get('value', '')
                if date_val and len(date_val) >= 4:
                    try:
                        year = int(date_val[:4])
                        if 500 < year < 2030:
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
    media = db.execute("SELECT COUNT(*) FROM entities WHERE source='wikidata_media_phase8'").fetchone()[0]
    print(f"\n=== Wikidata Media Expansion Complete ===", flush=True)
    print(f"New entities: {total_new:,}", flush=True)
    print(f"Media entities: {media:,}", flush=True)
    print(f"Total entities: {total:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
