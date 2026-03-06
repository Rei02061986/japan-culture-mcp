"""
Phase 7 A3: Bulk fetch Japanese cultural persons from Wikidata.
Target: 3,000+ person entities.
Uses SERVICE wikibase:label for reliable Japanese labels.
"""
import requests
import json
import time
import sqlite3
import os

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.3 (contact@example.com)'
}

PERSON_CATEGORIES = {
    'mangaka': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q191633 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'anime_director': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q28389479 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'voice_actor': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q622807 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'game_designer': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q210167 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'film_director': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q2526255 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'novelist': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q36180 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'composer': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q36834 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'potter': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q1209498 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'living_national_treasure': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P166 wd:Q372986 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'ukiyoe_artist': {
        'query': """SELECT ?item ?itemLabel ?birth ?death WHERE {{
  ?item wdt:P106 wd:Q1028181 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  OPTIONAL {{ ?item wdt:P570 ?death }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'architect': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q42973 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
    'photographer': {
        'query': """SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q33231 ; wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 500 OFFSET {offset}""",
    },
}

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
                print(f"    HTTP {resp.status_code}", flush=True)
                time.sleep(30)
        except Exception as e:
            print(f"    ERROR: {e}", flush=True)
            time.sleep(30)
    return []

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

def main():
    db = sqlite3.connect(DB_PATH)

    # Load existing entity labels for dedup
    existing = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing.add(row[0])

    print(f"Existing entities: {len(existing):,}", flush=True)

    total_new = 0
    os.makedirs('data/wikidata', exist_ok=True)

    # Category -> theme mapping for auto-tagging
    CATEGORY_THEME = {
        'mangaka': 'craft_mastery',
        'anime_director': 'craft_mastery',
        'voice_actor': 'performing_arts',
        'game_designer': 'craft_mastery',
        'film_director': 'craft_mastery',
        'novelist': 'craft_mastery',
        'composer': 'musical_arts',
        'potter': 'traditional_craft',
        'living_national_treasure': 'traditional_craft',
        'ukiyoe_artist': 'ukiyoe_craft',
        'architect': 'craft_mastery',
        'photographer': 'visual_arts',
    }

    CATEGORY_MEDIUM = {
        'mangaka': 'manga',
        'anime_director': 'anime',
        'voice_actor': 'anime',
        'game_designer': 'game',
        'film_director': 'film',
        'novelist': 'literature',
        'composer': 'music',
        'potter': 'ceramic',
        'living_national_treasure': 'traditional_craft',
        'ukiyoe_artist': 'ukiyoe',
        'architect': 'architecture',
        'photographer': 'photography',
    }

    for cat_name, config in PERSON_CATEGORIES.items():
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

        # Save raw data
        with open(f'data/wikidata/persons_{cat_name}.json', 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False)

        # Insert into DB
        cat_new = 0
        theme = CATEGORY_THEME.get(cat_name, 'craft_mastery')
        medium = CATEGORY_MEDIUM.get(cat_name, 'unknown')

        for rec in all_results:
            label = rec.get('itemLabel', {}).get('value', '')
            if not label or label in existing:
                continue
            # Skip Q-ID labels (unresolved)
            if label.startswith('Q') and label[1:].isdigit():
                continue

            wikidata_uri = rec.get('item', {}).get('value', '')
            wikidata_id = wikidata_uri.split('/')[-1] if wikidata_uri else None

            db.execute("""
                INSERT INTO entities (label_ja, entity_type, wikidata_id, source)
                VALUES (?, 'person', ?, 'wikidata_persons')
            """, (label, wikidata_id))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing.add(label)

            # Auto-tag
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'person_category', 0.8)", (eid, theme))
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'person_category', 0.8)", (eid, medium))
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'intellectual', 'person_category', 0.7)", (eid,))

            # Era from birth year
            birth = rec.get('birth', {}).get('value', '')
            if birth and len(birth) >= 4:
                try:
                    year = int(birth[:4])
                    era = year_to_era(year)
                    db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'birth_year', 0.9)", (eid, era))
                except ValueError:
                    pass

            cat_new += 1

        db.commit()
        total_new += cat_new
        print(f"  New persons: {cat_new}", flush=True)

    # Final stats
    person_count = db.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"\n=== Person Expansion Complete ===", flush=True)
    print(f"New persons added: {total_new:,}", flush=True)
    print(f"Total persons: {person_count:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
