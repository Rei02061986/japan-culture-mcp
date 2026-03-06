"""
Phase 9 Stream C: Additional Wikidata entity expansion.
Target: 50,000+ new entities from categories not yet covered.
Shrines, temples, historical figures, cultural properties, performing arts, etc.
"""
import requests
import time
import sqlite3
import json
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.4 (contact@example.com)'
}

CATEGORIES = {
    'shrines': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q845945 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'place',
        'tags': [('theme', 'shrine_temple', 0.9), ('theme', 'sacred_profane', 0.7),
                 ('experience', 'reflective', 0.7)],
    },
    'temples': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q160742 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'place',
        'tags': [('theme', 'shrine_temple', 0.9), ('theme', 'sacred_profane', 0.8),
                 ('experience', 'reflective', 0.8)],
    },
    'castles': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q1549591 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'place',
        'tags': [('theme', 'samurai', 0.8), ('theme', 'power_rebellion', 0.6),
                 ('medium', 'architecture', 0.9), ('experience', 'aesthetic', 0.7)],
    },
    'jp_musicians': {
        'query': """SELECT ?item ?itemLabel ?birthDate WHERE {{
  ?item wdt:P106 wd:Q639669 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birthDate }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('medium', 'music', 0.9), ('theme', 'music_performance', 0.8),
                 ('experience', 'aesthetic', 0.7)],
    },
    'jp_architects': {
        'query': """SELECT ?item ?itemLabel ?birthDate WHERE {{
  ?item wdt:P106 wd:Q42973 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birthDate }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('medium', 'architecture', 0.9), ('theme', 'craft_mastery', 0.7),
                 ('experience', 'aesthetic', 0.8)],
    },
    'jp_sculptors': {
        'query': """SELECT ?item ?itemLabel ?birthDate WHERE {{
  ?item wdt:P106 wd:Q1281618 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birthDate }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('medium', 'sculpture', 0.9), ('theme', 'craft_mastery', 0.8),
                 ('experience', 'aesthetic', 0.8)],
    },
    'jp_athletes': {
        'query': """SELECT ?item ?itemLabel ?birthDate ?sportLabel WHERE {{
  ?item wdt:P106/wdt:P279* wd:Q2066131 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P641 ?sport }}
  OPTIONAL {{ ?item wdt:P569 ?birthDate }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('theme', 'sports', 0.9), ('experience', 'physical', 0.8)],
    },
    'cultural_properties': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P1435 ?designation .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('theme', 'community_tradition', 0.7), ('experience', 'aesthetic', 0.7)],
    },
    'jp_gardens': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q1107656 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.9), ('theme', 'seasonal_beauty', 0.7),
                 ('medium', 'architecture', 0.6), ('experience', 'aesthetic', 0.9)],
    },
    'jp_festivals': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q132241 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'event',
        'tags': [('theme', 'matsuri', 0.9), ('theme', 'community_tradition', 0.8),
                 ('experience', 'social', 0.8), ('experience', 'physical', 0.6)],
    },
    'jp_traditional_music': {
        'query': """SELECT ?item ?itemLabel WHERE {{
  {{ ?item wdt:P31/wdt:P279* wd:Q34379 . ?item wdt:P495 wd:Q17 . }}
  UNION
  {{ ?item wdt:P31/wdt:P279* wd:Q105543609 . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'concept',
        'tags': [('medium', 'music', 0.9), ('theme', 'music_performance', 0.8),
                 ('theme', 'community_tradition', 0.6), ('experience', 'aesthetic', 0.8)],
    },
    'jp_hot_springs': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q27185 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.7), ('experience', 'physical', 0.9),
                 ('experience', 'social', 0.6)],
    },
    'jp_mountains': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q8502 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.9), ('theme', 'sacred_profane', 0.5),
                 ('experience', 'physical', 0.8), ('experience', 'adventure', 0.7)],
    },
    'jp_islands': {
        'query': """SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q23442 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.8), ('experience', 'adventure', 0.7)],
    },
    'jp_writers': {
        'query': """SELECT ?item ?itemLabel ?birthDate WHERE {{
  ?item wdt:P106 wd:Q36180 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birthDate }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('medium', 'literature', 0.9), ('theme', 'literary_arts', 0.8),
                 ('experience', 'intellectual', 0.8)],
    },
    'jp_actors': {
        'query': """SELECT ?item ?itemLabel ?birthDate WHERE {{
  ?item wdt:P106 wd:Q33999 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birthDate }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('theme', 'kabuki_theater', 0.5), ('experience', 'aesthetic', 0.7)],
    },
    'jp_directors': {
        'query': """SELECT ?item ?itemLabel ?birthDate WHERE {{
  ?item wdt:P106 wd:Q2526255 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birthDate }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('medium', 'anime', 0.5), ('experience', 'aesthetic', 0.7)],
    },
    'jp_historical_figures': {
        'query': """SELECT ?item ?itemLabel ?birthDate ?deathDate WHERE {{
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P569 ?birthDate .
  ?item wdt:P570 ?deathDate .
  FILTER(YEAR(?deathDate) < 1900)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'person',
        'tags': [('theme', 'community_tradition', 0.6), ('experience', 'intellectual', 0.7)],
    },
}

# Prefecture coordinate mapping
PREF_COORDS = {
    'hokkaido': (43.06, 141.35), 'aomori': (40.82, 140.74), 'iwate': (39.70, 141.15),
    'miyagi': (38.27, 140.87), 'akita': (39.72, 140.10), 'yamagata': (38.24, 140.33),
    'fukushima': (37.75, 140.47), 'ibaraki': (36.34, 140.45), 'tochigi': (36.57, 139.88),
    'gunma': (36.39, 139.06), 'saitama': (35.86, 139.65), 'chiba': (35.61, 140.12),
    'tokyo': (35.68, 139.69), 'kanagawa': (35.45, 139.64), 'niigata': (37.90, 139.02),
    'toyama': (36.70, 137.21), 'ishikawa': (36.59, 136.63), 'fukui': (36.07, 136.22),
    'yamanashi': (35.66, 138.57), 'nagano': (36.65, 138.18), 'gifu': (35.39, 136.72),
    'shizuoka': (34.98, 138.38), 'aichi': (35.18, 136.91), 'mie': (34.73, 136.51),
    'shiga': (35.00, 135.87), 'kyoto': (35.01, 135.77), 'osaka': (34.69, 135.50),
    'hyogo': (34.69, 135.18), 'nara': (34.69, 135.83), 'wakayama': (34.23, 135.17),
    'tottori': (35.50, 134.24), 'shimane': (35.47, 133.05), 'okayama': (34.66, 133.93),
    'hiroshima': (34.40, 132.46), 'yamaguchi': (34.19, 131.47), 'tokushima': (34.07, 134.56),
    'kagawa': (34.34, 134.04), 'ehime': (33.84, 132.77), 'kochi': (33.56, 133.53),
    'fukuoka': (33.59, 130.40), 'saga': (33.25, 130.30), 'nagasaki': (32.74, 129.87),
    'kumamoto': (32.79, 130.74), 'oita': (33.24, 131.61), 'miyazaki': (31.91, 131.42),
    'kagoshima': (31.56, 130.56), 'okinawa': (26.34, 127.80),
}

GEO_REGIONS = {
    'hokkaido': 'hokkaido', 'aomori': 'tohoku', 'iwate': 'tohoku', 'miyagi': 'tohoku',
    'akita': 'tohoku', 'yamagata': 'tohoku', 'fukushima': 'tohoku',
    'ibaraki': 'kanto', 'tochigi': 'kanto', 'gunma': 'kanto', 'saitama': 'kanto',
    'chiba': 'kanto', 'tokyo': 'kanto', 'kanagawa': 'kanto',
    'niigata': 'chubu', 'toyama': 'chubu', 'ishikawa': 'chubu', 'fukui': 'chubu',
    'yamanashi': 'chubu', 'nagano': 'chubu', 'gifu': 'chubu', 'shizuoka': 'chubu', 'aichi': 'chubu',
    'mie': 'kinki', 'shiga': 'kinki', 'kyoto': 'kinki', 'osaka': 'kinki',
    'hyogo': 'kinki', 'nara': 'kinki', 'wakayama': 'kinki',
    'tottori': 'chugoku', 'shimane': 'chugoku', 'okayama': 'chugoku',
    'hiroshima': 'chugoku', 'yamaguchi': 'chugoku',
    'tokushima': 'shikoku', 'kagawa': 'shikoku', 'ehime': 'shikoku', 'kochi': 'shikoku',
    'fukuoka': 'kyushu', 'saga': 'kyushu', 'nagasaki': 'kyushu', 'kumamoto': 'kyushu',
    'oita': 'kyushu', 'miyazaki': 'kyushu', 'kagoshima': 'kyushu', 'okinawa': 'kyushu',
}


def sparql_fetch(query_template, offset=0):
    q = query_template.format(offset=offset)
    for attempt in range(3):
        try:
            resp = requests.get(WIKIDATA_SPARQL, params={'query': q}, headers=HEADERS, timeout=120)
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


def coord_to_geo(lat, lon):
    """Map coordinates to geography value_code."""
    # Rough mapping
    if lat > 41: return 'hokkaido'
    if lat > 38: return 'tohoku'
    if 34.5 < lat < 37 and 138.5 < lon < 141: return 'kanto'
    if 34.5 < lat < 38 and 135.5 < lon < 138.5: return 'chubu'
    if 33.5 < lat < 35.5 and 134.5 < lon < 136.5: return 'kinki'
    if 33.5 < lat < 35.5 and 131 < lon < 134.5: return 'chugoku'
    if 33 < lat < 34.5 and 132 < lon < 135: return 'shikoku'
    if lat < 33.5 and lon < 132: return 'kyushu'
    if lat < 27: return 'kyushu'  # Okinawa -> kyushu region
    return None


def main():
    db = sqlite3.connect(DB_PATH)

    # Build existing label set for dedup
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])

    existing_qids = set()
    for row in db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        existing_qids.add(row[0])

    print(f"Existing entities: {len(existing_labels):,} labels, {len(existing_qids):,} QIDs", flush=True)

    total_new = 0

    for cat_name, config in CATEGORIES.items():
        print(f"\n=== {cat_name} ===", flush=True)
        all_bindings = []
        offset = 0

        while True:
            bindings = sparql_fetch(config['query'], offset)
            if not bindings:
                break
            all_bindings.extend(bindings)
            print(f"  offset={offset}, got={len(bindings)}, total={len(all_bindings)}", flush=True)
            if len(bindings) < 2000:
                break
            offset += 2000
            time.sleep(10)

        cat_new = 0
        for b in all_bindings:
            uri = b.get('item', {}).get('value', '')
            label = b.get('itemLabel', {}).get('value', '')

            if not label or label.startswith('Q'):
                continue

            qid = uri.split('/')[-1] if 'wikidata.org' in uri else None

            # Dedup by QID or label
            if qid and qid in existing_qids:
                continue
            if label in existing_labels:
                continue

            # Parse coordinates
            lat, lon = None, None
            coord_str = b.get('coord', {}).get('value', '')
            if coord_str:
                m = re.search(r'Point\(([^ ]+) ([^ ]+)\)', coord_str)
                if m:
                    lon, lat = float(m.group(1)), float(m.group(2))

            # Parse birth date for era
            birth_year = None
            for date_field in ('birthDate', 'deathDate'):
                date_str = b.get(date_field, {}).get('value', '')
                if date_str:
                    m = re.match(r'(\d{4})', date_str)
                    if m:
                        birth_year = int(m.group(1))
                        break

            # Insert entity
            cur = db.execute("""
                INSERT INTO entities (label_ja, entity_type, source, wikidata_id, lat, lon)
                VALUES (?, ?, 'wd_phase9', ?, ?, ?)
            """, (label, config['entity_type'], qid, lat, lon))
            eid = cur.lastrowid

            # Add tags
            for axis, value_code, confidence in config['tags']:
                db.execute("""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, ?, ?, 'wd_phase9', ?)
                """, (eid, axis, value_code, confidence))

            # Era tag from date
            if birth_year:
                era = year_to_era(birth_year)
                db.execute("""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'era', ?, 'wd_phase9_date', 0.9)
                """, (eid, era))

            # Geography tag from coords
            if lat and lon:
                geo = coord_to_geo(lat, lon)
                if geo:
                    db.execute("""
                        INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'geography', ?, 'wd_phase9_coord', 0.8)
                    """, (eid, geo))

            existing_labels.add(label)
            if qid:
                existing_qids.add(qid)
            cat_new += 1

        db.commit()
        total_new += cat_new
        print(f"  New entities: {cat_new:,} (running total: {total_new:,})", flush=True)

    # Add English labels via pykakasi for new entities
    print("\n=== Adding English labels ===", flush=True)
    try:
        import pykakasi
        kks = pykakasi.kakasi()
        missing = db.execute("""
            SELECT id, label_ja FROM entities
            WHERE source = 'wd_phase9' AND label_en IS NULL AND label_ja IS NOT NULL
        """).fetchall()

        updated = 0
        for eid, label_ja in missing:
            try:
                result = kks.convert(label_ja)
                romaji = ' '.join(item['hepburn'] for item in result)
                if romaji and romaji != label_ja:
                    db.execute("UPDATE entities SET label_en = ? WHERE id = ?", (romaji, eid))
                    updated += 1
            except:
                pass

        db.commit()
        print(f"  Romanized: {updated:,}", flush=True)
    except ImportError:
        print("  pykakasi not available, skipping romanization", flush=True)

    # Final stats
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n=== Stream C Wikidata Expansion Complete ===", flush=True)
    print(f"New entities: {total_new:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
