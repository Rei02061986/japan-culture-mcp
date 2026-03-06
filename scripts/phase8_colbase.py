"""
Phase 8A-1: ColBase items via JapanSearch SPARQL.
Fetch museum collection items from national museums.
Target: >= 5,000 entities.
"""
import requests
import json
import time
import sqlite3
import os

DB_PATH = "ontology/culture_ontology.db"
JPS_SPARQL = "https://jpsearch.go.jp/rdf/sparql"

# Query for ColBase-sourced items (national museums)
# ColBase providers include: 東京国立博物館, 京都国立博物館, 奈良国立博物館, 九州国立博物館
PROVIDERS = [
    "東京国立博物館",
    "京都国立博物館",
    "奈良国立博物館",
    "九州国立博物館",
    "奈良文化財研究所",
]

QUERY_BY_PROVIDER = """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  ?item schema:provider ?prov .
  ?prov rdfs:label ?provLabel .
  FILTER(CONTAINS(?provLabel, "{provider}"))
  OPTIONAL {{ ?item schema:additionalType ?type . }}
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
"""

# Also query by cultural property categories directly
CATEGORY_QUERIES = {
    "national_treasure": """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  FILTER(CONTAINS(STR(?type), "国宝") || CONTAINS(?label, "国宝"))
  OPTIONAL {{ ?item schema:additionalType ?type . }}
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "important_cultural_property": """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  ?item schema:additionalType ?type .
  FILTER(CONTAINS(STR(?type), "重要文化財"))
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "painting_art": """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  ?item schema:additionalType ?type .
  FILTER(CONTAINS(STR(?type), "絵画") || CONTAINS(STR(?type), "日本画") || CONTAINS(STR(?type), "洋画"))
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "sculpture": """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  ?item schema:additionalType ?type .
  FILTER(CONTAINS(STR(?type), "彫刻") || CONTAINS(STR(?type), "仏像"))
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "craft_ceramics": """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  ?item schema:additionalType ?type .
  FILTER(CONTAINS(STR(?type), "工芸") || CONTAINS(STR(?type), "陶磁") || CONTAINS(STR(?type), "漆工"))
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "calligraphy": """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  ?item schema:additionalType ?type .
  FILTER(CONTAINS(STR(?type), "書跡") || CONTAINS(STR(?type), "書"))
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
""",
}


# Theme mapping from type keywords
TYPE_TO_THEME = {
    '絵画': 'visual_arts', '日本画': 'visual_arts', '洋画': 'visual_arts',
    '彫刻': 'visual_arts', '仏像': 'sacred_profane',
    '工芸': 'traditional_craft', '陶磁': 'traditional_craft', '漆工': 'traditional_craft',
    '染織': 'traditional_craft', '金工': 'traditional_craft',
    '書跡': 'calligraphy', '書': 'calligraphy',
    '浮世絵': 'ukiyoe_craft', '版画': 'ukiyoe_craft',
    '考古': 'historical_event', '歴史': 'historical_event',
    '刀剣': 'samurai', '甲冑': 'samurai', '武具': 'samurai',
    '能': 'performing_arts', '歌舞伎': 'performing_arts',
    '茶道': 'wabi_sabi', '茶': 'wabi_sabi',
    '建築': 'architecture',
}

# Medium mapping from type keywords
TYPE_TO_MEDIUM = {
    '絵画': 'painting', '日本画': 'painting', '洋画': 'painting',
    '彫刻': 'sculpture', '仏像': 'sculpture',
    '工芸': 'craft', '陶磁': 'craft', '漆工': 'craft',
    '染織': 'craft', '金工': 'craft',
    '書跡': 'literature', '書': 'literature',
    '浮世絵': 'ukiyoe', '版画': 'ukiyoe',
    '刀剣': 'craft', '甲冑': 'craft',
}

# Geography mapping from provider
PROVIDER_TO_GEO = {
    '東京': 'kanto',
    '京都': 'kinki',
    '奈良': 'kinki',
    '九州': 'kyushu',
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

def guess_era_from_label(label):
    """Try to guess era from label text."""
    era_keywords = {
        '縄文': 'ancient', '弥生': 'ancient', '古墳': 'ancient',
        '奈良時代': 'ancient', '平安': 'ancient', '飛鳥': 'ancient',
        '鎌倉': 'medieval', '室町': 'medieval', '南北朝': 'medieval',
        '戦国': 'medieval', '安土桃山': 'medieval',
        '江戸': 'edo_early', '元禄': 'edo_early',
        '幕末': 'edo_late', '文化文政': 'edo_late',
        '明治': 'meiji_taisho', '大正': 'meiji_taisho',
        '昭和': 'showa_postwar',
    }
    for keyword, era in era_keywords.items():
        if keyword in label:
            return era
    return None


def sparql_fetch(query, offset=0):
    q = query.format(offset=offset)
    for attempt in range(3):
        try:
            resp = requests.get(
                JPS_SPARQL,
                params={'query': q},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=120
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


def fetch_paginated(query_template, label, max_results=5000):
    all_results = []
    offset = 0
    while offset < max_results:
        print(f"  [{label}] offset={offset}...", flush=True)
        bindings = sparql_fetch(query_template, offset)
        if not bindings:
            break
        all_results.extend(bindings)
        print(f"    Got {len(bindings)}, total: {len(all_results)}", flush=True)
        if len(bindings) < 500:
            break
        offset += 500
        time.sleep(5)
    return all_results


def infer_tags(label, type_str, provider_str):
    """Infer theme, medium, era, geography tags from metadata."""
    tags = []

    # Theme and medium from type
    type_text = type_str or ''
    label_text = label or ''
    combined = type_text + ' ' + label_text

    theme_found = False
    for keyword, theme in TYPE_TO_THEME.items():
        if keyword in combined:
            tags.append(('theme', theme, 'colbase_type', 0.7))
            theme_found = True
            break

    if not theme_found:
        tags.append(('theme', 'craft_mastery', 'colbase_default', 0.5))

    for keyword, medium in TYPE_TO_MEDIUM.items():
        if keyword in combined:
            tags.append(('medium', medium, 'colbase_type', 0.7))
            break

    # Geography from provider
    prov = provider_str or ''
    for keyword, geo in PROVIDER_TO_GEO.items():
        if keyword in prov:
            tags.append(('geography', geo, 'colbase_provider', 0.6))
            break

    # Era from label
    era = guess_era_from_label(combined)
    if era:
        tags.append(('era', era, 'colbase_label', 0.6))

    # Experience: aesthetic for art items
    tags.append(('experience', 'aesthetic', 'colbase_default', 0.7))

    return tags


def main():
    db = sqlite3.connect(DB_PATH)
    os.makedirs('data/colbase', exist_ok=True)

    # Load existing labels for dedup
    existing = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing.add(row[0])
    print(f"Existing entities: {len(existing):,}", flush=True)

    all_raw = []
    total_new = 0

    # 1) Fetch by provider
    for provider in PROVIDERS:
        print(f"\n=== Provider: {provider} ===", flush=True)
        query = QUERY_BY_PROVIDER.replace("{provider}", provider)
        results = fetch_paginated(query, provider, max_results=10000)

        with open(f'data/colbase/provider_{provider}.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)

        new_count = 0
        for rec in results:
            label = rec.get('label', {}).get('value', '').strip()
            if not label or label in existing or len(label) < 2:
                continue

            type_str = rec.get('type', {}).get('value', '')
            thumb = rec.get('thumbnail', {}).get('value', '')

            db.execute("""
                INSERT INTO entities (label_ja, entity_type, source)
                VALUES (?, 'artifact', 'colbase_phase8')
            """, (label,))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing.add(label)

            # Auto-tag
            for axis, code, src, conf in infer_tags(label, type_str, provider):
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, ?, ?, ?, ?)
                """, (eid, axis, code, src, conf))

            new_count += 1

        db.commit()
        total_new += new_count
        print(f"  New from {provider}: {new_count:,}", flush=True)

    # 2) Fetch by category
    for cat_name, query_template in CATEGORY_QUERIES.items():
        print(f"\n=== Category: {cat_name} ===", flush=True)
        results = fetch_paginated(query_template, cat_name, max_results=10000)

        with open(f'data/colbase/category_{cat_name}.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)

        new_count = 0
        for rec in results:
            label = rec.get('label', {}).get('value', '').strip()
            if not label or label in existing or len(label) < 2:
                continue

            type_str = rec.get('type', {}).get('value', '')
            thumb = rec.get('thumbnail', {}).get('value', '')

            db.execute("""
                INSERT INTO entities (label_ja, entity_type, source)
                VALUES (?, 'artifact', 'colbase_phase8')
            """, (label,))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing.add(label)

            for axis, code, src, conf in infer_tags(label, type_str, ''):
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, ?, ?, ?, ?)
                """, (eid, axis, code, src, conf))

            new_count += 1

        db.commit()
        total_new += new_count
        print(f"  New from {cat_name}: {new_count:,}", flush=True)

    # Final stats
    colbase_count = db.execute("SELECT COUNT(*) FROM entities WHERE source='colbase_phase8'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"\n=== ColBase Import Complete ===", flush=True)
    print(f"Total new entities: {total_new:,}", flush=True)
    print(f"ColBase entities in DB: {colbase_count:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
