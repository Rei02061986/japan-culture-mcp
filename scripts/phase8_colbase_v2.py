"""
Phase 8A-1 v2: ColBase/museum items via JapanSearch SPARQL keyword search.
Uses artifact-specific keywords to find museum collection items.
Target: >= 5,000 entities.
"""
import requests
import json
import time
import sqlite3
import os

DB_PATH = "ontology/culture_ontology.db"
JPS_SPARQL = "https://jpsearch.go.jp/rdf/sparql"

# Museum/artifact-specific keywords
KEYWORDS = [
    # National treasures & cultural properties
    "国宝", "重要文化財",
    # Painting types
    "屏風絵", "掛軸", "襖絵", "絵巻物", "水墨画", "大和絵",
    # Sculpture
    "仏像", "観音", "阿弥陀", "不動明王", "菩薩", "如来",
    # Crafts
    "蒔絵", "漆器", "印籠", "根付", "七宝",
    # Weapons & armor
    "太刀", "短刀", "脇差", "甲冑", "鍔",
    # Ceramics
    "茶碗", "花器", "壺", "皿", "有田焼", "九谷焼", "備前焼", "萩焼", "楽焼",
    "志野焼", "織部焼",
    # Textiles
    "友禅", "西陣織", "絞り染",
    # Calligraphy & books
    "古筆", "写経", "巻子本",
    # Architecture models/parts
    "厨子", "須弥壇",
    # Musical instruments
    "琵琶", "琴", "三味線", "尺八", "太鼓",
    # Masks & theater
    "能面", "狂言面",
    # Other traditional items
    "香炉", "花瓶", "硯", "刀装具",
]

QUERY_TEMPLATE = """
SELECT ?item ?label ?type ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  FILTER(CONTAINS(?label, "{keyword}"))
  OPTIONAL {{ ?item schema:additionalType ?type . }}
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
"""

# Theme mapping for keywords
KEYWORD_THEMES = {
    '国宝': 'craft_mastery', '重要文化財': 'craft_mastery',
    '屏風': 'visual_arts', '掛軸': 'visual_arts', '襖絵': 'visual_arts',
    '絵巻': 'visual_arts', '水墨': 'visual_arts', '大和絵': 'visual_arts',
    '仏像': 'sacred_profane', '観音': 'sacred_profane', '阿弥陀': 'sacred_profane',
    '不動明王': 'sacred_profane', '菩薩': 'sacred_profane', '如来': 'sacred_profane',
    '蒔絵': 'traditional_craft', '漆器': 'traditional_craft', '印籠': 'traditional_craft',
    '根付': 'traditional_craft', '七宝': 'traditional_craft',
    '太刀': 'samurai', '短刀': 'samurai', '脇差': 'samurai', '甲冑': 'samurai', '鍔': 'samurai',
    '茶碗': 'wabi_sabi', '花器': 'traditional_craft', '壺': 'traditional_craft',
    '皿': 'traditional_craft',
    '有田焼': 'traditional_craft', '九谷焼': 'traditional_craft', '備前焼': 'traditional_craft',
    '萩焼': 'traditional_craft', '楽焼': 'traditional_craft', '志野焼': 'traditional_craft',
    '織部焼': 'traditional_craft',
    '友禅': 'traditional_craft', '西陣織': 'traditional_craft', '絞り染': 'traditional_craft',
    '古筆': 'calligraphy', '写経': 'sacred_profane', '巻子本': 'literary_arts',
    '厨子': 'sacred_profane', '須弥壇': 'sacred_profane',
    '琵琶': 'musical_arts', '琴': 'musical_arts', '三味線': 'musical_arts',
    '尺八': 'musical_arts', '太鼓': 'musical_arts',
    '能面': 'performing_arts', '狂言面': 'performing_arts',
    '香炉': 'wabi_sabi', '花瓶': 'traditional_craft', '硯': 'calligraphy',
    '刀装具': 'samurai',
}


def fetch_keyword(keyword, max_results=2000):
    all_results = []
    offset = 0

    while offset < max_results:
        query = QUERY_TEMPLATE.format(keyword=keyword, offset=offset)

        for attempt in range(3):
            try:
                resp = requests.get(
                    JPS_SPARQL,
                    params={'query': query},
                    headers={'Accept': 'application/sparql-results+json'},
                    timeout=120
                )
                if resp.status_code == 200:
                    bindings = resp.json().get('results', {}).get('bindings', [])
                    if not bindings:
                        return all_results
                    all_results.extend(bindings)
                    print(f"    [{keyword}] offset={offset}, got {len(bindings)}, total: {len(all_results)}", flush=True)
                    if len(bindings) < 500:
                        return all_results
                    offset += 500
                    time.sleep(5)
                    break
                else:
                    print(f"    HTTP {resp.status_code}, attempt {attempt+1}", flush=True)
                    time.sleep(30)
            except Exception as e:
                print(f"    ERROR: {e}", flush=True)
                time.sleep(30)
        else:
            return all_results

    return all_results


def main():
    db = sqlite3.connect(DB_PATH)
    os.makedirs('data/colbase', exist_ok=True)

    existing = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing.add(row[0])
    print(f"Existing entities: {len(existing):,}", flush=True)

    total_new = 0

    for keyword in KEYWORDS:
        print(f"\n=== {keyword} ===", flush=True)
        results = fetch_keyword(keyword)
        print(f"  Raw results: {len(results):,}", flush=True)

        # Determine theme for this keyword
        theme = 'craft_mastery'
        for kw, t in KEYWORD_THEMES.items():
            if kw in keyword:
                theme = t
                break

        new_count = 0
        for rec in results:
            label = rec.get('label', {}).get('value', '').strip()
            if not label or label in existing or len(label) < 2:
                continue

            db.execute("""
                INSERT INTO entities (label_ja, entity_type, source)
                VALUES (?, 'artifact', 'colbase_phase8')
            """, (label,))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing.add(label)

            # Tag
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'jps_keyword', 0.7)", (eid, theme))
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'aesthetic', 'artifact_default', 0.7)", (eid,))

            new_count += 1

        db.commit()
        total_new += new_count
        print(f"  New entities: {new_count:,}", flush=True)

        if total_new >= 8000:
            print(f"  Reached {total_new:,} entities, stopping", flush=True)
            break

    colbase_count = db.execute("SELECT COUNT(*) FROM entities WHERE source='colbase_phase8'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"\n=== ColBase v2 Import Complete ===", flush=True)
    print(f"New this run: {total_new:,}", flush=True)
    print(f"Total ColBase: {colbase_count:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
