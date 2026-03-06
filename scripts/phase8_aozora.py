"""
Phase 8A-3: Aozora Bunko (青空文庫) works import.
Fetch work list CSV from GitHub, insert works + authors.
Target: >= 10,000 work entities.
"""
import requests
import csv
import io
import json
import time
import sqlite3
import os

DB_PATH = "ontology/culture_ontology.db"

# Aozora Bunko CSV from GitHub
# The CSV contains: 作品ID, 作品名, 姓, 名, 分類番号, ...
CSV_URL = "https://www.aozora.gr.jp/index_pages/list_person_all_extended_utf8.zip"
CSV_FALLBACK = "https://raw.githubusercontent.com/aozorabunko/aozorabunko/master/index_pages/list_person_all_extended_utf8.csv"

# Alternative: use the simpler work list
WORK_LIST_URL = "https://www.aozora.gr.jp/index_pages/list_person_all_utf8.zip"

# NDC (日本十進分類法) top-level categories → theme mapping
NDC_TO_THEME = {
    '0': 'intellectual',      # 総記
    '1': 'sacred_profane',    # 哲学・宗教
    '2': 'historical_event',  # 歴史
    '3': 'community_tradition',  # 社会科学
    '4': 'nature_communion',  # 自然科学
    '5': 'craft_mastery',     # 技術
    '6': 'craft_mastery',     # 産業
    '7': 'performing_arts',   # 芸術
    '8': 'literary_arts',     # 言語
    '9': 'literary_arts',     # 文学
}

# Famous author name → specific theme mapping
AUTHOR_THEMES = {
    '芥川龍之介': ['death_rebirth', 'identity_self'],
    '太宰治': ['death_rebirth', 'identity_self'],
    '夏目漱石': ['identity_self', 'everyday_beauty'],
    '宮沢賢治': ['nature_communion', 'otherworld'],
    '樋口一葉': ['love_bond', 'everyday_beauty'],
    '泉鏡花': ['supernatural', 'love_bond'],
    '中島敦': ['identity_self', 'journey_boundary'],
    '坂口安吾': ['humor_satire', 'identity_self'],
    '梶井基次郎': ['everyday_beauty', 'death_rebirth'],
    '森鷗外': ['identity_self', 'historical_event'],
    '島崎藤村': ['identity_self', 'nature_communion'],
    '谷崎潤一郎': ['love_bond', 'everyday_beauty'],
    '川端康成': ['love_bond', 'seasonal_beauty'],
    '三島由紀夫': ['death_rebirth', 'identity_self'],
    '江戸川乱歩': ['identity_self', 'supernatural'],
    '夢野久作': ['supernatural', 'death_rebirth'],
    '小泉八雲': ['yokai', 'supernatural'],
    '中原中也': ['love_bond', 'death_rebirth'],
    '萩原朔太郎': ['identity_self', 'supernatural'],
    '与謝野晶子': ['love_bond', 'identity_self'],
    '正岡子規': ['nature_communion', 'seasonal_beauty'],
    '石川啄木': ['identity_self', 'everyday_beauty'],
    '国木田独歩': ['nature_communion', 'everyday_beauty'],
    '徳富蘆花': ['nature_communion', 'identity_self'],
    '幸田露伴': ['craft_mastery', 'identity_self'],
    '尾崎紅葉': ['love_bond', 'everyday_beauty'],
    '田山花袋': ['identity_self', 'journey_boundary'],
    '北原白秋': ['nature_communion', 'love_bond'],
    '吉川英治': ['samurai', 'historical_event'],
    '中里介山': ['samurai', 'journey_boundary'],
    '岡本綺堂': ['identity_self', 'historical_event'],
}

# Label keywords for theme inference
TITLE_KEYWORDS = {
    '恋': 'love_bond', '愛': 'love_bond',
    '死': 'death_rebirth', '幽霊': 'supernatural',
    '妖': 'yokai', '怪': 'yokai', '化け': 'yokai',
    '鬼': 'yokai', '幽': 'supernatural',
    '戦': 'war_conflict', '武': 'samurai', '剣': 'swordplay',
    '花': 'seasonal_beauty', '桜': 'seasonal_beauty', '雪': 'seasonal_beauty',
    '山': 'nature_communion', '海': 'nature_communion', '川': 'nature_communion',
    '旅': 'journey_boundary', '道': 'journey_boundary',
    '笑': 'humor_satire',
    '神': 'sacred_profane', '仏': 'sacred_profane',
    '夢': 'otherworld',
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


def fetch_csv():
    """Fetch Aozora Bunko work list CSV."""
    # Try the direct CSV first
    print("Fetching Aozora Bunko CSV...", flush=True)

    # Try ZIP first (more complete)
    try:
        resp = requests.get(CSV_URL, timeout=120)
        if resp.status_code == 200:
            import zipfile
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            for name in zf.namelist():
                if name.endswith('.csv'):
                    with zf.open(name) as f:
                        content = f.read().decode('utf-8')
                        return content
    except Exception as e:
        print(f"  ZIP failed: {e}", flush=True)

    # Fallback to raw CSV
    try:
        resp = requests.get(CSV_FALLBACK, timeout=120)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        print(f"  CSV fallback failed: {e}", flush=True)

    return None


def parse_csv(content):
    """Parse the Aozora Bunko CSV into structured records."""
    reader = csv.reader(io.StringIO(content))
    header = next(reader, None)

    if not header:
        return []

    # Find column indices
    # Typical columns: 作品ID, 作品名, 作品名読み, ソート用読み, 副題, 副題読み,
    #   原題, 初出, 分類番号, 文字遣い種別, 作品著作権フラグ,
    #   公開日, 最終更新日, 図書カードURL, 人物ID, 姓, 名, 姓読み, 名読み,
    #   姓ローマ字, 名ローマ字, 役割フラグ, 生年月日, 没年月日, ...
    col_map = {}
    for i, col in enumerate(header):
        col_map[col.strip()] = i

    print(f"  CSV columns: {list(col_map.keys())[:15]}...", flush=True)

    records = []
    for row in reader:
        if len(row) < 10:
            continue
        try:
            work_id = row[col_map.get('作品ID', 0)].strip() if '作品ID' in col_map else row[0].strip()
            title = row[col_map.get('作品名', 1)].strip() if '作品名' in col_map else row[1].strip()
            ndc = row[col_map.get('分類番号', 8)].strip() if '分類番号' in col_map else (row[8].strip() if len(row) > 8 else '')
            surname = row[col_map.get('姓', 15)].strip() if '姓' in col_map else (row[15].strip() if len(row) > 15 else '')
            firstname = row[col_map.get('名', 16)].strip() if '名' in col_map else (row[16].strip() if len(row) > 16 else '')
            birth = row[col_map.get('生年月日', 22)].strip() if '生年月日' in col_map else (row[22].strip() if len(row) > 22 else '')
            death = row[col_map.get('没年月日', 23)].strip() if '没年月日' in col_map else (row[23].strip() if len(row) > 23 else '')
        except (IndexError, KeyError):
            continue

        if not title:
            continue

        author = f"{surname}{firstname}".strip()
        records.append({
            'work_id': work_id,
            'title': title,
            'ndc': ndc,
            'author': author,
            'birth': birth,
            'death': death,
        })

    return records


def main():
    db = sqlite3.connect(DB_PATH)
    os.makedirs('data/aozora', exist_ok=True)

    # Load existing labels for dedup
    existing = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing.add(row[0])
    print(f"Existing entities: {len(existing):,}", flush=True)

    # Fetch CSV
    content = fetch_csv()
    if not content:
        print("ERROR: Could not fetch Aozora Bunko CSV", flush=True)
        db.close()
        return

    # Parse
    records = parse_csv(content)
    print(f"Parsed {len(records):,} records from CSV", flush=True)

    # Save raw
    with open('data/aozora/works_raw.json', 'w', encoding='utf-8') as f:
        json.dump(records[:100], f, ensure_ascii=False, indent=2)  # Sample

    # Deduplicate by title+author
    seen_works = set()
    seen_authors = set()
    new_works = 0
    new_authors = 0

    for rec in records:
        title = rec['title']
        author = rec['author']
        work_key = f"{title}_{author}"

        # Insert author as person entity
        if author and author not in existing and author not in seen_authors:
            seen_authors.add(author)
            db.execute("""
                INSERT INTO entities (label_ja, entity_type, source)
                VALUES (?, 'person', 'aozora_phase8')
            """, (author,))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing.add(author)

            # Tag author
            db.execute("""
                INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                VALUES (?, 'theme', 'literary_arts', 'aozora', 0.9)
            """, (eid,))
            db.execute("""
                INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                VALUES (?, 'medium', 'literature', 'aozora', 0.9)
            """, (eid,))
            db.execute("""
                INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                VALUES (?, 'experience', 'intellectual', 'aozora', 0.7)
            """, (eid,))

            # Author-specific themes
            if author in AUTHOR_THEMES:
                for theme in AUTHOR_THEMES[author]:
                    db.execute("""
                        INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'theme', ?, 'aozora_author', 0.8)
                    """, (eid, theme))

            # Era from birth year
            birth = rec.get('birth', '')
            if birth and len(birth) >= 4:
                try:
                    year = int(birth[:4])
                    era = year_to_era(year)
                    db.execute("""
                        INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'era', ?, 'birth_year', 0.9)
                    """, (eid, era))
                except ValueError:
                    pass

            new_authors += 1

        # Insert work
        if work_key not in seen_works and title not in existing:
            seen_works.add(work_key)
            db.execute("""
                INSERT INTO entities (label_ja, entity_type, source)
                VALUES (?, 'work', 'aozora_phase8')
            """, (title,))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing.add(title)

            # Default tags: literature
            db.execute("""
                INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                VALUES (?, 'medium', 'literature', 'aozora', 0.95)
            """, (eid,))
            db.execute("""
                INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                VALUES (?, 'experience', 'intellectual', 'aozora', 0.7)
            """, (eid,))

            # Theme from NDC
            ndc = rec.get('ndc', '')
            if ndc:
                ndc_top = ndc[0] if ndc else ''
                theme = NDC_TO_THEME.get(ndc_top, 'literary_arts')
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'theme', ?, 'ndc_classification', 0.7)
                """, (eid, theme))
            else:
                # Theme from title keywords
                theme_found = False
                for keyword, theme in TITLE_KEYWORDS.items():
                    if keyword in title:
                        db.execute("""
                            INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                            VALUES (?, 'theme', ?, 'title_keyword', 0.6)
                        """, (eid, theme))
                        theme_found = True
                        break
                if not theme_found:
                    db.execute("""
                        INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'theme', 'literary_arts', 'aozora_default', 0.5)
                    """, (eid,))

            # Author-specific themes apply to works too
            if author in AUTHOR_THEMES:
                for theme in AUTHOR_THEMES[author]:
                    db.execute("""
                        INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'theme', ?, 'author_theme', 0.6)
                    """, (eid, theme))

            # Era from author birth year (approximate work era)
            birth = rec.get('birth', '')
            if birth and len(birth) >= 4:
                try:
                    year = int(birth[:4])
                    # Works typically produced 20-50 years after birth
                    active_year = year + 30
                    era = year_to_era(active_year)
                    db.execute("""
                        INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'era', ?, 'author_birth_estimate', 0.6)
                    """, (eid, era))
                except ValueError:
                    pass

            new_works += 1

        if (new_works + new_authors) % 2000 == 0 and (new_works + new_authors) > 0:
            db.commit()
            print(f"  Progress: {new_works:,} works, {new_authors:,} authors", flush=True)

    db.commit()

    # Final stats
    aozora_works = db.execute("SELECT COUNT(*) FROM entities WHERE source='aozora_phase8' AND entity_type='work'").fetchone()[0]
    aozora_persons = db.execute("SELECT COUNT(*) FROM entities WHERE source='aozora_phase8' AND entity_type='person'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"\n=== Aozora Bunko Import Complete ===", flush=True)
    print(f"New works: {new_works:,}", flush=True)
    print(f"New authors: {new_authors:,}", flush=True)
    print(f"Aozora works in DB: {aozora_works:,}", flush=True)
    print(f"Aozora authors in DB: {aozora_persons:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
