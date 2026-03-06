"""
Phase 6C: Auto-tag all untagged entities with 5-axis tags.
Rule-based, no LLM. Fast processing.
"""

import sqlite3
import re
from typing import Optional, Tuple

DB_PATH = "ontology/culture_ontology.db"

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[AutoTag] {msg}\n")

# === Theme keyword mapping ===
KEYWORD_THEMES = [
    # Yokai/supernatural
    (['妖怪', '化け', '幽霊', '鬼', '百鬼', '怪談', '怪奇', '霊'], 'yokai'),
    (['忍者', '忍', 'ニンジャ'], 'ninja'),
    (['侍', '武士', '武道', '剣', '刀'], 'samurai'),
    (['神社', '鳥居', '神道', '祭神'], 'shrine_temple'),
    (['寺', '仏', '仏教', '菩薩', '如来', '阿弥陀'], 'sacred_profane'),
    (['城', '城郭', '天守'], 'war_conflict'),
    (['祭', '踊', '盆踊', '山車', '神輿'], 'community_tradition'),
    (['温泉', '湯', '風呂'], 'nature_communion'),
    (['桜', '花見', '梅', '藤'], 'seasonal_beauty'),
    (['紅葉', '秋', '落葉'], 'seasonal_beauty'),
    (['茶', '侘び', '寂び'], 'wabi_sabi'),
    (['禅', '坐禅', '瞑想'], 'reflective_beauty'),
    (['料理', '食', '寿司', '和食', 'ラーメン', '蕎麦', '酒'], 'food_culture'),
    (['庭', '庭園', '枯山水'], 'garden_beauty'),
    (['花', '草', '自然', '山', '海', '川', '森', '滝'], 'nature_communion'),
    (['魔法', '魔', '異世界', 'ファンタジー'], 'magic'),
    (['転生', '死', '霊', '冥'], 'death_rebirth'),
    (['恋', '愛', '結婚', '恋愛'], 'love_bond'),
    (['学園', '青春', '学校', '部活'], 'identity_self'),
    (['ロボット', 'メカ', '機械', 'ガンダム', 'マクロス'], 'mecha'),
    (['浮世絵', '版画', '錦絵', '木版'], 'ukiyoe_craft'),
    (['屏風', '絵巻', '掛軸', '襖絵'], 'ukiyoe_craft'),
    (['歌舞伎', '能', '狂言', '文楽', '人形浄瑠璃'], 'performing_arts'),
    (['俳句', '和歌', '短歌', '万葉', '古今'], 'literary_spirit'),
    (['着物', '織物', '染色', '友禅'], 'craft_mastery'),
    (['陶芸', '焼', '磁器', '陶器', '漆'], 'craft_mastery'),
    (['相撲', '柔道', '剣道', '空手', '合気道'], 'martial_arts'),
    (['SF', '宇宙', 'サイエンス', '未来'], 'science_fiction'),
    (['推理', '探偵', 'ミステリー', '事件'], 'mystery'),
    (['スポーツ', '野球', 'サッカー', 'バスケ', 'テニス'], 'sports'),
    (['音楽', 'バンド', 'ライブ', '歌', '楽器', 'アイドル'], 'performing_arts'),
    (['戦争', '戦', '軍', '兵'], 'war_conflict'),
    (['旅', '冒険', '漂流', '航海'], 'journey_boundary'),
    (['変身', '変容', '成長'], 'transformation'),
]

# === Era detection ===
def detect_era_from_date(date_str):
    """Detect era code from date string."""
    if not date_str:
        return None
    # Extract year
    m = re.search(r'(\d{4})', str(date_str))
    if not m:
        return None
    year = int(m.group(1))
    if year < 710:
        return 'ancient'
    elif year < 1185:
        return 'heian'
    elif year < 1333:
        return 'kamakura'
    elif year < 1573:
        return 'muromachi'
    elif year < 1603:
        return 'azuchi_momoyama'
    elif year < 1868:
        return 'edo'
    elif year < 1912:
        return 'meiji'
    elif year < 1926:
        return 'taisho'
    elif year < 1989:
        return 'showa'
    elif year < 2019:
        return 'heisei'
    else:
        return 'reiwa'

# === Medium detection ===
def detect_medium(label, entity_type, source):
    """Detect medium from entity metadata."""
    if source and 'madb' in source:
        if 'manga' in source:
            return 'manga'
        elif 'anime' in source:
            return 'anime'
        elif 'game' in source:
            return 'game'
        elif 'media_art' in source:
            return 'media_art'

    if source and 'anilist' in source:
        return 'anime'

    label_lower = label.lower() if label else ''

    # Keyword-based
    if any(k in label_lower for k in ['漫画', 'マンガ', 'コミック']):
        return 'manga'
    if any(k in label_lower for k in ['アニメ', 'anime']):
        return 'anime'
    if any(k in label_lower for k in ['ゲーム', 'game']):
        return 'game'
    if any(k in label_lower for k in ['映画', 'film', 'シネマ']):
        return 'film'
    if any(k in label_lower for k in ['小説', 'ノベル', '物語', '文学']):
        return 'literature'
    if any(k in label_lower for k in ['浮世絵', '版画', '錦絵']):
        return 'ukiyoe'
    if any(k in label_lower for k in ['絵巻', '屏風', '掛軸', '襖']):
        return 'painting'
    if any(k in label_lower for k in ['神社', '寺', '城', '庭園']):
        return 'architecture'

    if entity_type == 'place':
        return 'architecture'
    if entity_type == 'person':
        return None  # people don't have a medium
    if entity_type == 'event':
        return 'festival'

    return None

# === Geography from coordinates ===
REGIONS = {
    'hokkaido': (41.3, 45.6, 139.3, 145.8),
    'tohoku': (37.7, 41.5, 139.0, 141.7),
    'kanto': (35.0, 37.0, 138.5, 140.9),
    'chubu': (34.5, 37.8, 136.0, 139.0),
    'kinki': (33.4, 35.8, 134.0, 136.8),
    'chugoku': (33.7, 35.6, 130.8, 134.4),
    'shikoku': (32.7, 34.3, 132.0, 134.8),
    'kyushu': (30.0, 34.0, 129.5, 132.0),
    'okinawa': (24.0, 27.0, 122.0, 131.0),
}

def coords_to_region(lat, lon):
    if not lat or not lon:
        return None
    for name, (lat_min, lat_max, lon_min, lon_max) in REGIONS.items():
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return None

# === Geography from label keywords ===
REGION_KEYWORDS = {
    '北海道': 'hokkaido', '札幌': 'hokkaido',
    '青森': 'tohoku', '岩手': 'tohoku', '宮城': 'tohoku', '秋田': 'tohoku',
    '山形': 'tohoku', '福島': 'tohoku', '仙台': 'tohoku',
    '東京': 'kanto', '神奈川': 'kanto', '千葉': 'kanto', '埼玉': 'kanto',
    '茨城': 'kanto', '栃木': 'kanto', '群馬': 'kanto', '鎌倉': 'kanto',
    '横浜': 'kanto', '浅草': 'kanto', '新宿': 'kanto', '渋谷': 'kanto',
    '愛知': 'chubu', '名古屋': 'chubu', '石川': 'chubu', '金沢': 'chubu',
    '新潟': 'chubu', '長野': 'chubu', '富山': 'chubu', '福井': 'chubu',
    '岐阜': 'chubu', '静岡': 'chubu', '山梨': 'chubu',
    '京都': 'kinki', '大阪': 'kinki', '奈良': 'kinki', '兵庫': 'kinki',
    '滋賀': 'kinki', '和歌山': 'kinki', '神戸': 'kinki',
    '広島': 'chugoku', '岡山': 'chugoku', '島根': 'chugoku',
    '鳥取': 'chugoku', '山口': 'chugoku', '出雲': 'chugoku',
    '香川': 'shikoku', '徳島': 'shikoku', '愛媛': 'shikoku', '高知': 'shikoku',
    '福岡': 'kyushu', '佐賀': 'kyushu', '長崎': 'kyushu', '熊本': 'kyushu',
    '大分': 'kyushu', '宮崎': 'kyushu', '鹿児島': 'kyushu',
    '沖縄': 'okinawa', '琉球': 'okinawa', '那覇': 'okinawa',
}

def label_to_region(label):
    if not label:
        return None
    for keyword, region in REGION_KEYWORDS.items():
        if keyword in label:
            return region
    return None

# === Experience mode ===
def detect_experience(label, entity_type, medium):
    if entity_type == 'place':
        if any(k in (label or '') for k in ['神社', '寺', '庭園', '墓']):
            return 'reflective'
        elif any(k in (label or '') for k in ['城', '博物館', '美術館']):
            return 'intellectual'
        elif any(k in (label or '') for k in ['温泉', '公園', '自然']):
            return 'sensory'
        else:
            return 'aesthetic'
    elif entity_type == 'work':
        if medium in ('anime', 'manga', 'game'):
            return 'immersive'
        elif medium in ('film',):
            return 'aesthetic'
        elif medium in ('literature',):
            return 'intellectual'
        else:
            return 'aesthetic'
    elif entity_type == 'event':
        return 'participatory'
    elif entity_type == 'person':
        return 'intellectual'
    return None


def main():
    db = sqlite3.connect(DB_PATH)

    # Get all untagged entities
    untagged = db.execute("""
        SELECT e.id, e.label_ja, e.entity_type, e.source, e.lat, e.lon,
               e.wikidata_id, e.madb_id, e.anilist_id, e.ndl_id
        FROM entities e
        LEFT JOIN entity_tags et ON e.id = et.entity_id
        WHERE et.id IS NULL
    """).fetchall()

    log(f"Untagged entities: {len(untagged):,}")

    tagged_count = 0
    batch = []

    for row in untagged:
        eid, label, etype, source, lat, lon, wikidata_id, madb_id, anilist_id, ndl_id = row
        if not label:
            continue

        tags = []

        # Theme
        for keywords, theme_code in KEYWORD_THEMES:
            if any(k in label for k in keywords):
                tags.append(('theme', theme_code))
        if not any(t[0] == 'theme' for t in tags):
            if etype == 'place':
                tags.append(('theme', 'community_tradition'))
            elif etype == 'work':
                tags.append(('theme', 'craft_mastery'))

        # Medium
        medium = detect_medium(label, etype, source)
        if medium:
            tags.append(('medium', medium))

        # Era
        era = detect_era_from_date(source)  # Some sources have date info
        if not era:
            if anilist_id:
                era = 'heisei'
            elif ndl_id:
                era = 'edo'  # NDL classical texts default
            elif etype == 'place' and any(k in (label or '') for k in ['城', '寺', '神社']):
                era = 'medieval'
        if era:
            tags.append(('era', era))

        # Geography
        region = coords_to_region(lat, lon) or label_to_region(label)
        if region:
            tags.append(('geography', region))

        # Experience
        exp = detect_experience(label, etype, medium)
        if exp:
            tags.append(('experience', exp))

        for axis, value_code in tags:
            batch.append((eid, axis, value_code, 'auto_phase6', 0.6))

        tagged_count += 1
        if tagged_count % 5000 == 0:
            db.executemany(
                "INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, ?, ?, ?, ?)",
                batch
            )
            db.commit()
            batch = []
            log(f"  Tagged {tagged_count:,}...")

    # Final batch
    if batch:
        db.executemany(
            "INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, ?, ?, ?, ?)",
            batch
        )
        db.commit()

    total_tags = db.execute("SELECT COUNT(*) FROM entity_tags WHERE source='auto_phase6'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    tagged_entities = db.execute("""
        SELECT COUNT(DISTINCT entity_id) FROM entity_tags
    """).fetchone()[0]

    log(f"\n=== Auto-Tag Complete ===")
    log(f"Entities tagged: {tagged_count:,}")
    log(f"Tags created: {total_tags:,}")
    log(f"Total entities: {total_entities:,}")
    log(f"Entities with tags: {tagged_entities:,} ({tagged_entities/total_entities*100:.1f}%)")

    db.close()

if __name__ == "__main__":
    main()
