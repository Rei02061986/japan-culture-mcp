"""
Phase 8B-1: Reduce craft_mastery dominance to <= 25%.
1. Expand keyword dictionary for MADB works
2. Add more genre-based theme inference
3. Use AniList title matching improvements
"""
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"

# Expanded keywords (much broader than Phase 7)
KEYWORDS = {
    # Love & Romance
    '恋': 'love_bond', '愛': 'love_bond', 'ラブ': 'love_bond',
    'ハート': 'love_bond', 'キス': 'love_bond', '結婚': 'love_bond',
    '彼女': 'love_bond', '彼氏': 'love_bond', 'カノジョ': 'love_bond',
    'ハーレム': 'love_bond', 'カップル': 'love_bond',
    '嫁': 'love_bond', '妻': 'love_bond', '夫婦': 'love_bond',
    '告白': 'love_bond', 'デート': 'love_bond',

    # Battle & War
    '戦': 'war_conflict', 'バトル': 'war_conflict', '闘': 'war_conflict',
    'ファイト': 'war_conflict', '格闘': 'war_conflict',
    '軍': 'war_conflict', '兵': 'war_conflict', '銃': 'war_conflict',

    # Magic & Supernatural
    '魔': 'magic', '魔法': 'magic', '魔王': 'supernatural',
    '魔女': 'magic', '魔術': 'magic', 'マジカル': 'magic',
    '呪': 'supernatural', '霊': 'supernatural', '悪魔': 'supernatural',
    '天使': 'supernatural', 'エルフ': 'supernatural',
    '精霊': 'supernatural', '吸血鬼': 'supernatural',
    'ヴァンパイア': 'supernatural', 'ゾンビ': 'supernatural',

    # Isekai & Otherworld
    '異世界': 'isekai', '転生': 'reincarnation', '転移': 'isekai',
    'チート': 'isekai', 'ダンジョン': 'isekai',
    'レベル': 'isekai', 'スキル': 'isekai',
    '勇者': 'isekai', '召喚': 'isekai',
    'ファンタジー': 'otherworld',

    # Yokai & Japanese supernatural
    '妖怪': 'yokai', '鬼': 'yokai', '幽霊': 'ghost_spirit',
    '怪': 'yokai', '化け': 'yokai', '狐': 'yokai',
    'おばけ': 'yokai', 'ゆうれい': 'ghost_spirit',
    '百鬼': 'yokai',

    # Ninja & Samurai
    '忍': 'ninja', '忍者': 'ninja', 'ニンジャ': 'ninja',
    '侍': 'samurai', '武士': 'samurai', '剣': 'swordplay',
    '刀': 'swordplay', 'ソード': 'swordplay',
    '武道': 'martial_arts', '柔道': 'martial_arts', '空手': 'martial_arts',

    # Mecha & Robot
    'ロボ': 'mecha', 'ガンダム': 'mecha', 'メカ': 'mecha',
    'マシン': 'mecha', 'サイボーグ': 'mecha',

    # School & Youth
    '学園': 'coming_of_age', '学校': 'coming_of_age', '高校': 'coming_of_age',
    '中学': 'coming_of_age', '大学': 'coming_of_age', '青春': 'coming_of_age',
    'スクール': 'coming_of_age', '部活': 'coming_of_age',
    '先生': 'coming_of_age', '生徒': 'coming_of_age',
    '少女': 'coming_of_age', '少年': 'coming_of_age',

    # Mystery & Detective
    '探偵': 'identity_self', '推理': 'identity_self', 'ミステリ': 'identity_self',
    '事件': 'identity_self', '謎': 'identity_self', '犯人': 'identity_self',
    'サスペンス': 'identity_self', 'トリック': 'identity_self',

    # Food & Cuisine
    '料理': 'food_cuisine', 'グルメ': 'food_cuisine', '食': 'food_cuisine',
    'レストラン': 'food_cuisine', 'ラーメン': 'food_cuisine',
    '寿司': 'food_cuisine', '酒': 'food_cuisine', 'カフェ': 'food_cuisine',
    '弁当': 'food_cuisine', 'パン': 'food_cuisine',

    # Music & Performance
    '音楽': 'musical_arts', 'バンド': 'musical_arts', '歌': 'musical_arts',
    'アイドル': 'musical_arts', 'ライブ': 'musical_arts',
    'ロック': 'musical_arts', 'ピアノ': 'musical_arts',

    # Space & Sci-Fi
    '宇宙': 'journey_boundary', 'SF': 'otherworld', '未来': 'otherworld',
    'ロケット': 'journey_boundary', '惑星': 'journey_boundary',
    'サイエンス': 'otherworld', 'タイムマシン': 'otherworld',

    # Horror & Death
    'ホラー': 'death_rebirth', '恐怖': 'death_rebirth', '死': 'death_rebirth',
    '殺': 'death_rebirth', 'デスゲーム': 'death_game',
    'サバイバル': 'survival', '地獄': 'death_rebirth',

    # Comedy & Humor
    'コメディ': 'humor_satire', 'ギャグ': 'humor_satire', '笑': 'humor_satire',
    'お笑い': 'humor_satire', '4コマ': 'humor_satire',
    'ほのぼの': 'everyday_beauty', '癒': 'everyday_beauty',

    # Daily Life & Slice of Life
    '日常': 'everyday_beauty', '生活': 'everyday_beauty',
    'ほっこり': 'everyday_beauty', 'のんびり': 'everyday_beauty',
    'ペット': 'everyday_beauty', '家族': 'family_life',
    '子育て': 'family_life', '育児': 'family_life',

    # Sports
    'サッカー': 'sports', '野球': 'sports', 'テニス': 'sports',
    'バスケ': 'sports', 'バレー': 'sports', 'ゴルフ': 'sports',
    'ボクシング': 'sports', 'レース': 'sports', '競馬': 'sports',
    '水泳': 'sports', '陸上': 'sports', 'スポーツ': 'sports',

    # Traditional & Sacred
    '茶': 'wabi_sabi', '華道': 'traditional_craft',
    '書道': 'calligraphy', '祭': 'matsuri',
    '神社': 'shrine_temple', '寺': 'shrine_temple',
    '仏': 'sacred_profane', '神': 'sacred_profane',

    # Nature
    '温泉': 'nature_communion', '旅': 'journey_boundary',
    '桜': 'seasonal_beauty', '花': 'nature_communion',
    '山': 'nature_communion', '海': 'nature_communion',
    '釣り': 'nature_communion', 'キャンプ': 'nature_communion',
    '農': 'nature_communion',

    # Fantasy creatures
    'ドラゴン': 'supernatural', '竜': 'supernatural', '龍': 'supernatural',
    'モンスター': 'supernatural',

    # Power & Politics
    '王': 'power_rebellion', '姫': 'love_bond', '帝国': 'power_rebellion',
    '革命': 'power_rebellion', '政治': 'power_rebellion',
    'マフィア': 'power_rebellion', 'ヤクザ': 'yakuza',

    # Adventure & Journey
    '海賊': 'journey_boundary', '冒険': 'adventure_quest',
    '探検': 'adventure_quest', 'クエスト': 'adventure_quest',
    '宝': 'adventure_quest',

    # Medical & Science
    '医': 'identity_self', '病院': 'identity_self',
    '刑事': 'identity_self', '警察': 'identity_self',

    # Vehicles & Racing
    '車': 'journey_boundary', 'バイク': 'journey_boundary',
    '飛行': 'journey_boundary',

    # Fashion & Art
    'ファッション': 'everyday_beauty', 'モデル': 'everyday_beauty',

    # Historical
    '三国志': 'historical_event', '戦国': 'samurai',
    '幕末': 'samurai', '信長': 'samurai', '秀吉': 'samurai',
    '家康': 'samurai', '源氏': 'historical_event', '平家': 'historical_event',

    # Transformation
    '変身': 'henshin', 'ヒーロー': 'henshin', '仮面': 'henshin',
    'プリキュア': 'magic', 'セーラームーン': 'magic',

    # Gaming
    'ゲーム': 'identity_self', 'オンライン': 'identity_self',
    'VR': 'otherworld', 'ネット': 'identity_self',
}


def main():
    db = sqlite3.connect(DB_PATH)

    # Stats before
    craft_before = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme' AND value_code='craft_mastery'").fetchone()[0]
    total_before = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme'").fetchone()[0]
    print(f"Before: craft_mastery {craft_before:,}/{total_before:,} ({craft_before/total_before*100:.1f}%)", flush=True)

    # Get all entities with ONLY craft_mastery as theme (any source)
    rows = db.execute("""
        SELECT e.id, e.label_ja
        FROM entities e
        JOIN entity_tags et ON e.id = et.entity_id
        WHERE et.axis = 'theme' AND et.value_code = 'craft_mastery'
        AND NOT EXISTS (
            SELECT 1 FROM entity_tags et2
            WHERE et2.entity_id = e.id AND et2.axis = 'theme'
            AND et2.value_code != 'craft_mastery'
        )
    """).fetchall()
    print(f"Entities with ONLY craft_mastery: {len(rows):,}", flush=True)

    retagged = 0
    for eid, label in rows:
        if not label:
            continue

        new_themes = set()
        for keyword, theme in KEYWORDS.items():
            if keyword in label and theme != 'craft_mastery':
                new_themes.add(theme)

        if new_themes:
            db.execute("""
                DELETE FROM entity_tags
                WHERE entity_id = ? AND axis = 'theme' AND value_code = 'craft_mastery'
            """, (eid,))

            for theme in new_themes:
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'theme', ?, 'keyword_retag_v2', 0.65)
                """, (eid, theme))

            retagged += 1

        if retagged % 2000 == 0 and retagged > 0:
            db.commit()
            print(f"  Retagged: {retagged:,}", flush=True)

    db.commit()

    # Phase 2: For remaining craft_mastery-only entities, check if they're persons
    # Persons should have more specific themes based on their entity_type context
    person_only_cm = db.execute("""
        SELECT e.id, e.label_ja
        FROM entities e
        JOIN entity_tags et ON e.id = et.entity_id
        WHERE et.axis = 'theme' AND et.value_code = 'craft_mastery'
        AND e.entity_type = 'person'
        AND NOT EXISTS (
            SELECT 1 FROM entity_tags et2
            WHERE et2.entity_id = e.id AND et2.axis = 'theme'
            AND et2.value_code != 'craft_mastery'
        )
    """).fetchall()
    print(f"\nPersons with ONLY craft_mastery: {len(person_only_cm):,}", flush=True)

    # For persons, check their medium tag to infer better theme
    person_retagged = 0
    MEDIUM_TO_BETTER_THEME = {
        'manga': 'visual_arts',
        'anime': 'visual_arts',
        'anime_tv': 'visual_arts',
        'anime_movie': 'visual_arts',
        'literature': 'literary_arts',
        'music': 'musical_arts',
        'game': 'interactive_arts',
        'architecture': 'architecture',
        'film': 'visual_arts',
        'ukiyoe': 'ukiyoe_craft',
        'photography': 'visual_arts',
    }

    for eid, label in person_only_cm:
        medium_row = db.execute("""
            SELECT value_code FROM entity_tags WHERE entity_id = ? AND axis = 'medium'
        """, (eid,)).fetchone()

        if medium_row and medium_row[0] in MEDIUM_TO_BETTER_THEME:
            better = MEDIUM_TO_BETTER_THEME[medium_row[0]]
            db.execute("""
                UPDATE entity_tags SET value_code = ?, source = 'medium_infer'
                WHERE entity_id = ? AND axis = 'theme' AND value_code = 'craft_mastery'
            """, (better, eid))
            person_retagged += 1

    db.commit()
    print(f"  Person retagged via medium: {person_retagged:,}", flush=True)

    # Stats after
    craft_after = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme' AND value_code='craft_mastery'").fetchone()[0]
    total_after = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme'").fetchone()[0]

    print(f"\n=== Craft Mastery Fix Complete ===", flush=True)
    print(f"Works retagged by keyword: {retagged:,}", flush=True)
    print(f"Persons retagged by medium: {person_retagged:,}", flush=True)
    print(f"craft_mastery: {craft_before:,} → {craft_after:,}", flush=True)
    print(f"Ratio: {craft_before/total_before*100:.1f}% → {craft_after/total_after*100:.1f}%", flush=True)
    db.close()

if __name__ == "__main__":
    main()
