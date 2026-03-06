"""
Phase 7 A2: Retag MADB works that have only craft_mastery.
Use label keywords to assign more specific themes.
"""
import sqlite3

DB_PATH = "ontology/culture_ontology.db"

EXTENDED_KEYWORDS = {
    '恋': 'love_bond', '愛': 'love_bond', 'ラブ': 'love_bond',
    '戦': 'war_conflict', 'バトル': 'war_conflict', '闘': 'war_conflict',
    '魔': 'magic', '魔法': 'magic', '魔王': 'supernatural',
    '異世界': 'isekai', '転生': 'death_rebirth', '転移': 'isekai',
    '妖怪': 'yokai', '鬼': 'yokai', '幽霊': 'supernatural', '怪': 'yokai',
    '忍': 'ninja', '忍者': 'ninja',
    '侍': 'samurai', '武士': 'samurai', '剣': 'swordplay',
    'ロボ': 'mecha', 'ガンダム': 'mecha', 'メカ': 'mecha',
    '学園': 'identity_self', '学校': 'identity_self', '高校': 'identity_self',
    '探偵': 'identity_self', '推理': 'identity_self', 'ミステリ': 'identity_self',
    '料理': 'food_culture', 'グルメ': 'food_culture',
    'スポーツ': 'craft_mastery', '野球': 'craft_mastery', 'サッカー': 'craft_mastery',
    '音楽': 'musical_arts', 'バンド': 'musical_arts', '歌': 'musical_arts',
    '宇宙': 'journey_boundary', 'SF': 'otherworld',
    'ホラー': 'death_rebirth', '恐怖': 'death_rebirth',
    'コメディ': 'humor_satire', 'ギャグ': 'humor_satire', '笑': 'humor_satire',
    '日常': 'everyday_beauty', 'ほのぼの': 'everyday_beauty',
    '茶': 'wabi_sabi', '華道': 'traditional_craft', '書道': 'calligraphy_craft',
    '祭': 'community_tradition', '神社': 'shrine_temple', '寺': 'shrine_temple',
    '仏': 'sacred_profane', '神': 'sacred_profane',
    '温泉': 'nature_communion', '旅': 'journey_boundary',
    '桜': 'seasonal_beauty', '花': 'nature_communion',
    'ドラゴン': 'supernatural', '竜': 'supernatural', '龍': 'supernatural',
    '海賊': 'journey_boundary', '冒険': 'journey_boundary',
    '王': 'power_rebellion', '姫': 'love_bond',
    '死': 'death_rebirth', '殺': 'death_rebirth',
    'ファンタジー': 'otherworld', '魔女': 'magic',
    '猫': 'nature_communion', '犬': 'nature_communion',
    '医': 'craft_mastery', '病院': 'identity_self',
    '刑事': 'identity_self', '警察': 'identity_self',
}

def main():
    db = sqlite3.connect(DB_PATH)

    # Get MADB works with craft_mastery as their only theme
    rows = db.execute("""
        SELECT e.id, e.label_ja
        FROM entities e
        JOIN entity_tags et ON e.id = et.entity_id
        WHERE et.axis = 'theme' AND et.value_code = 'craft_mastery'
        AND e.source = 'madb_phase6'
        AND e.entity_type = 'work'
        AND NOT EXISTS (
            SELECT 1 FROM entity_tags et2
            WHERE et2.entity_id = e.id AND et2.axis = 'theme'
            AND et2.value_code != 'craft_mastery'
        )
    """).fetchall()

    print(f"MADB works with ONLY craft_mastery: {len(rows):,}", flush=True)

    retagged = 0
    for eid, label in rows:
        if not label:
            continue

        new_themes = set()
        for keyword, theme in EXTENDED_KEYWORDS.items():
            if keyword in label and theme != 'craft_mastery':
                new_themes.add(theme)

        if new_themes:
            # Remove craft_mastery
            db.execute("""
                DELETE FROM entity_tags
                WHERE entity_id = ? AND axis = 'theme' AND value_code = 'craft_mastery'
            """, (eid,))

            # Add new themes
            for theme in new_themes:
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'theme', ?, 'keyword_retag', 0.7)
                """, (eid, theme))

            retagged += 1

        if retagged % 1000 == 0 and retagged > 0:
            db.commit()
            print(f"  Retagged: {retagged:,}", flush=True)

    db.commit()

    # Stats
    craft_count = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme' AND value_code='craft_mastery'").fetchone()[0]
    total_theme = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme'").fetchone()[0]

    print(f"\n=== MADB Retag Complete ===", flush=True)
    print(f"Retagged: {retagged:,} / {len(rows):,}", flush=True)
    print(f"craft_mastery remaining: {craft_count:,}/{total_theme:,} ({craft_count/total_theme*100:.1f}%)", flush=True)
    db.close()

if __name__ == "__main__":
    main()
