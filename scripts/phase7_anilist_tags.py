"""
Phase 7 A1: Import AniList genres/tags into entity_tags.
Replace generic craft_mastery with specific themes from AniList data.
"""
import sqlite3
import json
import os
import sys

DB_PATH = "ontology/culture_ontology.db"

GENRE_TO_THEME = {
    'Action': 'war_conflict',
    'Adventure': 'journey_boundary',
    'Comedy': 'humor_satire',
    'Drama': 'identity_self',
    'Fantasy': 'otherworld',
    'Horror': 'death_rebirth',
    'Mahou Shoujo': 'magic',
    'Mecha': 'mecha',
    'Music': 'musical_arts',
    'Mystery': 'identity_self',
    'Psychological': 'identity_self',
    'Romance': 'love_bond',
    'Sci-Fi': 'otherworld',
    'Slice of Life': 'everyday_beauty',
    'Sports': 'craft_mastery',
    'Supernatural': 'supernatural',
    'Thriller': 'power_rebellion',
    'Ecchi': None,
    'Hentai': None,
}

TAG_TO_THEME = {
    'Youkai': 'yokai',
    'Isekai': 'isekai',
    'Magic': 'magic',
    'Mythology': 'mythology',
    'Samurai': 'samurai',
    'Ninja': 'ninja',
    'Military': 'war_conflict',
    'War': 'war_conflict',
    'Reincarnation': 'death_rebirth',
    'Time Travel': 'transformation',
    'Revenge': 'power_rebellion',
    'Coming of Age': 'identity_self',
    'Food': 'food_culture',
    'Music': 'musical_arts',
    'Calligraphy': 'calligraphy_craft',
    'Kabuki': 'kabuki_craft',
    'Rakugo': 'rakugo_craft',
    'Martial Arts': 'martial_craft',
    'Swordplay': 'swordplay',
    'Survival': 'death_rebirth',
    'School': 'identity_self',
    'Love Triangle': 'love_bond',
    'Demons': 'yokai',
    'Gods': 'sacred_profane',
    'Zombies': 'death_rebirth',
    'Vampire': 'supernatural',
    'Ghost': 'supernatural',
    'Kaiju': 'kaiju',
    'Robots': 'mecha',
    'Space': 'journey_boundary',
    'Post-Apocalyptic': 'death_rebirth',
    'Cyberpunk': 'otherworld',
    'Steampunk': 'otherworld',
    'Villainess': 'power_rebellion',
    'Anti-Hero': 'power_rebellion',
    'Otaku Culture': 'identity_self',
    'Cultivation': 'craft_mastery',
    'Alchemy': 'transformation',
    'Archery': 'martial_craft',
    'Sumo': 'community_tradition',
    'Judo': 'martial_craft',
    'Go': 'craft_mastery',
    'Shogi': 'craft_mastery',
    'Iyashikei': 'everyday_beauty',
    'CGDCT': 'everyday_beauty',
    'Tokusatsu': 'supernatural',
}

FORMAT_TO_MEDIUM = {
    'TV': 'anime_tv',
    'TV_SHORT': 'anime_tv',
    'MOVIE': 'anime_movie',
    'SPECIAL': 'anime',
    'OVA': 'anime_ova',
    'ONA': 'anime',
    'MUSIC': 'music',
    'MANGA': 'manga',
    'ONE_SHOT': 'manga',
    'NOVEL': 'literature',
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

def main():
    db = sqlite3.connect(DB_PATH)

    # Load AniList data
    anilist_data = []
    for fname in ['anime.json', 'manga.json']:
        fpath = f'data/anilist/{fname}'
        try:
            with open(fpath) as f:
                anilist_data.extend(json.load(f))
        except FileNotFoundError:
            print(f"  {fpath} not found, skipping")

    print(f"AniList records: {len(anilist_data)}", flush=True)

    # Cache entity lookups
    entities = {}
    for row in db.execute("SELECT id, label_ja, label_en, anilist_id FROM entities"):
        eid, label_ja, label_en, aid = row
        if aid:
            entities[f"anilist:{aid}"] = eid
        if label_ja:
            entities[f"ja:{label_ja}"] = eid
        if label_en:
            entities[f"en:{label_en.lower()}"] = eid

    matched = 0
    tags_added = 0
    craft_mastery_removed = 0

    for item in anilist_data:
        anilist_id = item.get('id')
        title = item.get('title', {})
        native = title.get('native', '')
        romaji = title.get('romaji', '')
        english = title.get('english', '')

        # Entity matching
        entity_id = None
        if anilist_id:
            entity_id = entities.get(f"anilist:{anilist_id}")
        if not entity_id and native:
            entity_id = entities.get(f"ja:{native}")
        if not entity_id and english:
            entity_id = entities.get(f"en:{english.lower()}")
        if not entity_id and romaji:
            entity_id = entities.get(f"en:{romaji.lower()}")

        if not entity_id:
            continue

        matched += 1

        # Remove generic craft_mastery
        deleted = db.execute("""
            DELETE FROM entity_tags
            WHERE entity_id = ? AND axis = 'theme' AND value_code = 'craft_mastery'
            AND source = 'auto_phase6'
        """, (entity_id,)).rowcount
        craft_mastery_removed += deleted

        # Genres -> theme tags
        genres = item.get('genres', []) or []
        for genre in genres:
            theme = GENRE_TO_THEME.get(genre)
            if theme:
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'theme', ?, 'anilist_genre', 0.85)
                """, (entity_id, theme))
                tags_added += 1

        # Tags -> theme tags
        tags = item.get('tags', []) or []
        for tag in tags:
            tag_name = tag.get('name', '')
            tag_rank = tag.get('rank', 0)
            if tag_rank < 30:
                continue
            theme = TAG_TO_THEME.get(tag_name)
            if theme:
                confidence = min(0.9, 0.5 + tag_rank / 200)
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'theme', ?, 'anilist_tag', ?)
                """, (entity_id, theme, confidence))
                tags_added += 1

        # Format -> medium tag
        fmt = item.get('format')
        if fmt:
            medium = FORMAT_TO_MEDIUM.get(fmt)
            if medium:
                db.execute("""
                    DELETE FROM entity_tags
                    WHERE entity_id = ? AND axis = 'medium' AND source = 'auto_phase6'
                """, (entity_id,))
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'medium', ?, 'anilist_format', 0.95)
                """, (entity_id, medium))
                tags_added += 1

        # seasonYear -> era tag
        year = item.get('seasonYear')
        if year:
            era = year_to_era(year)
            if era:
                db.execute("""
                    DELETE FROM entity_tags
                    WHERE entity_id = ? AND axis = 'era' AND source = 'auto_phase6'
                """, (entity_id,))
                db.execute("""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'era', ?, 'anilist_year', 0.95)
                """, (entity_id, era))
                tags_added += 1

        if matched % 500 == 0:
            db.commit()
            print(f"  Matched: {matched}, tags: {tags_added}", flush=True)

    db.commit()

    # Check craft_mastery reduction
    craft_count = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme' AND value_code='craft_mastery'").fetchone()[0]
    total_theme = db.execute("SELECT COUNT(*) FROM entity_tags WHERE axis='theme'").fetchone()[0]

    print(f"\n=== AniList Tag Import Complete ===", flush=True)
    print(f"Matched: {matched}", flush=True)
    print(f"Tags added: {tags_added}", flush=True)
    print(f"craft_mastery removed: {craft_mastery_removed}", flush=True)
    print(f"craft_mastery remaining: {craft_count}/{total_theme} ({craft_count/total_theme*100:.1f}%)", flush=True)
    db.close()

if __name__ == "__main__":
    main()
