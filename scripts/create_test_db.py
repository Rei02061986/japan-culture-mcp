#!/usr/bin/env python3
"""Create a small test database for CI testing.

Generates a SQLite database with ~100 entities, ~50 connections,
FTS5 full-text search index, and R-Tree spatial index.

Usage:
    python scripts/create_test_db.py [--output /path/to/test.db]

Environment:
    TEST_DB_PATH: Alternative way to specify output path (default: /tmp/test_culture_ontology.db)
"""

import argparse
import os
import random
import sqlite3
import sys


def create_test_db(db_path: str) -> None:
    """Create a test database with sample data."""

    # Remove existing DB
    if os.path.exists(db_path):
        os.unlink(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    cur = conn.cursor()

    # ── Schema ──────────────────────────────────────────────
    cur.executescript("""
        CREATE TABLE entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wikidata_id TEXT UNIQUE,
            label_ja TEXT,
            label_en TEXT,
            entity_type TEXT,
            madb_id TEXT,
            ndl_id TEXT,
            anilist_id TEXT,
            dbpedia_uri TEXT,
            lat REAL,
            lon REAL,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            source TEXT
        );

        CREATE TABLE connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_a_id INTEGER REFERENCES entities(id),
            entity_b_id INTEGER REFERENCES entities(id),
            connection_type TEXT,
            theme_distance REAL,
            era_distance REAL,
            medium_distance REAL,
            geography_distance REAL,
            experience_distance REAL,
            serendipity_score REAL,
            explanation TEXT,
            source TEXT,
            confidence REAL,
            llm_verdict TEXT DEFAULT 'keep',
            llm_reason TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE entity_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER REFERENCES entities(id),
            axis TEXT,
            value_code TEXT
        );

        CREATE INDEX idx_entity_tags_entity ON entity_tags(entity_id);
        CREATE INDEX idx_entity_tags_axis ON entity_tags(axis, value_code);
        CREATE INDEX idx_connections_a ON connections(entity_a_id);
        CREATE INDEX idx_connections_b ON connections(entity_b_id);
        CREATE INDEX idx_connections_type ON connections(connection_type);
        CREATE INDEX idx_entities_type ON entities(entity_type);
        CREATE INDEX idx_entities_source ON entities(source);
    """)

    # ── FTS5 Virtual Table ─────────────────────────────────
    cur.execute("""
        CREATE VIRTUAL TABLE entities_fts USING fts5(
            label_ja, label_en,
            content='entities',
            content_rowid='id'
        )
    """)

    # ── R-Tree Virtual Table ───────────────────────────────
    cur.execute("""
        CREATE VIRTUAL TABLE entities_rtree USING rtree(
            id,
            min_lat, max_lat,
            min_lon, max_lon
        )
    """)

    # ── FTS5 Auto-sync Triggers ────────────────────────────
    cur.executescript("""
        CREATE TRIGGER entities_ai AFTER INSERT ON entities BEGIN
            INSERT INTO entities_fts(rowid, label_ja, label_en)
            VALUES (new.id, new.label_ja, new.label_en);
        END;

        CREATE TRIGGER entities_ad AFTER DELETE ON entities BEGIN
            INSERT INTO entities_fts(entities_fts, rowid, label_ja, label_en)
            VALUES ('delete', old.id, old.label_ja, old.label_en);
        END;

        CREATE TRIGGER entities_au AFTER UPDATE ON entities BEGIN
            INSERT INTO entities_fts(entities_fts, rowid, label_ja, label_en)
            VALUES ('delete', old.id, old.label_ja, old.label_en);
            INSERT INTO entities_fts(rowid, label_ja, label_en)
            VALUES (new.id, new.label_ja, new.label_en);
        END;
    """)

    # ── R-Tree Auto-sync Triggers ──────────────────────────
    cur.executescript("""
        CREATE TRIGGER entities_rtree_ai AFTER INSERT ON entities
        WHEN new.lat IS NOT NULL AND new.lon IS NOT NULL BEGIN
            INSERT INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
            VALUES (new.id, new.lat, new.lat, new.lon, new.lon);
        END;

        CREATE TRIGGER entities_rtree_ad AFTER DELETE ON entities
        WHEN old.lat IS NOT NULL AND old.lon IS NOT NULL BEGIN
            DELETE FROM entities_rtree WHERE id = old.id;
        END;

        CREATE TRIGGER entities_rtree_au AFTER UPDATE ON entities
        WHEN old.lat IS NOT NULL AND old.lon IS NOT NULL BEGIN
            DELETE FROM entities_rtree WHERE id = old.id;
            INSERT OR REPLACE INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
            SELECT new.id, new.lat, new.lat, new.lon, new.lon
            WHERE new.lat IS NOT NULL AND new.lon IS NOT NULL;
        END;
    """)

    # ── Seed Data ──────────────────────────────────────────
    # People
    people = [
        ("Q5589", "葛飾北斎", "Katsushika Hokusai", "person", 35.7147, 139.8041, "wikidata"),
        ("Q187231", "歌川広重", "Utagawa Hiroshige", "person", 35.6762, 139.6503, "wikidata"),
        ("Q315862", "喜多川歌麿", "Kitagawa Utamaro", "person", 35.6895, 139.6917, "wikidata"),
        ("Q457580", "世阿弥", "Zeami", "person", None, None, "wikidata"),
        ("Q313093", "近松門左衛門", "Chikamatsu Monzaemon", "person", None, None, "wikidata"),
        ("Q152388", "夏目漱石", "Natsume Soseki", "person", 35.7141, 139.7774, "wikidata"),
        ("Q170790", "宮崎駿", "Hayao Miyazaki", "person", 35.7128, 139.6544, "wikidata"),
        ("Q231229", "手塚治虫", "Osamu Tezuka", "person", 34.7920, 135.4090, "wikidata"),
    ]

    # Temples and Shrines
    temples = [
        ("Q11618", "金閣寺", "Kinkaku-ji", "temple", 35.0394, 135.7292, "wikidata"),
        ("Q210725", "清水寺", "Kiyomizu-dera", "temple", 34.9949, 135.7850, "wikidata"),
        ("Q170495", "東大寺", "Todai-ji", "temple", 34.6890, 135.8398, "wikidata"),
        ("Q616072", "法隆寺", "Horyu-ji", "temple", 34.6145, 135.7340, "wikidata"),
        ("Q834547", "厳島神社", "Itsukushima Shrine", "shrine", 34.2961, 132.3198, "wikidata"),
        ("Q694568", "伏見稲荷大社", "Fushimi Inari Taisha", "shrine", 34.9671, 135.7727, "wikidata"),
        ("Q911461", "鶴岡八幡宮", "Tsurugaoka Hachimangu", "shrine", 35.3258, 139.5566, "osm"),
        ("Q731879", "浅草寺", "Senso-ji", "temple", 35.7148, 139.7967, "osm"),
    ]

    # Anime and Manga
    anime = [
        ("Q193422", "鬼滅の刃", "Demon Slayer", "anime", None, None, "anilist"),
        ("Q865153", "蟲師", "Mushishi", "anime", None, None, "anilist"),
        ("Q217012", "進撃の巨人", "Attack on Titan", "anime", None, None, "anilist"),
        ("Q864811", "千と千尋の神隠し", "Spirited Away", "anime", None, None, "anilist"),
        ("Q211786", "スラムダンク", "Slam Dunk", "anime", None, None, "anilist"),
        ("Q607827", "AKIRA", "Akira", "anime", None, None, "anilist"),
        ("Q244760", "もののけ姫", "Princess Mononoke", "anime", None, None, "anilist"),
        ("Q850870", "攻殻機動隊", "Ghost in the Shell", "anime", None, None, "anilist"),
    ]

    # Artworks
    artworks = [
        ("Q200798", "冨嶽三十六景", "Thirty-six Views of Mount Fuji", "artwork", None, None, "wikidata"),
        ("Q660581", "東海道五十三次", "Fifty-three Stations of the Tokaido", "artwork", None, None, "wikidata"),
        ("Q1123722", "源氏物語絵巻", "Tale of Genji Scroll", "artwork", None, None, "wikidata"),
        ("Q672936", "鳥獣戯画", "Choju-giga", "artwork", None, None, "wikidata"),
    ]

    # Places
    places = [
        ("Q847382", "京都", "Kyoto", "place", 35.0116, 135.7681, "wikidata"),
        ("Q35765", "奈良", "Nara", "place", 34.6851, 135.8049, "wikidata"),
        ("Q17108578", "鎌倉", "Kamakura", "place", 35.3192, 139.5467, "wikidata"),
        (None, "鎌倉高校前駅", "Kamakura-Koko-Mae Station", "place", 35.3058, 139.4968, "osm"),
        (None, "奈良公園", "Nara Park", "place", 34.6851, 135.8430, "osm"),
        (None, "嵐山", "Arashiyama", "place", 35.0094, 135.6722, "osm"),
    ]

    # Festivals
    festivals = [
        (None, "祇園祭", "Gion Matsuri", "festival", 35.0036, 135.7785, "wikidata"),
        (None, "ねぶた祭", "Nebuta Matsuri", "festival", 40.8242, 140.7407, "wikidata"),
        (None, "阿波おどり", "Awa Odori", "festival", 34.0658, 134.5592, "wikidata"),
        (None, "ひな祭り", "Hina Matsuri", "festival", None, None, "wikidata"),
        (None, "七夕", "Tanabata", "event", None, None, "wikidata"),
        (None, "お盆", "Obon", "event", None, None, "wikidata"),
    ]

    # Cultural concepts
    concepts = [
        (None, "浮世絵", "Ukiyo-e", "art_form", None, None, "wikidata"),
        (None, "能", "Noh", "performing_art", None, None, "wikidata"),
        (None, "歌舞伎", "Kabuki", "performing_art", None, None, "wikidata"),
        (None, "茶道", "Tea Ceremony", "cultural_practice", None, None, "wikidata"),
        (None, "俳句", "Haiku", "literary_form", None, None, "wikidata"),
        (None, "書道", "Calligraphy", "art_form", None, None, "wikidata"),
        (None, "和食", "Washoku", "food_culture", None, None, "wikidata"),
        (None, "着物", "Kimono", "craft", None, None, "wikidata"),
        (None, "日本庭園", "Japanese Garden", "art_form", None, None, "wikidata"),
        (None, "侘び寂び", "Wabi-sabi", "aesthetic", None, None, "wikidata"),
    ]

    # Traditional crafts
    crafts = [
        (None, "有田焼", "Arita Ware", "craft", 33.1964, 129.8858, "wikidata"),
        (None, "京友禅", "Kyo-Yuzen", "craft", 35.0116, 135.7681, "wikidata"),
        (None, "輪島塗", "Wajima Lacquerware", "craft", 37.3908, 136.8990, "wikidata"),
        (None, "西陣織", "Nishijin-ori", "craft", 35.0345, 135.7487, "wikidata"),
        (None, "備前焼", "Bizen Ware", "craft", 34.7458, 134.1680, "wikidata"),
        (None, "九谷焼", "Kutani Ware", "craft", 36.2720, 136.3469, "wikidata"),
    ]

    # Literature
    literature = [
        (None, "源氏物語", "The Tale of Genji", "literature", None, None, "aozora"),
        (None, "枕草子", "The Pillow Book", "literature", None, None, "aozora"),
        (None, "吾輩は猫である", "I Am a Cat", "literature", None, None, "aozora"),
        (None, "坊っちゃん", "Botchan", "literature", None, None, "aozora"),
        (None, "走れメロス", "Run, Melos!", "literature", None, None, "aozora"),
        (None, "雪国", "Snow Country", "literature", None, None, "aozora"),
    ]

    # Museums
    museums = [
        (None, "東京国立博物館", "Tokyo National Museum", "museum", 35.7189, 139.7765, "wikidata"),
        (None, "京都国立博物館", "Kyoto National Museum", "museum", 34.9914, 135.7730, "wikidata"),
        (None, "国立西洋美術館", "National Museum of Western Art", "museum", 35.7154, 139.7760, "wikidata"),
        (None, "すみだ北斎美術館", "Sumida Hokusai Museum", "museum", 35.6962, 139.8014, "wikidata"),
    ]

    all_entities = people + temples + anime + artworks + places + festivals + concepts + crafts + literature + museums

    for e in all_entities:
        cur.execute("""
            INSERT OR IGNORE INTO entities
            (wikidata_id, label_ja, label_en, entity_type, lat, lon, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, e)

    conn.commit()

    # ── Entity Tags ────────────────────────────────────────
    # Fetch entity IDs by label
    entity_map = {}
    for row in cur.execute("SELECT id, label_ja FROM entities").fetchall():
        entity_map[row[1]] = row[0]

    tags = []

    # People tags
    for name, era, themes, media in [
        ("葛飾北斎", "edo", ["ukiyoe", "nature_communion"], ["ukiyoe"]),
        ("歌川広重", "edo", ["ukiyoe", "seasonal_beauty"], ["ukiyoe"]),
        ("喜多川歌麿", "edo", ["ukiyoe", "love_bond"], ["ukiyoe"]),
        ("世阿弥", "muromachi", ["performing_arts", "zen"], ["theater"]),
        ("近松門左衛門", "edo", ["performing_arts", "love_bond"], ["theater"]),
        ("夏目漱石", "meiji", ["literary_arts"], ["literature"]),
        ("宮崎駿", "showa", ["nature_communion", "yokai"], ["anime_film"]),
        ("手塚治虫", "showa", ["literary_arts"], ["manga"]),
    ]:
        eid = entity_map.get(name)
        if eid:
            tags.append((eid, "era", era))
            for t in themes:
                tags.append((eid, "theme", t))
            for m in media:
                tags.append((eid, "medium", m))

    # Temple tags
    for name, era, themes, geo in [
        ("金閣寺", "muromachi", ["buddhism", "zen"], "kinki"),
        ("清水寺", "nara", ["buddhism"], "kinki"),
        ("東大寺", "nara", ["buddhism"], "kinki"),
        ("法隆寺", "asuka", ["buddhism"], "kinki"),
        ("厳島神社", "heian", ["shinto"], "chugoku"),
        ("伏見稲荷大社", "nara", ["shinto"], "kinki"),
        ("鶴岡八幡宮", "kamakura", ["shinto", "samurai"], "kanto"),
        ("浅草寺", "asuka", ["buddhism"], "kanto"),
    ]:
        eid = entity_map.get(name)
        if eid:
            tags.append((eid, "era", era))
            for t in themes:
                tags.append((eid, "theme", t))
            tags.append((eid, "geography", geo))

    # Anime tags
    for name, era, themes, media in [
        ("鬼滅の刃", "reiwa", ["yokai", "samurai"], ["anime_tv"]),
        ("蟲師", "heisei", ["yokai", "nature_communion"], ["anime_tv"]),
        ("進撃の巨人", "heisei", ["samurai"], ["anime_tv"]),
        ("千と千尋の神隠し", "heisei", ["yokai", "nature_communion"], ["anime_film"]),
        ("スラムダンク", "heisei", ["youth"], ["anime_tv", "manga"]),
        ("AKIRA", "showa", ["technology"], ["anime_film"]),
        ("もののけ姫", "heisei", ["nature_communion", "yokai"], ["anime_film"]),
        ("攻殻機動隊", "heisei", ["technology"], ["anime_film"]),
    ]:
        eid = entity_map.get(name)
        if eid:
            tags.append((eid, "era", era))
            for t in themes:
                tags.append((eid, "theme", t))
            for m in media:
                tags.append((eid, "medium", m))

    # Art form / concept tags
    for name, era, themes, media in [
        ("浮世絵", "edo", ["ukiyoe"], ["ukiyoe"]),
        ("能", "muromachi", ["performing_arts", "zen"], ["theater"]),
        ("歌舞伎", "edo", ["performing_arts"], ["theater"]),
        ("茶道", "muromachi", ["zen", "seasonal_beauty"], ["cultural_practice"]),
        ("俳句", "edo", ["literary_arts", "seasonal_beauty"], ["literature"]),
    ]:
        eid = entity_map.get(name)
        if eid:
            tags.append((eid, "era", era))
            for t in themes:
                tags.append((eid, "theme", t))
            for m in media:
                tags.append((eid, "medium", m))

    cur.executemany(
        "INSERT INTO entity_tags (entity_id, axis, value_code) VALUES (?, ?, ?)",
        tags,
    )

    # ── Connections ─────────────────────────────────────────
    connection_types = [
        "creator_work", "thematic_resonance", "same_theme", "shared_genre",
        "geographic_cultural", "heritage_location", "pilgrimage_filming",
        "pilgrimage_proximity", "influence", "adaptation", "shared_motif",
        "era_bridge", "cultural_echo", "temporal_echo", "medium_cross",
    ]

    connections = []

    # Manually curated connections
    manual = [
        ("葛飾北斎", "冨嶽三十六景", "creator_work", 0.92, "Hokusai created his masterpiece series"),
        ("歌川広重", "東海道五十三次", "creator_work", 0.90, "Hiroshige's most famous series"),
        ("葛飾北斎", "蟲師", "thematic_resonance", 0.85, "Both explore supernatural nature themes"),
        ("葛飾北斎", "浮世絵", "same_theme", 0.90, "Hokusai is a master of ukiyo-e"),
        ("歌川広重", "浮世絵", "same_theme", 0.90, "Hiroshige is a master of ukiyo-e"),
        ("能", "歌舞伎", "shared_genre", 0.70, "Both are Japanese traditional performing arts"),
        ("世阿弥", "能", "creator_work", 0.88, "Zeami established Noh theater aesthetics"),
        ("金閣寺", "清水寺", "geographic_cultural", 0.65, "Both are famous Kyoto temples"),
        ("金閣寺", "京都", "heritage_location", 0.60, "Kinkaku-ji is a Kyoto landmark"),
        ("清水寺", "京都", "heritage_location", 0.60, "Kiyomizu-dera is a Kyoto landmark"),
        ("スラムダンク", "鎌倉高校前駅", "pilgrimage_filming", 0.95, "Iconic railroad crossing scene"),
        ("鎌倉高校前駅", "鶴岡八幡宮", "pilgrimage_proximity", 0.50, "Both in Kamakura area"),
        ("東大寺", "奈良公園", "geographic_cultural", 0.55, "Todai-ji is adjacent to Nara Park"),
        ("鬼滅の刃", "蟲師", "shared_motif", 0.75, "Both feature supernatural beings"),
        ("千と千尋の神隠し", "もののけ姫", "same_theme", 0.72, "Both are Ghibli films about nature spirits"),
        ("宮崎駿", "千と千尋の神隠し", "creator_work", 0.95, "Miyazaki directed Spirited Away"),
        ("宮崎駿", "もののけ姫", "creator_work", 0.95, "Miyazaki directed Princess Mononoke"),
        ("手塚治虫", "AKIRA", "influence", 0.60, "Tezuka's manga pioneered the genre"),
        ("夏目漱石", "吾輩は猫である", "creator_work", 0.90, "Soseki's debut novel"),
        ("夏目漱石", "坊っちゃん", "creator_work", 0.90, "Soseki's famous novel"),
        ("京都", "祇園祭", "heritage_location", 0.70, "Gion Matsuri is Kyoto's greatest festival"),
        ("近松門左衛門", "歌舞伎", "creator_work", 0.80, "Chikamatsu wrote for kabuki and bunraku"),
        ("葛飾北斎", "すみだ北斎美術館", "heritage_location", 0.75, "Museum dedicated to Hokusai"),
        ("東大寺", "奈良", "heritage_location", 0.60, "Todai-ji is a Nara landmark"),
        ("伏見稲荷大社", "京都", "heritage_location", 0.60, "Fushimi Inari is in Kyoto"),
        ("茶道", "侘び寂び", "thematic_resonance", 0.80, "Tea ceremony embodies wabi-sabi aesthetics"),
        ("能", "侘び寂び", "thematic_resonance", 0.75, "Noh embodies wabi-sabi aesthetics"),
        ("有田焼", "備前焼", "shared_genre", 0.55, "Both are Japanese pottery traditions"),
        ("有田焼", "九谷焼", "shared_genre", 0.55, "Both are Japanese pottery traditions"),
        ("京友禅", "西陣織", "geographic_cultural", 0.60, "Both are Kyoto textile traditions"),
        ("浮世絵", "能", "era_bridge", 0.50, "Both are classical Japanese art forms"),
        ("葛飾北斎", "歌川広重", "shared_genre", 0.80, "Contemporary ukiyo-e masters"),
        ("鬼滅の刃", "進撃の巨人", "temporal_echo", 0.45, "Both are popular modern anime"),
        ("京都国立博物館", "京都", "heritage_location", 0.55, "Museum is in Kyoto"),
        ("東京国立博物館", "浅草寺", "geographic_cultural", 0.40, "Both in Ueno/Asakusa area"),
    ]

    for label_a, label_b, ctype, score, explanation in manual:
        id_a = entity_map.get(label_a)
        id_b = entity_map.get(label_b)
        if id_a and id_b:
            connections.append((
                id_a, id_b, ctype,
                round(random.uniform(0.0, 0.5), 2),  # theme_distance
                round(random.uniform(0.0, 0.5), 2),  # era_distance
                round(random.uniform(0.0, 0.5), 2),  # medium_distance
                round(random.uniform(0.0, 0.5), 2),  # geography_distance
                round(random.uniform(0.0, 0.5), 2),  # experience_distance
                score, explanation, "test", 0.9, "keep",
            ))

    # Add some random connections to reach ~50
    entity_ids = list(entity_map.values())
    while len(connections) < 50:
        a = random.choice(entity_ids)
        b = random.choice(entity_ids)
        if a != b:
            ctype = random.choice(connection_types)
            score = round(random.uniform(0.3, 0.9), 2)
            connections.append((
                a, b, ctype,
                round(random.uniform(0.0, 0.8), 2),
                round(random.uniform(0.0, 0.8), 2),
                round(random.uniform(0.0, 0.8), 2),
                round(random.uniform(0.0, 0.8), 2),
                round(random.uniform(0.0, 0.8), 2),
                score, f"Auto-generated {ctype} connection", "test", 0.7, "keep",
            ))

    cur.executemany("""
        INSERT INTO connections
        (entity_a_id, entity_b_id, connection_type, theme_distance, era_distance,
         medium_distance, geography_distance, experience_distance, serendipity_score,
         explanation, source, confidence, llm_verdict)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, connections)

    # FTS5 and R-Tree are auto-populated by triggers on entity INSERT

    conn.commit()

    # ── Summary ────────────────────────────────────────────
    entity_count = cur.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conn_count = cur.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    tag_count = cur.execute("SELECT COUNT(*) FROM entity_tags").fetchone()[0]
    fts_count = cur.execute("SELECT COUNT(*) FROM entities_fts").fetchone()[0]
    rtree_count = cur.execute("SELECT COUNT(*) FROM entities_rtree").fetchone()[0]

    conn.close()

    print(f"Test database created: {db_path}")
    print(f"  Entities:    {entity_count}")
    print(f"  Connections: {conn_count}")
    print(f"  Tags:        {tag_count}")
    print(f"  FTS5 rows:   {fts_count}")
    print(f"  R-Tree rows: {rtree_count}")


def main():
    parser = argparse.ArgumentParser(description="Create test database for CI")
    parser.add_argument(
        "--output", "-o",
        default=os.environ.get("TEST_DB_PATH", "/tmp/test_culture_ontology.db"),
        help="Output database path",
    )
    args = parser.parse_args()
    create_test_db(args.output)


if __name__ == "__main__":
    main()
