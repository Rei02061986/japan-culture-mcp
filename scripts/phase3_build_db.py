"""Phase 3: Culture Ontology SQLite Database Builder
Reads taxonomy JSONs and builds the ontology database with all tables and seed data.
Python 3.8 compatible.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ONTOLOGY_DIR = BASE_DIR / "ontology"
RESP_DIR = BASE_DIR / "responses" / "phase2_5"
DB_PATH = ONTOLOGY_DIR / "culture_ontology.db"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def create_schema(conn: sqlite3.Connection):
    """Create all tables"""
    conn.executescript("""
    -- ==========================================
    -- Culture Ontology Database Schema v0.1
    -- ==========================================

    -- 5軸の定義
    CREATE TABLE IF NOT EXISTS axes (
        id TEXT PRIMARY KEY,
        name_ja TEXT NOT NULL,
        name_en TEXT NOT NULL,
        description TEXT
    );

    -- テーマ軸の値
    CREATE TABLE IF NOT EXISTS theme_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name_ja TEXT NOT NULL,
        name_en TEXT NOT NULL,
        parent_code TEXT,
        description TEXT,
        FOREIGN KEY (parent_code) REFERENCES theme_values(code)
    );

    -- 時代軸の値
    CREATE TABLE IF NOT EXISTS era_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name_ja TEXT NOT NULL,
        name_en TEXT NOT NULL,
        year_from INTEGER,
        year_to INTEGER,
        description TEXT
    );

    -- 媒体軸の値
    CREATE TABLE IF NOT EXISTS medium_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name_ja TEXT NOT NULL,
        name_en TEXT NOT NULL,
        parent_code TEXT,
        description TEXT,
        FOREIGN KEY (parent_code) REFERENCES medium_values(code)
    );

    -- 地理軸の値
    CREATE TABLE IF NOT EXISTS geography_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name_ja TEXT NOT NULL,
        name_en TEXT NOT NULL,
        parent_code TEXT,
        lat REAL,
        lon REAL,
        level TEXT,
        FOREIGN KEY (parent_code) REFERENCES geography_values(code)
    );

    -- 体験モード軸の値
    CREATE TABLE IF NOT EXISTS experience_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name_ja TEXT NOT NULL,
        name_en TEXT NOT NULL,
        description TEXT
    );

    -- ==========================================
    -- ソース分類 → 独自軸のマッピング
    -- ==========================================

    CREATE TABLE IF NOT EXISTS anilist_tag_mapping (
        anilist_tag_id INTEGER,
        anilist_tag_name TEXT NOT NULL,
        anilist_category TEXT NOT NULL,
        axis TEXT NOT NULL,
        axis_value_code TEXT,
        confidence REAL DEFAULT 1.0,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS madb_class_mapping (
        madb_class_uri TEXT NOT NULL,
        madb_class_name TEXT NOT NULL,
        medium_value_code TEXT NOT NULL,
        notes TEXT,
        FOREIGN KEY (medium_value_code) REFERENCES medium_values(code)
    );

    -- ==========================================
    -- エンティティ（Wikidata IDハブ）
    -- ==========================================

    CREATE TABLE IF NOT EXISTS entities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wikidata_id TEXT UNIQUE,
        label_ja TEXT NOT NULL,
        label_en TEXT,
        entity_type TEXT NOT NULL,
        madb_id TEXT,
        ndl_id TEXT,
        anilist_id INTEGER,
        dbpedia_uri TEXT,
        lat REAL,
        lon REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_entities_wikidata ON entities(wikidata_id);
    CREATE INDEX IF NOT EXISTS idx_entities_madb ON entities(madb_id);
    CREATE INDEX IF NOT EXISTS idx_entities_ndl ON entities(ndl_id);
    CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);

    -- エンティティへの5軸タグ付け
    CREATE TABLE IF NOT EXISTS entity_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_id INTEGER NOT NULL,
        axis TEXT NOT NULL,
        value_code TEXT NOT NULL,
        source TEXT NOT NULL,
        confidence REAL DEFAULT 1.0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (entity_id) REFERENCES entities(id)
    );

    CREATE INDEX IF NOT EXISTS idx_entity_tags_entity ON entity_tags(entity_id);
    CREATE INDEX IF NOT EXISTS idx_entity_tags_axis ON entity_tags(axis, value_code);

    -- ==========================================
    -- 接続グラフ（セレンディピティエンジン用）
    -- ==========================================

    CREATE TABLE IF NOT EXISTS connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_a_id INTEGER NOT NULL,
        entity_b_id INTEGER NOT NULL,
        connection_type TEXT NOT NULL,
        theme_distance REAL,
        era_distance REAL,
        medium_distance REAL,
        geography_distance REAL,
        experience_distance REAL,
        serendipity_score REAL,
        explanation TEXT,
        source TEXT,
        confidence REAL DEFAULT 1.0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (entity_a_id) REFERENCES entities(id),
        FOREIGN KEY (entity_b_id) REFERENCES entities(id)
    );

    CREATE INDEX IF NOT EXISTS idx_connections_a ON connections(entity_a_id);
    CREATE INDEX IF NOT EXISTS idx_connections_b ON connections(entity_b_id);
    CREATE INDEX IF NOT EXISTS idx_connections_serendipity ON connections(serendipity_score DESC);

    -- 接続文法ルール
    CREATE TABLE IF NOT EXISTS connection_grammar (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        theme_min REAL, theme_max REAL,
        era_min REAL, era_max REAL,
        medium_min REAL, medium_max REAL,
        geography_min REAL, geography_max REAL,
        experience_min REAL, experience_max REAL,
        quality_label TEXT,
        weight REAL DEFAULT 1.0
    );
    """)


def seed_axes(conn: sqlite3.Connection):
    """Insert 5-axis definitions"""
    axes = [
        ('theme', 'テーマ', 'Theme', '死と再生、変容、異界、日常の美など文化的大テーマ'),
        ('era', '時代', 'Era', '古代〜令和の9区分'),
        ('medium', '媒体', 'Medium', '絵画、文学、アニメ、ゲーム、祭礼など'),
        ('geography', '地理', 'Geography', '地方→都道府県→市区町村→スポット'),
        ('experience', '体験モード', 'Experience Mode', '知的探索、美的鑑賞、身体的体験など'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO axes VALUES (?, ?, ?, ?)", axes
    )


def seed_era_values(conn: sqlite3.Connection):
    """Insert era axis values"""
    eras = [
        ('ancient', '古代', 'Ancient', -10000, 1185, '縄文・弥生・古墳・飛鳥・奈良・平安前期'),
        ('medieval', '中世', 'Medieval', 1185, 1573, '鎌倉・室町・戦国'),
        ('edo_early', '近世前期', 'Early Edo', 1573, 1700, '安土桃山・江戸前期'),
        ('edo_late', '近世後期', 'Late Edo', 1700, 1868, '江戸中期・後期・幕末'),
        ('meiji_taisho', '明治大正', 'Meiji-Taisho', 1868, 1926, '近代化・大正デモクラシー'),
        ('showa_prewar', '昭和戦前', 'Showa Pre-war', 1926, 1945, '軍国主義・戦時'),
        ('showa_postwar', '昭和戦後', 'Showa Post-war', 1945, 1989, '復興・高度成長・バブル'),
        ('heisei', '平成', 'Heisei', 1989, 2019, 'デジタル化・クールジャパン'),
        ('reiwa', '令和', 'Reiwa', 2019, 2100, '現代'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO era_values (code, name_ja, name_en, year_from, year_to, description) VALUES (?, ?, ?, ?, ?, ?)",
        eras,
    )


def seed_medium_values(conn: sqlite3.Connection):
    """Insert medium axis values (hierarchical)"""
    media = [
        # Top-level categories
        ('painting', '絵画', 'Painting', None, '絵巻物・屏風絵・掛軸等'),
        ('ukiyoe', '浮世絵', 'Ukiyo-e', 'painting', '木版画・肉筆浮世絵'),
        ('sculpture', '彫刻', 'Sculpture', None, '仏像・木彫・石彫'),
        ('architecture', '建築', 'Architecture', None, '城郭・寺社・庭園'),
        ('literature', '文学', 'Literature', None, '古典文学・近現代文学'),
        ('classical_text', '古典籍', 'Classical Text', 'literature', '写本・版本・草双紙'),
        ('theater', '演劇', 'Theater', None, '日本の伝統芸能・現代演劇'),
        ('kabuki', '歌舞伎', 'Kabuki', 'theater', None),
        ('noh', '能', 'Noh', 'theater', None),
        ('bunraku', '文楽', 'Bunraku', 'theater', '人形浄瑠璃'),
        ('manga', '漫画', 'Manga', None, '日本の漫画全般'),
        ('manga_book', '漫画（単行本）', 'Manga (Tankōbon)', 'manga', None),
        ('manga_series', '漫画（シリーズ）', 'Manga (Series)', 'manga', None),
        ('manga_magazine', '漫画雑誌', 'Manga Magazine', 'manga', None),
        ('manga_magazine_issue', '漫画雑誌号', 'Manga Magazine Issue', 'manga', None),
        ('anime', 'アニメ', 'Anime', None, '日本のアニメーション全般'),
        ('anime_tv', 'TVアニメ', 'TV Anime', 'anime', None),
        ('anime_tv_series', 'TVアニメシリーズ', 'TV Anime Series', 'anime', None),
        ('anime_movie', 'アニメ映画', 'Anime Movie', 'anime', None),
        ('anime_ova', 'OVA/ONA', 'OVA/ONA', 'anime', 'DVD/BD/配信向けアニメ'),
        ('anime_video_package', '映像パッケージ', 'Video Package', 'anime', 'DVD/BD等'),
        ('game', 'ゲーム', 'Game', None, 'ビデオゲーム全般'),
        ('game_console', 'コンソールゲーム', 'Console Game', 'game', None),
        ('game_mobile', 'モバイルゲーム', 'Mobile Game', 'game', None),
        ('game_pc', 'PCゲーム', 'PC Game', 'game', None),
        ('music', '音楽', 'Music', None, None),
        ('festival', '祭礼', 'Festival', None, '祭り・年中行事'),
        ('craft', '工芸', 'Craft', None, '陶磁器・漆器・織物等'),
        ('media_art', 'メディアアート', 'Media Art', None, '現代美術・インスタレーション'),
        ('light_novel', 'ライトノベル', 'Light Novel', 'literature', None),
        ('tokusatsu', '特撮', 'Tokusatsu', None, '特殊撮影作品'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO medium_values (code, name_ja, name_en, parent_code, description) VALUES (?, ?, ?, ?, ?)",
        media,
    )


def seed_experience_values(conn: sqlite3.Connection):
    """Insert experience mode axis values"""
    experiences = [
        ('intellectual', '知的探索', 'Intellectual Exploration', '歴史的背景、文脈、意味の理解'),
        ('aesthetic', '美的鑑賞', 'Aesthetic Appreciation', '視覚的・聴覚的美の体験'),
        ('physical', '身体的体験', 'Physical Experience', '歩く、作る、食べる、参加する'),
        ('social', '社交', 'Social', '地域の人との交流、祭りへの参加'),
        ('reflective', '内省', 'Reflection', '静寂、瞑想、精神的体験'),
        ('adventure', '冒険', 'Adventure', '未知の場所・体験への挑戦'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO experience_values (code, name_ja, name_en, description) VALUES (?, ?, ?, ?)",
        experiences,
    )


def seed_theme_values(conn: sqlite3.Connection):
    """Insert theme axis values — top-level themes + AniList-derived sub-themes"""
    # Top-level themes (独自定義, CCDMの文化的深さに対応)
    top_themes = [
        ('death_rebirth', '死と再生', 'Death and Rebirth', None, '死・再生・輪廻・転生のモチーフ'),
        ('transformation', '変容', 'Transformation', None, '変身・成長・変化のモチーフ'),
        ('journey_boundary', '旅と境界', 'Journey and Boundaries', None, '旅・境界・越境のモチーフ'),
        ('nature_communion', '自然との交感', 'Communion with Nature', None, '自然への畏敬・共生・季節感'),
        ('power_rebellion', '権力と反逆', 'Power and Rebellion', None, '権力構造・反抗・革命'),
        ('everyday_beauty', '日常の美', 'Beauty in the Everyday', None, '侘寂・もののあはれ・日常の発見'),
        ('otherworld', '異界', 'Otherworld', None, '異世界・あの世・幻想世界'),
        ('war_conflict', '戦争と葛藤', 'War and Conflict', None, '戦争・闘争・内面的葛藤'),
        ('love_bond', '愛と絆', 'Love and Bonds', None, '恋愛・家族愛・友情・絆'),
        ('humor_satire', '笑いと風刺', 'Humor and Satire', None, '滑稽・風刺・パロディ'),
        ('craft_mastery', '技と極み', 'Craft and Mastery', None, '技術の追求・職人気質・道の精神'),
        ('sacred_profane', '聖と俗', 'Sacred and Profane', None, '宗教・信仰・聖俗の境界'),
        ('identity_self', 'アイデンティティ', 'Identity and Self', None, '自己探求・成長・存在意義'),
        ('community_tradition', '共同体と伝統', 'Community and Tradition', None, '地域社会・伝統文化・継承'),
        ('supernatural', '超自然', 'Supernatural', None, '超自然的存在・超能力・怪異'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO theme_values (code, name_ja, name_en, parent_code, description) VALUES (?, ?, ?, ?, ?)",
        top_themes,
    )

    # Sub-themes from AniList tags + Japan culture specifics
    sub_themes = [
        # supernatural children
        ('yokai', '妖怪', 'Yōkai', 'supernatural', None),
        ('ghost_spirit', '幽霊・霊', 'Ghost/Spirit', 'supernatural', None),
        ('demon', '鬼・悪魔', 'Demon', 'supernatural', None),
        ('god_deity', '神', 'God/Deity', 'supernatural', None),
        ('magic', '魔法', 'Magic', 'supernatural', None),
        ('curse', '呪い', 'Curse', 'supernatural', None),
        ('exorcism', '退魔・祓い', 'Exorcism', 'supernatural', None),
        ('kaiju', '怪獣', 'Kaijū', 'supernatural', None),
        ('vampire_werewolf', '吸血鬼・人狼', 'Vampire/Werewolf', 'supernatural', None),
        ('super_power', '超能力', 'Super Power', 'supernatural', None),
        # otherworld children
        ('isekai', '異世界', 'Isekai', 'otherworld', None),
        ('afterlife', 'あの世', 'Afterlife', 'otherworld', None),
        ('virtual_world', '仮想世界', 'Virtual World', 'otherworld', None),
        ('space', '宇宙', 'Space', 'otherworld', None),
        ('post_apocalyptic', '終末後', 'Post-Apocalyptic', 'otherworld', None),
        ('alternate_universe', '別世界線', 'Alternate Universe', 'otherworld', None),
        ('dungeon', 'ダンジョン', 'Dungeon', 'otherworld', None),
        # transformation children
        ('henshin', '変身', 'Henshin', 'transformation', None),
        ('shapeshifting', '変化', 'Shapeshifting', 'transformation', None),
        ('body_swap', '入れ替わり', 'Body Swap', 'transformation', None),
        ('coming_of_age', '成長', 'Coming of Age', 'transformation', None),
        ('reincarnation', '転生', 'Reincarnation', 'transformation', None),
        # death_rebirth children
        ('death_game', 'デスゲーム', 'Death Game', 'death_rebirth', None),
        ('survival', 'サバイバル', 'Survival', 'death_rebirth', None),
        ('tragedy', '悲劇', 'Tragedy', 'death_rebirth', None),
        ('horror', 'ホラー', 'Horror', 'death_rebirth', None),
        # journey_boundary children
        ('travel', '旅', 'Travel', 'journey_boundary', None),
        ('foreign_land', '異国', 'Foreign Land', 'journey_boundary', None),
        ('adventure_quest', '冒険', 'Adventure', 'journey_boundary', None),
        # nature_communion children
        ('agriculture', '農業', 'Agriculture', 'nature_communion', None),
        ('animal', '動物', 'Animals', 'nature_communion', None),
        ('environmental', '環境', 'Environmental', 'nature_communion', None),
        ('fishing_outdoor', '釣り・アウトドア', 'Fishing/Outdoor', 'nature_communion', None),
        # power_rebellion children
        ('politics', '政治', 'Politics', 'power_rebellion', None),
        ('military', '軍事', 'Military', 'power_rebellion', None),
        ('crime', '犯罪', 'Crime', 'power_rebellion', None),
        ('espionage', '諜報', 'Espionage', 'power_rebellion', None),
        ('revenge', '復讐', 'Revenge', 'power_rebellion', None),
        ('class_struggle', '階級闘争', 'Class Struggle', 'power_rebellion', None),
        # everyday_beauty children
        ('food_cuisine', '食・料理', 'Food/Cuisine', 'everyday_beauty', None),
        ('family_life', '家族', 'Family Life', 'everyday_beauty', None),
        ('iyashikei', '癒し系', 'Iyashikei', 'everyday_beauty', None),
        ('fashion', 'ファッション', 'Fashion', 'everyday_beauty', None),
        # war_conflict children
        ('battle', '戦闘', 'Battle', 'war_conflict', None),
        ('martial_arts', '武術・格闘', 'Martial Arts', 'war_conflict', None),
        ('swordplay', '剣術', 'Swordplay', 'war_conflict', None),
        # love_bond children
        ('romance', '恋愛', 'Romance', 'love_bond', None),
        ('found_family', '疑似家族', 'Found Family', 'love_bond', None),
        ('friendship', '友情', 'Friendship', 'love_bond', None),
        # humor_satire children
        ('parody', 'パロディ', 'Parody', 'humor_satire', None),
        ('slapstick', 'スラップスティック', 'Slapstick', 'humor_satire', None),
        ('manzai', '漫才', 'Manzai', 'humor_satire', None),
        ('rakugo', '落語', 'Rakugo', 'humor_satire', None),
        # craft_mastery children
        ('calligraphy', '書道', 'Calligraphy', 'craft_mastery', None),
        ('tea_ceremony', '茶道', 'Tea Ceremony', 'craft_mastery', None),
        ('shogi_go', '将棋・囲碁', 'Shogi/Go', 'craft_mastery', None),
        ('cooking_mastery', '料理道', 'Culinary Mastery', 'craft_mastery', None),
        ('music_performance', '音楽演奏', 'Music Performance', 'craft_mastery', None),
        ('sports', 'スポーツ', 'Sports', 'craft_mastery', None),
        # sacred_profane children
        ('mythology', '神話', 'Mythology', 'sacred_profane', None),
        ('religion', '宗教', 'Religion', 'sacred_profane', None),
        ('shrine_temple', '神社仏閣', 'Shrine/Temple', 'sacred_profane', None),
        # identity_self children
        ('gender_identity', 'ジェンダー', 'Gender Identity', 'identity_self', None),
        ('otaku_culture', 'オタク文化', 'Otaku Culture', 'identity_self', None),
        ('hikikomori', 'ひきこもり', 'Hikikomori', 'identity_self', None),
        # community_tradition children
        ('samurai', '武士・侍', 'Samurai', 'community_tradition', None),
        ('ninja', '忍者', 'Ninja', 'community_tradition', None),
        ('matsuri', '祭り', 'Festival/Matsuri', 'community_tradition', None),
        ('yakuza', 'ヤクザ', 'Yakuza', 'community_tradition', None),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO theme_values (code, name_ja, name_en, parent_code, description) VALUES (?, ?, ?, ?, ?)",
        sub_themes,
    )


def seed_geography_values(conn: sqlite3.Connection):
    """Insert geography axis values — regions and major prefectures"""
    regions = [
        # Regions
        ('hokkaido', '北海道', 'Hokkaido', None, 43.0642, 141.3469, 'region'),
        ('tohoku', '東北', 'Tohoku', None, 38.2682, 140.8694, 'region'),
        ('kanto', '関東', 'Kanto', None, 35.6895, 139.6917, 'region'),
        ('chubu', '中部', 'Chubu', None, 36.6513, 138.1810, 'region'),
        ('kinki', '近畿', 'Kinki', None, 34.6851, 135.8050, 'region'),
        ('chugoku', '中国', 'Chugoku', None, 34.3853, 132.4553, 'region'),
        ('shikoku', '四国', 'Shikoku', None, 33.5597, 133.5311, 'region'),
        ('kyushu', '九州・沖縄', 'Kyushu/Okinawa', None, 33.5902, 130.4017, 'region'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO geography_values (code, name_ja, name_en, parent_code, lat, lon, level) VALUES (?, ?, ?, ?, ?, ?, ?)",
        regions,
    )

    # Key cultural prefectures/cities
    places = [
        ('tokyo', '東京都', 'Tokyo', 'kanto', 35.6895, 139.6917, 'prefecture'),
        ('kyoto', '京都府', 'Kyoto', 'kinki', 35.0116, 135.7681, 'prefecture'),
        ('nara', '奈良県', 'Nara', 'kinki', 34.6851, 135.8050, 'prefecture'),
        ('osaka', '大阪府', 'Osaka', 'kinki', 34.6937, 135.5023, 'prefecture'),
        ('kamakura', '鎌倉', 'Kamakura', 'kanto', 35.3192, 139.5467, 'city'),
        ('nikko', '日光', 'Nikko', 'kanto', 36.7500, 139.5986, 'city'),
        ('asakusa', '浅草', 'Asakusa', 'tokyo', 35.7148, 139.7967, 'spot'),
        ('akihabara', '秋葉原', 'Akihabara', 'tokyo', 35.7023, 139.7745, 'spot'),
        ('gion', '祇園', 'Gion', 'kyoto', 35.0037, 135.7756, 'spot'),
        ('fushimi_inari', '伏見稲荷', 'Fushimi Inari', 'kyoto', 34.9671, 135.7727, 'spot'),
        ('hiroshima', '広島県', 'Hiroshima', 'chugoku', 34.3853, 132.4553, 'prefecture'),
        ('kanagawa', '神奈川県', 'Kanagawa', 'kanto', 35.4478, 139.6425, 'prefecture'),
        ('hokkaido_pref', '北海道', 'Hokkaido', 'hokkaido', 43.0642, 141.3469, 'prefecture'),
        ('okinawa', '沖縄県', 'Okinawa', 'kyushu', 26.3344, 127.8056, 'prefecture'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO geography_values (code, name_ja, name_en, parent_code, lat, lon, level) VALUES (?, ?, ?, ?, ?, ?, ?)",
        places,
    )


def seed_connection_grammar(conn: sqlite3.Connection):
    """Insert initial connection grammar rules"""
    rules = [
        ('good_surprise_classic', 'テーマ近接×時代遠距離×媒体異種 = 良い意外性',
         0.0, 0.3, 0.5, 1.0, 0.5, 1.0, None, None, None, None, 'good_surprise', 1.0),
        ('obvious', '全軸近接 = 当たり前',
         0.0, 0.2, 0.0, 0.2, 0.0, 0.2, None, None, None, None, 'obvious', 0.3),
        ('random', '全軸遠距離 = 恣意的',
         0.7, 1.0, 0.7, 1.0, 0.7, 1.0, None, None, None, None, 'bad_surprise', 0.1),
        ('era_bridge', 'テーマ同一×時代を橋渡し = 教育的',
         0.0, 0.1, 0.3, 0.8, 0.0, 1.0, None, None, None, None, 'good_surprise', 1.2),
        ('medium_cross', '同テーマ・同時代×媒体横断 = 文化比較',
         0.0, 0.2, 0.0, 0.3, 0.5, 1.0, None, None, None, None, 'good_surprise', 1.1),
        ('geo_theme', '同テーマ×地理遠距離 = 文化伝播',
         0.0, 0.2, 0.0, 0.5, 0.0, 0.5, 0.5, 1.0, None, None, 'good_surprise', 1.0),
        ('experience_shift', '同テーマ×体験モード変化 = 多面的理解',
         0.0, 0.2, 0.0, 0.3, 0.0, 0.3, 0.0, 0.3, 0.5, 1.0, 'good_surprise', 1.0),
    ]
    conn.executemany(
        """INSERT OR IGNORE INTO connection_grammar
        (name, description, theme_min, theme_max, era_min, era_max,
         medium_min, medium_max, geography_min, geography_max,
         experience_min, experience_max, quality_label, weight)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rules,
    )


def seed_anilist_tag_mapping(conn: sqlite3.Connection):
    """Map AniList tags to ontology axes using taxonomy data"""
    # Load AniList taxonomy
    taxonomy_path = ONTOLOGY_DIR / "anilist_taxonomy.json"
    if not taxonomy_path.exists():
        print("  [SKIP] anilist_taxonomy.json not found")
        return

    with open(taxonomy_path, encoding="utf-8") as f:
        taxonomy = json.load(f)

    # Load full tags for IDs
    tags_path = RESP_DIR / "anilist_tags_full.json"
    tag_id_map = {}
    if tags_path.exists():
        with open(tags_path, encoding="utf-8") as f:
            tags_data = json.load(f)
        for tag in tags_data.get("data", {}).get("MediaTagCollection", []):
            tag_id_map[tag["name"]] = tag["id"]

    # Category → axis mapping
    cat_axis = taxonomy.get("category_to_axis_mapping", {})

    # Map each tag to appropriate axis + axis_value_code
    # Tag → theme_value mapping (best effort)
    tag_to_theme = {
        # Theme-Fantasy
        "Youkai": "yokai", "Isekai": "isekai", "Magic": "magic",
        "Kaiju": "kaiju", "Mythology": "mythology", "Exorcism": "exorcism",
        "Curses": "curse", "Henshin": "henshin", "Shapeshifting": "shapeshifting",
        "Body Swapping": "body_swap", "Necromancy": "ghost_spirit",
        "Super Power": "super_power", "Superhero": "super_power",
        "Reverse Isekai": "isekai", "Fairy Tale": "mythology",
        "Cultivation": "craft_mastery", "Alchemy": "magic",
        "Steampunk": "otherworld", "Wuxia": "martial_arts",
        # Setting-Universe
        "Afterlife": "afterlife", "Alternate Universe": "alternate_universe",
        "Virtual World": "virtual_world", "Space": "space",
        "Post-Apocalyptic": "post_apocalyptic", "Urban Fantasy": "supernatural",
        "Augmented Reality": "virtual_world", "Omegaverse": "alternate_universe",
        # Theme-Romance
        "Love Triangle": "romance", "Unrequited Love": "romance",
        "Boys' Love": "romance", "Yuri": "romance",
        "Cohabitation": "romance", "Matchmaking": "romance",
        "Age Gap": "romance", "Heterosexual": "romance",
        "Female Harem": "romance", "Male Harem": "romance",
        "Mixed Gender Harem": "romance", "Polyamorous": "romance",
        "Teens' Love": "romance",
        # Cast-Traits → theme
        "Ninja": "ninja", "Samurai": "samurai",
        "Ghost": "ghost_spirit", "Demons": "demon", "Gods": "god_deity",
        "Angels": "god_deity", "Vampire": "vampire_werewolf",
        "Werewolf": "vampire_werewolf", "Zombie": "ghost_spirit",
        "Witch": "magic", "Dragons": "kaiju",
        "Robots": "super_power", "Cyborg": "super_power",
        "Aliens": "otherworld", "Detective": "crime",
        "Pirates": "adventure_quest", "Cowboys": "journey_boundary",
        "Shrine Maiden": "sacred_profane", "Oiran": "community_tradition",
        "Idol": "music_performance", "Hikikomori": "hikikomori",
        "Delinquents": "power_rebellion",
        # Theme-Action
        "Martial Arts": "martial_arts", "Swordplay": "swordplay",
        "Battle Royale": "battle", "Espionage": "espionage",
        "Archery": "martial_arts", "Guns": "battle",
        # Theme-Other
        "War": "war_conflict", "Crime": "crime", "Travel": "travel",
        "Survival": "survival", "Reincarnation": "reincarnation",
        "Religion": "religion", "Philosophy": "identity_self",
        "Politics": "politics", "Animals": "animal",
        "Environmental": "environmental", "Food": "food_cuisine",
        "Death Game": "death_game", "Gore": "horror",
        "Body Horror": "horror", "Cosmic Horror": "horror",
        "Gambling": "craft_mastery", "Economics": "power_rebellion",
        "Marriage": "love_bond", "Found Family": "found_family",
        "Otaku Culture": "otaku_culture", "Rescue": "adventure_quest",
        "Lost Civilization": "otherworld", "Pandemic": "death_rebirth",
        "Medicine": "craft_mastery", "Slavery": "power_rebellion",
        "Memory Manipulation": "supernatural",
        "Gender Bending": "gender_identity",
        "LGBTQ+ Themes": "gender_identity",
        "Noir": "crime", "Terrorism": "war_conflict",
        "Blackmail": "crime", "Royal Affairs": "power_rebellion",
        "Mountaineering": "adventure_quest",
        "Software Development": "craft_mastery",
        # Theme-Drama
        "Coming of Age": "coming_of_age", "Revenge": "revenge",
        "Tragedy": "tragedy", "Bullying": "identity_self",
        "Class Struggle": "class_struggle", "Conspiracy": "espionage",
        "Suicide": "death_rebirth", "Rehabilitation": "transformation",
        "Kingdom Management": "power_rebellion",
        # Theme-Slice of Life
        "Agriculture": "agriculture", "Family Life": "family_life",
        "Iyashikei": "iyashikei", "Parenthood": "family_life",
        "Horticulture": "nature_communion",
        # Theme-Arts
        "Kabuki": "craft_mastery", "Rakugo": "rakugo",
        "Calligraphy": "calligraphy", "Food": "food_cuisine",
        "Fashion": "fashion", "Photography": "aesthetic",
        "Classic Literature": "craft_mastery", "Writing": "craft_mastery",
        "Acting": "craft_mastery", "Ballet": "craft_mastery",
        "Manzai": "manzai", "Drawing": "craft_mastery",
        # Theme-Arts-Music
        "Band": "music_performance", "Classical Music": "music_performance",
        "Dancing": "music_performance", "Rock Music": "music_performance",
        "Jazz Music": "music_performance", "Musical Theater": "music_performance",
        # Theme-Comedy
        "Parody": "parody", "Satire": "humor_satire",
        "Slapstick": "slapstick", "Surreal Comedy": "humor_satire",
        # Theme-Game-Card & Board Game
        "Shogi": "shogi_go", "Go": "shogi_go", "Karuta": "craft_mastery",
        "Mahjong": "craft_mastery",
        # Theme-Game-Sport → sports
        "Sumo": "sports", "Judo": "sports", "Baseball": "sports",
        "Basketball": "sports", "Football": "sports", "Tennis": "sports",
        "Boxing": "sports", "Swimming": "sports", "Volleyball": "sports",
        "Wrestling": "martial_arts", "Fencing": "swordplay",
        "Cycling": "sports", "Rugby": "sports", "Fishing": "fishing_outdoor",
        # Theme-Sci-Fi
        "Cyberpunk": "otherworld", "Space Opera": "space",
        "Time Loop": "transformation", "Time Manipulation": "transformation",
        "Tokusatsu": "supernatural",
        # Theme-Sci-Fi-Mecha
        "Real Robot": "battle", "Super Robot": "battle",
        # Theme-Other-Organisations
        "Yakuza": "yakuza", "Military": "military", "Police": "crime",
        "Assassins": "war_conflict", "Gangs": "crime",
        "Criminal Organization": "crime", "Mafia": "crime",
        # Theme-Other-Vehicle
        "Trains": "journey_boundary", "Ships": "journey_boundary",
        "Aviation": "journey_boundary",
        # Setting-Scene → geography/experience
        "Rural": "nature_communion", "Urban": "everyday_beauty",
        "School": "identity_self", "Camping": "fishing_outdoor",
        "Wilderness": "nature_communion",
    }

    # Axis resolution for categories
    axis_map = {
        "テーマ": "theme",
        "テーマ/体験": "theme",
        "時代": "era",
        "媒体": "medium",
        "地理/体験": "geography",
        "メタ（軸外）": "meta",
    }

    rows = []
    for cat_name, cat_data in taxonomy.get("tag_categories", {}).items():
        tags = cat_data.get("tags", [])
        axis_ja = cat_data.get("ontology_axis", "テーマ")
        axis = axis_map.get(axis_ja, "theme")

        for tag_name in tags:
            tag_id = tag_id_map.get(tag_name, 0)
            value_code = tag_to_theme.get(tag_name)
            confidence = 0.9 if value_code else 0.5
            rows.append((tag_id, tag_name, cat_name, axis, value_code, confidence, None))

    conn.executemany(
        "INSERT OR IGNORE INTO anilist_tag_mapping VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    print(f"  AniList tag mappings: {len(rows)} inserted")
    mapped = sum(1 for r in rows if r[4] is not None)
    print(f"  Mapped to theme_values: {mapped}/{len(rows)} ({100*mapped//len(rows)}%)")


def seed_madb_class_mapping(conn: sqlite3.Connection):
    """Map MADB classes to medium axis values"""
    BASE_URI = "https://mediaarts-db.artmuseums.go.jp/data/class#"
    mappings = [
        (f"{BASE_URI}MangaBook", "MangaBook", "manga_book", None),
        (f"{BASE_URI}MangaBookSeries", "MangaBookSeries", "manga_series", None),
        (f"{BASE_URI}MangaMagazineIssue", "MangaMagazineIssue", "manga_magazine_issue", None),
        (f"{BASE_URI}MangaMagazinePublication", "MangaMagazinePublication", "manga_magazine", "雑誌連載"),
        (f"{BASE_URI}MangaMagazine", "MangaMagazine", "manga_magazine", "雑誌タイトル"),
        (f"{BASE_URI}MangaOther", "MangaOther", "manga", "その他漫画"),
        (f"{BASE_URI}AnimationTVProgram", "AnimationTVProgram", "anime_tv", "エピソード単位"),
        (f"{BASE_URI}AnimationTVRegularSeries", "AnimationTVRegularSeries", "anime_tv_series", None),
        (f"{BASE_URI}AnimationTVSpecialSeries", "AnimationTVSpecialSeries", "anime_tv_series", "TVスペシャル"),
        (f"{BASE_URI}AnimationVideoPackage", "AnimationVideoPackage", "anime_video_package", "DVD/BD/VHS"),
        (f"{BASE_URI}AnimationVideoPackageSeries", "AnimationVideoPackageSeries", "anime_ova", "シリーズ"),
        (f"{BASE_URI}AnimationMovie", "AnimationMovie", "anime_movie", None),
        (f"{BASE_URI}AnimationMovieSeries", "AnimationMovieSeries", "anime_movie", "シリーズ"),
        (f"{BASE_URI}AnimationRelatedItem", "AnimationRelatedItem", "anime", "関連アイテム"),
        (f"{BASE_URI}GamePackage", "GamePackage", "game_console", None),
        (f"{BASE_URI}GameWork", "GameWork", "game", "作品レベル"),
        (f"{BASE_URI}GameVariation", "GameVariation", "game_console", "バリエーション"),
        (f"{BASE_URI}GameRelatedItem", "GameRelatedItem", "game", "関連アイテム"),
        (f"{BASE_URI}MediaArtExhibitionOrPerformance", "MediaArtExhibitionOrPerformance", "media_art", "展示/パフォーマンス"),
        (f"{BASE_URI}MediaArtEvent", "MediaArtEvent", "media_art", "イベント"),
        (f"{BASE_URI}MediaArtRelatedItem", "MediaArtRelatedItem", "media_art", "関連アイテム"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO madb_class_mapping VALUES (?, ?, ?, ?)",
        mappings,
    )
    print(f"  MADB class mappings: {len(mappings)} inserted")


def load_wikidata_mappings(conn: sqlite3.Connection):
    """Load Wikidata ID mappings if available"""
    mapping_path = ONTOLOGY_DIR / "wikidata_id_mapping.json"
    if not mapping_path.exists():
        print("  [SKIP] wikidata_id_mapping.json not yet available")
        return 0

    with open(mapping_path, encoding="utf-8") as f:
        data = json.load(f)

    mappings = data.get("mappings", [])
    count = 0
    for m in mappings:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO entities
                (wikidata_id, label_ja, label_en, entity_type, madb_id, ndl_id, dbpedia_uri, lat, lon)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    m.get("wikidata_id"),
                    m.get("label_ja", ""),
                    m.get("label_en"),
                    m.get("entity_type", "work"),
                    m.get("madb_id"),
                    m.get("ndl_id"),
                    m.get("dbpedia_uri"),
                    m.get("lat"),
                    m.get("lon"),
                ),
            )
            count += 1
        except Exception:
            pass
    print(f"  Wikidata entities loaded: {count}")
    return count


def main():
    print("=" * 60)
    print("Phase 3: Building Culture Ontology Database")
    print(f"Started: {now_iso()}")
    print(f"Output: {DB_PATH}")
    print("=" * 60)

    # Remove existing DB for clean rebuild
    if DB_PATH.exists():
        DB_PATH.unlink()
        print("  Removed existing DB")

    conn = sqlite3.connect(str(DB_PATH))

    print("\n[1/8] Creating schema...")
    create_schema(conn)
    conn.commit()

    print("[2/8] Seeding axes...")
    seed_axes(conn)
    conn.commit()

    print("[3/8] Seeding era values...")
    seed_era_values(conn)
    conn.commit()

    print("[4/8] Seeding medium values...")
    seed_medium_values(conn)
    conn.commit()

    print("[5/8] Seeding experience values...")
    seed_experience_values(conn)
    conn.commit()

    print("[6/8] Seeding theme values...")
    seed_theme_values(conn)
    conn.commit()

    print("[7/8] Seeding geography values...")
    seed_geography_values(conn)
    conn.commit()

    print("[8/8] Seeding connection grammar...")
    seed_connection_grammar(conn)
    conn.commit()

    print("\n--- Source Mappings ---")
    seed_anilist_tag_mapping(conn)
    conn.commit()

    seed_madb_class_mapping(conn)
    conn.commit()

    print("\n--- Wikidata Entities ---")
    load_wikidata_mappings(conn)
    conn.commit()

    # Summary
    print("\n" + "=" * 60)
    print("Database Summary")
    print("=" * 60)

    tables = [
        "axes", "theme_values", "era_values", "medium_values",
        "geography_values", "experience_values",
        "anilist_tag_mapping", "madb_class_mapping",
        "entities", "entity_tags", "connections", "connection_grammar",
    ]
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")
        except Exception:
            print(f"  {table}: [error]")

    conn.close()
    print(f"\nDatabase saved: {DB_PATH}")
    print(f"Size: {DB_PATH.stat().st_size:,} bytes")
    print(f"Completed: {now_iso()}")


if __name__ == "__main__":
    main()
