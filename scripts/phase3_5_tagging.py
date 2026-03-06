"""Phase 3.5: Entity Tagging + Connection Graph Builder
Tags all 1,197 entities with 5-axis values and builds the connection graph.
Python 3.8+ compatible (runs locally, uses requests for API calls).
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

BASE_DIR = Path(__file__).parent.parent
ONTOLOGY_DIR = BASE_DIR / "ontology"
DB_PATH = ONTOLOGY_DIR / "culture_ontology.db"
MAPPING_PATH = ONTOLOGY_DIR / "wikidata_id_mapping.json"

WIKIDATA_UA = "japan-culture-mcp/0.2 (teddykmk@gmail.com)"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ================================================================
# Occupation → Axis Mappings
# ================================================================

OCCUPATION_TO_MEDIUM = {
    "浮世絵師": "ukiyoe",
    "日本画家": "painting",
    "画家": "painting",
    "版画家": "ukiyoe",
    "彫刻家": "sculpture",
    "陶芸家": "craft",
    "書家": "craft",
    "建築家": "architecture",
    "漫画家": "manga",
    "小説家": "literature",
    "作家": "literature",
    "著作家": "literature",
    "脚本家": "literature",
    "詩人": "literature",
    "俳人": "literature",
    "歌人": "literature",
    "アニメーター": "anime",
    "アニメ監督": "anime",
    "映画監督": "anime_movie",
    "声優": "anime",
    "作曲家": "music",
    "音楽家": "music",
    "歌手": "music",
    "歌舞伎役者": "kabuki",
    "能楽師": "noh",
    "俳優": "theater",
    "ゲームクリエイター": "game",
    "写真家": "painting",
}

OCCUPATION_TO_THEME = {
    "浮世絵師": ["everyday_beauty", "craft_mastery"],
    "日本画家": ["craft_mastery"],
    "画家": ["craft_mastery"],
    "版画家": ["craft_mastery"],
    "彫刻家": ["craft_mastery"],
    "陶芸家": ["craft_mastery"],
    "書家": ["craft_mastery", "calligraphy"],
    "漫画家": ["craft_mastery"],
    "小説家": ["craft_mastery"],
    "作家": ["craft_mastery"],
    "著作家": ["craft_mastery"],
    "詩人": ["craft_mastery", "everyday_beauty"],
    "俳人": ["nature_communion", "everyday_beauty"],
    "歌人": ["love_bond", "nature_communion"],
    "作曲家": ["craft_mastery"],
    "歌舞伎役者": ["community_tradition", "craft_mastery"],
    "能楽師": ["sacred_profane", "craft_mastery"],
}

OCCUPATION_TO_EXPERIENCE = {
    "浮世絵師": "aesthetic",
    "日本画家": "aesthetic",
    "画家": "aesthetic",
    "彫刻家": "aesthetic",
    "漫画家": "intellectual",
    "小説家": "intellectual",
    "作家": "intellectual",
    "著作家": "intellectual",
    "詩人": "reflective",
    "俳人": "reflective",
    "歌人": "reflective",
    "作曲家": "aesthetic",
    "音楽家": "aesthetic",
    "歌手": "aesthetic",
    "歌舞伎役者": "aesthetic",
    "能楽師": "reflective",
    "映画監督": "intellectual",
    "アニメ監督": "intellectual",
    "声優": "aesthetic",
    "写真家": "aesthetic",
}

# ================================================================
# Region bounds for geography tagging
# ================================================================

REGION_BOUNDS = {
    "hokkaido": {"lat_min": 41.3, "lat_max": 45.6, "lon_min": 139.3, "lon_max": 145.8},
    "tohoku": {"lat_min": 37.7, "lat_max": 41.5, "lon_min": 139.0, "lon_max": 141.7},
    "kanto": {"lat_min": 35.0, "lat_max": 37.0, "lon_min": 138.5, "lon_max": 140.9},
    "chubu": {"lat_min": 34.5, "lat_max": 37.8, "lon_min": 136.0, "lon_max": 139.0},
    "kinki": {"lat_min": 33.4, "lat_max": 35.8, "lon_min": 134.0, "lon_max": 136.8},
    "chugoku": {"lat_min": 33.7, "lat_max": 35.6, "lon_min": 130.8, "lon_max": 134.4},
    "shikoku": {"lat_min": 32.7, "lat_max": 34.3, "lon_min": 132.0, "lon_max": 134.8},
    "kyushu": {"lat_min": 30.0, "lat_max": 34.0, "lon_min": 129.5, "lon_max": 132.0},
}

PLACE_KEYWORDS_TO_THEME = {
    "神社": ["sacred_profane", "shrine_temple"],
    "大社": ["sacred_profane", "shrine_temple"],
    "神宮": ["sacred_profane", "shrine_temple"],
    "寺": ["sacred_profane", "shrine_temple"],
    "院": ["sacred_profane", "shrine_temple"],
    "堂": ["sacred_profane"],
    "城": ["war_conflict", "community_tradition"],
    "庭園": ["nature_communion", "everyday_beauty"],
    "園": ["nature_communion"],
    "山": ["nature_communion"],
    "滝": ["nature_communion"],
    "温泉": ["nature_communion"],
    "美術館": ["craft_mastery"],
    "博物館": ["craft_mastery"],
    "塔": ["sacred_profane"],
    "橋": ["journey_boundary"],
    "島": ["journey_boundary"],
}

PLACE_KEYWORDS_TO_EXPERIENCE = {
    "神社": "reflective",
    "大社": "reflective",
    "神宮": "reflective",
    "寺": "reflective",
    "院": "reflective",
    "城": "intellectual",
    "庭園": "aesthetic",
    "美術館": "aesthetic",
    "博物館": "intellectual",
    "温泉": "physical",
    "山": "adventure",
    "滝": "adventure",
    "島": "adventure",
    "公園": "physical",
    "園": "aesthetic",
}


# ================================================================
# Era determination
# ================================================================

ERA_RANGES = [
    ("ancient", -10000, 1185),
    ("medieval", 1185, 1573),
    ("edo_early", 1573, 1700),
    ("edo_late", 1700, 1868),
    ("meiji_taisho", 1868, 1926),
    ("showa_prewar", 1926, 1945),
    ("showa_postwar", 1945, 1989),
    ("heisei", 1989, 2019),
    ("reiwa", 2019, 2100),
]


def year_to_era(year: int) -> Optional[str]:
    for code, y_from, y_to in ERA_RANGES:
        if y_from <= year < y_to:
            return code
    return None


def coord_to_region(lat: float, lon: float) -> Optional[str]:
    for region, bounds in REGION_BOUNDS.items():
        if (bounds["lat_min"] <= lat <= bounds["lat_max"] and
                bounds["lon_min"] <= lon <= bounds["lon_max"]):
            return region
    return None


# ================================================================
# Wikidata REST API helpers
# ================================================================

def get_entity_claims(qid: str) -> Dict[str, Any]:
    """Fetch entity claims from Wikidata REST API"""
    try:
        resp = requests.get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            headers={"User-Agent": WIKIDATA_UA},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        entity = data.get("entities", {}).get(qid, {})
        return entity.get("claims", {})
    except Exception as e:
        return {}


def extract_year_from_claims(claims: Dict, prop: str) -> Optional[int]:
    """Extract year from a time claim (P569=birth, P570=death)"""
    cl = claims.get(prop, [])
    if not cl:
        return None
    snak = cl[0].get("mainsnak", {})
    dv = snak.get("datavalue", {})
    if dv.get("type") == "time":
        time_str = dv.get("value", {}).get("time", "")
        m = re.match(r"[+-]?(\d{4})", time_str)
        if m:
            return int(m.group(1))
    return None


# ================================================================
# Task A1: Tag person entities
# ================================================================

def tag_persons(conn: sqlite3.Connection):
    print("\n" + "=" * 60)
    print("Task A1: Tagging person entities")
    print("=" * 60)

    # Load occupation data from wikidata mapping
    with open(MAPPING_PATH, encoding="utf-8") as f:
        mapping_data = json.load(f)

    occ_by_qid = {}
    for m in mapping_data.get("mappings", []):
        if m.get("entity_type") == "person" and m.get("occupation"):
            occ_by_qid[m["wikidata_id"]] = m["occupation"]

    # Get all person entities
    persons = conn.execute(
        "SELECT id, wikidata_id, label_ja FROM entities WHERE entity_type='person'"
    ).fetchall()

    print(f"  Persons: {len(persons)}")
    print(f"  Occupations in mapping: {len(occ_by_qid)}")

    tags_inserted = 0
    era_queried = 0

    for i, (eid, qid, label) in enumerate(persons):
        occ = occ_by_qid.get(qid, "")

        # Medium tag from occupation
        medium = OCCUPATION_TO_MEDIUM.get(occ)
        if medium:
            conn.execute(
                "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'wikidata_occupation', 0.9)",
                (eid, medium),
            )
            tags_inserted += 1

        # Theme tags from occupation
        themes = OCCUPATION_TO_THEME.get(occ, [])
        for theme in themes:
            conn.execute(
                "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'wikidata_occupation', 0.7)",
                (eid, theme),
            )
            tags_inserted += 1

        # Experience tag from occupation
        exp = OCCUPATION_TO_EXPERIENCE.get(occ)
        if exp:
            conn.execute(
                "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', ?, 'wikidata_occupation', 0.7)",
                (eid, exp),
            )
            tags_inserted += 1

        # Era tag: query Wikidata for birth/death dates (batch of first 200 + sample)
        if i < 200 or i % 5 == 0:
            claims = get_entity_claims(qid)
            if claims:
                birth_year = extract_year_from_claims(claims, "P569")
                death_year = extract_year_from_claims(claims, "P570")

                active_year = None
                if birth_year and death_year:
                    active_year = (birth_year + death_year) // 2
                elif birth_year:
                    active_year = birth_year + 30  # approximate active period
                elif death_year:
                    active_year = death_year - 30

                if active_year:
                    era = year_to_era(active_year)
                    if era:
                        conn.execute(
                            "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'wikidata_dates', 0.8)",
                            (eid, era),
                        )
                        tags_inserted += 1

                era_queried += 1
                time.sleep(0.5)  # Rate limit

            if i > 0 and i % 50 == 0:
                conn.commit()
                print(f"    Progress: {i}/{len(persons)}, tags: {tags_inserted}, era queries: {era_queried}")

    conn.commit()
    print(f"  Done: {tags_inserted} tags inserted, {era_queried} era queries")
    return tags_inserted


# ================================================================
# Task A2: Tag place entities
# ================================================================

def tag_places(conn: sqlite3.Connection):
    print("\n" + "=" * 60)
    print("Task A2: Tagging place entities")
    print("=" * 60)

    places = conn.execute(
        "SELECT id, wikidata_id, label_ja, lat, lon FROM entities WHERE entity_type='place'"
    ).fetchall()

    print(f"  Places: {len(places)}")
    tags_inserted = 0

    for eid, qid, label, lat, lon in places:
        # Geography tag from coordinates
        if lat is not None and lon is not None:
            region = coord_to_region(lat, lon)
            if region:
                conn.execute(
                    "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', ?, 'coordinates', 0.95)",
                    (eid, region),
                )
                tags_inserted += 1

        # Theme tags from label keywords
        if label:
            for keyword, themes in PLACE_KEYWORDS_TO_THEME.items():
                if keyword in label:
                    for theme in themes:
                        conn.execute(
                            "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'label_keyword', 0.7)",
                            (eid, theme),
                        )
                        tags_inserted += 1
                    break  # First match only

        # Experience tag from label keywords
        if label:
            for keyword, exp in PLACE_KEYWORDS_TO_EXPERIENCE.items():
                if keyword in label:
                    conn.execute(
                        "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', ?, 'label_keyword', 0.6)",
                        (eid, exp),
                    )
                    tags_inserted += 1
                    break

        # Medium tag: all places are physical/architectural
        conn.execute(
            "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'entity_type', 0.5)",
            (eid, "architecture"),
        )
        tags_inserted += 1

    conn.commit()
    print(f"  Done: {tags_inserted} tags inserted")
    return tags_inserted


# ================================================================
# Task A3: Tag work entities
# ================================================================

# AniList tag → theme_value mapping (from anilist_tag_mapping table)
GENRE_TO_THEME = {
    "Action": "battle",
    "Adventure": "adventure_quest",
    "Comedy": "humor_satire",
    "Drama": "identity_self",
    "Fantasy": "supernatural",
    "Horror": "horror",
    "Mahou Shoujo": "magic",
    "Mecha": "battle",
    "Music": "music_performance",
    "Mystery": "crime",
    "Psychological": "identity_self",
    "Romance": "romance",
    "Sci-Fi": "otherworld",
    "Slice of Life": "everyday_beauty",
    "Sports": "sports",
    "Supernatural": "supernatural",
    "Thriller": "death_rebirth",
}

FORMAT_TO_MEDIUM = {
    "TV": "anime_tv",
    "TV_SHORT": "anime_tv",
    "MOVIE": "anime_movie",
    "OVA": "anime_ova",
    "ONA": "anime_ova",
    "SPECIAL": "anime_tv",
    "MANGA": "manga",
    "ONE_SHOT": "manga",
    "NOVEL": "light_novel",
    "MUSIC": "music",
}


def search_anilist(keyword: str) -> Optional[Dict]:
    """Search AniList for a work and return tags/genres/format"""
    query = """
    query ($search: String!) {
      Media(search: $search) {
        id genres format seasonYear
        tags { name category }
      }
    }
    """
    try:
        resp = requests.post(
            "https://graphql.anilist.co",
            json={"query": query, "variables": {"search": keyword}},
            timeout=15,
        )
        if resp.ok:
            data = resp.json()
            return data.get("data", {}).get("Media")
    except Exception:
        pass
    return None


def tag_works(conn: sqlite3.Connection):
    print("\n" + "=" * 60)
    print("Task A3: Tagging work entities")
    print("=" * 60)

    # Load anilist_tag_mapping for tag → theme resolution
    tag_to_theme = {}
    for row in conn.execute(
        "SELECT anilist_tag_name, axis_value_code FROM anilist_tag_mapping WHERE axis='theme' AND axis_value_code IS NOT NULL"
    ):
        tag_to_theme[row[0]] = row[1]

    works = conn.execute(
        "SELECT id, wikidata_id, label_ja, label_en FROM entities WHERE entity_type='work'"
    ).fetchall()

    print(f"  Works: {len(works)}")
    tags_inserted = 0
    anilist_found = 0

    # Load mapping to check work_type
    with open(MAPPING_PATH, encoding="utf-8") as f:
        mapping_data = json.load(f)
    work_type_by_qid = {}
    for m in mapping_data.get("mappings", []):
        if m.get("entity_type") == "work":
            work_type_by_qid[m["wikidata_id"]] = m.get("work_type", "anime")

    for i, (eid, qid, label_ja, label_en) in enumerate(works):
        work_type = work_type_by_qid.get(qid, "anime")

        # Default medium from work_type
        default_medium = "anime" if work_type == "anime" else "manga"
        conn.execute(
            "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'wikidata_type', 0.8)",
            (eid, default_medium),
        )
        tags_inserted += 1

        # Try AniList search
        search_term = label_en if label_en else label_ja
        if search_term:
            media = search_anilist(search_term)
            if not media and label_ja and label_ja != search_term:
                media = search_anilist(label_ja)

            if media:
                anilist_found += 1

                # Format → medium
                fmt = media.get("format")
                if fmt and fmt in FORMAT_TO_MEDIUM:
                    # Update to more specific medium
                    conn.execute(
                        "UPDATE entity_tags SET value_code=?, source='anilist_format', confidence=0.95 "
                        "WHERE entity_id=? AND axis='medium' AND source='wikidata_type'",
                        (FORMAT_TO_MEDIUM[fmt], eid),
                    )

                # Genres → theme
                for genre in (media.get("genres") or []):
                    theme = GENRE_TO_THEME.get(genre)
                    if theme:
                        conn.execute(
                            "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'anilist_genre', 0.85)",
                            (eid, theme),
                        )
                        tags_inserted += 1

                # Tags → theme
                for tag in (media.get("tags") or [])[:5]:
                    theme = tag_to_theme.get(tag.get("name"))
                    if theme:
                        conn.execute(
                            "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'anilist_tag', 0.8)",
                            (eid, theme),
                        )
                        tags_inserted += 1

                # seasonYear → era
                year = media.get("seasonYear")
                if year:
                    era = year_to_era(year)
                    if era:
                        conn.execute(
                            "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'anilist_year', 0.95)",
                            (eid, era),
                        )
                        tags_inserted += 1

            time.sleep(1.0)  # AniList rate limit

        # Keyword-based theme fallback
        if label_ja:
            keyword_themes = {
                "妖怪": "yokai", "鬼": "demon", "忍者": "ninja",
                "侍": "samurai", "武士": "samurai",
                "魔法": "magic", "魔女": "magic",
                "ロボット": "battle", "ガンダム": "battle",
                "恋": "romance", "愛": "romance",
                "戦": "war_conflict", "戦争": "war_conflict",
                "探偵": "crime", "殺人": "crime",
                "宇宙": "space", "星": "space",
                "学園": "identity_self", "学校": "identity_self",
                "料理": "food_cuisine", "グルメ": "food_cuisine",
            }
            for kw, theme in keyword_themes.items():
                if kw in label_ja:
                    conn.execute(
                        "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'label_keyword', 0.6)",
                        (eid, theme),
                    )
                    tags_inserted += 1

        if i > 0 and i % 20 == 0:
            conn.commit()
            print(f"    Progress: {i}/{len(works)}, AniList found: {anilist_found}")

    conn.commit()
    print(f"  Done: {tags_inserted} tags, AniList matches: {anilist_found}/{len(works)}")
    return tags_inserted


# ================================================================
# Task B: Connection Graph Generation
# ================================================================

def get_entity_tags(conn: sqlite3.Connection, entity_id: int) -> Dict[str, Set[str]]:
    """Get all tags for an entity grouped by axis"""
    tags: Dict[str, Set[str]] = {}
    for row in conn.execute(
        "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (entity_id,)
    ):
        tags.setdefault(row[0], set()).add(row[1])
    return tags


def calculate_distance(a_values: Set[str], b_values: Set[str]) -> float:
    """Jaccard distance between two sets of axis values"""
    if not a_values and not b_values:
        return 0.5  # unknown
    if not a_values or not b_values:
        return 0.5
    intersection = len(a_values & b_values)
    union = len(a_values | b_values)
    similarity = intersection / union if union > 0 else 0
    return 1.0 - similarity


def era_numeric_distance(a_eras: Set[str], b_eras: Set[str]) -> float:
    """Numeric distance between era codes based on time gap"""
    era_order = {code: i for i, (code, _, _) in enumerate(ERA_RANGES)}
    if not a_eras or not b_eras:
        return 0.5
    a_idx = min(era_order.get(e, 4) for e in a_eras)
    b_idx = min(era_order.get(e, 4) for e in b_eras)
    return abs(a_idx - b_idx) / 8.0  # Normalize to 0-1


def evaluate_connection(distances: Dict[str, float]) -> Tuple[float, str, str]:
    """Evaluate connection quality using grammar rules. Returns (score, quality, rule_name)"""
    theme_d = distances.get("theme", 0.5)
    era_d = distances.get("era", 0.5)
    medium_d = distances.get("medium", 0.5)
    geo_d = distances.get("geography", 0.5)
    exp_d = distances.get("experience", 0.5)

    # era_bridge: same theme, different era → best
    if theme_d < 0.3 and era_d > 0.3:
        score = 1.2 * (1.0 - theme_d) * era_d
        return score, "good_surprise", "era_bridge"

    # medium_cross: same theme/era, different medium
    if theme_d < 0.3 and medium_d > 0.5:
        score = 1.1 * (1.0 - theme_d) * medium_d
        return score, "good_surprise", "medium_cross"

    # good_surprise_classic: theme close, era far, medium different
    if theme_d < 0.3 and era_d > 0.5 and medium_d > 0.5:
        score = 1.0 * (1.0 - theme_d) * era_d * medium_d
        return score, "good_surprise", "good_surprise_classic"

    # geo_theme: same theme, different geography
    if theme_d < 0.3 and geo_d > 0.5:
        score = 1.0 * (1.0 - theme_d) * geo_d
        return score, "good_surprise", "geo_theme"

    # obvious: all close
    if theme_d < 0.2 and era_d < 0.2 and medium_d < 0.2:
        return 0.2, "obvious", "obvious"

    # random: all far
    if theme_d > 0.7 and era_d > 0.7 and medium_d > 0.7:
        return 0.05, "bad_surprise", "random"

    # Default moderate
    return 0.3, "moderate", "default"


def generate_explanation(
    a_label: str, b_label: str,
    a_tags: Dict[str, Set[str]], b_tags: Dict[str, Set[str]],
    conn_type: str, shared_themes: Set[str],
    conn: sqlite3.Connection,
) -> str:
    """Generate a human-readable explanation for a connection"""
    # Get Japanese names for shared themes
    theme_names = []
    for code in list(shared_themes)[:2]:
        row = conn.execute("SELECT name_ja FROM theme_values WHERE code=?", (code,)).fetchone()
        if row:
            theme_names.append(row[0])

    theme_str = "・".join(theme_names) if theme_names else "共通のテーマ"

    # Get era names
    def get_era_name(tags):
        eras = tags.get("era", set())
        if eras:
            code = list(eras)[0]
            row = conn.execute("SELECT name_ja FROM era_values WHERE code=?", (code,)).fetchone()
            return row[0] if row else ""
        return ""

    # Get medium names
    def get_medium_name(tags):
        media = tags.get("medium", set())
        if media:
            code = list(media)[0]
            row = conn.execute("SELECT name_ja FROM medium_values WHERE code=?", (code,)).fetchone()
            return row[0] if row else ""
        return ""

    a_era = get_era_name(a_tags)
    b_era = get_era_name(b_tags)
    a_med = get_medium_name(a_tags)
    b_med = get_medium_name(b_tags)

    if conn_type == "era_bridge":
        return f"{a_label}（{a_era}）と{b_label}（{b_era}）は、「{theme_str}」というテーマで時代を超えて繋がる。"
    elif conn_type == "medium_cross":
        return f"{a_label}（{a_med}）と{b_label}（{b_med}）は、「{theme_str}」というテーマを異なる媒体で表現している。"
    elif conn_type == "geo_theme":
        a_geo = list(a_tags.get("geography", {"?"}))
        b_geo = list(b_tags.get("geography", {"?"}))
        return f"{a_label}と{b_label}は、「{theme_str}」というテーマで離れた土地を結ぶ。"
    else:
        return f"{a_label}と{b_label}は「{theme_str}」で繋がる。"


def build_connections(conn: sqlite3.Connection):
    print("\n" + "=" * 60)
    print("Task B: Building connection graph")
    print("=" * 60)

    # Get all entities with theme tags
    theme_groups: Dict[str, List[int]] = {}
    for row in conn.execute(
        "SELECT entity_id, value_code FROM entity_tags WHERE axis='theme'"
    ):
        theme_groups.setdefault(row[1], []).append(row[0])

    print(f"  Theme groups: {len(theme_groups)}")
    for theme, eids in sorted(theme_groups.items(), key=lambda x: -len(x[1]))[:10]:
        print(f"    {theme}: {len(eids)} entities")

    # Cache all entity tags
    all_entities = conn.execute("SELECT id, label_ja, entity_type FROM entities").fetchall()
    entity_info = {eid: (label, etype) for eid, label, etype in all_entities}

    tag_cache: Dict[int, Dict[str, Set[str]]] = {}
    for eid, _, _ in all_entities:
        tag_cache[eid] = get_entity_tags(conn, eid)

    connections_inserted = 0
    good_surprise_count = 0
    seen_pairs: Set[Tuple[int, int]] = set()

    # For each theme group, generate connections
    for theme_code, entity_ids in theme_groups.items():
        if len(entity_ids) < 2:
            continue

        for i, eid_a in enumerate(entity_ids):
            if connections_inserted >= 500:
                break
            for eid_b in entity_ids[i + 1:]:
                if connections_inserted >= 500:
                    break

                pair = (min(eid_a, eid_b), max(eid_a, eid_b))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                a_tags = tag_cache.get(eid_a, {})
                b_tags = tag_cache.get(eid_b, {})
                a_info = entity_info.get(eid_a, ("", ""))
                b_info = entity_info.get(eid_b, ("", ""))

                # Skip if same entity type AND same medium AND same era (too obvious)
                if a_info[1] == b_info[1]:
                    a_med = a_tags.get("medium", set())
                    b_med = b_tags.get("medium", set())
                    a_era = a_tags.get("era", set())
                    b_era = b_tags.get("era", set())
                    if a_med == b_med and a_era == b_era:
                        continue

                # Calculate distances
                distances = {
                    "theme": calculate_distance(a_tags.get("theme", set()), b_tags.get("theme", set())),
                    "era": era_numeric_distance(a_tags.get("era", set()), b_tags.get("era", set())),
                    "medium": calculate_distance(a_tags.get("medium", set()), b_tags.get("medium", set())),
                    "geography": calculate_distance(a_tags.get("geography", set()), b_tags.get("geography", set())),
                    "experience": calculate_distance(a_tags.get("experience", set()), b_tags.get("experience", set())),
                }

                score, quality, rule_name = evaluate_connection(distances)

                if score < 0.3:
                    continue  # Skip low-quality connections

                # Shared themes for explanation
                shared_themes = (a_tags.get("theme", set()) & b_tags.get("theme", set()))
                if not shared_themes:
                    shared_themes = {theme_code}

                explanation = generate_explanation(
                    a_info[0], b_info[0], a_tags, b_tags,
                    rule_name, shared_themes, conn,
                )

                conn.execute(
                    """INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type,
                     theme_distance, era_distance, medium_distance,
                     geography_distance, experience_distance,
                     serendipity_score, explanation, source, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        eid_a, eid_b, rule_name,
                        distances["theme"], distances["era"], distances["medium"],
                        distances["geography"], distances["experience"],
                        score, explanation, "auto", min(score, 1.0),
                    ),
                )
                connections_inserted += 1
                if quality == "good_surprise":
                    good_surprise_count += 1

        if connections_inserted >= 500:
            break

    conn.commit()
    print(f"  Done: {connections_inserted} connections, {good_surprise_count} good_surprise")
    return connections_inserted, good_surprise_count


# ================================================================
# Main
# ================================================================

def main():
    print("=" * 60)
    print("Phase 3.5: Entity Tagging + Connection Graph")
    print(f"Started: {now_iso()}")
    print("=" * 60)

    conn = sqlite3.connect(str(DB_PATH))

    # Clear existing tags and connections for clean run
    conn.execute("DELETE FROM entity_tags")
    conn.execute("DELETE FROM connections")
    conn.commit()

    # Task A1: Person tagging
    person_tags = tag_persons(conn)

    # Task A2: Place tagging
    place_tags = tag_places(conn)

    # Task A3: Work tagging
    work_tags = tag_works(conn)

    # Task B: Connection graph
    conns, good = build_connections(conn)

    # Final summary
    print("\n" + "=" * 60)
    print("Final Summary")
    print("=" * 60)

    for row in conn.execute(
        "SELECT axis, COUNT(*) FROM entity_tags GROUP BY axis ORDER BY COUNT(*) DESC"
    ):
        print(f"  {row[0]}: {row[1]} tags")

    total_tagged = conn.execute(
        "SELECT COUNT(DISTINCT entity_id) FROM entity_tags"
    ).fetchone()[0]
    total_entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n  Tagged entities: {total_tagged}/{total_entities} ({100*total_tagged//total_entities}%)")

    print(f"\n  Connections: {conns}")
    print(f"  Good surprise: {good}")

    # Top connections by score
    print("\n  Top 10 connections:")
    for row in conn.execute(
        """SELECT c.serendipity_score, c.connection_type, c.explanation,
                  a.label_ja, b.label_ja
           FROM connections c
           JOIN entities a ON c.entity_a_id = a.id
           JOIN entities b ON c.entity_b_id = b.id
           ORDER BY c.serendipity_score DESC
           LIMIT 10"""
    ):
        print(f"    [{row[0]:.2f}] {row[1]}: {row[2][:80]}")

    conn.close()
    print(f"\nCompleted: {now_iso()}")


if __name__ == "__main__":
    main()
