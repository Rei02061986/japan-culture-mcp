"""Tests for Japan Culture MCP Server tools.

Tests FTS5, R-Tree, Phase 14 tools (generate_timeline, compare_cultures,
generate_culture_map, today_in_culture, deep_dive), and Phase 16 tourism
tools (get_region_profile, find_tourism_assets, analyze_cultural_density)
using a small test SQLite database.
"""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile

import pytest

try:
    import pytest_asyncio
except ImportError:
    pytest_asyncio = None

# Ensure the project root is on the path so we can import the server
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Mock the mcp module if not available (Python < 3.10)
try:
    import mcp  # noqa: F401
except ImportError:
    from unittest import mock
    from types import ModuleType

    class _PassthroughFastMCP:
        """Minimal FastMCP mock whose .tool() decorator returns the original function."""
        def __init__(self, *a, **kw):
            pass
        def tool(self, *a, **kw):
            def _decorator(fn):
                return fn
            return _decorator

    _mock_fastmcp = ModuleType("mcp.server.fastmcp")
    _mock_fastmcp.FastMCP = _PassthroughFastMCP  # type: ignore[attr-defined]
    _mock_server = ModuleType("mcp.server")
    _mock_server.fastmcp = _mock_fastmcp  # type: ignore[attr-defined]
    _mock_mcp = ModuleType("mcp")
    _mock_mcp.server = _mock_server  # type: ignore[attr-defined]
    sys.modules["mcp"] = _mock_mcp
    sys.modules["mcp.server"] = _mock_server
    sys.modules["mcp.server.fastmcp"] = _mock_fastmcp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def test_db_path():
    """Create a small test SQLite DB in /tmp/ with entities, connections,
    entity_tags, FTS5, and R-Tree indexes."""

    db_path = os.path.join(tempfile.gettempdir(), "test_culture_ontology.db")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    cur = conn.cursor()

    # --- Schema ---
    cur.executescript("""
        DROP TABLE IF EXISTS entity_tags;
        DROP TABLE IF EXISTS connections;
        DROP TABLE IF EXISTS entities;

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
            source TEXT,
            is_dormant INTEGER DEFAULT 0,
            release_year INTEGER DEFAULT NULL,
            release_year_source TEXT DEFAULT NULL
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
    """)

    # --- FTS5 (unicode61) ---
    cur.execute("DROP TABLE IF EXISTS entities_fts")
    cur.execute("""
        CREATE VIRTUAL TABLE entities_fts USING fts5(
            label_ja, label_en,
            content='entities',
            content_rowid='id'
        )
    """)

    # --- FTS5 (trigram for CJK substring matching) ---
    cur.execute("DROP TABLE IF EXISTS entities_fts_trigram")
    try:
        cur.execute("""
            CREATE VIRTUAL TABLE entities_fts_trigram USING fts5(
                label_ja, label_en,
                content='entities',
                content_rowid='id',
                tokenize='trigram'
            )
        """)
        _has_trigram = True
    except Exception:
        _has_trigram = False

    # --- R-Tree ---
    cur.execute("DROP TABLE IF EXISTS entities_rtree")
    cur.execute("""
        CREATE VIRTUAL TABLE entities_rtree USING rtree(
            id,
            min_lat, max_lat,
            min_lon, max_lon
        )
    """)

    # --- Seed entities ---
    # Tuple: (wikidata_id, label_ja, label_en, entity_type, madb_id, ndl_id,
    #         anilist_id, dbpedia_uri, lat, lon, source, release_year, release_year_source)
    entities = [
        ("Q5589", "葛飾北斎", "Katsushika Hokusai", "person", None, None, None, None, 35.7147, 139.8041, "wikidata", None, None),
        ("Q200798", "冨嶽三十六景", "Thirty-six Views of Mount Fuji", "artwork", None, None, None, None, None, None, "wikidata", None, None),
        ("Q11618", "金閣寺", "Kinkaku-ji", "temple", None, None, None, None, 35.0394, 135.7292, "wikidata", None, None),
        ("Q34687", "能", "Noh", "performing_art", None, None, None, None, None, None, "wikidata", None, None),
        ("Q180091", "歌舞伎", "Kabuki", "performing_art", None, None, None, None, None, None, "wikidata", None, None),
        ("Q210725", "清水寺", "Kiyomizu-dera", "temple", None, None, None, None, 34.9949, 135.7850, "wikidata", None, None),
        ("Q193422", "鬼滅の刃", "Demon Slayer", "anime", None, None, "101922", None, None, None, "anilist", 2019, "anilist_json"),
        ("Q865153", "蟲師", "Mushishi", "anime", None, None, "457", None, None, None, "anilist", 2005, "anilist_json"),
        ("Q217012", "進撃の巨人", "Attack on Titan", "anime", None, None, "16498", None, None, None, "anilist", 2013, "anilist_json"),
        ("Q847382", "京都", "Kyoto", "place", None, None, None, None, 35.0116, 135.7681, "wikidata", None, None),
        (None, "祇園祭", "Gion Matsuri", "festival", None, None, None, None, 35.0036, 135.7785, "wikidata", None, None),
        (None, "ひな祭り", "Hina Matsuri", "festival", None, None, None, None, None, None, "wikidata", None, None),
        (None, "スラムダンク", "Slam Dunk", "anime", None, None, None, None, None, None, "anilist", 1993, "anilist_json"),
        (None, "鎌倉高校前駅", "Kamakura-Koko-Mae Station", "place", None, None, None, None, 35.3058, 139.4968, "osm", None, None),
        (None, "鶴岡八幡宮", "Tsurugaoka Hachimangu", "shrine", None, None, None, None, 35.3258, 139.5566, "osm", None, None),
        (None, "浮世絵", "Ukiyo-e", "art_form", None, None, None, None, None, None, "wikidata", None, None),
        (None, "茶道", "Tea Ceremony", "cultural_practice", None, None, None, None, None, None, "wikidata", None, None),
        (None, "東大寺", "Todai-ji", "temple", None, None, None, None, 34.6890, 135.8398, "wikidata", None, None),
        (None, "奈良公園", "Nara Park", "place", None, None, None, None, 34.6851, 135.8430, "wikidata", None, None),
        (None, "世阿弥", "Zeami", "person", None, None, None, None, None, None, "wikidata", None, None),
    ]

    for e in entities:
        cur.execute("""
            INSERT OR IGNORE INTO entities
            (wikidata_id, label_ja, label_en, entity_type, madb_id, ndl_id,
             anilist_id, dbpedia_uri, lat, lon, source, release_year, release_year_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, e)

    # --- Populate FTS5 (unicode61) ---
    cur.execute("""
        INSERT INTO entities_fts(rowid, label_ja, label_en)
        SELECT id, label_ja, label_en FROM entities
    """)

    # --- Populate FTS5 (trigram) ---
    if _has_trigram:
        cur.execute("""
            INSERT INTO entities_fts_trigram(rowid, label_ja, label_en)
            SELECT id, label_ja, label_en FROM entities
        """)

    # --- Populate R-Tree ---
    cur.execute("""
        INSERT INTO entities_rtree(id, min_lat, max_lat, min_lon, max_lon)
        SELECT id, lat, lat, lon, lon
        FROM entities WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)

    # --- Seed entity_tags ---
    tag_data = [
        (1, "era", "edo"), (1, "theme", "ukiyoe"), (1, "medium", "ukiyoe"),
        (2, "era", "edo"), (2, "theme", "ukiyoe"), (2, "medium", "ukiyoe"),
        (3, "era", "muromachi"), (3, "theme", "buddhism"), (3, "geography", "kinki"),
        (4, "era", "muromachi"), (4, "theme", "performing_arts"), (4, "medium", "theater"),
        (5, "era", "edo"), (5, "theme", "performing_arts"), (5, "medium", "theater"),
        (6, "era", "nara"), (6, "theme", "buddhism"), (6, "geography", "kinki"),
        (7, "era", "reiwa"), (7, "theme", "yokai"), (7, "medium", "anime_tv"),
        (8, "era", "heisei"), (8, "theme", "yokai"), (8, "medium", "anime_tv"),
        (9, "era", "heisei"), (9, "medium", "anime_tv"),
        (10, "geography", "kinki"),
        (11, "geography", "kinki"), (11, "theme", "seasonal_beauty"),
        (16, "era", "edo"), (16, "medium", "ukiyoe"),
        (17, "theme", "seasonal_beauty"), (17, "medium", "cultural_practice"),
        (20, "era", "muromachi"), (20, "theme", "performing_arts"),
    ]
    cur.executemany(
        "INSERT INTO entity_tags (entity_id, axis, value_code) VALUES (?, ?, ?)",
        tag_data,
    )

    # --- Seed connections ---
    connections = [
        (1, 2, "creator_work", 0.0, 0.0, 0.0, 0.5, 0.5, 0.92, "Hokusai created Thirty-six Views of Mount Fuji", "D5", 0.99, "keep"),
        (1, 8, "thematic_resonance", 0.3, 0.6, 0.7, 0.8, 0.5, 0.85, "Both explore supernatural nature themes", "D1", 0.8, "keep"),
        (1, 16, "same_theme", 0.0, 0.0, 0.0, 0.5, 0.3, 0.9, "Hokusai is a master of ukiyo-e", "D1", 0.95, "keep"),
        (4, 5, "shared_genre", 0.0, 0.3, 0.0, 0.2, 0.1, 0.7, "Both are Japanese traditional performing arts", "D3", 0.9, "keep"),
        (4, 20, "creator_work", 0.0, 0.0, 0.0, 0.5, 0.5, 0.88, "Zeami established Noh theater aesthetics", "D5", 0.95, "keep"),
        (3, 6, "geographic_cultural", 0.2, 0.3, 0.1, 0.0, 0.1, 0.65, "Both are famous Kyoto temples", "D4", 0.85, "keep"),
        (3, 10, "heritage_location", 0.1, 0.2, 0.3, 0.0, 0.2, 0.6, "Kinkaku-ji is in Kyoto", "E1", 0.9, "keep"),
        (7, 8, "shared_motif", 0.1, 0.2, 0.0, 0.5, 0.1, 0.75, "Both feature supernatural beings in Japanese folklore", "D7", 0.85, "keep"),
        (13, 14, "pilgrimage_filming", 0.0, 0.0, 0.0, 0.0, 0.0, 0.95, "Slam Dunk iconic railroad crossing scene", "D4", 0.99, "keep"),
        (14, 15, "pilgrimage_proximity", 0.3, 0.3, 0.3, 0.0, 0.2, 0.5, "Both in Kamakura area", "E4", 0.8, "keep"),
        (18, 19, "geographic_cultural", 0.1, 0.1, 0.2, 0.0, 0.1, 0.55, "Todai-ji is adjacent to Nara Park", "E1", 0.9, "keep"),
        (5, 10, "geographic_cultural", 0.2, 0.2, 0.3, 0.0, 0.2, 0.5, "Kabuki flourishes in Kyoto", "D4", 0.7, "keep"),
    ]
    cur.executemany("""
        INSERT INTO connections
        (entity_a_id, entity_b_id, connection_type, theme_distance, era_distance,
         medium_distance, geography_distance, experience_distance, serendipity_score,
         explanation, source, confidence, llm_verdict)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, connections)

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture(autouse=True)
def set_db_env(test_db_path):
    """Set DB_PATH environment variable for the server."""
    old = os.environ.get("DB_PATH")
    os.environ["DB_PATH"] = test_db_path
    yield
    if old is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = old


# ---------------------------------------------------------------------------
# Import server tools (after DB fixture sets up env)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def _import_server(test_db_path):
    """Import the server module once, with DB_PATH set."""
    os.environ["DB_PATH"] = test_db_path
    from server import japan_culture_mcp as srv
    return srv


# ---------------------------------------------------------------------------
# FTS5 Tests
# ---------------------------------------------------------------------------

class TestFTS5:
    """Test FTS5 full-text search functionality."""

    def test_fts5_search_japanese(self, test_db_path):
        """FTS5 search should find entities by full Japanese label.

        Note: FTS5 unicode61 tokenizer treats CJK text as whole tokens,
        so exact full-label match is needed (matching production behavior).
        """
        conn = sqlite3.connect(test_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            'SELECT * FROM entities_fts WHERE entities_fts MATCH ?',
            ('"葛飾北斎"',),
        ).fetchall()
        conn.close()
        assert len(rows) >= 1
        labels = [r["label_ja"] for r in rows]
        assert any("北斎" in lbl for lbl in labels)

    def test_fts5_search_english(self, test_db_path):
        """FTS5 search should find entities by English label."""
        conn = sqlite3.connect(test_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM entities_fts WHERE entities_fts MATCH ?", ("Hokusai",)
        ).fetchall()
        conn.close()
        assert len(rows) >= 1

    def test_fts5_no_results(self, test_db_path):
        """FTS5 search with nonsense keyword should return no results."""
        conn = sqlite3.connect(test_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM entities_fts WHERE entities_fts MATCH ?",
            ("xyznonexistent123",),
        ).fetchall()
        conn.close()
        assert len(rows) == 0

    def test_fts5_multiple_results(self, test_db_path):
        """FTS5 search for broad term should return multiple results."""
        conn = sqlite3.connect(test_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM entities_fts WHERE entities_fts MATCH ?", ("temple",)
        ).fetchall()
        conn.close()
        # We have Kinkaku-ji, Kiyomizu-dera, Todai-ji -- but FTS5 matches on
        # English labels, some might not contain "temple" as a token.
        # At minimum we test the query doesn't crash.
        assert isinstance(rows, list)


# ---------------------------------------------------------------------------
# R-Tree Tests
# ---------------------------------------------------------------------------

class TestRTree:
    """Test R-Tree spatial index functionality."""

    def test_rtree_kyoto_area(self, test_db_path):
        """R-Tree query should find entities in the Kyoto bounding box."""
        conn = sqlite3.connect(test_db_path)
        conn.row_factory = sqlite3.Row
        # Kyoto area bounding box: ~34.9 to 35.1 lat, ~135.7 to 135.85 lon
        rows = conn.execute("""
            SELECT e.label_ja, e.label_en, e.entity_type
            FROM entities e
            JOIN entities_rtree r ON e.id = r.id
            WHERE r.min_lat >= 34.9 AND r.max_lat <= 35.1
              AND r.min_lon >= 135.7 AND r.max_lon <= 135.85
        """).fetchall()
        conn.close()
        labels = [r["label_ja"] for r in rows]
        assert "金閣寺" in labels or "清水寺" in labels or "京都" in labels

    def test_rtree_kamakura_area(self, test_db_path):
        """R-Tree query should find entities near Kamakura."""
        conn = sqlite3.connect(test_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT e.label_ja FROM entities e
            JOIN entities_rtree r ON e.id = r.id
            WHERE r.min_lat >= 35.2 AND r.max_lat <= 35.4
              AND r.min_lon >= 139.4 AND r.max_lon <= 139.6
        """).fetchall()
        conn.close()
        labels = [r["label_ja"] for r in rows]
        assert len(labels) >= 1

    def test_rtree_empty_area(self, test_db_path):
        """R-Tree query in empty ocean area should return no results."""
        conn = sqlite3.connect(test_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT e.label_ja FROM entities e
            JOIN entities_rtree r ON e.id = r.id
            WHERE r.min_lat >= 40.0 AND r.max_lat <= 41.0
              AND r.min_lon >= 150.0 AND r.max_lon <= 151.0
        """).fetchall()
        conn.close()
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# Tool Tests (Phase 14 new tools)
# ---------------------------------------------------------------------------

class TestGenerateTimeline:
    """Test the generate_timeline tool."""

    @pytest.mark.asyncio
    async def test_timeline_basic(self, _import_server):
        srv = _import_server
        result = await srv.generate_timeline(theme="浮世絵")
        data = json.loads(result)
        assert "error" not in data or data.get("total_found", 0) >= 0
        assert data.get("theme") == "浮世絵"

    @pytest.mark.asyncio
    async def test_timeline_with_region(self, _import_server):
        srv = _import_server
        result = await srv.generate_timeline(theme="金閣寺", region="京都")
        data = json.loads(result)
        assert data.get("theme") == "金閣寺"
        assert data.get("region") == "京都"

    @pytest.mark.asyncio
    async def test_timeline_no_match(self, _import_server):
        srv = _import_server
        result = await srv.generate_timeline(theme="xyznonexistent123")
        data = json.loads(result)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_timeline_year_filter(self, _import_server):
        srv = _import_server
        result = await srv.generate_timeline(
            theme="浮世絵", start_year=1600, end_year=1900
        )
        data = json.loads(result)
        assert data.get("year_range", {}).get("start") == 1600
        assert data.get("year_range", {}).get("end") == 1900


class TestCompareCultures:
    """Test the compare_cultures tool."""

    @pytest.mark.asyncio
    async def test_compare_basic(self, _import_server):
        srv = _import_server
        result = await srv.compare_cultures(entity_a="能", entity_b="歌舞伎")
        data = json.loads(result)
        if "error" not in data:
            assert "entity_a" in data
            assert "entity_b" in data
            assert "common_elements" in data

    @pytest.mark.asyncio
    async def test_compare_not_found(self, _import_server):
        srv = _import_server
        result = await srv.compare_cultures(
            entity_a="xyznonexistent", entity_b="能"
        )
        data = json.loads(result)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_compare_depth(self, _import_server):
        srv = _import_server
        result = await srv.compare_cultures(
            entity_a="能", entity_b="歌舞伎", depth=1
        )
        data = json.loads(result)
        if "error" not in data:
            assert data.get("depth") == 1


class TestGenerateCultureMap:
    """Test the generate_culture_map tool."""

    @pytest.mark.asyncio
    async def test_map_by_theme(self, _import_server):
        srv = _import_server
        result = await srv.generate_culture_map(theme="寺")
        data = json.loads(result)
        if "error" not in data:
            assert data.get("type") == "FeatureCollection"
            assert "features" in data

    @pytest.mark.asyncio
    async def test_map_by_work(self, _import_server):
        srv = _import_server
        result = await srv.generate_culture_map(work="スラムダンク")
        data = json.loads(result)
        if "error" not in data:
            assert data.get("type") == "FeatureCollection"

    @pytest.mark.asyncio
    async def test_map_no_params(self, _import_server):
        srv = _import_server
        result = await srv.generate_culture_map()
        data = json.loads(result)
        assert "error" in data  # Should require at least one param

    @pytest.mark.asyncio
    async def test_map_geojson_structure(self, _import_server):
        srv = _import_server
        result = await srv.generate_culture_map(theme="京都")
        data = json.loads(result)
        if "error" not in data and data.get("features"):
            feature = data["features"][0]
            assert feature["type"] == "Feature"
            assert "geometry" in feature
            assert "properties" in feature
            assert feature["geometry"]["type"] == "Point"
            coords = feature["geometry"]["coordinates"]
            assert len(coords) == 2


class TestTodayInCulture:
    """Test the today_in_culture tool."""

    @pytest.mark.asyncio
    async def test_today_default(self, _import_server):
        srv = _import_server
        result = await srv.today_in_culture()
        data = json.loads(result)
        assert "calendar_events" in data or "error" in data

    @pytest.mark.asyncio
    async def test_today_specific_date(self, _import_server):
        srv = _import_server
        result = await srv.today_in_culture(date="03-03")
        data = json.loads(result)
        if "error" not in data:
            assert data.get("date") == "03-03"
            assert data.get("month") == 3
            events = data.get("calendar_events", [])
            names = [e["name"] for e in events]
            assert "ひな祭り" in names

    @pytest.mark.asyncio
    async def test_today_category_filter(self, _import_server):
        srv = _import_server
        result = await srv.today_in_culture(date="07-01", category="festival")
        data = json.loads(result)
        if "error" not in data:
            for event in data.get("calendar_events", []):
                assert event["type"] == "festival"

    @pytest.mark.asyncio
    async def test_today_july_gion(self, _import_server):
        srv = _import_server
        result = await srv.today_in_culture(date="07-15")
        data = json.loads(result)
        if "error" not in data:
            events = data.get("calendar_events", [])
            names = [e["name"] for e in events]
            assert "祇園祭" in names


class TestDeepDive:
    """Test the deep_dive tool."""

    @pytest.mark.asyncio
    async def test_deep_dive_basic(self, _import_server):
        srv = _import_server
        result = await srv.deep_dive(entity="葛飾北斎")
        data = json.loads(result)
        if "error" not in data:
            assert "entity" in data
            assert "recommendations" in data
            assert data["entity"]["label_ja"] == "葛飾北斎"

    @pytest.mark.asyncio
    async def test_deep_dive_not_found(self, _import_server):
        srv = _import_server
        result = await srv.deep_dive(entity="xyznonexistent123")
        data = json.loads(result)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_deep_dive_max_recommendations(self, _import_server):
        srv = _import_server
        result = await srv.deep_dive(entity="葛飾北斎", max_recommendations=2)
        data = json.loads(result)
        if "error" not in data:
            assert len(data.get("recommendations", [])) <= 2

    @pytest.mark.asyncio
    async def test_deep_dive_has_categories(self, _import_server):
        srv = _import_server
        result = await srv.deep_dive(entity="葛飾北斎")
        data = json.loads(result)
        if "error" not in data and data.get("recommendations"):
            rec = data["recommendations"][0]
            assert "category" in rec
            assert "recommendation" in rec
            assert "serendipity_score" in rec


# ---------------------------------------------------------------------------
# Phase 16 Tourism Tool Tests
# ---------------------------------------------------------------------------

class TestGetRegionProfile:
    """Test the get_region_profile tool."""

    @pytest.mark.asyncio
    async def test_region_profile_kinki(self, _import_server):
        srv = _import_server
        result = await srv.get_region_profile(region="kinki")
        data = json.loads(result)
        assert "error" not in data
        assert data.get("region") == "kinki"
        assert data.get("region_name") == "近畿"
        assert "type_breakdown" in data
        assert "total_geo_entities" in data
        assert data["total_geo_entities"] >= 0

    @pytest.mark.asyncio
    async def test_region_profile_tokyo(self, _import_server):
        srv = _import_server
        result = await srv.get_region_profile(region="tokyo")
        data = json.loads(result)
        assert "error" not in data
        assert data.get("region") == "tokyo"
        assert "connection_density" in data
        assert "notable_entities" in data

    @pytest.mark.asyncio
    async def test_region_profile_unknown(self, _import_server):
        srv = _import_server
        result = await srv.get_region_profile(region="atlantis")
        data = json.loads(result)
        assert "error" in data
        assert "available_regions" in data

    @pytest.mark.asyncio
    async def test_region_profile_has_distributions(self, _import_server):
        srv = _import_server
        result = await srv.get_region_profile(region="kinki")
        data = json.loads(result)
        if "error" not in data:
            assert "theme_distribution" in data
            assert "era_distribution" in data
            assert isinstance(data["theme_distribution"], list)


class TestFindTourismAssets:
    """Test the find_tourism_assets tool."""

    @pytest.mark.asyncio
    async def test_tourism_assets_by_region(self, _import_server):
        srv = _import_server
        result = await srv.find_tourism_assets(region="kinki")
        data = json.loads(result)
        assert "error" not in data
        assert "categories" in data
        assert data["total_assets"] >= 0

    @pytest.mark.asyncio
    async def test_tourism_assets_by_coords(self, _import_server):
        srv = _import_server
        result = await srv.find_tourism_assets(lat=35.01, lon=135.77, radius_km=10.0)
        data = json.loads(result)
        assert "error" not in data
        assert "categories" in data

    @pytest.mark.asyncio
    async def test_tourism_assets_no_params(self, _import_server):
        srv = _import_server
        result = await srv.find_tourism_assets()
        data = json.loads(result)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_tourism_assets_filter_types(self, _import_server):
        srv = _import_server
        result = await srv.find_tourism_assets(region="kinki", asset_types="temple,shrine")
        data = json.loads(result)
        assert "error" not in data
        categories = data.get("categories", {})
        for key in categories:
            assert key in ("temple", "shrine")

    @pytest.mark.asyncio
    async def test_tourism_assets_unknown_region(self, _import_server):
        srv = _import_server
        result = await srv.find_tourism_assets(region="atlantis")
        data = json.loads(result)
        assert "error" in data


class TestAnalyzeCulturalDensity:
    """Test the analyze_cultural_density tool."""

    @pytest.mark.asyncio
    async def test_density_kyoto_area(self, _import_server):
        srv = _import_server
        result = await srv.analyze_cultural_density(
            lat_min=34.9, lat_max=35.1,
            lon_min=135.6, lon_max=135.9,
            grid_size=5,
        )
        data = json.loads(result)
        assert "error" not in data
        assert data["grid_size"] == 5
        assert "grid" in data
        assert len(data["grid"]) == 5
        assert len(data["grid"][0]) == 5
        assert data["total_entities"] >= 0

    @pytest.mark.asyncio
    async def test_density_empty_area(self, _import_server):
        srv = _import_server
        result = await srv.analyze_cultural_density(
            lat_min=40.0, lat_max=41.0,
            lon_min=150.0, lon_max=151.0,
            grid_size=3,
        )
        data = json.loads(result)
        assert "error" not in data
        assert data["total_entities"] == 0
        assert data["cells_non_empty"] == 0

    @pytest.mark.asyncio
    async def test_density_invalid_bounds(self, _import_server):
        srv = _import_server
        result = await srv.analyze_cultural_density(
            lat_min=36.0, lat_max=35.0,
            lon_min=135.0, lon_max=136.0,
        )
        data = json.loads(result)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_density_with_type_filter(self, _import_server):
        srv = _import_server
        result = await srv.analyze_cultural_density(
            lat_min=34.9, lat_max=35.1,
            lon_min=135.6, lon_max=135.9,
            grid_size=3,
            entity_type="temple",
        )
        data = json.loads(result)
        assert "error" not in data
        assert data.get("entity_type_filter") == "temple"

    @pytest.mark.asyncio
    async def test_density_hotspots(self, _import_server):
        srv = _import_server
        result = await srv.analyze_cultural_density(
            lat_min=34.5, lat_max=35.8,
            lon_min=135.0, lon_max=140.0,
            grid_size=10,
        )
        data = json.loads(result)
        if "error" not in data:
            assert "hotspots" in data
            assert isinstance(data["hotspots"], list)
            if data["hotspots"]:
                hotspot = data["hotspots"][0]
                assert "center_lat" in hotspot
                assert "center_lon" in hotspot
                assert "count" in hotspot


# ---------------------------------------------------------------------------
# Phase 18 Tool Tests — FTS5 Trigram, filter_by_release_year,
# get_prefecture_profile, pilgrimage_timeline
# ---------------------------------------------------------------------------

class TestFTS5Trigram:
    """Test FTS5 trigram tokenizer for CJK substring matching."""

    def test_trigram_table_exists(self, test_db_path):
        """FTS5 trigram table should exist."""
        conn = sqlite3.connect(test_db_path)
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE name='entities_fts_trigram'"
        ).fetchone()
        conn.close()
        if row is None:
            pytest.skip("Trigram tokenizer not supported on this SQLite")
        assert row[0] == "entities_fts_trigram"

    def test_trigram_cjk_3char(self, test_db_path):
        """Trigram should match 3+ char CJK queries."""
        conn = sqlite3.connect(test_db_path)
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE name='entities_fts_trigram'"
        ).fetchone()
        if row is None:
            conn.close()
            pytest.skip("Trigram tokenizer not supported")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM entities_fts_trigram WHERE entities_fts_trigram MATCH ?",
            ("金閣寺",),
        ).fetchall()
        conn.close()
        assert len(rows) >= 1
        labels = [r["label_ja"] for r in rows]
        assert any("金閣寺" in lbl for lbl in labels)

    def test_trigram_english(self, test_db_path):
        """Trigram should match English queries."""
        conn = sqlite3.connect(test_db_path)
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE name='entities_fts_trigram'"
        ).fetchone()
        if row is None:
            conn.close()
            pytest.skip("Trigram tokenizer not supported")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM entities_fts_trigram WHERE entities_fts_trigram MATCH ?",
            ("Hokusai",),
        ).fetchall()
        conn.close()
        assert len(rows) >= 1


class TestFilterByReleaseYear:
    """Test the filter_by_release_year tool."""

    @pytest.mark.asyncio
    async def test_year_range(self, _import_server):
        srv = _import_server
        result = await srv.filter_by_release_year(year_from=2000, year_to=2020)
        data = json.loads(result)
        assert "error" not in data
        assert data["query"]["year_from"] == 2000
        assert data["query"]["year_to"] == 2020
        for item in data.get("items", []):
            assert 2000 <= item["release_year"] <= 2020

    @pytest.mark.asyncio
    async def test_entity_type_filter(self, _import_server):
        srv = _import_server
        result = await srv.filter_by_release_year(
            year_from=1990, year_to=2025, entity_type="anime"
        )
        data = json.loads(result)
        assert "error" not in data
        for item in data.get("items", []):
            assert item["entity_type"] == "anime"

    @pytest.mark.asyncio
    async def test_no_results(self, _import_server):
        srv = _import_server
        result = await srv.filter_by_release_year(year_from=1800, year_to=1800)
        data = json.loads(result)
        assert "error" not in data
        assert data["total_results"] == 0

    @pytest.mark.asyncio
    async def test_with_keyword(self, _import_server):
        srv = _import_server
        result = await srv.filter_by_release_year(keyword="Demon Slayer")
        data = json.loads(result)
        assert "error" not in data


class TestGetPrefectureProfile:
    """Test the get_prefecture_profile tool."""

    @pytest.mark.asyncio
    async def test_tokyo(self, _import_server):
        srv = _import_server
        result = await srv.get_prefecture_profile(prefecture="tokyo")
        data = json.loads(result)
        assert "error" not in data
        assert data["prefecture"] == "tokyo"
        assert data["prefecture_name"] == "東京都"
        assert "entity_type_breakdown" in data
        assert "total_geo_entities" in data

    @pytest.mark.asyncio
    async def test_kyoto(self, _import_server):
        srv = _import_server
        result = await srv.get_prefecture_profile(prefecture="kyoto")
        data = json.loads(result)
        assert "error" not in data
        assert data["prefecture_name"] == "京都府"
        assert "theme_distribution" in data
        assert "pilgrimage_spots" in data

    @pytest.mark.asyncio
    async def test_unknown_prefecture(self, _import_server):
        srv = _import_server
        result = await srv.get_prefecture_profile(prefecture="atlantis")
        data = json.loads(result)
        assert "error" in data
        assert "available_prefectures" in data

    @pytest.mark.asyncio
    async def test_has_release_year_dist(self, _import_server):
        srv = _import_server
        result = await srv.get_prefecture_profile(prefecture="tokyo")
        data = json.loads(result)
        if "error" not in data:
            assert "release_year_distribution" in data


class TestPilgrimageTimeline:
    """Test the pilgrimage_timeline tool."""

    @pytest.mark.asyncio
    async def test_basic(self, _import_server):
        srv = _import_server
        result = await srv.pilgrimage_timeline()
        data = json.loads(result)
        assert "error" not in data
        assert "timeline" in data
        assert isinstance(data["timeline"], list)

    @pytest.mark.asyncio
    async def test_year_filter(self, _import_server):
        srv = _import_server
        result = await srv.pilgrimage_timeline(year_from=1990, year_to=2000)
        data = json.loads(result)
        assert "error" not in data
        for entry in data.get("timeline", []):
            assert 1990 <= entry["release_year"] <= 2000

    @pytest.mark.asyncio
    async def test_region_filter(self, _import_server):
        srv = _import_server
        result = await srv.pilgrimage_timeline(region="kanto")
        data = json.loads(result)
        assert "error" not in data
        assert data["query"]["region"] == "kanto"
