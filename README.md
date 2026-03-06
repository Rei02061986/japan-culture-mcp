# Japan Culture MCP Server

![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue)
![MCP](https://img.shields.io/badge/MCP-1.0%2B-green)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow)
![Entities: 10M+](https://img.shields.io/badge/Entities-10M%2B-red)
![Tools: 39](https://img.shields.io/badge/Tools-39-purple)
![Geo: 750K+](https://img.shields.io/badge/Geo-750K%2B-orange)

A Model Context Protocol (MCP) server that provides AI assistants with deep access to Japanese cultural knowledge. Powered by an ontology database of **10,000,000+ entities** and **3,900,000+ cultural connections** spanning from ancient classical arts to modern subculture, sourced from **156+ authoritative databases**.

## Features

- **10M+ Entities**: Anime, manga, ukiyo-e, temples, shrines, festivals, literature, crafts, national treasures, museums, and more
- **3.9M+ Connections**: Cultural serendipity graph linking seemingly unrelated cultural elements across themes, eras, media, geography, and experience
- **39 MCP Tools**: Comprehensive search, discovery, comparison, mapping, tourism, and exploration capabilities
- **FTS5 Full-Text Search**: 225x faster than LIKE queries (4ms vs 900ms) for Japanese/English text
- **R-Tree Spatial Index**: Lightning-fast geographic queries for **750K+ geolocated entities**
- **Pilgrimage Support**: 3,900+ anime/film pilgrimage locations with route generation
- **Tourism Analysis**: Region profiles, tourism asset discovery, cultural density heatmaps
- **156+ Data Sources**: JapanSearch, Wikidata, MADB, AniList, NDL, OSM, DBpedia, and many more
- **5-Axis Ontology**: Theme (83 values), Era (10), Medium (18), Geography (13), Experience (9)

## Quick Start

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "japan-culture": {
      "command": "python3",
      "args": ["-m", "server.japan_culture_mcp"],
      "cwd": "/path/to/japan_culture_mcp",
      "env": {
        "DB_PATH": "/path/to/japan_culture_mcp/ontology/culture_ontology.db"
      }
    }
  }
}
```

### pip Install

```bash
pip install -e .
japan-culture-mcp
```

### Docker

```bash
# Copy your database to data/
cp ontology/culture_ontology.db data/

# Build and run
docker-compose up -d
```

## Tools (39)

### Core Ontology Tools

| # | Tool | Description |
|---|------|-------------|
| 1 | `search_anime` | Search anime/manga via AniList GraphQL API |
| 2 | `search_media_arts` | Search MADB (manga, anime, games) via SPARQL |
| 3 | `cross_reference` | Cross-reference AniList and MADB results |
| 4 | `search_japan_search` | Search 264+ cultural institution databases via JapanSearch |
| 5 | `search_wikidata` | Search Wikidata for Japanese cultural entities |
| 6 | `resolve_entity` | Resolve entity names to Wikidata IDs |
| 7 | `get_ndl_manifest` | Get IIIF Manifest from National Diet Library |
| 8 | `get_ndl_ocr_text` | Get OCR text from NDL digital collections |
| 9 | `search_ndl` | Search National Diet Library via SRU |
| 10 | `search_dbpedia_ja` | Search DBpedia Japanese |
| 11 | `iiif_get_manifest` | Fetch any IIIF manifest (CODH, NDL, e-Museum) |
| 12 | `get_map_tile_url` | Get GSI (Geospatial Information Authority) map tiles |
| 13 | `get_heritage_map_url` | Get Cultural Heritage WebGIS map URLs |
| 14 | `get_tourism_stats` | Get tourism statistics from e-Stat |
| 15 | `cross_reference_v2` | Cross-reference across all data sources |

### Serendipity & Discovery Tools

| # | Tool | Description |
|---|------|-------------|
| 16 | `find_serendipity` | Discover unexpected cultural connections (e.g., Hokusai -> Mushishi) |
| 17 | `explore_axis` | Explore culture along 5 axes (theme/era/medium/geography/experience) |
| 18 | `get_entity_detail` | Get detailed entity profile with tags, connections, coordinates |
| 19 | `get_cultural_route` | Generate cultural routes by theme and region |
| 20 | `search_culture` | Cross-search ontology DB and external APIs |

### Specialized Search Tools

| # | Tool | Description |
|---|------|-------------|
| 21 | `search_traditional_crafts` | Search traditional crafts (ceramics, textiles, lacquerware) |
| 22 | `search_literature` | Search literature (Aozora Bunko + Wikidata) |
| 23 | `search_artworks` | Search artworks and museum collections |
| 24 | `search_festivals` | Search festivals and seasonal events |
| 25 | `search_living_national_treasures` | Search Living National Treasures (Ningen Kokuho) |
| 26 | `generate_serendipity_route` | Generate serendipity connection graph routes |
| 27 | `explore_connections` | BFS exploration of connection graph (up to depth 3) |
| 28 | `get_culture_stats` | Get ontology database statistics |

### Pilgrimage & Location Tools

| # | Tool | Description |
|---|------|-------------|
| 29 | `search_pilgrimage` | Search anime/film pilgrimage spots by work, region, or coordinates |
| 30 | `generate_pilgrimage_route` | Generate pilgrimage routes (anime + cultural spots) |
| 31 | `get_nearby_culture` | Search cultural resources near given coordinates |

### Phase 14 New Tools

| # | Tool | Description |
|---|------|-------------|
| 32 | `generate_timeline` | Generate cultural timelines for any theme with era/region filters |
| 33 | `compare_cultures` | Compare two cultural elements to find commonalities and differences |
| 34 | `generate_culture_map` | Generate GeoJSON culture maps (pilgrimage, crafts, festivals) |
| 35 | `today_in_culture` | Get today's cultural events, festivals, and seasonal topics |
| 36 | `deep_dive` | Get categorized deep-dive recommendations for any entity |

### Phase 16 Tourism Tools

| # | Tool | Description |
|---|------|-------------|
| 37 | `get_region_profile` | Generate comprehensive cultural profile for any Japanese region |
| 38 | `find_tourism_assets` | Find tourism assets by category (shrines, temples, pilgrimage, food, etc.) |
| 39 | `analyze_cultural_density` | Grid-based cultural density analysis for heatmap visualization |

## Data Sources

| Source | Entities | Description |
|--------|----------|-------------|
| JapanSearch SPARQL | ~6,500,000 | Prints, books, historical documents, photographs, newspapers, music, video |
| Wikidata | ~300,000 | Shrines, temples, athletes, music, people, companies, films, games, characters |
| MADB | ~115,000 | Manga (250K volumes), anime (9K titles), games (35K) |
| OSM (OpenStreetMap) | ~100,000 | Temples, shrines, torii gates, cultural landmarks |
| National Land Info | ~44,000 | Tourism spots, cultural properties, world heritage, visitor facilities |
| ToMuCo | ~41,000 | Tokyo museum collections |
| DBpedia Japanese | ~23,000 | Places, persons, events, works |
| AniList | ~17,500 | Anime and manga with rich metadata |
| Aozora Bunko | ~16,000 | Classical and modern Japanese literature |
| ColBase | ~9,000 | National museum collections and artifacts |
| NDL | ~3,700 | Classical texts, ukiyo-e prints |

See [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md) for the complete list with URLs and licensing.

## Architecture

```
japan_culture_mcp/
  server/
    japan_culture_mcp.py    # MCP server (39 tools, v1.2.0)
    google_maps_integration.py  # Google Maps route support
  ontology/
    culture_ontology.db     # SQLite DB (~3GB, 10M+ entities)
  scripts/                  # Data pipeline scripts
  docs/                     # Documentation
  tests/                    # Test suite
```

### Database Schema

- **entities** (10M+ rows): `id, wikidata_id, label_ja, label_en, entity_type, madb_id, ndl_id, anilist_id, dbpedia_uri, lat, lon, source, ...`
- **connections** (3.9M+ rows): `entity_a_id, entity_b_id, connection_type, serendipity_score, explanation, theme/era/medium/geography/experience distances, ...`
- **entities_fts** (FTS5): Full-text index on `label_ja, label_en`
- **entities_rtree** (R-Tree): Spatial index on `lat, lon`
- **entity_tags**: 5-axis ontology tags (theme, era, medium, geography, experience)

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for full details.

## 5-Axis Ontology

| Axis | Values | Examples |
|------|--------|---------|
| Theme | 83 | yokai, samurai, love_bond, seasonal_beauty, nature_communion |
| Era | 10 | ancient, nara, heian, kamakura, muromachi, azuchi_momoyama, edo, meiji, showa, reiwa |
| Medium | 18 | manga, anime_tv, ukiyoe, architecture, literature, music, film |
| Geography | 13 | 8 regions + key prefectures/cities |
| Experience | 9 | aesthetic, intellectual, reflective, physical, social |

## Examples

### Discover unexpected connections

Ask Claude: "Find cultural connections between Hokusai and modern anime"

The `find_serendipity` tool will traverse the connection graph to reveal links like:
- Hokusai's Great Wave -> Studio Ghibli's Ponyo (shared ocean/wave motif)
- Hokusai's yokai art -> Mushishi (supernatural nature themes)
- Edo-period ukiyo-e -> Attack on Titan (dramatic composition techniques)

### Generate a pilgrimage route

Ask Claude: "Create an anime pilgrimage route in Kamakura"

The `generate_pilgrimage_route` tool combines anime filming locations with nearby cultural landmarks:
- Slam Dunk railroad crossing (Kamakura Koko Mae)
- Tsurugaoka Hachimangu shrine
- Great Buddha of Kamakura

### Compare two cultural traditions

Ask Claude: "Compare Noh theater and Kabuki"

The `compare_cultures` tool reveals shared and unique elements:
- Common: Japanese theater tradition, masks, stylized movement
- Noh-unique: Muromachi era origins, Zen influence, minimalism
- Kabuki-unique: Edo-era popular entertainment, elaborate costumes, onnagata

### Cultural map generation

Ask Claude: "Show me a map of pottery-producing regions in Japan"

The `generate_culture_map` tool returns GeoJSON data with all ceramic production sites, categorized by style and era.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DB_PATH` | Yes | Path to `culture_ontology.db` |
| `GOOGLE_MAPS_API_KEY` | No | Enables Google Maps route generation (falls back to GSI tiles) |
| `OPENAI_API_KEY` | No | For LLM-based connection quality scoring |

## Development

```bash
# Install with test dependencies
pip install -e ".[test]"

# Create test database
python scripts/create_test_db.py

# Run tests
pytest tests/ -v

# Run benchmarks
python scripts/benchmark.py
```

## License

MIT License. See [LICENSE](LICENSE) for details.

Data sourced from open government and community databases. See [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md) for individual source licenses.
