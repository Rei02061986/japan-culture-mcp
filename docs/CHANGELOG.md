# Changelog

All notable changes to the Japan Culture MCP Server project.

## [1.1.0] - Phase 14 (2025-01)

### Added
- **5 new MCP tools** (32-36): generate_timeline, compare_cultures, generate_culture_map, today_in_culture, deep_dive
- Timeline generation with era/region filtering and connection context
- Cultural comparison tool with shared/unique element analysis and tag diff
- GeoJSON culture map generation for pilgrimage, crafts, festivals
- Daily culture feed combining cultural calendar with ontology DB festivals
- Deep dive recommendations categorized by people/works/places/themes
- Cultural calendar with 40+ seasonal events across all 12 months
- Era-to-year mapping for timeline ordering

### Changed
- Server version bumped to v1.1.0
- Tool count increased from 31 to 36
- Server instructions updated to reflect new capabilities

## [1.0.0] - Phase 13 (2024-12)

### Added
- **FTS5 full-text search index** on entities_fts (label_ja, label_en)
  - 225x faster than LIKE queries (4ms vs 900ms)
  - Content-sync mode with auto-sync triggers
  - Unicode61 tokenizer for Japanese/English
- **R-Tree spatial index** on entities_rtree (lat, lon)
  - 106,419 geo-indexed entries
  - Auto-sync triggers on lat/lon changes
  - Bounding-box queries in ~2ms
- Connection pooling with thread-safe database access
- PRAGMA tuning (WAL, cache_size, mmap_size)
- LRU cache for frequently accessed entities

### Changed
- Entity count: 5.5M -> 7,326,918 (from 156 sources)
- Connection count: 450K -> 650,001
- Pilgrimage locations: 3,500 -> 3,943
- All ontology tools now use FTS5 instead of LIKE queries

## [0.9.0] - Phase 12 (2024-11)

### Added
- Connection strategies D1-D9 implemented:
  - D1: Theme clustering
  - D2: Era bridging
  - D3: Medium crossing
  - D4: Geographic cultural
  - D5: Creator-work linkage
  - D6: Influence chains
  - D7: Shared motif detection
  - D8: Adaptation mapping
  - D9: Temporal echo
- Massive JapanSearch SPARQL harvest (6.5M records via date-range pagination)
- Additional Wikidata entity harvests

### Changed
- Entity count: 2.5M -> 5,500,000 (from 131 sources)
- Connection count: 264K -> 450,000
- Source count: 110 -> 131

## [0.8.0] - Phase 11 (2024-10)

### Added
- LLM-based connection quality filtering (keep/drop verdicts)
- Entity deduplication pipeline
- Entity resolution across data sources

### Changed
- Connection quality improved significantly (dropped ~30% low-quality connections)
- Entity count consolidated from ~8M raw to ~7M after dedup

## [0.7.0] - Phase 10 (2024-10)

### Added
- Quality scoring for connections
- Confidence values for all connections
- Explanation text for all connection types

### Changed
- All connections now have human-readable explanations
- Serendipity scoring algorithm refined

## [0.6.0] - Phase 9 (2024-09)

### Added
- ToMuCo (Tokyo Museum Collection) integration: ~41,000 entities
- Additional National Land Info datasets
- OSM completion for all Japan regions

### Changed
- Entity count: ~3.5M -> ~5M
- Geographic coverage significantly expanded

## [0.5.0] - Phase 8 (2024-09)

### Added
- OSM Overpass integration: ~100,000 temples/shrines/torii
- National Land Numerical Information: ~44,000 entities
  - Tourism spots
  - Cultural properties
  - World heritage sites
  - Visitor facilities

### Changed
- Entity count: ~2.5M -> ~3.5M
- Geographic entity coverage dramatically improved

## [0.4.0] - Phase 7 (2024-08)

### Added
- `search_pilgrimage` tool: Search anime/film pilgrimage spots
- `generate_pilgrimage_route` tool: Generate pilgrimage routes
- `get_nearby_culture` tool: R-Tree based proximity search
- Pilgrimage connections from Wikidata P840/P915 properties
- Cross-connections between pilgrimage locations

### Changed
- Tool count: 28 -> 31
- Pilgrimage locations: 0 -> 2,000+
- Pilgrimage connections: 0 -> 64,000+

## [0.3.0] - Phase 6 (2024-08)

### Added
- `search_traditional_crafts` tool
- `search_literature` tool
- `search_artworks` tool
- `search_festivals` tool
- `search_living_national_treasures` tool
- `generate_serendipity_route` tool
- `explore_connections` tool (BFS graph traversal)
- `get_culture_stats` tool
- Aozora Bunko integration: ~16,000 literary works
- ColBase integration: ~9,000 museum artifacts

### Changed
- Tool count: 20 -> 28
- Entity count: ~1M -> ~2.5M
- Connection count: 0 -> 264,000

## [0.2.0] - Phase 4-5 (2024-07)

### Added
- `find_serendipity` tool: Cultural serendipity discovery
- `explore_axis` tool: 5-axis cultural exploration
- `get_entity_detail` tool: Detailed entity profiles
- `get_cultural_route` tool: Cultural route generation
- `search_culture` tool: Hybrid ontology + API search
- 5-axis ontology design (theme/era/medium/geography/experience)
- Entity tags system with axis-value classification
- Connection graph with serendipity scoring

### Changed
- Tool count: 15 -> 20
- Entity count: ~500K -> ~1M
- Ontology database created with schema design

## [0.1.0] - Phase 2-3 (2024-06)

### Added
- Initial MCP server with 15 tools
- `search_anime` (AniList GraphQL)
- `search_media_arts` (MADB SPARQL)
- `cross_reference` (AniList + MADB)
- `search_japan_search` (JapanSearch SPARQL + Easy API)
- `search_wikidata` (Wikidata SPARQL)
- `resolve_entity` (Wikidata entity resolution)
- `get_ndl_manifest` (NDL IIIF)
- `get_ndl_ocr_text` (NDL OCR)
- `search_ndl` (NDL SRU)
- `search_dbpedia_ja` (DBpedia Japanese)
- `iiif_get_manifest` (Generic IIIF)
- `get_map_tile_url` (GSI map tiles)
- `get_heritage_map_url` (Cultural Heritage WebGIS)
- `get_tourism_stats` (e-Stat)
- `cross_reference_v2` (Multi-source cross-reference)
- Claude Desktop configuration support

## [0.0.1] - Phase 1 (2024-05)

### Added
- Project initialization
- Requirements specification
- API research and feasibility testing
- Data source evaluation
