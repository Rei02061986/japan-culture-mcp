# Architecture

## System Overview

The Japan Culture MCP Server is a Model Context Protocol (MCP) server that provides AI assistants with structured access to a large-scale Japanese cultural knowledge graph. The system combines a local SQLite ontology database with real-time queries to external cultural APIs.

```
+------------------+       stdio/SSE        +---------------------+
|  Claude Desktop  | <--------------------> |  MCP Server (36     |
|  or any MCP      |                        |  tools, Python)     |
|  client          |                        |                     |
+------------------+                        +----+--------+-------+
                                                 |        |
                                    +------------+        +-------------+
                                    |                                   |
                              +-----v------+                   +--------v--------+
                              | Ontology   |                   | External APIs   |
                              | SQLite DB  |                   | (live queries)  |
                              | (~3GB)     |                   |                 |
                              +------------+                   +-----------------+
                              | entities   |                   | AniList GraphQL |
                              | connections|                   | MADB SPARQL     |
                              | entity_tags|                   | JapanSearch     |
                              | FTS5 index |                   | Wikidata SPARQL |
                              | R-Tree idx |                   | NDL SRU/IIIF   |
                              +------------+                   | DBpedia SPARQL  |
                                                               | GSI Tiles       |
                                                               | e-Stat          |
                                                               | Google Maps     |
                                                               +-----------------+
```

## Database Schema

### Entity-Relationship Diagram (Text)

```
+-------------------+          +--------------------+          +-------------------+
|    entities       |          |    connections      |          |   entity_tags     |
+-------------------+          +--------------------+          +-------------------+
| id (PK, INTEGER)  |<---+    | id (PK, INTEGER)   |    +---->| id (PK, INTEGER)  |
| wikidata_id (UQ)  |    |    | entity_a_id (FK)---+----+     | entity_id (FK) ---|--+
| label_ja (TEXT)    |    |    | entity_b_id (FK)---+----+     | axis (TEXT)       |  |
| label_en (TEXT)    |    +----| connection_type     |         | value_code (TEXT)  |  |
| entity_type (TEXT) |         | theme_distance      |         +-------------------+  |
| madb_id (TEXT)     |         | era_distance        |                                |
| ndl_id (TEXT)      |         | medium_distance     |         +-------------------+  |
| anilist_id (TEXT)  |         | geography_distance  |         |   entities_fts    |  |
| dbpedia_uri (TEXT) |         | experience_distance |         |   (FTS5 virtual)  |  |
| lat (REAL)         |         | serendipity_score   |         +-------------------+  |
| lon (REAL)         |         | explanation (TEXT)   |         | label_ja          |  |
| created_at         |         | source (TEXT)        |         | label_en          |  |
| updated_at         |         | confidence (REAL)    |         +-------------------+  |
| source (TEXT)      |         | llm_verdict (TEXT)   |                                |
+-------------------+          | llm_reason (TEXT)    |         +-------------------+  |
        |                      | created_at           |         |  entities_rtree   |  |
        |                      +--------------------+          |  (R-Tree virtual) |  |
        |                                                       +-------------------+  |
        +---------------------------------------------------------------<--------------+
                                                                | id (maps to       |
                                                                |   entities.id)    |
                                                                | minLat, maxLat    |
                                                                | minLon, maxLon    |
                                                                +-------------------+
```

### Table Details

#### entities (~10M rows)

Primary table storing all cultural entities.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment primary key |
| wikidata_id | TEXT UNIQUE | Wikidata Q-ID (e.g., Q5589) |
| label_ja | TEXT | Japanese label |
| label_en | TEXT | English label |
| entity_type | TEXT | Type (e.g., anime, shrine, person, artwork, festival) |
| madb_id | TEXT | MADB identifier |
| ndl_id | TEXT | National Diet Library identifier |
| anilist_id | TEXT | AniList identifier |
| dbpedia_uri | TEXT | DBpedia Japanese URI |
| lat | REAL | Latitude (WGS84) |
| lon | REAL | Longitude (WGS84) |
| created_at | TEXT | ISO 8601 creation timestamp |
| updated_at | TEXT | ISO 8601 update timestamp |
| source | TEXT | Data source identifier |

**Important**: No `description`, `subtype`, `tags`, or `external_id` columns exist. Always use `lat`/`lon`, not `latitude`/`longitude`. The `wikidata_id` column has a UNIQUE constraint -- always use `INSERT OR IGNORE` and pre-check.

#### connections (~800K rows)

Stores cultural connections between entity pairs.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment primary key |
| entity_a_id | INTEGER FK | First entity reference |
| entity_b_id | INTEGER FK | Second entity reference |
| connection_type | TEXT | Connection category (see below) |
| theme_distance | REAL | Theme axis distance (0-1) |
| era_distance | REAL | Era axis distance (0-1) |
| medium_distance | REAL | Medium axis distance (0-1) |
| geography_distance | REAL | Geography axis distance (0-1) |
| experience_distance | REAL | Experience axis distance (0-1) |
| serendipity_score | REAL | Composite serendipity score |
| explanation | TEXT | Human-readable explanation |
| source | TEXT | Connection source/strategy |
| confidence | REAL | Connection confidence (0-1) |
| llm_verdict | TEXT | LLM quality verdict ("keep"/"drop") |
| llm_reason | TEXT | LLM reasoning for verdict |
| created_at | TEXT | ISO 8601 creation timestamp |

**Connection types**: `pilgrimage_narrative`, `pilgrimage_filming`, `pilgrimage_same_location`, `pilgrimage_proximity`, `pilgrimage_regional`, `same_theme`, `shared_genre`, `medium_cross`, `thematic_resonance`, `cultural_echo`, `era_bridge`, `temporal_echo`, `shared_motif`, `creator_work`, `influence`, `adaptation`, `geographic_cultural`, `heritage_location`

#### entity_tags

5-axis ontology classification for entities.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment primary key |
| entity_id | INTEGER FK | Entity reference |
| axis | TEXT | Axis name: theme/era/medium/geography/experience |
| value_code | TEXT | Axis value code (e.g., "yokai", "edo", "manga") |

## Index Architecture

### FTS5 Full-Text Search

The `entities_fts` virtual table provides full-text search over `label_ja` and `label_en` using SQLite FTS5.

**Configuration**:
- Content table: `entities` (content-sync mode)
- Tokenizer: unicode61 (handles both Japanese and English)
- Auto-sync: INSERT/UPDATE/DELETE triggers keep FTS5 in sync with `entities`

**Performance**: 225x faster than `LIKE '%keyword%'` queries (4ms average vs 900ms).

**Query syntax**:
```sql
-- Simple search
SELECT * FROM entities_fts WHERE entities_fts MATCH 'keyword';

-- Prefix search
SELECT * FROM entities_fts WHERE entities_fts MATCH 'key*';

-- Boolean search
SELECT * FROM entities_fts WHERE entities_fts MATCH 'anime AND kyoto';
```

### R-Tree Spatial Index

The `entities_rtree` virtual table provides spatial indexing for geographic queries.

**Schema**:
```sql
CREATE VIRTUAL TABLE entities_rtree USING rtree(
    id,          -- maps to entities.id
    minLat, maxLat,
    minLon, maxLon
);
```

**Configuration**:
- 106,419 geo-indexed entries
- Auto-sync: INSERT/UPDATE/DELETE triggers keep R-Tree in sync when lat/lon changes

**Query syntax**:
```sql
-- Bounding box query
SELECT e.* FROM entities e
JOIN entities_rtree r ON e.id = r.id
WHERE r.minLat >= ? AND r.maxLat <= ?
  AND r.minLon >= ? AND r.maxLon <= ?;
```

### SQLite PRAGMA Configuration

Optimal settings for read-heavy workloads:
```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
PRAGMA cache_size=-64000;    -- 64MB cache
PRAGMA mmap_size=268435456;  -- 256MB memory-mapped I/O
```

## Tool Categories

### Category 1: External API Tools (Tools 1-15)

These tools query external APIs in real-time and do not require the local database.

| Tool | API | Protocol |
|------|-----|----------|
| search_anime | AniList | GraphQL |
| search_media_arts | MADB | SPARQL |
| cross_reference | AniList + MADB | GraphQL + SPARQL |
| search_japan_search | JapanSearch | SPARQL / REST |
| search_wikidata | Wikidata | SPARQL |
| resolve_entity | Wikidata | SPARQL |
| get_ndl_manifest | NDL | IIIF |
| get_ndl_ocr_text | NDL | REST |
| search_ndl | NDL | SRU |
| search_dbpedia_ja | DBpedia JP | SPARQL |
| iiif_get_manifest | Various | IIIF |
| get_map_tile_url | GSI | Tile URL |
| get_heritage_map_url | Cultural Agency | URL |
| get_tourism_stats | e-Stat | REST |
| cross_reference_v2 | Multiple | Mixed |

### Category 2: Ontology DB Tools (Tools 16-36)

These tools query the local SQLite ontology database with FTS5 and R-Tree indexes.

| Tool | Primary Index | Description |
|------|--------------|-------------|
| find_serendipity | FTS5 + connections | Serendipity discovery |
| explore_axis | entity_tags | 5-axis exploration |
| get_entity_detail | FTS5 | Entity profile |
| get_cultural_route | FTS5 + R-Tree | Route generation |
| search_culture | FTS5 + external APIs | Hybrid search |
| search_traditional_crafts | FTS5 + entity_tags | Craft search |
| search_literature | FTS5 + entity_tags | Literature search |
| search_artworks | FTS5 + entity_tags | Art search |
| search_festivals | FTS5 + entity_tags | Festival search |
| search_living_national_treasures | FTS5 | National treasure search |
| generate_serendipity_route | FTS5 + connections | Route via graph |
| explore_connections | connections (BFS) | Graph traversal |
| get_culture_stats | aggregate queries | Statistics |
| search_pilgrimage | FTS5 + connections | Pilgrimage search |
| generate_pilgrimage_route | FTS5 + R-Tree | Pilgrimage routing |
| get_nearby_culture | R-Tree | Proximity search |
| generate_timeline | FTS5 + entity_tags | Timeline generation |
| compare_cultures | FTS5 + connections | Cultural comparison |
| generate_culture_map | FTS5 + R-Tree | GeoJSON map |
| today_in_culture | FTS5 + calendar | Daily culture |
| deep_dive | FTS5 + connections | Deep recommendations |

## Connection Graph Structure

The connection graph models cultural relationships as weighted, undirected edges between entity pairs. Each connection has:

1. **Connection type**: Categorical label (e.g., `same_theme`, `pilgrimage_proximity`)
2. **5-axis distances**: Numerical distances along each ontology axis (0.0 = identical, 1.0 = maximally different)
3. **Serendipity score**: Composite score indicating how "surprising" the connection is
4. **LLM verdict**: Quality gate -- only connections with `llm_verdict = 'keep'` are served

### Connection Strategies

| ID | Strategy | Description |
|----|----------|-------------|
| D1 | Theme clustering | Connect entities sharing rare theme tags |
| D2 | Era bridging | Connect entities from different eras sharing themes |
| D3 | Medium crossing | Connect entities across different media types |
| D4 | Geographic cultural | Connect entities in the same geographic region |
| D5 | Creator-work | Connect creators with their works |
| D6 | Influence chains | Connect entities with known influence relationships |
| D7 | Shared motif | Connect entities sharing visual/narrative motifs |
| D8 | Adaptation | Connect original works with adaptations |
| D9 | Temporal echo | Connect entities from similar time periods |
| E1 | Temple proximity | Connect temples/shrines within walking distance |
| E2 | Source clustering | Connect entities from the same data source |
| E3 | Cross-type label match | Connect entities with matching labels across types |
| E4 | 3km proximity | Connect geolocated entities within 3km |

## Data Pipeline Overview

```
Phase 1-3: Foundation
  - AniList, MADB, JapanSearch, Wikidata basic queries
  - ~500K entities

Phase 4-6: Expansion
  - JapanSearch SPARQL pagination (date-based + label-based)
  - NDL, DBpedia, ColBase, Aozora Bunko
  - ~2M entities

Phase 7-9: Deep Collection
  - OSM Overpass (temples, shrines, torii)
  - National Land Info (tourism, cultural properties)
  - ToMuCo museum collections
  - ~5M entities

Phase 10-11: Quality & Enrichment
  - LLM-based connection quality filtering
  - Deduplication and entity resolution
  - ~7M entities after cleaning

Phase 12: Scale
  - Massive JapanSearch harvest (6.5M records)
  - Connection strategy D1-D9 implementation
  - 5.5M entities, 450K connections

Phase 13: Performance
  - FTS5 full-text search index
  - R-Tree spatial index
  - Connection pooling, PRAGMA tuning, LRU cache
  - 7.3M entities, 650K connections

Phase 14: Intelligence
  - Timeline generation (generate_timeline)
  - Cultural comparison (compare_cultures)
  - GeoJSON culture maps (generate_culture_map)
  - Daily culture feed (today_in_culture)
  - Deep dive recommendations (deep_dive)
  - 10M+ entities, 800K+ connections, 36 tools
```

## Performance Characteristics

| Operation | Latency | Index Used |
|-----------|---------|------------|
| FTS5 text search | ~4ms | entities_fts |
| LIKE text search | ~900ms | Full table scan |
| R-Tree bounding box | ~2ms | entities_rtree |
| Connection graph BFS (depth 2) | ~50ms | connections + entities |
| Connection graph BFS (depth 3) | ~200ms | connections + entities |
| Entity by ID | <1ms | entities PK |
| Tag lookup | ~5ms | entity_tags index |
| Full stats query | ~500ms | Aggregate scans |
