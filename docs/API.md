# API Reference

Complete documentation for all 36 MCP tools provided by the Japan Culture MCP Server v1.1.0.

---

## Tool 1: search_anime

Search anime and manga via the AniList GraphQL API.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword (English or Japanese) |
| media_type | string | No | "ANIME" | Media type: "ANIME" or "MANGA" |
| max_results | int | No | 10 | Maximum results (1-25) |

**Example Response**:
```json
{
  "source": "AniList",
  "query": "Demon Slayer",
  "media_type": "ANIME",
  "total_found": 5,
  "returned": 5,
  "results": [
    {
      "id": 101922,
      "title_romaji": "Kimetsu no Yaiba",
      "title_english": "Demon Slayer",
      "title_native": "鬼滅の刃",
      "type": "ANIME",
      "format": "TV",
      "genres": ["Action", "Fantasy"],
      "tags": ["Demons", "Swordplay", "Historical"],
      "year": 2019,
      "score": 86,
      "popularity": 250000,
      "url": "https://anilist.co/anime/101922"
    }
  ]
}
```

---

## Tool 2: search_media_arts

Search the MADB (Media Arts Database) via SPARQL for manga, anime, and games.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword (Japanese recommended) |
| max_results | int | No | 20 | Maximum results (1-50) |

**Example Response**:
```json
{
  "source": "MADB (メディア芸術データベース)",
  "query": "鬼滅の刃",
  "returned": 8,
  "type_breakdown": {"AnimationSeries": 3, "ComicSeries": 2, "GameSoftware": 3},
  "results": [
    {
      "item": "https://mediaarts-db.artmuseums.go.jp/...",
      "label": "鬼滅の刃",
      "type_label": "AnimationSeries",
      "datePublished": "2019"
    }
  ]
}
```

---

## Tool 3: cross_reference

Cross-reference AniList and MADB results for a unified cultural view.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword |
| anilist_type | string | No | "ANIME" | AniList media type: "ANIME" or "MANGA" |
| max_results | int | No | 10 | Maximum results per source |

**Example Response**:
```json
{
  "query": "進撃の巨人",
  "cross_reference_summary": {
    "anilist_count": 5,
    "madb_count": 12,
    "matched_count": 2
  },
  "matched_items": [...],
  "anilist_results": [...],
  "madb_results": [...]
}
```

---

## Tool 4: search_japan_search

Search 264+ Japanese cultural institution databases via JapanSearch.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword (Japanese recommended) |
| method | string | No | "sparql" | "sparql" or "easy" |
| limit | int | No | 20 | Maximum results (1-100) |

**Example Response**:
```json
{
  "source": "ジャパンサーチ (SPARQL)",
  "query": "浮世絵 富士",
  "returned": 20,
  "results": [
    {
      "item": "https://jpsearch.go.jp/data/...",
      "label": "冨嶽三十六景 神奈川沖浪裏",
      "type": "絵画",
      "date": "1831"
    }
  ]
}
```

---

## Tool 5: search_wikidata

Search Wikidata for Japanese cultural entities via SPARQL.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword |
| query_type | string | No | "anime" | Query type: "anime", "person", "place", "artwork", "general" |
| limit | int | No | 20 | Maximum results |

---

## Tool 6: resolve_entity

Resolve an entity name to Wikidata ID and structured data.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| name | string | Yes | - | Entity name to resolve |
| entity_type | string | No | "any" | Entity type hint |

---

## Tool 7: get_ndl_manifest

Get IIIF Manifest from National Diet Library digital collections.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| pid | string | Yes | - | NDL persistent identifier (e.g., "1287112") |

---

## Tool 8: get_ndl_ocr_text

Get OCR text from NDL digital collections.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| pid | string | Yes | - | NDL persistent identifier |

---

## Tool 9: search_ndl

Search National Diet Library via SRU protocol.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword |
| max_results | int | No | 10 | Maximum results |

---

## Tool 10: search_dbpedia_ja

Search DBpedia Japanese for entity information.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| resource_name | string | Yes | - | Resource name (e.g., "葛飾北斎") |

---

## Tool 11: iiif_get_manifest

Fetch any IIIF manifest (CODH, NDL, e-Museum, etc.).

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| manifest_url | string | Yes | - | Full IIIF manifest URL |

---

## Tool 12: get_map_tile_url

Get GSI (Geospatial Information Authority) map tile URL.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| lat | float | Yes | - | Latitude |
| lon | float | Yes | - | Longitude |
| zoom | int | No | 15 | Zoom level (1-18) |
| layer | string | No | "std" | Map layer: "std", "pale", "photo", "relief" |

---

## Tool 13: get_heritage_map_url

Get Cultural Heritage WebGIS map URL centered on given coordinates.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| lat | float | Yes | - | Latitude |
| lon | float | Yes | - | Longitude |
| zoom | int | No | 15 | Zoom level |

---

## Tool 14: get_tourism_stats

Get tourism statistics from e-Stat government API.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| stat_type | string | No | "inbound_visitors" | Statistic type |
| year | int | No | None | Year filter |

---

## Tool 15: cross_reference_v2

Cross-reference across all data sources (AniList, MADB, Wikidata, JapanSearch, NDL).

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword |
| sources | list[str] | No | None | Sources to query (default: all) |
| max_results | int | No | 5 | Maximum results per source |

---

## Tool 16: find_serendipity

Discover unexpected cultural connections through the ontology connection graph.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Starting keyword (e.g., "北斎", "yokai") |
| max_results | int | No | 10 | Maximum serendipity connections to return |

**Example Response**:
```json
{
  "source": "ontology_db (find_serendipity)",
  "query": "北斎",
  "entity": {
    "label_ja": "葛飾北斎",
    "label_en": "Katsushika Hokusai",
    "entity_type": "person"
  },
  "serendipity_connections": [
    {
      "to_label": "蟲師",
      "connection_type": "thematic_resonance",
      "serendipity_score": 0.85,
      "explanation": "Both explore the relationship between humans and supernatural nature"
    }
  ]
}
```

---

## Tool 17: explore_axis

Explore culture along one of the 5 ontology axes.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| axis | string | Yes | - | Axis: "theme", "era", "medium", "geography", "experience" |
| value | string | No | None | Specific axis value (e.g., "yokai", "edo") |
| limit | int | No | 20 | Maximum results |

---

## Tool 18: get_entity_detail

Get detailed entity profile including tags, connections, and coordinates.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| entity_name | string | Yes | - | Entity name to look up |

---

## Tool 19: get_cultural_route

Generate a cultural route by theme and/or region.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| theme | string | No | None | Theme keyword (e.g., "ukiyo-e", "tea ceremony") |
| region | string | No | None | Region filter (e.g., "Kyoto", "Tohoku") |
| max_stops | int | No | 10 | Maximum route stops |

---

## Tool 20: search_culture

Hybrid search across ontology DB and external APIs.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| keyword | string | Yes | - | Search keyword |
| sources | string | No | "all" | Sources: "all", "db", "external" |
| limit | int | No | 20 | Maximum results |

---

## Tool 21: search_traditional_crafts

Search traditional Japanese crafts.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| region | string | No | None | Region filter |
| craft_type | string | No | None | Craft type (e.g., "ceramics", "textiles") |
| keyword | string | No | None | Free-text keyword |

---

## Tool 22: search_literature

Search Japanese literature (Aozora Bunko + Wikidata).

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| author | string | No | None | Author name |
| keyword | string | No | None | Work title or keyword |
| era | string | No | None | Era filter |

---

## Tool 23: search_artworks

Search artworks and museum collections.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| artist | string | No | None | Artist name |
| museum | string | No | None | Museum name |
| keyword | string | No | None | Free-text keyword |

---

## Tool 24: search_festivals

Search festivals and seasonal events.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| region | string | No | None | Region filter |
| keyword | string | No | None | Free-text keyword |
| month | int | No | None | Month filter (1-12) |

---

## Tool 25: search_living_national_treasures

Search Living National Treasures (Ningen Kokuho).

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| field | string | No | None | Field (e.g., "ceramics", "dyeing", "metalwork") |
| keyword | string | No | None | Free-text keyword |

---

## Tool 26: generate_serendipity_route

Generate a serendipity route by traversing the connection graph from a starting point.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| start_keyword | string | Yes | - | Starting entity keyword |
| depth | int | No | 5 | Route depth (number of hops) |
| strategy | string | No | "highest_serendipity" | Strategy: "highest_serendipity", "diverse", "geographic" |

---

## Tool 27: explore_connections

BFS exploration of the connection graph up to depth 3.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| entity_name | string | Yes | - | Starting entity name |
| max_depth | int | No | 2 | Maximum BFS depth (1-3) |
| max_per_level | int | No | 10 | Maximum connections per level |

---

## Tool 28: get_culture_stats

Get ontology database statistics (entity counts, connection counts, source breakdown).

**Parameters**: None

**Example Response**:
```json
{
  "source": "ontology_db (get_culture_stats)",
  "entities": {
    "total": 10234567,
    "by_type": {
      "artwork": 3200000,
      "person": 1500000,
      "place": 800000,
      "anime": 25000
    }
  },
  "connections": {
    "total": 800123,
    "by_type": {
      "pilgrimage_same_location": 250000,
      "same_theme": 150000
    }
  },
  "sources": {
    "total": 160,
    "top_10": [...]
  }
}
```

---

## Tool 29: search_pilgrimage

Search anime/film pilgrimage spots.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| work_title | string | No | None | Anime/film title |
| region | string | No | None | Region filter |
| lat | float | No | None | Latitude for proximity search |
| lon | float | No | None | Longitude for proximity search |
| radius_km | float | No | 5.0 | Search radius in km |

---

## Tool 30: generate_pilgrimage_route

Generate a pilgrimage route combining anime locations with cultural spots.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| work_title | string | No | None | Anime/film title for pilgrimage |
| theme | string | No | None | Cultural theme to include |
| region | string | No | None | Region filter |
| max_stops | int | No | 10 | Maximum route stops |

---

## Tool 31: get_nearby_culture

Search cultural resources near given coordinates using R-Tree spatial index.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| lat | float | Yes | - | Latitude |
| lon | float | Yes | - | Longitude |
| radius_km | float | No | 2.0 | Search radius in km |
| entity_type | string | No | None | Filter by entity type |
| limit | int | No | 20 | Maximum results |

---

## Tool 32: generate_timeline

Generate a cultural timeline for any theme, with optional era and region filters.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| theme | string | Yes | - | Theme keyword (e.g., "浮世絵", "anime", "tea ceremony") |
| region | string | No | None | Region filter (e.g., "京都", "東北") |
| start_year | int | No | None | Start year filter (e.g., 1600) |
| end_year | int | No | None | End year filter (e.g., 1900) |
| max_events | int | No | 20 | Maximum events (1-50) |

**Example Response**:
```json
{
  "source": "ontology_db (generate_timeline)",
  "theme": "浮世絵",
  "region": null,
  "year_range": {"start": null, "end": null},
  "total_found": 45,
  "results_count": 20,
  "timeline": [
    {
      "label_ja": "菱川師宣",
      "label_en": "Hishikawa Moronobu",
      "entity_type": "person",
      "era_code": "edo",
      "era_name": "江戸",
      "approximate_year": 1673,
      "connections": [
        {
          "type": "influence",
          "to": "鈴木春信",
          "explanation": "Pioneer of ukiyo-e prints influenced later artists"
        }
      ]
    }
  ]
}
```

---

## Tool 33: compare_cultures

Compare two cultural elements to discover commonalities, differences, and unexpected connections.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| entity_a | string | Yes | - | First entity (e.g., "京都", "能") |
| entity_b | string | Yes | - | Second entity (e.g., "金沢", "歌舞伎") |
| depth | int | No | 2 | Graph exploration depth (1-3) |

**Example Response**:
```json
{
  "source": "ontology_db (compare_cultures)",
  "entity_a": {
    "label_ja": "能",
    "entity_type": "performing_art",
    "total_connections": 42
  },
  "entity_b": {
    "label_ja": "歌舞伎",
    "entity_type": "performing_art",
    "total_connections": 58
  },
  "common_elements": {
    "count": 12,
    "entities": [
      {"label_ja": "面", "entity_type": "craft"},
      {"label_ja": "京都", "entity_type": "place"}
    ],
    "shared_tags": {
      "theme": ["performing_arts"],
      "medium": ["theater"]
    }
  },
  "unique_to_a": {
    "count": 30,
    "sample": [{"label_ja": "世阿弥", "entity_type": "person"}],
    "unique_tags": {"era": ["muromachi"]}
  },
  "unique_to_b": {
    "count": 46,
    "sample": [{"label_ja": "市川團十郎", "entity_type": "person"}],
    "unique_tags": {"era": ["edo"]}
  }
}
```

---

## Tool 34: generate_culture_map

Generate GeoJSON culture maps for visualization.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| theme | string | No | None | Theme keyword (e.g., "陶磁器", "国宝") |
| region | string | No | None | Region filter |
| entity_type | string | No | None | Entity type filter (e.g., "place", "festival") |
| work | string | No | None | Work title for pilgrimage map |
| max_features | int | No | 100 | Maximum features (1-500) |

**Example Response**:
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [135.7681, 35.0116]
      },
      "properties": {
        "name": "清水寺",
        "name_en": "Kiyomizu-dera",
        "entity_type": "temple",
        "source": "wikidata",
        "tags": {"theme": ["buddhism"], "era": ["nara"]}
      }
    }
  ],
  "metadata": {
    "source": "ontology_db (generate_culture_map)",
    "total_features": 85
  }
}
```

---

## Tool 35: today_in_culture

Get today's cultural events, festivals, and seasonal topics.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| date | string | No | Today | Date in MM-DD format (e.g., "03-03") |
| category | string | No | None | Category filter: "festival", "event", "food" |

**Example Response**:
```json
{
  "source": "ontology_db + cultural_calendar (today_in_culture)",
  "date": "03-03",
  "month": 3,
  "calendar_events": [
    {
      "name": "ひな祭り",
      "type": "event",
      "description": "3月3日。桃の節句。雛人形を飾る。"
    }
  ],
  "db_festivals": [
    {
      "name": "お水取り",
      "name_en": "Omizutori",
      "entity_type": "festival",
      "source": "wikidata"
    }
  ],
  "seasonal_culture": [
    {
      "name": "雛人形",
      "name_en": "Hina dolls",
      "entity_type": "craft"
    }
  ]
}
```

---

## Tool 36: deep_dive

Get categorized deep-dive recommendations for any entity.

**Parameters**:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| entity | string | Yes | - | Entity name (e.g., "葛飾北斎", "金閣寺") |
| max_recommendations | int | No | 5 | Maximum recommendations (1-10) |

**Example Response**:
```json
{
  "source": "ontology_db (deep_dive)",
  "entity": {
    "label_ja": "葛飾北斎",
    "label_en": "Katsushika Hokusai",
    "entity_type": "person",
    "tags": {"theme": ["ukiyoe", "nature_communion"], "era": ["edo"], "medium": ["ukiyoe"]}
  },
  "total_connections": 87,
  "recommendations_count": 5,
  "recommendations": [
    {
      "category": "作品関係",
      "category_key": "works",
      "recommendation": "冨嶽三十六景",
      "entity_type": "artwork",
      "connection_type": "creator_work",
      "serendipity_score": 0.92,
      "reason": "Most famous work series by Hokusai",
      "total_in_category": 23
    },
    {
      "category": "場所関係",
      "category_key": "places",
      "recommendation": "すみだ北斎美術館",
      "entity_type": "museum",
      "connection_type": "heritage_location",
      "serendipity_score": 0.78,
      "reason": "Museum dedicated to Hokusai in his hometown"
    }
  ],
  "category_summary": {
    "works": {"label": "作品関係", "count": 23},
    "places": {"label": "場所関係", "count": 12},
    "people": {"label": "人物関係", "count": 8},
    "themes": {"label": "テーマ関係", "count": 15}
  }
}
```

---

## Error Handling

All tools return JSON responses. On error, the response contains an `error` field:

```json
{
  "error": "AniList API error: 429",
  "detail": "Rate limit exceeded"
}
```

Common error patterns:
- External API timeout (30s default)
- Rate limiting (especially Wikidata: 10s between queries)
- Entity not found in ontology DB
- Database connection issues (ensure `DB_PATH` is set)
