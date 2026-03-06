# Phase 6: Large-Scale Data Crawl + Entity Expansion Report

## Summary

Phase 6 expanded the Japan Culture MCP ontology database from 1,330 entities to **130,592 entities** (98x growth) and from 242 keep connections to **5,050 keep connections** (21x growth), through bulk API data fetching, automated tagging, LLM connection generation, and rule-based connection filtering.

**Total LLM API Cost: $0.388** (GPT-4o-mini)

---

## Phase 6A: Bulk Data Fetching

### Data Sources

| Source | Records | Files | Size |
|--------|---------|-------|------|
| MADB (Media Arts DB) | 209,000+ | 5 | 50.1 MB |
| Wikidata | 25,000+ | 19 | 14.7 MB |
| AniList | 17,500+ | 2 | 39.6 MB |
| NDL (National Diet Library) | 4,994 | 10 | 1.0 MB |
| JapanSearch | 60,000+ | 1 | 54.0 MB |
| **Total** | **~316,000** | **37** | **159.4 MB** |

### MADB Breakdown
- manga_series: 195,000+ (largest dataset)
- anime_series: 6,000+
- anime_movie: 2,800+
- game_work: 5,000+

### Wikidata Categories (19 categories)
Shrines (10,000+), temples, castles (101), world heritage (34), anime TV series (4,100+), anime films (1,700+), manga series (9,300+), light novels, ukiyo-e artists, festivals, onsen, gardens, video games, directors, voice actors, writers, prefectures, performing arts

### AniList
- ANIME: 17,500+ titles (popularity-sorted)
- MANGA: in progress (rate-limited at 90 req/min)

### NDL (10 searches)
ukiyoe, nishikie, kotenseki, emaki, byobu, yokai_art, hokusai, hiroshige, sharaku, utamaro

### JapanSearch (35 themes)
35 Japanese culture keywords covering traditional arts, martial arts, seasonal themes, literary works, performing arts, crafts

---

## Phase 6B: Entity Integration

### Entity Totals

| Source | Entities | % |
|--------|----------|---|
| MADB | 114,840 | 87.9% |
| Wikidata | 10,212 | 7.8% |
| NDL | 3,756 | 2.9% |
| Phase 3 (original) | 1,330 | 1.0% |
| LLM-generated | 454 | 0.3% |
| **Total** | **130,592** | **100%** |

### Entity Types

| Type | Count | % |
|------|-------|---|
| work | 118,877 | 91.0% |
| place | 10,505 | 8.0% |
| person | 1,064 | 0.8% |
| other | 146 | 0.1% |

### Deduplication
- Exact match + normalized string comparison (threshold 0.85)
- Wikidata ID matching for cross-source dedup
- From ~316,000 raw records to 130,592 unique entities

---

## Phase 6C: Auto-Tagging

### Coverage
- **130,118 / 130,592 entities tagged (99.6%)**
- 5-axis tagging system (theme, era, medium, geography, experience)

### Tag Distribution

| Axis | Unique Values | Total Tags |
|------|---------------|------------|
| Theme | 67 | 136,394 |
| Experience | 8 | 129,904 |
| Medium | 14 | 18,404 |
| Era | 10 | 7,356 |
| Geography | 9 | 4,967 |
| **Total** | **108** | **297,025** |

### Top Themes
1. craft_mastery: 93,162
2. love_bond: 6,635
3. community_tradition: 5,711
4. nature_communion: 5,156
5. war_conflict: 3,270
6. shrine_temple: 3,181
7. magic: 3,020
8. yokai: 1,776
9. death_rebirth: 1,484
10. performing_arts: 1,293

---

## Phase 6D: LLM Connection Generation

### Process
- Selected 1,638 priority entities (Wikidata places, people, high-connectivity, NDL works)
- GPT-4o-mini, batch size 5, 328 batches
- Temperature 0.8, JSON response format

### Results

| Metric | Value |
|--------|-------|
| Entities processed | 1,638 |
| API suggestions | 3,057 |
| Connections saved | 2,893 |
| Save rate | 94.6% |
| API cost | $0.221 |

### Connection Types Generated
- influence: 556
- thematic_resonance: 741
- shared_motif: 455
- geographic_cultural: 382
- adaptation: 251
- era_bridge: (included in mixed types)
- medium_cross: (included in mixed types)

---

## Phase 6E: Rule Connections + LLM Filter

### Process
- Generated candidates from entities sharing theme tags but differing in medium/era
- GPT-4o-mini filter in batches of 20, temperature 0.3
- 67 theme groups, prioritizing cross-medium and cross-era pairs

### Results

| Metric | Value |
|--------|-------|
| Candidates generated | 3,733 |
| Keep | 1,915 |
| Reject | 1,818 |
| Keep rate | 51.3% |
| API cost | $0.167 |

### Connection Types
| Type | Keep Count |
|------|-----------|
| medium_cross | 1,372 |
| era_bridge | 770 |
| geo_theme | 504 |
| thematic_resonance | (included above) |

---

## Final Database State

| Metric | Value |
|--------|-------|
| **Total entities** | **130,592** |
| **Total connections** | **5,906** |
| **Keep connections** | **5,050** |
| Tagged entities | 130,118 (99.6%) |
| LLM phase6 connections | 2,893 |
| Rule phase6 connections | 1,915 |
| Original connections (keep) | 242 |
| Total API cost (LLM) | $0.388 |

---

## Completion Criteria Verification

| Criterion | Status | Value |
|-----------|--------|-------|
| Raw data in data/ | PASS | 37 files, 159.4 MB |
| Entities >= 10,000 | PASS | 130,592 |
| 80%+ tag coverage | PASS | 99.6% |
| Keep connections >= 1,000 | PASS | 5,050 |
| LLM connections >= 500 | PASS | 2,893 |
| find_serendipity("北斎") >= 5 keep | PASS | 135 entities, 8 keep |
| find_serendipity("妖怪") >= 5 keep | PASS | 266 entities, 16 keep |
| find_serendipity("忍者") returns results | PASS | 142 entities, 5 keep |
| find_serendipity("茶道") returns results | PASS | 5 entities, 15 keep |
| find_serendipity("浅草") >= 5 keep | PASS | 87 entities, 6 keep |
| PHASE6_DATA_REPORT.md | PASS | This file |
| Total API cost < $10 | PASS | $0.388 |

**All 12 criteria: PASS**

---

## Technical Notes

### Issues Encountered & Resolved

1. **MADB SPARQL property URIs**: MADB uses `https://schema.org/name` (HTTPS), not `http://`. Fixed by using `rdfs:label`.

2. **Wikidata label resolution**: `rdfs:label` with `FILTER(LANG(...))` returned 0 results for most categories. Fixed with `SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }`.

3. **NDL SRU XML parsing**: Record data was XML-escaped inside `<recordData>` tags. Fixed with `html.unescape()` + `ET.fromstring()`.

4. **AniList rate limiting**: Strict 90 req/min limit with frequent 429s. Handled with exponential backoff.

5. **Background process API calls**: OpenAI API calls (both SDK and httpx) initially hung in background shell processes. Resolved by using raw httpx with explicit timeouts.

### Architecture Decisions

- Used GPT-4o-mini for all LLM operations (cost-efficient at $0.15/M input, $0.60/M output)
- Rule-based auto-tagging for scale (130K entities in seconds vs LLM minutes)
- 5-axis tagging: theme (67 values), era (10), medium (14), geography (9), experience (8)
- Batch API calls: 5 entities/batch for generation, 20 candidates/batch for filtering
- SQLite with per-batch commits for crash resilience
