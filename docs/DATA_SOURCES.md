# Data Sources

Complete list of data sources integrated into the Japan Culture MCP ontology database.

## Primary Sources

### JapanSearch (~6,500,000 entities)

- **URL**: https://jpsearch.go.jp/
- **API**: SPARQL endpoint (`https://jpsearch.go.jp/rdf/sparql`) + Easy API
- **Content**: Aggregated metadata from 264+ Japanese cultural institutions
- **Categories**: Prints, books, historical documents, photographs, newspapers, music, video, specimens, magazines
- **License**: CC BY 4.0 (metadata), individual content licenses vary by institution
- **Notes**: Largest single source. Harvested via rdfs:label pagination and schema:datePublished date-range queries.

### Wikidata (~300,000 entities)

- **URL**: https://www.wikidata.org/
- **API**: SPARQL endpoint (`https://query.wikidata.org/sparql`)
- **Content**: Structured data for Japanese cultural entities
- **Categories**: Shrines, temples, athletes, music, people, companies, schools, films, games, buildings, characters, pilgrimage locations (P840/P915)
- **License**: CC0 (public domain)
- **Notes**: Primary source for cross-referencing IDs and pilgrimage locations. 10s wait between queries, retry on 504.

### MADB - Media Arts Database (~115,000 entities)

- **URL**: https://mediaarts-db.bunka.go.jp/
- **API**: SPARQL endpoint (`https://mediaarts-db.artmuseums.go.jp/sparql`)
- **Content**: Agency for Cultural Affairs database of media arts
- **Categories**: Manga (250K+ volumes), anime (9K+ titles), games (35K+)
- **License**: Government Open Data (Japan)
- **Notes**: Authoritative source for manga/anime/game metadata.

### OpenStreetMap (~100,000 entities)

- **URL**: https://www.openstreetmap.org/
- **API**: Overpass API
- **Content**: Geographic data for cultural landmarks
- **Categories**: Temples, shrines, torii gates, cultural sites
- **License**: ODbL (Open Database License)
- **Notes**: Split queries by region to avoid timeouts. Excellent coverage for temples and shrines.

### National Land Numerical Information (~44,000 entities)

- **URL**: https://nlftp.mlit.go.jp/
- **API**: Direct download (GeoJSON/Shapefile)
- **Content**: Government geographic datasets
- **Categories**: Tourism spots, cultural properties, world heritage sites, visitor facilities
- **License**: Government Open Data (Japan), CC BY 4.0
- **Notes**: Authoritative geographic data from Ministry of Land, Infrastructure, Transport and Tourism.

### ToMuCo - Tokyo Museum Collection (~41,000 entities)

- **URL**: https://www.museum.or.jp/tomuco/
- **API**: REST API
- **Content**: Metadata from Tokyo-area museum collections
- **Categories**: Art, artifacts, historical objects
- **License**: CC BY 4.0
- **Notes**: Aggregated from multiple Tokyo museums.

### DBpedia Japanese (~23,000 entities)

- **URL**: https://ja.dbpedia.org/
- **API**: SPARQL endpoint (`https://ja.dbpedia.org/sparql`)
- **Content**: Structured data extracted from Japanese Wikipedia
- **Categories**: Places, persons, events, works
- **License**: CC BY-SA 3.0
- **Notes**: Good for biographical and event data.

### AniList (~17,500 entities)

- **URL**: https://anilist.co/
- **API**: GraphQL endpoint (`https://graphql.anilist.co`)
- **Content**: Anime and manga metadata with community data
- **Categories**: Anime series, manga, light novels
- **License**: Free API (rate-limited)
- **Notes**: Rich metadata including genres, tags, studios, scores, popularity.

### Aozora Bunko (~16,000 entities)

- **URL**: https://www.aozora.gr.jp/
- **API**: CSV index + full text
- **Content**: Public domain Japanese literature
- **Categories**: Classical literature, modern literature (pre-1968 copyright expiry)
- **License**: Public domain (individual works), CC BY for metadata
- **Notes**: Full text available for all works.

### ColBase - National Museum Collections (~9,000 entities)

- **URL**: https://colbase.nich.go.jp/
- **API**: REST API
- **Content**: Collections from Japan's four national museums
- **Categories**: Art, artifacts, archaeological items, natural history specimens
- **License**: CC BY 4.0
- **Notes**: High-quality metadata with IIIF image links.

### National Diet Library (~3,700 entities)

- **URL**: https://www.ndl.go.jp/
- **API**: SRU (`https://iss.ndl.go.jp/api/sru`), IIIF, OCR API
- **Content**: Digital collections from the National Diet Library
- **Categories**: Classical texts, ukiyo-e prints, historical maps
- **License**: Various (mostly public domain for pre-modern works)
- **Notes**: IIIF manifests available for image viewing. OCR text via Lab API.

## Additional Sources

### e-Stat (Tourism Statistics)

- **URL**: https://www.e-stat.go.jp/
- **API**: REST API (`https://api.e-stat.go.jp/rest/3.0/`)
- **Content**: Government statistics
- **Categories**: Inbound visitors, domestic tourism, cultural facility attendance
- **License**: Government Open Data (Japan)

### Geospatial Information Authority (GSI) Map Tiles

- **URL**: https://maps.gsi.go.jp/
- **API**: Tile URL pattern (`https://cyberjapandata.gsi.go.jp/xyz/{layer}/{z}/{x}/{y}.png`)
- **Content**: Japanese government map tiles
- **Categories**: Standard, satellite, historical, elevation
- **License**: Government Open Data (Japan)

### Cultural Heritage WebGIS

- **URL**: https://bunka.nii.ac.jp/
- **API**: URL-based map access
- **Content**: Cultural property locations
- **License**: Government Open Data (Japan)

### Google Maps (Optional)

- **URL**: https://developers.google.com/maps
- **API**: Directions API, Places API
- **Content**: Route generation and place information
- **License**: Google Maps Platform Terms of Service (API key required)
- **Notes**: Optional dependency. Falls back to GSI tiles when not configured.

## Source Counts by Phase

| Phase | Sources Added | Cumulative | Key Additions |
|-------|-------------|------------|---------------|
| 2-3 | 5 | 5 | AniList, MADB, JapanSearch, Wikidata, NDL |
| 4-6 | 15 | 20 | DBpedia, ColBase, Aozora, GSI, e-Stat |
| 7-9 | 40 | 60 | OSM regions, National Land Info, ToMuCo |
| 10-11 | 50 | 110 | JapanSearch expansion (date-range harvest) |
| 12 | 21 | 131 | JapanSearch deep harvest, additional Wikidata queries |
| 13 | 25 | 156 | Final JapanSearch sweep, OSM completion |
| 14 | 4+ | 160+ | Additional enrichment sources |

## License Summary

| License | Sources | Notes |
|---------|---------|-------|
| CC0 (Public Domain) | Wikidata | Freely usable without attribution |
| CC BY 4.0 | JapanSearch, ColBase, ToMuCo, National Land Info | Attribution required |
| CC BY-SA 3.0 | DBpedia Japanese | Share-alike required |
| ODbL | OpenStreetMap | Share-alike for database rights |
| Public Domain | Aozora Bunko (works), NDL (pre-modern) | No restrictions |
| Government Open Data | MADB, e-Stat, GSI, Cultural Heritage | Japan government open data policy |
| API Terms | AniList, Google Maps | Subject to API provider terms |

All metadata in the ontology database is used in compliance with the respective source licenses. The ontology database itself (as a compilation) is released under MIT license. Individual data entries retain the license of their source.
