"""Japan Culture MCP Server — v1.3.0
古典文化〜現代サブカルチャーを横断検索する日本文化MCPサーバー
AniList + MADB + JapanSearch + Wikidata + NDL + DBpedia + GSI + Google Maps + ToMuCo + 国土数値情報
10,000,000+エンティティ、800,000+文化的接続（64,000+聖地巡礼）のオントロジーDB搭載
FTS5全文検索(trigram CJK対応) + R-Tree空間インデックスによる高速検索
Phase 18: FTS5 trigram CJK修正、release_yearフィルタ、都道府県プロファイル、聖地タイムライン、
一括地域比較、CCDMエマージェンス分析、データセットエクスポート追加（45ツール）
"""

import asyncio
import json
import math
import os
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "japan-culture",
    instructions=(
        "日本文化セレンディピティエンジン MCP v1.3.0 — "
        "10,000,000+エンティティと800,000+文化的接続（64,000+聖地巡礼接続含む）を持つオントロジーDBを搭載。"
        "5軸（テーマ・時代・媒体・地理・体験）で文化的セレンディピティを発見。"
        "160+ソースを統合: JapanSearch, Wikidata, MADB, AniList, NDL, MusicBrainz, DBpedia, ToMuCo, OSM, 国土数値情報, 地理院タイル, Google Maps。"
        "古典文化（浮世絵・古典籍・文化財・伝統工芸）から現代サブカルチャー（アニメ・漫画・ゲーム）まで45ツールで横断検索可能。"
        "FTS5 trigram全文検索(CJK部分一致対応) + R-Tree空間インデックスで高速検索。"
        "聖地巡礼検索・ルート生成・周辺文化検索・release_yearフィルタ・都道府県プロファイル・聖地タイムライン・"
        "一括地域比較・CCDMエマージェンス分析・データセットエクスポートにも対応。"
    ),
)

# ── constants ──────────────────────────────────────────────
ANILIST_ENDPOINT = "https://graphql.anilist.co"
MADB_SPARQL_ENDPOINT = "https://mediaarts-db.artmuseums.go.jp/sparql"
JPSEARCH_SPARQL_ENDPOINT = "https://jpsearch.go.jp/rdf/sparql"
JPSEARCH_EASY_ENDPOINT = "https://jpsearch.go.jp/rdf/es"
WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
DBPEDIA_JA_ENDPOINT = "https://ja.dbpedia.org/sparql"
NDL_IIIF_BASE = "https://www.dl.ndl.go.jp/api/iiif"
NDL_OCR_BASE = "https://lab.ndl.go.jp/dl/api/book/fulltext-json"
NDL_SRU_ENDPOINT = "https://iss.ndl.go.jp/api/sru"
GSI_TILE_BASE = "https://cyberjapandata.gsi.go.jp/xyz"
ESTAT_ENDPOINT = "https://api.e-stat.go.jp/rest/3.0/app/getStatsData"

HTTP_TIMEOUT = 30.0
WIKIDATA_UA = "japan-culture-mcp/0.2 (teddykmk@gmail.com)"


# ── AniList helpers ────────────────────────────────────────
ANILIST_SEARCH_QUERY = """
query ($search: String!, $type: MediaType, $perPage: Int) {
  Page(page: 1, perPage: $perPage) {
    pageInfo { total currentPage lastPage hasNextPage }
    media(search: $search, type: $type, sort: POPULARITY_DESC) {
      id
      title { romaji english native }
      type
      format
      genres
      tags { name category rank }
      description(asHtml: false)
      seasonYear
      season
      studios(isMain: true) { nodes { id name } }
      averageScore
      popularity
      siteUrl
      coverImage { large }
    }
  }
}
"""


async def _anilist_search(
    keyword: str,
    media_type: Optional[str] = None,
    per_page: int = 10,
) -> dict[str, Any]:
    variables: dict[str, Any] = {"search": keyword, "perPage": min(per_page, 25)}
    if media_type:
        variables["type"] = media_type.upper()

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(
            ANILIST_ENDPOINT,
            json={"query": ANILIST_SEARCH_QUERY, "variables": variables},
        )
        resp.raise_for_status()
        return resp.json()


# ── MADB helpers ───────────────────────────────────────────
def _build_madb_query(keyword: str, limit: int = 20) -> str:
    return f"""
    PREFIX schema: <https://schema.org/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/data/property#>
    PREFIX madbclass: <https://mediaarts-db.artmuseums.go.jp/data/class#>

    SELECT ?item ?label ?type ?datePublished ?genre ?creator ?description
    WHERE {{
      ?item rdfs:label ?label .
      ?item a ?type .
      FILTER(CONTAINS(STR(?label), "{keyword}"))
      OPTIONAL {{ ?item schema:datePublished ?datePublished }}
      OPTIONAL {{ ?item schema:genre ?genre }}
      OPTIONAL {{ ?item schema:creator ?creator }}
      OPTIONAL {{ ?item schema:description ?description }}
    }}
    LIMIT {limit}
    """


async def _madb_search(keyword: str, limit: int = 20) -> dict[str, Any]:
    query = _build_madb_query(keyword, limit)
    params = {"query": query, "output": "json"}
    headers = {"Accept": "application/sparql-results+json"}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(
            MADB_SPARQL_ENDPOINT,
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()


def _format_madb_results(raw: dict[str, Any]) -> list[dict[str, str]]:
    """SPARQL bindings を読みやすいリストに変換"""
    bindings = raw.get("results", {}).get("bindings", [])
    results = []
    for b in bindings:
        item: dict[str, str] = {}
        for key in ("item", "label", "type", "datePublished", "genre", "creator", "description"):
            if key in b:
                item[key] = b[key].get("value", "")
        # type URI からクラス名を抽出
        if "type" in item:
            item["type_label"] = item["type"].rsplit("#", 1)[-1] if "#" in item["type"] else item["type"].rsplit("/", 1)[-1]
        results.append(item)
    return results


# ── MCP Tools ──────────────────────────────────────────────

@mcp.tool()
async def search_anime(
    keyword: str,
    media_type: str = "ANIME",
    max_results: int = 10,
) -> str:
    """AniList GraphQL APIでアニメ・漫画作品を検索する。

    Args:
        keyword: 検索キーワード（英語・日本語どちらも可）
        media_type: メディアタイプ。"ANIME" または "MANGA"
        max_results: 最大取得件数（1-25）
    """
    try:
        data = await _anilist_search(keyword, media_type, max_results)
        page = data.get("data", {}).get("Page", {})
        media_list = page.get("media", [])
        page_info = page.get("pageInfo", {})

        results = []
        for m in media_list:
            title = m.get("title", {})
            studios = [s["name"] for s in m.get("studios", {}).get("nodes", [])]
            tags = [t["name"] for t in (m.get("tags") or [])[:5]]

            results.append({
                "id": m.get("id"),
                "title_romaji": title.get("romaji"),
                "title_english": title.get("english"),
                "title_native": title.get("native"),
                "type": m.get("type"),
                "format": m.get("format"),
                "genres": m.get("genres", []),
                "tags": tags,
                "year": m.get("seasonYear"),
                "season": m.get("season"),
                "studios": studios,
                "score": m.get("averageScore"),
                "popularity": m.get("popularity"),
                "description": (m.get("description") or "")[:200],
                "url": m.get("siteUrl"),
            })

        return json.dumps({
            "source": "AniList",
            "query": keyword,
            "media_type": media_type,
            "total_found": page_info.get("total", len(results)),
            "returned": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)

    except httpx.HTTPStatusError as e:
        return json.dumps({"error": f"AniList API error: {e.response.status_code}", "detail": str(e)})
    except Exception as e:
        return json.dumps({"error": f"AniList search failed: {str(e)}"})


@mcp.tool()
async def search_media_arts(
    keyword: str,
    max_results: int = 20,
) -> str:
    """メディア芸術データベース (MADB) SPARQLで漫画・アニメ・ゲームを検索する。

    25万冊の漫画、9千タイトルのアニメ、3.5万のゲームを収録した
    文化庁のデータベースをSPARQLで検索する。

    Args:
        keyword: 検索キーワード（日本語推奨）
        max_results: 最大取得件数（1-50）
    """
    try:
        raw = await _madb_search(keyword, min(max_results, 50))
        items = _format_madb_results(raw)

        # カテゴリ集計
        type_counts: dict[str, int] = {}
        for item in items:
            label = item.get("type_label", "unknown")
            type_counts[label] = type_counts.get(label, 0) + 1

        return json.dumps({
            "source": "MADB (メディア芸術データベース)",
            "query": keyword,
            "returned": len(items),
            "type_breakdown": type_counts,
            "results": items,
        }, ensure_ascii=False, indent=2)

    except httpx.HTTPStatusError as e:
        return json.dumps({"error": f"MADB API error: {e.response.status_code}", "detail": str(e)})
    except Exception as e:
        return json.dumps({"error": f"MADB search failed: {str(e)}"})


@mcp.tool()
async def cross_reference(
    keyword: str,
    anilist_type: str = "ANIME",
    max_results: int = 10,
) -> str:
    """AniListとMADBを同時に検索し、結果を統合して日本文化の横断ビューを提供する。

    同じキーワードで両データベースを検索し、一致する作品や
    関連する文化的コンテキストを見つける。

    Args:
        keyword: 検索キーワード（英語・日本語どちらも可）
        anilist_type: AniList側のメディアタイプ。"ANIME" または "MANGA"
        max_results: 各ソースからの最大取得件数
    """
    anilist_result = None
    madb_result = None
    errors: list[str] = []

    # 並行実行
    anilist_task = _anilist_search(keyword, anilist_type, max_results)
    madb_task = _madb_search(keyword, max_results)

    results = await asyncio.gather(anilist_task, madb_task, return_exceptions=True)

    # AniList結果
    if isinstance(results[0], Exception):
        errors.append(f"AniList: {str(results[0])}")
        anilist_items = []
    else:
        page = results[0].get("data", {}).get("Page", {})
        anilist_items = []
        for m in page.get("media", []):
            title = m.get("title", {})
            anilist_items.append({
                "source": "AniList",
                "id": m.get("id"),
                "title_romaji": title.get("romaji"),
                "title_english": title.get("english"),
                "title_native": title.get("native"),
                "type": m.get("type"),
                "genres": m.get("genres", []),
                "year": m.get("seasonYear"),
                "score": m.get("averageScore"),
                "url": m.get("siteUrl"),
            })

    # MADB結果
    if isinstance(results[1], Exception):
        errors.append(f"MADB: {str(results[1])}")
        madb_items = []
    else:
        madb_items = []
        for item in _format_madb_results(results[1]):
            madb_items.append({
                "source": "MADB",
                "uri": item.get("item", ""),
                "name": item.get("label", ""),
                "type": item.get("type_label", ""),
                "date": item.get("datePublished", ""),
                "description": (item.get("description") or "")[:200],
            })

    # 簡易マッチング: native title が MADB の name に含まれるか
    matches: list[dict[str, Any]] = []
    for a in anilist_items:
        native = a.get("title_native", "")
        if not native:
            continue
        for m in madb_items:
            madb_name = m.get("name", "")
            if native and madb_name and (native in madb_name or madb_name in native):
                matches.append({
                    "anilist_title": a.get("title_romaji"),
                    "anilist_native": native,
                    "madb_name": madb_name,
                    "madb_type": m.get("type"),
                    "madb_date": m.get("date"),
                    "anilist_url": a.get("url"),
                    "madb_uri": m.get("uri"),
                })

    return json.dumps({
        "query": keyword,
        "cross_reference_summary": {
            "anilist_count": len(anilist_items),
            "madb_count": len(madb_items),
            "matched_count": len(matches),
        },
        "matched_items": matches,
        "anilist_results": anilist_items,
        "madb_results": madb_items,
        "errors": errors if errors else None,
    }, ensure_ascii=False, indent=2)


# ── SPARQL helper ──────────────────────────────────────────

async def _sparql_query(
    endpoint: str,
    query: str,
    extra_headers: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    headers = {"Accept": "application/sparql-results+json"}
    if extra_headers:
        headers.update(extra_headers)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(endpoint, params={"query": query}, headers=headers)
        resp.raise_for_status()
        return resp.json()


def _bindings(raw: dict[str, Any]) -> list[dict[str, str]]:
    """SPARQL JSON結果からbindingsを取得し、value だけの dict リストに変換"""
    out = []
    for b in raw.get("results", {}).get("bindings", []):
        row: dict[str, str] = {}
        for k, v in b.items():
            row[k] = v.get("value", "")
        out.append(row)
    return out


# ══════════════════════════════════════════════════════════
# Phase 2A Tools
# ══════════════════════════════════════════════════════════

# ── 4. search_japan_search ─────────────────────────────────

@mcp.tool()
async def search_japan_search(
    keyword: str,
    method: str = "sparql",
    limit: int = 20,
) -> str:
    """ジャパンサーチで264以上の日本の文化機関DBを横断検索する。

    Args:
        keyword: 検索キーワード（日本語推奨）
        method: "sparql"（SPARQL）または "easy"（簡易API）
        limit: 最大取得件数（1-100）
    """
    try:
        limit = min(limit, 100)
        if method == "easy":
            try:
                async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                    resp = await client.get(
                        JPSEARCH_EASY_ENDPOINT,
                        params={"keyword": keyword, "format": "json"},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    return json.dumps({
                        "source": "ジャパンサーチ (Easy SPARQL)",
                        "query": keyword,
                        "data": data,
                    }, ensure_ascii=False, indent=2)
            except Exception:
                # Easy API may be unavailable; fall back to SPARQL
                method = "sparql"
        if method == "sparql":
            query = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX schema: <https://schema.org/>
            SELECT ?item ?label ?type ?provider ?thumbnail WHERE {{
              ?item rdfs:label ?label .
              FILTER(CONTAINS(?label, "{keyword}"))
              OPTIONAL {{ ?item schema:additionalType ?type }}
              OPTIONAL {{ ?item schema:provider ?provider }}
              OPTIONAL {{ ?item schema:thumbnail ?thumbnail }}
            }}
            LIMIT {limit}
            """
            raw = await _sparql_query(JPSEARCH_SPARQL_ENDPOINT, query)
            items = _bindings(raw)
            return json.dumps({
                "source": "ジャパンサーチ (SPARQL)",
                "query": keyword,
                "returned": len(items),
                "results": items,
            }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"JapanSearch failed: {str(e)}"})


# ── 5. search_wikidata ─────────────────────────────────────

WIKIDATA_TEMPLATES: dict[str, str] = {
    "cultural_heritage": """
        SELECT ?item ?itemLabel ?coord ?image WHERE {{
          ?item wdt:P1435 ?status ;
                wdt:P17 wd:Q17 ;
                rdfs:label ?l .
          FILTER(LANG(?l) = "ja")
          FILTER(CONTAINS(?l, "{keyword}"))
          OPTIONAL {{ ?item wdt:P625 ?coord }}
          OPTIONAL {{ ?item wdt:P18 ?image }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
        }}
        LIMIT {limit}
    """,
    "anime": """
        SELECT ?item ?itemLabel ?studioLabel ?authorLabel ?startDate WHERE {{
          ?item wdt:P31/wdt:P279* wd:Q1107 ;
                rdfs:label ?l .
          FILTER(LANG(?l) = "ja")
          FILTER(CONTAINS(?l, "{keyword}"))
          OPTIONAL {{ ?item wdt:P272 ?studio }}
          OPTIONAL {{ ?item wdt:P50 ?author }}
          OPTIONAL {{ ?item wdt:P580 ?startDate }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
        }}
        LIMIT {limit}
    """,
    "historical_person": """
        SELECT ?item ?itemLabel ?birthDate ?deathDate ?birthPlaceLabel ?occupationLabel ?image WHERE {{
          ?item wdt:P31 wd:Q5 ;
                wdt:P27 wd:Q17 ;
                rdfs:label ?l .
          FILTER(LANG(?l) = "ja")
          FILTER(CONTAINS(?l, "{keyword}"))
          OPTIONAL {{ ?item wdt:P569 ?birthDate }}
          OPTIONAL {{ ?item wdt:P570 ?deathDate }}
          OPTIONAL {{ ?item wdt:P19 ?birthPlace }}
          OPTIONAL {{ ?item wdt:P106 ?occupation }}
          OPTIONAL {{ ?item wdt:P18 ?image }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
        }}
        LIMIT {limit}
    """,
}


@mcp.tool()
async def search_wikidata(
    keyword: str,
    query_type: str = "anime",
    include_coordinates: bool = False,
    limit: int = 50,
) -> str:
    """Wikidataで日本文化関連エンティティを検索する。全DBを繋ぐハブとして機能。

    Args:
        keyword: 検索キーワード
        query_type: "cultural_heritage"（文化財）, "anime"（アニメ）, "historical_person"（歴史上の人物）
        include_coordinates: 座標を含めるか
        limit: 最大取得件数
    """
    try:
        template = WIKIDATA_TEMPLATES.get(query_type, WIKIDATA_TEMPLATES["anime"])
        query = template.format(keyword=keyword, limit=min(limit, 200))
        raw = await _sparql_query(
            WIKIDATA_SPARQL_ENDPOINT, query,
            extra_headers={"User-Agent": WIKIDATA_UA},
        )
        items = _bindings(raw)
        if not include_coordinates:
            for item in items:
                item.pop("coord", None)
        return json.dumps({
            "source": "Wikidata",
            "query": keyword,
            "query_type": query_type,
            "returned": len(items),
            "results": items,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Wikidata search failed: {str(e)}"})


# ── Wikidata REST API helpers (SPARQL fallback) ──────────

WIKIDATA_REST_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_DATA = "https://www.wikidata.org/wiki/Special:EntityData"

# Property IDs for external identifiers
_WD_EXT_PROPS = {
    "P349": "ndl_id",
    "P4082": "madb_id",
    "P214": "viaf_id",
    "P213": "isni_id",
    "P18": "image",
    "P625": "coord",
}


def _get_claim_value(claims: dict[str, Any], prop: str) -> Optional[str]:
    """Extract the first value of a Wikidata claim."""
    claim_list = claims.get(prop, [])
    if not claim_list:
        return None
    snak = claim_list[0].get("mainsnak", {})
    dv = snak.get("datavalue", {})
    if dv.get("type") == "string":
        return dv.get("value")
    if dv.get("type") == "wikibase-entityid":
        return dv.get("value", {}).get("id")
    if dv.get("type") == "globecoordinate":
        v = dv.get("value", {})
        return f"Point({v.get('longitude', 0)},{v.get('latitude', 0)})"
    return str(dv.get("value", ""))


async def _wikidata_rest_search(
    keyword: str,
    language: str = "ja",
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Wikidata REST API (wbsearchentities) — fast, no timeout."""
    params = {
        "action": "wbsearchentities",
        "search": keyword,
        "language": language,
        "format": "json",
        "limit": min(limit, 50),
        "type": "item",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            WIKIDATA_REST_API,
            params=params,
            headers={"User-Agent": WIKIDATA_UA},
        )
        resp.raise_for_status()
        data = resp.json()
    return data.get("search", [])


async def _wikidata_get_entity(qid: str) -> dict[str, Any]:
    """Wikidata REST API — fetch full entity data by QID."""
    url = f"{WIKIDATA_ENTITY_DATA}/{qid}.json"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, headers={"User-Agent": WIKIDATA_UA})
        resp.raise_for_status()
        data = resp.json()

    entity = data.get("entities", {}).get(qid, {})
    claims = entity.get("claims", {})
    labels = entity.get("labels", {})
    descs = entity.get("descriptions", {})

    result: dict[str, Any] = {
        "wikidata_id": qid,
        "wikidata_url": f"https://www.wikidata.org/entity/{qid}",
        "name": labels.get("ja", {}).get("value") or labels.get("en", {}).get("value", ""),
        "name_en": labels.get("en", {}).get("value"),
        "description": descs.get("ja", {}).get("value") or descs.get("en", {}).get("value", ""),
    }

    # Extract external IDs
    for prop, field in _WD_EXT_PROPS.items():
        val = _get_claim_value(claims, prop)
        if val:
            result[field] = val

    # Image: build Commons URL
    if result.get("image"):
        img_name = result["image"]
        # Convert to Commons thumbnail URL
        import hashlib
        md5 = hashlib.md5(img_name.replace(" ", "_").encode()).hexdigest()
        safe_name = img_name.replace(" ", "_")
        result["image_url"] = (
            f"https://upload.wikimedia.org/wikipedia/commons/thumb/"
            f"{md5[0]}/{md5[:2]}/{safe_name}/300px-{safe_name}"
        )

    return result


# ── 6. resolve_entity ──────────────────────────────────────

@mcp.tool()
async def resolve_entity(
    name: str,
    entity_type: str = "any",
) -> str:
    """名前からWikidata IDおよび外部ID（NDL, MADB, DBpedia等）を解決する。表記揺れ解消の鍵。

    REST API（高速）→ SPARQL（フォールバック）の2段構成。

    Args:
        name: エンティティ名（例: "葛飾北斎", "ゲゲゲの鬼太郎"）
        entity_type: "person", "work", "place", "any"
    """
    results = []
    method_used = "rest_api"

    try:
        # Step 1: REST API search (fast, reliable)
        search_results = await _wikidata_rest_search(name, limit=5)

        if search_results:
            # Step 2: Fetch entity details for each result
            for sr in search_results[:5]:
                qid = sr.get("id", "")
                if not qid:
                    continue
                try:
                    entity = await _wikidata_get_entity(qid)
                    results.append(entity)
                except Exception:
                    # If entity fetch fails, include basic info from search
                    results.append({
                        "wikidata_id": qid,
                        "wikidata_url": f"https://www.wikidata.org/entity/{qid}",
                        "name": sr.get("label", ""),
                        "description": sr.get("description", ""),
                    })

        # If REST API returned nothing, fall back to SPARQL
        if not results:
            method_used = "sparql_fallback"
            type_filter = ""
            if entity_type == "person":
                type_filter = "?item wdt:P31 wd:Q5 ."
            elif entity_type == "work":
                type_filter = "?item wdt:P31/wdt:P279* wd:Q17537576 ."
            elif entity_type == "place":
                type_filter = "?item wdt:P31/wdt:P279* wd:Q515 ."

            query = f"""
            SELECT ?item ?itemLabel ?itemDescription
                   ?ndl ?madb ?viaf ?isni ?image WHERE {{
              {type_filter}
              ?item rdfs:label ?l .
              FILTER(LANG(?l) = "ja")
              FILTER(CONTAINS(?l, "{name}"))
              OPTIONAL {{ ?item wdt:P349  ?ndl }}
              OPTIONAL {{ ?item wdt:P4082 ?madb }}
              OPTIONAL {{ ?item wdt:P214  ?viaf }}
              OPTIONAL {{ ?item wdt:P213  ?isni }}
              OPTIONAL {{ ?item wdt:P18   ?image }}
              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
            }}
            LIMIT 10
            """
            raw = await _sparql_query(
                WIKIDATA_SPARQL_ENDPOINT, query,
                extra_headers={"User-Agent": WIKIDATA_UA},
            )
            items = _bindings(raw)
            for item in items:
                wikidata_id = item.get("item", "").rsplit("/", 1)[-1] if "item" in item else None
                results.append({
                    "name": item.get("itemLabel", ""),
                    "description": item.get("itemDescription", ""),
                    "wikidata_id": wikidata_id,
                    "wikidata_url": item.get("item", ""),
                    "ndl_id": item.get("ndl"),
                    "madb_id": item.get("madb"),
                    "viaf_id": item.get("viaf"),
                    "image": item.get("image"),
                })

        return json.dumps({
            "source": f"Wikidata (resolve_entity, {method_used})",
            "name": name,
            "entity_type": entity_type,
            "returned": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"resolve_entity failed: {str(e)}"})


# ── 7. get_ndl_manifest ───────────────────────────────────

@mcp.tool()
async def get_ndl_manifest(pid: str) -> str:
    """国立国会図書館デジタルコレクションのIIIF Manifestを取得する。

    Args:
        pid: NDLデジタルコレクションのPID（例: "1286328"）
    """
    try:
        url = f"{NDL_IIIF_BASE}/{pid}/manifest.json"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        label = data.get("label", "")
        sequences = data.get("sequences", [])
        pages = []
        if sequences:
            canvases = sequences[0].get("canvases", [])
            for c in canvases:
                page_info: dict[str, Any] = {
                    "label": c.get("label", ""),
                    "width": c.get("width"),
                    "height": c.get("height"),
                }
                images = c.get("images", [])
                if images:
                    res = images[0].get("resource", {})
                    svc = res.get("service", {})
                    page_info["image_base"] = svc.get("@id", "")
                    page_info["thumbnail"] = f"{svc.get('@id', '')}/full/200,/0/default.jpg"
                pages.append(page_info)

        return json.dumps({
            "source": "NDL IIIF",
            "pid": pid,
            "manifest_url": url,
            "label": label,
            "total_pages": len(pages),
            "pages": pages[:20],
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"NDL manifest failed: {str(e)}"})


# ── 8. get_ndl_ocr_text ───────────────────────────────────

@mcp.tool()
async def get_ndl_ocr_text(pid: str) -> str:
    """国立国会図書館デジタルコレクションのOCRテキストを取得する。

    Args:
        pid: NDLデジタルコレクションのPID（例: "897115"）
    """
    try:
        url = f"{NDL_OCR_BASE}/{pid}"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        pages = []
        if isinstance(data, list):
            for page_data in data[:20]:
                page_text = ""
                if isinstance(page_data, dict):
                    for block in page_data.get("contents", []):
                        if isinstance(block, dict):
                            page_text += block.get("text", "") + "\n"
                        elif isinstance(block, str):
                            page_text += block + "\n"
                pages.append(page_text.strip())
        elif isinstance(data, dict):
            pages.append(json.dumps(data, ensure_ascii=False)[:2000])

        return json.dumps({
            "source": "NDL OCR",
            "pid": pid,
            "total_pages": len(pages),
            "pages": pages,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"NDL OCR failed: {str(e)}"})


# ── 9. search_ndl ──────────────────────────────────────────

@mcp.tool()
async def search_ndl(
    keyword: str,
    max_results: int = 10,
) -> str:
    """NDLサーチ（SRU API）で国立国会図書館の蔵書を検索する。

    Args:
        keyword: 検索キーワード
        max_results: 最大取得件数（1-50）
    """
    try:
        # NDL SRU requires CQL syntax: use 'anywhere' index for full-text search
        cql_query = f'anywhere="{keyword}"'
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(
                NDL_SRU_ENDPOINT,
                params={
                    "operation": "searchRetrieve",
                    "query": cql_query,
                    "maximumRecords": str(min(max_results, 50)),
                    "recordSchema": "dcndl",
                },
            )
            resp.raise_for_status()

        # SRU returns XML, parse key fields
        text = resp.text
        import re
        records = re.findall(r"<recordData>(.*?)</recordData>", text, re.DOTALL)
        items = []
        for rec in records:
            title_m = re.search(r"<dc:title[^>]*>([^<]+)</dc:title>", rec)
            creator_m = re.search(r"<dc:creator[^>]*>([^<]+)</dc:creator>", rec)
            date_m = re.search(r"<dc:date[^>]*>([^<]+)</dc:date>", rec)
            pub_m = re.search(r"<dc:publisher[^>]*>([^<]+)</dc:publisher>", rec)
            id_m = re.search(r"<dc:identifier[^>]*>([^<]+)</dc:identifier>", rec)
            items.append({
                "title": title_m.group(1) if title_m else "",
                "creator": creator_m.group(1) if creator_m else "",
                "date": date_m.group(1) if date_m else "",
                "publisher": pub_m.group(1) if pub_m else "",
                "identifier": id_m.group(1) if id_m else "",
            })

        total_m = re.search(r"<numberOfRecords>(\d+)</numberOfRecords>", text)
        total = int(total_m.group(1)) if total_m else len(items)

        return json.dumps({
            "source": "NDL SRU",
            "query": keyword,
            "total_found": total,
            "returned": len(items),
            "results": items,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"NDL search failed: {str(e)}"})


# ══════════════════════════════════════════════════════════
# Phase 2B Tools
# ══════════════════════════════════════════════════════════

# ── 10. search_dbpedia_ja ──────────────────────────────────

@mcp.tool()
async def search_dbpedia_ja(
    resource_name: str,
) -> str:
    """DBpedia Japaneseでエンティティの属性を取得する。

    Args:
        resource_name: リソース名（例: "葛飾北斎", "ゲゲゲの鬼太郎"）
    """
    try:
        query = f"""
        SELECT ?prop ?propLabel ?value WHERE {{
          <http://ja.dbpedia.org/resource/{resource_name}> ?prop ?value .
          OPTIONAL {{
            ?prop rdfs:label ?propLabel .
            FILTER(LANG(?propLabel) = "ja")
          }}
        }}
        LIMIT 50
        """
        raw = await _sparql_query(DBPEDIA_JA_ENDPOINT, query)
        items = _bindings(raw)

        # Group by property
        props: dict[str, list[str]] = {}
        for item in items:
            p = item.get("propLabel") or item.get("prop", "")
            p_short = p.rsplit("/", 1)[-1] if "/" in p else p
            v = item.get("value", "")
            if p_short not in props:
                props[p_short] = []
            if v not in props[p_short]:
                props[p_short].append(v)

        return json.dumps({
            "source": "DBpedia Japanese",
            "resource": resource_name,
            "property_count": len(props),
            "properties": props,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"DBpedia search failed: {str(e)}"})


# ── 11. iiif_get_manifest ─────────────────────────────────

@mcp.tool()
async def iiif_get_manifest(manifest_url: str) -> str:
    """汎用IIIFマニフェストを取得する（CODH・NDL・e-Museum等で共通利用可能）。

    Args:
        manifest_url: IIIF ManifestのURL
    """
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(manifest_url)
            resp.raise_for_status()
            data = resp.json()

        label = data.get("label", "")
        if isinstance(label, dict):
            label = label.get("ja", [label.get("en", [""])])[0] if isinstance(label.get("ja"), list) else str(label)

        sequences = data.get("sequences", [])
        pages = []
        if sequences:
            for c in sequences[0].get("canvases", [])[:30]:
                page: dict[str, Any] = {"label": c.get("label", "")}
                images = c.get("images", [])
                if images:
                    res = images[0].get("resource", {})
                    svc = res.get("service", {})
                    base_id = svc.get("@id", "")
                    page["image_url"] = f"{base_id}/full/full/0/default.jpg" if base_id else ""
                    page["thumbnail"] = f"{base_id}/full/200,/0/default.jpg" if base_id else ""
                pages.append(page)

        return json.dumps({
            "source": "IIIF",
            "manifest_url": manifest_url,
            "label": label,
            "total_pages": len(pages),
            "pages": pages,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"IIIF manifest failed: {str(e)}"})


# ── 12. get_map_tile_url ──────────────────────────────────

GSI_LAYERS = {
    "standard": "std",
    "photo": "seamlessphoto",
    "relief": "relief",
    "historical_rapid": "rapid",
    "pale": "pale",
}


@mcp.tool()
async def get_map_tile_url(
    lat: float,
    lon: float,
    zoom: int = 15,
    layer: str = "standard",
) -> str:
    """国土地理院の地図タイルURLを生成する（申請不要・出典記載のみ）。

    Args:
        lat: 緯度
        lon: 経度
        zoom: ズームレベル（0-18）
        layer: "standard"（標準地図）, "photo"（航空写真）, "relief"（色別標高図）, "historical_rapid"（明治期迅速測図）, "pale"（淡色地図）
    """
    layer_code = GSI_LAYERS.get(layer, "std")
    zoom = max(0, min(zoom, 18))

    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)

    tile_url = f"{GSI_TILE_BASE}/{layer_code}/{zoom}/{x}/{y}.png"

    return json.dumps({
        "source": "国土地理院",
        "layer": layer,
        "layer_code": layer_code,
        "lat": lat,
        "lon": lon,
        "zoom": zoom,
        "tile_x": x,
        "tile_y": y,
        "tile_url": tile_url,
        "attribution": "国土地理院 (https://maps.gsi.go.jp/development/ichiran.html)",
    }, ensure_ascii=False, indent=2)


# ── 13. get_heritage_map_url ──────────────────────────────

@mcp.tool()
async def get_heritage_map_url(
    lat: float,
    lon: float,
    zoom: int = 15,
) -> str:
    """文化財総覧WebGISのURL（奈良文化財研究所）を生成する。

    Args:
        lat: 緯度
        lon: 経度
        zoom: ズームレベル
    """
    url = f"https://heritagemap.nabunken.go.jp/#/{zoom}/{lat}/{lon}"
    return json.dumps({
        "source": "文化財総覧WebGIS",
        "lat": lat,
        "lon": lon,
        "zoom": zoom,
        "url": url,
    }, ensure_ascii=False, indent=2)


# ── 14. get_tourism_stats ─────────────────────────────────

@mcp.tool()
async def get_tourism_stats(
    stat_type: str = "inbound_visitors",
    year: Optional[int] = None,
) -> str:
    """e-Stat APIで訪日外国人統計等の観光統計を取得する。

    要環境変数 ESTAT_APP_ID。

    Args:
        stat_type: "inbound_visitors"（訪日外国人）
        year: 対象年（指定なしで最新）
    """
    app_id = os.environ.get("ESTAT_APP_ID")
    if not app_id:
        return json.dumps({
            "error": "ESTAT_APP_ID environment variable not set. Register at https://www.e-stat.go.jp/ to get an appId.",
            "setup_url": "https://www.e-stat.go.jp/",
        })

    stat_ids = {
        "inbound_visitors": "0003317273",
    }
    stat_data_id = stat_ids.get(stat_type, stat_ids["inbound_visitors"])

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(
                ESTAT_ENDPOINT,
                params={
                    "appId": app_id,
                    "statsDataId": stat_data_id,
                    "metaGetFlg": "Y",
                    "lang": "J",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        return json.dumps({
            "source": "e-Stat",
            "stat_type": stat_type,
            "data": data,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"e-Stat failed: {str(e)}"})


# ══════════════════════════════════════════════════════════
# cross_reference_v2
# ══════════════════════════════════════════════════════════

@mcp.tool()
async def cross_reference_v2(
    keyword: str,
    sources: Optional[list[str]] = None,
) -> str:
    """全データソースを横断検索し、結果を統合する。

    Args:
        keyword: 検索キーワード
        sources: 使用するソースのリスト。デフォルトは全て。
                 選択肢: "anilist", "madb", "wikidata", "ndl", "japan_search", "dbpedia"
    """
    if sources is None:
        sources = ["anilist", "madb", "wikidata", "ndl", "japan_search"]

    tasks: dict[str, Any] = {}
    if "anilist" in sources:
        tasks["anilist"] = _anilist_search(keyword, "ANIME", 10)
    if "madb" in sources:
        tasks["madb"] = _madb_search(keyword, 10)
    if "wikidata" in sources:
        wq = f"""
        SELECT ?item ?itemLabel ?itemDescription WHERE {{
          ?item rdfs:label ?l .
          FILTER(LANG(?l) = "ja")
          FILTER(CONTAINS(?l, "{keyword}"))
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
        }}
        LIMIT 10
        """
        tasks["wikidata"] = _sparql_query(
            WIKIDATA_SPARQL_ENDPOINT, wq,
            extra_headers={"User-Agent": WIKIDATA_UA},
        )
    if "japan_search" in sources:
        jq = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?item ?label WHERE {{
          ?item rdfs:label ?label .
          FILTER(CONTAINS(?label, "{keyword}"))
        }}
        LIMIT 10
        """
        tasks["japan_search"] = _sparql_query(JPSEARCH_SPARQL_ENDPOINT, jq)
    if "ndl" in sources:
        async def _ndl_sru():
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
                r = await c.get(NDL_SRU_ENDPOINT, params={
                    "operation": "searchRetrieve",
                    "query": f'anywhere="{keyword}"',
                    "maximumRecords": "5",
                    "recordSchema": "dcndl",
                })
                r.raise_for_status()
                return {"text": r.text, "status": r.status_code}
        tasks["ndl"] = _ndl_sru()

    # 並行実行
    keys = list(tasks.keys())
    raw_results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    output: dict[str, Any] = {"query": keyword, "sources": {}, "errors": []}

    for i, key in enumerate(keys):
        r = raw_results[i]
        if isinstance(r, Exception):
            output["errors"].append(f"{key}: {str(r)}")
            output["sources"][key] = {"status": "error", "count": 0}
            continue

        if key == "anilist":
            media = r.get("data", {}).get("Page", {}).get("media", [])
            output["sources"]["anilist"] = {
                "status": "ok",
                "count": len(media),
                "items": [
                    {
                        "title": m.get("title", {}).get("romaji"),
                        "title_native": m.get("title", {}).get("native"),
                        "type": m.get("type"),
                        "year": m.get("seasonYear"),
                        "url": m.get("siteUrl"),
                    }
                    for m in media
                ],
            }
        elif key == "madb":
            items = _format_madb_results(r)
            output["sources"]["madb"] = {
                "status": "ok",
                "count": len(items),
                "items": [
                    {"name": it.get("label", ""), "type": it.get("type_label", ""), "uri": it.get("item", "")}
                    for it in items
                ],
            }
        elif key == "wikidata":
            items = _bindings(r)
            output["sources"]["wikidata"] = {
                "status": "ok",
                "count": len(items),
                "items": [
                    {"name": it.get("itemLabel", ""), "desc": it.get("itemDescription", ""), "id": it.get("item", "")}
                    for it in items
                ],
            }
        elif key == "japan_search":
            items = _bindings(r)
            output["sources"]["japan_search"] = {
                "status": "ok",
                "count": len(items),
                "items": [{"label": it.get("label", ""), "uri": it.get("item", "")} for it in items],
            }
        elif key == "ndl":
            import re as _re
            text = r.get("text", "")
            total_m = _re.search(r"<numberOfRecords>(\d+)</numberOfRecords>", text)
            records = _re.findall(r"<dc:title[^>]*>([^<]+)</dc:title>", text)
            output["sources"]["ndl"] = {
                "status": "ok",
                "total": int(total_m.group(1)) if total_m else 0,
                "count": len(records),
                "titles": records[:10],
            }

    # Summary
    total = sum(s.get("count", 0) for s in output["sources"].values())
    output["summary"] = {
        "total_results": total,
        "sources_queried": len(keys),
        "sources_succeeded": len(keys) - len(output["errors"]),
    }
    if not output["errors"]:
        output.pop("errors")

    return json.dumps(output, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════
# Phase 3.5 Tools — Ontology-driven discovery
# ══════════════════════════════════════════════════════════

import sqlite3
import threading
import functools
from pathlib import Path

_ONTOLOGY_DB = Path(os.environ.get("DB_PATH", Path(__file__).parent.parent / "ontology" / "culture_ontology.db"))
_thread_local = threading.local()
_HAS_FTS5: Optional[bool] = None
_HAS_FTS5_TRIGRAM: Optional[bool] = None
_HAS_RTREE: Optional[bool] = None


def _get_db() -> sqlite3.Connection:
    """Get a read-only, PRAGMA-tuned connection with thread-local pooling."""
    conn = getattr(_thread_local, "db_conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except Exception:
            conn = None
    conn = sqlite3.connect(f"file:{_ONTOLOGY_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")    # 64 MB
    conn.execute("PRAGMA mmap_size=268435456")  # 256 MB
    conn.execute("PRAGMA temp_store=MEMORY")
    _thread_local.db_conn = conn
    return conn


def _has_fts5() -> bool:
    """Check if entities_fts virtual table exists."""
    global _HAS_FTS5
    if _HAS_FTS5 is None:
        db = _get_db()
        r = db.execute("SELECT name FROM sqlite_master WHERE name='entities_fts'").fetchone()
        _HAS_FTS5 = r is not None
    return _HAS_FTS5


def _has_fts5_trigram() -> bool:
    """Check if entities_fts_trigram virtual table exists."""
    global _HAS_FTS5_TRIGRAM
    if _HAS_FTS5_TRIGRAM is None:
        db = _get_db()
        r = db.execute("SELECT name FROM sqlite_master WHERE name='entities_fts_trigram'").fetchone()
        _HAS_FTS5_TRIGRAM = r is not None
    return _HAS_FTS5_TRIGRAM


def _has_rtree() -> bool:
    """Check if entities_rtree virtual table exists."""
    global _HAS_RTREE
    if _HAS_RTREE is None:
        db = _get_db()
        r = db.execute("SELECT name FROM sqlite_master WHERE name='entities_rtree'").fetchone()
        _HAS_RTREE = r is not None
    return _HAS_RTREE


def _fts_search(db, keyword: str, limit: int = 20):
    """FTS5 full-text search: trigram (>=3 chars) -> unicode61 -> LIKE fallback.

    Trigram tokenizer requires >=3 code-point queries. For shorter CJK queries
    (e.g. "北斎", "聖地"), fall back to unicode61 phrase match.
    """
    kw_len = len(keyword)
    if _has_fts5_trigram() and kw_len >= 3:
        safe_kw = keyword.replace("'", "''")
        return db.execute(
            'SELECT e.id, e.wikidata_id, e.label_ja, e.label_en, e.entity_type, e.source '
            'FROM entities e JOIN entities_fts_trigram f ON e.id = f.rowid '
            'WHERE entities_fts_trigram MATCH ? ORDER BY rank LIMIT ?',
            (safe_kw, limit),
        ).fetchall()
    if _has_fts5():
        safe_kw = keyword.replace('"', '""')
        return db.execute(
            'SELECT e.id, e.wikidata_id, e.label_ja, e.label_en, e.entity_type, e.source '
            'FROM entities e JOIN entities_fts f ON e.id = f.rowid '
            'WHERE entities_fts MATCH ? ORDER BY rank LIMIT ?',
            (f'"{safe_kw}"', limit),
        ).fetchall()
    return db.execute(
        "SELECT id, wikidata_id, label_ja, label_en, entity_type, source "
        "FROM entities WHERE label_ja LIKE ? LIMIT ?",
        (f"%{keyword}%", limit),
    ).fetchall()


def _rtree_nearby(db, lat: float, lon: float, radius_km: float, limit: int = 100):
    """R-Tree spatial search with fallback to bounding box."""
    lat_offset = radius_km / 111.0
    lon_offset = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))
    if _has_rtree():
        return db.execute("""
            SELECT e.id, e.label_ja, e.label_en, e.entity_type, e.lat, e.lon,
                   e.wikidata_id, e.source
            FROM entities e
            JOIN entities_rtree rt ON e.id = rt.id
            WHERE rt.min_lat BETWEEN ? AND ?
              AND rt.min_lon BETWEEN ? AND ?
            ORDER BY ABS(e.lat - ?) + ABS(e.lon - ?)
            LIMIT ?
        """, (lat - lat_offset, lat + lat_offset,
              lon - lon_offset, lon + lon_offset,
              lat, lon, limit)).fetchall()
    return db.execute("""
        SELECT id, label_ja, label_en, entity_type, lat, lon, wikidata_id, source
        FROM entities
        WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
        ORDER BY ABS(lat - ?) + ABS(lon - ?)
        LIMIT ?
    """, (lat - lat_offset, lat + lat_offset,
          lon - lon_offset, lon + lon_offset,
          lat, lon, limit)).fetchall()


# Simple LRU cache for repeated queries
@functools.lru_cache(maxsize=2048)
def _cached_entity_by_id(entity_id: int):
    db = _get_db()
    return db.execute(
        "SELECT id, label_ja, label_en, entity_type FROM entities WHERE id=?",
        (entity_id,)
    ).fetchone()


# ── 16. find_serendipity ─────────────────────────────────

@mcp.tool()
async def find_serendipity(
    keyword: str,
    max_results: int = 10,
    min_score: float = 0.3,
    search_mode: str = "auto",
) -> str:
    """エンティティ名またはテーマからセレンディピティ接続を探す。

    オントロジーDBの接続グラフから、意外で面白い文化的繋がりを発見する。
    5軸（テーマ・時代・媒体・地理・体験モード）の距離プロファイルに基づき、
    「テーマは近いが時代や媒体が遠い」ような"良い驚き"を優先して返す。
    LLM品質フィルタ済み（GPT-4o評価でkeep判定の接続のみ返す）。

    Args:
        keyword: エンティティ名またはテーマ名（部分一致、例: "北斎", "妖怪", "金閣寺"）
        max_results: 最大取得件数（1-50）
        min_score: 最低セレンディピティスコア（0.0-1.0）
        search_mode: 検索モード。"entity"=エンティティ名のみ、"theme"=テーマ値のみ、"auto"=エンティティ→テーマの順で検索
    """
    try:
        db = _get_db()
        max_results = max(1, min(max_results, 50))
        if search_mode not in ("entity", "theme", "auto"):
            search_mode = "auto"

        # Check if llm_verdict column exists
        cols = [r[1] for r in db.execute("PRAGMA table_info(connections)").fetchall()]
        has_llm = "llm_verdict" in cols

        # Find matching entities (FTS5 accelerated)
        entities = []
        theme_search_used = False
        if search_mode in ("entity", "auto"):
            entities = _fts_search(db, keyword, limit=50)

        if not entities and search_mode in ("theme", "auto"):
            # Fallback: search theme_values for keyword match, then find entities with that theme
            theme_match = db.execute(
                "SELECT code, name_ja, parent_code FROM theme_values WHERE name_ja LIKE ? OR name_en LIKE ? OR code LIKE ?",
                (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"),
            ).fetchone()
            if theme_match:
                # Collect theme codes: matched + children + siblings under same parent
                theme_codes = [theme_match["code"]]
                for child in db.execute(
                    "SELECT code FROM theme_values WHERE parent_code = ?",
                    (theme_match["code"],),
                ).fetchall():
                    theme_codes.append(child["code"])
                if not any(True for _ in theme_codes[1:]) and theme_match["parent_code"]:
                    # No children found — also search siblings
                    for sib in db.execute(
                        "SELECT code FROM theme_values WHERE parent_code = ? AND code != ?",
                        (theme_match["parent_code"], theme_match["code"]),
                    ).fetchall():
                        theme_codes.append(sib["code"])
                placeholders = ",".join("?" for _ in theme_codes)
                theme_entities = db.execute(
                    f"""SELECT DISTINCT e.id, e.wikidata_id, e.label_ja, e.label_en, e.entity_type
                       FROM entities e
                       JOIN entity_tags et ON e.id = et.entity_id
                       WHERE et.axis = 'theme' AND et.value_code IN ({placeholders})
                       LIMIT 5""",
                    theme_codes,
                ).fetchall()
                if theme_entities:
                    entities = theme_entities
                    theme_search_used = True
                else:
                    db.close()
                    return json.dumps({
                        "source": "ontology_db (find_serendipity)",
                        "query": keyword,
                        "matched_theme": {"code": theme_match["code"], "name_ja": theme_match["name_ja"]},
                        "searched_codes": theme_codes,
                        "error": "テーマは見つかりましたが、該当エンティティがありません",
                    }, ensure_ascii=False, indent=2)
            else:
                db.close()
                return json.dumps({
                    "source": "ontology_db (find_serendipity)",
                    "query": keyword,
                    "error": "エンティティが見つかりません",
                    "hint": "キーワードを短くするか、別の表記を試してください",
                }, ensure_ascii=False, indent=2)

        # Pick entity with most connections when multiple match
        if len(entities) > 1:
            best_entity = entities[0]
            best_count = 0
            for e in entities:
                cnt = db.execute(
                    "SELECT COUNT(*) FROM connections WHERE entity_a_id=? OR entity_b_id=?",
                    (e["id"], e["id"]),
                ).fetchone()[0]
                if cnt > best_count:
                    best_count = cnt
                    best_entity = e
            entity = best_entity
        else:
            entity = entities[0]
        eid = entity["id"]

        # Get this entity's tags
        my_tags: dict[str, list[str]] = {}
        for row in db.execute(
            "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (eid,)
        ):
            my_tags.setdefault(row["axis"], []).append(row["value_code"])

        # Resolve tag names
        def resolve_tag_name(axis: str, code: str) -> str:
            table = f"{axis}_values"
            try:
                row = db.execute(
                    f"SELECT name_ja FROM {table} WHERE code=?", (code,)
                ).fetchone()
                return row["name_ja"] if row else code
            except Exception:
                return code

        my_tags_display: dict[str, list[str]] = {}
        for axis, codes in my_tags.items():
            my_tags_display[axis] = [resolve_tag_name(axis, c) for c in codes]

        # Find connections: LLM-generated first, then rule-generated keep
        has_source = "source" in cols
        llm_filter = "AND c.llm_verdict = 'keep'" if has_llm else ""
        source_sort = "CASE WHEN c.source = 'llm_generated' THEN 0 ELSE 1 END," if has_source else ""
        quality_sort = "c.llm_serendipity_quality DESC," if has_llm else ""

        connections = db.execute(
            f"""SELECT c.*, a.label_ja AS a_label, a.entity_type AS a_type,
                      b.label_ja AS b_label, b.entity_type AS b_type
               FROM connections c
               JOIN entities a ON c.entity_a_id = a.id
               JOIN entities b ON c.entity_b_id = b.id
               WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                 AND c.serendipity_score >= ?
                 {llm_filter}
               ORDER BY {source_sort} {quality_sort} c.serendipity_score DESC
               LIMIT ?""",
            (eid, eid, min_score, max_results),
        ).fetchall()

        # Fallback: if LLM filter yields 0, try without filter
        if not connections and has_llm:
            connections = db.execute(
                f"""SELECT c.*, a.label_ja AS a_label, a.entity_type AS a_type,
                          b.label_ja AS b_label, b.entity_type AS b_type
                   FROM connections c
                   JOIN entities a ON c.entity_a_id = a.id
                   JOIN entities b ON c.entity_b_id = b.id
                   WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                     AND c.serendipity_score >= ?
                   ORDER BY {source_sort} c.serendipity_score DESC
                   LIMIT ?""",
                (eid, eid, min_score, max_results),
            ).fetchall()

        results = []
        for c in connections:
            # Determine which entity is the "other"
            if c["entity_a_id"] == eid:
                other_label = c["b_label"]
                other_type = c["b_type"]
                other_id = c["entity_b_id"]
            else:
                other_label = c["a_label"]
                other_type = c["a_type"]
                other_id = c["entity_a_id"]

            # Get other entity's tags
            other_tags: dict[str, list[str]] = {}
            for row in db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?",
                (other_id,),
            ):
                other_tags.setdefault(row["axis"], []).append(row["value_code"])

            other_tags_display: dict[str, list[str]] = {}
            for axis, codes in other_tags.items():
                other_tags_display[axis] = [resolve_tag_name(axis, c) for c in codes]

            result_item = {
                "connected_entity": other_label,
                "entity_type": other_type,
                "serendipity_score": round(c["serendipity_score"], 3),
                "connection_type": c["connection_type"],
                "explanation": c["explanation"],
                "distances": {
                    "theme": round(c["theme_distance"], 3) if c["theme_distance"] else None,
                    "era": round(c["era_distance"], 3) if c["era_distance"] else None,
                    "medium": round(c["medium_distance"], 3) if c["medium_distance"] else None,
                    "geography": round(c["geography_distance"], 3) if c["geography_distance"] else None,
                    "experience": round(c["experience_distance"], 3) if c["experience_distance"] else None,
                },
                "other_tags": other_tags_display,
            }
            if has_llm and c["llm_verdict"]:
                result_item["llm_verdict"] = c["llm_verdict"]
                result_item["llm_cultural_relevance"] = c["llm_cultural_relevance"]
                result_item["llm_serendipity_quality"] = c["llm_serendipity_quality"]
                result_item["llm_reason"] = c["llm_explanation"]
            results.append(result_item)

        # Also get alternative entity matches if multiple
        alt_entities = []
        if len(entities) > 1:
            for e in entities[1:5]:
                alt_entities.append({
                    "label_ja": e["label_ja"],
                    "entity_type": e["entity_type"],
                    "wikidata_id": e["wikidata_id"],
                })

        db.close()
        response_data = {
            "source": "ontology_db (find_serendipity)",
            "query": keyword,
            "search_mode": "theme" if theme_search_used else "entity",
            "llm_filtered": has_llm,
            "matched_entity": {
                "label_ja": entity["label_ja"],
                "label_en": entity["label_en"],
                "entity_type": entity["entity_type"],
                "wikidata_id": entity["wikidata_id"],
                "tags": my_tags_display,
            },
            "connections_found": len(results),
            "results": results,
        }
        if alt_entities:
            response_data["alternative_matches"] = alt_entities
        return json.dumps(response_data, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": f"find_serendipity failed: {str(e)}"})


# ── 17. explore_axis ──────────────────────────────────────

@mcp.tool()
async def explore_axis(
    axis: str,
    value: Optional[str] = None,
    entity_type: Optional[str] = None,
    limit: int = 20,
) -> str:
    """オントロジーの5軸（テーマ・時代・媒体・地理・体験モード）を探索する。

    軸の値一覧を表示したり、特定の軸値を持つエンティティを検索したりできる。
    「江戸時代の浮世絵師を探す」「妖怪テーマの作品一覧」などの発見的探索に最適。

    Args:
        axis: 軸名（"theme", "era", "medium", "geography", "experience"）
        value: 軸値コード（指定するとその値を持つエンティティを返す。省略すると値一覧を返す）
        entity_type: エンティティタイプでフィルタ（"person", "work", "place"。省略で全て）
        limit: 最大取得件数（エンティティ検索時、1-100）
    """
    valid_axes = {"theme", "era", "medium", "geography", "experience"}
    if axis not in valid_axes:
        return json.dumps({
            "error": f"Invalid axis: {axis}",
            "valid_axes": list(valid_axes),
        }, ensure_ascii=False, indent=2)

    try:
        db = _get_db()
        limit = max(1, min(limit, 100))
        table = f"{axis}_values"

        if value is None:
            # List all values for this axis with entity counts
            values = db.execute(
                f"""SELECT v.code, v.name_ja, v.name_en,
                           COUNT(DISTINCT et.entity_id) as entity_count
                    FROM {table} v
                    LEFT JOIN entity_tags et ON et.value_code = v.code AND et.axis = ?
                    GROUP BY v.code
                    ORDER BY entity_count DESC""",
                (axis,),
            ).fetchall()

            items = []
            for v in values:
                item: dict[str, Any] = {
                    "code": v["code"],
                    "name_ja": v["name_ja"],
                    "name_en": v["name_en"],
                    "entity_count": v["entity_count"],
                }
                # Add parent_code if applicable
                if axis in ("theme", "medium", "geography"):
                    try:
                        parent = db.execute(
                            f"SELECT parent_code FROM {table} WHERE code=?",
                            (v["code"],),
                        ).fetchone()
                        if parent and parent["parent_code"]:
                            item["parent_code"] = parent["parent_code"]
                    except Exception:
                        pass
                items.append(item)

            # Stats
            total_entities = db.execute(
                "SELECT COUNT(DISTINCT entity_id) FROM entity_tags WHERE axis=?",
                (axis,),
            ).fetchone()[0]

            db.close()
            return json.dumps({
                "source": "ontology_db (explore_axis)",
                "axis": axis,
                "mode": "list_values",
                "total_values": len(items),
                "total_tagged_entities": total_entities,
                "values": items,
            }, ensure_ascii=False, indent=2)

        else:
            # Search entities with this axis value
            # First resolve the value name
            value_info = db.execute(
                f"SELECT code, name_ja, name_en FROM {table} WHERE code=?",
                (value,),
            ).fetchone()

            if not value_info:
                # Try matching by Japanese name
                value_info = db.execute(
                    f"SELECT code, name_ja, name_en FROM {table} WHERE name_ja LIKE ?",
                    (f"%{value}%",),
                ).fetchone()
                if value_info:
                    value = value_info["code"]

            if not value_info:
                db.close()
                all_codes = [r["code"] for r in db.execute(f"SELECT code FROM {table}").fetchall()]
                return json.dumps({
                    "error": f"Value '{value}' not found in {axis} axis",
                    "available_values": all_codes,
                }, ensure_ascii=False, indent=2)

            # Query entities
            type_filter = ""
            params: list[Any] = [axis, value]
            if entity_type:
                type_filter = "AND e.entity_type = ?"
                params.append(entity_type)
            params.append(limit)

            entities = db.execute(
                f"""SELECT DISTINCT e.id, e.label_ja, e.label_en,
                           e.entity_type, e.wikidata_id,
                           et.confidence, et.source
                    FROM entities e
                    JOIN entity_tags et ON e.id = et.entity_id
                    WHERE et.axis = ? AND et.value_code = ?
                    {type_filter}
                    ORDER BY et.confidence DESC
                    LIMIT ?""",
                params,
            ).fetchall()

            # Get all tags for each entity
            results = []
            for e in entities:
                all_tags: dict[str, list[str]] = {}
                for row in db.execute(
                    "SELECT axis, value_code FROM entity_tags WHERE entity_id=?",
                    (e["id"],),
                ):
                    all_tags.setdefault(row["axis"], []).append(row["value_code"])

                results.append({
                    "label_ja": e["label_ja"],
                    "label_en": e["label_en"],
                    "entity_type": e["entity_type"],
                    "wikidata_id": e["wikidata_id"],
                    "confidence": round(e["confidence"], 2),
                    "tag_source": e["source"],
                    "all_tags": all_tags,
                })

            # Type breakdown
            type_counts: dict[str, int] = {}
            for r in results:
                t = r["entity_type"]
                type_counts[t] = type_counts.get(t, 0) + 1

            db.close()
            return json.dumps({
                "source": "ontology_db (explore_axis)",
                "axis": axis,
                "value": {
                    "code": value_info["code"],
                    "name_ja": value_info["name_ja"],
                    "name_en": value_info["name_en"],
                },
                "mode": "search_entities",
                "entity_type_filter": entity_type,
                "returned": len(results),
                "type_breakdown": type_counts,
                "entities": results,
            }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": f"explore_axis failed: {str(e)}"})


# ══════════════════════════════════════════════════════════
# Phase 7 Tools — Entity Detail + Cultural Route
# ══════════════════════════════════════════════════════════

# ── 18. get_entity_detail ────────────────────────────────

@mcp.tool()
async def get_entity_detail(
    entity_name: str,
) -> str:
    """オントロジーDBからエンティティの詳細情報を取得する。

    タグ（5軸）、接続先、外部ID、座標情報を含む包括的なプロファイルを返す。

    Args:
        entity_name: エンティティ名（部分一致、例: "葛飾北斎", "金閣寺"）
    """
    try:
        db = _get_db()

        # Find entity
        entities = db.execute(
            "SELECT id, label_ja, label_en, entity_type, wikidata_id, lat, lon "
            "FROM entities WHERE label_ja LIKE ? LIMIT 10",
            (f"%{entity_name}%",),
        ).fetchall()

        if not entities:
            db.close()
            return json.dumps({"error": f"'{entity_name}' が見つかりません"}, ensure_ascii=False)

        entity = entities[0]
        eid = entity["id"]

        # Tags
        tags = {}
        for row in db.execute(
            "SELECT axis, value_code, confidence, source FROM entity_tags WHERE entity_id=?",
            (eid,),
        ):
            tags.setdefault(row["axis"], []).append({
                "value": row["value_code"],
                "confidence": round(row["confidence"], 2),
            })

        # Connections
        connections = db.execute(
            """SELECT c.connection_type, c.serendipity_score, c.llm_explanation,
                      c.llm_verdict, c.source,
                      ea.label_ja as a_label, eb.label_ja as b_label
               FROM connections c
               JOIN entities ea ON c.entity_a_id = ea.id
               JOIN entities eb ON c.entity_b_id = eb.id
               WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                 AND c.llm_verdict = 'keep'
               ORDER BY c.serendipity_score DESC
               LIMIT 20""",
            (eid, eid),
        ).fetchall()

        conn_list = []
        for c in connections:
            other = c["b_label"] if c["a_label"] == entity["label_ja"] else c["a_label"]
            conn_list.append({
                "connected_to": other,
                "type": c["connection_type"],
                "score": round(c["serendipity_score"], 3) if c["serendipity_score"] else None,
                "explanation": c["llm_explanation"],
            })

        # Alt matches
        alt = [{"label_ja": e["label_ja"], "entity_type": e["entity_type"]} for e in entities[1:5]]

        db.close()
        result = {
            "source": "ontology_db (get_entity_detail)",
            "entity": {
                "label_ja": entity["label_ja"],
                "label_en": entity["label_en"],
                "entity_type": entity["entity_type"],
                "wikidata_id": entity["wikidata_id"],
                "lat": entity["lat"],
                "lon": entity["lon"],
            },
            "tags": tags,
            "connections": conn_list,
            "connection_count": len(conn_list),
        }
        if alt:
            result["alternative_matches"] = alt
        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": f"get_entity_detail failed: {str(e)}"})


# ── 19. get_cultural_route ───────────────────────────────

@mcp.tool()
async def get_cultural_route(
    theme: str = None,
    region: str = None,
    start_location: str = None,
    limit: int = 10,
) -> str:
    """文化テーマに基づいた聖地巡礼・文化ルートを生成する。

    SQLiteの座標付きエンティティ（10,000件の文化財・神社仏閣）と
    Google Maps APIを組み合わせて、テーマ性のある文化ルートを提案する。
    Google Maps API Keyがない場合はスポットリストのみ返す。

    Args:
        theme: テーマコード（例: "yokai", "sacred_profane", "ukiyoe_craft", "samurai"）
        region: 地域コード（例: "kanto", "kinki", "kyushu", "hokkaido"）
        start_location: 起点の地名（オプション）
        limit: 最大スポット数（1-20）
    """
    try:
        db = _get_db()
        limit = max(1, min(limit, 20))

        # Build query
        query = """
        SELECT DISTINCT e.id, e.label_ja, e.lat, e.lon, e.wikidata_id, e.entity_type
        FROM entities e
        JOIN entity_tags et_theme ON e.id = et_theme.entity_id AND et_theme.axis = 'theme'
        LEFT JOIN entity_tags et_geo ON e.id = et_geo.entity_id AND et_geo.axis = 'geography'
        WHERE e.lat IS NOT NULL AND e.lon IS NOT NULL
        """
        params = []

        if theme:
            query += " AND et_theme.value_code = ?"
            params.append(theme)

        if region:
            query += " AND et_geo.value_code = ?"
            params.append(region)

        query += " ORDER BY RANDOM()"
        query += f" LIMIT {limit}"

        spots = db.execute(query, params).fetchall()

        if not spots:
            db.close()
            return json.dumps({
                "error": f"テーマ={theme}, 地域={region} のスポットが見つかりません",
                "hint": "theme: yokai, shrine_temple, samurai, seasonal_beauty, ukiyoe_craft など / region: hokkaido, tohoku, kanto, chubu, kinki, chugoku, shikoku, kyushu",
            }, ensure_ascii=False, indent=2)

        enriched_spots = []
        for spot in spots:
            spot_data = {
                "name": spot["label_ja"],
                "lat": spot["lat"],
                "lon": spot["lon"],
                "wikidata_id": spot["wikidata_id"],
                "entity_type": spot["entity_type"],
            }

            # Get serendipity connections
            connections = db.execute(
                """SELECT c.llm_explanation,
                          CASE WHEN c.entity_a_id = ? THEN eb.label_ja ELSE ea.label_ja END as connected_to
                   FROM connections c
                   JOIN entities ea ON c.entity_a_id = ea.id
                   JOIN entities eb ON c.entity_b_id = eb.id
                   WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                     AND c.llm_verdict = 'keep'
                   ORDER BY c.serendipity_score DESC
                   LIMIT 3""",
                (spot["id"], spot["id"], spot["id"]),
            ).fetchall()

            spot_data["serendipity_connections"] = [
                {"explanation": c["llm_explanation"], "connected_to": c["connected_to"]}
                for c in connections
            ]

            # GSI map tile URL
            import math as _math
            zoom = 15
            n = 2 ** zoom
            x = int((spot["lon"] + 180.0) / 360.0 * n)
            lat_rad = _math.radians(spot["lat"])
            y = int((1.0 - _math.log(_math.tan(lat_rad) + 1.0 / _math.cos(lat_rad)) / _math.pi) / 2.0 * n)
            spot_data["map_tile_url"] = f"https://cyberjapandata.gsi.go.jp/xyz/std/{zoom}/{x}/{y}.png"

            enriched_spots.append(spot_data)

        # Try Google Maps route if API key is available
        route_info = None
        google_maps_enabled = bool(os.environ.get("GOOGLE_MAPS_API_KEY"))
        if google_maps_enabled and len(enriched_spots) >= 2:
            try:
                from server.google_maps_integration import GoogleMapsClient
                maps_client = GoogleMapsClient()
                waypoints = [{"lat": s["lat"], "lon": s["lon"]} for s in enriched_spots]
                route_info = await maps_client.compute_route(waypoints)
            except Exception as e:
                route_info = {"error": str(e)}

        db.close()
        return json.dumps({
            "source": "ontology_db + GSI" + (" + Google Maps" if google_maps_enabled else ""),
            "theme": theme,
            "region": region,
            "total_spots": len(enriched_spots),
            "spots": enriched_spots,
            "route": route_info,
            "google_maps_enabled": google_maps_enabled,
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": f"get_cultural_route failed: {str(e)}"})


# ── 20. search_culture (alias for cross_reference_v2) ────

@mcp.tool()
async def search_culture(
    keyword: str,
    sources: str = "all",
) -> str:
    """日本文化を横断検索する（cross_reference_v2のシンプル版）。

    まずオントロジーDBを検索し、次に外部APIで補完する。

    Args:
        keyword: 検索キーワード（例: "北斎", "妖怪", "金閣寺"）
        sources: "all", "db_only", "api_only"
    """
    results = {"query": keyword, "sources": {}}

    # 1. Ontology DB search (FTS5 accelerated)
    if sources in ("all", "db_only"):
        try:
            db = _get_db()
            entities = _fts_search(db, keyword, limit=20)

            results["sources"]["ontology_db"] = {
                "count": len(entities),
                "items": [
                    {"label_ja": e["label_ja"], "entity_type": e["entity_type"], "wikidata_id": e["wikidata_id"]}
                    for e in entities
                ],
            }
        except Exception as e:
            results["sources"]["ontology_db"] = {"error": str(e)}

    # 2. External API search (delegate to cross_reference_v2)
    if sources in ("all", "api_only"):
        try:
            cr_result = await cross_reference_v2(keyword, sources=["anilist", "wikidata"])
            cr_data = json.loads(cr_result)
            results["sources"]["external_apis"] = cr_data.get("sources", {})
        except Exception as e:
            results["sources"]["external_apis"] = {"error": str(e)}

    total = sum(
        s.get("count", 0) for s in results["sources"].values() if isinstance(s, dict) and "count" in s
    )
    results["total_results"] = total

    # Next steps suggestion based on results
    next_steps = []
    db_items = results.get("sources", {}).get("ontology_db", {}).get("items", [])
    has_works = any(i.get("entity_type") in ("work", "anime", "manga", "game") for i in db_items)
    has_places = any(i.get("entity_type") == "place" for i in db_items)

    if has_works:
        next_steps.append({
            "tool": "search_pilgrimage",
            "reason": "この作品の聖地巡礼スポットを検索できます",
        })
    if has_places:
        next_steps.append({
            "tool": "get_nearby_culture",
            "reason": "この場所の周辺文化資源を検索できます",
        })
    if total == 0:
        next_steps.append({
            "tool": "get_prefecture_profile",
            "reason": "地域名での検索をお試しください",
        })
    if total > 0:
        next_steps.append({
            "tool": "deep_dive",
            "reason": "特定のエンティティの詳細情報を取得できます",
        })
    results["next_steps"] = next_steps

    return json.dumps(results, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════
# Phase 9 Tools (21-28)
# ══════════════════════════════════════════════════════════


# ── 21. search_traditional_crafts ────────────────────────

@mcp.tool()
async def search_traditional_crafts(
    region: str = None,
    craft_type: str = None,
    keyword: str = None,
    limit: int = 10,
) -> str:
    """日本の伝統的工芸品（経産省指定244品目+関連）を検索する。

    産地、素材、技法、人間国宝との関連を返す。

    Args:
        region: 地域フィルタ（hokkaido, tohoku, kanto, chubu, kinki, chugoku, shikoku, kyushu）
        craft_type: 工芸種別（ceramics, textiles, lacquerware, metalwork, woodwork, paper, dolls）
        keyword: 検索キーワード（例: "漆", "染", "焼"）
        limit: 最大取得件数（1-50）
    """
    CRAFT_TYPE_MAP = {
        'ceramics': ['陶磁', '焼', '磁器'],
        'textiles': ['染織', '織', '染', '絣', '紬'],
        'lacquerware': ['漆', '塗'],
        'metalwork': ['金工', '鋳物', '打刃物'],
        'woodwork': ['木竹工', '指物', '曲物'],
        'paper': ['和紙', '紙'],
        'dolls': ['人形', '張子'],
    }
    try:
        db = _get_db()
        limit = min(limit, 50)

        conditions = ["e.entity_type = 'cultural_practice'"]
        params = []

        if keyword:
            conditions.append("e.label_ja LIKE ?")
            params.append(f"%{keyword}%")

        if craft_type and craft_type in CRAFT_TYPE_MAP:
            type_conditions = []
            for kw in CRAFT_TYPE_MAP[craft_type]:
                type_conditions.append("e.label_ja LIKE ?")
                params.append(f"%{kw}%")
            conditions.append(f"({' OR '.join(type_conditions)})")

        if region:
            conditions.append("""e.id IN (
                SELECT entity_id FROM entity_tags WHERE axis='geography' AND value_code=?
            )""")
            params.append(region)

        where = " AND ".join(conditions)
        rows = db.execute(f"""
            SELECT e.id, e.label_ja, e.label_en, e.entity_type, e.wikidata_id
            FROM entities e WHERE {where} LIMIT ?
        """, params + [limit]).fetchall()

        results = []
        for r in rows:
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (r["id"],)
            ).fetchall()
            tag_dict = {}
            for t in tags:
                tag_dict.setdefault(t["axis"], []).append(t["value_code"])
            results.append({
                "label_ja": r["label_ja"],
                "label_en": r["label_en"],
                "wikidata_id": r["wikidata_id"],
                "tags": tag_dict,
            })

        db.close()
        return json.dumps({
            "source": "ontology_db (search_traditional_crafts)",
            "query": {"region": region, "craft_type": craft_type, "keyword": keyword},
            "count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"search_traditional_crafts failed: {str(e)}"})


# ── 22. search_literature ────────────────────────────────

@mcp.tool()
async def search_literature(
    author: str = None,
    keyword: str = None,
    era: str = None,
    limit: int = 10,
) -> str:
    """青空文庫の日本文学作品（14,783作品）を検索する。

    作家名、キーワード、時代で絞り込み可能。
    アニメ化・映画化された原作の発見にも使える。

    Args:
        author: 作家名（例: "夏目漱石", "芥川龍之介"）
        keyword: 作品タイトルのキーワード
        era: 時代フィルタ（meiji_taisho, showa_prewar, showa_postwar, heisei）
        limit: 最大取得件数（1-50）
    """
    try:
        db = _get_db()
        limit = min(limit, 50)

        conditions = ["e.source = 'aozora_phase8'", "e.entity_type = 'work'"]
        params = []

        if keyword:
            conditions.append("e.label_ja LIKE ?")
            params.append(f"%{keyword}%")

        if author:
            conditions.append("""e.id IN (
                SELECT c.entity_b_id FROM connections c
                JOIN entities a ON c.entity_a_id = a.id
                WHERE a.label_ja LIKE ? AND a.entity_type = 'person'
                UNION
                SELECT c.entity_a_id FROM connections c
                JOIN entities a ON c.entity_b_id = a.id
                WHERE a.label_ja LIKE ? AND a.entity_type = 'person'
            )""")
            params.extend([f"%{author}%", f"%{author}%"])

        if not author:
            # Simple label search for author name in works
            if author:
                pass
            elif keyword:
                pass
            else:
                pass  # no additional filter

        if era:
            conditions.append("""e.id IN (
                SELECT entity_id FROM entity_tags WHERE axis='era' AND value_code=?
            )""")
            params.append(era)

        where = " AND ".join(conditions)
        rows = db.execute(f"""
            SELECT e.id, e.label_ja, e.label_en
            FROM entities e WHERE {where}
            ORDER BY e.label_ja LIMIT ?
        """, params + [limit]).fetchall()

        results = []
        for r in rows:
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (r["id"],)
            ).fetchall()
            tag_dict = {}
            for t in tags:
                tag_dict.setdefault(t["axis"], []).append(t["value_code"])
            results.append({
                "label_ja": r["label_ja"],
                "label_en": r["label_en"],
                "tags": tag_dict,
            })

        db.close()
        return json.dumps({
            "source": "ontology_db (search_literature)",
            "query": {"author": author, "keyword": keyword, "era": era},
            "count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"search_literature failed: {str(e)}"})


# ── 23. search_artworks ──────────────────────────────────

@mcp.tool()
async def search_artworks(
    artist: str = None,
    museum: str = None,
    medium: str = None,
    era: str = None,
    keyword: str = None,
    designation: str = None,
    limit: int = 10,
) -> str:
    """日本の美術作品を横断検索する（ToMuCo 35K + ColBase + 国宝・重文16K）。

    作品名、作者、所蔵館、時代、技法で検索。国宝・重要文化財のフィルタリング可能。

    Args:
        artist: 作者名
        museum: 所蔵館名
        medium: 技法（painting, ukiyoe, sculpture, craft, photography）
        era: 時代フィルタ
        keyword: 作品タイトルのキーワード
        designation: 文化財指定（national_treasure, important_cultural_property, any）
        limit: 最大取得件数（1-50）
    """
    try:
        db = _get_db()
        limit = min(limit, 50)

        conditions = ["e.entity_type IN ('artifact', 'work')"]
        params = []
        art_sources = ['tomuco_oai_phase8', 'tomuco_phase8', 'colbase_phase8', 'wikidata_cultural_phase8']

        if keyword:
            conditions.append("e.label_ja LIKE ?")
            params.append(f"%{keyword}%")

        if artist:
            conditions.append("e.label_ja LIKE ?")
            params.append(f"%{artist}%")

        if designation == 'national_treasure':
            conditions.append("e.source = 'wikidata_cultural_phase8'")
            conditions.append("e.label_ja LIKE '%国宝%'")
        elif designation == 'important_cultural_property':
            conditions.append("e.source = 'wikidata_cultural_phase8'")

        if medium:
            conditions.append("""e.id IN (
                SELECT entity_id FROM entity_tags WHERE axis='medium' AND value_code=?
            )""")
            params.append(medium)

        if era:
            conditions.append("""e.id IN (
                SELECT entity_id FROM entity_tags WHERE axis='era' AND value_code=?
            )""")
            params.append(era)

        where = " AND ".join(conditions)
        rows = db.execute(f"""
            SELECT e.id, e.label_ja, e.label_en, e.source
            FROM entities e WHERE {where}
            ORDER BY e.label_ja LIMIT ?
        """, params + [limit]).fetchall()

        results = []
        for r in rows:
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (r["id"],)
            ).fetchall()
            tag_dict = {}
            for t in tags:
                tag_dict.setdefault(t["axis"], []).append(t["value_code"])
            results.append({
                "label_ja": r["label_ja"],
                "label_en": r["label_en"],
                "source": r["source"],
                "tags": tag_dict,
            })

        db.close()
        return json.dumps({
            "source": "ontology_db (search_artworks)",
            "query": {"artist": artist, "museum": museum, "medium": medium,
                      "era": era, "keyword": keyword, "designation": designation},
            "count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"search_artworks failed: {str(e)}"})


# ── 24. search_festivals ─────────────────────────────────

@mcp.tool()
async def search_festivals(
    region: str = None,
    keyword: str = None,
    limit: int = 10,
) -> str:
    """日本の祭り・無形文化遺産（1,125件）を検索する。

    地域やキーワードで絞り込み可能。祭りと関連する文化財・場所との接続も返す。

    Args:
        region: 地域フィルタ（hokkaido, tohoku, kanto, chubu, kinki, chugoku, shikoku, kyushu）
        keyword: 検索キーワード（例: "火", "踊", "山車"）
        limit: 最大取得件数（1-50）
    """
    try:
        db = _get_db()
        limit = min(limit, 50)

        conditions = ["(e.entity_type = 'festival' OR "
                      "e.id IN (SELECT entity_id FROM entity_tags WHERE axis='theme' AND value_code='matsuri'))"]
        params = []

        if keyword:
            conditions.append("e.label_ja LIKE ?")
            params.append(f"%{keyword}%")

        if region:
            conditions.append("""e.id IN (
                SELECT entity_id FROM entity_tags WHERE axis='geography' AND value_code=?
            )""")
            params.append(region)

        where = " AND ".join(conditions)
        rows = db.execute(f"""
            SELECT e.id, e.label_ja, e.label_en, e.lat, e.lon
            FROM entities e WHERE {where}
            ORDER BY e.label_ja LIMIT ?
        """, params + [limit]).fetchall()

        results = []
        for r in rows:
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (r["id"],)
            ).fetchall()
            tag_dict = {}
            for t in tags:
                tag_dict.setdefault(t["axis"], []).append(t["value_code"])

            item = {
                "label_ja": r["label_ja"],
                "label_en": r["label_en"],
                "tags": tag_dict,
            }
            if r["lat"] and r["lon"]:
                item["coordinates"] = {"lat": r["lat"], "lon": r["lon"]}
            results.append(item)

        db.close()
        return json.dumps({
            "source": "ontology_db (search_festivals)",
            "query": {"region": region, "keyword": keyword},
            "count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"search_festivals failed: {str(e)}"})


# ── 25. search_living_national_treasures ─────────────────

@mcp.tool()
async def search_living_national_treasures(
    field: str = None,
    keyword: str = None,
    limit: int = 10,
) -> str:
    """人間国宝（重要無形文化財保持者）を検索する。

    工芸技術と芸能の両分野をカバー。関連する作品・工芸品との接続も返す。

    Args:
        field: 分野（ceramics, textiles, lacquer, metalwork, woodwork, noh, kabuki, bunraku, music）
        keyword: 検索キーワード
        limit: 最大取得件数（1-50）
    """
    FIELD_KEYWORDS = {
        'ceramics': ['陶', '磁', '焼'],
        'textiles': ['染', '織', '絣'],
        'lacquer': ['漆', '塗'],
        'metalwork': ['金工', '鋳', '鍛'],
        'woodwork': ['木', '竹'],
        'noh': ['能'],
        'kabuki': ['歌舞伎'],
        'bunraku': ['文楽', '人形浄瑠璃'],
        'music': ['音楽', '琴', '尺八', '三味線'],
    }
    try:
        db = _get_db()
        limit = min(limit, 50)

        conditions = [
            "e.entity_type = 'person'",
            "e.id IN (SELECT entity_id FROM entity_tags WHERE axis='theme' AND value_code='traditional_craft')",
        ]
        params = []

        if keyword:
            conditions.append("e.label_ja LIKE ?")
            params.append(f"%{keyword}%")

        if field and field in FIELD_KEYWORDS:
            field_conds = []
            for kw in FIELD_KEYWORDS[field]:
                field_conds.append("e.label_ja LIKE ?")
                params.append(f"%{kw}%")
            conditions.append(f"({' OR '.join(field_conds)})")

        where = " AND ".join(conditions)
        rows = db.execute(f"""
            SELECT e.id, e.label_ja, e.label_en
            FROM entities e WHERE {where}
            ORDER BY e.label_ja LIMIT ?
        """, params + [limit]).fetchall()

        results = []
        for r in rows:
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (r["id"],)
            ).fetchall()
            tag_dict = {}
            for t in tags:
                tag_dict.setdefault(t["axis"], []).append(t["value_code"])

            # Get connected works
            connections = db.execute("""
                SELECT e2.label_ja, c.explanation
                FROM connections c
                JOIN entities e2 ON (c.entity_a_id = e2.id OR c.entity_b_id = e2.id)
                WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                AND e2.id != ?
                AND c.llm_verdict = 'keep'
                LIMIT 5
            """, (r["id"], r["id"], r["id"])).fetchall()

            results.append({
                "label_ja": r["label_ja"],
                "label_en": r["label_en"],
                "tags": tag_dict,
                "connections": [{"entity": c["label_ja"], "explanation": c["explanation"]} for c in connections],
            })

        db.close()
        return json.dumps({
            "source": "ontology_db (search_living_national_treasures)",
            "query": {"field": field, "keyword": keyword},
            "count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"search_living_national_treasures failed: {str(e)}"})


# ── 26. generate_serendipity_route ───────────────────────

@mcp.tool()
async def generate_serendipity_route(
    start_keyword: str,
    depth: int = 5,
    region: str = None,
    mix_eras: bool = True,
    mix_media: bool = True,
) -> str:
    """セレンディピティルートを生成する。

    キーワードから出発し、接続グラフを辿って
    意外な文化的発見の連鎖を生成する。

    例: start="北斎", depth=5 →
      北斎 → 冨嶽三十六景 → 漫画の語源 → 手塚治虫 → お茶の水

    Args:
        start_keyword: 出発点のキーワード
        depth: 辿るステップ数（3-10）
        region: 地域制約（指定すると地域内に限定）
        mix_eras: 異なる時代を跨ぐことを優先
        mix_media: 異なるメディアを跨ぐことを優先
    """
    try:
        db = _get_db()
        depth = max(3, min(depth, 10))

        # Find starting entity
        start = db.execute(
            "SELECT id, label_ja, label_en, entity_type FROM entities WHERE label_ja LIKE ? LIMIT 1",
            (f"%{start_keyword}%",)
        ).fetchone()

        if not start:
            db.close()
            return json.dumps({"error": f"Entity not found: {start_keyword}"})

        route = []
        visited = {start["id"]}
        current_id = start["id"]

        # Get tags of current entity
        def get_tags(eid):
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (eid,)
            ).fetchall()
            result = {}
            for t in tags:
                result.setdefault(t["axis"], []).append(t["value_code"])
            return result

        current_tags = get_tags(start["id"])
        route.append({
            "step": 0,
            "entity": start["label_ja"],
            "entity_en": start["label_en"],
            "type": start["entity_type"],
            "tags": current_tags,
            "connection": None,
        })

        for step in range(1, depth + 1):
            # Get connections for current entity, sorted by serendipity
            connections = db.execute("""
                SELECT c.entity_a_id, c.entity_b_id, c.serendipity_score, c.explanation,
                       c.connection_type
                FROM connections c
                WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                AND c.llm_verdict = 'keep'
                ORDER BY c.serendipity_score DESC
                LIMIT 50
            """, (current_id, current_id)).fetchall()

            best_next = None
            best_score = -1

            for conn in connections:
                next_id = conn["entity_b_id"] if conn["entity_a_id"] == current_id else conn["entity_a_id"]
                if next_id in visited:
                    continue

                next_entity = db.execute(
                    "SELECT id, label_ja, label_en, entity_type FROM entities WHERE id=?",
                    (next_id,)
                ).fetchone()
                if not next_entity:
                    continue

                next_tags = get_tags(next_id)

                # Region filter
                if region and next_tags.get('geography'):
                    if region not in next_tags['geography']:
                        continue

                score = conn["serendipity_score"] or 0.5

                # Boost for era mixing
                if mix_eras and current_tags.get('era') and next_tags.get('era'):
                    if set(current_tags['era']) != set(next_tags['era']):
                        score += 0.3

                # Boost for media mixing
                if mix_media and current_tags.get('medium') and next_tags.get('medium'):
                    if set(current_tags['medium']) != set(next_tags['medium']):
                        score += 0.3

                if score > best_score:
                    best_score = score
                    best_next = (next_entity, conn, next_tags)

            if not best_next:
                break

            next_entity, conn, next_tags = best_next
            visited.add(next_entity["id"])
            current_id = next_entity["id"]
            current_tags = next_tags

            route.append({
                "step": step,
                "entity": next_entity["label_ja"],
                "entity_en": next_entity["label_en"],
                "type": next_entity["entity_type"],
                "tags": next_tags,
                "connection": conn["explanation"],
                "connection_type": conn["connection_type"],
            })

        # Generate narrative
        narrative_parts = []
        for i, stop in enumerate(route):
            if i == 0:
                narrative_parts.append(stop["entity"])
            else:
                narrative_parts.append(f"→ {stop['entity']}")

        db.close()
        return json.dumps({
            "source": "ontology_db (generate_serendipity_route)",
            "start": start_keyword,
            "depth_requested": depth,
            "steps_completed": len(route) - 1,
            "narrative": " ".join(narrative_parts),
            "route": route,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"generate_serendipity_route failed: {str(e)}"})


# ── 27. explore_connections ──────────────────────────────

@mcp.tool()
async def explore_connections(
    entity_name: str,
    max_depth: int = 2,
    limit_per_level: int = 5,
) -> str:
    """エンティティの接続グラフを探索する。

    指定したエンティティから出発し、max_depth階層まで接続を辿って返す。

    例: entity="千利休", max_depth=2 →
      Level 0: 千利休
      Level 1: 茶道、楽焼、豊臣秀吉
      Level 2: 京都、へうげもの(漫画)、信楽焼

    Args:
        entity_name: エンティティ名（部分一致可）
        max_depth: 探索深度（1-3）
        limit_per_level: 各階層の最大接続数（1-20）
    """
    try:
        db = _get_db()
        max_depth = max(1, min(max_depth, 3))
        limit_per_level = max(1, min(limit_per_level, 20))

        fts_results = _fts_search(db, entity_name, limit=1)
        start = fts_results[0] if fts_results else None

        if not start:
            return json.dumps({"error": f"Entity not found: {entity_name}"})

        levels = {}
        current_ids = {start["id"]}
        visited = {start["id"]}

        levels[0] = [{
            "entity": start["label_ja"],
            "entity_en": start["label_en"],
            "type": start["entity_type"],
        }]

        for depth in range(1, max_depth + 1):
            level_items = []
            next_ids = set()

            for cid in current_ids:
                conns = db.execute("""
                    SELECT c.entity_a_id, c.entity_b_id, c.explanation, c.serendipity_score
                    FROM connections c
                    WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                    AND c.llm_verdict = 'keep'
                    ORDER BY c.serendipity_score DESC
                    LIMIT ?
                """, (cid, cid, limit_per_level)).fetchall()

                for conn in conns:
                    next_id = conn["entity_b_id"] if conn["entity_a_id"] == cid else conn["entity_a_id"]
                    if next_id in visited:
                        continue

                    entity = db.execute(
                        "SELECT label_ja, label_en, entity_type FROM entities WHERE id=?",
                        (next_id,)
                    ).fetchone()
                    if not entity:
                        continue

                    visited.add(next_id)
                    next_ids.add(next_id)
                    level_items.append({
                        "entity": entity["label_ja"],
                        "entity_en": entity["label_en"],
                        "type": entity["entity_type"],
                        "connection": conn["explanation"],
                        "score": conn["serendipity_score"],
                    })

            levels[depth] = level_items[:limit_per_level * len(current_ids)]
            current_ids = next_ids

            if not next_ids:
                break

        db.close()
        return json.dumps({
            "source": "ontology_db (explore_connections)",
            "root": entity_name,
            "max_depth": max_depth,
            "levels": {str(k): v for k, v in levels.items()},
            "total_entities": sum(len(v) for v in levels.values()),
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"explore_connections failed: {str(e)}"})


# ── 28. get_culture_stats ────────────────────────────────

@mcp.tool()
async def get_culture_stats() -> str:
    """日本文化オントロジーの統計情報を返す。

    エンティティ数、接続数、ソース別内訳、テーマ分布、時代分布、地域分布を返す。
    """
    try:
        db = _get_db()

        total_entities = db.execute("SELECT COUNT(*) as c FROM entities").fetchone()["c"]
        total_connections = db.execute("SELECT COUNT(*) as c FROM connections WHERE llm_verdict='keep'").fetchone()["c"]

        # Source breakdown
        sources = db.execute(
            "SELECT source, COUNT(*) as c FROM entities GROUP BY source ORDER BY c DESC LIMIT 15"
        ).fetchall()

        # Entity type breakdown
        types = db.execute(
            "SELECT entity_type, COUNT(*) as c FROM entities GROUP BY entity_type ORDER BY c DESC LIMIT 10"
        ).fetchall()

        # Theme distribution (top 20)
        themes = db.execute("""
            SELECT value_code, COUNT(*) as c FROM entity_tags
            WHERE axis='theme' GROUP BY value_code ORDER BY c DESC LIMIT 20
        """).fetchall()

        # Era distribution
        eras = db.execute("""
            SELECT value_code, COUNT(*) as c FROM entity_tags
            WHERE axis='era' GROUP BY value_code ORDER BY c DESC
        """).fetchall()

        # Geography distribution
        geo = db.execute("""
            SELECT value_code, COUNT(*) as c FROM entity_tags
            WHERE axis='geography' GROUP BY value_code ORDER BY c DESC LIMIT 15
        """).fetchall()

        # English label coverage
        en_count = db.execute(
            "SELECT COUNT(*) as c FROM entities WHERE label_en IS NOT NULL AND label_en != ''"
        ).fetchone()["c"]

        # Axis coverage
        axis_coverage = {}
        for axis in ['theme', 'era', 'medium', 'geography', 'experience']:
            count = db.execute(
                "SELECT COUNT(DISTINCT entity_id) as c FROM entity_tags WHERE axis=?", (axis,)
            ).fetchone()["c"]
            axis_coverage[axis] = {"count": count, "percentage": round(100 * count / total_entities, 1)}

        db.close()
        return json.dumps({
            "source": "ontology_db (get_culture_stats)",
            "total_entities": total_entities,
            "total_keep_connections": total_connections,
            "connection_density": round(100 * total_connections / total_entities, 1),
            "english_label_coverage": round(100 * en_count / total_entities, 1),
            "sources": {r["source"]: r["c"] for r in sources},
            "entity_types": {r["entity_type"]: r["c"] for r in types},
            "theme_distribution": {r["value_code"]: r["c"] for r in themes},
            "era_distribution": {r["value_code"]: r["c"] for r in eras},
            "geography_distribution": {r["value_code"]: r["c"] for r in geo},
            "axis_coverage": axis_coverage,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"get_culture_stats failed: {str(e)}"})


# ── 29. search_pilgrimage ─────────────────────────────────

@mcp.tool()
async def search_pilgrimage(
    work_title: str = None,
    region: str = None,
    lat: float = None,
    lon: float = None,
    radius_km: float = 50.0,
    limit: int = 20,
) -> str:
    """聖地巡礼スポットを検索する。

    アニメ・漫画・映画・ゲームなどの作品の舞台やロケ地を検索する。
    作品名、地域、または座標で検索可能。

    Args:
        work_title: 作品名で検索（部分一致）。例: "君の名は", "スラムダンク", "鬼滅の刃"
        region: 地域コードで絞り込み。例: "kanto", "kinki", "chubu"
        lat: 緯度（座標検索の場合）
        lon: 経度（座標検索の場合）
        radius_km: 座標検索の半径（km、デフォルト50）
        limit: 最大結果数（1-50）
    """
    try:
        db = _get_db()
        limit = max(1, min(limit, 50))
        results = []

        if work_title:
            # Search by work title → find pilgrimage connections
            # Also search explanation text for katakana/romaji variants
            rows = db.execute("""
                SELECT DISTINCT
                    e_work.label_ja AS work_name,
                    e_work.entity_type AS work_type,
                    e_loc.label_ja AS location_name,
                    e_loc.lat, e_loc.lon,
                    c.connection_type,
                    c.explanation
                FROM connections c
                JOIN entities e_work ON (
                    (c.entity_a_id = e_work.id AND c.entity_b_id IN (SELECT id FROM entities WHERE lat IS NOT NULL))
                    OR (c.entity_b_id = e_work.id AND c.entity_a_id IN (SELECT id FROM entities WHERE lat IS NOT NULL))
                )
                JOIN entities e_loc ON (
                    (c.entity_a_id = e_loc.id AND e_loc.id != e_work.id)
                    OR (c.entity_b_id = e_loc.id AND e_loc.id != e_work.id)
                )
                WHERE c.connection_type LIKE 'pilgrimage%'
                AND (e_work.label_ja LIKE ? OR e_work.label_en LIKE ? OR c.explanation LIKE ?)
                AND e_loc.lat IS NOT NULL
                LIMIT ?
            """, (f"%{work_title}%", f"%{work_title}%", f"%{work_title}%", limit)).fetchall()

            for r in rows:
                spot = {
                    "work": r["work_name"],
                    "work_type": r["work_type"],
                    "location": r["location_name"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "type": r["connection_type"],
                    "description": r["explanation"],
                }
                if r["lat"] and r["lon"]:
                    zoom = 15
                    n = 2 ** zoom
                    x = int((r["lon"] + 180.0) / 360.0 * n)
                    lat_rad = math.radians(r["lat"])
                    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
                    spot["map_tile_url"] = f"https://cyberjapandata.gsi.go.jp/xyz/std/{zoom}/{x}/{y}.png"
                results.append(spot)

        elif lat is not None and lon is not None:
            # Search by coordinates — find pilgrimage spots near location
            # Approximate degree offset for radius
            lat_offset = radius_km / 111.0
            lon_offset = radius_km / (111.0 * math.cos(math.radians(lat)))

            rows = db.execute("""
                SELECT DISTINCT
                    e_loc.label_ja AS location_name,
                    e_loc.lat, e_loc.lon, e_loc.entity_type,
                    e_work.label_ja AS work_name,
                    c.explanation, c.connection_type
                FROM entities e_loc
                JOIN connections c ON (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
                JOIN entities e_work ON (
                    (c.entity_a_id = e_work.id AND e_work.id != e_loc.id)
                    OR (c.entity_b_id = e_work.id AND e_work.id != e_loc.id)
                )
                WHERE c.connection_type LIKE 'pilgrimage%'
                AND e_loc.lat BETWEEN ? AND ?
                AND e_loc.lon BETWEEN ? AND ?
                ORDER BY ABS(e_loc.lat - ?) + ABS(e_loc.lon - ?)
                LIMIT ?
            """, (
                lat - lat_offset, lat + lat_offset,
                lon - lon_offset, lon + lon_offset,
                lat, lon, limit,
            )).fetchall()

            for r in rows:
                dist_km = math.sqrt(
                    ((r["lat"] - lat) * 111.0) ** 2 +
                    ((r["lon"] - lon) * 111.0 * math.cos(math.radians(lat))) ** 2
                )
                results.append({
                    "location": r["location_name"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "work": r["work_name"],
                    "type": r["connection_type"],
                    "description": r["explanation"],
                    "distance_km": round(dist_km, 1),
                })

        else:
            # Browse all pilgrimage spots, optionally filtered by region
            region_filter = ""
            params = []
            if region:
                region_filter = """
                    AND e_loc.id IN (
                        SELECT entity_id FROM entity_tags WHERE axis='geography' AND value_code=?
                    )
                """
                params.append(region)

            rows = db.execute(f"""
                SELECT DISTINCT
                    e_work.label_ja AS work_name,
                    e_loc.label_ja AS location_name,
                    e_loc.lat, e_loc.lon,
                    c.explanation, c.connection_type
                FROM connections c
                JOIN entities e_work ON (c.entity_a_id = e_work.id OR c.entity_b_id = e_work.id)
                JOIN entities e_loc ON (
                    (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
                    AND e_loc.id != e_work.id
                )
                WHERE c.connection_type LIKE 'pilgrimage%'
                AND e_loc.lat IS NOT NULL
                {region_filter}
                ORDER BY RANDOM()
                LIMIT ?
            """, (*params, limit)).fetchall()

            for r in rows:
                results.append({
                    "work": r["work_name"],
                    "location": r["location_name"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "type": r["connection_type"],
                    "description": r["explanation"],
                })

        db.close()

        # Stats
        pilgrimage_count = 0
        try:
            db2 = _get_db()
            pilgrimage_count = db2.execute(
                "SELECT COUNT(*) as c FROM connections WHERE connection_type LIKE 'pilgrimage%'"
            ).fetchone()["c"]
            db2.close()
        except Exception:
            pass

        return json.dumps({
            "source": "ontology_db (search_pilgrimage)",
            "query": {"work_title": work_title, "region": region, "lat": lat, "lon": lon, "radius_km": radius_km},
            "total_pilgrimage_connections": pilgrimage_count,
            "results_count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"search_pilgrimage failed: {str(e)}"})


# ── 30. generate_pilgrimage_route ─────────────────────────

@mcp.tool()
async def generate_pilgrimage_route(
    work_title: str = None,
    theme: str = None,
    region: str = None,
    start_lat: float = None,
    start_lon: float = None,
    max_spots: int = 10,
    include_cultural: bool = True,
) -> str:
    """聖地巡礼ルートを生成する。

    アニメ・漫画などの聖地巡礼スポットと、周辺の文化スポット（神社仏閣・文化財等）を
    組み合わせたルートを生成する。Google Maps APIがあればルート情報も付加。

    Args:
        work_title: 作品名（例: "鬼滅の刃", "スラムダンク"）
        theme: テーマコード（yokai, samurai, seasonal_beauty等）。作品名と併用可
        region: 地域コード（kanto, kinki, chubu等）
        start_lat: 出発点の緯度（オプション）
        start_lon: 出発点の経度（オプション）
        max_spots: 最大スポット数（3-20）
        include_cultural: 聖地以外の文化スポットも含める（デフォルトTrue）
    """
    try:
        db = _get_db()
        max_spots = max(3, min(max_spots, 20))
        spots = []

        # Step 1: Get pilgrimage spots
        pilgrimage_query_parts = ["c.connection_type LIKE 'pilgrimage%'", "e_loc.lat IS NOT NULL"]
        params = []

        if work_title:
            pilgrimage_query_parts.append("(e_work.label_ja LIKE ? OR e_work.label_en LIKE ?)")
            params.append(f"%{work_title}%")
            params.append(f"%{work_title}%")

        if region:
            pilgrimage_query_parts.append("""
                e_loc.id IN (SELECT entity_id FROM entity_tags WHERE axis='geography' AND value_code=?)
            """)
            params.append(region)

        where_clause = " AND ".join(pilgrimage_query_parts)
        params.append(max_spots)

        pilgrimage_spots = db.execute(f"""
            SELECT DISTINCT
                e_work.label_ja AS work_name,
                e_loc.label_ja AS location_name,
                e_loc.lat, e_loc.lon,
                c.explanation, c.connection_type,
                e_loc.id AS loc_id
            FROM connections c
            JOIN entities e_work ON (c.entity_a_id = e_work.id OR c.entity_b_id = e_work.id)
            JOIN entities e_loc ON (
                (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
                AND e_loc.id != e_work.id
            )
            WHERE {where_clause}
            ORDER BY RANDOM()
            LIMIT ?
        """, params).fetchall()

        for ps in pilgrimage_spots:
            spots.append({
                "name": ps["location_name"],
                "lat": ps["lat"],
                "lon": ps["lon"],
                "type": "pilgrimage",
                "work": ps["work_name"],
                "description": ps["explanation"],
            })

        # Step 2: If include_cultural, add nearby cultural spots
        if include_cultural and spots:
            center_lat = sum(s["lat"] for s in spots) / len(spots)
            center_lon = sum(s["lon"] for s in spots) / len(spots)
            remaining = max_spots - len(spots)

            if remaining > 0:
                lat_offset = 0.5  # ~55km
                lon_offset = 0.5

                culture_filter = ""
                culture_params = [
                    center_lat - lat_offset, center_lat + lat_offset,
                    center_lon - lon_offset, center_lon + lon_offset,
                ]

                if theme:
                    culture_filter = "AND e.id IN (SELECT entity_id FROM entity_tags WHERE axis='theme' AND value_code=?)"
                    culture_params.append(theme)

                # Exclude pilgrimage spot IDs
                pilgrimage_loc_ids = [ps["loc_id"] for ps in pilgrimage_spots]
                if pilgrimage_loc_ids:
                    placeholders = ",".join("?" * len(pilgrimage_loc_ids))
                    culture_filter += f" AND e.id NOT IN ({placeholders})"
                    culture_params.extend(pilgrimage_loc_ids)

                culture_params.extend([center_lat, center_lon, remaining])

                cultural_spots = db.execute(f"""
                    SELECT e.label_ja, e.lat, e.lon, e.entity_type, e.wikidata_id
                    FROM entities e
                    WHERE e.lat IS NOT NULL AND e.lon IS NOT NULL
                    AND e.lat BETWEEN ? AND ?
                    AND e.lon BETWEEN ? AND ?
                    {culture_filter}
                    ORDER BY ABS(e.lat - ?) + ABS(e.lon - ?)
                    LIMIT ?
                """, culture_params).fetchall()

                for cs in cultural_spots:
                    spots.append({
                        "name": cs["label_ja"],
                        "lat": cs["lat"],
                        "lon": cs["lon"],
                        "type": "cultural",
                        "entity_type": cs["entity_type"],
                        "wikidata_id": cs["wikidata_id"],
                    })

        # Step 3: Sort by geography (simple nearest-neighbor)
        if spots and start_lat is not None and start_lon is not None:
            ordered = []
            remaining = list(spots)
            current = {"lat": start_lat, "lon": start_lon}
            while remaining:
                nearest = min(remaining, key=lambda s: (
                    (s["lat"] - current["lat"]) ** 2 + (s["lon"] - current["lon"]) ** 2
                ))
                ordered.append(nearest)
                remaining.remove(nearest)
                current = nearest
            spots = ordered

        # Step 4: Add map tiles
        for spot in spots:
            if spot.get("lat") and spot.get("lon"):
                zoom = 15
                n = 2 ** zoom
                x = int((spot["lon"] + 180.0) / 360.0 * n)
                lat_rad = math.radians(spot["lat"])
                y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
                spot["map_tile_url"] = f"https://cyberjapandata.gsi.go.jp/xyz/std/{zoom}/{x}/{y}.png"

        # Step 5: Google Maps route (if available)
        route_info = None
        google_maps_enabled = bool(os.environ.get("GOOGLE_MAPS_API_KEY"))
        if google_maps_enabled and len(spots) >= 2:
            try:
                from server.google_maps_integration import GoogleMapsClient
                maps_client = GoogleMapsClient()
                waypoints = [{"lat": s["lat"], "lon": s["lon"]} for s in spots]
                route_info = await maps_client.compute_route(waypoints)
            except Exception as e:
                route_info = {"error": str(e)}

        db.close()
        return json.dumps({
            "source": "ontology_db (generate_pilgrimage_route)" + (" + Google Maps" if google_maps_enabled else ""),
            "query": {"work_title": work_title, "theme": theme, "region": region},
            "total_spots": len(spots),
            "pilgrimage_spots": sum(1 for s in spots if s.get("type") == "pilgrimage"),
            "cultural_spots": sum(1 for s in spots if s.get("type") == "cultural"),
            "spots": spots,
            "route": route_info,
            "google_maps_enabled": google_maps_enabled,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"generate_pilgrimage_route failed: {str(e)}"})


# ── 31. get_nearby_culture ────────────────────────────────

@mcp.tool()
async def get_nearby_culture(
    lat: float,
    lon: float,
    radius_km: float = 10.0,
    entity_type: str = None,
    theme: str = None,
    limit: int = 20,
) -> str:
    """指定座標の周辺にある文化リソースを検索する。

    神社仏閣、文化財、アニメ聖地、博物館、伝統工芸、歴史的建造物など、
    オントロジーDB内の座標付きエンティティを距離順で返す。

    Args:
        lat: 緯度（必須）。例: 35.6762（東京）
        lon: 経度（必須）。例: 139.6503（東京）
        radius_km: 検索半径（km、デフォルト10、最大200）
        entity_type: エンティティタイプで絞り込み（place, shrine, temple, artwork等）
        theme: テーマコードで絞り込み（yokai, samurai, seasonal_beauty等）
        limit: 最大結果数（1-100）
    """
    try:
        db = _get_db()
        radius_km = max(0.1, min(radius_km, 200.0))
        limit = max(1, min(limit, 100))

        lat_offset = radius_km / 111.0
        lon_offset = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))

        # Use R-Tree spatial index if available
        if _has_rtree() and not entity_type and not theme:
            rows = _rtree_nearby(db, lat, lon, radius_km, limit=limit * 2)
        else:
            filters = [
                "e.lat IS NOT NULL",
                "e.lon IS NOT NULL",
                "e.lat BETWEEN ? AND ?",
                "e.lon BETWEEN ? AND ?",
            ]
            params = [lat - lat_offset, lat + lat_offset, lon - lon_offset, lon + lon_offset]

            if entity_type:
                filters.append("e.entity_type = ?")
                params.append(entity_type)

            if theme:
                filters.append("e.id IN (SELECT entity_id FROM entity_tags WHERE axis='theme' AND value_code=?)")
                params.append(theme)

            where_clause = " AND ".join(filters)
            params.extend([lat, lon, limit * 2])

            rows = db.execute(f"""
                SELECT e.id, e.label_ja, e.label_en, e.entity_type, e.lat, e.lon,
                       e.wikidata_id, e.source
                FROM entities e
                WHERE {where_clause}
                ORDER BY ABS(e.lat - ?) + ABS(e.lon - ?)
                LIMIT ?
            """, params).fetchall()

        results = []
        for r in rows:
            dist_km = math.sqrt(
                ((r["lat"] - lat) * 111.0) ** 2 +
                ((r["lon"] - lon) * 111.0 * math.cos(math.radians(lat))) ** 2
            )
            if dist_km > radius_km:
                continue

            item = {
                "name": r["label_ja"],
                "name_en": r["label_en"],
                "entity_type": r["entity_type"],
                "lat": r["lat"],
                "lon": r["lon"],
                "distance_km": round(dist_km, 2),
                "wikidata_id": r["wikidata_id"],
                "source": r["source"],
            }

            # Get tags
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (r["id"],)
            ).fetchall()
            tag_dict = {}
            for t in tags:
                tag_dict.setdefault(t["axis"], []).append(t["value_code"])
            item["tags"] = tag_dict

            # Check pilgrimage connections
            pilgrim = db.execute("""
                SELECT e2.label_ja AS work_name, c.explanation
                FROM connections c
                JOIN entities e2 ON (
                    (c.entity_a_id = e2.id AND c.entity_b_id = ?)
                    OR (c.entity_b_id = e2.id AND c.entity_a_id = ?)
                )
                WHERE c.connection_type LIKE 'pilgrimage%'
                AND e2.id != ?
                LIMIT 3
            """, (r["id"], r["id"], r["id"])).fetchall()

            if pilgrim:
                item["pilgrimage_works"] = [
                    {"work": p["work_name"], "description": p["explanation"]}
                    for p in pilgrim
                ]

            # Map tile
            zoom = 15
            n = 2 ** zoom
            x = int((r["lon"] + 180.0) / 360.0 * n)
            lat_rad = math.radians(r["lat"])
            y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
            item["map_tile_url"] = f"https://cyberjapandata.gsi.go.jp/xyz/std/{zoom}/{x}/{y}.png"

            results.append(item)

        db.close()
        return json.dumps({
            "source": "ontology_db (get_nearby_culture)",
            "query": {"lat": lat, "lon": lon, "radius_km": radius_km, "entity_type": entity_type, "theme": theme},
            "results_count": len(results),
            "results": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"get_nearby_culture failed: {str(e)}"})


# ══════════════════════════════════════════════════════════
# Phase 14 Tools — New Features (v1.1.0)
# ══════════════════════════════════════════════════════════

# ── Era lookup for timeline ─────────────────────────────
_ERA_RANGES = [
    ("ancient", "古代", -10000, 1185),
    ("medieval", "中世", 1185, 1573),
    ("edo_early", "近世前期", 1573, 1700),
    ("edo_late", "近世後期", 1700, 1868),
    ("meiji_taisho", "明治大正", 1868, 1926),
    ("showa_prewar", "昭和戦前", 1926, 1945),
    ("showa_postwar", "昭和戦後", 1945, 1989),
    ("heisei", "平成", 1989, 2019),
    ("reiwa", "令和", 2019, 2100),
]


def _era_to_year(era_code: str) -> int:
    """Convert era code to representative year (midpoint)."""
    for code, _, y_from, y_to in _ERA_RANGES:
        if code == era_code:
            return (y_from + y_to) // 2
    return 0


# ── 32. generate_timeline ─────────────────────────────────

@mcp.tool()
async def generate_timeline(
    theme: str,
    region: str = None,
    start_year: int = None,
    end_year: int = None,
    max_events: int = 20,
) -> str:
    """任意のテーマの文化的時系列を生成する。

    テーマ（浮世絵、アニメ、茶道等）を指定すると、関連するエンティティを
    時代順に並べたタイムラインを返す。地域や年代で絞り込み可能。

    Args:
        theme: テーマキーワード（例: "浮世絵", "アニメ", "京都", "茶道"）
        region: 地域フィルタ（例: "京都", "東北"）。指定時はその地域に関連するもののみ。
        start_year: 開始年フィルタ（例: 1600）
        end_year: 終了年フィルタ（例: 1900）
        max_events: 最大イベント数（1-50、default: 20）
    """
    try:
        db = _get_db()
        max_events = max(1, min(max_events, 50))

        # 1. FTS5 search for theme
        entities = _fts_search(db, theme, limit=200)
        if not entities:
            return json.dumps({"error": f"No entities found for theme: {theme}"}, ensure_ascii=False)

        entity_ids = [e["id"] for e in entities]

        # 2. Get era tags for matched entities
        timeline_entries = []
        batch_size = 100
        for i in range(0, len(entity_ids), batch_size):
            batch = entity_ids[i:i + batch_size]
            placeholders = ",".join("?" for _ in batch)
            rows = db.execute(f"""
                SELECT et.entity_id, et.value_code, e.label_ja, e.label_en,
                       e.entity_type, e.lat, e.lon, e.source
                FROM entity_tags et
                JOIN entities e ON et.entity_id = e.id
                WHERE et.axis = 'era' AND et.entity_id IN ({placeholders})
            """, batch).fetchall()

            for r in rows:
                year = _era_to_year(r["value_code"])
                if start_year and year < start_year:
                    continue
                if end_year and year > end_year:
                    continue
                timeline_entries.append({
                    "entity_id": r["entity_id"],
                    "label_ja": r["label_ja"],
                    "label_en": r["label_en"],
                    "entity_type": r["entity_type"],
                    "era_code": r["value_code"],
                    "approximate_year": year,
                    "lat": r["lat"],
                    "lon": r["lon"],
                })

        # 3. Optional region filter
        if region:
            region_entities = _fts_search(db, region, limit=500)
            region_ids = {e["id"] for e in region_entities}
            # Also filter by geography tags
            geo_ids = set()
            for r in db.execute(
                "SELECT entity_id FROM entity_tags WHERE axis='geography' AND value_code LIKE ?",
                (f"%{region}%",)
            ).fetchall():
                geo_ids.add(r["entity_id"])
            allowed = region_ids | geo_ids
            if allowed:
                timeline_entries = [t for t in timeline_entries if t["entity_id"] in allowed]

        # 4. Deduplicate and sort by year
        seen = set()
        unique = []
        for t in timeline_entries:
            if t["entity_id"] not in seen:
                seen.add(t["entity_id"])
                unique.append(t)
        unique.sort(key=lambda x: x["approximate_year"])

        # 5. Get connection context for top entries
        result_entries = unique[:max_events]
        for entry in result_entries:
            conns = db.execute("""
                SELECT c.connection_type, c.explanation, c.serendipity_score,
                       e2.label_ja AS connected_to
                FROM connections c
                JOIN entities e2 ON (
                    CASE WHEN c.entity_a_id = ? THEN c.entity_b_id ELSE c.entity_a_id END = e2.id
                )
                WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                AND c.llm_verdict = 'keep'
                ORDER BY c.serendipity_score DESC
                LIMIT 3
            """, (entry["entity_id"], entry["entity_id"], entry["entity_id"])).fetchall()
            if conns:
                entry["connections"] = [
                    {"type": c["connection_type"], "to": c["connected_to"],
                     "explanation": c["explanation"]}
                    for c in conns
                ]

        # Map era codes to readable names
        era_names = {code: name_ja for code, name_ja, _, _ in _ERA_RANGES}
        for entry in result_entries:
            entry["era_name"] = era_names.get(entry["era_code"], entry["era_code"])

        return json.dumps({
            "source": "ontology_db (generate_timeline)",
            "theme": theme,
            "region": region,
            "year_range": {"start": start_year, "end": end_year},
            "total_found": len(unique),
            "results_count": len(result_entries),
            "timeline": result_entries,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"generate_timeline failed: {str(e)}"})


# ── 33. compare_cultures ──────────────────────────────────

@mcp.tool()
async def compare_cultures(
    entity_a: str,
    entity_b: str,
    depth: int = 2,
) -> str:
    """2つの文化要素を比較して共通点・相違点・意外な接続を発見する。

    2つのエンティティやテーマを指定すると、接続グラフを探索して
    共通する文化的要素、それぞれ固有の要素、そして意外な繋がりを返す。

    Args:
        entity_a: 比較対象A（例: "京都", "浮世絵", "能"）
        entity_b: 比較対象B（例: "金沢", "印象派", "歌舞伎"）
        depth: 探索深度 1-3（default: 2）。深いほど間接的な接続も発見。
    """
    try:
        db = _get_db()
        depth = max(1, min(depth, 3))

        def get_entity_graph(keyword, depth_level):
            """Get entity and its connection graph up to given depth."""
            entities = _fts_search(db, keyword, limit=10)
            if not entities:
                return None, set(), []

            primary = entities[0]
            primary_id = primary["id"]
            visited = {primary_id}
            all_connections = []
            frontier = {primary_id}

            for d in range(depth_level):
                if not frontier:
                    break
                next_frontier = set()
                for eid in frontier:
                    conns = db.execute("""
                        SELECT c.*, e_other.label_ja, e_other.label_en, e_other.entity_type,
                               e_other.id AS other_id
                        FROM connections c
                        JOIN entities e_other ON (
                            CASE WHEN c.entity_a_id = ? THEN c.entity_b_id
                                 ELSE c.entity_a_id END = e_other.id
                        )
                        WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                        AND c.llm_verdict = 'keep'
                        ORDER BY c.serendipity_score DESC
                        LIMIT 20
                    """, (eid, eid, eid)).fetchall()

                    for c in conns:
                        other_id = c["other_id"]
                        all_connections.append({
                            "from_id": eid,
                            "to_id": other_id,
                            "to_label": c["label_ja"],
                            "to_type": c["entity_type"],
                            "connection_type": c["connection_type"],
                            "explanation": c["explanation"],
                            "score": c["serendipity_score"],
                        })
                        if other_id not in visited:
                            visited.add(other_id)
                            next_frontier.add(other_id)
                frontier = next_frontier

            return primary, visited, all_connections

        # Get graphs for both entities
        primary_a, ids_a, conns_a = get_entity_graph(entity_a, depth)
        primary_b, ids_b, conns_b = get_entity_graph(entity_b, depth)

        if not primary_a:
            return json.dumps({"error": f"Entity not found: {entity_a}"}, ensure_ascii=False)
        if not primary_b:
            return json.dumps({"error": f"Entity not found: {entity_b}"}, ensure_ascii=False)

        # Find common connected entities
        common_ids = ids_a & ids_b
        common_ids.discard(primary_a["id"])
        common_ids.discard(primary_b["id"])

        common_entities = []
        for cid in list(common_ids)[:20]:
            e = _cached_entity_by_id(cid)
            if e:
                common_entities.append({
                    "label_ja": e["label_ja"],
                    "label_en": e["label_en"],
                    "entity_type": e["entity_type"],
                })

        # Unique to each
        only_a = ids_a - ids_b - {primary_a["id"]}
        only_b = ids_b - ids_a - {primary_b["id"]}

        unique_a = []
        for uid in list(only_a)[:10]:
            e = _cached_entity_by_id(uid)
            if e:
                unique_a.append({"label_ja": e["label_ja"], "entity_type": e["entity_type"]})

        unique_b = []
        for uid in list(only_b)[:10]:
            e = _cached_entity_by_id(uid)
            if e:
                unique_b.append({"label_ja": e["label_ja"], "entity_type": e["entity_type"]})

        # Get tags comparison
        def get_tags(entity_id):
            tags = db.execute(
                "SELECT axis, value_code FROM entity_tags WHERE entity_id = ?",
                (entity_id,)
            ).fetchall()
            result = {}
            for t in tags:
                result.setdefault(t["axis"], []).append(t["value_code"])
            return result

        tags_a = get_tags(primary_a["id"])
        tags_b = get_tags(primary_b["id"])

        # Find common and different tags
        common_tags = {}
        diff_tags = {"a_only": {}, "b_only": {}}
        all_axes = set(tags_a.keys()) | set(tags_b.keys())
        for axis in all_axes:
            vals_a = set(tags_a.get(axis, []))
            vals_b = set(tags_b.get(axis, []))
            common = vals_a & vals_b
            if common:
                common_tags[axis] = list(common)
            a_only = vals_a - vals_b
            b_only = vals_b - vals_a
            if a_only:
                diff_tags["a_only"][axis] = list(a_only)
            if b_only:
                diff_tags["b_only"][axis] = list(b_only)

        return json.dumps({
            "source": "ontology_db (compare_cultures)",
            "entity_a": {
                "label_ja": primary_a["label_ja"],
                "label_en": primary_a["label_en"],
                "entity_type": primary_a["entity_type"],
                "total_connections": len(ids_a) - 1,
            },
            "entity_b": {
                "label_ja": primary_b["label_ja"],
                "label_en": primary_b["label_en"],
                "entity_type": primary_b["entity_type"],
                "total_connections": len(ids_b) - 1,
            },
            "common_elements": {
                "count": len(common_entities),
                "entities": common_entities,
                "shared_tags": common_tags,
            },
            "unique_to_a": {
                "count": len(only_a),
                "sample": unique_a,
                "unique_tags": diff_tags["a_only"],
            },
            "unique_to_b": {
                "count": len(only_b),
                "sample": unique_b,
                "unique_tags": diff_tags["b_only"],
            },
            "depth": depth,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"compare_cultures failed: {str(e)}"})


# ── 34. generate_culture_map ──────────────────────────────

@mcp.tool()
async def generate_culture_map(
    theme: str = None,
    region: str = None,
    entity_type: str = None,
    work: str = None,
    max_features: int = 100,
) -> str:
    """テーマ別の文化地図データをGeoJSON形式で生成する。

    指定テーマの文化要素を地図上にマッピングしたGeoJSON FeatureCollectionを返す。
    聖地巡礼マップ、伝統工芸分布、祭り地図等に使用可能。

    Args:
        theme: テーマキーワード（例: "陶磁器", "国宝", "祭り"）
        region: 地域フィルタ（例: "東北", "京都"）
        entity_type: エンティティタイプフィルタ（例: "place", "building", "festival"）
        work: 作品名（聖地巡礼マップ用。例: "スラムダンク", "君の名は。"）
        max_features: 最大フィーチャー数（1-500、default: 100）
    """
    try:
        db = _get_db()
        max_features = max(1, min(max_features, 500))
        features = []

        if work:
            # Pilgrimage mode: find locations connected to the work
            work_entities = _fts_search(db, work, limit=5)
            if not work_entities:
                return json.dumps({"error": f"Work not found: {work}"}, ensure_ascii=False)

            for we in work_entities:
                rows = db.execute("""
                    SELECT e.id, e.label_ja, e.label_en, e.entity_type,
                           e.lat, e.lon, c.connection_type, c.explanation
                    FROM connections c
                    JOIN entities e ON (
                        CASE WHEN c.entity_a_id = ? THEN c.entity_b_id
                             ELSE c.entity_a_id END = e.id
                    )
                    WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
                    AND c.connection_type LIKE 'pilgrimage%'
                    AND e.lat IS NOT NULL AND e.lon IS NOT NULL
                    LIMIT ?
                """, (we["id"], we["id"], we["id"], max_features)).fetchall()

                for r in rows:
                    features.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [r["lon"], r["lat"]],
                        },
                        "properties": {
                            "name": r["label_ja"],
                            "name_en": r["label_en"],
                            "entity_type": r["entity_type"],
                            "connection_type": r["connection_type"],
                            "description": r["explanation"],
                            "work": we["label_ja"],
                        },
                    })
        else:
            # Theme/region search mode
            search_kw = theme or region or ""
            if not search_kw:
                return json.dumps({"error": "At least one of theme, region, or work must be provided"}, ensure_ascii=False)

            entities = _fts_search(db, search_kw, limit=500)

            for e in entities:
                eid = e["id"]
                # Get full entity with coords
                full = db.execute(
                    "SELECT id, label_ja, label_en, entity_type, lat, lon, source "
                    "FROM entities WHERE id = ? AND lat IS NOT NULL AND lon IS NOT NULL",
                    (eid,)
                ).fetchone()
                if not full:
                    continue
                if entity_type and full["entity_type"] != entity_type:
                    continue

                props = {
                    "name": full["label_ja"],
                    "name_en": full["label_en"],
                    "entity_type": full["entity_type"],
                    "source": full["source"],
                }

                # Get tags
                tags = db.execute(
                    "SELECT axis, value_code FROM entity_tags WHERE entity_id = ?",
                    (eid,)
                ).fetchall()
                if tags:
                    tag_dict = {}
                    for t in tags:
                        tag_dict.setdefault(t["axis"], []).append(t["value_code"])
                    props["tags"] = tag_dict

                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [full["lon"], full["lat"]],
                    },
                    "properties": props,
                })

                if len(features) >= max_features:
                    break

        geojson = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "source": "ontology_db (generate_culture_map)",
                "query": {
                    "theme": theme, "region": region,
                    "entity_type": entity_type, "work": work,
                },
                "total_features": len(features),
            },
        }
        return json.dumps(geojson, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"generate_culture_map failed: {str(e)}"})


# ── 35. today_in_culture ──────────────────────────────────

# Major Japanese cultural calendar (month → events)
_CULTURAL_CALENDAR = {
    1: [
        {"name": "正月", "type": "event", "description": "新年の祝い。初詣、おせち料理、門松。"},
        {"name": "成人の日", "type": "event", "description": "新成人を祝う国民の祝日（1月第2月曜日）。"},
        {"name": "七草粥", "type": "food", "description": "1月7日に七草粥を食べる風習。"},
    ],
    2: [
        {"name": "節分", "type": "event", "description": "2月3日頃。豆まき、恵方巻き。立春の前日。"},
        {"name": "札幌雪まつり", "type": "festival", "description": "2月上旬。大通公園の大雪像。"},
        {"name": "梅の花見", "type": "event", "description": "梅の花が咲き始める季節。"},
    ],
    3: [
        {"name": "ひな祭り", "type": "event", "description": "3月3日。桃の節句。雛人形を飾る。"},
        {"name": "お水取り", "type": "event", "description": "東大寺二月堂の修二会。3月1-14日。"},
        {"name": "桜の開花", "type": "event", "description": "3月下旬から桜前線が北上開始。"},
    ],
    4: [
        {"name": "花見", "type": "event", "description": "桜の下で宴会。日本の春の風物詩。"},
        {"name": "高山祭", "type": "festival", "description": "4月14-15日。岐阜県高山市。日本三大美祭。"},
    ],
    5: [
        {"name": "端午の節句", "type": "event", "description": "5月5日。こどもの日。鯉のぼり、柏餅。"},
        {"name": "三社祭", "type": "festival", "description": "5月第3週末。浅草神社。東京最大の祭り。"},
        {"name": "葵祭", "type": "festival", "description": "5月15日。京都三大祭の一つ。"},
    ],
    6: [
        {"name": "梅雨入り", "type": "event", "description": "雨季の始まり。紫陽花の季節。"},
        {"name": "YOSAKOIソーラン祭り", "type": "festival", "description": "6月上旬。札幌。"},
    ],
    7: [
        {"name": "七夕", "type": "event", "description": "7月7日。織姫と彦星の伝説。短冊に願い事。"},
        {"name": "祇園祭", "type": "festival", "description": "7月1-31日。京都八坂神社。日本三大祭。"},
        {"name": "隅田川花火大会", "type": "festival", "description": "7月最終土曜日。東京の花火大会。"},
    ],
    8: [
        {"name": "お盆", "type": "event", "description": "8月13-16日。先祖の霊を迎える。盆踊り。"},
        {"name": "阿波おどり", "type": "festival", "description": "8月12-15日。徳島市。日本最大の盆踊り。"},
        {"name": "ねぶた祭", "type": "festival", "description": "8月2-7日。青森。巨大灯籠。"},
        {"name": "コミックマーケット", "type": "event", "description": "8月中旬。世界最大の同人誌即売会。"},
    ],
    9: [
        {"name": "お月見", "type": "event", "description": "中秋の名月を愛でる。団子、ススキ。"},
        {"name": "岸和田だんじり祭", "type": "festival", "description": "9月中旬。大阪。勇壮な山車。"},
    ],
    10: [
        {"name": "紅葉狩り", "type": "event", "description": "紅葉を楽しむ。京都が特に有名。"},
        {"name": "時代祭", "type": "festival", "description": "10月22日。京都三大祭の一つ。"},
        {"name": "秋祭り", "type": "event", "description": "収穫を祝う各地の秋祭り。"},
    ],
    11: [
        {"name": "七五三", "type": "event", "description": "11月15日。子供の成長を祝い神社に参拝。"},
        {"name": "紅葉の見頃", "type": "event", "description": "京都・奈良の紅葉が見頃。"},
        {"name": "酉の市", "type": "event", "description": "11月の酉の日。商売繁盛の縁起物「熊手」。"},
    ],
    12: [
        {"name": "冬至", "type": "event", "description": "柚子湯、かぼちゃを食べる風習。"},
        {"name": "除夜の鐘", "type": "event", "description": "12月31日。108回の鐘を撞く。"},
        {"name": "忠臣蔵", "type": "event", "description": "12月14日。赤穂浪士の討ち入り記念日。"},
        {"name": "年越しそば", "type": "food", "description": "大晦日にそばを食べる風習。"},
    ],
}


@mcp.tool()
async def today_in_culture(
    date: str = None,
    category: str = None,
) -> str:
    """今日の日付に関連する日本文化トピックを返す。

    日付に関連する祭り、行事、季節の文化イベント、関連する文化要素を返す。
    文化カレンダー + オントロジーDBの祭り・イベントデータを組み合わせ。

    Args:
        date: 日付（MM-DD形式、例: "03-03"）。省略時は今日。
        category: カテゴリフィルタ（"festival", "event", "food" 等）。省略時は全カテゴリ。
    """
    try:
        from datetime import datetime

        if date:
            parts = date.split("-")
            month = int(parts[0])
            day = int(parts[1]) if len(parts) > 1 else 1
        else:
            now = datetime.now()
            month = now.month
            day = now.day

        # 1. Cultural calendar events for this month
        calendar_events = _CULTURAL_CALENDAR.get(month, [])
        if category:
            calendar_events = [e for e in calendar_events if e["type"] == category]

        # 2. Search DB for festivals in this month
        db = _get_db()
        month_names = {
            1: "一月 正月", 2: "二月 節分", 3: "三月 ひな祭り 桃",
            4: "四月 花見 桜", 5: "五月 端午", 6: "六月 梅雨",
            7: "七月 七夕", 8: "八月 お盆 盆踊り", 9: "九月 月見",
            10: "十月 紅葉", 11: "十一月 七五三", 12: "十二月 年末",
        }
        month_kw = month_names.get(month, "")

        db_festivals = []
        if month_kw:
            for kw in month_kw.split():
                results = _fts_search(db, kw, limit=10)
                for r in results:
                    if r["entity_type"] in ("festival", "event", "cultural_practice"):
                        db_festivals.append({
                            "name": r["label_ja"],
                            "name_en": r["label_en"],
                            "entity_type": r["entity_type"],
                            "source": r["source"],
                        })

        # Deduplicate
        seen = set()
        unique_festivals = []
        for f in db_festivals:
            if f["name"] not in seen:
                seen.add(f["name"])
                unique_festivals.append(f)

        # 3. Seasonal cultural entities (crafts, food, etc.)
        seasonal_keywords = {
            1: "初詣 おせち", 2: "豆まき 梅", 3: "雛人形 桃の花",
            4: "桜 花見", 5: "鯉のぼり 菖蒲", 6: "紫陽花 蛍",
            7: "花火 浴衣 風鈴", 8: "灯籠 盆踊り 夏祭り", 9: "団子 ススキ 月見",
            10: "紅葉 栗 秋", 11: "菊 千歳飴", 12: "柚子 餅つき",
        }
        seasonal = []
        for kw in seasonal_keywords.get(month, "").split():
            results = _fts_search(db, kw, limit=5)
            for r in results:
                if r["label_ja"] not in seen:
                    seen.add(r["label_ja"])
                    seasonal.append({
                        "name": r["label_ja"],
                        "name_en": r["label_en"],
                        "entity_type": r["entity_type"],
                    })

        return json.dumps({
            "source": "ontology_db + cultural_calendar (today_in_culture)",
            "date": f"{month:02d}-{day:02d}",
            "month": month,
            "calendar_events": calendar_events,
            "db_festivals": unique_festivals[:15],
            "seasonal_culture": seasonal[:15],
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"today_in_culture failed: {str(e)}"})


# ── 36. deep_dive ─────────────────────────────────────────

@mcp.tool()
async def deep_dive(
    entity: str,
    max_recommendations: int = 5,
) -> str:
    """エンティティの深掘り推薦を生成する。

    あるエンティティについて「もっと知りたい」時に、
    人物関係、作品関係、場所関係、テーマ関係、文化財関係など
    カテゴリ別の深掘り推薦を返す。

    Args:
        entity: エンティティ名（例: "葛飾北斎", "金閣寺", "進撃の巨人"）
        max_recommendations: 推薦数（1-10、default: 5）
    """
    try:
        db = _get_db()
        max_recommendations = max(1, min(max_recommendations, 10))

        # 1. Find entity
        entities = _fts_search(db, entity, limit=5)
        if not entities:
            return json.dumps({"error": f"Entity not found: {entity}"}, ensure_ascii=False)

        primary = entities[0]
        eid = primary["id"]

        # 2. Get all connections
        conns = db.execute("""
            SELECT c.connection_type, c.serendipity_score, c.explanation,
                   c.theme_distance, c.era_distance, c.medium_distance,
                   c.geography_distance, c.experience_distance,
                   e_other.id AS other_id, e_other.label_ja, e_other.label_en,
                   e_other.entity_type, e_other.lat, e_other.lon
            FROM connections c
            JOIN entities e_other ON (
                CASE WHEN c.entity_a_id = ? THEN c.entity_b_id
                     ELSE c.entity_a_id END = e_other.id
            )
            WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
            AND c.llm_verdict = 'keep'
            ORDER BY c.serendipity_score DESC
            LIMIT 100
        """, (eid, eid, eid)).fetchall()

        # 3. Categorize connections
        categories = {
            "people": {"label": "人物関係", "types": {"creator_work", "influence", "adaptation"}, "items": []},
            "works": {"label": "作品関係", "types": {"same_theme", "shared_genre", "medium_cross", "thematic_resonance"}, "items": []},
            "places": {"label": "場所関係", "types": {"pilgrimage_same_location", "pilgrimage_landmark", "pilgrimage_proximity", "pilgrimage_regional", "pilgrimage_filming", "geographic_cultural", "heritage_location"}, "items": []},
            "themes": {"label": "テーマ関係", "types": {"cultural_echo", "era_bridge", "temporal_echo", "shared_motif"}, "items": []},
            "other": {"label": "その他", "types": set(), "items": []},
        }

        for c in conns:
            ct = c["connection_type"]
            placed = False
            for cat_key, cat in categories.items():
                if ct in cat["types"]:
                    cat["items"].append({
                        "entity_id": c["other_id"],
                        "label_ja": c["label_ja"],
                        "label_en": c["label_en"],
                        "entity_type": c["entity_type"],
                        "connection_type": ct,
                        "serendipity_score": c["serendipity_score"],
                        "explanation": c["explanation"],
                        "has_location": c["lat"] is not None,
                    })
                    placed = True
                    break
            if not placed:
                categories["other"]["items"].append({
                    "entity_id": c["other_id"],
                    "label_ja": c["label_ja"],
                    "label_en": c["label_en"],
                    "entity_type": c["entity_type"],
                    "connection_type": ct,
                    "serendipity_score": c["serendipity_score"],
                    "explanation": c["explanation"],
                })

        # 4. Build recommendations — pick top from each category
        recommendations = []
        for cat_key, cat in categories.items():
            if not cat["items"]:
                continue
            # Sort by serendipity score
            cat["items"].sort(key=lambda x: x.get("serendipity_score", 0) or 0, reverse=True)
            top = cat["items"][0]
            recommendations.append({
                "category": cat["label"],
                "category_key": cat_key,
                "recommendation": top["label_ja"],
                "recommendation_en": top["label_en"],
                "entity_type": top["entity_type"],
                "connection_type": top["connection_type"],
                "serendipity_score": top["serendipity_score"],
                "reason": top["explanation"],
                "total_in_category": len(cat["items"]),
            })

        recommendations.sort(key=lambda x: x.get("serendipity_score", 0) or 0, reverse=True)
        recommendations = recommendations[:max_recommendations]

        # 5. Get entity tags for context
        tags = db.execute(
            "SELECT axis, value_code FROM entity_tags WHERE entity_id = ?",
            (eid,)
        ).fetchall()
        tag_dict = {}
        for t in tags:
            tag_dict.setdefault(t["axis"], []).append(t["value_code"])

        return json.dumps({
            "source": "ontology_db (deep_dive)",
            "entity": {
                "label_ja": primary["label_ja"],
                "label_en": primary["label_en"],
                "entity_type": primary["entity_type"],
                "tags": tag_dict,
            },
            "total_connections": len(conns),
            "recommendations_count": len(recommendations),
            "recommendations": recommendations,
            "category_summary": {
                cat_key: {"label": cat["label"], "count": len(cat["items"])}
                for cat_key, cat in categories.items() if cat["items"]
            },
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"deep_dive failed: {str(e)}"})


# ══════════════════════════════════════════════════════════
# Phase 16 Tools — Tourism & Region Analysis (v1.2.0)
# ══════════════════════════════════════════════════════════

# Region code -> (label_ja, center_lat, center_lon, radius_km)
_REGION_DEFS = {
    "hokkaido": ("北海道", 43.0642, 141.3469, 200),
    "tohoku": ("東北", 39.7, 140.1, 150),
    "kanto": ("関東", 35.7, 139.7, 100),
    "chubu": ("中部", 36.2, 137.9, 150),
    "kinki": ("近畿", 34.7, 135.5, 100),
    "chugoku": ("中国", 34.4, 132.5, 120),
    "shikoku": ("四国", 33.8, 133.5, 80),
    "kyushu": ("九州", 33.0, 131.0, 150),
    "okinawa": ("沖縄", 26.3344, 127.8056, 100),
    "tokyo": ("東京", 35.6762, 139.6503, 30),
    "kyoto": ("京都", 35.0116, 135.7681, 30),
    "osaka": ("大阪", 34.6937, 135.5023, 25),
    "nara": ("奈良", 34.6851, 135.8049, 25),
}


# ── 37. get_region_profile ─────────────────────────────────

@mcp.tool()
async def get_region_profile(
    region: str,
) -> str:
    """指定地域の文化プロファイルを生成する。

    地域内のエンティティ統計、テーマ分布、時代分布、主要文化資産、
    接続密度などを一覧で返す。観光計画やコンテンツ企画の基礎データに。

    Args:
        region: 地域コード（hokkaido, tohoku, kanto, chubu, kinki, chugoku, shikoku, kyushu, okinawa, tokyo, kyoto, osaka, nara）
    """
    try:
        db = _get_db()
        region = region.lower().strip()

        if region not in _REGION_DEFS:
            return json.dumps({
                "error": f"Unknown region: {region}",
                "available_regions": list(_REGION_DEFS.keys()),
            }, ensure_ascii=False)

        label_ja, center_lat, center_lon, radius_km = _REGION_DEFS[region]

        lat_offset = radius_km / 111.0
        lon_offset = radius_km / (111.0 * max(math.cos(math.radians(center_lat)), 0.01))

        # 1. Geo-based entities in region
        geo_entities = db.execute("""
            SELECT entity_type, COUNT(*) as cnt
            FROM entities
            WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
              AND is_dormant = 0
            GROUP BY entity_type
            ORDER BY cnt DESC
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()

        type_breakdown = {}
        total_geo = 0
        for row in geo_entities:
            type_breakdown[row["entity_type"] or "unknown"] = row["cnt"]
            total_geo += row["cnt"]

        # 2. Tag-based entities in region
        tag_count = db.execute(
            "SELECT COUNT(DISTINCT entity_id) FROM entity_tags WHERE axis='geography' AND value_code=?",
            (region,)
        ).fetchone()[0]

        # 3. Theme distribution
        themes = db.execute("""
            SELECT et.value_code, COUNT(*) as cnt
            FROM entity_tags et
            JOIN entities e ON et.entity_id = e.id
            WHERE et.axis = 'theme'
              AND e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND e.is_dormant = 0
            GROUP BY et.value_code
            ORDER BY cnt DESC
            LIMIT 15
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()
        theme_dist = [{"theme": t["value_code"], "count": t["cnt"]} for t in themes]

        # 4. Era distribution
        eras = db.execute("""
            SELECT et.value_code, COUNT(*) as cnt
            FROM entity_tags et
            JOIN entities e ON et.entity_id = e.id
            WHERE et.axis = 'era'
              AND e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND e.is_dormant = 0
            GROUP BY et.value_code
            ORDER BY cnt DESC
            LIMIT 10
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()
        era_dist = [{"era": e["value_code"], "count": e["cnt"]} for e in eras]

        # 5. Notable entities (most connected)
        notable = db.execute("""
            SELECT e.label_ja, e.label_en, e.entity_type,
                   COUNT(c.id) as conn_count
            FROM entities e
            JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
            WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND e.is_dormant = 0
            GROUP BY e.id
            ORDER BY conn_count DESC
            LIMIT 10
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()
        notable_list = [{
            "name": n["label_ja"],
            "name_en": n["label_en"],
            "type": n["entity_type"],
            "connections": n["conn_count"],
        } for n in notable]

        # 6. Connection density
        conn_count = db.execute("""
            SELECT COUNT(*) FROM connections c
            JOIN entities ea ON c.entity_a_id = ea.id
            WHERE ea.lat BETWEEN ? AND ? AND ea.lon BETWEEN ? AND ?
              AND ea.is_dormant = 0
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchone()[0]

        # 7. Pilgrimage spots
        pilgrimage_count = db.execute("""
            SELECT COUNT(DISTINCT e.id) FROM entities e
            JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
            WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND c.connection_type LIKE 'pilgrimage%'
              AND e.is_dormant = 0
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchone()[0]

        density = conn_count / max(total_geo, 1)

        return json.dumps({
            "source": "ontology_db (get_region_profile)",
            "region": region,
            "region_name": label_ja,
            "center": {"lat": center_lat, "lon": center_lon},
            "radius_km": radius_km,
            "total_geo_entities": total_geo,
            "tag_based_entities": tag_count,
            "type_breakdown": type_breakdown,
            "connection_count": conn_count,
            "connection_density": round(density, 3),
            "pilgrimage_spots": pilgrimage_count,
            "theme_distribution": theme_dist,
            "era_distribution": era_dist,
            "notable_entities": notable_list,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"get_region_profile failed: {str(e)}"})


# ── 38. find_tourism_assets ─────────────────────────────────

@mcp.tool()
async def find_tourism_assets(
    region: str = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: float = 30.0,
    asset_types: str = "all",
    limit: int = 30,
) -> str:
    """地域の観光文化資産を種類別に一覧する。

    寺社仏閣、聖地巡礼スポット、文化財、博物館、伝統工芸等を
    カテゴリ別に整理して返す。観光ルート企画やガイド作成に最適。

    Args:
        region: 地域コード（tokyo, kyoto等）。lat/lonの代わりに使用可
        lat: 緯度（regionを指定しない場合に必須）
        lon: 経度（regionを指定しない場合に必須）
        radius_km: 検索半径km（デフォルト30、最大200）
        asset_types: "all" または "shrine,temple,place,artifact" 等カンマ区切り
        limit: カテゴリあたりの最大件数（1-50）
    """
    try:
        db = _get_db()
        radius_km = max(0.1, min(radius_km, 200.0))
        limit = max(1, min(limit, 50))

        # Resolve center coordinates
        if region:
            region = region.lower().strip()
            if region not in _REGION_DEFS:
                return json.dumps({
                    "error": f"Unknown region: {region}",
                    "available_regions": list(_REGION_DEFS.keys()),
                }, ensure_ascii=False)
            _, center_lat, center_lon, default_radius = _REGION_DEFS[region]
            if radius_km == 30.0:
                radius_km = default_radius
        elif lat is not None and lon is not None:
            center_lat, center_lon = lat, lon
        else:
            return json.dumps({
                "error": "Either 'region' or both 'lat' and 'lon' must be specified."
            })

        lat_offset = radius_km / 111.0
        lon_offset = radius_km / (111.0 * max(math.cos(math.radians(center_lat)), 0.01))

        # Define tourism-relevant categories
        categories = {
            "shrine": {"label": "神社", "types": ["shrine"]},
            "temple": {"label": "寺院", "types": ["temple"]},
            "place": {"label": "名所・史跡", "types": ["place"]},
            "artifact": {"label": "文化財・美術品", "types": ["artifact"]},
            "building": {"label": "歴史的建造物", "types": ["building"]},
            "food": {"label": "食文化", "types": ["food"]},
            "festival": {"label": "祭り・行事", "types": ["festival", "event"]},
            "tradition": {"label": "伝統文化", "types": ["tradition", "cultural_practice"]},
            "pilgrimage": {"label": "聖地巡礼", "types": ["_pilgrimage_"]},
        }

        # Filter requested categories
        if asset_types != "all":
            requested = set(t.strip() for t in asset_types.split(","))
            categories = {k: v for k, v in categories.items() if k in requested}

        results = {}

        for cat_key, cat_info in categories.items():
            if cat_key == "pilgrimage":
                # Special query for pilgrimage spots
                rows = db.execute("""
                    SELECT DISTINCT e.id, e.label_ja, e.label_en, e.entity_type,
                           e.lat, e.lon, c.explanation
                    FROM entities e
                    JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
                    WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
                      AND c.connection_type LIKE 'pilgrimage%'
                      AND e.is_dormant = 0
                    ORDER BY e.label_ja
                    LIMIT ?
                """, (center_lat - lat_offset, center_lat + lat_offset,
                      center_lon - lon_offset, center_lon + lon_offset, limit)).fetchall()
            else:
                type_placeholders = ",".join(["?"] * len(cat_info["types"]))
                rows = db.execute(f"""
                    SELECT e.id, e.label_ja, e.label_en, e.entity_type,
                           e.lat, e.lon
                    FROM entities e
                    WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
                      AND e.entity_type IN ({type_placeholders})
                      AND e.is_dormant = 0
                    ORDER BY e.label_ja
                    LIMIT ?
                """, (center_lat - lat_offset, center_lat + lat_offset,
                      center_lon - lon_offset, center_lon + lon_offset,
                      *cat_info["types"], limit)).fetchall()

            items = []
            for r in rows:
                dist_km = math.sqrt(
                    ((r["lat"] - center_lat) * 111.0) ** 2 +
                    ((r["lon"] - center_lon) * 111.0 * math.cos(math.radians(center_lat))) ** 2
                )
                item = {
                    "name": r["label_ja"],
                    "name_en": r["label_en"],
                    "type": r["entity_type"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "distance_km": round(dist_km, 1),
                }
                if cat_key == "pilgrimage" and r["explanation"]:
                    item["description"] = r["explanation"]
                items.append(item)

            items.sort(key=lambda x: x["distance_km"])
            if items:
                results[cat_key] = {
                    "label": cat_info["label"],
                    "count": len(items),
                    "items": items,
                }

        total_assets = sum(c["count"] for c in results.values())
        return json.dumps({
            "source": "ontology_db (find_tourism_assets)",
            "query": {
                "region": region,
                "center": {"lat": center_lat, "lon": center_lon},
                "radius_km": radius_km,
            },
            "total_assets": total_assets,
            "categories_found": len(results),
            "categories": results,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"find_tourism_assets failed: {str(e)}"})


# ── 39. analyze_cultural_density ───────────────────────────

@mcp.tool()
async def analyze_cultural_density(
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    grid_size: int = 10,
    entity_type: str = None,
) -> str:
    """指定領域の文化密度を格子状に分析しヒートマップデータを返す。

    バウンディングボックスをN×Nグリッドに分割し、各セルのエンティティ数と
    主要タイプを返す。文化密度の可視化や観光戦略の分析に利用。

    Args:
        lat_min: 最南端の緯度。例: 34.5（京都南部）
        lat_max: 最北端の緯度。例: 35.5（京都北部）
        lon_min: 最西端の経度。例: 135.0
        lon_max: 最東端の経度。例: 136.0
        grid_size: グリッド分割数（2-50、デフォルト10 → 10×10=100セル）
        entity_type: エンティティタイプで絞り込み（place, shrine, temple等）
    """
    try:
        db = _get_db()
        grid_size = max(2, min(grid_size, 50))

        lat_step = (lat_max - lat_min) / grid_size
        lon_step = (lon_max - lon_min) / grid_size

        if lat_step <= 0 or lon_step <= 0:
            return json.dumps({"error": "lat_max must be > lat_min and lon_max must be > lon_min"})

        # Get all entities in the bounding box
        params = [lat_min, lat_max, lon_min, lon_max]
        type_filter = ""
        if entity_type:
            type_filter = "AND e.entity_type = ?"
            params.append(entity_type)

        rows = db.execute(f"""
            SELECT e.lat, e.lon, e.entity_type
            FROM entities e
            WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND e.is_dormant = 0
              {type_filter}
        """, params).fetchall()

        # Build grid
        grid = [[{"count": 0, "types": {}} for _ in range(grid_size)] for _ in range(grid_size)]

        for r in rows:
            row_idx = min(int((r["lat"] - lat_min) / lat_step), grid_size - 1)
            col_idx = min(int((r["lon"] - lon_min) / lon_step), grid_size - 1)
            row_idx = max(0, row_idx)
            col_idx = max(0, col_idx)
            grid[row_idx][col_idx]["count"] += 1
            etype = r["entity_type"] or "unknown"
            grid[row_idx][col_idx]["types"][etype] = grid[row_idx][col_idx]["types"].get(etype, 0) + 1

        # Find top cells and compute stats
        all_counts = []
        cells = []
        for ri in range(grid_size):
            for ci in range(grid_size):
                cell = grid[ri][ci]
                count = cell["count"]
                all_counts.append(count)
                cell_lat = lat_min + (ri + 0.5) * lat_step
                cell_lon = lon_min + (ci + 0.5) * lon_step
                # Only include non-empty cells
                if count > 0:
                    top_type = max(cell["types"], key=cell["types"].get) if cell["types"] else None
                    cells.append({
                        "row": ri,
                        "col": ci,
                        "center_lat": round(cell_lat, 4),
                        "center_lon": round(cell_lon, 4),
                        "count": count,
                        "dominant_type": top_type,
                    })

        cells.sort(key=lambda x: x["count"], reverse=True)
        total = sum(all_counts)
        max_count = max(all_counts) if all_counts else 0
        non_empty = sum(1 for c in all_counts if c > 0)

        # Top 10 hotspots
        hotspots = cells[:10]

        return json.dumps({
            "source": "ontology_db (analyze_cultural_density)",
            "bounding_box": {
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max,
            },
            "grid_size": grid_size,
            "entity_type_filter": entity_type,
            "total_entities": total,
            "cells_total": grid_size * grid_size,
            "cells_non_empty": non_empty,
            "max_cell_count": max_count,
            "avg_cell_count": round(total / (grid_size * grid_size), 1),
            "hotspots": hotspots,
            "grid": [[grid[ri][ci]["count"] for ci in range(grid_size)] for ri in range(grid_size)],
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"analyze_cultural_density failed: {str(e)}"})


# ══════════════════════════════════════════════════════════
# Phase 18 Tools — release_year Filter, Prefecture Profile, Pilgrimage Timeline (v1.3.0)
# ══════════════════════════════════════════════════════════

# Prefecture definitions: code -> (name_ja, center_lat, center_lon, radius_km)
_PREFECTURE_DEFS = {
    "hokkaido": ("北海道", 43.0642, 141.3469, 120),
    "aomori": ("青森県", 40.8244, 140.7400, 50),
    "iwate": ("岩手県", 39.7036, 141.1527, 60),
    "miyagi": ("宮城県", 38.2688, 140.8721, 40),
    "akita": ("秋田県", 39.7186, 140.1024, 50),
    "yamagata": ("山形県", 38.2404, 140.3633, 40),
    "fukushima": ("福島県", 37.7500, 140.4678, 50),
    "ibaraki": ("茨城県", 36.3419, 140.4468, 35),
    "tochigi": ("栃木県", 36.5658, 139.8836, 35),
    "gunma": ("群馬県", 36.3912, 139.0608, 35),
    "saitama": ("埼玉県", 35.8569, 139.6489, 25),
    "chiba": ("千葉県", 35.6047, 140.1233, 30),
    "tokyo": ("東京都", 35.6762, 139.6503, 25),
    "kanagawa": ("神奈川県", 35.4478, 139.6425, 20),
    "niigata": ("新潟県", 37.9026, 139.0236, 55),
    "toyama": ("富山県", 36.6953, 137.2114, 30),
    "ishikawa": ("石川県", 36.5946, 136.6256, 35),
    "fukui": ("福井県", 35.8562, 136.2258, 30),
    "yamanashi": ("山梨県", 35.6642, 138.5684, 25),
    "nagano": ("長野県", 36.2378, 138.1813, 45),
    "gifu": ("岐阜県", 35.3912, 136.7223, 35),
    "shizuoka": ("静岡県", 34.9769, 138.3831, 40),
    "aichi": ("愛知県", 35.1802, 136.9066, 30),
    "mie": ("三重県", 34.7303, 136.5086, 40),
    "shiga": ("滋賀県", 35.0045, 135.8686, 25),
    "kyoto": ("京都府", 35.0116, 135.7681, 30),
    "osaka": ("大阪府", 34.6937, 135.5023, 20),
    "hyogo": ("兵庫県", 34.6913, 135.1830, 35),
    "nara": ("奈良県", 34.6851, 135.8049, 20),
    "wakayama": ("和歌山県", 33.9499, 135.3748, 30),
    "tottori": ("鳥取県", 35.5039, 134.2383, 30),
    "shimane": ("島根県", 35.4723, 133.0505, 40),
    "okayama": ("岡山県", 34.6618, 133.9345, 30),
    "hiroshima": ("広島県", 34.3966, 132.4596, 35),
    "yamaguchi": ("山口県", 34.1861, 131.4714, 35),
    "tokushima": ("徳島県", 34.0658, 134.5593, 25),
    "kagawa": ("香川県", 34.3401, 134.0434, 20),
    "ehime": ("愛媛県", 33.8417, 132.7661, 30),
    "kochi": ("高知県", 33.5597, 133.5311, 35),
    "fukuoka": ("福岡県", 33.6064, 130.4183, 30),
    "saga": ("佐賀県", 33.2494, 130.2988, 20),
    "nagasaki": ("長崎県", 32.7503, 129.8779, 30),
    "kumamoto": ("熊本県", 32.7898, 130.7418, 30),
    "oita": ("大分県", 33.2382, 131.6126, 30),
    "miyazaki": ("宮崎県", 31.9111, 131.4239, 35),
    "kagoshima": ("鹿児島県", 31.5602, 130.5581, 40),
    "okinawa": ("沖縄県", 26.3344, 127.8056, 60),
}


# ── 40. filter_by_release_year ─────────────────────────────

@mcp.tool()
async def filter_by_release_year(
    year_from: int = None,
    year_to: int = None,
    entity_type: str = None,
    keyword: str = None,
    limit: int = 30,
) -> str:
    """release_year（発表年）でエンティティをフィルタリングする。

    アニメ・漫画・映画・ゲーム等の作品をrelease_yearで検索可能。
    キーワードやエンティティタイプで絞り込みも可能。

    Args:
        year_from: 開始年（例: 2010）
        year_to: 終了年（例: 2020）
        entity_type: エンティティタイプフィルタ（例: "anime", "work", "manga"）
        keyword: キーワードフィルタ（FTS5検索を併用）
        limit: 最大結果数（1-100、default: 30）
    """
    try:
        db = _get_db()
        limit = max(1, min(limit, 100))

        conditions = ["release_year IS NOT NULL", "is_dormant = 0"]
        params = []

        if year_from is not None:
            conditions.append("release_year >= ?")
            params.append(year_from)
        if year_to is not None:
            conditions.append("release_year <= ?")
            params.append(year_to)
        if entity_type:
            conditions.append("entity_type = ?")
            params.append(entity_type)

        # If keyword specified, intersect with FTS5 results
        keyword_ids = None
        if keyword:
            fts_results = _fts_search(db, keyword, limit=500)
            keyword_ids = {r["id"] for r in fts_results}
            if not keyword_ids:
                return json.dumps({
                    "source": "ontology_db (filter_by_release_year)",
                    "query": {"year_from": year_from, "year_to": year_to,
                              "entity_type": entity_type, "keyword": keyword},
                    "total_results": 0,
                    "items": [],
                }, ensure_ascii=False, indent=2)

        where = " AND ".join(conditions)
        query = f"""
            SELECT id, label_ja, label_en, entity_type, release_year,
                   release_year_source, source, wikidata_id
            FROM entities
            WHERE {where}
            ORDER BY release_year DESC
            LIMIT ?
        """
        params.append(limit * 5 if keyword_ids else limit)

        rows = db.execute(query, params).fetchall()

        items = []
        for r in rows:
            if keyword_ids is not None and r["id"] not in keyword_ids:
                continue
            items.append({
                "label_ja": r["label_ja"],
                "label_en": r["label_en"],
                "entity_type": r["entity_type"],
                "release_year": r["release_year"],
                "release_year_source": r["release_year_source"],
                "source": r["source"],
                "wikidata_id": r["wikidata_id"],
            })
            if len(items) >= limit:
                break

        return json.dumps({
            "source": "ontology_db (filter_by_release_year)",
            "query": {"year_from": year_from, "year_to": year_to,
                      "entity_type": entity_type, "keyword": keyword},
            "total_results": len(items),
            "items": items,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"filter_by_release_year failed: {str(e)}"})


# ── 41. get_prefecture_profile ─────────────────────────────

@mcp.tool()
async def get_prefecture_profile(
    prefecture: str,
) -> str:
    """都道府県単位の文化プロファイルを生成する。

    都道府県内のエンティティ統計、テーマ分布、時代分布、主要文化資産、
    聖地巡礼スポット数などを返す。地方創生や観光計画のデータに。

    Args:
        prefecture: 都道府県コード（例: "tokyo", "kyoto", "hokkaido", "okinawa"）
                    47都道府県すべてに対応。
    """
    try:
        db = _get_db()
        pref = prefecture.lower().strip()

        if pref not in _PREFECTURE_DEFS:
            return json.dumps({
                "error": f"Unknown prefecture: {pref}",
                "available_prefectures": list(_PREFECTURE_DEFS.keys()),
            }, ensure_ascii=False)

        name_ja, center_lat, center_lon, radius_km = _PREFECTURE_DEFS[pref]

        lat_offset = radius_km / 111.0
        lon_offset = radius_km / (111.0 * max(math.cos(math.radians(center_lat)), 0.01))

        # 1. Entity type breakdown
        geo_entities = db.execute("""
            SELECT entity_type, COUNT(*) as cnt
            FROM entities
            WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
              AND is_dormant = 0
            GROUP BY entity_type
            ORDER BY cnt DESC
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()

        type_breakdown = {}
        total_geo = 0
        for row in geo_entities:
            type_breakdown[row["entity_type"] or "unknown"] = row["cnt"]
            total_geo += row["cnt"]

        # 2. Theme distribution
        themes = db.execute("""
            SELECT et.value_code, COUNT(*) as cnt
            FROM entity_tags et
            JOIN entities e ON et.entity_id = e.id
            WHERE et.axis = 'theme'
              AND e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND e.is_dormant = 0
            GROUP BY et.value_code
            ORDER BY cnt DESC
            LIMIT 15
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()
        theme_dist = [{"theme": t["value_code"], "count": t["cnt"]} for t in themes]

        # 3. Era distribution
        eras = db.execute("""
            SELECT et.value_code, COUNT(*) as cnt
            FROM entity_tags et
            JOIN entities e ON et.entity_id = e.id
            WHERE et.axis = 'era'
              AND e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND e.is_dormant = 0
            GROUP BY et.value_code
            ORDER BY cnt DESC
            LIMIT 10
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()
        era_dist = [{"era": e["value_code"], "count": e["cnt"]} for e in eras]

        # 4. Pilgrimage spots in area
        pilgrimage_count = db.execute("""
            SELECT COUNT(DISTINCT e.id)
            FROM entities e
            JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
            WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND c.connection_type LIKE 'pilgrimage%'
              AND e.is_dormant = 0
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchone()[0]

        # 5. Notable entities (most connected)
        notable = db.execute("""
            SELECT e.label_ja, e.label_en, e.entity_type,
                   COUNT(c.id) as conn_count
            FROM entities e
            LEFT JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
                AND c.llm_verdict = 'keep'
            WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
              AND e.is_dormant = 0
            GROUP BY e.id
            ORDER BY conn_count DESC
            LIMIT 10
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()
        notable_list = [
            {"label_ja": n["label_ja"], "label_en": n["label_en"],
             "entity_type": n["entity_type"], "connections": n["conn_count"]}
            for n in notable
        ]

        # 6. Release year distribution (works in area)
        year_dist = db.execute("""
            SELECT release_year, COUNT(*) as cnt
            FROM entities
            WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
              AND release_year IS NOT NULL
              AND is_dormant = 0
            GROUP BY release_year
            ORDER BY release_year DESC
            LIMIT 20
        """, (center_lat - lat_offset, center_lat + lat_offset,
              center_lon - lon_offset, center_lon + lon_offset)).fetchall()
        year_list = [{"year": y["release_year"], "count": y["cnt"]} for y in year_dist]

        return json.dumps({
            "source": "ontology_db (get_prefecture_profile)",
            "prefecture": pref,
            "prefecture_name": name_ja,
            "center": {"lat": center_lat, "lon": center_lon},
            "radius_km": radius_km,
            "total_geo_entities": total_geo,
            "entity_type_breakdown": type_breakdown,
            "theme_distribution": theme_dist,
            "era_distribution": era_dist,
            "pilgrimage_spots": pilgrimage_count,
            "notable_entities": notable_list,
            "release_year_distribution": year_list,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"get_prefecture_profile failed: {str(e)}"})


# ── 42. pilgrimage_timeline ─────────────────────────────────

@mcp.tool()
async def pilgrimage_timeline(
    region: str = None,
    year_from: int = None,
    year_to: int = None,
    limit: int = 30,
) -> str:
    """聖地巡礼スポットをrelease_year順に時系列で表示する。

    作品の発表年順に聖地巡礼スポットを並べ、コンテンツツーリズムの
    時系列変化を可視化する。地域フィルタや年代フィルタも可能。

    Args:
        region: 地域コード（例: "kanto", "kinki"）。省略時は全国。
        year_from: 開始年（例: 2000）
        year_to: 終了年（例: 2024）
        limit: 最大結果数（1-100、default: 30）
    """
    try:
        db = _get_db()
        limit = max(1, min(limit, 100))

        # Build query for pilgrimage connections with release_year
        conditions = [
            "c.connection_type LIKE 'pilgrimage%'",
            "e_work.release_year IS NOT NULL",
            "e_loc.lat IS NOT NULL",
            "e_loc.is_dormant = 0",
        ]
        params = []

        if year_from is not None:
            conditions.append("e_work.release_year >= ?")
            params.append(year_from)
        if year_to is not None:
            conditions.append("e_work.release_year <= ?")
            params.append(year_to)

        # Region filter using bounding box
        if region:
            region = region.lower().strip()
            region_def = _REGION_DEFS.get(region) or _PREFECTURE_DEFS.get(region)
            if region_def:
                _, rlat, rlon, rrad = region_def
                lat_off = rrad / 111.0
                lon_off = rrad / (111.0 * max(math.cos(math.radians(rlat)), 0.01))
                conditions.append("e_loc.lat BETWEEN ? AND ?")
                params.extend([rlat - lat_off, rlat + lat_off])
                conditions.append("e_loc.lon BETWEEN ? AND ?")
                params.extend([rlon - lon_off, rlon + lon_off])

        where = " AND ".join(conditions)

        rows = db.execute(f"""
            SELECT
                e_work.label_ja AS work_name,
                e_work.label_en AS work_name_en,
                e_work.entity_type AS work_type,
                e_work.release_year,
                e_loc.label_ja AS location_name,
                e_loc.lat, e_loc.lon,
                c.explanation,
                c.connection_type
            FROM connections c
            JOIN entities e_work ON (c.entity_a_id = e_work.id OR c.entity_b_id = e_work.id)
            JOIN entities e_loc ON (
                (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
                AND e_loc.id != e_work.id
            )
            WHERE {where}
            ORDER BY e_work.release_year ASC
            LIMIT ?
        """, (*params, limit * 3)).fetchall()

        # Group by work
        works = {}
        for r in rows:
            key = r["work_name"]
            if key not in works:
                works[key] = {
                    "work_name": r["work_name"],
                    "work_name_en": r["work_name_en"],
                    "work_type": r["work_type"],
                    "release_year": r["release_year"],
                    "locations": [],
                }
            if len(works[key]["locations"]) < 5:
                works[key]["locations"].append({
                    "name": r["location_name"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "type": r["connection_type"],
                    "description": r["explanation"],
                })

        # Sort by release_year and limit
        timeline = sorted(works.values(), key=lambda x: x["release_year"])[:limit]

        # Add location count
        for entry in timeline:
            entry["location_count"] = len(entry["locations"])

        return json.dumps({
            "source": "ontology_db (pilgrimage_timeline)",
            "query": {"region": region, "year_from": year_from, "year_to": year_to},
            "total_works": len(timeline),
            "timeline": timeline,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"pilgrimage_timeline failed: {str(e)}"})


# ══════════════════════════════════════════════════════════
# Phase 18 Stream 5 Tools — Bulk API, CCDM Analysis, Export (v1.3.0)
# ══════════════════════════════════════════════════════════

# ── 43. bulk_region_profiles ──────────────────────────────

@mcp.tool()
async def bulk_region_profiles(
    prefectures: str = None,
    include_pilgrimage: bool = True,
) -> str:
    """複数都道府県の文化プロファイルを一括取得して比較する。

    地域間比較やCCDMの地域分析に使用。都道府県を指定しない場合は全47都道府県を対象にする。

    Args:
        prefectures: カンマ区切りの都道府県コード（例: "tokyo,kyoto,osaka"）。
                     未指定で全47都道府県。
        include_pilgrimage: 聖地巡礼スポット数を含める（デフォルト: True）。
    """
    try:
        db = _get_db()

        if prefectures:
            codes = [p.strip().lower() for p in prefectures.split(",")]
            invalid = [c for c in codes if c not in _PREFECTURE_DEFS]
            if invalid:
                return json.dumps({
                    "error": f"Unknown prefectures: {invalid}",
                    "available_prefectures": list(_PREFECTURE_DEFS.keys()),
                }, ensure_ascii=False)
        else:
            codes = list(_PREFECTURE_DEFS.keys())

        profiles = {}
        for code in codes:
            name_ja, center_lat, center_lon, radius_km = _PREFECTURE_DEFS[code]
            lat_offset = radius_km / 111.0
            lon_offset = radius_km / (111.0 * max(math.cos(math.radians(center_lat)), 0.01))
            bb = (center_lat - lat_offset, center_lat + lat_offset,
                  center_lon - lon_offset, center_lon + lon_offset)

            # Entity count by type
            rows = db.execute("""
                SELECT entity_type, COUNT(*) as cnt
                FROM entities
                WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
                  AND is_dormant = 0
                GROUP BY entity_type ORDER BY cnt DESC
            """, bb).fetchall()

            type_breakdown = {}
            total = 0
            for r in rows:
                type_breakdown[r["entity_type"] or "unknown"] = r["cnt"]
                total += r["cnt"]

            profile = {
                "name_ja": name_ja,
                "total_entities": total,
                "entity_types": type_breakdown,
            }

            if include_pilgrimage:
                pc = db.execute("""
                    SELECT COUNT(DISTINCT e.id)
                    FROM entities e
                    JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
                    WHERE e.lat BETWEEN ? AND ? AND e.lon BETWEEN ? AND ?
                      AND c.connection_type LIKE 'pilgrimage%'
                      AND e.is_dormant = 0
                """, bb).fetchone()[0]
                profile["pilgrimage_spots"] = pc

            profiles[code] = profile

        # Comparison rankings
        sorted_total = sorted(profiles.items(), key=lambda x: -x[1]["total_entities"])
        comparison = {
            "most_entities": sorted_total[0][0] if sorted_total else None,
            "least_entities": sorted_total[-1][0] if sorted_total else None,
            "entity_count_ranking": [
                {"prefecture": k, "count": v["total_entities"]}
                for k, v in sorted_total[:10]
            ],
        }
        if include_pilgrimage:
            sorted_pilg = sorted(profiles.items(), key=lambda x: -x[1].get("pilgrimage_spots", 0))
            comparison["most_pilgrimage"] = sorted_pilg[0][0] if sorted_pilg else None
            comparison["pilgrimage_ranking"] = [
                {"prefecture": k, "count": v.get("pilgrimage_spots", 0)}
                for k, v in sorted_pilg[:10]
            ]

        return json.dumps({
            "source": "ontology_db (bulk_region_profiles)",
            "prefecture_count": len(profiles),
            "profiles": profiles,
            "comparison": comparison,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"bulk_region_profiles failed: {str(e)}"})


# ── 44. ccdm_emergence_analysis ───────────────────────────

@mcp.tool()
async def ccdm_emergence_analysis(
    region: str = None,
    year_from: int = 1980,
    year_to: int = 2025,
    medium: str = None,
) -> str:
    """CCDMのEmergenceフェーズを定量化する専用分析ツール。

    年ごとの新規作品数・新規聖地数・累積聖地数を時系列で返す。
    K^I（無形文化資本）プロキシ指標も計算する。

    Args:
        region: 都道府県コード（例: "kyoto"）。未指定で全国。
        year_from: 開始年（デフォルト: 1980）。
        year_to: 終了年（デフォルト: 2025）。
        medium: メディアフィルタ（例: "anime_tv", "manga", "game"）。
    """
    try:
        db = _get_db()

        # Build pilgrimage query with optional region filter
        where_clauses = [
            "c.connection_type LIKE 'pilgrimage%'",
            "w.is_dormant = 0",
            "w.release_year IS NOT NULL",
            "w.release_year BETWEEN ? AND ?",
        ]
        params = [year_from, year_to]

        if region:
            r = region.lower().strip()
            if r in _PREFECTURE_DEFS:
                name_ja, clat, clon, rad = _PREFECTURE_DEFS[r]
                lat_off = rad / 111.0
                lon_off = rad / (111.0 * max(math.cos(math.radians(clat)), 0.01))
                where_clauses.append("loc.lat BETWEEN ? AND ? AND loc.lon BETWEEN ? AND ?")
                params.extend([clat - lat_off, clat + lat_off, clon - lon_off, clon + lon_off])

        if medium:
            where_clauses.append("""
                EXISTS (SELECT 1 FROM entity_tags et
                        WHERE et.entity_id = w.id AND et.axis = 'medium' AND et.value_code = ?)
            """)
            params.append(medium)

        where_sql = " AND ".join(where_clauses)

        rows = db.execute(f"""
            SELECT w.release_year,
                   w.id as work_id, w.label_ja as work_name,
                   loc.id as loc_id, loc.label_ja as loc_name
            FROM connections c
            JOIN entities w ON c.entity_a_id = w.id
            JOIN entities loc ON c.entity_b_id = loc.id
            WHERE {where_sql}
            UNION
            SELECT w.release_year,
                   w.id as work_id, w.label_ja as work_name,
                   loc.id as loc_id, loc.label_ja as loc_name
            FROM connections c
            JOIN entities w ON c.entity_b_id = w.id
            JOIN entities loc ON c.entity_a_id = loc.id
            WHERE {where_sql}
        """, params + params).fetchall()

        # Group by year
        year_data = {}
        all_works = set()
        all_locs = set()
        for yr in range(year_from, year_to + 1):
            year_data[yr] = {"works": set(), "locations": set()}

        for r in rows:
            yr = r["release_year"]
            if yr in year_data:
                year_data[yr]["works"].add(r["work_id"])
                year_data[yr]["locations"].add(r["loc_id"])
                all_works.add(r["work_id"])
                all_locs.add(r["loc_id"])

        # Build timeline
        timeline = []
        cumulative_works = set()
        cumulative_locs = set()
        for yr in range(year_from, year_to + 1):
            new_works = year_data[yr]["works"] - cumulative_works
            new_locs = year_data[yr]["locations"] - cumulative_locs
            cumulative_works |= year_data[yr]["works"]
            cumulative_locs |= year_data[yr]["locations"]
            timeline.append({
                "year": yr,
                "new_works": len(new_works),
                "new_pilgrimage_spots": len(new_locs),
                "cumulative_works": len(cumulative_works),
                "cumulative_spots": len(cumulative_locs),
            })

        # K^I proxy (diversity / density)
        # Shannon entropy of entity types across all locations
        if all_locs:
            type_counts = db.execute("""
                SELECT entity_type, COUNT(*) as cnt
                FROM entities WHERE id IN ({})
                GROUP BY entity_type
            """.format(",".join("?" * len(all_locs))), list(all_locs)).fetchall()

            total_tc = sum(t["cnt"] for t in type_counts)
            entropy = 0.0
            for t in type_counts:
                p = t["cnt"] / total_tc
                if p > 0:
                    entropy -= p * math.log(p)
            diversity_index = round(entropy, 3)
        else:
            diversity_index = 0.0

        # Pop-traditional co-occurrence
        pop_trad = 0
        if all_locs:
            pop_trad = db.execute("""
                SELECT COUNT(*) FROM connections
                WHERE connection_type IN ('pop_traditional', 'cross_type_label_match')
                  AND (entity_a_id IN ({ids}) OR entity_b_id IN ({ids}))
            """.format(ids=",".join("?" * len(all_locs))), list(all_locs) + list(all_locs)).fetchone()[0]

        # Peak year
        peak_year = max(timeline, key=lambda x: x["new_pilgrimage_spots"])["year"] if timeline else None
        span = year_to - year_from + 1
        emergence_rate = round(len(all_locs) / span, 2) if span > 0 else 0

        return json.dumps({
            "source": "ontology_db (ccdm_emergence_analysis)",
            "query": {"region": region, "year_from": year_from, "year_to": year_to, "medium": medium},
            "total_works": len(all_works),
            "total_pilgrimage_spots": len(all_locs),
            "emergence_timeline": timeline,
            "peak_year": peak_year,
            "emergence_rate": emergence_rate,
            "k_i_proxy": {
                "diversity_index": diversity_index,
                "pilgrimage_density": round(len(all_locs) / max(len(all_works), 1), 2),
                "pop_trad_cooccurrence": pop_trad,
            },
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"ccdm_emergence_analysis failed: {str(e)}"})


# ── 45. export_dataset ────────────────────────────────────

@mcp.tool()
async def export_dataset(
    dataset_type: str,
    prefecture: str = None,
    limit: int = 500,
) -> str:
    """分析用データセットをJSON形式でエクスポートする。

    データサイエンティスト・研究者向けの構造化データ出力。

    Args:
        dataset_type: データセット種別。
            "pilgrimage" — 聖地巡礼データ（作品×スポット×座標）
            "release_year" — 公開年付きエンティティ一覧
            "pop_trad" — ポップ×伝統文化の交差接続
            "geo_culture" — 座標付き文化資源
        prefecture: 都道府県コードでフィルタ（任意、例: "kyoto"）。
        limit: 最大件数（デフォルト: 500）。
    """
    try:
        db = _get_db()
        valid_types = ("pilgrimage", "release_year", "pop_trad", "geo_culture")
        if dataset_type not in valid_types:
            return json.dumps({
                "error": f"Invalid dataset_type: {dataset_type}",
                "valid_types": list(valid_types),
            })

        # Optional prefecture bounding box
        bb = None
        if prefecture:
            pref = prefecture.lower().strip()
            if pref in _PREFECTURE_DEFS:
                name_ja, clat, clon, rad = _PREFECTURE_DEFS[pref]
                lat_off = rad / 111.0
                lon_off = rad / (111.0 * max(math.cos(math.radians(clat)), 0.01))
                bb = (clat - lat_off, clat + lat_off, clon - lon_off, clon + lon_off)

        data = []

        if dataset_type == "pilgrimage":
            geo_filter = ""
            params = []
            if bb:
                geo_filter = "AND loc.lat BETWEEN ? AND ? AND loc.lon BETWEEN ? AND ?"
                params = list(bb)
            rows = db.execute(f"""
                SELECT w.label_ja as work_name, w.label_en as work_name_en,
                       w.entity_type as work_type, w.release_year,
                       loc.label_ja as location_name, loc.label_en as location_name_en,
                       loc.lat, loc.lon, c.connection_type
                FROM connections c
                JOIN entities w ON c.entity_a_id = w.id
                JOIN entities loc ON c.entity_b_id = loc.id
                WHERE c.connection_type LIKE 'pilgrimage%'
                  AND w.is_dormant = 0 AND loc.is_dormant = 0
                  {geo_filter}
                LIMIT ?
            """, params + [limit]).fetchall()
            data = [dict(r) for r in rows]

        elif dataset_type == "release_year":
            geo_filter = ""
            params = []
            if bb:
                geo_filter = "AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
                params = list(bb)
            rows = db.execute(f"""
                SELECT label_ja, label_en, entity_type, release_year, release_year_source, source
                FROM entities
                WHERE release_year IS NOT NULL AND is_dormant = 0
                  {geo_filter}
                ORDER BY release_year DESC
                LIMIT ?
            """, params + [limit]).fetchall()
            data = [dict(r) for r in rows]

        elif dataset_type == "pop_trad":
            geo_filter = ""
            params = []
            if bb:
                geo_filter = """AND (EXISTS (SELECT 1 FROM entities e2
                     WHERE e2.id = c.entity_a_id AND e2.lat BETWEEN ? AND ? AND e2.lon BETWEEN ? AND ?)
                  OR EXISTS (SELECT 1 FROM entities e3
                     WHERE e3.id = c.entity_b_id AND e3.lat BETWEEN ? AND ? AND e3.lon BETWEEN ? AND ?))"""
                params = list(bb) + list(bb)
            rows = db.execute(f"""
                SELECT a.label_ja as entity_a, a.entity_type as type_a,
                       b.label_ja as entity_b, b.entity_type as type_b,
                       c.connection_type, c.explanation, c.confidence
                FROM connections c
                JOIN entities a ON c.entity_a_id = a.id
                JOIN entities b ON c.entity_b_id = b.id
                WHERE c.connection_type IN ('pop_traditional', 'cross_type_label_match')
                  AND a.is_dormant = 0 AND b.is_dormant = 0
                  {geo_filter}
                LIMIT ?
            """, params + [limit]).fetchall()
            data = [dict(r) for r in rows]

        elif dataset_type == "geo_culture":
            geo_filter = ""
            params = []
            if bb:
                geo_filter = "AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
                params = list(bb)
            rows = db.execute(f"""
                SELECT label_ja, label_en, entity_type, lat, lon, source
                FROM entities
                WHERE lat IS NOT NULL AND lon IS NOT NULL AND is_dormant = 0
                  {geo_filter}
                LIMIT ?
            """, params + [limit]).fetchall()
            data = [dict(r) for r in rows]

        return json.dumps({
            "source": "ontology_db (export_dataset)",
            "dataset_type": dataset_type,
            "prefecture": prefecture,
            "count": len(data),
            "limit": limit,
            "data": data,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"export_dataset failed: {str(e)}"})


# ── Entry point ────────────────────────────────────────────
if __name__ == "__main__":
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport not in ("stdio", "sse", "streamable-http"):
        transport = "stdio"
    if transport in ("sse", "streamable-http"):
        port = int(os.environ.get("PORT", "8008"))
        mcp.run(transport=transport, host="0.0.0.0", port=port)
    else:
        mcp.run(transport=transport)
