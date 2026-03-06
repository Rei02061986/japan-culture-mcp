#!/usr/bin/env python3
import json
import os
import textwrap
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from SPARQLWrapper import JSON as SPARQL_JSON
from SPARQLWrapper import SPARQLWrapper

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESP_DIR = os.path.join(BASE_DIR, "responses")
REPORT_PATH = os.path.join(BASE_DIR, "api_report.md")
TIMEOUT = 30
UA = "japan-culture-mcp-api-test/1.0"

os.makedirs(RESP_DIR, exist_ok=True)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save_json(name, obj):
    path = os.path.join(RESP_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    return path


def save_text(name, text):
    path = os.path.join(RESP_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def err_payload(api, endpoint, exc):
    return {
        "api": api,
        "endpoint": endpoint,
        "ok": False,
        "error": str(exc),
        "timestamp": now_iso(),
    }


def request_get(api, endpoint, params=None, headers=None, name="response.json"):
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    try:
        r = requests.get(endpoint, params=params, headers=h, timeout=TIMEOUT)
        ct = r.headers.get("Content-Type", "")
        payload = {
            "api": api,
            "endpoint": r.url,
            "ok": r.ok,
            "status_code": r.status_code,
            "content_type": ct,
            "headers": dict(r.headers),
            "timestamp": now_iso(),
        }
        if "json" in ct:
            try:
                payload["data"] = r.json()
            except Exception:
                payload["text"] = r.text
        else:
            payload["text"] = r.text
        save_json(name, payload)
        return payload
    except Exception as e:
        payload = err_payload(api, endpoint, e)
        save_json(name, payload)
        return payload


def request_post_graphql(api, endpoint, query, variables, name):
    headers = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": UA}
    body = {"query": query, "variables": variables}
    try:
        r = requests.post(endpoint, json=body, headers=headers, timeout=TIMEOUT)
        payload = {
            "api": api,
            "endpoint": endpoint,
            "ok": r.ok,
            "status_code": r.status_code,
            "content_type": r.headers.get("Content-Type", ""),
            "headers": dict(r.headers),
            "timestamp": now_iso(),
            "request": body,
        }
        try:
            payload["data"] = r.json()
        except Exception:
            payload["text"] = r.text
        save_json(name, payload)
        return payload
    except Exception as e:
        payload = err_payload(api, endpoint, e)
        save_json(name, payload)
        return payload


def request_sparql(api, endpoint, query, name):
    try:
        s = SPARQLWrapper(endpoint)
        s.setQuery(query)
        s.setReturnFormat(SPARQL_JSON)
        s.setTimeout(TIMEOUT)
        res = s.query().convert()
        payload = {
            "api": api,
            "endpoint": endpoint,
            "ok": True,
            "timestamp": now_iso(),
            "query": query,
            "data": res,
        }
        save_json(name, payload)
        return payload
    except Exception as e:
        payload = err_payload(api, endpoint, e)
        payload["query"] = query
        save_json(name, payload)
        return payload


def flatten_keys(obj, prefix=""):
    keys = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            nk = f"{prefix}.{k}" if prefix else k
            keys.add(nk)
            keys.update(flatten_keys(v, nk))
    elif isinstance(obj, list) and obj:
        keys.update(flatten_keys(obj[0], prefix + "[]" if prefix else "[]"))
    return keys


def first_record_from_anilist(payload):
    try:
        page = payload["data"]["data"]["Page"]
        media = page.get("media", [])
        return media[0] if media else {}
    except Exception:
        return {}


def first_record_from_sparql(payload):
    try:
        b = payload["data"]["results"]["bindings"]
        return b[0] if b else {}
    except Exception:
        return {}


def first_record_from_json_path(payload, path):
    cur = payload
    try:
        for p in path:
            cur = cur[p]
        if isinstance(cur, list) and cur:
            return cur[0]
        if isinstance(cur, dict):
            return cur
    except Exception:
        pass
    return {}


def md_table_fields(first):
    if not first:
        return "| フィールド | 型 |\n|---|---|\n| (取得不可) | - |"
    rows = ["| フィールド | 型 |", "|---|---|"]
    for k, v in sorted(first.items()):
        t = type(v).__name__
        rows.append(f"| `{k}` | `{t}` |")
    return "\n".join(rows)


def short_json(obj, max_chars=1200):
    s = json.dumps(obj, ensure_ascii=False, indent=2)
    if len(s) > max_chars:
        return s[:max_chars] + "\n... (truncated)"
    return s


def status_mark(*payloads):
    for p in payloads:
        if p.get("ok"):
            return "✅ 成功"
    return "❌ 失敗"


def is_ok(payload):
    return isinstance(payload, dict) and bool(payload.get("ok"))


def has_dns_error(payload):
    if not isinstance(payload, dict):
        return False
    e = str(payload.get("error", ""))
    keywords = ["NameResolutionError", "nodename nor servname provided", "Failed to resolve"]
    return any(k in e for k in keywords)


def run_tests():
    results = {}

    # 1) AniList GraphQL
    anilist_ep = "https://graphql.anilist.co"
    q1 = """
    query ($search: String, $type: MediaType, $sort: [MediaSort], $perPage: Int) {
      Page(page: 1, perPage: $perPage) {
        media(search: $search, type: $type, sort: $sort) {
          id
          title { romaji english native }
          genres
          tags { name category }
          description
          seasonYear
          studios(isMain: true) { nodes { id name } }
          averageScore
          popularity
          siteUrl
        }
      }
    }
    """
    q2 = """
    query ($search: String, $type: MediaType) {
      Media(search: $search, type: $type) {
        id
        title { romaji english native }
        genres
        tags { name category }
        description
        seasonYear
        studios(isMain: true) { nodes { id name } }
        averageScore
        popularity
        siteUrl
        characters(page: 1, perPage: 5) {
          edges {
            role
            node { id name { full native } siteUrl }
          }
        }
        relations {
          edges {
            relationType
            node { id type title { romaji native } siteUrl }
          }
        }
        externalLinks {
          id
          site
          url
          language
          type
        }
      }
    }
    """
    q3 = """
    query {
      GenreCollection
      MediaTagCollection {
        id
        name
        category
        isAdult
      }
    }
    """
    results["anilist_q1"] = request_post_graphql("AniList", anilist_ep, q1, {
        "search": "yokai", "type": "ANIME", "sort": ["POPULARITY_DESC"], "perPage": 10
    }, "anilist_query1.json")
    results["anilist_q2"] = request_post_graphql("AniList", anilist_ep, q2, {
        "search": "Mushishi", "type": "ANIME"
    }, "anilist_query2.json")
    results["anilist_q3"] = request_post_graphql("AniList", anilist_ep, q3, {}, "anilist_query3.json")

    # 2) Japan Search SPARQL + Web API
    jp_sparql_ep = "https://jpsearch.go.jp/api/sparql"
    jq1 = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?s ?label WHERE {
      ?s rdfs:label ?label .
      FILTER(CONTAINS(STR(?label), "浅草"))
    }
    LIMIT 20
    """
    jq2 = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?s ?label WHERE {
      ?s rdfs:label ?label .
      FILTER(CONTAINS(STR(?label), "妖怪"))
    }
    LIMIT 20
    """
    results["jpsearch_sparql_q1"] = request_sparql("JapanSearch SPARQL", jp_sparql_ep, jq1, "jpsearch_sparql_q1.json")
    results["jpsearch_sparql_q2"] = request_sparql("JapanSearch SPARQL", jp_sparql_ep, jq2, "jpsearch_sparql_q2.json")
    results["jpsearch_item_search"] = request_get(
        "JapanSearch ItemSearch",
        "https://jpsearch.go.jp/api/item/search",
        params={"q": "妖怪", "size": 5},
        name="jpsearch_item_search.json",
    )

    # 3) MADB SPARQL
    madb_ep = "https://mediaarts-db.artmuseums.go.jp/sparql"
    mq1 = """
    PREFIX schema: <http://schema.org/>
    SELECT ?s ?name WHERE {
      ?s schema:name ?name .
      FILTER(CONTAINS(STR(?name), "妖怪") && CONTAINS(STR(?name), "アニメ"))
    }
    LIMIT 20
    """
    mq2 = """
    PREFIX schema: <http://schema.org/>
    SELECT ?s ?name ?description WHERE {
      ?s schema:name ?name .
      OPTIONAL { ?s schema:description ?description }
      FILTER(CONTAINS(STR(?name), "ゲゲゲの鬼太郎"))
    }
    LIMIT 20
    """
    mq3 = """
    PREFIX schema: <http://schema.org/>
    SELECT ?s ?name WHERE {
      ?s schema:name ?name .
      FILTER(CONTAINS(STR(?name), "浮世絵") && CONTAINS(STR(?name), "漫画"))
    }
    LIMIT 20
    """
    mq4 = """
    SELECT DISTINCT ?class WHERE {
      ?s a ?class .
    }
    LIMIT 100
    """
    results["madb_q1"] = request_sparql("MADB SPARQL", madb_ep, mq1, "madb_sparql_q1.json")
    results["madb_q2"] = request_sparql("MADB SPARQL", madb_ep, mq2, "madb_sparql_q2.json")
    results["madb_q3"] = request_sparql("MADB SPARQL", madb_ep, mq3, "madb_sparql_q3.json")
    results["madb_classes"] = request_sparql("MADB SPARQL", madb_ep, mq4, "madb_ontology_classes.json")

    # 4) NDL
    ndl_search = request_get(
        "NDL Lab Search",
        "https://lab.ndl.go.jp/dl/api/search",
        params={"keyword": "浅草", "rows": 5},
        name="ndl_search.json",
    )
    results["ndl_search"] = ndl_search

    sru_url = "https://iss.ndl.go.jp/api/sru"
    sru_params = {
        "operation": "searchRetrieve",
        "query": "浅草 AND 浮世絵",
        "maximumRecords": 5,
        "recordSchema": "dcndl",
    }
    sru_res = request_get("NDL SRU", sru_url, params=sru_params, name="ndl_sru.json")
    results["ndl_sru"] = sru_res
    if sru_res.get("ok") and "text" in sru_res:
        save_text("ndl_sru.xml", sru_res.get("text", ""))

    # IIIF manifest trial from search result
    iiif_trial = {"api": "NDL IIIF Manifest", "ok": False, "timestamp": now_iso()}
    try:
        pid = None
        if ndl_search.get("ok"):
            data = ndl_search.get("data", {})
            # Try common candidates
            candidates = []
            if isinstance(data, dict):
                if isinstance(data.get("items"), list) and data["items"]:
                    candidates.extend(data["items"])
                if isinstance(data.get("result"), list) and data["result"]:
                    candidates.extend(data["result"])
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                for key in ["pid", "PID", "id", "identifier"]:
                    if key in item and item[key]:
                        pid = str(item[key])
                        break
                if pid:
                    break
        iiif_trial["pid"] = pid
        if pid:
            manifest_ep = f"https://lab.ndl.go.jp/dl/api/iiif/{pid}/manifest"
            m = request_get("NDL IIIF Manifest", manifest_ep, name="ndl_iiif_manifest.json")
            iiif_trial.update(m)
        else:
            iiif_trial["error"] = "PID not found in search response"
            save_json("ndl_iiif_manifest.json", iiif_trial)
    except Exception as e:
        iiif_trial["error"] = str(e)
        save_json("ndl_iiif_manifest.json", iiif_trial)
    results["ndl_iiif"] = iiif_trial

    # 5) ColBase
    results["colbase_home"] = request_get("ColBase", "https://colbase.nich.go.jp/", name="colbase_home.json")
    results["colbase_search"] = request_get(
        "ColBase",
        "https://colbase.nich.go.jp/collection_items",
        params={"locale": "ja", "keyword": "浮世絵"},
        name="colbase_search.json",
    )
    results["colbase_via_jpsearch"] = request_get(
        "ColBase via JapanSearch",
        "https://jpsearch.go.jp/api/item/search",
        params={"q": "colbase 浮世絵", "size": 5},
        name="colbase_via_jpsearch.json",
    )

    # 6) WebGIS + sitereports
    heritage_candidates = [
        "https://heritagemap.nabunken.go.jp/",
        "https://heritagemap.nabunken.go.jp/geoserver/wms?service=WMS&request=GetCapabilities",
        "https://heritagemap.nabunken.go.jp/geoserver/wfs?service=WFS&request=GetCapabilities",
        "https://heritagemap.nabunken.go.jp/api",
    ]
    heritage_results = []
    for idx, u in enumerate(heritage_candidates, start=1):
        r = request_get("HeritageMap", u, name=f"heritagemap_probe_{idx}.json")
        heritage_results.append(r)
    results["heritage_probes"] = heritage_results

    results["sitereports_api"] = request_get(
        "SiteReports API", "https://sitereports.nabunken.go.jp/api", name="sitereports_api_root.json"
    )
    results["sitereports_search"] = request_get(
        "SiteReports API", "https://sitereports.nabunken.go.jp/api/search", params={"q": "浅草", "size": 5}, name="sitereports_api_search.json"
    )

    # 7) CODH
    results["codh_pmjt_iiif"] = request_get("CODH", "https://codh.rois.ac.jp/pmjt/iiif/", name="codh_pmjt_iiif.json")
    results["codh_char_shape"] = request_get("CODH", "https://codh.rois.ac.jp/char-shape/", name="codh_char_shape.json")
    results["codh_edo_maps"] = request_get("CODH", "https://codh.rois.ac.jp/edo-maps/", name="codh_edo_maps.json")

    return results


def build_report(results):
    anilist_sample = first_record_from_anilist(results.get("anilist_q1", {}))
    anilist_fields = md_table_fields(anilist_sample)

    jps_sample = first_record_from_sparql(results.get("jpsearch_sparql_q1", {}))
    jps_fields = md_table_fields(jps_sample)

    madb_sample = first_record_from_sparql(results.get("madb_q1", {}))
    madb_fields = md_table_fields(madb_sample)

    ndl_sample = first_record_from_json_path(results.get("ndl_search", {}), ["data", "items"])
    if not ndl_sample:
        ndl_sample = first_record_from_json_path(results.get("ndl_search", {}), ["data", "result"])
    ndl_fields = md_table_fields(ndl_sample)

    colbase_sample = {
        "status_code": results.get("colbase_search", {}).get("status_code"),
        "content_type": results.get("colbase_search", {}).get("content_type"),
        "url": results.get("colbase_search", {}).get("endpoint"),
    }
    colbase_fields = md_table_fields(colbase_sample)

    # Pick first successful heritage probe for sample
    h_sample = {}
    for p in results.get("heritage_probes", []):
        if p.get("ok"):
            h_sample = {
                "status_code": p.get("status_code"),
                "content_type": p.get("content_type"),
                "url": p.get("endpoint"),
            }
            break
    heritage_fields = md_table_fields(h_sample)

    codh_sample = {
        "pmjt_iiif_status": results.get("codh_pmjt_iiif", {}).get("status_code"),
        "char_shape_status": results.get("codh_char_shape", {}).get("status_code"),
        "edo_maps_status": results.get("codh_edo_maps", {}).get("status_code"),
    }
    codh_fields = md_table_fields(codh_sample)

    api_ok = {
        "AniList GraphQL": any(is_ok(results.get(k, {})) for k in ["anilist_q1", "anilist_q2", "anilist_q3"]),
        "Japan Search SPARQL/Web API": any(is_ok(results.get(k, {})) for k in ["jpsearch_sparql_q1", "jpsearch_sparql_q2", "jpsearch_item_search"]),
        "MADB SPARQL": any(is_ok(results.get(k, {})) for k in ["madb_q1", "madb_q2", "madb_q3", "madb_classes"]),
        "NDL (Search/SRU/IIIF)": any(is_ok(results.get(k, {})) for k in ["ndl_search", "ndl_sru", "ndl_iiif"]),
        "ColBase": any(is_ok(results.get(k, {})) for k in ["colbase_home", "colbase_search", "colbase_via_jpsearch"]),
        "HeritageMap/SiteReports": any(
            [is_ok(p) for p in results.get("heritage_probes", [])]
            + [is_ok(results.get("sitereports_api", {})), is_ok(results.get("sitereports_search", {}))]
        ),
        "CODH": any(is_ok(results.get(k, {})) for k in ["codh_pmjt_iiif", "codh_char_shape", "codh_edo_maps"]),
    }

    all_payloads = []
    for v in results.values():
        if isinstance(v, list):
            all_payloads.extend(v)
        else:
            all_payloads.append(v)
    dns_blocked = any(has_dns_error(p) for p in all_payloads)

    working_apis = [k for k, v in api_ok.items() if v]
    failed_apis = [k for k, v in api_ok.items() if not v]

    working_md = "\n".join(f"  - {a}" for a in working_apis) if working_apis else "  - 該当なし（今回実行では成功レスポンスなし）"
    failed_md = "\n".join(f"  - {a}" for a in failed_apis) if failed_apis else "  - 該当なし"
    unexpected = [
        "同一ドメインでもJSON/HTML/XMLが混在し、MCPツールで正規化層が必要",
        "IIIF manifestはPID依存で、検索レスポンスからPID抽出ロジックが必要",
    ]
    if dns_blocked:
        unexpected.insert(0, "実行環境のDNS解決が制限され、全APIで外部疎通に失敗")
    unexpected_md = "\n".join(f"  - {x}" for x in unexpected)

    report = f"""# API疎通テストレポート

- 実行日時(UTC): {now_iso()}
- 実行スクリプト: `scripts/api_connectivity_test.py`
- レスポンス保存先: `responses/`

## 1. AniList GraphQL
- 疎通結果: {status_mark(results.get('anilist_q1', {}), results.get('anilist_q2', {}), results.get('anilist_q3', {}))}
- エンドポイントURL: `https://graphql.anilist.co`
- 認証要件: 不要
- レスポンス形式: JSON (GraphQL)
- サンプル（最初の1レコード）:

```json
{short_json(anilist_sample)}
```

- フィールド一覧
{anilist_fields}
- レート制限・最大取得件数: 90 req/min（指定情報）。`perPage` で件数制御。
- 多言語対応状況: `title.romaji/english/native` により多言語タイトル対応を確認。
- MCP設計メモ:
  - ツール名案: `anilist_search_media`, `anilist_get_media_detail`, `anilist_list_tags`
  - パラメータ設計案: `query`, `type`, `sort`, `page`, `per_page`, `include_relations`, `include_characters`
  - キャッシュ戦略: 検索結果TTL短め(10-30分)、作品詳細TTL長め(24h)
  - 他APIとの接続ポイント: 作品名・タグをJP/MADB検索キーワードに連携

## 2. ジャパンサーチ SPARQL / Web API
- 疎通結果: {status_mark(results.get('jpsearch_sparql_q1', {}), results.get('jpsearch_sparql_q2', {}), results.get('jpsearch_item_search', {}))}
- エンドポイントURL: `https://jpsearch.go.jp/api/sparql`, `https://jpsearch.go.jp/api/item/search`
- 認証要件: 不要
- レスポンス形式: SPARQL JSON / JSON
- サンプル（最初の1レコード）:

```json
{short_json(jps_sample)}
```

- フィールド一覧
{jps_fields}
- レート制限・最大取得件数: 公開ドキュメント上の明示値は今回未確認。SPARQLは`LIMIT`で制御。
- 多言語対応状況: ラベル文字列依存（日本語含む）。
- MCP設計メモ:
  - ツール名案: `jpsearch_sparql_query`, `jpsearch_item_search`
  - パラメータ設計案: `keyword`, `limit`, `offset`, `sparql_query`
  - キャッシュ戦略: クエリ文字列ハッシュをキーに12-24h
  - 他APIとの接続ポイント: ColBaseやNDL由来のキーワード/著者名を横断投入

## 3. MADB SPARQL
- 疎通結果: {status_mark(results.get('madb_q1', {}), results.get('madb_q2', {}), results.get('madb_q3', {}), results.get('madb_classes', {}))}
- エンドポイントURL: `https://mediaarts-db.artmuseums.go.jp/sparql`
- 認証要件: 不要
- レスポンス形式: SPARQL JSON
- サンプル（最初の1レコード）:

```json
{short_json(madb_sample)}
```

- フィールド一覧
{madb_fields}
- レート制限・最大取得件数: 明示値未確認。`LIMIT`指定で制御。
- 多言語対応状況: 作品名・説明の文字列に日本語を含む可能性あり。
- MCP設計メモ:
  - ツール名案: `madb_search_titles`, `madb_get_title_detail`, `madb_list_classes`
  - パラメータ設計案: `keyword`, `limit`, `class_uri`, `include_description`
  - キャッシュ戦略: オントロジーは長期キャッシュ(7日)、検索は24h
  - 他APIとの接続ポイント: AniList作品名との照合、時代語彙(浮世絵等)をCODHへ接続

## 4. NDL 次世代デジタルライブラリー
- 疎通結果: {status_mark(results.get('ndl_search', {}), results.get('ndl_sru', {}), results.get('ndl_iiif', {}))}
- エンドポイントURL: `https://lab.ndl.go.jp/dl/api/search`, `https://iss.ndl.go.jp/api/sru`
- 認証要件: 不要
- レスポンス形式: JSON / XML(SRU)
- サンプル（最初の1レコード）:

```json
{short_json(ndl_sample)}
```

- フィールド一覧
{ndl_fields}
- レート制限・最大取得件数: `rows`, `maximumRecords` で制御（明示レート未確認）。
- 多言語対応状況: 日本語クエリ対応を前提に検索可能。
- MCP設計メモ:
  - ツール名案: `ndl_search`, `ndl_sru_search`, `ndl_get_iiif_manifest`
  - パラメータ設計案: `keyword`, `rows`, `query`, `record_schema`, `pid`
  - キャッシュ戦略: 検索結果6-24h、manifest 7日
  - 他APIとの接続ポイント: 地名(浅草)や主題語をWebGIS・JP Searchへ連携

## 5. ColBase
- 疎通結果: {status_mark(results.get('colbase_home', {}), results.get('colbase_search', {}), results.get('colbase_via_jpsearch', {}))}
- エンドポイントURL: `https://colbase.nich.go.jp/`, `https://colbase.nich.go.jp/collection_items`
- 認証要件: 不要（閲覧時）
- レスポンス形式: 主にHTML（公開APIは今回確認できず）
- サンプル（最初の1レコード）:

```json
{short_json(colbase_sample)}
```

- フィールド一覧
{colbase_fields}
- レート制限・最大取得件数: 公開API仕様としては未確認。
- 多言語対応状況: `locale=ja` パラメータあり。
- MCP設計メモ:
  - ツール名案: `colbase_search_web`（スクレイピング前提は要利用規約確認）
  - パラメータ設計案: `keyword`, `locale`, `page`
  - キャッシュ戦略: HTML検索結果を短期(1-6h)
  - 他APIとの接続ポイント: JP Search検索語の補完先として利用

## 6. 文化財総覧WebGIS / 全国遺跡報告総覧API
- 疎通結果: {status_mark(*(results.get('heritage_probes', []) + [results.get('sitereports_api', {}), results.get('sitereports_search', {})]))}
- エンドポイントURL: `https://heritagemap.nabunken.go.jp/`, `https://sitereports.nabunken.go.jp/api`
- 認証要件: 不要
- レスポンス形式: HTML/XML/JSON（エンドポイントに依存）
- サンプル（最初の1レコード）:

```json
{short_json(h_sample)}
```

- フィールド一覧
{heritage_fields}
- レート制限・最大取得件数: 未確認。
- 多言語対応状況: 日本語UI/地名を前提。
- MCP設計メモ:
  - ツール名案: `heritagemap_probe_layers`, `sitereports_search`
  - パラメータ設計案: `bbox`, `keyword`, `limit`, `format`
  - キャッシュ戦略: 地理レイヤ情報は日次、検索結果は24h
  - 他APIとの接続ポイント: 地名キー(浅草等)でNDL/JP Searchと連携

## 7. CODH
- 疎通結果: {status_mark(results.get('codh_pmjt_iiif', {}), results.get('codh_char_shape', {}), results.get('codh_edo_maps', {}))}
- エンドポイントURL: `https://codh.rois.ac.jp/pmjt/iiif/`, `https://codh.rois.ac.jp/char-shape/`, `https://codh.rois.ac.jp/edo-maps/`
- 認証要件: 不要
- レスポンス形式: 主にHTML（IIIF配下にJSON manifestが存在する可能性）
- サンプル（最初の1レコード）:

```json
{short_json(codh_sample)}
```

- フィールド一覧
{codh_fields}
- レート制限・最大取得件数: 未確認。
- 多言語対応状況: 日本語データセット中心、英語説明ページ混在。
- MCP設計メモ:
  - ツール名案: `codh_list_datasets`, `codh_fetch_iiif`, `codh_search_charshape`
  - パラメータ設計案: `dataset`, `manifest_url`, `keyword`
  - キャッシュ戦略: データセット索引は長期(7日)、manifestは3-7日
  - 他APIとの接続ポイント: 浮世絵・古典籍キーワードをNDL/MADBへ展開

## 総合所見
- 動いたAPI（MCP統合に使える）:
{working_md}
- 動かなかった/制限が大きいAPI:
{failed_md}
- 想定外の発見:
{unexpected_md}
- 横断接続ポイント（地理・作品ID・人物・時代）:
  - 地理: 浅草など地名キーワードでNDL・WebGIS・SiteReports連携
  - 作品ID: AniList `id/siteUrl`、NDL `pid`、IIIF `manifest`
  - 人物: 作品著者・キャラ名をJP Search/MADB検索語へ流用
  - 時代: 浮世絵・江戸・妖怪を主題語として横断検索
- 次のステップへの推奨:
  1. 成功したAPIのレスポンススキーマを固定化し、Pydanticモデルを定義
  2. WebGIS/SiteReportsの正式APIドキュメントを調査し、地理検索ツールを設計
  3. ColBase/CODHは利用規約と提供フォーマットを確認し、公式API優先で統合方針を決定
"""

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)


def main():
    results = run_tests()
    build_report(results)
    save_json("run_summary.json", {
        "generated_at": now_iso(),
        "report": REPORT_PATH,
        "responses_dir": RESP_DIR,
        "keys": sorted(list(results.keys())),
    })
    print("Done. Report generated:", REPORT_PATH)


if __name__ == "__main__":
    main()
