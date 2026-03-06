"""妖怪 Integration Test — Phase 2 横断テスト
テーマ「妖怪」で全データソースを横断検索し、MCP ツール相当の処理を実行。
Python 3.8 compatible (VM execution).
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

BASE_DIR = Path(__file__).parent.parent
RESP_DIR = BASE_DIR / "responses" / "yokai_integration"
RESP_DIR.mkdir(parents=True, exist_ok=True)

UA = "japan-culture-mcp/0.2 (research-project)"
TIMEOUT = 30


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save(name, data):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, str):
            f.write(data)
        else:
            json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {name}")
    return str(path)


# ================================================================
# 1. AniList — search_anime equivalent
# ================================================================
def test_anilist_yokai():
    print("\n" + "=" * 60)
    print("[1] AniList: search_anime('妖怪')")
    print("=" * 60)

    query = """
    query ($search: String!, $type: MediaType, $perPage: Int) {
      Page(page: 1, perPage: $perPage) {
        pageInfo { total currentPage lastPage hasNextPage }
        media(search: $search, type: $type, sort: POPULARITY_DESC) {
          id
          title { romaji english native }
          type format genres
          tags { name category rank }
          description(asHtml: false)
          seasonYear season
          studios(isMain: true) { nodes { id name } }
          averageScore popularity siteUrl
        }
      }
    }
    """
    try:
        resp = requests.post(
            "https://graphql.anilist.co",
            json={"query": query, "variables": {"search": "妖怪", "type": "ANIME", "perPage": 10}},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        media = data.get("data", {}).get("Page", {}).get("media", [])
        result = {
            "source": "AniList",
            "query": "妖怪",
            "ok": True,
            "total": data.get("data", {}).get("Page", {}).get("pageInfo", {}).get("total", 0),
            "returned": len(media),
            "results": [
                {
                    "id": m.get("id"),
                    "title_romaji": m.get("title", {}).get("romaji"),
                    "title_native": m.get("title", {}).get("native"),
                    "genres": m.get("genres", []),
                    "year": m.get("seasonYear"),
                    "score": m.get("averageScore"),
                    "url": m.get("siteUrl"),
                }
                for m in media
            ],
            "ts": now_iso(),
        }
        save("01_anilist_yokai.json", result)
        print(f"  [OK] {len(media)} anime found")
        return result
    except Exception as e:
        result = {"source": "AniList", "ok": False, "error": str(e), "ts": now_iso()}
        save("01_anilist_yokai.json", result)
        print(f"  [FAIL] {e}")
        return result


# ================================================================
# 2. MADB — search_media_arts equivalent
# ================================================================
def test_madb_yokai():
    print("\n" + "=" * 60)
    print("[2] MADB: search_media_arts('妖怪')")
    print("=" * 60)

    query = """
    PREFIX schema: <https://schema.org/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/data/property#>
    PREFIX madbclass: <https://mediaarts-db.artmuseums.go.jp/data/class#>

    SELECT ?item ?label ?type ?datePublished ?genre ?creator ?description
    WHERE {
      ?item rdfs:label ?label .
      ?item a ?type .
      FILTER(CONTAINS(STR(?label), "妖怪"))
      OPTIONAL { ?item schema:datePublished ?datePublished }
      OPTIONAL { ?item schema:genre ?genre }
      OPTIONAL { ?item schema:creator ?creator }
      OPTIONAL { ?item schema:description ?description }
    }
    LIMIT 20
    """
    try:
        resp = requests.get(
            "https://mediaarts-db.artmuseums.go.jp/sparql",
            params={"query": query, "output": "json"},
            headers={"Accept": "application/sparql-results+json"},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        bindings = data.get("results", {}).get("bindings", [])
        items = []
        for b in bindings:
            item = {}
            for k in ("item", "label", "type", "datePublished", "genre", "creator", "description"):
                if k in b:
                    item[k] = b[k].get("value", "")
            if "type" in item:
                item["type_label"] = item["type"].rsplit("#", 1)[-1] if "#" in item["type"] else item["type"].rsplit("/", 1)[-1]
            items.append(item)

        type_counts = {}
        for it in items:
            t = it.get("type_label", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1

        result = {
            "source": "MADB",
            "query": "妖怪",
            "ok": True,
            "returned": len(items),
            "type_breakdown": type_counts,
            "results": items,
            "ts": now_iso(),
        }
        save("02_madb_yokai.json", result)
        print(f"  [OK] {len(items)} items, types: {type_counts}")
        return result
    except Exception as e:
        result = {"source": "MADB", "ok": False, "error": str(e), "ts": now_iso()}
        save("02_madb_yokai.json", result)
        print(f"  [FAIL] {e}")
        return result


# ================================================================
# 3. JapanSearch — search_japan_search equivalent
# ================================================================
def test_jpsearch_yokai():
    print("\n" + "=" * 60)
    print("[3] ジャパンサーチ: search_japan_search('妖怪')")
    print("=" * 60)

    # SPARQL method
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX schema: <https://schema.org/>
    SELECT ?item ?label ?type ?provider ?thumbnail WHERE {
      ?item rdfs:label ?label .
      FILTER(CONTAINS(?label, "妖怪"))
      OPTIONAL { ?item schema:additionalType ?type }
      OPTIONAL { ?item schema:provider ?provider }
      OPTIONAL { ?item schema:thumbnail ?thumbnail }
    }
    LIMIT 20
    """
    try:
        resp = requests.get(
            "https://jpsearch.go.jp/rdf/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json", "User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        bindings = data.get("results", {}).get("bindings", [])
        items = []
        for b in bindings:
            row = {}
            for k, v in b.items():
                row[k] = v.get("value", "")
            items.append(row)

        result = {
            "source": "JapanSearch SPARQL",
            "query": "妖怪",
            "ok": True,
            "returned": len(items),
            "results": items,
            "ts": now_iso(),
        }
        save("03_jpsearch_yokai_sparql.json", result)
        print(f"  [OK] SPARQL: {len(items)} items")
    except Exception as e:
        result = {"source": "JapanSearch SPARQL", "ok": False, "error": str(e), "ts": now_iso()}
        save("03_jpsearch_yokai_sparql.json", result)
        print(f"  [FAIL] SPARQL: {e}")

    time.sleep(1)

    # Easy API method
    try:
        resp2 = requests.get(
            "https://jpsearch.go.jp/rdf/es",
            params={"keyword": "妖怪", "format": "json"},
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp2.raise_for_status()
        data2 = resp2.json() if "json" in resp2.headers.get("Content-Type", "") else {"text": resp2.text[:2000]}
        result2 = {
            "source": "JapanSearch Easy",
            "query": "妖怪",
            "ok": True,
            "data": data2,
            "ts": now_iso(),
        }
        save("03_jpsearch_yokai_easy.json", result2)
        print(f"  [OK] Easy API response received")
    except Exception as e:
        result2 = {"source": "JapanSearch Easy", "ok": False, "error": str(e), "ts": now_iso()}
        save("03_jpsearch_yokai_easy.json", result2)
        print(f"  [FAIL] Easy API: {e}")


# ================================================================
# 4. Wikidata — search_wikidata + resolve_entity equivalent
# ================================================================
def test_wikidata_yokai():
    print("\n" + "=" * 60)
    print("[4] Wikidata: search_wikidata('妖怪') + resolve_entity")
    print("=" * 60)

    # 4a. Search anime type (use rdfs:label directly, not ?itemLabel from SERVICE)
    query_anime = """
    SELECT ?item ?itemLabel ?studioLabel ?authorLabel ?startDate WHERE {
      ?item wdt:P31/wdt:P279* wd:Q1107 ;
            rdfs:label ?l .
      FILTER(LANG(?l) = "ja")
      FILTER(CONTAINS(?l, "妖怪"))
      OPTIONAL { ?item wdt:P272 ?studio }
      OPTIONAL { ?item wdt:P50 ?author }
      OPTIONAL { ?item wdt:P580 ?startDate }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
    }
    LIMIT 30
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "japan-culture-mcp/0.2 (teddykmk@gmail.com)",
    }
    try:
        resp = requests.get(
            "https://query.wikidata.org/sparql",
            params={"query": query_anime},
            headers=headers,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        bindings = data.get("results", {}).get("bindings", [])
        items = [{k: v.get("value", "") for k, v in b.items()} for b in bindings]
        result = {
            "source": "Wikidata (anime query)",
            "query": "妖怪",
            "ok": True,
            "returned": len(items),
            "results": items,
            "ts": now_iso(),
        }
        save("04_wikidata_yokai_anime.json", result)
        print(f"  [OK] Anime: {len(items)} items")
    except Exception as e:
        result = {"source": "Wikidata anime", "ok": False, "error": str(e), "ts": now_iso()}
        save("04_wikidata_yokai_anime.json", result)
        print(f"  [FAIL] Anime: {e}")

    time.sleep(2)

    # 4b. Resolve entity "鳥山石燕" (Toriyama Sekien - yokai artist)
    query_resolve = """
    SELECT ?item ?itemLabel ?itemDescription
           ?ndl ?madb ?viaf ?isni ?image WHERE {
      ?item wdt:P31 wd:Q5 ;
            rdfs:label ?l .
      FILTER(LANG(?l) = "ja")
      FILTER(CONTAINS(?l, "鳥山石燕"))
      OPTIONAL { ?item wdt:P349  ?ndl }
      OPTIONAL { ?item wdt:P4082 ?madb }
      OPTIONAL { ?item wdt:P214  ?viaf }
      OPTIONAL { ?item wdt:P213  ?isni }
      OPTIONAL { ?item wdt:P18   ?image }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
    }
    LIMIT 5
    """
    try:
        resp2 = requests.get(
            "https://query.wikidata.org/sparql",
            params={"query": query_resolve},
            headers=headers,
            timeout=TIMEOUT,
        )
        resp2.raise_for_status()
        data2 = resp2.json()
        bindings2 = data2.get("results", {}).get("bindings", [])
        items2 = [{k: v.get("value", "") for k, v in b.items()} for b in bindings2]
        result2 = {
            "source": "Wikidata (resolve_entity: 鳥山石燕)",
            "ok": True,
            "returned": len(items2),
            "results": items2,
            "ts": now_iso(),
        }
        save("04_wikidata_toriyama_resolve.json", result2)
        print(f"  [OK] Toriyama Sekien resolve: {len(items2)} items")
    except Exception as e:
        result2 = {"source": "Wikidata resolve", "ok": False, "error": str(e), "ts": now_iso()}
        save("04_wikidata_toriyama_resolve.json", result2)
        print(f"  [FAIL] Resolve: {e}")


# ================================================================
# 5. NDL — search_ndl + get_ndl_manifest equivalent
# ================================================================
def test_ndl_yokai():
    print("\n" + "=" * 60)
    print("[5] NDL: search_ndl('妖怪') + IIIF manifest")
    print("=" * 60)

    # 5a. SRU search (CQL syntax: anywhere="keyword")
    try:
        resp = requests.get(
            "https://iss.ndl.go.jp/api/sru",
            params={
                "operation": "searchRetrieve",
                "query": 'anywhere="妖怪"',
                "maximumRecords": "10",
                "recordSchema": "dcndl",
            },
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        text = resp.text
        records = re.findall(r"<recordData>(.*?)</recordData>", text, re.DOTALL)
        items = []
        for rec in records:
            title_m = re.search(r"<dc:title[^>]*>([^<]+)</dc:title>", rec)
            creator_m = re.search(r"<dc:creator[^>]*>([^<]+)</dc:creator>", rec)
            date_m = re.search(r"<dc:date[^>]*>([^<]+)</dc:date>", rec)
            pub_m = re.search(r"<dc:publisher[^>]*>([^<]+)</dc:publisher>", rec)
            items.append({
                "title": title_m.group(1) if title_m else "",
                "creator": creator_m.group(1) if creator_m else "",
                "date": date_m.group(1) if date_m else "",
                "publisher": pub_m.group(1) if pub_m else "",
            })
        total_m = re.search(r"<numberOfRecords>(\d+)</numberOfRecords>", text)
        total = int(total_m.group(1)) if total_m else len(items)

        result = {
            "source": "NDL SRU",
            "query": "妖怪",
            "ok": True,
            "total_found": total,
            "returned": len(items),
            "results": items,
            "ts": now_iso(),
        }
        save("05_ndl_yokai_sru.json", result)
        print(f"  [OK] SRU: {len(items)} records (total: {total})")
    except Exception as e:
        result = {"source": "NDL SRU", "ok": False, "error": str(e), "ts": now_iso()}
        save("05_ndl_yokai_sru.json", result)
        print(f"  [FAIL] SRU: {e}")

    time.sleep(1)

    # 5b. IIIF manifest for 北斎漫画 (contains yokai illustrations)
    try:
        resp2 = requests.get(
            "https://www.dl.ndl.go.jp/api/iiif/1286328/manifest.json",
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp2.raise_for_status()
        data2 = resp2.json()
        label = data2.get("label", "")
        sequences = data2.get("sequences", [])
        page_count = len(sequences[0].get("canvases", [])) if sequences else 0

        result2 = {
            "source": "NDL IIIF",
            "pid": "1286328",
            "ok": True,
            "label": label,
            "total_pages": page_count,
            "ts": now_iso(),
        }
        save("05_ndl_iiif_hokusai_manga.json", result2)
        print(f"  [OK] IIIF: {label}, {page_count} pages")
    except Exception as e:
        result2 = {"source": "NDL IIIF", "ok": False, "error": str(e), "ts": now_iso()}
        save("05_ndl_iiif_hokusai_manga.json", result2)
        print(f"  [FAIL] IIIF: {e}")


# ================================================================
# 6. DBpedia — search_dbpedia_ja equivalent
# ================================================================
def test_dbpedia_yokai():
    print("\n" + "=" * 60)
    print("[6] DBpedia: search_dbpedia_ja('鳥山石燕')")
    print("=" * 60)

    query = """
    SELECT ?prop ?value WHERE {
      <http://ja.dbpedia.org/resource/鳥山石燕> ?prop ?value .
    }
    LIMIT 50
    """
    try:
        resp = requests.get(
            "https://ja.dbpedia.org/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json", "User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        bindings = data.get("results", {}).get("bindings", [])
        props = {}
        for b in bindings:
            p = b.get("prop", {}).get("value", "")
            p_short = p.rsplit("/", 1)[-1] if "/" in p else p
            v = b.get("value", {}).get("value", "")
            if p_short not in props:
                props[p_short] = []
            if v not in props[p_short]:
                props[p_short].append(v)

        result = {
            "source": "DBpedia Japanese",
            "resource": "鳥山石燕",
            "ok": True,
            "property_count": len(props),
            "properties": props,
            "ts": now_iso(),
        }
        save("06_dbpedia_toriyama.json", result)
        print(f"  [OK] {len(props)} properties")
    except Exception as e:
        result = {"source": "DBpedia", "ok": False, "error": str(e), "ts": now_iso()}
        save("06_dbpedia_toriyama.json", result)
        print(f"  [FAIL] {e}")


# ================================================================
# 7. GSI Tiles — get_map_tile_url equivalent
# ================================================================
def test_gsi_yokai():
    print("\n" + "=" * 60)
    print("[7] GSI: 調布市（ゲゲゲの鬼太郎の舞台）")
    print("=" * 60)

    # Chofu city, Tokyo (setting of GeGeGe no Kitaro)
    lat, lon = 35.6505, 139.5418
    zoom = 15

    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)

    tile_url = f"https://cyberjapandata.gsi.go.jp/xyz/std/{zoom}/{x}/{y}.png"

    try:
        resp = requests.head(tile_url, timeout=TIMEOUT)
        result = {
            "source": "国土地理院",
            "context": "調布市（ゲゲゲの鬼太郎の舞台）",
            "ok": resp.ok,
            "status": resp.status_code,
            "lat": lat,
            "lon": lon,
            "zoom": zoom,
            "tile_x": x,
            "tile_y": y,
            "tile_url": tile_url,
            "ts": now_iso(),
        }
        save("07_gsi_chofu_kitaro.json", result)
        print(f"  [{'OK' if resp.ok else 'FAIL'}] Tile URL: {tile_url}")
    except Exception as e:
        result = {"source": "GSI", "ok": False, "error": str(e), "ts": now_iso()}
        save("07_gsi_chofu_kitaro.json", result)
        print(f"  [FAIL] {e}")


# ================================================================
# 8. Cross-reference summary
# ================================================================
def build_cross_reference(results):
    print("\n" + "=" * 60)
    print("[8] Cross-reference v2: 妖怪 横断サマリー")
    print("=" * 60)

    summary = {
        "theme": "妖怪",
        "description": "日本の妖怪文化を古典〜現代まで横断検索した統合結果",
        "sources_tested": [],
        "connections": [],
        "ts": now_iso(),
    }

    # Collect source summaries
    for r in results:
        if r and isinstance(r, dict):
            src = r.get("source", "unknown")
            ok = r.get("ok", False)
            count = r.get("returned", 0) or r.get("total_found", 0) or r.get("property_count", 0)
            summary["sources_tested"].append({
                "source": src,
                "ok": ok,
                "count": count,
            })

    # Identify connections
    summary["connections"] = [
        {
            "from": "AniList",
            "to": "MADB",
            "type": "title_match",
            "note": "AniList title.native とMADB rdfs:label の文字列一致で同一作品を特定",
        },
        {
            "from": "Wikidata",
            "to": "NDL/DBpedia",
            "type": "external_ids",
            "note": "Wikidata P349(NDL ID), P4082(MADB ID)で他DBのレコードを参照",
        },
        {
            "from": "鳥山石燕",
            "to": "浮世絵/妖怪絵",
            "type": "artist_works",
            "note": "鳥山石燕→画図百鬼夜行→NDL古典籍IIIF、DBpedia属性情報で補完",
        },
        {
            "from": "ゲゲゲの鬼太郎",
            "to": "調布市",
            "type": "geographic",
            "note": "AniList作品→GSI地図タイル（調布市）で聖地巡礼ビュー生成可能",
        },
    ]

    ok_count = sum(1 for s in summary["sources_tested"] if s["ok"])
    total_count = len(summary["sources_tested"])
    summary["overall"] = {
        "sources_ok": ok_count,
        "sources_total": total_count,
        "success_rate": f"{ok_count}/{total_count}",
    }

    save("08_cross_reference_yokai.json", summary)
    print(f"  Sources: {ok_count}/{total_count} OK")
    for conn in summary["connections"]:
        print(f"  Connection: {conn['from']} → {conn['to']} ({conn['type']})")

    return summary


# ================================================================
# Main
# ================================================================
def main():
    print(f"妖怪 Integration Test — Phase 2 横断テスト")
    print(f"Started: {now_iso()}")
    print(f"Output: {RESP_DIR}")

    results = []

    r1 = test_anilist_yokai()
    results.append(r1)
    time.sleep(1)

    r2 = test_madb_yokai()
    results.append(r2)
    time.sleep(1)

    r3 = test_jpsearch_yokai()
    # r3 doesn't return a single result (has two saves)
    time.sleep(2)

    r4 = test_wikidata_yokai()
    time.sleep(2)

    r5 = test_ndl_yokai()
    time.sleep(1)

    r6 = test_dbpedia_yokai()
    results.append(r6)
    time.sleep(1)

    r7 = test_gsi_yokai()
    results.append(r7)

    # Build cross-reference
    cross = build_cross_reference(results)

    # Final summary
    print("\n" + "=" * 60)
    print("Integration Test Complete")
    print("=" * 60)
    files = sorted(RESP_DIR.glob("*.json"))
    ok_count = 0
    fail_count = 0
    for f in files:
        try:
            d = json.load(open(f))
            ok = d.get("ok", False)
            label = d.get("source", f.stem)
            if ok:
                ok_count += 1
            else:
                fail_count += 1
            print(f"  [{'OK' if ok else 'FAIL'}] {label}")
        except Exception:
            pass
    print(f"\nTotal: {ok_count} OK, {fail_count} FAIL, {len(files)} files")
    print(f"Completed: {now_iso()}")


if __name__ == "__main__":
    main()
