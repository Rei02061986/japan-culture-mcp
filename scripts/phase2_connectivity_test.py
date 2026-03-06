"""Phase 2 API Connectivity Test
JapanSearch, Wikidata, NDL IIIF/OCR, DBpedia, GSI tiles, OAI-PMH
Python 3.8 compatible.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

BASE_DIR = Path(__file__).parent.parent
RESP_DIR = BASE_DIR / "responses" / "phase2"
RESP_DIR.mkdir(parents=True, exist_ok=True)

UA = "japan-culture-mcp/0.2 (research-project)"
TIMEOUT = 30


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save(name: str, data: Any) -> str:
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, str):
            f.write(data)
        else:
            json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {name}")
    return str(path)


def test_get(label: str, url: str, params: Dict = None, headers: Dict = None,
             fname: str = "response.json") -> Dict:
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    try:
        r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
        ct = r.headers.get("Content-Type", "")
        result = {
            "label": label,
            "url": r.url if hasattr(r, 'url') else url,
            "ok": r.ok,
            "status": r.status_code,
            "content_type": ct,
            "size": len(r.content),
            "ts": now_iso(),
        }
        if "json" in ct:
            try:
                result["data"] = r.json()
            except Exception:
                result["text_preview"] = r.text[:2000]
        elif "xml" in ct or "html" in ct:
            result["text_preview"] = r.text[:3000]
        else:
            result["text_preview"] = r.text[:1000]
        save(fname, result)
        status = "OK" if r.ok else f"HTTP {r.status_code}"
        print(f"  [{status}] {label} ({len(r.content)} bytes)")
        return result
    except Exception as e:
        result = {"label": label, "url": url, "ok": False, "error": str(e), "ts": now_iso()}
        save(fname, result)
        print(f"  [FAIL] {label}: {e}")
        return result


def test_sparql(label: str, endpoint: str, query: str, fname: str) -> Dict:
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": UA,
    }
    try:
        r = requests.get(endpoint, params={"query": query}, headers=headers, timeout=TIMEOUT)
        ct = r.headers.get("Content-Type", "")
        result = {
            "label": label,
            "endpoint": endpoint,
            "ok": r.ok,
            "status": r.status_code,
            "content_type": ct,
            "query": query.strip(),
            "ts": now_iso(),
        }
        if r.ok and "json" in ct:
            data = r.json()
            bindings = data.get("results", {}).get("bindings", [])
            result["binding_count"] = len(bindings)
            result["data"] = data
            print(f"  [OK] {label}: {len(bindings)} bindings")
        else:
            result["text_preview"] = r.text[:2000]
            print(f"  [HTTP {r.status_code}] {label}")
        save(fname, result)
        return result
    except Exception as e:
        result = {"label": label, "endpoint": endpoint, "ok": False, "error": str(e), "ts": now_iso()}
        save(fname, result)
        print(f"  [FAIL] {label}: {e}")
        return result


# ================================================================
# 1. Japan Search (corrected endpoints)
# ================================================================
def test_japan_search():
    print("\n" + "=" * 60)
    print("[1] ジャパンサーチ（新エンドポイント）")
    print("=" * 60)

    # SPARQL
    test_sparql(
        "JapanSearch SPARQL - 浅草",
        "https://jpsearch.go.jp/rdf/sparql",
        """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?s ?label WHERE {
          ?s rdfs:label ?label .
          FILTER(CONTAINS(?label, "浅草"))
        }
        LIMIT 10
        """,
        "jpsearch_sparql_asakusa.json",
    )
    time.sleep(1)

    test_sparql(
        "JapanSearch SPARQL - 妖怪",
        "https://jpsearch.go.jp/rdf/sparql",
        """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>
        SELECT ?s ?label ?type ?provider WHERE {
          ?s rdfs:label ?label .
          FILTER(CONTAINS(?label, "妖怪"))
          OPTIONAL { ?s schema:additionalType ?type }
          OPTIONAL { ?s schema:provider ?provider }
        }
        LIMIT 20
        """,
        "jpsearch_sparql_yokai.json",
    )
    time.sleep(1)

    # Easy SPARQL
    test_get(
        "JapanSearch Easy SPARQL - 浮世絵",
        "https://jpsearch.go.jp/rdf/es",
        params={"keyword": "浮世絵", "format": "json"},
        fname="jpsearch_easy_ukiyoe.json",
    )
    time.sleep(1)

    # Cross Search Web API
    test_get(
        "JapanSearch Cross API",
        "https://jpsearch.go.jp/api/item/search/jps-cross",
        params={"keyword": "妖怪", "size": 5},
        fname="jpsearch_cross_yokai.json",
    )


# ================================================================
# 2. Wikidata SPARQL
# ================================================================
def test_wikidata():
    print("\n" + "=" * 60)
    print("[2] Wikidata SPARQL")
    print("=" * 60)

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "japan-culture-mcp/0.2 (teddykmk@gmail.com)",
    }

    # Q1: Cultural heritage with coordinates
    test_sparql(
        "Wikidata - Cultural Heritage (coords)",
        "https://query.wikidata.org/sparql",
        """
        SELECT ?site ?siteLabel ?coord WHERE {
          ?site wdt:P1435 ?status ;
                wdt:P17 wd:Q17 ;
                wdt:P625 ?coord .
          VALUES ?status { wd:Q9259 wd:Q46921 wd:Q744098 }
          SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
        }
        LIMIT 50
        """,
        "wikidata_cultural_heritage.json",
    )
    time.sleep(2)

    # Q2: Anime + studio + author
    test_sparql(
        "Wikidata - Anime works",
        "https://query.wikidata.org/sparql",
        """
        SELECT ?anime ?animeLabel ?studio ?studioLabel ?author ?authorLabel WHERE {
          ?anime wdt:P31/wdt:P279* wd:Q1107 .
          OPTIONAL { ?anime wdt:P272 ?studio. }
          OPTIONAL { ?anime wdt:P50  ?author. }
          SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
        }
        LIMIT 50
        """,
        "wikidata_anime.json",
    )
    time.sleep(2)

    # Q3: Toriyama Sekien + Hokusai works
    test_sparql(
        "Wikidata - Toriyama Sekien & Hokusai",
        "https://query.wikidata.org/sparql",
        """
        SELECT ?creator ?creatorLabel ?work ?workLabel WHERE {
          VALUES ?creator { wd:Q1151382 wd:Q5569 }
          ?work wdt:P170 ?creator .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
        }
        LIMIT 50
        """,
        "wikidata_ukiyoe_artists.json",
    )
    time.sleep(2)

    # Q4: External IDs (NDL, MADB)
    test_sparql(
        "Wikidata - External IDs (NDL/MADB)",
        "https://query.wikidata.org/sparql",
        """
        SELECT ?item ?itemLabel ?ndl ?madb WHERE {
          ?item wdt:P31/wdt:P279* wd:Q1107 .
          OPTIONAL { ?item wdt:P349  ?ndl. }
          OPTIONAL { ?item wdt:P4082 ?madb. }
          FILTER(BOUND(?ndl) || BOUND(?madb))
          SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
        }
        LIMIT 100
        """,
        "wikidata_external_ids.json",
    )


# ================================================================
# 3. NDL IIIF + OCR + SRU
# ================================================================
def test_ndl():
    print("\n" + "=" * 60)
    print("[3] NDL IIIF + OCR + SRU")
    print("=" * 60)

    # IIIF Manifest
    test_get(
        "NDL IIIF Manifest (北斎漫画)",
        "https://www.dl.ndl.go.jp/api/iiif/1286328/manifest.json",
        fname="ndl_iiif_manifest.json",
    )
    time.sleep(1)

    # Image API info
    test_get(
        "NDL IIIF Image info.json",
        "https://www.dl.ndl.go.jp/api/iiif/1286328/1/info.json",
        fname="ndl_iiif_image_info.json",
    )
    time.sleep(1)

    # OCR fulltext
    test_get(
        "NDL OCR fulltext-json",
        "https://lab.ndl.go.jp/dl/api/book/fulltext-json/897115",
        fname="ndl_ocr_fulltext.json",
    )
    time.sleep(1)

    # SRU search
    test_get(
        "NDL SRU - 浮世絵",
        "https://iss.ndl.go.jp/api/sru",
        params={
            "operation": "searchRetrieve",
            "query": "浮世絵",
            "maximumRecords": "5",
            "recordSchema": "dcndl",
        },
        fname="ndl_sru_ukiyoe.json",
    )


# ================================================================
# 4. DBpedia Japanese
# ================================================================
def test_dbpedia():
    print("\n" + "=" * 60)
    print("[4] DBpedia Japanese")
    print("=" * 60)

    test_sparql(
        "DBpedia - 葛飾北斎",
        "https://ja.dbpedia.org/sparql",
        """
        SELECT ?prop ?value WHERE {
          <http://ja.dbpedia.org/resource/葛飾北斎> ?prop ?value .
        }
        LIMIT 50
        """,
        "dbpedia_hokusai.json",
    )
    time.sleep(1)

    test_sparql(
        "DBpedia - 鳥山石燕",
        "https://ja.dbpedia.org/sparql",
        """
        SELECT ?prop ?value WHERE {
          <http://ja.dbpedia.org/resource/鳥山石燕> ?prop ?value .
        }
        LIMIT 50
        """,
        "dbpedia_toriyama_sekien.json",
    )


# ================================================================
# 5. GSI Tiles
# ================================================================
def test_gsi_tiles():
    print("\n" + "=" * 60)
    print("[5] 地理院タイル")
    print("=" * 60)

    # Standard map (Asakusa area: z=14, x=14552, y=6451)
    test_get(
        "GSI Standard Map Tile",
        "https://cyberjapandata.gsi.go.jp/xyz/std/14/14552/6451.png",
        fname="gsi_standard_tile.json",
    )
    time.sleep(1)

    # Historical rapid survey map
    test_get(
        "GSI Historical Rapid Survey",
        "https://cyberjapandata.gsi.go.jp/xyz/rapid/18/232836/103222.png",
        fname="gsi_historical_tile.json",
    )
    time.sleep(1)

    # Relief map
    test_get(
        "GSI Relief Map",
        "https://cyberjapandata.gsi.go.jp/xyz/relief/10/909/403.png",
        fname="gsi_relief_tile.json",
    )


# ================================================================
# 6. OAI-PMH SiteReports
# ================================================================
def test_oai_pmh():
    print("\n" + "=" * 60)
    print("[6] OAI-PMH SiteReports")
    print("=" * 60)

    test_get(
        "SiteReports OAI-PMH Identify",
        "http://sitereports.nabunken.go.jp/api/oai/request",
        params={"verb": "Identify"},
        fname="sitereports_oai_identify.json",
    )
    time.sleep(1)

    test_get(
        "SiteReports OAI-PMH ListSets",
        "http://sitereports.nabunken.go.jp/api/oai/request",
        params={"verb": "ListSets"},
        fname="sitereports_oai_listsets.json",
    )


# ================================================================
# Main
# ================================================================
def main():
    print(f"Phase 2 API Connectivity Test")
    print(f"Started: {now_iso()}")
    print(f"Output: {RESP_DIR}")

    test_japan_search()
    time.sleep(2)

    test_wikidata()
    time.sleep(2)

    test_ndl()
    time.sleep(2)

    test_dbpedia()
    time.sleep(2)

    test_gsi_tiles()
    time.sleep(1)

    test_oai_pmh()

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    files = sorted(RESP_DIR.glob("*.json"))
    ok_count = 0
    fail_count = 0
    for f in files:
        try:
            d = json.load(open(f))
            ok = d.get("ok", False)
            label = d.get("label", f.stem)
            status_icon = "OK" if ok else "FAIL"
            if ok:
                ok_count += 1
            else:
                fail_count += 1
            bc = d.get("binding_count", "")
            extra = f" ({bc} bindings)" if bc != "" else ""
            print(f"  [{status_icon}] {label}{extra}")
        except Exception:
            pass
    print(f"\nTotal: {ok_count} OK, {fail_count} FAIL, {len(files)} files")
    print(f"Completed: {now_iso()}")


if __name__ == "__main__":
    main()
