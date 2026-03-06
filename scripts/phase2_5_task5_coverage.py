"""Phase 2.5 Task 5: 5テーマ×全ソースカバレッジテスト + Wikidata ID取得率
Python 3.8 compatible.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

BASE_DIR = Path(__file__).parent.parent
RESP_DIR = BASE_DIR / "responses" / "phase2_5"
RESP_DIR.mkdir(parents=True, exist_ok=True)

UA = "japan-culture-mcp/0.2 (research-project)"
WIKIDATA_UA = "japan-culture-mcp/0.2 (teddykmk@gmail.com)"
TIMEOUT = 30


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save_json(name, data):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {name}")


def save_text(name, text):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  Saved: {name}")


THEMES = [
    {"keyword": "妖怪", "en": "yokai"},
    {"keyword": "浮世絵", "en": "ukiyo-e"},
    {"keyword": "茶道", "en": "tea ceremony"},
    {"keyword": "祭り", "en": "matsuri"},
    {"keyword": "忍者", "en": "ninja"},
]


def query_anilist(keyword):
    query = """
    query ($search: String!, $perPage: Int) {
      Page(page: 1, perPage: $perPage) {
        pageInfo { total }
        media(search: $search, type: ANIME, sort: POPULARITY_DESC) {
          id title { romaji native } genres seasonYear averageScore
        }
      }
    }
    """
    try:
        resp = requests.post(
            "https://graphql.anilist.co",
            json={"query": query, "variables": {"search": keyword, "perPage": 10}},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        page = data.get("data", {}).get("Page", {})
        total = page.get("pageInfo", {}).get("total", 0)
        media = page.get("media", [])
        return {"ok": True, "total": total, "returned": len(media), "top5": [
            {"title": m.get("title", {}).get("native", m.get("title", {}).get("romaji", "")), "year": m.get("seasonYear")}
            for m in media[:5]
        ]}
    except Exception as e:
        return {"ok": False, "error": str(e), "total": 0, "returned": 0}


def query_madb(keyword):
    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?item ?label ?type WHERE {{
      ?item rdfs:label ?label .
      ?item a ?type .
      FILTER(CONTAINS(STR(?label), "{keyword}"))
    }}
    LIMIT 20
    """
    try:
        resp = requests.get(
            "https://mediaarts-db.artmuseums.go.jp/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        bindings = resp.json().get("results", {}).get("bindings", [])
        items = []
        for b in bindings:
            items.append({
                "label": b.get("label", {}).get("value", ""),
                "type": b.get("type", {}).get("value", "").rsplit("#", 1)[-1],
            })
        return {"ok": True, "total": len(items), "returned": len(items), "top5": items[:5]}
    except Exception as e:
        return {"ok": False, "error": str(e), "total": 0, "returned": 0}


def query_jpsearch(keyword):
    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?item ?label WHERE {{
      ?item rdfs:label ?label .
      FILTER(CONTAINS(?label, "{keyword}"))
    }}
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
        bindings = resp.json().get("results", {}).get("bindings", [])
        items = [{"label": b.get("label", {}).get("value", "")} for b in bindings]
        return {"ok": True, "total": len(items), "returned": len(items), "top5": items[:5]}
    except Exception as e:
        return {"ok": False, "error": str(e), "total": 0, "returned": 0}


def query_wikidata(keyword):
    query = f"""
    SELECT ?item ?itemLabel WHERE {{
      ?item rdfs:label ?l .
      FILTER(LANG(?l) = "ja")
      FILTER(CONTAINS(?l, "{keyword}"))
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
    }}
    LIMIT 20
    """
    try:
        resp = requests.get(
            "https://query.wikidata.org/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json", "User-Agent": WIKIDATA_UA},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        bindings = resp.json().get("results", {}).get("bindings", [])
        items = [{"label": b.get("itemLabel", {}).get("value", ""), "id": b.get("item", {}).get("value", "")} for b in bindings]
        return {"ok": True, "total": len(items), "returned": len(items), "top5": items[:5]}
    except Exception as e:
        return {"ok": False, "error": str(e), "total": 0, "returned": 0}


def query_ndl(keyword):
    try:
        resp = requests.get(
            "https://iss.ndl.go.jp/api/sru",
            params={
                "operation": "searchRetrieve",
                "query": f'anywhere="{keyword}"',
                "maximumRecords": "5",
                "recordSchema": "dcndl",
            },
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        text = resp.text
        total_m = re.search(r"<numberOfRecords>(\d+)</numberOfRecords>", text)
        total = int(total_m.group(1)) if total_m else 0
        titles = re.findall(r"<dc:title[^>]*>([^<]+)</dc:title>", text)
        return {"ok": True, "total": total, "returned": len(titles), "top5": [{"title": t} for t in titles[:5]]}
    except Exception as e:
        return {"ok": False, "error": str(e), "total": 0, "returned": 0}


def query_dbpedia(keyword):
    query = f"""
    SELECT ?item ?label ?abstract WHERE {{
      ?item rdfs:label ?label .
      FILTER(CONTAINS(?label, "{keyword}"))
      FILTER(LANG(?label) = "ja")
      OPTIONAL {{ ?item <http://dbpedia.org/ontology/abstract> ?abstract . FILTER(LANG(?abstract) = "ja") }}
    }}
    LIMIT 10
    """
    try:
        resp = requests.get(
            "https://ja.dbpedia.org/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json", "User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        bindings = resp.json().get("results", {}).get("bindings", [])
        items = [{"label": b.get("label", {}).get("value", "")} for b in bindings]
        return {"ok": True, "total": len(items), "returned": len(items), "top5": items[:5]}
    except Exception as e:
        return {"ok": False, "error": str(e), "total": 0, "returned": 0}


# ================================================================
# Task 5C: Wikidata外部ID取得率
# ================================================================

def test_wikidata_external_ids(keyword):
    """Wikidataで検索し、外部ID（NDL, MADB, VIAF）の取得率を調べる"""
    query = f"""
    SELECT ?item ?itemLabel ?ndl ?madb ?viaf WHERE {{
      ?item rdfs:label ?l .
      FILTER(LANG(?l) = "ja")
      FILTER(CONTAINS(?l, "{keyword}"))
      OPTIONAL {{ ?item wdt:P349  ?ndl }}
      OPTIONAL {{ ?item wdt:P4082 ?madb }}
      OPTIONAL {{ ?item wdt:P214  ?viaf }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
    }}
    LIMIT 10
    """
    try:
        resp = requests.get(
            "https://query.wikidata.org/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json", "User-Agent": WIKIDATA_UA},
            timeout=60,
        )
        resp.raise_for_status()
        bindings = resp.json().get("results", {}).get("bindings", [])
        total = len(bindings)
        has_ndl = sum(1 for b in bindings if "ndl" in b)
        has_madb = sum(1 for b in bindings if "madb" in b)
        has_viaf = sum(1 for b in bindings if "viaf" in b)
        items = []
        for b in bindings:
            items.append({
                "label": b.get("itemLabel", {}).get("value", ""),
                "ndl": b.get("ndl", {}).get("value", "") if "ndl" in b else None,
                "madb": b.get("madb", {}).get("value", "") if "madb" in b else None,
                "viaf": b.get("viaf", {}).get("value", "") if "viaf" in b else None,
            })
        return {
            "ok": True,
            "total": total,
            "has_ndl": has_ndl,
            "has_madb": has_madb,
            "has_viaf": has_viaf,
            "items": items,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "total": 0}


# ================================================================
# Main
# ================================================================

def main():
    print("=" * 60)
    print("Phase 2.5 Task 5: 5テーマカバレッジテスト")
    print(f"Started: {now_iso()}")
    print("=" * 60)

    sources = {
        "AniList": query_anilist,
        "MADB": query_madb,
        "JapanSearch": query_jpsearch,
        "Wikidata": query_wikidata,
        "NDL": query_ndl,
        "DBpedia": query_dbpedia,
    }

    coverage = {}
    wikidata_ids = {}

    for theme in THEMES:
        kw = theme["keyword"]
        print(f"\n{'='*60}")
        print(f"Theme: {kw} ({theme['en']})")
        print(f"{'='*60}")

        theme_results = {}
        for source_name, query_fn in sources.items():
            print(f"\n  [{source_name}] {kw}...")
            result = query_fn(kw)
            theme_results[source_name] = result
            status = "OK" if result["ok"] else "FAIL"
            print(f"  [{status}] {source_name}: {result.get('total', 0)} total, {result.get('returned', 0)} returned")
            time.sleep(1)

        coverage[kw] = theme_results

        # Wikidata external IDs
        print(f"\n  [Wikidata IDs] {kw}...")
        id_result = test_wikidata_external_ids(kw)
        wikidata_ids[kw] = id_result
        if id_result["ok"]:
            print(f"  Total: {id_result['total']}, NDL: {id_result['has_ndl']}, MADB: {id_result['has_madb']}, VIAF: {id_result['has_viaf']}")
        else:
            print(f"  [FAIL] {id_result.get('error', 'unknown')}")
        time.sleep(2)

    # Save results
    save_json("task5_coverage_full.json", {"coverage": coverage, "wikidata_ids": wikidata_ids, "ts": now_iso()})

    # Generate markdown tables
    md = ["# カバレッジマトリクス\n"]
    md.append(f"生成日: {now_iso()}\n")

    # Coverage matrix
    md.append("## 5テーマ × 全ソース\n")
    header = "| テーマ | " + " | ".join(sources.keys()) + " | 合計 |"
    sep = "|" + "|".join(["--------"] * (len(sources) + 2)) + "|"
    md.append(header)
    md.append(sep)

    for theme in THEMES:
        kw = theme["keyword"]
        row = [kw]
        total = 0
        for src in sources.keys():
            r = coverage.get(kw, {}).get(src, {})
            cnt = r.get("total", 0)
            ok = "✅" if r.get("ok") else "❌"
            row.append(f"{ok} {cnt:,}")
            if r.get("ok"):
                total += cnt
        row.append(str(total))
        md.append("| " + " | ".join(row) + " |")

    # Wikidata ID coverage
    md.append("\n## Wikidata外部IDカバレッジ\n")
    md.append("| テーマ | 試行数 | NDL ID | MADB ID | VIAF ID |")
    md.append("|--------|--------|--------|---------|---------|")
    for theme in THEMES:
        kw = theme["keyword"]
        r = wikidata_ids.get(kw, {})
        if r.get("ok"):
            total = r["total"]
            md.append(f"| {kw} | {total} | {r['has_ndl']}/{total} | {r['has_madb']}/{total} | {r['has_viaf']}/{total} |")
        else:
            md.append(f"| {kw} | ❌ | - | - | - |")

    # Top results per theme
    md.append("\n## テーマ別Top5サンプル\n")
    for theme in THEMES:
        kw = theme["keyword"]
        md.append(f"\n### {kw}\n")
        for src in sources.keys():
            r = coverage.get(kw, {}).get(src, {})
            top = r.get("top5", [])
            if top:
                md.append(f"**{src}** ({r.get('total', 0)}件):")
                for item in top:
                    label = item.get("title", item.get("label", item.get("name", "?")))
                    md.append(f"  - {label}")
                md.append("")

    save_text("coverage_matrix.md", "\n".join(md))

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for theme in THEMES:
        kw = theme["keyword"]
        counts = []
        for src in sources.keys():
            r = coverage.get(kw, {}).get(src, {})
            ok = "✅" if r.get("ok") else "❌"
            counts.append(f"{src}:{ok}{r.get('total', 0)}")
        print(f"  {kw}: {' | '.join(counts)}")
    print(f"\nCompleted: {now_iso()}")


if __name__ == "__main__":
    main()
