"""Phase 2.5 Task 6: エッジケース・エラーハンドリング監査
Python 3.8 compatible.
"""
from __future__ import annotations

import json
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


# Test helpers for each API
def test_anilist(keyword, label=""):
    try:
        resp = requests.post(
            "https://graphql.anilist.co",
            json={"query": 'query($s:String!){Page(perPage:3){media(search:$s){id title{native}}}}', "variables": {"search": keyword}},
            timeout=TIMEOUT,
        )
        return {"ok": resp.ok, "status": resp.status_code, "size": len(resp.content)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def test_madb(keyword, label=""):
    query = f"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?item ?label WHERE {{ ?item rdfs:label ?label . FILTER(CONTAINS(STR(?label), "{keyword}")) }} LIMIT 3"""
    try:
        resp = requests.get(
            "https://mediaarts-db.artmuseums.go.jp/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=TIMEOUT,
        )
        return {"ok": resp.ok, "status": resp.status_code, "size": len(resp.content)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def test_jpsearch(keyword, label=""):
    query = f"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?item ?label WHERE {{ ?item rdfs:label ?label . FILTER(CONTAINS(?label, "{keyword}")) }} LIMIT 3"""
    try:
        resp = requests.get(
            "https://jpsearch.go.jp/rdf/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json", "User-Agent": UA},
            timeout=TIMEOUT,
        )
        return {"ok": resp.ok, "status": resp.status_code, "size": len(resp.content)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def test_wikidata(keyword, label=""):
    query = f"""SELECT ?item ?itemLabel WHERE {{
      ?item rdfs:label ?l . FILTER(LANG(?l) = "ja") FILTER(CONTAINS(?l, "{keyword}"))
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en". }}
    }} LIMIT 3"""
    try:
        resp = requests.get(
            "https://query.wikidata.org/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json", "User-Agent": WIKIDATA_UA},
            timeout=TIMEOUT,
        )
        return {"ok": resp.ok, "status": resp.status_code, "size": len(resp.content)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def test_ndl_sru(keyword, label=""):
    try:
        resp = requests.get(
            "https://iss.ndl.go.jp/api/sru",
            params={"operation": "searchRetrieve", "query": f'anywhere="{keyword}"', "maximumRecords": "3", "recordSchema": "dcndl"},
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        return {"ok": resp.ok, "status": resp.status_code, "size": len(resp.content)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def test_ndl_manifest(pid, label=""):
    try:
        resp = requests.get(
            f"https://www.dl.ndl.go.jp/api/iiif/{pid}/manifest.json",
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        return {"ok": resp.ok, "status": resp.status_code, "size": len(resp.content)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ================================================================
# Edge case test suites
# ================================================================

EDGE_CASES = [
    {"label": "空文字列", "keyword": "", "expect": "error or empty"},
    {"label": "存在しないキーワード", "keyword": "xyznonexistent12345", "expect": "empty results"},
    {"label": "旧字体", "keyword": "髙橋", "expect": "may or may not match"},
    {"label": "超長文(200文字)", "keyword": "あ" * 200, "expect": "error or truncated"},
    {"label": "英語のみ", "keyword": "samurai sword", "expect": "varies by source"},
    {"label": "絵文字含む", "keyword": "🎌日本文化", "expect": "may error on SPARQL"},
    {"label": "SQLインジェクション風", "keyword": '"; DROP TABLE--', "expect": "safe, no results"},
    {"label": "SPARQLインジェクション風", "keyword": '} UNION { ?s ?p ?o', "expect": "error or safe"},
    {"label": "HTMLタグ", "keyword": "<script>alert(1)</script>", "expect": "safe, no results"},
    {"label": "改行含む", "keyword": "日本\n文化", "expect": "varies"},
    {"label": "1文字", "keyword": "刀", "expect": "results from most sources"},
]


def main():
    print("=" * 60)
    print("Phase 2.5 Task 6: エッジケース・エラーハンドリング監査")
    print(f"Started: {now_iso()}")
    print("=" * 60)

    sources = {
        "AniList": test_anilist,
        "MADB": test_madb,
        "JapanSearch": test_jpsearch,
        "Wikidata": test_wikidata,
        "NDL_SRU": test_ndl_sru,
    }

    results = []

    for case in EDGE_CASES:
        kw = case["keyword"]
        label = case["label"]
        print(f"\n--- {label} (keyword='{kw[:50]}{'...' if len(kw) > 50 else ''}') ---")

        case_result = {"label": label, "keyword": kw[:100], "expect": case["expect"], "results": {}}

        for src_name, test_fn in sources.items():
            r = test_fn(kw, label)
            case_result["results"][src_name] = r
            status = "OK" if r.get("ok") else f"ERR({r.get('status', r.get('error', '?'))})"
            print(f"  {src_name}: {status}")
            time.sleep(0.5)

        results.append(case_result)
        time.sleep(1)

    # NDL manifest edge cases
    print("\n--- NDL Manifest: 存在しないPID ---")
    ndl_invalid = test_ndl_manifest("9999999999", "invalid PID")
    results.append({
        "label": "NDL存在しないPID",
        "keyword": "9999999999",
        "expect": "404",
        "results": {"NDL_Manifest": ndl_invalid},
    })
    print(f"  NDL_Manifest: {'OK' if ndl_invalid.get('ok') else 'ERR'} (status={ndl_invalid.get('status', '?')})")

    # Save results
    save_json("task6_edge_cases.json", {"results": results, "ts": now_iso()})

    # Generate markdown report
    md = ["# エッジケーステスト結果\n"]
    md.append(f"生成日: {now_iso()}\n")

    md.append("## 結果一覧\n")
    md.append("| テスト | 期待 | AniList | MADB | JPS | Wikidata | NDL |")
    md.append("|--------|------|---------|------|-----|----------|-----|")

    for case in results:
        row = [case["label"], case["expect"]]
        for src in ["AniList", "MADB", "JapanSearch", "Wikidata", "NDL_SRU"]:
            r = case.get("results", {}).get(src, {})
            if r.get("ok"):
                row.append("✅")
            elif "error" in r:
                err = str(r["error"])[:20]
                row.append(f"❌ {err}")
            else:
                s = r.get("status", "?")
                row.append(f"⚠️ {s}")
        md.append("| " + " | ".join(row) + " |")

    # Security assessment
    md.append("\n## セキュリティ評価\n")
    md.append("- SQLインジェクション: SPARQLエンドポイントにはSQL注入は無関係")
    md.append("- SPARQLインジェクション: 文字列リテラル内にクエリ構文が入った場合の挙動を確認")
    md.append("- XSS: MCPサーバーはJSONを返すため直接的なリスクは低い")
    md.append("- タイムアウト: 各ソースのHTTPタイムアウトは30秒に設定")
    md.append("- リトライ: 現在未実装（Phase 3で検討）")

    save_text("edge_case_results.md", "\n".join(md))

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for case in results:
        label = case["label"]
        ok_count = sum(1 for r in case.get("results", {}).values() if r.get("ok"))
        total = len(case.get("results", {}))
        print(f"  {label}: {ok_count}/{total} OK")
    print(f"\nCompleted: {now_iso()}")


if __name__ == "__main__":
    main()
