#!/usr/bin/env python3
"""
Phase 17 C1: Codex User Test — 5 personas x 5 questions = 25 tests.

Calls MCP tool functions directly (Python import, no HTTP server needed).
Evaluates response quality using heuristic scoring.
"""
import asyncio
import csv
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = str(PROJECT_ROOT / "ontology" / "culture_ontology.db")
OUTPUT_CSV = PROJECT_ROOT / "reports" / "phase18_codex_user_test.csv"
OUTPUT_CSV.parent.mkdir(exist_ok=True)

# Set DB_PATH env for server import
os.environ["DB_PATH"] = DB_PATH

# Mock mcp module if not available (Python 3.9)
try:
    import mcp  # noqa: F401
except ImportError:
    from types import ModuleType

    class _PassthroughFastMCP:
        def __init__(self, *a, **kw):
            pass
        def tool(self, *a, **kw):
            def _decorator(fn):
                return fn
            return _decorator

    _mock_fastmcp = ModuleType("mcp.server.fastmcp")
    _mock_fastmcp.FastMCP = _PassthroughFastMCP
    _mock_server = ModuleType("mcp.server")
    _mock_server.fastmcp = _mock_fastmcp
    _mock_mcp = ModuleType("mcp")
    _mock_mcp.server = _mock_server
    sys.modules["mcp"] = _mock_mcp
    sys.modules["mcp.server"] = _mock_server
    sys.modules["mcp.server.fastmcp"] = _mock_fastmcp

# ===== Personas and Questions =====
PERSONAS = [
    {
        "id": "P1", "name": "観光プランナー",
        "context": "京都への訪日外国人向けツアーを企画している旅行代理店担当者",
        "questions": [
            {"q": "京都で伝統工芸とアニメ聖地が近いエリアを教えて", "tool": "find_tourism_assets", "args": {"region": "kinki", "asset_types": "shrine,temple,pilgrimage"}},
            {"q": "瀬戸内で文化資源密度が最も高い地域はどこ？", "tool": "analyze_cultural_density", "args": {"lat_min": 33.8, "lat_max": 34.8, "lon_min": 132.0, "lon_max": 134.5}},
            {"q": "スラムダンクの聖地・鎌倉高校前駅周辺の伝統文化施設は？", "tool": "get_nearby_culture", "args": {"lat": 35.3058, "lon": 139.4968, "radius_km": 5}},
            {"q": "新潟ゆかりの文化人・芸術家を一覧して", "tool": "get_prefecture_profile", "args": {"prefecture": "niigata"}},
            {"q": "外国人に刺さる知られていない聖地を推薦して", "tool": "search_pilgrimage", "args": {"limit": 10}},
        ]
    },
    {
        "id": "P2", "name": "CCDM研究者",
        "context": "文化資本動態モデルを研究する経済学者",
        "questions": [
            {"q": "1995年前後のアニメ聖地巡礼スポットの変化を教えて", "tool": "pilgrimage_timeline", "args": {"year_from": 1990, "year_to": 2000}},
            {"q": "都道府県別の文化資源カテゴリ分布を確認したい", "tool": "get_prefecture_profile", "args": {"prefecture": "kyoto"}},
            {"q": "ポップカルチャーと伝統文化財が共存するケースを列挙して", "tool": "export_dataset", "args": {"dataset_type": "pop_trad", "limit": 30}},
            {"q": "聖地スポットが多い都道府県を教えて", "tool": "bulk_region_profiles", "args": {"include_pilgrimage": True}},
            {"q": "release_year=2010前後のアニメ作品と聖地統計を出して", "tool": "filter_by_release_year", "args": {"year_from": 2008, "year_to": 2012, "entity_type": "anime", "limit": 30}},
        ]
    },
    {
        "id": "P3", "name": "アニメファン（外国人）",
        "context": "日本在住のアニメファン。好きな作品の聖地を効率よく巡りたい",
        "questions": [
            {"q": "鬼滅の刃の舞台になった場所を教えて", "tool": "deep_dive", "args": {"entity": "鬼滅の刃", "max_recommendations": 10}},
            {"q": "ガールズ&パンツァーの聖地・大洗町周辺の観光スポットは？", "tool": "get_nearby_culture", "args": {"lat": 36.3133, "lon": 140.5764, "radius_km": 10}},
            {"q": "東京近郊で複数の聖地を回れるルートを作って", "tool": "generate_culture_map", "args": {"theme": "アニメ", "region": "kanto"}},
            {"q": "エヴァンゲリオンゆかりの箱根エリアの文化施設を教えて", "tool": "get_nearby_culture", "args": {"lat": 35.2326, "lon": 139.1070, "radius_km": 10}},
            {"q": "進撃の巨人ゆかりの大分県スポットは？", "tool": "search_culture", "args": {"keyword": "進撃の巨人"}},
        ]
    },
    {
        "id": "P4", "name": "地方自治体職員",
        "context": "地方創生担当。自分の地域の文化資源を活用したコンテンツツーリズム施策を検討中",
        "questions": [
            {"q": "高知県の文化資源を種類別に整理して", "tool": "get_prefecture_profile", "args": {"prefecture": "kochi"}},
            {"q": "鳥取県でアニメ・映画の撮影地になった場所は？", "tool": "get_prefecture_profile", "args": {"prefecture": "tottori"}},
            {"q": "島根県の伝統工芸と関連するポップカルチャー作品は？", "tool": "export_dataset", "args": {"dataset_type": "pop_trad", "prefecture": "島根", "limit": 30}},
            {"q": "四国4県の文化資源密度を比較して", "tool": "analyze_cultural_density", "args": {"lat_min": 32.8, "lat_max": 34.4, "lon_min": 132.0, "lon_max": 134.8}},
            {"q": "徳島県のコンテンツツーリズムのポテンシャルを評価して", "tool": "find_tourism_assets", "args": {"region": "shikoku"}},
        ]
    },
    {
        "id": "P5", "name": "データサイエンティスト",
        "context": "観光データ分析プロジェクト担当。プログラムでAPIを活用したい",
        "questions": [
            {"q": "analyze_cultural_densityの使い方と出力形式を教えて", "tool": "analyze_cultural_density", "args": {"lat_min": 34.9, "lat_max": 35.1, "lon_min": 135.7, "lon_max": 135.9}},
            {"q": "1990年以降のアニメ作品と関連聖地を一覧したい", "tool": "filter_by_release_year", "args": {"year_from": 1990, "entity_type": "anime", "limit": 30}},
            {"q": "get_prefecture_profileで返ってくるJSONの構造を教えて", "tool": "get_prefecture_profile", "args": {"prefecture": "tokyo"}},
            {"q": "聖地巡礼の時系列変化を見たい", "tool": "pilgrimage_timeline", "args": {"year_from": 2000, "year_to": 2024}},
            {"q": "release_yearが付いたアニメ作品をフィルタして取得したい", "tool": "filter_by_release_year", "args": {"year_from": 2019, "year_to": 2019, "entity_type": "anime"}},
        ]
    },
]


def evaluate_response(response_text, question):
    """Heuristic scoring of MCP response quality."""
    scores = {
        "relevance": 1,
        "completeness": 1,
        "usability": 1,
        "accuracy_confidence": 1,
        "satisfaction": 1,
        "missing": "",
        "improvement": "",
        "best_aspect": "",
    }

    if not response_text or response_text.startswith("ERROR"):
        scores["missing"] = "ツール実行エラー"
        scores["improvement"] = "エラーハンドリングの改善"
        return scores

    resp_len = len(response_text)

    # Parse as JSON if possible
    try:
        data = json.loads(response_text)
        is_json = True
    except (json.JSONDecodeError, TypeError):
        data = None
        is_json = False

    # --- Relevance: CJK n-gram overlap for Japanese text ---
    import re as _re

    # Build searchable text: flatten all JSON string values
    search_text = response_text
    if is_json:
        def _extract_strings(obj):
            strs = []
            if isinstance(obj, str):
                strs.append(obj)
            elif isinstance(obj, dict):
                for v in obj.values():
                    strs.extend(_extract_strings(v))
            elif isinstance(obj, list):
                for v in obj:
                    strs.extend(_extract_strings(v))
            return strs
        search_text = " ".join(_extract_strings(data))

    # Split question on common particles, then extract 2+ char CJK segments
    q_cleaned = _re.sub(r'[のをはがでとにもへやかなるたてれいうえお？！。、]', ' ', question)
    q_segments = [s for s in _re.findall(r'[\u3040-\u9fff\uff00-\uffef]{2,}', q_cleaned)]
    if not q_segments:
        q_segments = question.replace("？", "").split()
    matched_seg = sum(1 for seg in q_segments if seg in search_text)
    seg_ratio = matched_seg / max(len(q_segments), 1)
    scores["relevance"] = min(5, max(1, int(1 + seg_ratio * 4)))

    # --- Completeness: response length and data richness ---
    if resp_len > 5000:
        scores["completeness"] = 5
    elif resp_len > 2000:
        scores["completeness"] = 4
    elif resp_len > 500:
        scores["completeness"] = 3
    elif resp_len > 100:
        scores["completeness"] = 2
    else:
        scores["completeness"] = 1

    # Bonus for structured data
    if is_json:
        if isinstance(data, dict):
            keys = len(data.keys())
            if keys > 5:
                scores["completeness"] = min(5, scores["completeness"] + 1)
        elif isinstance(data, list):
            if len(data) > 5:
                scores["completeness"] = min(5, scores["completeness"] + 1)

    # --- Usability: structured output, clear formatting ---
    scores["usability"] = 3  # baseline
    if is_json:
        scores["usability"] = 4
        if isinstance(data, dict) and any(k in data for k in ["entities", "results", "hotspots", "items", "connections"]):
            scores["usability"] = 5
    elif "entities" in response_text.lower() or "results" in response_text.lower():
        scores["usability"] = 4

    # --- Accuracy confidence: based on data presence ---
    if is_json and isinstance(data, dict):
        has_counts = any(k in data for k in ["total_entities", "entity_count", "total", "count"])
        has_items = any(k in data for k in ["entities", "items", "results", "connections"])
        if has_counts and has_items:
            scores["accuracy_confidence"] = 5
        elif has_counts or has_items:
            scores["accuracy_confidence"] = 4
        else:
            scores["accuracy_confidence"] = 3
    elif resp_len > 200:
        scores["accuracy_confidence"] = 3
    else:
        scores["accuracy_confidence"] = 2

    # --- Satisfaction: weighted average ---
    sat = (scores["relevance"] * 0.3 + scores["completeness"] * 0.25 +
           scores["usability"] * 0.25 + scores["accuracy_confidence"] * 0.2)
    scores["satisfaction"] = round(sat, 1)

    # --- Qualitative feedback ---
    if scores["satisfaction"] >= 4:
        scores["best_aspect"] = "構造化されたデータが返された"
        scores["missing"] = "特になし"
        scores["improvement"] = "特になし"
    elif scores["satisfaction"] >= 3:
        scores["best_aspect"] = "基本的なデータが返された"
        scores["missing"] = "より詳細なフィルタリング結果"
        scores["improvement"] = "質問意図に沿ったプレフィルタリング"
    else:
        scores["best_aspect"] = "レスポンスは返った"
        scores["missing"] = "質問に関連するデータが不足"
        scores["improvement"] = "ツールの対応範囲拡大が必要"

    return scores


async def call_tool(srv_module, tool_name, args):
    """Call an MCP tool function directly."""
    func = getattr(srv_module, tool_name, None)
    if func is None:
        return f"ERROR: tool '{tool_name}' not found in server module"

    try:
        result = await func(**args)
        if isinstance(result, str):
            return result
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        return f"ERROR: {type(e).__name__}: {e}"


async def main():
    t0 = time.time()
    print("=" * 70, flush=True)
    print("Phase 17 C1: Codex User Test (25 questions)", flush=True)
    print(f"DB: {DB_PATH}", flush=True)
    print("=" * 70, flush=True)

    # Verify DB exists
    if not os.path.exists(DB_PATH):
        print(f"ERROR: DB not found at {DB_PATH}", flush=True)
        return

    # Import server module
    print("\nImporting server module...", flush=True)
    from server import japan_culture_mcp as srv
    print("  Done.", flush=True)

    all_results = []
    total_q = 0

    for persona in PERSONAS:
        print(f"\n=== {persona['id']}: {persona['name']} ===", flush=True)

        for qi, qdata in enumerate(persona["questions"], 1):
            total_q += 1
            question = qdata["q"]
            tool_name = qdata["tool"]
            tool_args = qdata["args"]

            print(f"  Q{qi}: {question[:50]}...", end=" ", flush=True)

            # Call MCP tool
            mcp_resp = await call_tool(srv, tool_name, tool_args)
            resp_len = len(mcp_resp)

            # Evaluate
            scores = evaluate_response(mcp_resp, question)

            row = {
                "persona_id": persona["id"],
                "persona_name": persona["name"],
                "question_no": qi,
                "question": question,
                "tool_used": tool_name,
                "mcp_response_length": resp_len,
                "mcp_response_preview": mcp_resp[:200].replace("\n", " "),
                **scores,
            }
            all_results.append(row)

            sat = scores.get("satisfaction", 0)
            print(f"[{tool_name}] len={resp_len:,} sat={sat}/5", flush=True)

    # Save CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
        writer.writeheader()
        writer.writerows(all_results)

    # Summary
    valid = [r for r in all_results if isinstance(r.get("satisfaction"), (int, float)) and r["satisfaction"] > 0]
    print(f"\n{'='*70}", flush=True)
    print(f"完了: {len(valid)}/25問", flush=True)
    if valid:
        avg_sat = sum(r["satisfaction"] for r in valid) / len(valid)
        avg_usa = sum(r["usability"] for r in valid) / len(valid)
        avg_rel = sum(r["relevance"] for r in valid) / len(valid)
        avg_comp = sum(r["completeness"] for r in valid) / len(valid)
        low = [r for r in valid if r["satisfaction"] < 3]
        print(f"平均満足度:         {avg_sat:.2f}/5", flush=True)
        print(f"平均ユーザビリティ: {avg_usa:.2f}/5", flush=True)
        print(f"平均関連性:         {avg_rel:.2f}/5", flush=True)
        print(f"平均網羅性:         {avg_comp:.2f}/5", flush=True)
        print(f"低スコア(<3):       {len(low)}問", flush=True)
        if low:
            for r in low:
                print(f"  [{r['persona_id']}] Q{r['question_no']}: {r['question'][:40]} -> sat={r['satisfaction']}", flush=True)

    elapsed = time.time() - t0
    print(f"\nDuration: {elapsed:.1f}s", flush=True)
    print(f"Results: {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
