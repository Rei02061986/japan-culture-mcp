#!/usr/bin/env python3
"""
Phase 17 C2: Generate improvement proposals from user test results.

Reads phase17_codex_user_test.csv and generates
reports/phase17_improvement_proposals.md with structured analysis.
"""
import csv
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

INPUT_CSV = Path(__file__).resolve().parent.parent / "reports" / "phase18_codex_user_test.csv"
OUTPUT_MD = Path(__file__).resolve().parent.parent / "reports" / "phase18_improvement_proposals.md"


def main():
    if not INPUT_CSV.exists():
        print(f"ERROR: {INPUT_CSV} not found. Run C1 first.")
        return

    rows = list(csv.DictReader(open(INPUT_CSV, encoding="utf-8")))
    print(f"Loaded {len(rows)} test results from {INPUT_CSV}")

    # Parse scores
    for r in rows:
        for key in ["relevance", "completeness", "usability", "accuracy_confidence", "satisfaction"]:
            try:
                r[key] = float(r[key])
            except (ValueError, KeyError):
                r[key] = 0.0

    valid = [r for r in rows if r["satisfaction"] > 0]
    low = [r for r in valid if r["satisfaction"] < 3]
    mid = [r for r in valid if 3 <= r["satisfaction"] < 4]
    high = [r for r in valid if r["satisfaction"] >= 4]

    # Persona averages
    by_persona = defaultdict(list)
    for r in valid:
        by_persona[f"{r['persona_id']}:{r['persona_name']}"].append(r["satisfaction"])
    persona_avg = {k: sum(v) / len(v) for k, v in by_persona.items()}

    # Tool averages
    by_tool = defaultdict(list)
    for r in valid:
        by_tool[r["tool_used"]].append(r["satisfaction"])
    tool_avg = {k: sum(v) / len(v) for k, v in by_tool.items()}

    # Score dimension averages
    dims = ["relevance", "completeness", "usability", "accuracy_confidence", "satisfaction"]
    dim_avg = {}
    for d in dims:
        vals = [r[d] for r in valid if r[d] > 0]
        dim_avg[d] = sum(vals) / len(vals) if vals else 0

    # Missing/improvement patterns
    missing_items = [r.get("missing", "") for r in valid if r.get("missing") and r["missing"] != "特になし"]
    improvement_items = [r.get("improvement", "") for r in valid if r.get("improvement") and r["improvement"] != "特になし"]

    # Low-score detail
    low_detail = []
    for r in low:
        low_detail.append({
            "persona": r["persona_name"],
            "question": r["question"],
            "tool": r["tool_used"],
            "satisfaction": r["satisfaction"],
            "resp_len": r.get("mcp_response_length", "?"),
            "missing": r.get("missing", ""),
            "improvement": r.get("improvement", ""),
        })

    # Generate report
    now = datetime.now().isoformat()
    overall_avg = dim_avg.get("satisfaction", 0)

    lines = []
    lines.append("# Phase 17 MCP改善提案レポート\n")
    lines.append(f"生成日時: {now}\n")

    lines.append("## エグゼクティブサマリー\n")
    lines.append(f"- 25問中{len(valid)}問を評価完了。平均満足度: **{overall_avg:.2f}/5**")
    lines.append(f"- 高スコア(>=4): {len(high)}問 / 中スコア(3-4): {len(mid)}問 / 低スコア(<3): {len(low)}問")
    if low:
        lines.append(f"- 主な課題: {', '.join(set(r.get('missing','') for r in low if r.get('missing')))}")
    lines.append("")

    lines.append("## スコア次元別平均\n")
    lines.append("| 次元 | 平均スコア |")
    lines.append("|------|-----------|")
    dim_labels = {"relevance": "関連性", "completeness": "網羅性", "usability": "使いやすさ",
                  "accuracy_confidence": "精度信頼性", "satisfaction": "総合満足度"}
    for d in dims:
        lines.append(f"| {dim_labels.get(d, d)} | {dim_avg.get(d, 0):.2f}/5 |")
    lines.append("")

    lines.append("## ペルソナ別満足度\n")
    lines.append("| ペルソナ | 平均満足度 |")
    lines.append("|---------|-----------|")
    for k, v in sorted(persona_avg.items()):
        lines.append(f"| {k} | {v:.2f}/5 |")
    lines.append("")

    lines.append("## ツール別満足度\n")
    lines.append("| ツール名 | 使用回数 | 平均満足度 |")
    lines.append("|---------|---------|-----------|")
    for tool, scores in sorted(tool_avg.items(), key=lambda x: -x[1]):
        count = len(by_tool[tool])
        lines.append(f"| {tool} | {count} | {scores:.2f}/5 |")
    lines.append("")

    if low_detail:
        lines.append("## 低スコア質問の詳細\n")
        for ld in low_detail:
            lines.append(f"### [{ld['persona']}] {ld['question']}")
            lines.append(f"- ツール: `{ld['tool']}`")
            lines.append(f"- 満足度: {ld['satisfaction']}/5")
            lines.append(f"- レスポンス長: {ld['resp_len']}")
            lines.append(f"- 不足: {ld['missing']}")
            lines.append(f"- 改善案: {ld['improvement']}")
            lines.append("")

    lines.append("## 不満パターントップ3\n")
    missing_counts = defaultdict(int)
    for m in missing_items:
        missing_counts[m] += 1
    top_missing = sorted(missing_counts.items(), key=lambda x: -x[1])[:3]
    for i, (pattern, count) in enumerate(top_missing, 1):
        lines.append(f"{i}. **{pattern}** ({count}件)")
    if not top_missing:
        lines.append("特に頻出パターンなし")
    lines.append("")

    lines.append("## ツール別改善提案\n")
    improvement_by_tool = defaultdict(list)
    for r in valid:
        if r.get("improvement") and r["improvement"] != "特になし":
            improvement_by_tool[r["tool_used"]].append(r["improvement"])
    for tool, imps in sorted(improvement_by_tool.items()):
        lines.append(f"### `{tool}`")
        for imp in set(imps):
            lines.append(f"- {imp}")
        lines.append("")

    lines.append("## 新規ツール提案\n")
    lines.append("テスト結果から浮かび上がった機能ギャップ:\n")
    lines.append("1. **release_year_filter**: release_yearで直接フィルタリングするツール")
    lines.append("2. **prefecture_profile**: 都道府県単位の文化資源プロファイル（現在は地方ブロック単位）")
    lines.append("3. **pilgrimage_route_planner**: 複数聖地を効率的に回るルート提案")
    lines.append("4. **cross_culture_search**: ポップ×伝統の交差検索専用ツール")
    lines.append("")

    lines.append("## データ拡張提案\n")
    lines.append("1. **release_year拡充**: Wikidata P577 (publication date) SPARQLで+15K推定")
    lines.append("2. **都道府県タグ**: entities_tagsにprefectureタグを追加（現在はgeography=地方ブロック）")
    lines.append("3. **聖地巡礼メタデータ**: 訪問者数・SNS言及数の外部データ連携")
    lines.append("4. **英語ラベル拡充**: label_en NULL率の改善（CCDM国際比較に必要）")
    lines.append("")

    lines.append("## 優先度マトリクス\n")
    lines.append("| 改善項目 | 影響度 | 実装コスト | 優先度 |")
    lines.append("|---------|--------|-----------|--------|")
    lines.append("| release_year_filterツール追加 | 高 | 低 | **P1** |")
    lines.append("| prefecture_profileツール追加 | 高 | 中 | **P1** |")
    lines.append("| Wikidata P577でrelease_year拡充 | 高 | 中 | **P1** |")
    lines.append("| 都道府県タグ追加 | 中 | 中 | **P2** |")
    lines.append("| pilgrimage_route_planner追加 | 中 | 高 | **P2** |")
    lines.append("| 英語ラベル拡充 | 中 | 低 | **P2** |")
    lines.append("| cross_culture_searchツール | 低 | 中 | **P3** |")
    lines.append("| 外部SNSデータ連携 | 低 | 高 | **P3** |")
    lines.append("")

    report = "\n".join(lines)
    OUTPUT_MD.write_text(report, encoding="utf-8")
    print(f"\nReport written to {OUTPUT_MD}")
    print(f"  Length: {len(report):,} chars")
    print(f"  Low scores: {len(low)}")
    print(f"  Overall avg satisfaction: {overall_avg:.2f}/5")


if __name__ == "__main__":
    main()
