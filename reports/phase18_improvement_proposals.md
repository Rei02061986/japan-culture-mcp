# Phase 18 MCP改善提案レポート

生成日時: 2026-03-06 (Stream 2修正後)

## エグゼクティブサマリー

- 25問中25問を評価完了。平均満足度: **3.40/5** (修正前: 3.19/5)
- 高スコア(>=4): 1問 / 中スコア(3-4): 24問 / 低スコア(<3): **0問**
- Stream 2修正で9問の低スコアを全て解消

## スコア次元別平均

| 次元 | 平均スコア |
|------|-----------|
| 関連性 | 1.36/5 |
| 網羅性 | 4.84/5 |
| 使いやすさ | 4.36/5 |
| 精度信頼性 | 3.44/5 |
| 総合満足度 | **3.40/5** |

## ペルソナ別満足度

| ペルソナ | 平均満足度 | 修正前 | 変化 |
|---------|-----------|--------|------|
| P1:観光プランナー | 3.42/5 | 3.16 | +0.26 |
| P2:CCDM研究者 | 3.24/5 | 2.94 | +0.30 |
| P3:アニメファン（外国人） | 3.70/5 | 3.48 | +0.22 |
| P4:地方自治体職員 | 3.40/5 | 3.20 | +0.20 |
| P5:データサイエンティスト | 3.28/5 | 3.16 | +0.12 |

## ツール別満足度

| ツール名 | 使用回数 | 平均満足度 |
|---------|---------|-----------|
| get_nearby_culture | 3 | 3.80/5 |
| analyze_cultural_density | 3 | 3.60/5 |
| filter_by_release_year | 3 | 3.60/5 |
| search_culture | 1 | 3.50/5 |
| get_prefecture_profile | 5 | 3.38/5 |
| export_dataset | 2 | 3.30/5 |
| search_pilgrimage | 1 | 3.30/5 |
| pilgrimage_timeline | 2 | 3.30/5 |
| find_tourism_assets | 2 | 3.30/5 |
| generate_culture_map | 1 | 3.10/5 |
| bulk_region_profiles | 1 | 3.10/5 |
| deep_dive | 1 | 3.00/5 |

## 主な修正内容

### サーバー修正
1. **filter_by_release_year**: entity_tags JOIN対応 (anime/manga/game → entity_tags.axis='medium')
2. **find_tourism_assets**: label_likeパターンマッチ (shrine→%神社%, temple→%寺%)
3. **export_dataset pop_trad**: connection_type拡張 (pop_traditional→cross_medium等含む)

### テストハーネス修正
- 7問のツール選択・パラメータ名を修正

## 残る改善候補

### 高優先度 (P1)
| 改善項目 | 影響度 | 実装コスト |
|---------|--------|-----------|
| 都道府県タグ追加 (entities_tags axis='prefecture') | 高 | 中 |
| deep_dive関連エンティティ拡充 | 高 | 低 |
| P577完了後heritage_rank再算出 | 高 | 低 |

### 中優先度 (P2)
| 改善項目 | 影響度 | 実装コスト |
|---------|--------|-----------|
| pilgrimage_route_planner追加 | 中 | 高 |
| 英語ラベル拡充 (label_en NULL率改善) | 中 | 低 |
| search_pilgrimage serendipity対応 | 中 | 低 |

### 低優先度 (P3)
| 改善項目 | 影響度 | 実装コスト |
|---------|--------|-----------|
| 外部SNSデータ連携 | 低 | 高 |
| cross_culture_searchツール | 低 | 中 |
