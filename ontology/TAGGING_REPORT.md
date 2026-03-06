# Phase 3.5 タグ付け・接続グラフ レポート

**日付**: 2026-02-28
**DB**: `ontology/culture_ontology.db` (1,060 KB)

---

## 1. 概要

Phase 3で構築した1,197エンティティに対し、5軸（テーマ・時代・媒体・地理・体験モード）のタグを自動付与し、500件の接続グラフを生成した。

| 指標 | 目標 | 結果 | 判定 |
|------|------|------|------|
| エンティティ タグ率 | 80%+ | **100%** (1,197/1,197) | OK |
| テーマタグ数 | >500 | **1,470** | OK |
| 媒体タグ数 | >800 | **1,197** | OK |
| 時代タグ数 | >800 | **1,197** | OK |
| 接続数 | >100 | **500** | OK |
| good_surprise | >50 | **51** | OK |
| MCP tools | 17 | **17** | OK |

**合計タグ数**: 5,020

---

## 2. タグ付け手法

### 2.1 人物エンティティ (955件)

| 軸 | 手法 | ソース | 信頼度 |
|----|------|--------|--------|
| medium | 職業→媒体マッピング (28種) | `wikidata_occupation` | 0.9 |
| theme | 職業→テーママッピング (17種) | `wikidata_occupation` | 0.7 |
| experience | 職業→体験マッピング (20種) | `wikidata_occupation` | 0.7 |
| era | Wikidata REST API (P569/P570) | `wikidata_dates` | 0.8 |
| era | 職業→時代デフォルト (未取得分) | `occupation_default` | 0.5 |

- **Wikidata API呼び出し**: 351件（先頭200件 + 5件おき）
- **職業カバー率**: 100%（955件全てにoccupationあり）
- **時代タグ**: API由来 348件 + デフォルト 607件

### 2.2 場所エンティティ (123件)

| 軸 | 手法 | ソース | 信頼度 |
|----|------|--------|--------|
| geography | 座標→地域判定 (8地方) | `coordinates` | 0.95 |
| theme | ラベルキーワード (17語) | `label_keyword` | 0.7 |
| experience | ラベルキーワード (15語) | `label_keyword` | 0.6 |
| medium | 全件 "architecture" | `entity_type` | 0.5 |
| era | キーワード→時代 / デフォルト "medieval" | `heritage_default` | 0.4 |

- **地理タグ**: 115/123件 (93%) — 8件は地域境界外
- **テーマ top keyword**: 神社(15), 寺(12), 城(8), 庭園(5)

### 2.3 作品エンティティ (119件)

| 軸 | 手法 | ソース | 信頼度 |
|----|------|--------|--------|
| medium | Wikidata work_type (デフォルト) | `wikidata_type` | 0.8 |
| medium | AniList format (更新) | `anilist_format` | 0.95 |
| theme | AniList genres (17ジャンル) | `anilist_genre` | 0.85 |
| theme | AniList tags (上位5件) | `anilist_tag` | 0.8 |
| theme | ラベルキーワード (20語) | `label_keyword` | 0.6 |
| era | AniList seasonYear | `anilist_year` | 0.95 |
| era | デフォルト "showa_postwar" | `work_default` | 0.4 |

- **AniListマッチ**: 38/119件 (32%)
- **マッチ失敗理由**: Wikidata日本語ラベルとAniListの表記揺れ（例: 正式名称 vs 通称）

---

## 3. 軸別分布

### 3.1 テーマ軸 (1,470タグ)

| テーマ | 件数 | 割合 |
|--------|------|------|
| 技と極み (craft_mastery) | 963 | 65.5% |
| 日常の美 (everyday_beauty) | 183 | 12.4% |
| 聖と俗 (sacred_profane) | 49 | 3.3% |
| 神社仏閣 (shrine_temple) | 47 | 3.2% |
| アイデンティティ (identity_self) | 27 | 1.8% |
| 戦争と葛藤 (war_conflict) | 26 | 1.8% |
| その他 37テーマ | 175 | 11.9% |

**観察**: `craft_mastery`が圧倒的に多い。これは職業ベースの推論で「○○家」「○○師」が全て「技の極み」に分類されるため。テーマの多様性向上にはサブテーマの細分化が有効。

### 3.2 時代軸 (1,197タグ)

| 時代 | 件数 | 割合 |
|------|------|------|
| 昭和戦後 (1945-1989) | 689 | 57.6% |
| 明治大正 (1868-1926) | 188 | 15.7% |
| 中世 (1185-1573) | 85 | 7.1% |
| 平成 (1989-2019) | 77 | 6.4% |
| 近世後期 (1700-1868) | 65 | 5.4% |
| 昭和戦前 (1926-1945) | 37 | 3.1% |
| 古代 (~1185) | 27 | 2.3% |
| 近世前期 (1573-1700) | 27 | 2.3% |
| 令和 (2019~) | 2 | 0.2% |

**観察**: 昭和戦後が過半数。漫画家(200件)がこの時代に集中しているため。

### 3.3 媒体軸 (1,197タグ)

| 媒体 | 件数 |
|------|------|
| 漫画 | 263 |
| 絵画 | 199 |
| 文学 | 197 |
| 音楽 | 197 |
| 浮世絵 | 175 |
| 建築 | 123 |
| アニメ | 27 |
| TVアニメ | 9 |
| アニメ映画 | 4 |
| OVA/ONA | 2 |
| ライトノベル | 1 |

### 3.4 地理軸 (115タグ)

| 地域 | 件数 |
|------|------|
| 近畿 | 47 |
| 関東 | 28 |
| 九州・沖縄 | 16 |
| 中国 | 9 |
| 中部 | 7 |
| 東北 | 5 |
| 北海道 | 3 |

**観察**: 場所エンティティのみ（人物・作品には地理タグなし）。近畿が最多（京都・奈良の文化財集中）。

### 3.5 体験モード軸 (1,041タグ)

| モード | 件数 |
|--------|------|
| 美的鑑賞 (aesthetic) | 574 |
| 知的探索 (intellectual) | 403 |
| 内省 (reflective) | 47 |
| 冒険 (adventure) | 16 |
| 身体的体験 (physical) | 1 |
| 社交 (social) | 0 |

---

## 4. 接続グラフ

### 4.1 統計

| 指標 | 値 |
|------|-----|
| 接続総数 | 500 |
| good_surprise (era_bridge) | 51 |
| moderate (default) | 449 |
| ユニークエンティティペア | 500 |
| 人物-人物 | 441 |
| 作品-作品 | 59 |

### 4.2 接続タイプ

- **era_bridge** (51件): テーマが近く、時代が遠い接続。最もセレンディピティが高い。
  - 例: 横山松三郎（近世後期）↔ 村上隆（平成）— 「技と極み・日常の美」
  - 例: 横山松三郎（近世後期）↔ 奈良美智（平成）— 「技と極み・日常の美」

- **default** (449件): 中程度のスコア（0.3）で通過した接続。テーマは近いが特定のパターンに合致しないもの。

### 4.3 スコア分布

| スコア帯 | 件数 |
|----------|------|
| 高 (>=0.5) | 10 |
| 中 (0.3-0.5) | 490 |

### 4.4 課題

1. **medium_cross/geo_themeが0件**: craft_mastery偏重のため、テーマは近いが媒体・地理が遠い組み合わせが少ない
2. **人物-場所/作品-場所の接続が0件**: 場所のテーマ（shrine_temple等）と人物のテーマ（craft_mastery）が重ならない
3. **接続の多様性**: 51件のera_bridge中、横山松三郎が10件に関与 — ハブノードが偏っている

---

## 5. MCP ツール

### 5.1 find_serendipity (Tool #16)

```
find_serendipity(keyword="北斎")
→ 葛飾北斎 (person)
  tags: {medium: painting, theme: craft_mastery, experience: aesthetic, era: edo_late}
  connections: 1件 (横山松三郎, score: 0.30, type: default)
```

**仕様**:
- エンティティ名部分一致 → 5軸タグ表示 → 接続グラフからセレンディピティ接続を返す
- `min_score`パラメータでフィルタ可能
- 接続の距離プロファイル（5軸）と説明テキストを含む

### 5.2 explore_axis (Tool #17)

```
explore_axis(axis="theme")
→ 84テーマ値一覧 + 各値のエンティティ数

explore_axis(axis="era", value="edo_late")
→ 近世後期のエンティティ65件（与謝蕪村, 葛飾北斎, 歌川広重, ...）
```

**仕様**:
- `value`省略時: 軸の全値一覧 + エンティティカウント
- `value`指定時: その値を持つエンティティ検索（全タグ付き）
- `entity_type`フィルタ対応

---

## 6. タグ信頼度分析

| 信頼度 | 件数 | 割合 | ソース例 |
|--------|------|------|----------|
| 0.95 | — | — | coordinates, anilist_format, anilist_year |
| 0.9 | 1,123 | 22% | wikidata_occupation (medium) |
| 0.85 | — | — | anilist_genre |
| 0.8 | 599 | 12% | wikidata_dates, anilist_tag, wikidata_type |
| 0.7 | 2,237 | 45% | wikidata_occupation (theme, experience) |
| 0.6 | 104 | 2% | label_keyword (experience, keyword theme) |
| 0.5 | 730 | 15% | occupation_default, entity_type |
| 0.4 | 227 | 5% | heritage_default, work_default |

**平均信頼度**: 0.72

---

## 7. 改善提案

### 短期（Phase 4向け）
1. **テーマ多様化**: craft_mastery内をサブテーマ（書道/陶芸/日本画/漫画技法 等）に細分化
2. **人物→地理**: 出生地(P19)をWikidataから取得し、人物にも地理タグを付与
3. **AniListマッチ改善**: romaji/english titleでの照合追加（現在のJA→EN fallbackに加え）
4. **接続生成の改善**: medium_cross/geo_themeルールが発火するようテーマ偏重を解消

### 中期
5. **令和エンティティ拡充**: 現在2件のみ。2019年以降の作品・アーティストを追加
6. **体験モード**: social/physicalが極端に少ない。祭り・温泉・料理イベント等を追加
7. **人物-場所接続**: ゆかりの地(P937)をWikidataから取得し、cross-type接続を生成

---

## 8. 実行ログ

```
Phase 3.5 実行: 2026-02-28
Task A1 (persons): 3,388 tags, 351 era queries (Wikidata REST API)
Task A2 (places):  476 tags, 0 API calls
Task A3 (works):   322 tags, 119 AniList queries (38 matched)
Era supplement:    834 tags (occupation/heritage/work defaults)
Task B (graph):    500 connections, 51 good_surprise

Total: 5,020 tags, 500 connections
DB size: 1,060 KB
MCP tools: 17 (find_serendipity + explore_axis added)
```
