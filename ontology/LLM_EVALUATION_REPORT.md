# Phase 4.5 LLM品質フィルタ レポート

**日付**: 2026-02-28
**モデル**: GPT-4o
**コスト**: $1.24 (input: 214K tokens, output: 70K tokens)

---

## 1. 概要

977件の接続をGPT-4oで評価し、文化的関連性とセレンディピティの質を判定。
意味のない接続をrejectし、高品質な121件のみをkeepとして残した。

| 指標 | 値 |
|------|------|
| 評価対象 | 977接続 |
| Keep | **121** (12.4%) |
| Reject | 856 (87.6%) |
| バッチサイズ | 10件 |
| バッチ数 | 98 |
| 処理時間 | 約8分 |
| APIコスト | **$1.24** |

---

## 2. ルールタイプ別生存率

| ルールタイプ | Keep | Total | 生存率 | 平均CR | 平均SQ |
|-------------|------|-------|--------|--------|--------|
| medium_cross | 59 | 183 | **32.2%** | 0.31 | 0.34 |
| experience_shift | 9 | 30 | **30.0%** | 0.41 | 0.33 |
| era_bridge | 32 | 313 | 10.2% | 0.26 | 0.26 |
| geo_theme | 20 | 260 | 7.7% | 0.26 | 0.23 |
| cross_type_person_place | 1 | 85 | 1.2% | 0.21 | 0.17 |
| default | 0 | 106 | 0.0% | 0.24 | 0.20 |

**CR** = cultural_relevance, **SQ** = serendipity_quality

**観察**:
- `medium_cross`（異なるメディア間）と`experience_shift`（異なる体験モード間）が最も高品質
- `default`は全件reject — テーマが近いだけでは文化的つながりにならない
- `cross_type_person_place`はほぼ全reject — 同地域というだけの汎用的な接続は無意味

---

## 3. Keep接続のエンティティタイプ

| タイプ | 件数 |
|--------|------|
| work↔work | 72 |
| person↔person | 21 |
| place↔place | 10 |
| place↔work | 10 |
| person↔work | 7 |
| person↔place | 1 |

作品同士の接続が最も高品質。人物-場所の接続は具体的なゆかりがないとrejectされる。

---

## 4. TOP10 最高スコア接続

| CR | SQ | 接続 | タイプ |
|----|----|----|--------|
| 0.8 | 0.7 | 奈良国立博物館 ↔ 白鶴美術館 | experience_shift |
| 0.8 | 0.7 | 白鶴美術館 ↔ 京都国立博物館 | experience_shift |
| 0.7 | 0.6 | 宇宙戦艦ヤマト ↔ 機動戦士ガンダム | medium_cross |
| 0.6 | 0.6 | セーラームーン ↔ ヤマト2199 | medium_cross |
| 0.6 | 0.6 | 花咲くいろは ↔ この世界の片隅に | medium_cross |
| 0.5 | 0.6 | セーラームーン ↔ 魔女の宅急便 | medium_cross |
| 0.5 | 0.6 | まどか☆マギカ ↔ ヱヴァンゲリヲン | medium_cross |
| 0.6 | 0.5 | 東京国立博物館 ↔ 白鶴美術館 | experience_shift |
| 0.6 | 0.5 | 国立科学博物館 ↔ 白鶴美術館 | experience_shift |
| 0.6 | 0.5 | ヱヴァンゲリヲン ↔ ガンダム | medium_cross |

---

## 5. テストケース

### 5.1 find_serendipity("北斎")

- **エンティティ**: 葛飾北斎 (person)
- **Keep接続**: 0件
- **全接続**: 6件 (全てreject)
- **理由**: 北斎↔偕楽園等はcross_type_person_placeで「関東の文化財」というだけの汎用接続。GPT-4oが「具体的なゆかりなし」と判定。

```
偕楽園 → reject (視覚芸術というテーマで結ばれるが、具体的な文化的関連性が薄い)
岩宿遺跡 → reject
国立科学博物館 → reject
```

### 5.2 find_serendipity("浅草")

- **エンティティ**: 浅草神社 (place)
- **Keep接続**: 0件
- **全接続**: 10件 (全てreject)
- **理由**: geo_themeで「聖と俗」を共有する他の神社仏閣との接続だが、GPT-4oは「同じ宗教施設というだけで新しい発見がない」と判定。

### 5.3 find_serendipity("鬼滅の刃")

- **エンティティ**: 鬼滅の刃 (work)
- **Keep接続**: 1件
- **結果**:

```
[0.5/0.4] 火の鳥 (geo_theme, score=0.4) → keep
  理由: 手塚治虫の「火の鳥」と「鬼滅の刃」は共に生死と超自然をテーマにしており、文化的な共鳴がある
```

### 5.4 find_serendipity("妖怪")

- **検索モード**: theme（テーマ階層フォールバック → yokai + 兄弟テーマ10種で検索）
- **最適エンティティ**: ヱヴァンゲリヲン新劇場版 (9件keep)
- **結果**:

```
[0.6/0.5] 機動戦士ガンダム (medium_cross) → keep
[0.5/0.6] 魔法少女まどか☆マギカ (medium_cross) → keep
[0.4/0.5] サイボーグ009 (medium_cross) → keep
[0.4/0.5] センゴク (medium_cross) → keep
[0.4/0.4] 装甲騎兵ボトムズ (medium_cross) → keep
```

### 5.5 find_serendipity("忍者")

- **検索モード**: theme（theme_values.name_ja LIKE '%忍者%' → ninja）
- **エンティティ**: 0件（忍者テーマを持つエンティティがDBに不在）
- **対策**: Phase 5でNARUTO等の忍者作品にninjaテーマを付与

---

## 6. 完了条件チェック

| 条件 | 結果 | 判定 |
|------|------|------|
| 全件評価済み | 977/977 | **OK** |
| 北斎↔偕楽園がreject | reject | **OK** |
| 鬼滅↔アンパンマンがreject | reject | **OK** |
| 「妖怪」テーマ検索で結果 | 9件keep (via ヱヴァンゲリヲン) | **OK** |
| LLM_EVALUATION_REPORT.md | 本ファイル | **OK** |

---

## 7. GPTプロンプト設計

### System Prompt
```
日本文化の専門家として、文化的関連性(0-1)と意外性の質(0-1)を評価。
- 同じ地域・時代だけの接続はreject
- テーマが近くても文化的文脈が異なればreject
- 異ジャンル・時代を超えた本質的共鳴があればkeep
- 人物↔場所は具体的ゆかりがある場合のみkeep
```

### 判定基準
- verdict = "keep": 両スコア平均 >= 0.4
- verdict = "reject": 両スコア平均 < 0.4

---

## 8. MCP改善

### find_serendipity 変更点
1. `llm_verdict='keep'` フィルタ追加（keep接続のみ返す）
2. `search_mode` パラメータ追加: "entity" / "theme" / "auto"
3. LLM評価データ（cr, sq, reason）をレスポンスに含める
4. keepが0件の場合、フィルタなしにフォールバック

---

## 9. 改善提案 (Phase 5向け)

1. **低生存率ルールの改善**:
   - `cross_type_person_place` (1.2%): Wikidata P937（ゆかりの地）ベースの接続に置換
   - `default` (0%): ルール自体を廃止
   - `geo_theme` (7.7%): テーマ重複度を厳格化（2テーマ以上の共有を要求）

2. **高品質接続の拡充**:
   - `medium_cross` (32.2%): 作品間のメディアクロス接続を増やす
   - `experience_shift` (30.0%): 博物館・美術館間の体験差接続を増やす

3. **エンティティ拡充**:
   - NARUTO、犬夜叉等の忍者・妖怪作品を追加
   - 金閣寺（鹿苑寺の別名）を検索可能に

4. **接続説明文の改善**:
   - GPT-4oのreasonをexplanationに反映（現在のテンプレートより高品質）

---

## 10. 実行ログ

```
Phase 4.5 実行: 2026-02-28

Task A: ontology/llm_evaluator.py 作成
  - ConnectionCandidate, LLMEvaluation, GPTConnectionEvaluator
  - 10件バッチ評価、json_object response format

Task B: 977件全件評価
  - 98バッチ × 1秒間隔
  - input: 214,161 tokens, output: 70,335 tokens
  - コスト: $1.24 (GPT-4o)
  - 結果: keep=121, reject=856

Task C: find_serendipity 改善
  - llm_verdict='keep' フィルタ
  - search_mode パラメータ
  - LLM評価データ付きレスポンス

Task D: 本レポート

DB: 977接続 (keep=121, reject=856)
新規テーブル: llm_evaluations (977行)
新規カラム: llm_cultural_relevance, llm_serendipity_quality, llm_explanation, llm_verdict
```
