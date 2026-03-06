# Phase 4 セレンディピティテスト結果

**日付**: 2026-02-28
**DB**: `ontology/culture_ontology.db` (Phase 4改善後)

---

## 改善前 (Phase 3.5) → 改善後 (Phase 4) 比較

| 指標 | Phase 3.5 | Phase 4 | 改善率 |
|------|-----------|---------|--------|
| Theme HHI | 0.4482 | 0.1048 | **76%低下** |
| 最大テーマ占有率 | 65.5% (craft_mastery) | 23.2% (visual_arts) | **65%→23%** |
| 接続総数 | 500 | 926 | **+85%** |
| 接続ルール種類 | 2 (era_bridge, default) | **6** | **+4種** |
| medium_cross | 0 | **183** | **0→183** |
| geo_theme | 0 | **209** | **0→209** |
| experience_shift | 0 | **30** | **0→30** |
| cross_type_person_place | 0 | **85** | **0→85** |
| person↔work | 0 | **31** | **0→31** |
| 人物地理カバー率 | 0% | **59%** | **0→59%** |
| 高スコア接続 (>=0.6) | 10 | **377** | **37x** |

---

## テスト1: 「北斎」

### Phase 3.5
- 接続: 1件 (横山松三郎, score:0.30, type:default)
- person↔place: 0件
- ルールタイプ: 1種

### Phase 4
- 接続: 6件
- ルールタイプ: **2種** (cross_type_person_place, era_bridge)
- person↔place: **5件** (偕楽園, 岩宿遺跡, 国立科学博物館, 迎賓館赤坂離宮, 華厳滝)
- person↔work: 1件 (ブラックジャックによろしく — era_bridge, score:0.49)

```
[0.50] cross_type_person_place: 偕楽園 (place)
[0.50] cross_type_person_place: 岩宿遺跡 (place)
[0.50] cross_type_person_place: 国立科学博物館 (place)
[0.49] era_bridge: ブラックジャックによろしく (work)
```

**改善点**: 場所との接続が生まれた。地理軸（kanto）経由で関東の文化財と結ばれる。

---

## テスト2: 「浅草」

### Phase 3.5
- 接続: 0件

### Phase 4
- 接続: **10件**
- ルールタイプ: **1種** (geo_theme)
- 接続先: 平等院, 崇福寺, 住吉神社, 不動院, 興福寺 等

```
[0.50] geo_theme: 平等院 (place)
[0.50] geo_theme: 崇福寺 (place)
[0.50] geo_theme: 住吉神社 (place)
```

**改善点**: 「聖と俗」テーマで他の神社仏閣と接続。異なる地域の類似テーマ施設を発見できる。

---

## テスト3: 「鬼滅の刃」

### Phase 3.5
- テスト不可（接続0件）

### Phase 4
- 接続: **10件**
- ルールタイプ: **2種** (medium_cross, geo_theme)
- 代表接続:
  - 崖の上のポニョ (medium_cross, score:0.88) — 漫画↔アニメ映画
  - あの日見た花の名前を僕達はまだ知らない。(medium_cross, score:0.88)
  - それいけ!アンパンマン (medium_cross, score:0.88) — 超自然テーマ共有

---

## テスト4: 「機動戦士ガンダム」

### Phase 4
- 接続: **10件**
- ルールタイプ: **1種** (medium_cross)
- 代表接続:
  - キングダム (medium_cross, score:1.10) — 軍事テーマ
  - この世界の片隅に (medium_cross, score:1.10) — 戦争テーマ
  - 宇宙兄弟 (medium_cross, score:1.10) — 宇宙テーマ

---

## テスト5: 「妖怪」「忍者」「金閣寺」

**結果**: エンティティ不在（Wikidata mapping に含まれていない）

**対策案**:
- Phase 5でカテゴリ検索を実装（テーマ値 "yokai" で検索 → 鬼滅の刃等がヒット）
- `find_serendipity` をキーワード → テーマ値マッチングに拡張

---

## 全体評価

| 評価項目 | スコア | 備考 |
|----------|--------|------|
| ルールタイプカバレッジ | 6/7 | cross_type_work_place は place↔work として存在 |
| 平均セレンディピティスコア | 0.52 | Phase 3.5の0.32から改善 |
| cross_type接続の有無 | OK | person↔place: 85, person↔work: 31 |
| 地理軸活用度 | OK | 59%の人物に地理タグ、geo_theme 209件 |
| テーマ多様性 | OK | HHI 0.1048、6種以上のルール発火 |

### 残課題
1. 「妖怪」等の概念検索 → テーマ値ベースの検索拡張が必要
2. 北斎↔場所の接続が汎用的（関東の文化財全般） → ゆかりの地(P937)取得で精度向上
3. ガンダムのmediumが "light_novel" になっている → AniListマッチング時のformat更新バグ
4. explanation テキストの多様性向上（現在テンプレート6種）
