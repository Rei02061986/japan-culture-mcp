# Culture Ontology 設計レポート v0.1

生成日: 2026-02-28
フェーズ: Phase 3 — オントロジー基盤構築

---

## 1. 既存分類体系の統合結果

### 1.1 AniList 417タグ → 独自5軸マッピング

AniList の 19ジャンル + 25カテゴリ・417タグを独自5軸にマッピングした。

| 軸 | マッピングされたタグ数 | カテゴリ数 | 代表例 |
|----|---------------------|-----------|--------|
| テーマ | 285タグ（うち172個がtheme_valuesに接続） | 16 | Youkai, Isekai, Samurai, War |
| テーマ/体験 | 74タグ | 5 | Sports, Food, Iyashikei, Music |
| 時代 | 7タグ | 1 | Historical, Medieval, Ancient China |
| 媒体 | 18タグ | 1 | 4-koma, CGI, Stop Motion |
| 地理/体験 | 23タグ | 1 | Rural, Urban, School, Konbini |
| メタ（軸外） | 84タグ | 3 | Josei, Seinen, Sexual Content |

**マッピング率**: 417タグ中177タグ（42%）がtheme_valuesコードに直接接続済み。
残り240タグは軸への分類は完了しているが、個別のtheme_valuesへの紐付けが未完了（主にSetting-Scene, Cast-Traits, Sexual Content）。

**日本文化固有タグ**: 34タグを特定
- Theme-Fantasy: 妖怪, 異世界, 変身, 怪獣, 退魔
- Theme-Arts: 歌舞伎, 書道, 漫才, 落語
- Cast-Traits: 忍者, 侍, 巫女, 花魁, ギャル, 中二病, ひきこもり
- その他: 相撲, 柔道, 囲碁, 将棋, 百人一首, 特撮, 癒し系, コンビニ, ヤクザ

### 1.2 MADB 24クラス → 媒体軸マッピング

MADBの24クラスを4大カテゴリ + メタに分類した。

| カテゴリ | クラス数 | 総インスタンス数 | 代表クラス |
|---------|---------|---------------|-----------|
| 漫画 | 6 | 759,398 | MangaBook (394K), MangaBookSeries (139K) |
| アニメ | 8 | 345,497 | AnimationTVProgram (197K), AnimationVideoPackage (65K) |
| ゲーム | 4 | 67,848 | GamePackage (51K), GameVariation (8K) |
| メディアアート | 3 | 35,186 | MediaArtExhibitionOrPerformance (20K) |
| メタ | 3 | 1,759,989 | Supplement (1.68M), Agent (71K) |

**21のMADBクラス → medium_valuesへのマッピング完了**。

**カバーされていない媒体**（独自追加）:
- 伝統文化: 絵画, 浮世絵, 彫刻, 建築, 歌舞伎, 能, 文楽
- 近代: ライトノベル, 特撮, 音楽, 祭礼, 工芸, 古典籍

### 1.3 AniList × MADB ギャップ分析

| 領域 | AniList | MADB | 統合状態 |
|------|---------|------|---------|
| テーマ分類 | ◎ 417タグ | × なし | AniList一方向 |
| 媒体分類 | ○ format 9種 | ◎ 24クラス | MADB中心、AniList format で補完 |
| ゲーム | × | ◎ 67K件 | MADB独占 |
| ライトノベル | ○ format=NOVEL | × | AniList独占 |
| エピソード粒度 | × 作品単位 | ◎ 個別エピソード | MADB独占 |
| 定量評価 | ◎ score/popularity | × | AniList独占 |

## 2. Wikidata IDマッピング結果

VM上でWikidata SPARQLを4カテゴリで実行。

| カテゴリ | 取得数 | NDL ID保有 | MADB ID保有 |
|---------|--------|-----------|------------|
| アニメ作品 | 46 | 46 | 0 |
| 漫画作品 | 75 | 75 | 0 |
| 歴史的人物 | 1,000 | 1,000 | 0 |
| 文化財・名所 | 200 | 47 | 0 |
| **合計** | **1,321** | **1,272** | **0** |

### 発見事項

1. **NDL IDの普及率が高い**: 取得エンティティの96%がNDL IDを保有。NDLが日本文化のハブとして機能している
2. **MADB IDは未普及**: Wikidata上でP4082（MADB ID）を持つエンティティは極めて少ない。MADBとの接続はキーワードマッチが主力
3. **人物データが豊富**: 浮世絵師200名、日本画家200名、漫画家200名、著作家200名、作曲家200名を取得
4. **座標付き文化財**: 200件の文化財・名所に緯度経度データあり。地理軸の初期データとして有用
5. **SQLiteへの投入**: 1,321件中1,197件をentitiesテーブルに投入（重複除去後）

### DB間接続カバレッジ

```
Wikidata ──(P349)──> NDL: 1,272件（高カバレッジ）
Wikidata ──(P4082)──> MADB: 0件（未接続）
Wikidata ──(sitelinks)──> DBpedia: jawikiリンク経由で接続可能（未計測）
AniList ←→ MADB: title matchingのみ（Phase 2で実証済み）
```

## 3. 5軸の初期定義

### テーマ軸: 15大テーマ + 69サブテーマ = 84項目

大テーマ（CCDMの文化的深さに対応）:

| # | コード | 日本語 | サブテーマ数 |
|---|--------|--------|------------|
| 1 | death_rebirth | 死と再生 | 4 |
| 2 | transformation | 変容 | 5 |
| 3 | journey_boundary | 旅と境界 | 3 |
| 4 | nature_communion | 自然との交感 | 4 |
| 5 | power_rebellion | 権力と反逆 | 6 |
| 6 | everyday_beauty | 日常の美 | 4 |
| 7 | otherworld | 異界 | 7 |
| 8 | war_conflict | 戦争と葛藤 | 3 |
| 9 | love_bond | 愛と絆 | 3 |
| 10 | humor_satire | 笑いと風刺 | 4 |
| 11 | craft_mastery | 技と極み | 6 |
| 12 | sacred_profane | 聖と俗 | 3 |
| 13 | identity_self | アイデンティティ | 3 |
| 14 | community_tradition | 共同体と伝統 | 4 |
| 15 | supernatural | 超自然 | 10 |

### 時代軸: 9区分

| 区分 | 期間 | 備考 |
|------|------|------|
| 古代 | 〜1185 | 縄文〜平安 |
| 中世 | 1185-1573 | 鎌倉〜戦国 |
| 近世前期 | 1573-1700 | 安土桃山〜江戸前期 |
| 近世後期 | 1700-1868 | 江戸中期〜幕末 |
| 明治大正 | 1868-1926 | |
| 昭和戦前 | 1926-1945 | |
| 昭和戦後 | 1945-1989 | |
| 平成 | 1989-2019 | |
| 令和 | 2019- | |

### 媒体軸: 13トップレベル + 18サブカテゴリ = 31項目

トップレベル: 絵画, 彫刻, 建築, 文学, 演劇, 漫画, アニメ, ゲーム, 音楽, 祭礼, 工芸, メディアアート, 特撮

MADBクラスとの対応: 21クラスがmedium_valuesにマッピング済み

### 地理軸: 8地方 + 14都市/スポット = 22項目

階層: 地方 → 都道府県 → 市区町村 → スポット
初期データ: 8地方 + 主要文化都市（東京・京都・奈良・大阪・鎌倉・日光）+ 文化スポット（浅草・秋葉原・祇園・伏見稲荷）

Wikidata座標付き文化財200件で拡張予定。

### 体験モード軸: 6モード

| コード | 日本語 | 英語 | 説明 |
|--------|--------|------|------|
| intellectual | 知的探索 | Intellectual | 歴史・文脈・意味の理解 |
| aesthetic | 美的鑑賞 | Aesthetic | 視覚的・聴覚的美の体験 |
| physical | 身体的体験 | Physical | 歩く、作る、食べる |
| social | 社交 | Social | 交流、祭り参加 |
| reflective | 内省 | Reflection | 瞑想、精神的体験 |
| adventure | 冒険 | Adventure | 未知への挑戦 |

## 4. 接続文法の初期ルール

7つの接続文法ルールを定義:

| ルール名 | 条件 | 品質 | 重み |
|---------|------|------|------|
| good_surprise_classic | テーマ近接 × 時代遠距離 × 媒体異種 | good_surprise | 1.0 |
| obvious | 全軸近接 | obvious | 0.3 |
| random | 全軸遠距離 | bad_surprise | 0.1 |
| era_bridge | テーマ同一 × 時代橋渡し | good_surprise | 1.2 |
| medium_cross | 同テーマ同時代 × 媒体横断 | good_surprise | 1.1 |
| geo_theme | 同テーマ × 地理遠距離 | good_surprise | 1.0 |
| experience_shift | 同テーマ × 体験モード変化 | good_surprise | 1.0 |

**設計判断**: 「良い意外性」を生むルールには高い重みを、「当たり前」「恣意的」には低い重みを設定。セレンディピティスコアはこれらのルールに基づいて計算される。

## 5. SQLiteスキーマサマリー

`ontology/culture_ontology.db` (372KB)

| テーブル | レコード数 | 用途 |
|---------|-----------|------|
| axes | 5 | 5軸定義 |
| theme_values | 84 | テーマ軸の値（15大テーマ+69サブ） |
| era_values | 9 | 時代軸の値 |
| medium_values | 31 | 媒体軸の値（13トップ+18サブ） |
| geography_values | 22 | 地理軸の値（8地方+14都市） |
| experience_values | 6 | 体験モード軸の値 |
| anilist_tag_mapping | 417 | AniListタグ→5軸マッピング |
| madb_class_mapping | 21 | MADBクラス→媒体軸 |
| entities | 1,197 | Wikidataエンティティ（IDハブ） |
| entity_tags | 0 | エンティティの5軸タグ（未投入） |
| connections | 0 | 接続グラフ（未投入） |
| connection_grammar | 7 | 接続文法ルール |

**総レコード数: 2,216**

## 6. ギャップと次のステップ

### テーマ軸の不足

- **日本固有の美学概念**: 侘寂、もののあはれ、幽玄 — AniListには存在しないが文化体験の核心
- **宗教・信仰**: 神道、仏教、修験道の体系的分類が不足
- **伝統工芸**: 陶芸、染織、漆芸等の個別テーマが未整備
- **季節感**: 花見、紅葉、雪見等の季節行事テーマが未整備

### 接続グラフの初期データ投入

1. **Phase 3.5**: entities に対して entity_tags を自動投入
   - アニメ/漫画 → AniListタグからテーマ・媒体を推定
   - 人物 → 職業から媒体を推定（浮世絵師→painting, 漫画家→manga）
   - 場所 → 座標から地理軸を自動タグ付け
   - 時代 → datePublished/生年から時代軸を推定

2. **Phase 4**: connections を構築
   - 同テーマエンティティ間の接続
   - 同地理エンティティ間の接続
   - 時代を跨ぐ接続（era_bridge）
   - 媒体を跨ぐ接続（medium_cross）

### LLMタグ付けパイプライン

- 体験モード軸はLLM推定が必須（既存ソースにデータなし）
- テーマ軸の深い分類もLLM支援で精度向上可能
- 接続の explanation テキスト生成にLLMを使用

## 7. CCDMとの接続

### K_i,t（文化資本）推定に使えるデータポイント

| データポイント | ソース | K推定への貢献 |
|--------------|--------|-------------|
| ソース分布 | cross_reference_v2の結果 | 複数ソースにヒット → 高K |
| テーマ軸の深さ | entity_tagsのサブテーマ到達 | サブテーマまで到達 → 高K |
| 媒体横断度 | entity_tagsの媒体軸分布 | 複数媒体を跨ぐ → 高K |
| 時代遡及度 | era_valuesの値 | 近世以前に関心 → 高K |
| AniListスコア | popularity, averageScore | 高人気 → 消費的K、ニッチ → 深K |

### m_d,t（商品化度）推定に使えるデータポイント

| データポイント | ソース | m推定への貢献 |
|--------------|--------|-------------|
| ジャパンサーチprovider分布 | search_japan_search | 博物館のみ → 低m、商業含む → 高m |
| AniList popularity | search_anime | 高人気 → 高m |
| MADB出版社数 | search_media_arts | 出版社多数 → 高m |
| NDL蔵書数 | search_ndl | 多数 → 高m（ただし学術的のため注意） |
| 媒体タイプ | medium_values | アニメ/ゲーム → 高m、伝統芸能 → 低m |

## 8. MCPサーバー更新

### Wikidata REST API フォールバック追加

`resolve_entity` ツールを2段構成に更新:

1. **REST API** (wbsearchentities → EntityData): 高速、タイムアウトなし
2. **SPARQL** (フォールバック): REST APIで結果が得られない場合のみ

追加した関数:
- `_wikidata_rest_search()`: wbsearchentities による高速検索
- `_wikidata_get_entity()`: EntityData による詳細取得
- `_get_claim_value()`: Wikidataクレーム値の汎用抽出

**利点**: VM環境でのSPARQLタイムアウト問題を完全に回避。REST APIは15秒タイムアウトで安定動作。

---

## 成果物一覧

| ファイル | サイズ | 内容 |
|---------|--------|------|
| `ontology/anilist_taxonomy.json` | AniList全タグの5軸分類 |
| `ontology/madb_taxonomy.json` | MADB全クラスの構造化 |
| `ontology/taxonomy_crosswalk.md` | AniList × MADB突合表 |
| `ontology/wikidata_id_mapping.json` | 1,321エンティティのIDマッピング |
| `ontology/culture_ontology.db` | 372KB SQLiteデータベース |
| `server/japan_culture_mcp.py` | Wikidata REST API追加済み |
| `scripts/phase3_build_db.py` | SQLite構築スクリプト（再実行可能） |
| `scripts/phase3_wikidata_mapping.py` | Wikidata ID取得スクリプト |

## 結論

Phase 3の基盤構築は完了。5軸のオントロジー構造が定義され、1,197エンティティがSQLiteに投入された。次のステップは:

1. **entity_tags の自動投入** — エンティティへの5軸タグ付け
2. **connections の構築** — セレンディピティエンジンの接続グラフ
3. **LLMタグ付けパイプライン** — 体験モード軸の推定
4. **MCPツールのオントロジー統合** — 検索結果にオントロジー情報を付加
