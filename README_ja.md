# Japan Culture MCP Server (日本文化MCPサーバー)

![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue)
![MCP](https://img.shields.io/badge/MCP-1.0%2B-green)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow)
![Entities: 10M+](https://img.shields.io/badge/Entities-10M%2B-red)
![Tools: 39](https://img.shields.io/badge/Tools-39-purple)
![Geo: 750K+](https://img.shields.io/badge/Geo-750K%2B-orange)

AIアシスタントに日本文化の深い知識へのアクセスを提供するModel Context Protocol (MCP) サーバーです。**1,000万以上のエンティティ**と**390万以上の文化的接続**を持つオントロジーDBを搭載し、古典文化から現代サブカルチャーまで**156以上の権威あるデータソース**から横断検索が可能です。

## 特徴

- **1,000万以上のエンティティ**: アニメ、漫画、浮世絵、寺社、祭り、文学、伝統工芸、人間国宝、博物館コレクションなど
- **390万以上の接続**: テーマ・時代・媒体・地理・体験を超えた「意外な繋がり」を発見する文化的セレンディピティグラフ
- **39のMCPツール**: 検索、発見、比較、マッピング、観光分析、探索の包括的機能
- **FTS5全文検索**: LIKE検索の225倍高速（4ms vs 900ms）で日本語・英語テキストを検索
- **R-Tree空間インデックス**: **750,000以上**の位置情報付きエンティティの高速地理検索
- **聖地巡礼対応**: 3,900以上のアニメ・映画聖地巡礼スポットとルート生成
- **観光分析**: 地域プロファイル、観光資産検索、文化密度ヒートマップ
- **156以上のデータソース**: ジャパンサーチ、Wikidata、MADB、AniList、NDL、OSM、DBpediaなど
- **5軸オントロジー**: テーマ（83値）、時代（10）、媒体（18）、地理（13）、体験（9）

## クイックスタート

### Claude Desktop

`claude_desktop_config.json` に以下を追加:

```json
{
  "mcpServers": {
    "japan-culture": {
      "command": "python3",
      "args": ["-m", "server.japan_culture_mcp"],
      "cwd": "/path/to/japan_culture_mcp",
      "env": {
        "DB_PATH": "/path/to/japan_culture_mcp/ontology/culture_ontology.db"
      }
    }
  }
}
```

### pip インストール

```bash
pip install -e .
japan-culture-mcp
```

### Docker

```bash
# データベースをdata/にコピー
cp ontology/culture_ontology.db data/

# ビルド・起動
docker-compose up -d
```

## ツール一覧 (39)

### コアツール

| # | ツール | 説明 |
|---|--------|------|
| 1 | `search_anime` | AniList GraphQL APIでアニメ・漫画を検索 |
| 2 | `search_media_arts` | MADB SPARQLで漫画・アニメ・ゲームを検索 |
| 3 | `cross_reference` | AniListとMADBの結果をクロスリファレンス |
| 4 | `search_japan_search` | ジャパンサーチで264以上の文化機関DBを横断検索 |
| 5 | `search_wikidata` | Wikidataで日本文化エンティティを検索 |
| 6 | `resolve_entity` | エンティティ名をWikidata IDに解決 |
| 7 | `get_ndl_manifest` | 国立国会図書館のIIIFマニフェストを取得 |
| 8 | `get_ndl_ocr_text` | NDLデジタルコレクションのOCRテキストを取得 |
| 9 | `search_ndl` | NDLをSRUで検索 |
| 10 | `search_dbpedia_ja` | DBpedia Japaneseを検索 |
| 11 | `iiif_get_manifest` | 汎用IIIFマニフェスト取得（CODH・NDL・e-Museum等） |
| 12 | `get_map_tile_url` | 国土地理院タイルURLを取得 |
| 13 | `get_heritage_map_url` | 文化財総覧WebGIS URLを取得 |
| 14 | `get_tourism_stats` | e-Statから観光統計を取得 |
| 15 | `cross_reference_v2` | 全データソース横断検索 |

### セレンディピティ・発見ツール

| # | ツール | 説明 |
|---|--------|------|
| 16 | `find_serendipity` | 文化的セレンディピティを発見（例: 北斎 -> 蟲師） |
| 17 | `explore_axis` | 5軸（テーマ/時代/媒体/地理/体験）で文化を探索 |
| 18 | `get_entity_detail` | エンティティの詳細プロファイル（タグ・接続・座標） |
| 19 | `get_cultural_route` | テーマ+地域で文化ルート生成 |
| 20 | `search_culture` | オントロジーDB + 外部API横断検索 |

### 特化型検索ツール

| # | ツール | 説明 |
|---|--------|------|
| 21 | `search_traditional_crafts` | 伝統工芸検索（陶磁器、織物、漆器等） |
| 22 | `search_literature` | 文学作品検索（青空文庫 + Wikidata） |
| 23 | `search_artworks` | 美術作品・博物館コレクション検索 |
| 24 | `search_festivals` | 祭り・季節行事検索 |
| 25 | `search_living_national_treasures` | 人間国宝検索 |
| 26 | `generate_serendipity_route` | セレンディピティ接続グラフ探索ルート生成 |
| 27 | `explore_connections` | 接続グラフのBFS探索（深さ3まで） |
| 28 | `get_culture_stats` | DB統計情報 |

### 聖地巡礼・位置情報ツール

| # | ツール | 説明 |
|---|--------|------|
| 29 | `search_pilgrimage` | 聖地巡礼スポット検索（作品名/地域/座標） |
| 30 | `generate_pilgrimage_route` | 聖地巡礼ルート生成（アニメ+文化スポット混合） |
| 31 | `get_nearby_culture` | 座標周辺の文化リソース検索 |

### Phase 14 新ツール

| # | ツール | 説明 |
|---|--------|------|
| 32 | `generate_timeline` | テーマ別文化タイムライン生成（時代/地域フィルタ対応） |
| 33 | `compare_cultures` | 2つの文化要素の比較（共通点・相違点・意外な接続） |
| 34 | `generate_culture_map` | GeoJSON形式の文化地図生成（聖地巡礼、工芸、祭り） |
| 35 | `today_in_culture` | 今日の文化トピック（祭り、行事、季節文化） |
| 36 | `deep_dive` | エンティティのカテゴリ別深掘り推薦 |

### Phase 16 観光ツール

| # | ツール | 説明 |
|---|--------|------|
| 37 | `get_region_profile` | 地域の文化プロファイル生成（エンティティ統計、テーマ分布、接続密度） |
| 38 | `find_tourism_assets` | 観光文化資産をカテゴリ別に一覧（寺社、聖地巡礼、食文化、祭り等） |
| 39 | `analyze_cultural_density` | 格子状の文化密度分析（ヒートマップ可視化用データ） |

## データソース

| ソース | エンティティ数 | 説明 |
|--------|---------------|------|
| ジャパンサーチ SPARQL | 約650万 | 版画、書籍、古文書、写真、新聞、音楽、映像 |
| Wikidata | 約30万 | 神社仏閣、スポーツ選手、音楽、人物、企業、映画、ゲーム、キャラクター |
| MADB | 約11.5万 | 漫画（25万冊）、アニメ（9千タイトル）、ゲーム（3.5万） |
| OSM (OpenStreetMap) | 約10万 | 寺社、鳥居、文化的ランドマーク |
| 国土数値情報 | 約4.4万 | 観光スポット、文化財、世界遺産、観光施設 |
| ToMuCo | 約4.1万 | 東京の博物館コレクション |
| DBpedia Japanese | 約2.3万 | 場所、人物、イベント、作品 |
| AniList | 約1.75万 | アニメ・漫画（リッチメタデータ） |
| 青空文庫 | 約1.6万 | 古典・近代日本文学 |
| ColBase | 約9千 | 国立博物館コレクション |
| NDL | 約3,700 | 古典籍、浮世絵 |

詳細は [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md) を参照。

## アーキテクチャ

```
japan_culture_mcp/
  server/
    japan_culture_mcp.py        # MCPサーバー（36ツール、v1.1.0）
    google_maps_integration.py  # Google Mapsルート生成
  ontology/
    culture_ontology.db         # SQLite DB（約3GB、1,000万以上のエンティティ）
  scripts/                      # データパイプラインスクリプト
  docs/                         # ドキュメント
  tests/                        # テストスイート
```

### データベーススキーマ

- **entities** (1,000万行以上): `id, wikidata_id, label_ja, label_en, entity_type, madb_id, ndl_id, anilist_id, dbpedia_uri, lat, lon, source, ...`
- **connections** (80万行以上): `entity_a_id, entity_b_id, connection_type, serendipity_score, explanation, ...`
- **entities_fts** (FTS5): `label_ja, label_en` の全文インデックス
- **entities_rtree** (R-Tree): `lat, lon` の空間インデックス
- **entity_tags**: 5軸オントロジータグ（テーマ、時代、媒体、地理、体験）

## 5軸オントロジー

| 軸 | 値の数 | 例 |
|----|--------|------|
| テーマ | 83 | yokai, samurai, love_bond, seasonal_beauty, nature_communion |
| 時代 | 10 | ancient, nara, heian, kamakura, muromachi, azuchi_momoyama, edo, meiji, showa, reiwa |
| 媒体 | 18 | manga, anime_tv, ukiyoe, architecture, literature, music, film |
| 地理 | 13 | 8地方 + 主要府県・都市 |
| 体験 | 9 | aesthetic, intellectual, reflective, physical, social |

## 使用例

### 意外な文化的繋がりを発見

Claudeに聞く: 「北斎と現代アニメの文化的繋がりを見つけて」

`find_serendipity` ツールが接続グラフを辿って以下のような繋がりを発見:
- 北斎の波 -> ジブリ「ポニョ」（海・波のモチーフ共有）
- 北斎の妖怪画 -> 蟲師（超自然的な自然テーマ）
- 江戸期の浮世絵 -> 進撃の巨人（劇的構図技法）

### 聖地巡礼ルート生成

Claudeに聞く: 「鎌倉のアニメ聖地巡礼ルートを作って」

`generate_pilgrimage_route` ツールがアニメのロケ地と近隣の文化スポットを組み合わせ:
- スラムダンク 踏切（鎌倉高校前駅）
- 鶴岡八幡宮
- 鎌倉大仏

### 2つの文化の比較

Claudeに聞く: 「能と歌舞伎を比較して」

`compare_cultures` ツールが共通点・固有要素を分析:
- 共通: 日本の伝統演劇、仮面、様式化された動き
- 能の特徴: 室町時代起源、禅の影響、ミニマリズム
- 歌舞伎の特徴: 江戸時代の大衆娯楽、豪華な衣装、女形

## 環境変数

| 変数 | 必須 | 説明 |
|------|------|------|
| `DB_PATH` | はい | `culture_ontology.db` へのパス |
| `GOOGLE_MAPS_API_KEY` | いいえ | Google Mapsルート生成を有効化（未設定時は地理院タイルにフォールバック） |
| `OPENAI_API_KEY` | いいえ | LLMベースの接続品質スコアリング用 |

## 開発

```bash
# テスト依存関係付きでインストール
pip install -e ".[test]"

# テストDB作成
python scripts/create_test_db.py

# テスト実行
pytest tests/ -v

# ベンチマーク実行
python scripts/benchmark.py
```

## ライセンス

MIT License. 詳細は [LICENSE](LICENSE) を参照。

データは公開された政府・コミュニティのデータベースから取得しています。各ソースのライセンスについては [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md) を参照してください。
