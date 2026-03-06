# API疎通テストレポート（再生成）

- 作成日: 2026-02-27
- 参照元: `responses/*.json`（計26ファイル）
- 疎通判定: ユーザー提示のVM実測結果を正として反映

## 1. AniList GraphQL
- 疎通結果: ✅ 成功
- エンドポイントURL: `https://graphql.anilist.co`
- 認証要件: 不要
- レスポンス形式: JSON（GraphQL）
- サンプル（最初の1レコード）: `responses/anilist_query1.json` の `data.data.Page.media[0]`

```json
{
  "id": 7592,
  "title": {
    "romaji": "Nurarihyon no Mago",
    "english": "Nura: Rise of the Yokai Clan",
    "native": "ぬらりひょんの孫"
  },
  "genres": ["Action", "Supernatural"],
  "tags": [
    {"name": "Male Protagonist", "category": "Cast-Main Cast"}
  ],
  "description": "Rikuo Nura appears to be an average middle school student...",
  "seasonYear": 2010,
  "studios": {"nodes": [{"id": 37, "name": "Studio DEEN"}]},
  "averageScore": 73,
  "popularity": 47753,
  "siteUrl": "https://anilist.co/anime/7592"
}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| `id` | `number` |
| `title` | `object` |
| `genres` | `array<string>` |
| `tags` | `array<object>` |
| `description` | `string` |
| `seasonYear` | `number` |
| `studios` | `object` |
| `averageScore` | `number` |
| `popularity` | `number` |
| `siteUrl` | `string` |

- レート制限・最大取得件数: 90 req/min（既知値）。件数は `page` / `perPage` で制御。
- 多言語対応状況: `title.romaji/english/native` で多言語タイトル取得可。
- MCP設計メモ:
  - `anilist_search_media`, `anilist_get_media_detail`, `anilist_list_tags`
  - JP系API連携用に `title.native` と `tags` を正規化して中間語彙に保存

## 2. ジャパンサーチ（Web API / SPARQL）
- 疎通結果: ❌ 失敗
- エンドポイントURL: `https://jpsearch.go.jp/api/item/search`, `https://jpsearch.go.jp/api/sparql`
- 認証要件: 不要
- レスポンス形式: JSON（item/search）, SPARQL JSON想定（ただし実測はSPARQLエラー）
- サンプル（最初の1レコード）:
  - `responses/jpsearch_item_search.json` の `data`

```json
{
  "type": "error",
  "title": "not-found",
  "timestamp": "2026/02/27 17:49:17.171 JST"
}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| `type` | `string` |
| `title` | `string` |
| `timestamp` | `string` |

- レート制限・最大取得件数: `size` 指定は可能だが、今回の実測では404/SPARQLエラーで有効データ取得不可。
- 多言語対応状況: サイト自体は日英UIあり。ただし今回のAPI疎通ではデータ確認不可。
- MCP設計メモ:
  - `jpsearch_item_search`, `jpsearch_sparql_query`
  - 現状は本番ツール化せず、再検証（正しい公開エンドポイント・クエリ仕様）を先行

## 3. MADB SPARQL
- 疎通結果: ✅ 成功
- エンドポイントURL: `https://mediaarts-db.artmuseums.go.jp/sparql`
- 認証要件: 不要
- レスポンス形式: SPARQL JSON
- サンプル（最初の1レコード）: `responses/madb_ontology_classes.json` の `data.results.bindings[0]`

```json
{
  "class": {
    "type": "uri",
    "value": "https://mediaarts-db.artmuseums.go.jp/data/class#MangaBook"
  }
}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| `class` | `object` |
| `class.type` | `string` |
| `class.value` | `string(uri)` |

- レート制限・最大取得件数: 公開明示値は未確認。`LIMIT`で制御。
- 多言語対応状況: 日本語語彙を含むデータをSPARQLで取得可能。
- MCP設計メモ:
  - `madb_list_classes`, `madb_sparql_query`, `madb_search_titles`
  - まずクラス列挙→対象クラス絞り込み→詳細取得の2段階実装が安全

## 4. NDL（次世代DL検索 / SRU / IIIF）
- 疎通結果: ⚠️ 部分成功
  - `ndl_search.json`: ok=True status=200
  - `ndl_sru.json`: ok=True status=200
  - `ndl_iiif_manifest.json`: ok=False
- エンドポイントURL: `https://lab.ndl.go.jp/dl/api/search`, `https://ndlsearch.ndl.go.jp/api/sru`
- 認証要件: 不要
- レスポンス形式: HTML（search実測）, XML（SRU実測）, IIIF manifest（今回は取得失敗）
- サンプル（最初の1レコード）:
  - `data`フィールドは対象ファイルに存在せず、抽出不可

```json
{}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| (dataフィールドなし) | - |

- レート制限・最大取得件数: `rows`, `maximumRecords` で件数制御可能。
- 多言語対応状況: 日本語クエリでの応答は確認。SRUエラーメッセージは英語。
- MCP設計メモ:
  - `ndl_search`, `ndl_sru_search`, `ndl_get_iiif_manifest`
  - SRUはクエリ構文バリデーションをMCP側で必須化（illegal query syntax対策）

## 5. ColBase
- 疎通結果: ⚠️ 部分成功
  - `colbase_home.json`: ok=True status=200
  - `colbase_search.json`: ok=True status=200
  - `colbase_via_jpsearch.json`: ok=False status=404
- エンドポイントURL: `https://colbase.nich.go.jp/`, `https://colbase.nich.go.jp/collection_items`
- 認証要件: 不要（閲覧時）
- レスポンス形式: HTML（直アクセス）, JSON（JP Search経由エラー）
- サンプル（最初の1レコード）: `responses/colbase_via_jpsearch.json` の `data`

```json
{
  "type": "error",
  "title": "not-found",
  "timestamp": "2026/02/27 17:49:17.766 JST"
}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| `type` | `string` |
| `title` | `string` |
| `timestamp` | `string` |

- レート制限・最大取得件数: 直アクセスHTMLでは公開API制限値を確認できず。
- 多言語対応状況: `locale=ja` パラメータあり。
- MCP設計メモ:
  - `colbase_open`, `colbase_search_web`
  - 公式API非公開前提で、HTML解析を行う場合は利用規約確認を必須化

## 6. 文化財総覧WebGIS（HeritageMap）
- 疎通結果: ✅ 成功
  - `heritagemap_probe_1..4.json` すべて ok=True status=200
- エンドポイントURL:
  - `https://heritagemap.nabunken.go.jp/`
  - `https://heritagemap.nabunken.go.jp/geoserver/wms?service=WMS&request=GetCapabilities`
  - `https://heritagemap.nabunken.go.jp/geoserver/wfs?service=WFS&request=GetCapabilities`
  - `https://heritagemap.nabunken.go.jp/api`
- 認証要件: 不要
- レスポンス形式: 実測はすべて `text/html`
- サンプル（最初の1レコード）:
  - `data`フィールドは対象ファイルに存在せず、抽出不可

```json
{}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| (dataフィールドなし) | - |

- レート制限・最大取得件数: 明示値未確認。
- 多言語対応状況: 日本語UI中心。
- MCP設計メモ:
  - `heritagemap_get_capabilities`, `heritagemap_search_by_bbox`
  - まずWMS/WFSの実データ取得URLを再特定し、HTML応答回避を優先

## 7. CODH
- 疎通結果: ❌ 失敗
  - `codh_char_shape.json`: ok=False（接続エラー）
  - `codh_edo_maps.json`: ok=False（接続エラー）
  - `codh_pmjt_iiif.json`: ok=False（接続エラー）
- エンドポイントURL: `https://codh.rois.ac.jp/char-shape/`, `https://codh.rois.ac.jp/edo-maps/`, `https://codh.rois.ac.jp/pmjt/iiif/`
- 認証要件: 不要
- レスポンス形式: 取得できず（connect timeout）
- サンプル（最初の1レコード）:
  - `data`フィールドなし、エラーのみ

```json
{}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| (dataフィールドなし) | - |

- レート制限・最大取得件数: 不明（疎通失敗のため確認不可）。
- 多言語対応状況: 確認不可。
- MCP設計メモ:
  - `codh_fetch_manifest`, `codh_charshape_search`
  - タイムアウト/再試行/代替ミラー設定を実装しない限り本番統合は保留

## 8. SiteReports API
- 疎通結果: ❌ 失敗
  - `sitereports_api_root.json`: ok=False status=404
  - `sitereports_api_search.json`: ok=False status=404
- エンドポイントURL: `https://sitereports.nabunken.go.jp/api`, `https://sitereports.nabunken.go.jp/api/search`
- 認証要件: 不要
- レスポンス形式: HTML（404ページ）
- サンプル（最初の1レコード）:
  - `data`フィールドなし

```json
{}
```

- フィールド一覧

| フィールド | 型 |
|---|---|
| (dataフィールドなし) | - |

- レート制限・最大取得件数: 確認不可。
- 多言語対応状況: 確認不可。
- MCP設計メモ:
  - APIルート見直し（`/api`以外の現行公開仕様の再特定）
  - 404時の自動フォールバック（サイト検索URLへの切替）を検討

## 総合所見

### 1) 動いたAPI（MCP統合に使える）
- AniList（GraphQL）
- MADB（SPARQL）
- NDL（検索/SRU）
- ColBase（サイト/検索ページ到達）
- HeritageMap（各probe 200）

### 2) 動かなかった/制限が大きいAPI
- ジャパンサーチ（item/search 404、SPARQLエラー）
- CODH（接続タイムアウト）
- SiteReports（API URL 404）

### 3) 想定外の発見
- `status=200`でもJSON/API本体ではなくSPAのHTMLシェルが返るケースが多い（NDL/HeritageMap/ColBase）。
- SRUはHTTP成功でもクエリ構文エラーを返しうる（`illegal query syntax`）。
- JP Search経由のColBase連携は404で、直アクセスと結果が分かれる。

### 4) 横断接続ポイント
- AniList `title.native` / `tags` をMADB・NDL検索語へ流用。
- MADBの`class` URIを軸に、NDLやColBaseの自由語検索をカテゴリ補強。
- 地名・時代語彙（浅草、浮世絵等）をHeritageMapとNDLで相互補完。

### 5) 次のステップへの推奨
1. 疎通判定を「HTTP成功」ではなく「期待MIME型・期待キー存在」で再定義（例: `application/json` + 必須キー）。
2. NDL/HeritageMap/ColBaseはHTML応答の先にある実APIエンドポイントを再探索して再試験。
3. JP Search / SiteReportsは最新ドキュメント基準でエンドポイントを再特定し、最小クエリで再疎通。
4. MCP実装はまずAniList+MADBを先行し、失敗系APIはフェイルソフト（リトライ・代替検索）付きで段階統合。
