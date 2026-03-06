# AniList × MADB 分類突合表

生成日: 2026-02-28

## 1. 媒体軸の対応

| 統合カテゴリ | AniList側 | MADB側 | 備考 |
|-------------|-----------|--------|------|
| TVアニメ | format=TV | AnimationTVProgram (197,665), AnimationTVRegularSeries (6,176) | AniListは作品単位、MADBはエピソード+シリーズ |
| TVアニメ（スペシャル） | format=TV_SHORT / SPECIAL | AnimationTVSpecialSeries (55) | MADBは極少数 |
| アニメ映画 | format=MOVIE | AnimationMovie (2,844), AnimationMovieSeries (2,576) | 両方とも作品レベル |
| OVA/ONA | format=OVA, format=ONA | AnimationVideoPackage (65,919) | MADBはDVD/BD単位で粒度が細かい |
| 漫画（単行本） | format=MANGA | MangaBook (394,536) | 1:1対応 |
| 漫画（連載/シリーズ） | format=MANGA | MangaBookSeries (139,130) | AniListは作品単位、MADBはシリーズ |
| 漫画（雑誌掲載） | — | MangaMagazineIssue (180,670), MangaMagazinePublication (30,023) | AniListには雑誌レベルのデータなし |
| 漫画雑誌 | — | MangaMagazine (5,753) | AniListにはなし |
| ライトノベル | format=NOVEL | — | MADBにはライトノベルカテゴリなし |
| ワンショット | format=ONE_SHOT | MangaOther (9,286) | 部分的対応 |
| ゲーム | — | GamePackage (51,656), GameWork (5,083), GameVariation (8,594) | AniListにはゲームデータなし |
| メディアアート | — | MediaArtExhibitionOrPerformance (20,845), MediaArtEvent (10,166) | AniListにはなし |
| 音楽 | format=MUSIC | — | MADBにはなし（ただしanime側にtrack情報あり） |

### 媒体軸の粒度比較

- **AniList**: 作品（Work）レベル。1作品=1エントリ。format で TV/MOVIE/MANGA/NOVEL/OVA/ONA/SPECIAL/MUSIC/ONE_SHOT を区別
- **MADB**: エピソード・パッケージレベル。1作品が複数エントリに展開（TV番組197K件はエピソード単位）。シリーズ→エピソード→パッケージの3層構造

## 2. テーマ軸の対応

| AniList タグ/ジャンル | MADB で検索可能か | 接続方法 | 独自テーマ軸分類 |
|---------------------|------------------|---------|----------------|
| Youkai (Theme-Fantasy) | ✅ キーワード「妖怪」 | keyword match | supernatural/yokai |
| Isekai (Theme-Fantasy) | ✅ キーワード「異世界」 | keyword match | otherworld/isekai |
| Historical (Setting-Time) | △ datePublishedで間接 | date range | era axis |
| Samurai (Cast-Traits) | ✅ キーワード「侍」「武士」 | keyword match | community_tradition |
| Ninja (Cast-Traits) | ✅ キーワード「忍者」 | keyword match | community_tradition |
| Kabuki (Theme-Arts) | ✅ キーワード「歌舞伎」 | keyword match | craft_mastery |
| Rakugo (Theme-Arts) | ✅ キーワード「落語」 | keyword match | craft_mastery |
| Shogi (Theme-Game-Card) | ✅ キーワード「将棋」 | keyword match | craft_mastery |
| Go (Theme-Game-Card) | ✅ キーワード「囲碁」 | keyword match | craft_mastery |
| Sumo (Theme-Game-Sport) | ✅ キーワード「相撲」 | keyword match | community_tradition |
| Judo (Theme-Game-Sport) | ✅ キーワード「柔道」 | keyword match | craft_mastery |
| Calligraphy (Theme-Arts) | ✅ キーワード「書道」 | keyword match | craft_mastery |
| Martial Arts (Theme-Action) | ✅ キーワード「武術」「格闘」 | keyword match | craft_mastery |
| Swordplay (Theme-Action) | ✅ キーワード「剣」 | keyword match | war_conflict |
| Food (Theme-Arts) | ✅ キーワード「料理」「グルメ」 | keyword match | everyday_beauty |
| Magic (Theme-Fantasy) | ✅ キーワード「魔法」 | keyword match | supernatural |
| Mythology (Theme-Fantasy) | △ キーワード「神話」 | keyword match | sacred_profane |
| Romance (genre) | △ キーワード「恋愛」 | keyword match | love_bond |
| Horror (genre) | ✅ キーワード「ホラー」 | keyword match | death_rebirth |
| Mecha (genre) | ✅ キーワード「ロボット」「メカ」 | keyword match | テーマ |
| Cyberpunk (Theme-Sci-Fi) | △ キーワードのみ | keyword match | テーマ |
| Time Loop (Theme-Sci-Fi) | ✅ キーワード「タイムループ」 | keyword match | transformation |
| War (Theme-Other) | ✅ キーワード「戦争」 | keyword match | war_conflict |
| Reincarnation (Theme-Other) | ✅ キーワード「転生」 | keyword match | death_rebirth |
| Coming of Age (Theme-Drama) | △ キーワード限定 | keyword match | identity_self |
| Revenge (Theme-Drama) | ✅ キーワード「復讐」 | keyword match | power_rebellion |
| Survival (Theme-Other) | ✅ キーワード「サバイバル」 | keyword match | death_rebirth |
| Iyashikei (Theme-Slice of Life) | △ AniList独自概念 | — | everyday_beauty |

### テーマ接続の方法論

1. **キーワードマッチ（✅）**: MADBのrdfs:label/schema:nameに対するFILTER(CONTAINS())で直接検索可能
2. **間接マッチ（△）**: datePublished等の属性から間接的に推定可能、またはキーワード選定に工夫が必要
3. **Wikidata経由**: AniListもMADBもWikidata IDを保有する場合、Wikidata P4082（MADB ID）経由で接続可能
4. **不可（—）**: 直接的な接続方法なし。LLMタグ付けが必要

## 3. 接続不可能な領域（ギャップ）

### AniListにあってMADBにないもの

| カテゴリ | 例 | 理由 |
|---------|------|------|
| ライトノベル | format=NOVEL | MADBにLNカテゴリなし |
| 音楽作品 | format=MUSIC | MADBは映像/漫画/ゲーム特化 |
| テーマタグ全般 | 417タグ | MADBにはテーマ分類体系がない |
| ジャンル分類 | 19ジャンル | MADBのgenreは媒体分類 |
| 人気スコア | popularity, averageScore | MADBに定量評価なし |
| 制作スタジオ | studios | MADBのcreatorは個人名中心 |
| 視聴シーズン | season, seasonYear | MADBはdatePublishedのみ |

### MADBにあってAniListにないもの

| カテゴリ | 例 | 理由 |
|---------|------|------|
| ゲーム | GamePackage (51K), GameWork (5K) | AniListはアニメ/漫画特化 |
| メディアアート | MediaArtExhibitionOrPerformance (20K) | AniListの対象外 |
| 漫画雑誌 | MangaMagazine, MangaMagazineIssue | AniListは作品レベルのみ |
| エピソード粒度 | AnimationTVProgram (197K個別エピソード) | AniListは作品単位 |
| 出版メタデータ | isbn, ndc, jpno, size, numberOfPages | AniListにはなし |
| 人物/組織DB | Agent (71K) | AniListのスタジオは限定的 |
| 価格情報 | price | AniListにはなし |

### 接続にWikidataが必要なもの

| 接続パターン | 説明 |
|-------------|------|
| AniList作品 ↔ MADB作品 | 表記揺れ解消（「鬼滅の刃」vs「きめつのやいば」）にWikidata P4082が有効 |
| 作品 ↔ 歴史的背景 | アニメの舞台となった場所・時代をWikidata座標/時代データで補完 |
| 作者 ↔ NDL典拠 | Wikidata P349でNDL典拠IDを取得し、著者の書誌情報を接続 |
| 文化財 ↔ 作品 | 浮世絵等の文化財（Wikidata）→ モチーフとなったアニメ（AniList） |

## 4. 5軸別カバレッジ評価

| 軸 | AniList | MADB | Wikidata | NDL | JapanSearch | 統合評価 |
|-----|---------|------|----------|-----|-------------|---------|
| テーマ | ◎ 417タグ+19ジャンル | × キーワードのみ | △ カテゴリ | △ NDC | △ provider | AniListが圧倒的 |
| 時代 | △ 7タグ | △ datePublished | ◎ 生年/没年/時代 | ○ 出版年 | ○ 時代分類 | Wikidata中心 |
| 媒体 | ○ format(9種) | ◎ 24クラス階層 | △ instance of | △ 資料種別 | △ type | MADB中心 |
| 地理 | × なし | △ location(出版地) | ◎ P625座標 | × なし | △ provider地域 | Wikidata中心 |
| 体験 | △ Setting-Scene | × なし | × なし | × なし | × なし | LLM推定が必要 |

### 判定基準
- ◎ = 体系的な分類体系を保有
- ○ = 実用的なデータあり
- △ = 間接的・部分的なデータのみ
- × = 該当データなし

## 5. 統合設計方針

1. **テーマ軸**: AniListタグ体系を基盤とし、独自大テーマ（15個）にマッピング。MADBはキーワード検索で補完
2. **時代軸**: Wikidataの年代データ + MADBのdatePublished + 独自9区分の時代区分表
3. **媒体軸**: MADBクラス階層を基盤とし、AniListのformatと統合。伝統文化（絵画・彫刻・工芸等）は独自追加
4. **地理軸**: Wikidataの座標データを基盤。地方→都道府県→市区町村→スポットの階層構造
5. **体験モード軸**: 既存ソースにはデータなし。LLMタグ付けまたはルールベースで推定
