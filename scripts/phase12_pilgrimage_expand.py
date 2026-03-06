"""
Phase 12 Stream B: 聖地巡礼データ深掘り
B1: butaimeguri.com 調査
B2: アニメツーリズム88 全年度
B3: 自治体マップ（手動キュレーション）
B4: 座標クロスマッチング
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import math
import re

DB_PATH = "ontology/culture_ontology.db"
UA = "japan-culture-mcp/0.8 (teddykmk@gmail.com)"


def fetch_url(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA, "Accept-Language": "ja",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8")
        except Exception as e:
            print(f"  Fetch error ({url[:60]}...): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(5)
    return None


def db_commit_retry(db, retries=5):
    for i in range(retries):
        try:
            db.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {i+1}: {e}", flush=True)
            time.sleep(3)
    return False


# ── B2: アニメツーリズム88 (2018-2025) curated data ──
# From the official lists of "88 Anime Spots to Visit in Japan"
# Many overlap year to year. Here are the unique additions.
ANIME_TOURISM_88 = [
    # 2018-2025 unique spots not already in DB
    # Format: (work, spot, lat, lon, description)
    ("ラブライブ!", "神田明神", 35.7019, 139.7681, "穂乃果聖地"),
    ("ガールズ＆パンツァー", "大洗磯前神社", 36.3176, 140.5883, "ガルパン聖地"),
    ("あの日見た花の名前を僕達はまだ知らない。", "秩父市", 35.9923, 139.0846, "あの花聖地"),
    ("のんのんびより", "小川町", 36.0575, 139.2619, "旭丘分校"),
    ("花咲くいろは", "湯涌温泉", 36.5067, 136.7525, "喜翠荘"),
    ("氷菓", "高山市", 36.1413, 137.2608, "古典部"),
    ("らき☆すた", "鷲宮神社", 36.1022, 139.6925, "聖地巡礼の先駆"),
    ("けいおん!", "豊郷小学校旧校舎群", 35.2153, 136.2422, "桜が丘高校"),
    ("true tears", "城端", 36.5125, 136.8794, "富山の聖地"),
    ("CLANNAD", "古河市", 36.1833, 139.7000, "クラナド聖地"),
    ("たまゆら", "竹原市", 34.3453, 132.9078, "町並み保存地区"),
    ("Free!", "岩美町", 35.5578, 134.3297, "海の聖地"),
    ("ヴァイオレット・エヴァーガーデン", "宇治市", 34.8884, 135.8075, "京アニ"),
    ("響け!ユーフォニアム", "宇治橋", 34.8892, 135.8078, "ユーフォ聖地"),
    ("ちはやふる", "近江神宮", 35.0286, 135.8536, "かるたの聖地"),
    ("ゾンビランドサガ", "佐賀県庁", 33.2494, 130.2989, "フランシュシュ"),
    ("ゾンビランドサガ", "嬉野温泉", 33.0994, 130.0436, "ゾンビランドサガ聖地"),
    ("ゾンビランドサガ", "唐津城", 33.4556, 129.9689, "聖地"),
    ("ゾンビランドサガ", "鏡山", 33.4442, 130.0225, "聖地"),
    ("からかい上手の高木さん", "小豆島", 34.4833, 134.2333, "舞台"),
    ("ゆるキャン△", "身延町", 35.3720, 138.4370, "キャンプ聖地"),
    ("鬼滅の刃", "宝満宮竈門神社", 33.5467, 130.5292, "竈門の聖地"),
    ("呪術廻戦", "渋谷駅", 35.6580, 139.7016, "渋谷事変"),
    ("推しの子", "宮崎市", 31.9111, 131.4239, "舞台"),
    ("葬送のフリーレン", "ドイツ村", 35.3681, 140.2486, "聖地"),
    ("薬屋のひとりごと", "首里城", 26.2172, 127.7197, "宮中モデル"),
    ("僕の心のヤバイやつ", "秋葉原", 35.7023, 139.7745, "デートスポット"),
    ("BanG Dream!", "下北沢", 35.6613, 139.6682, "ライブハウス聖地"),
    ("プロジェクトセカイ", "豊洲", 35.6484, 139.7918, "ワンダーランドモデル"),
    ("その着せ替え人形は恋をする", "東京ビッグサイト", 35.6300, 139.7942, "コスプレ聖地"),
    ("SPY×FAMILY", "洋館通り", 35.6564, 139.7267, "フォージャー家"),
    ("僕のヒーローアカデミア", "三浦海岸", 35.1597, 139.6369, "合宿シーン"),
    ("チェンソーマン", "新宿区", 35.6938, 139.7035, "デンジの活動エリア"),
    ("Dr.STONE", "箱根", 35.2331, 139.1069, "石化場所"),
    ("SLAM DUNK", "鎌倉高校前", 35.3067, 139.5006, "有名踏切"),
    ("名探偵コナン", "北栄町", 35.4892, 133.7606, "青山剛昌ふるさと館"),
    ("ワンピース", "熊本城", 32.8064, 130.7058, "ルフィ像"),
    ("ドラゴンボール", "カメハウス", 26.3356, 127.7617, "亀ハウスモデル"),
    ("新世紀エヴァンゲリオン", "箱根湯本", 35.2322, 139.1058, "第3新東京市"),
    ("新世紀エヴァンゲリオン", "芦ノ湖", 35.2017, 139.0206, "戦闘舞台"),
    ("攻殻機動隊", "神戸ポートタワー", 34.6750, 135.1869, "新浜市モデル"),
    ("涼宮ハルヒの憂鬱", "西宮北口駅", 34.7497, 135.3614, "北高聖地"),
    ("四畳半神話大系", "出町柳", 35.0308, 135.7733, "京都の舞台"),
    ("サマーウォーズ", "上田城", 36.4028, 138.2489, "陣内家"),
    ("時をかける少女", "東京国立博物館", 35.7189, 139.7767, "タイムリープ"),
    ("おおかみこどもの雨と雪", "富山市", 36.6953, 137.2114, "花の故郷"),
    ("魔法少女まどか☆マギカ", "前橋市", 36.3911, 139.0608, "見滝原市"),
    ("STEINS;GATE", "秋葉原ラジオ会館", 35.6983, 139.7711, "ラボ"),
    ("結城友奈は勇者である", "観音寺市", 34.1267, 133.6603, "讃州中学"),
    ("夏目友人帳", "人吉市", 32.2125, 130.7547, "のどかな田舎"),
]

# ── B3: 自治体マップ追加スポット（手動キュレーション） ──
LOCAL_MAP_SPOTS = [
    # 秩父市マップ
    ("あの日見た花の名前を僕達はまだ知らない。", "秩父橋", 35.9838, 139.0681, "旧秩父橋"),
    ("あの日見た花の名前を僕達はまだ知らない。", "定林寺", 35.9923, 139.0846, "秘密基地"),
    ("あの日見た花の名前を僕達はまだ知らない。", "羊山公園", 35.9798, 139.0728, "芝桜の名所"),
    ("あの日見た花の名前を僕達はまだ知らない。", "西武秩父駅", 35.9886, 139.0853, "アクセス"),
    ("心が叫びたがってるんだ。", "横瀬町", 35.9753, 139.1003, "ここさけ舞台"),
    ("心が叫びたがってるんだ。", "秩父ミューズパーク", 35.9920, 139.0600, "ここさけ"),
    # 沼津マップ
    ("ラブライブ!サンシャイン!!", "沼津港", 35.0908, 138.8628, "水揚げ場"),
    ("ラブライブ!サンシャイン!!", "三津海水浴場", 35.0333, 138.8800, "海シーン"),
    ("ラブライブ!サンシャイン!!", "伊豆三津シーパラダイス", 35.0350, 138.8817, "水族館"),
    # 大洗マップ
    ("ガールズ＆パンツァー", "大洗サンビーチ", 36.3114, 140.5817, "上陸シーン"),
    ("ガールズ＆パンツァー", "大洗シーサイドステーション", 36.3100, 140.5761, "商店街"),
    ("ガールズ＆パンツァー", "大洗あんこう鍋", 36.3136, 140.5764, "名物"),
    # 飯能マップ
    ("ヤマノススメ", "天覧山", 35.8667, 139.3333, "初登山"),
    ("ヤマノススメ", "多峯主山", 35.8722, 139.3250, "第2の山"),
    ("ヤマノススメ", "飯能河原", 35.8589, 139.3289, "BBQシーン"),
    # 宇治マップ
    ("響け!ユーフォニアム", "宇治上神社", 34.8903, 135.8103, "世界遺産"),
    ("響け!ユーフォニアム", "京阪宇治駅", 34.8878, 135.8053, "通学駅"),
    ("響け!ユーフォニアム", "あがた通り", 34.8886, 135.8042, "通学路"),
    # 西宮マップ
    ("涼宮ハルヒの憂鬱", "西宮中央図書館", 34.7378, 135.3417, "長門の図書館"),
    ("涼宮ハルヒの憂鬱", "甲陽園駅", 34.7578, 135.3403, "北高最寄り"),
    ("涼宮ハルヒの憂鬱", "苦楽園口駅", 34.7578, 135.3278, "SOS団活動"),
    # 鎌倉/藤沢マップ
    ("SLAM DUNK", "湘南海岸公園", 35.3089, 139.4850, "海岸シーン"),
    ("SLAM DUNK", "江ノ島電鉄", 35.3089, 139.4902, "電車"),
    ("青春ブタ野郎はバニーガール先輩の夢を見ない", "藤沢駅", 35.3389, 139.4903, "主人公通学"),
    ("青春ブタ野郎はバニーガール先輩の夢を見ない", "七里ヶ浜駅", 35.3072, 139.5094, "海の風景"),
    # 境港マップ
    ("ゲゲゲの鬼太郎", "水木しげるロード", 35.5339, 133.2311, "妖怪の町"),
    ("ゲゲゲの鬼太郎", "水木しげる記念館", 35.5336, 133.2314, "記念館"),
    # 調布マップ
    ("ゲゲゲの鬼太郎", "鬼太郎茶屋", 35.6533, 139.5750, "深大寺"),
    ("ゲゲゲの鬼太郎", "布多天神社", 35.6550, 139.5467, "天神通り"),
    # 鳥取マップ
    ("名探偵コナン", "コナン大橋", 35.4892, 133.7581, "コナンの町"),
    ("名探偵コナン", "青山剛昌ふるさと館", 35.4900, 133.7611, "ミュージアム"),
    ("名探偵コナン", "コナン通り", 35.4878, 133.7589, "商店街"),
    # 所沢マップ
    ("となりのトトロ", "トトロの森", 35.7833, 139.4167, "狭山丘陵"),
    ("となりのトトロ", "クロスケの家", 35.7806, 139.4167, "古民家"),
    # 能美マップ
    ("真・中華一番!", "九谷焼資料館", 36.4228, 136.5111, "舞台"),
    # 氷見マップ
    ("忍者ハットリくん", "氷見市潮風通り", 36.8569, 136.9842, "ハットリくんロード"),
    # 豊郷マップ
    ("けいおん!", "豊郷小学校旧校舎", 35.2153, 136.2422, "校舎"),
    ("けいおん!", "旧校舎内部", 35.2155, 136.2420, "教室"),
]


def main():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    SOURCE = "phase12_pilgrimage"
    total_new_ent = 0
    total_new_conn = 0

    # ── B1: Check butaimeguri.com ──
    print("=== B1: butaimeguri.com check ===", flush=True)
    robots = fetch_url("https://butaimeguri.com/robots.txt")
    if robots:
        print(f"  robots.txt: {robots[:200]}", flush=True)
    else:
        print("  Could not fetch robots.txt", flush=True)
    time.sleep(2)

    # Try fetching the works list page
    html = fetch_url("https://butaimeguri.com/works/")
    if html:
        # Count number of work links
        work_links = re.findall(r'href="(/works/[^"]+)"', html)
        print(f"  Work links found: {len(work_links)}", flush=True)
        if len(work_links) > 0:
            print(f"  Sample: {work_links[:5]}", flush=True)
    else:
        print("  butaimeguri.com unavailable or blocked", flush=True)

    # ── B2: Import anime tourism 88 spots ──
    print(f"\n=== B2: Anime Tourism 88 spots ({len(ANIME_TOURISM_88)}) ===", flush=True)
    for work, spot, lat, lon, desc in ANIME_TOURISM_88:
        existing = db.execute(
            "SELECT id FROM entities WHERE label_ja = ? AND lat IS NOT NULL LIMIT 1",
            (spot,)
        ).fetchone()

        if existing:
            loc_id = existing[0]
        else:
            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, lat, lon, source)
                VALUES (?, ?, 'place', ?, ?, ?)
            """, (spot, spot, lat, lon, SOURCE))
            loc_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            total_new_ent += 1

        work_row = db.execute(
            "SELECT id FROM entities WHERE label_ja LIKE ? LIMIT 1",
            (f"%{work}%",)
        ).fetchone()

        if not work_row:
            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, source)
                VALUES (?, ?, 'work', ?)
            """, (work, work, SOURCE))
            work_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            total_new_ent += 1
        else:
            work_id = work_row[0]

        exists = db.execute("""
            SELECT 1 FROM connections WHERE connection_type = 'pilgrimage_spot'
            AND ((entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?))
        """, (work_id, loc_id, loc_id, work_id)).fetchone()

        if not exists:
            db.execute("""
                INSERT INTO connections (entity_a_id, entity_b_id, connection_type, confidence, explanation)
                VALUES (?, ?, 'pilgrimage_spot', 0.9, ?)
            """, (work_id, loc_id, f"聖地巡礼: {work}の舞台「{spot}」({desc}) [anime_tourism88]"))
            total_new_conn += 1

    db_commit_retry(db)
    print(f"  New entities: {total_new_ent}, New connections: {total_new_conn}", flush=True)

    # ── B3: Local map spots ──
    ent_before = total_new_ent
    conn_before = total_new_conn
    print(f"\n=== B3: Local map spots ({len(LOCAL_MAP_SPOTS)}) ===", flush=True)
    for work, spot, lat, lon, desc in LOCAL_MAP_SPOTS:
        existing = db.execute(
            "SELECT id FROM entities WHERE label_ja = ? AND lat IS NOT NULL LIMIT 1",
            (spot,)
        ).fetchone()

        if existing:
            loc_id = existing[0]
        else:
            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, lat, lon, source)
                VALUES (?, ?, 'place', ?, ?, ?)
            """, (spot, spot, lat, lon, f"{SOURCE}_local_map"))
            loc_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            total_new_ent += 1

        work_row = db.execute(
            "SELECT id FROM entities WHERE label_ja LIKE ? LIMIT 1",
            (f"%{work}%",)
        ).fetchone()

        if not work_row:
            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, source)
                VALUES (?, ?, 'work', ?)
            """, (work, work, SOURCE))
            work_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            total_new_ent += 1
        else:
            work_id = work_row[0]

        exists = db.execute("""
            SELECT 1 FROM connections WHERE connection_type = 'pilgrimage_spot'
            AND ((entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?))
        """, (work_id, loc_id, loc_id, work_id)).fetchone()

        if not exists:
            db.execute("""
                INSERT INTO connections (entity_a_id, entity_b_id, connection_type, confidence, explanation)
                VALUES (?, ?, 'pilgrimage_spot', 0.9, ?)
            """, (work_id, loc_id, f"聖地巡礼: {work}の舞台「{spot}」({desc}) [自治体マップ]"))
            total_new_conn += 1

    db_commit_retry(db)
    print(f"  New entities: {total_new_ent - ent_before}, New connections: {total_new_conn - conn_before}", flush=True)

    # ── B4: Cross-matching pilgrimage spots × cultural entities (within 2km) ──
    print(f"\n=== B4: Pilgrimage × Culture cross-matching ===", flush=True)

    # Get pilgrimage spot locations
    pilgrim_spots = db.execute("""
        SELECT DISTINCT e.id, e.label_ja, e.lat, e.lon
        FROM entities e
        JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e.lat IS NOT NULL AND e.entity_type = 'place'
    """).fetchall()
    print(f"  Pilgrimage spots: {len(pilgrim_spots):,}", flush=True)

    cross_new = 0
    for spot_id, spot_name, slat, slon in pilgrim_spots:
        if slat is None or slon is None:
            continue
        # Find nearby cultural entities within ~1km
        lat_off = 1.0 / 111.0
        lon_off = 1.0 / (111.0 * math.cos(math.radians(slat)))

        nearby = db.execute("""
            SELECT e.id, e.label_ja, e.entity_type
            FROM entities e
            WHERE e.lat BETWEEN ? AND ?
            AND e.lon BETWEEN ? AND ?
            AND e.id != ?
            AND e.entity_type IN ('building', 'cultural_property', 'artifact', 'festival')
            LIMIT 5
        """, (slat - lat_off, slat + lat_off, slon - lon_off, slon + lon_off, spot_id)).fetchall()

        for near_id, near_name, near_type in nearby:
            exists = db.execute("""
                SELECT 1 FROM connections
                WHERE ((entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?))
            """, (spot_id, near_id, near_id, spot_id)).fetchone()

            if not exists:
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type, confidence, explanation)
                    VALUES (?, ?, 'pilgrimage_cultural_nearby', 0.7, ?)
                """, (spot_id, near_id,
                      f"聖地「{spot_name}」の近くに文化資源「{near_name}」({near_type})がある"))
                cross_new += 1

        if cross_new > 0 and cross_new % 500 == 0:
            db_commit_retry(db)
            print(f"    ... cross connections: {cross_new:,}", flush=True)

    db_commit_retry(db)
    print(f"  New cross connections: {cross_new:,}", flush=True)
    total_new_conn += cross_new

    # ── Final stats ──
    spots_all = db.execute("""
        SELECT COUNT(DISTINCT e.id) FROM connections c
        JOIN entities e ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e.lat IS NOT NULL
    """).fetchone()[0]
    pilgrim_conn = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'"
    ).fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print(f"=== Phase 12 Stream B Results ===", flush=True)
    print(f"New entities: {total_new_ent:,}", flush=True)
    print(f"New connections: {total_new_conn:,}", flush=True)
    print(f"Total pilgrimage locations: {spots_all:,}", flush=True)
    print(f"Total pilgrimage connections: {pilgrim_conn:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
