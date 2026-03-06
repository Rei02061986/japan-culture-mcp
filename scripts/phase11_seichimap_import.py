"""
Phase 11 Stream B-2: Import seichimap.jp + geocode addresses via Nominatim
120 spots from 8 works, addresses need geocoding to lat/lon
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import re

DB_PATH = "ontology/culture_ontology.db"
SOURCE = "seichimap.jp"

# All 120 spots from seichimap.jp scraping (8 works)
SEICHIMAP_SPOTS = [
    # ── 君の名は。 (14 spots) ──
    ("君の名は。", "諏訪湖・立石公園", "長野県諏訪市上諏訪10399", "糸守湖のモデル"),
    ("君の名は。", "松原湖", "長野県南佐久郡小海町豊里松原", "糸守湖の初期イメージモデル"),
    ("君の名は。", "飛騨古川駅", "岐阜県飛騨市古川町金森町8", "瀧が訪れた駅"),
    ("君の名は。", "気多若宮神社", "岐阜県飛騨市古川町上気多1297", "宮水神社のモデル"),
    ("君の名は。", "飛騨山王宮日枝神社", "岐阜県高山市城山156", "宮水神社の参道モデル"),
    ("君の名は。", "飛騨市図書館", "岐阜県飛騨市古川町本町2-22", "瀧が情報を調べた図書館"),
    ("君の名は。", "飛騨市宮川町落合バス停", "岐阜県飛騨市宮川町落合", "三葉がバスを待つシーン"),
    ("君の名は。", "新宿駅", "東京都新宿区新宿3丁目38-1", "瀧の日常の象徴"),
    ("君の名は。", "バスタ新宿", "東京都渋谷区千駄ヶ谷5丁目24-55", "高速バスターミナル"),
    ("君の名は。", "新宿歌舞伎町交差点", "東京都新宿区歌舞伎町1丁目", "東京の喧騒シーン"),
    ("君の名は。", "国立新美術館", "東京都港区六本木7丁目22-2", "デートシーン"),
    ("君の名は。", "須賀神社", "東京都新宿区須賀町5-6", "クライマックス再会の舞台"),
    ("君の名は。", "前田南駅", "秋田県北秋田市", "糸守駅のモデル"),
    ("君の名は。", "新宿警察署前", "東京都新宿区西新宿6丁目1-1", "歩くシーン"),

    # ── すずめの戸締まり (19 spots) ──
    ("すずめの戸締まり", "油津港", "宮崎県日南市油津4丁目12-16", "出会いの場所"),
    ("すずめの戸締まり", "湯の鶴温泉", "熊本県水俣市湯出", "廃墟温泉街モデル"),
    ("すずめの戸締まり", "杖立温泉", "熊本県阿蘇郡小国町下城", "廃墟温泉街モデル"),
    ("すずめの戸締まり", "湯平温泉", "大分県由布市湯布院町湯平", "石畳の温泉街モデル"),
    ("すずめの戸締まり", "旧豊後森機関庫", "大分県玖珠郡玖珠町岩室36-15", "後ろ戸モデル"),
    ("すずめの戸締まり", "佐賀関港", "大分県大分市佐賀関750-69", "フェリー乗船シーン"),
    ("すずめの戸締まり", "八幡浜港", "愛媛県八幡浜市出島1581-26", "四国上陸"),
    ("すずめの戸締まり", "JR伊予大洲駅", "愛媛県大洲市中村119", "駅舎"),
    ("すずめの戸締まり", "JR下灘駅", "愛媛県伊予市双海町大久保", "海に近い駅"),
    ("すずめの戸締まり", "大鳴門橋", "徳島県鳴門市鳴門町土佐泊浦", "移動ルート"),
    ("すずめの戸締まり", "明石海峡大橋", "兵庫県神戸市垂水区東舞子町2051", "ダイジンが走るシーン"),
    ("すずめの戸締まり", "東山商店街", "兵庫県神戸市兵庫区東山町2丁目3-20", "スナックモデル"),
    ("すずめの戸締まり", "鷲羽山ハイランド", "岡山県倉敷市下津井吹上303-1", "遊園地モデル"),
    ("すずめの戸締まり", "神戸おとぎの国", "兵庫県神戸市北区大沢町上大沢2150", "廃墟遊園地モデル"),
    ("すずめの戸締まり", "新神戸駅", "兵庫県神戸市中央区加納町1丁目3-1", "別れの場所"),
    ("すずめの戸締まり", "御茶ノ水駅", "東京都千代田区神田駿河台2丁目6", "東京編"),
    ("すずめの戸締まり", "道の駅大谷海岸", "宮城県気仙沼市本吉町三島9", "震災復興"),
    ("すずめの戸締まり", "織笠駅", "岩手県下閉伊郡山田町織笠", "クライマックス"),
    ("すずめの戸締まり", "二宮商店街", "兵庫県神戸市中央区琴ノ緒町4丁目6-12", "スナックモデル"),

    # ── ゆるキャン△ (22 spots) ──
    ("ゆるキャン△", "浩庵キャンプ場", "山梨県南巨摩郡身延町中ノ倉2926", "第1話の舞台"),
    ("ゆるキャン△", "内船駅", "山梨県南巨摩郡南部町内船", "なでしこ下車駅"),
    ("ゆるキャン△", "身延駅", "山梨県南巨摩郡身延町角打537", "野クルゆかりの駅"),
    ("ゆるキャン△", "旧下部小学校跡", "山梨県南巨摩郡身延町常葉1495", "本栖高校モデル"),
    ("ゆるキャン△", "栄昇堂", "山梨県南巨摩郡身延町角打3024", "みのぶまんじゅう"),
    ("ゆるキャン△", "セルバみのぶ店", "山梨県南巨摩郡身延町飯富2309-200", "買い出しシーン"),
    ("ゆるキャン△", "四尾連湖", "山梨県西八代郡市川三郷町山家四尾連", "ソロキャンプ"),
    ("ゆるキャン△", "笛吹川フルーツ公園", "山梨県山梨市江曽原1488", "夜景スポット"),
    ("ゆるキャン△", "ほったらかし温泉", "山梨県山梨市矢坪1669-18", "露天風呂"),
    ("ゆるキャン△", "夜叉神峠", "山梨県南アルプス市芦安芦倉", "ソロツーリング"),
    ("ゆるキャン△", "ふもとっぱら", "静岡県富士宮市麓156", "絶景キャンプ場"),
    ("ゆるキャン△", "道の駅朝霧高原", "静岡県富士宮市根原492-14", "ソフトクリーム"),
    ("ゆるキャン△", "まかいの牧場", "静岡県富士宮市内野1327-1", "クリスマス準備"),
    ("ゆるキャン△", "渚園キャンプ場", "静岡県浜松市西区舞阪町弁天島5005-1", "デュオキャンプ"),
    ("ゆるキャン△", "弁天島海浜公園", "静岡県浜松市西区舞阪町弁天島3775-2", "浜名湖シンボル"),
    ("ゆるキャン△", "浜名湖佐久米駅", "静岡県浜松市北区三ヶ日町佐久米725-9", "ユリカモメ"),
    ("ゆるキャン△", "高ボッチ高原", "長野県塩尻市片丘", "富士山展望"),
    ("ゆるキャン△", "霧ヶ峰高原", "長野県諏訪市霧ヶ峰", "ビーナスライン"),
    ("ゆるキャン△", "杖突峠", "長野県茅野市宮川", "展望地"),
    ("ゆるキャン△", "光前寺", "長野県駒ヶ根市赤穂29", "苔庭と三重塔"),
    ("ゆるキャン△", "こまくさの湯", "長野県駒ヶ根市赤穂759-4", "温泉施設"),
    ("ゆるキャン△", "陣馬形山キャンプ場", "長野県上伊那郡中川村大草1636", "クライマックス聖地"),

    # ── SLAM DUNK (12 spots) ──
    ("SLAM DUNK", "鎌倉高校前駅踏切", "神奈川県鎌倉市腰越1丁目1-25", "OPシーン"),
    ("SLAM DUNK", "鎌倉海浜公園坂ノ下", "神奈川県鎌倉市坂ノ下34-5", "流川楓自転車シーン"),
    ("SLAM DUNK", "秋葉台文化体育館", "神奈川県藤沢市遠藤2000-1", "海南戦の舞台"),
    ("SLAM DUNK", "平塚総合体育館", "神奈川県平塚市大原1-1", "決勝リーグ会場"),
    ("SLAM DUNK", "鵠沼海岸", "神奈川県藤沢市片瀬海岸3丁目", "EDシーン"),
    ("SLAM DUNK", "辻堂海岸", "神奈川県藤沢市辻堂西海岸", "映画ラストシーン"),
    ("SLAM DUNK", "広島経済大学石田記念体育館", "広島県広島市安佐南区祇園5丁目37-1", "山王戦モデル"),
    ("SLAM DUNK", "湯来温泉みどり荘", "広島県広島市佐伯区湯来町多田2661", "旅館モデル"),
    ("SLAM DUNK", "お好み村かずちゃん", "広島県広島市中区新天地5-13", "安西先生の食事シーン"),
    ("SLAM DUNK", "桃原西公園", "沖縄県中頭郡北谷町字桃原2-1", "リョータ幼少期"),
    ("SLAM DUNK", "渡具知ビーチ", "沖縄県中頭郡読谷村渡具知228", "秘密基地"),
    ("SLAM DUNK", "森子大物忌神社", "秋田県由利本荘市森子八乙女下99", "沢北参拝シーン"),

    # ── 聲の形 (12 spots) ──
    ("聲の形", "大垣公園", "岐阜県大垣市郭町2丁目53", "マリア遊具シーン"),
    ("聲の形", "新大橋", "岐阜県大垣市栗屋町", "硝子の告白シーン"),
    ("聲の形", "JR大垣駅", "岐阜県大垣市高屋町1丁目145", "待ち合わせ"),
    ("聲の形", "四季の広場", "岐阜県大垣市馬場町159", "象徴的スポット"),
    ("聲の形", "美登鯉橋", "岐阜県大垣市西外側町2丁目46", "鯉の餌シーン"),
    ("聲の形", "滝のトンネル", "岐阜県大垣市馬場町159", "ハトのシーン"),
    ("聲の形", "大垣駅通り", "岐阜県大垣市高屋町1丁目", "植野との再会"),
    ("聲の形", "総合福祉会館", "岐阜県大垣市馬場町124", "手話教室"),
    ("聲の形", "青柳橋", "岐阜県大垣市青柳町", "度胸試し"),
    ("聲の形", "養老駅", "岐阜県養老郡養老町鷲巣白石道1200", "木造駅舎"),
    ("聲の形", "養老の滝", "岐阜県養老郡養老町高林1298-2", "日本の滝百選"),
    ("聲の形", "金町8丁目交差点", "岐阜県岐阜市金町8丁目", "植野ビラ配り"),

    # ── 小市民シリーズ (18 spots) ──
    ("小市民シリーズ", "岐阜北高校", "岐阜県岐阜市則武清水1841-11", "船戸高校モデル"),
    ("小市民シリーズ", "忠節橋", "岐阜県岐阜市忠節町", "通学路"),
    ("小市民シリーズ", "AND LADY", "岐阜県岐阜市常盤町20", "Aliceカフェモデル"),
    ("小市民シリーズ", "セブンイレブン岐阜明徳町店", "岐阜県岐阜市明徳町1-1", "自転車盗難シーン"),
    ("小市民シリーズ", "柳ヶ瀬商店街", "岐阜県岐阜市柳ヶ瀬通2丁目3", "劇場通り"),
    ("小市民シリーズ", "ロボット水門", "岐阜県岐阜市御手洗", "推理シーン"),
    ("小市民シリーズ", "Princess Branche", "岐阜県岐阜市則武東2丁目18-28", "ケーキ店モデル"),
    ("小市民シリーズ", "三田洞弘法", "岐阜県岐阜市三田洞131", "自転車発見場所"),
    ("小市民シリーズ", "粕森公園", "岐阜県岐阜市粕森町", "イメージシーン"),
    ("小市民シリーズ", "伊奈波神社", "岐阜県岐阜市伊奈波通り1丁目1", "夏祭りシーン"),
    ("小市民シリーズ", "岐阜駅", "岐阜県岐阜市橋本町1丁目10", "木良駅モデル"),
    ("小市民シリーズ", "岐阜公園", "岐阜県岐阜市大宮町1丁目", "ED映像"),
    ("小市民シリーズ", "ナチュラルカフェ", "岐阜県岐阜市本町2丁目14", "セシリアモデル"),
    ("小市民シリーズ", "中問屋町", "岐阜県岐阜市問屋町", "心象風景"),
    ("小市民シリーズ", "アクアージュ柳ヶ瀬", "岐阜県岐阜市神田町4丁目16", "消防車シーン"),
    ("小市民シリーズ", "川原町屋", "岐阜県岐阜市玉井町28", "桜庵カフェモデル"),
    ("小市民シリーズ", "ぎふメディアコスモス", "岐阜県岐阜市司町40丁目5", "図書館"),
    ("小市民シリーズ", "梅林公園", "岐阜県岐阜市梅林南町", "SL型遊具"),

    # ── 天気の子 (12 spots) ──
    ("天気の子", "代々木会館跡", "東京都渋谷区代々木1丁目35-1", "天気の巫女覚醒"),
    ("天気の子", "アタミビル", "東京都新宿区歌舞伎町2丁目27-8", "帆高とアメの出会い"),
    ("天気の子", "マクドナルド西武新宿駅前店", "東京都新宿区歌舞伎町1丁目24-1", "帆高と陽菜の出会い"),
    ("天気の子", "朝日稲荷神社", "東京都中央区銀座3丁目8-12", "晴れ女の力"),
    ("天気の子", "田端駅南口", "東京都北区東田端1丁目17-1", "告白・再会シーン"),
    ("天気の子", "のぞき坂", "東京都豊島区高田2丁目12-21", "晴れ渡るシーン"),
    ("天気の子", "六本木ヒルズスカイデッキ", "東京都港区六本木6丁目10-1", "花火と夜空"),
    ("天気の子", "芝公園", "東京都港区芝公園1丁目", "須賀と萌花シーン"),
    ("天気の子", "高円寺氷川神社", "東京都杉並区高円寺南4丁目44-19", "気象神社"),
    ("天気の子", "竹芝客船ターミナル", "東京都港区海岸1丁目16-1", "帆高上京シーン"),
    ("天気の子", "お台場海浜公園", "東京都港区台場1丁目4", "展望デッキ"),
    ("天気の子", "神津島", "東京都神津島村", "帆高の故郷"),

    # ── 耳をすませば (10 spots) ──
    ("耳をすませば", "聖蹟桜ヶ丘駅", "東京都多摩市関戸1丁目10-10", "杉宮駅モデル"),
    ("耳をすませば", "いろは坂", "東京都多摩市桜ヶ丘4丁目43-25", "猫追いかけシーン"),
    ("耳をすませば", "いろは坂桜公園", "東京都多摩市桜ヶ丘4丁目33-9", "図書館の場所"),
    ("耳をすませば", "金比羅宮", "東京都多摩市桜ヶ丘1丁目54-4", "杉村告白シーン"),
    ("耳をすませば", "天守台", "東京都多摩市桜ヶ丘1丁目53-17", "天守の丘モデル"),
    ("耳をすませば", "桜ヶ丘ロータリー", "東京都多摩市桜ヶ丘4丁目1-1", "猫と骨董品店"),
    ("耳をすませば", "いろは坂の高台", "東京都多摩市桜ヶ丘4丁目42-14", "プロポーズシーン"),
    ("耳をすませば", "愛宕団地", "東京都多摩市愛宕2丁目5-2", "雫の住居モデル"),
    ("耳をすませば", "ノア洋菓子店", "東京都多摩市桜ヶ丘2丁目2-9", "ファン巡礼スポット"),
    ("耳をすませば", "Dining和桜", "東京都多摩市桜ヶ丘2丁目2-6", "耳をすませばうどん"),
]


def geocode_address(address):
    """Geocode a Japanese address using Nominatim."""
    q = urllib.parse.urlencode({
        "q": address,
        "format": "json",
        "countrycodes": "jp",
        "limit": 1,
    })
    url = f"https://nominatim.openstreetmap.org/search?{q}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "japan-culture-mcp/0.7 (teddykmk@gmail.com)",
        "Accept-Language": "ja",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"  Geocode error for {address}: {e}", flush=True)
    return None, None


def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")

    total = len(SEICHIMAP_SPOTS)
    print(f"Seichimap spots to geocode: {total}", flush=True)

    geocoded = 0
    failed = 0
    new_entities = 0
    new_connections = 0

    for i, (work, spot_name, address, desc) in enumerate(SEICHIMAP_SPOTS):
        # Rate limit: 1 req/sec for Nominatim
        if i > 0:
            time.sleep(1.1)

        lat, lon = geocode_address(address)

        if lat is None:
            # Try simpler query (just spot name + prefecture)
            pref = address.split("県")[0] + "県" if "県" in address else ""
            if pref:
                time.sleep(1.1)
                lat, lon = geocode_address(f"{spot_name} {pref}")

        if lat is None:
            failed += 1
            print(f"  [{i+1}/{total}] FAIL: {spot_name} ({address})", flush=True)
            continue

        geocoded += 1

        # Check if entity already exists
        existing = db.execute(
            "SELECT id FROM entities WHERE label_ja = ? AND lat IS NOT NULL",
            (spot_name,)
        ).fetchone()

        if existing:
            loc_id = existing[0]
        else:
            # Create entity
            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, lat, lon, source)
                VALUES (?, ?, 'place', ?, ?, ?)
            """, (spot_name, spot_name, lat, lon, SOURCE))
            loc_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            new_entities += 1

        # Find work entity
        work_row = db.execute(
            "SELECT id FROM entities WHERE label_ja = ? LIMIT 1",
            (work,)
        ).fetchone()

        if work_row:
            work_id = work_row[0]
            # Check if connection exists
            exists = db.execute("""
                SELECT 1 FROM connections
                WHERE connection_type = 'pilgrimage_spot'
                AND ((entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?))
            """, (work_id, loc_id, loc_id, work_id)).fetchone()

            if not exists:
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type, confidence, explanation)
                    VALUES (?, ?, 'pilgrimage_spot', 0.9, ?)
                """, (work_id, loc_id, f"聖地巡礼: {work}の舞台「{spot_name}」({desc}) [seichimap.jp]"))
                new_connections += 1

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{total}] geocoded={geocoded}, failed={failed}", flush=True)
            db.commit()

    db.commit()

    print(f"\n=== Seichimap Import Results ===", flush=True)
    print(f"Total spots: {total}", flush=True)
    print(f"Geocoded: {geocoded}", flush=True)
    print(f"Failed: {failed}", flush=True)
    print(f"New entities: {new_entities}", flush=True)
    print(f"New connections: {new_connections}", flush=True)

    # Show current pilgrimage stats
    pilgrim = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'"
    ).fetchone()[0]
    total_conn = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    total_ent = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\nTotal pilgrimage connections: {pilgrim:,}", flush=True)
    print(f"Total connections: {total_conn:,}", flush=True)
    print(f"Total entities: {total_ent:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
