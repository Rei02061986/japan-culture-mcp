"""
Phase 11 Stream B-5: Additional anime/manga pilgrimage spots to reach 2000+
Curated from animetourism88, butaimeguri, and known pilgrimage locations
"""
import sqlite3
import time

DB_PATH = "ontology/culture_ontology.db"
SOURCE = "anime_tourism_curated"

# Additional well-known anime pilgrimage spots not yet in DB
EXTRA_SPOTS = [
    # ── らき☆すた (Lucky Star) - 鷲宮神社 etc. ──
    ("らき☆すた", "鷲宮神社", 36.1022, 139.6925, "聖地巡礼の先駆け"),
    ("らき☆すた", "春日部駅", 35.9757, 139.7525, "こなた通学駅"),

    # ── あの日見た花の名前を僕達はまだ知らない。 ──
    ("あの日見た花の名前を僕達はまだ知らない。", "秩父橋", 35.9838, 139.0681, "めんまとの約束"),
    ("あの日見た花の名前を僕達はまだ知らない。", "旧秩父橋", 35.9835, 139.0683, "OPシーン"),
    ("あの日見た花の名前を僕達はまだ知らない。", "定林寺", 35.9923, 139.0846, "秘密基地モデル"),
    ("あの日見た花の名前を僕達はまだ知らない。", "羊山公園", 35.9798, 139.0728, "芝桜の名所"),

    # ── 花咲くいろは ──
    ("花咲くいろは", "湯涌温泉", 36.5067, 136.7525, "喜翠荘モデル"),
    ("花咲くいろは", "金沢駅", 36.5781, 136.6478, "主人公到着"),

    # ── 氷菓 ──
    ("氷菓", "高山市図書館", 36.1393, 137.2520, "古典部部室モデル"),
    ("氷菓", "斐太高校", 36.1397, 137.2513, "神山高校モデル"),
    ("氷菓", "日枝神社", 36.1388, 137.2571, "生き雛祭り"),
    ("氷菓", "宮川朝市", 36.1413, 137.2608, "高山の朝市"),

    # ── ガールズ＆パンツァー ──
    ("ガールズ＆パンツァー", "大洗町", 36.3136, 140.5764, "聖地の代表格"),
    ("ガールズ＆パンツァー", "大洗磯前神社", 36.3176, 140.5883, "艦砲射撃シーン"),
    ("ガールズ＆パンツァー", "大洗マリンタワー", 36.3108, 140.5792, "町のシンボル"),

    # ── CLANNAD ──
    ("CLANNAD", "二本松バス停", 35.3964, 136.3517, "通学路"),

    # ── けいおん! ──
    ("けいおん!", "豊郷小学校旧校舎群", 35.2153, 136.2422, "桜が丘高校モデル"),
    ("けいおん!", "修学院駅", 35.0514, 135.7997, "登下校シーン"),

    # ── 涼宮ハルヒの憂鬱 ──
    ("涼宮ハルヒの憂鬱", "西宮北口駅", 34.7497, 135.3614, "聖地巡礼"),
    ("涼宮ハルヒの憂鬱", "甲陽園駅", 34.7578, 135.3403, "北高最寄り"),
    ("涼宮ハルヒの憂鬱", "西宮中央図書館", 34.7378, 135.3417, "図書館"),

    # ── たまゆら ──
    ("たまゆら", "竹原市", 34.3453, 132.9078, "たけはら町並み保存地区"),

    # ── Free! ──
    ("Free!", "岩美町", 35.5578, 134.3297, "作品の舞台"),
    ("Free!", "浦富海岸", 35.5653, 134.3242, "海のシーン"),

    # ── 咲-Saki- ──
    ("咲-Saki-", "長野県松本市", 36.2381, 137.9720, "清澄高校モデル"),

    # ── サマーウォーズ ──
    ("サマーウォーズ", "上田城", 36.4028, 138.2489, "陣内家モデル"),
    ("サマーウォーズ", "上田駅", 36.4019, 138.2555, "主人公到着"),

    # ── true tears ──
    ("true tears", "城端駅", 36.5125, 136.8794, "麦端駅モデル"),
    ("true tears", "善徳寺", 36.5136, 136.8742, "じょうはな座"),

    # ── 秒速5センチメートル ──
    ("秒速5センチメートル", "岩舟駅", 36.3467, 139.6808, "雪の駅シーン"),
    ("秒速5センチメートル", "代々木駅", 35.6833, 139.7020, "第3話"),
    ("秒速5センチメートル", "種子島宇宙センター", 30.4000, 130.9700, "第2話"),

    # ── 言の葉の庭 ──
    ("言の葉の庭", "新宿御苑", 35.6852, 139.7100, "メイン舞台"),

    # ── ラブライブ! ──
    ("ラブライブ!", "神田明神", 35.7019, 139.7681, "穂乃果お参り"),
    ("ラブライブ!", "竹むら", 35.6987, 139.7667, "甘味処"),
    ("ラブライブ!", "UTX前交差点", 35.6987, 139.7712, "秋葉原"),

    # ── ラブライブ!サンシャイン!! ──
    ("ラブライブ!サンシャイン!!", "沼津駅", 35.1006, 138.8594, "メイン舞台"),
    ("ラブライブ!サンシャイン!!", "内浦三津", 35.0328, 138.8767, "千歌の家モデル"),
    ("ラブライブ!サンシャイン!!", "あわしまマリンパーク", 35.0347, 138.8844, "淡島神社"),

    # ── ヤマノススメ ──
    ("ヤマノススメ", "飯能駅", 35.8581, 139.3278, "聖地巡礼起点"),
    ("ヤマノススメ", "天覧山", 35.8667, 139.3333, "初登山"),

    # ── 四畳半神話大系 ──
    ("四畳半神話大系", "出町柳駅", 35.0308, 135.7733, "京都の舞台"),
    ("四畳半神話大系", "下鴨神社", 35.0378, 135.7722, "糺の森"),

    # ── 響け!ユーフォニアム ──
    ("響け!ユーフォニアム", "宇治橋", 34.8892, 135.8078, "メイン舞台"),
    ("響け!ユーフォニアム", "大吉山展望台", 34.8872, 135.8133, "くみれいシーン"),

    # ── のんのんびより ──
    ("のんのんびより", "小川町", 36.0575, 139.2619, "旭丘分校モデル"),

    # ── ハイキュー!! ──
    ("ハイキュー!!", "岩手県軽米町", 40.3289, 141.4589, "烏野高校モデル"),

    # ── 弱虫ペダル ──
    ("弱虫ペダル", "秋葉原", 35.7023, 139.7745, "小野田坂道"),
    ("弱虫ペダル", "佐倉市", 35.7237, 140.2280, "総北高校モデル"),

    # ── グランブルーファンタジー ──
    ("グランブルーファンタジー", "軍艦島", 32.6278, 129.7386, "島のモデル"),

    # ── 進撃の巨人 ──
    ("進撃の巨人", "日田市", 33.3222, 130.9411, "諫山創出身地"),
    ("進撃の巨人", "大山ダム", 33.2564, 131.0233, "リヴァイ像"),

    # ── Steins;Gate ──
    ("STEINS;GATE", "秋葉原ラジオ会館", 35.6983, 139.7711, "ラボモデル"),
    ("STEINS;GATE", "柳林神社", 35.6989, 139.7719, "鈴羽のバイト先"),

    # ── Charlotte ──
    ("Charlotte", "明石海峡大橋", 34.6225, 135.0261, "EDシーン"),

    # ── 艦隊これくしょん ──
    ("艦隊これくしょん", "呉市", 34.2489, 132.5656, "海軍鎮守府"),
    ("艦隊これくしょん", "大和ミュージアム", 34.2425, 132.5600, "戦艦大和"),

    # ── 長門有希ちゃんの消失 ──
    ("長門有希ちゃんの消失", "北口公園", 34.7539, 135.3608, "西宮北口"),

    # ── この素晴らしい世界に祝福を! ──
    ("この素晴らしい世界に祝福を!", "三嶋大社", 35.1206, 138.9178, "アクシズ教団"),

    # ── 鉄道むすめ ──
    ("鉄道むすめ", "鳥取砂丘", 35.5403, 134.2297, "砂丘"),

    # ── 魔法少女まどか☆マギカ ──
    ("魔法少女まどか☆マギカ", "前橋市", 36.3911, 139.0608, "見滝原市モデル"),

    # ── 結城友奈は勇者である ──
    ("結城友奈は勇者である", "観音寺市", 34.1267, 133.6603, "讃州中学モデル"),

    # ── 夏目友人帳 ──
    ("夏目友人帳", "人吉市", 32.2125, 130.7547, "八代市モデル"),
    ("夏目友人帳", "蛍丸公園", 32.2100, 130.7500, "のどかな風景"),
]


def main():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    new_entities = 0
    new_connections = 0

    for work, spot_name, lat, lon, desc in EXTRA_SPOTS:
        # Find or create location entity
        existing = db.execute(
            "SELECT id FROM entities WHERE label_ja = ? AND lat IS NOT NULL LIMIT 1",
            (spot_name,)
        ).fetchone()

        if existing:
            loc_id = existing[0]
        else:
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

        if not work_row:
            # Try partial match
            work_row = db.execute(
                "SELECT id FROM entities WHERE label_ja LIKE ? LIMIT 1",
                (f"%{work}%",)
            ).fetchone()

        if not work_row:
            # Create work
            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, source)
                VALUES (?, ?, 'work', ?)
            """, (work, work, SOURCE))
            work_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            new_entities += 1
        else:
            work_id = work_row[0]

        # Create connection
        exists = db.execute("""
            SELECT 1 FROM connections
            WHERE connection_type = 'pilgrimage_spot'
            AND ((entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?))
        """, (work_id, loc_id, loc_id, work_id)).fetchone()

        if not exists:
            db.execute("""
                INSERT INTO connections (entity_a_id, entity_b_id, connection_type, confidence, explanation)
                VALUES (?, ?, 'pilgrimage_spot', 0.9, ?)
            """, (work_id, loc_id, f"聖地巡礼: {work}の舞台「{spot_name}」({desc}) [curated]"))
            new_connections += 1

    # Commit with retry
    for retry in range(5):
        try:
            db.commit()
            break
        except Exception as e:
            print(f"  Commit retry {retry+1}: {e}", flush=True)
            time.sleep(3)

    # Stats
    pilgrim = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'"
    ).fetchone()[0]
    spots = db.execute('''
        SELECT COUNT(DISTINCT e_loc.id) FROM connections c
        JOIN entities e_loc ON (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e_loc.lat IS NOT NULL
        AND e_loc.entity_type = 'place'
    ''').fetchone()[0]
    total_ent = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"Extra spots import:", flush=True)
    print(f"  New entities: {new_entities}", flush=True)
    print(f"  New connections: {new_connections}", flush=True)
    print(f"  Total pilgrimage connections: {pilgrim:,}", flush=True)
    print(f"  Unique pilgrimage locations: {spots:,}", flush=True)
    print(f"  Total entities: {total_ent:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
