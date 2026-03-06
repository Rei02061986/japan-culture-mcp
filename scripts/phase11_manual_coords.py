"""
Phase 11 Stream B-4: Manual coordinates for failed geocoding spots
Well-known landmarks with known coordinates
"""
import sqlite3

DB_PATH = "ontology/culture_ontology.db"
SOURCE = "seichimap.jp"

# Manually curated coordinates for failed geocoding spots
MANUAL_SPOTS = [
    # 君の名は。
    ("君の名は。", "飛騨山王宮日枝神社", 36.1407, 137.2558),
    ("君の名は。", "新宿駅", 35.6896, 139.7006),
    ("君の名は。", "バスタ新宿", 35.6876, 139.7003),
    ("君の名は。", "国立新美術館", 35.6653, 139.7262),
    ("君の名は。", "須賀神社", 35.6878, 139.7187),
    ("君の名は。", "新宿警察署前", 35.6937, 139.6930),

    # すずめの戸締まり
    ("すずめの戸締まり", "神戸おとぎの国", 34.8337, 135.1673),
    ("すずめの戸締まり", "御茶ノ水駅", 35.6996, 139.7654),

    # ゆるキャン△
    ("ゆるキャン△", "旧下部小学校跡", 35.3720, 138.4370),
    ("ゆるキャン△", "セルバみのぶ店", 35.3810, 138.4410),
    ("ゆるキャン△", "渚園キャンプ場", 34.6869, 137.6069),

    # SLAM DUNK
    ("SLAM DUNK", "鎌倉高校前駅踏切", 35.3067, 139.5006),
    ("SLAM DUNK", "湯来温泉みどり荘", 34.4356, 132.2889),
    ("SLAM DUNK", "お好み村かずちゃん", 34.3932, 132.4581),
    ("SLAM DUNK", "桃原西公園", 26.3356, 127.7617),

    # 聲の形
    ("聲の形", "滝のトンネル", 35.3596, 136.6143),

    # 小市民シリーズ
    ("小市民シリーズ", "AND LADY", 35.4178, 136.7575),
    ("小市民シリーズ", "セブンイレブン岐阜明徳町店", 35.4192, 136.7603),
    ("小市民シリーズ", "Princess Branche", 35.4261, 136.7469),
    ("小市民シリーズ", "三田洞弘法", 35.4514, 136.7567),
    ("小市民シリーズ", "ナチュラルカフェ", 35.4208, 136.7547),

    # 天気の子
    ("天気の子", "代々木会館跡", 35.6827, 139.7021),
    ("天気の子", "アタミビル", 35.6963, 139.7036),
    ("天気の子", "マクドナルド西武新宿駅前店", 35.6960, 139.7005),
    ("天気の子", "朝日稲荷神社", 35.6722, 139.7695),
    ("天気の子", "田端駅南口", 35.7381, 139.7612),
    ("天気の子", "のぞき坂", 35.7137, 139.7214),
    ("天気の子", "六本木ヒルズスカイデッキ", 35.6604, 139.7292),
    ("天気の子", "高円寺氷川神社", 35.6996, 139.6503),
    ("天気の子", "竹芝客船ターミナル", 35.6553, 139.7618),
    ("天気の子", "お台場海浜公園", 35.6289, 139.7752),

    # 耳をすませば
    ("耳をすませば", "聖蹟桜ヶ丘駅", 35.6504, 139.4464),
    ("耳をすませば", "いろは坂", 35.6492, 139.4428),
    ("耳をすませば", "いろは坂桜公園", 35.6489, 139.4425),
    ("耳をすませば", "金比羅宮", 35.6470, 139.4449),
    ("耳をすませば", "天守台", 35.6468, 139.4453),
    ("耳をすませば", "桜ヶ丘ロータリー", 35.6499, 139.4418),
    ("耳をすませば", "いろは坂の高台", 35.6491, 139.4422),
    ("耳をすませば", "愛宕団地", 35.6452, 139.4379),
    ("耳をすませば", "ノア洋菓子店", 35.6503, 139.4445),
    ("耳をすませば", "Dining和桜", 35.6503, 139.4443),
]


def main():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    new_entities = 0
    new_connections = 0
    updated = 0

    for work, spot_name, lat, lon in MANUAL_SPOTS:
        # Check if entity exists (maybe created without coords)
        existing = db.execute(
            "SELECT id, lat FROM entities WHERE label_ja = ? LIMIT 1",
            (spot_name,)
        ).fetchone()

        if existing and existing[1] is not None:
            loc_id = existing[0]
        elif existing and existing[1] is None:
            # Update with coords
            db.execute(
                "UPDATE entities SET lat = ?, lon = ? WHERE id = ?",
                (lat, lon, existing[0])
            )
            loc_id = existing[0]
            updated += 1
        else:
            # Create new
            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, lat, lon, source)
                VALUES (?, ?, 'place', ?, ?, ?)
            """, (spot_name, spot_name, lat, lon, SOURCE))
            loc_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            new_entities += 1

        # Find work
        work_row = db.execute(
            "SELECT id FROM entities WHERE label_ja = ? LIMIT 1",
            (work,)
        ).fetchone()

        if work_row:
            work_id = work_row[0]
            exists = db.execute("""
                SELECT 1 FROM connections
                WHERE connection_type = 'pilgrimage_spot'
                AND ((entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?))
            """, (work_id, loc_id, loc_id, work_id)).fetchone()

            if not exists:
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type, confidence, explanation)
                    VALUES (?, ?, 'pilgrimage_spot', 0.95, ?)
                """, (work_id, loc_id, f"聖地巡礼: {work}の舞台「{spot_name}」[seichimap.jp/manual]"))
                new_connections += 1

    import time
    for retry in range(5):
        try:
            db.commit()
            break
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {retry+1}: {e}", flush=True)
            time.sleep(3)

    pilgrim = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'"
    ).fetchone()[0]

    print(f"Manual coords import:", flush=True)
    print(f"  New entities: {new_entities}", flush=True)
    print(f"  Updated coords: {updated}", flush=True)
    print(f"  New connections: {new_connections}", flush=True)
    print(f"  Total pilgrimage connections: {pilgrim:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
