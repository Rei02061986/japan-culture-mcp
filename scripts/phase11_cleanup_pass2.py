"""
Phase 11 Stream A-2: Second cleanup pass for remaining bad connections
Remove: 首都圏, 大日本帝国, additional venues, overly generic locations
"""
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"

# Additional locations to DELETE
BAD_LOCATIONS = {
    # L1/L2 that slipped through
    '首都圏', '大日本帝国', '東京湾', '日本列島', '太平洋',
    '本州', '内地', '関東平野',
    # Venues (concerts, sports, event halls)
    '渋谷公会堂', '名古屋市国際展示場', 'ナゴヤ球場',
    '東京メディアシティ', 'ミーツポート', '大井競馬場',
    '東京アクアティクスセンター', 'NTT日比谷ビル',
    '舞浜アンフィシアター', 'ニッポン放送',
    '東京体育館', '国立競技場', '味の素スタジアム',
    '秩父宮ラグビー場', '神宮球場', '東京スタジアム',
    'ラフォーレミュージアム', 'Bunkamuraオーチャードホール',
    'サントリーホール', '東京芸術劇場', '新国立劇場',
    '東京オペラシティ', '紀伊國屋ホール', '帝国劇場',
    '国立劇場', '歌舞伎座', '新橋演舞場',
    '有明コロシアム', '駒沢体育館',
}

# Patterns to match (partial)
BAD_PATTERNS = [
    'スタジアム', 'ドーム$', 'アリーナ$', 'ホール$', '劇場$', '体育館$',
    'コンサート', 'ライブハウス', '競馬場$', '競技場$', '球場$',
    '展示場$', '会議場$', 'センター$', 'シアター$',
]


def extract_location(explanation):
    m = re.search(r'同じ場所「(.+?)」', explanation)
    return m.group(1) if m else None


def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")

    # Get all remaining pilgrimage_same_location connections
    rows = db.execute("""
        SELECT id, explanation FROM connections
        WHERE connection_type = 'pilgrimage_same_location'
    """).fetchall()

    print(f"Remaining before pass 2: {len(rows):,}", flush=True)

    delete_ids = []
    for row in rows:
        location = extract_location(row[1] or '')
        if not location:
            continue

        # Check exact match
        if location in BAD_LOCATIONS:
            delete_ids.append(row[0])
            continue

        # Check patterns
        for pattern in BAD_PATTERNS:
            if re.search(pattern, location):
                delete_ids.append(row[0])
                break

    print(f"To delete: {len(delete_ids):,}", flush=True)

    # Delete
    batch_size = 10000
    for i in range(0, len(delete_ids), batch_size):
        batch = delete_ids[i:i + batch_size]
        placeholders = ','.join('?' * len(batch))
        db.execute(f"DELETE FROM connections WHERE id IN ({placeholders})", batch)
    db.commit()

    # Verify remaining
    remaining = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type = 'pilgrimage_same_location'"
    ).fetchone()[0]
    total = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    pilgrim = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'"
    ).fetchone()[0]

    print(f"\nDeleted: {len(delete_ids):,}", flush=True)
    print(f"Remaining same_location: {remaining:,}", flush=True)
    print(f"Total pilgrimage: {pilgrim:,}", flush=True)
    print(f"Total connections: {total:,}", flush=True)

    # Show top remaining locations
    print("\nTop 15 remaining locations:", flush=True)
    for row in db.execute("""
        SELECT
            SUBSTR(explanation, INSTR(explanation, '同じ場所「') + 5,
                   INSTR(SUBSTR(explanation, INSTR(explanation, '同じ場所「') + 5), '」') - 1) as location,
            COUNT(*) as c
        FROM connections
        WHERE connection_type = 'pilgrimage_same_location'
        GROUP BY location
        ORDER BY c DESC
        LIMIT 15
    """):
        print(f"  {row[0]}: {row[1]:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
