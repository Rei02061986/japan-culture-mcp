"""
Phase 11 Stream A: same-location 743,910件の品質フィルタリング
粒度分析 + フィルタリング実行

Granularity levels:
  L1: 国レベル（日本）→ DELETE
  L2: 地方レベル（関東、近畿）→ DELETE
  L3: 都道府県レベル（東京都）→ conditional
  L4: 市区町村レベル（秩父市）→ conditional (density-based)
  L5: スポットレベル（鎌倉高校前）→ KEEP
"""
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"

# ── Granularity classification ──

# L1: Country level
L1_LABELS = {'日本', 'Japan', '日本国'}

# L2: Region level
L2_LABELS = {
    '北海道地方', '東北地方', '関東地方', '中部地方', '近畿地方',
    '中国地方', '四国地方', '九州地方', '九州・沖縄地方',
    '東海地方', '北陸地方', '甲信越地方', '関西地方',
}

# L3: Prefecture level (47 prefectures)
L3_PREFECTURES = {
    '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
    '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
    '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県',
    '静岡県', '愛知県', '三重県', '滋賀県', '京都府', '大阪府', '兵庫県',
    '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県', '山口県',
    '徳島県', '香川県', '愛媛県', '高知県', '福岡県', '佐賀県', '長崎県',
    '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県',
}

# L4: Major cities (high-density, treat as L3 for filtering)
L4_HIGH_DENSITY_CITIES = {
    '東京', '大阪市', '横浜市', '名古屋市', '札幌市', '福岡市',
    '神戸市', '京都市', '川崎市', '広島市', 'さいたま市',
    '仙台市', '千葉市', '北九州市', '堺市', '新潟市',
    '浜松市', '熊本市', '相模原市', '岡山市', '静岡市',
}

# Concert venues / event spaces (L5 technically but not cultural pilgrimage)
VENUE_LABELS = {
    '横浜アリーナ', '東京ドーム', 'さいたまスーパーアリーナ', 'NHKホール',
    '東京国際フォーラム', '中野サンプラザ', '武道館', '日本武道館',
    '大阪城ホール', '幕張メッセ', 'ぴあアリーナMM', 'Zepp',
    '国立代々木競技場', '東京ガーデンシアター', 'パシフィコ横浜',
    '昭和女子大学人見記念講堂', '有明アリーナ', '国技館', '両国国技館',
    'マリンメッセ福岡', '大宮ソニックシティ', 'サンドーム福井',
    'ナゴヤドーム', 'バンテリンドーム', 'PayPayドーム', '京セラドーム',
    'TOKYO DOME CITY HALL', 'Bunkamura',
}


def extract_location(explanation):
    """Extract location name from explanation text."""
    # Format: ...同じ場所「LOCATION」を舞台...
    m = re.search(r'同じ場所「(.+?)」', explanation)
    if m:
        return m.group(1)
    return None


def classify_granularity(location):
    """Classify location into granularity level."""
    if not location:
        return 'UNKNOWN'

    if location in L1_LABELS:
        return 'L1'
    if location in L2_LABELS:
        return 'L2'
    if location in L3_PREFECTURES:
        return 'L3_prefecture'
    if location in L4_HIGH_DENSITY_CITIES:
        return 'L4_high_density'
    if location in VENUE_LABELS or any(v in location for v in ['ホール', 'ドーム', 'アリーナ', 'スタジアム', '競技場', 'Zepp', 'LIVE']):
        return 'L5_venue'

    # Check if it looks like a city (ends with 市/町/村/区)
    if re.search(r'(市|町|村|区)$', location):
        return 'L4_city'

    # Everything else is L5 (specific spot)
    return 'L5_spot'


def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")

    # ── Step 1: Granularity analysis ──
    print("=== Step 1: Granularity Analysis ===", flush=True)

    # Get all same-location connections
    rows = db.execute("""
        SELECT id, explanation, entity_a_id, entity_b_id
        FROM connections
        WHERE connection_type = 'pilgrimage_same_location'
    """).fetchall()
    print(f"Total pilgrimage_same_location: {len(rows):,}", flush=True)

    # Classify each connection
    granularity_counts = {}
    location_counts = {}
    delete_ids = []
    keep_ids = []

    for row in rows:
        conn_id = row[0]
        explanation = row[1] or ''
        location = extract_location(explanation)

        level = classify_granularity(location)
        granularity_counts[level] = granularity_counts.get(level, 0) + 1

        if location:
            location_counts[location] = location_counts.get(location, 0) + 1

        # Decide: DELETE or KEEP
        if level in ('L1', 'L2', 'L3_prefecture', 'L4_high_density', 'L5_venue'):
            delete_ids.append(conn_id)
        elif level == 'L4_city':
            # Keep cities with fewer than 5000 connections (more specific = better)
            if location_counts.get(location, 0) <= 5000:
                keep_ids.append(conn_id)
            else:
                delete_ids.append(conn_id)
        else:
            keep_ids.append(conn_id)

    print("\nGranularity breakdown:", flush=True)
    for level, count in sorted(granularity_counts.items(), key=lambda x: -x[1]):
        action = "DELETE" if level in ('L1', 'L2', 'L3_prefecture', 'L4_high_density', 'L5_venue') else "KEEP/CONDITIONAL"
        print(f"  {level}: {count:,} → {action}", flush=True)

    print(f"\nTop 15 locations by connection count:", flush=True)
    for loc, count in sorted(location_counts.items(), key=lambda x: -x[1])[:15]:
        level = classify_granularity(loc)
        print(f"  {loc}: {count:,} ({level})", flush=True)

    # ── Step 2: Additional filter — same entity_type at city level ──
    print("\n=== Step 2: Same-type filter at city level ===", flush=True)

    # For kept connections at L4_city, check if both entities have the same type
    # Same anime × anime at city level = low serendipity
    additional_deletes = 0
    final_keep = []

    for conn_id in keep_ids:
        row = db.execute("""
            SELECT ea.entity_type, eb.entity_type
            FROM connections c
            JOIN entities ea ON c.entity_a_id = ea.id
            JOIN entities eb ON c.entity_b_id = eb.id
            WHERE c.id = ?
        """, (conn_id,)).fetchone()

        if row and row[0] == row[1]:
            # Same type — check if it's a generic type
            if row[0] in ('anime', 'manga', 'film', 'tv', 'music', 'game', 'work'):
                # Check if this is an L4_city connection
                exp = db.execute("SELECT explanation FROM connections WHERE id=?", (conn_id,)).fetchone()
                if exp:
                    loc = extract_location(exp[0])
                    level = classify_granularity(loc)
                    if level == 'L4_city':
                        delete_ids.append(conn_id)
                        additional_deletes += 1
                        continue
        final_keep.append(conn_id)

    print(f"  Additional same-type@city deletes: {additional_deletes:,}", flush=True)

    # ── Step 3: Execute deletion ──
    print(f"\n=== Step 3: Deletion ===", flush=True)
    print(f"  To delete: {len(delete_ids):,}", flush=True)
    print(f"  To keep: {len(final_keep):,}", flush=True)

    # Delete in batches
    batch_size = 10000
    deleted_total = 0
    for i in range(0, len(delete_ids), batch_size):
        batch = delete_ids[i:i + batch_size]
        placeholders = ','.join('?' * len(batch))
        db.execute(f"DELETE FROM connections WHERE id IN ({placeholders})", batch)
        deleted_total += len(batch)
        if (i // batch_size) % 10 == 0:
            db.commit()
            print(f"  Deleted {deleted_total:,}/{len(delete_ids):,}...", flush=True)

    db.commit()
    print(f"  Deletion complete: {deleted_total:,} removed", flush=True)

    # ── Step 4: Summary ──
    total_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    pilgrim_conns = db.execute("SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'").fetchone()[0]
    same_loc = db.execute("SELECT COUNT(*) FROM connections WHERE connection_type = 'pilgrimage_same_location'").fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print(f"=== Stream A: Filtering Complete ===", flush=True)
    print(f"Deleted: {deleted_total:,}", flush=True)
    print(f"Remaining pilgrimage_same_location: {same_loc:,}", flush=True)
    print(f"Total pilgrimage connections: {pilgrim_conns:,}", flush=True)
    print(f"Total connections: {total_conns:,}", flush=True)

    # ── Quality sample ──
    print(f"\n=== Quality Sample (20 random kept connections) ===", flush=True)
    samples = db.execute("""
        SELECT ea.label_ja, ea.entity_type, eb.label_ja, eb.entity_type, c.explanation
        FROM connections c
        JOIN entities ea ON c.entity_a_id = ea.id
        JOIN entities eb ON c.entity_b_id = eb.id
        WHERE c.connection_type = 'pilgrimage_same_location'
        ORDER BY RANDOM()
        LIMIT 20
    """).fetchall()
    for s in samples:
        print(f"  {s[0]} ({s[1]}) ←→ {s[2]} ({s[3]})", flush=True)
        print(f"    {s[4][:100]}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
