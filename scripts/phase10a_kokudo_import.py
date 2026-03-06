"""
Phase 10A: 国土数値情報 (National Land Numerical Information) import.
Download and parse Shapefile data for tourism resources, cultural properties,
visitor facilities, and world heritage sites.
Target: ~100,000 new entities with coordinates.
"""
import io
import os
import re
import sqlite3
import struct
import time
import zipfile
from urllib.request import urlopen, Request

DB_PATH = "ontology/culture_ontology.db"

# Download URLs
DATASETS = {
    'tourism': {
        'url': 'https://nlftp.mlit.go.jp/ksj/gml/data/P12/P12-14/P12-14_GML.zip',
        'name_field': 'P12_002',
        'type_field': 'P12_007',
        'address_field': 'P12_006',
        'pref_field': 'P12_003',
        'entity_type': 'place',
        'source': 'kokudo_p12',
        'type_map': {
            '1': ('nature_communion', 'adventure', '自然・イベント'),
            '2': ('community_tradition', 'intellectual', '歴史・文化'),
            '3': ('nature_communion', 'physical', '温泉・健康'),
            '4': ('sports', 'physical', 'スポーツ・レクリエーション'),
            '5': ('everyday_beauty', 'social', '都市型観光'),
            '6': ('community_tradition', 'social', 'その他'),
        },
    },
    'cultural_properties': {
        'url': 'https://nlftp.mlit.go.jp/ksj/gml/data/P32/P32-14/P32-14_00_GML.zip',
        'name_field': 'P32_006',
        'type_field': 'P32_004',
        'address_field': 'P32_007',
        'pref_field': 'P32_002',
        'entity_type': 'artifact',
        'source': 'kokudo_p32',
        'type_map': {
            '1': ('craft_mastery', 'aesthetic', '有形文化財'),
            '2': ('community_tradition', 'aesthetic', '無形文化財'),
            '3': ('community_tradition', 'social', '民俗文化財'),
            '4': ('community_tradition', 'intellectual', '記念物'),
            '5': ('nature_communion', 'aesthetic', '文化的景観'),
            '6': ('architecture', 'aesthetic', '伝統的建造物群'),
            '7': ('craft_mastery', 'intellectual', '保存技術'),
        },
    },
    'world_heritage': {
        'url': 'https://nlftp.mlit.go.jp/ksj/gml/data/A34/A34-22/A34-230328_GML.zip',
        'name_field': None,  # Will handle specially - uses GeoJSON
        'entity_type': 'place',
        'source': 'kokudo_a34',
    },
}

# Prefecture code to geography mapping
PREF_TO_GEO = {
    '01': 'hokkaido',
    '02': 'tohoku', '03': 'tohoku', '04': 'tohoku', '05': 'tohoku', '06': 'tohoku', '07': 'tohoku',
    '08': 'kanto', '09': 'kanto', '10': 'kanto', '11': 'kanto', '12': 'kanto', '13': 'kanto', '14': 'kanto',
    '15': 'chubu', '16': 'chubu', '17': 'chubu', '18': 'chubu', '19': 'chubu', '20': 'chubu',
    '21': 'chubu', '22': 'chubu', '23': 'chubu',
    '24': 'kinki', '25': 'kinki', '26': 'kinki', '27': 'kinki', '28': 'kinki', '29': 'kinki', '30': 'kinki',
    '31': 'chugoku', '32': 'chugoku', '33': 'chugoku', '34': 'chugoku', '35': 'chugoku',
    '36': 'shikoku', '37': 'shikoku', '38': 'shikoku', '39': 'shikoku',
    '40': 'kyushu', '41': 'kyushu', '42': 'kyushu', '43': 'kyushu', '44': 'kyushu',
    '45': 'kyushu', '46': 'kyushu', '47': 'kyushu',
}

# Special prefectures
PREF_SPECIAL = {
    '13': 'tokyo', '26': 'kyoto', '27': 'osaka', '29': 'nara',
}


def download_zip(url):
    """Download a ZIP file and return its content."""
    print(f"  Downloading {url}...", flush=True)
    headers = {'User-Agent': 'japan-culture-mcp/0.5'}
    req = Request(url, headers=headers)
    try:
        resp = urlopen(req, timeout=120)
        data = resp.read()
        print(f"  Downloaded: {len(data):,} bytes", flush=True)
        return data
    except Exception as e:
        print(f"  Download error: {e}", flush=True)
        return None


def read_shapefile_from_zip(zip_data, prefix=None):
    """Read shapefile records from a ZIP archive.
    Returns list of (record_dict, lat, lon) tuples."""
    import shapefile

    z = zipfile.ZipFile(io.BytesIO(zip_data))
    names = z.namelist()

    # Find .shp files
    shp_files = [n for n in names if n.endswith('.shp')]
    if prefix:
        shp_files = [n for n in shp_files if prefix in n]

    # Prefer point files (xxxxa.shp for P12)
    point_files = [n for n in shp_files if 'a.' in n.lower() or n.count('.') == 1]
    if not point_files:
        point_files = shp_files

    results = []
    for shp_name in point_files:
        base = shp_name[:-4]
        dbf_name = base + '.dbf'
        shx_name = base + '.shx'

        if dbf_name not in names:
            continue

        # Extract files to temp
        shp_data = z.read(shp_name)
        dbf_data = z.read(dbf_name)
        shx_data = z.read(shx_name) if shx_name in names else None

        try:
            sf = shapefile.Reader(
                shp=io.BytesIO(shp_data),
                dbf=io.BytesIO(dbf_data),
                shx=io.BytesIO(shx_data) if shx_data else None,
                encoding='cp932'
            )

            fields = [f[0] for f in sf.fields[1:]]  # Skip deletion flag
            for sr in sf.shapeRecords():
                record = dict(zip(fields, sr.record))
                shape = sr.shape

                lat, lon = None, None
                if shape.shapeType in (1, 11, 21):  # Point types
                    lon, lat = shape.points[0]
                elif shape.shapeType in (3, 5, 13, 15, 23, 25):  # Line/Polygon
                    # Use centroid of bounding box
                    if hasattr(shape, 'bbox') and shape.bbox:
                        lon = (shape.bbox[0] + shape.bbox[2]) / 2
                        lat = (shape.bbox[1] + shape.bbox[3]) / 2
                    elif shape.points:
                        lon = sum(p[0] for p in shape.points) / len(shape.points)
                        lat = sum(p[1] for p in shape.points) / len(shape.points)

                results.append((record, lat, lon))

        except Exception as e:
            print(f"    Error reading {shp_name}: {e}", flush=True)

    return results


def read_geojson_from_zip(zip_data, pattern=None):
    """Read GeoJSON files from a ZIP archive."""
    import json

    z = zipfile.ZipFile(io.BytesIO(zip_data))
    names = z.namelist()

    geojson_files = [n for n in names if n.endswith('.geojson') or n.endswith('.json')]
    if pattern:
        geojson_files = [n for n in geojson_files if pattern in n]

    results = []
    for gj_name in geojson_files:
        try:
            data = json.loads(z.read(gj_name).decode('utf-8'))
            features = data.get('features', [])
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry', {})
                lat, lon = None, None
                if geom.get('type') == 'Point':
                    coords = geom.get('coordinates', [])
                    if len(coords) >= 2:
                        lon, lat = coords[0], coords[1]
                elif geom.get('type') in ('Polygon', 'MultiPolygon', 'LineString'):
                    # Use first coordinate
                    coords = geom.get('coordinates', [])
                    if coords:
                        if geom['type'] == 'Point':
                            lon, lat = coords[0], coords[1]
                        elif geom['type'] == 'LineString':
                            mid = coords[len(coords)//2]
                            lon, lat = mid[0], mid[1]
                        elif geom['type'] == 'Polygon':
                            ring = coords[0]
                            lon = sum(p[0] for p in ring) / len(ring)
                            lat = sum(p[1] for p in ring) / len(ring)
                        elif geom['type'] == 'MultiPolygon':
                            ring = coords[0][0]
                            lon = sum(p[0] for p in ring) / len(ring)
                            lat = sum(p[1] for p in ring) / len(ring)

                results.append((props, lat, lon))
        except Exception as e:
            print(f"    Error reading {gj_name}: {e}", flush=True)

    return results


def import_tourism(db, existing_labels, zip_data):
    """Import P12 tourism resources."""
    config = DATASETS['tourism']
    records = read_shapefile_from_zip(zip_data)
    print(f"  Parsed {len(records):,} records", flush=True)

    new_count = 0
    for record, lat, lon in records:
        name = str(record.get(config['name_field'], '') or '').strip()
        if not name or len(name) < 2 or name in existing_labels:
            continue

        type_code = str(record.get(config['type_field'], '') or '').strip()
        pref_code = str(record.get(config['pref_field'], '') or '').strip()[:2]

        theme, experience, type_name = config['type_map'].get(type_code, ('community_tradition', 'social', 'その他'))

        cur = db.execute("""
            INSERT INTO entities (label_ja, entity_type, source, lat, lon)
            VALUES (?, ?, ?, ?, ?)
        """, (name, config['entity_type'], config['source'], lat, lon))
        eid = cur.lastrowid

        # Theme tag
        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, ?, 0.9)",
                   (eid, theme, config['source']))
        # Experience tag
        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', ?, ?, 0.8)",
                   (eid, experience, config['source']))
        # Geography tag
        geo = PREF_SPECIAL.get(pref_code) or PREF_TO_GEO.get(pref_code)
        if geo:
            db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', ?, ?, 0.9)",
                       (eid, geo, config['source']))

        existing_labels.add(name)
        new_count += 1

    return new_count


def import_cultural_properties(db, existing_labels, zip_data):
    """Import P32 cultural properties."""
    config = DATASETS['cultural_properties']
    records = read_shapefile_from_zip(zip_data)
    print(f"  Parsed {len(records):,} records", flush=True)

    new_count = 0
    for record, lat, lon in records:
        name = str(record.get(config['name_field'], '') or '').strip()
        if not name or len(name) < 2 or name in existing_labels:
            continue

        type_code = str(record.get(config['type_field'], '') or '').strip()
        pref_code = str(record.get(config['pref_field'], '') or '').strip()[:2]

        theme, experience, type_name = config['type_map'].get(type_code, ('community_tradition', 'aesthetic', '文化財'))

        cur = db.execute("""
            INSERT INTO entities (label_ja, entity_type, source, lat, lon)
            VALUES (?, ?, ?, ?, ?)
        """, (name, config['entity_type'], config['source'], lat, lon))
        eid = cur.lastrowid

        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, ?, 0.9)",
                   (eid, theme, config['source']))
        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', ?, ?, 0.8)",
                   (eid, experience, config['source']))
        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', 'sacred_profane', ?, 0.5)",
                   (eid, config['source']))

        geo = PREF_SPECIAL.get(pref_code) or PREF_TO_GEO.get(pref_code)
        if geo:
            db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', ?, ?, 0.9)",
                       (eid, geo, config['source']))

        existing_labels.add(name)
        new_count += 1

    return new_count


def import_visitor_facilities(db, existing_labels):
    """Import P33 visitor facilities (per-prefecture download)."""
    new_count = 0
    for pref_code in range(1, 48):
        pref_str = f"{pref_code:02d}"
        url = f"https://nlftp.mlit.go.jp/ksj/gml/data/P33/P33-14/P33-14_{pref_str}_GML.zip"

        zip_data = download_zip(url)
        if not zip_data:
            continue

        records = read_shapefile_from_zip(zip_data)

        pref_new = 0
        for record, lat, lon in records:
            name = str(record.get('P33_005', '') or '').strip()
            if not name or len(name) < 2 or name in existing_labels:
                continue

            facility_type = str(record.get('P33_004', '') or '').strip()
            theme_map = {
                '1': 'visual_arts',    # Cinema
                '2': 'community_tradition',  # Public hall
                '3': 'kabuki_theater',  # Theater
                '4': 'visual_arts',    # Exhibition
                '5': 'sports',         # Gymnasium
                '6': 'community_tradition',  # Other
            }
            theme = theme_map.get(facility_type, 'community_tradition')

            cur = db.execute("""
                INSERT INTO entities (label_ja, entity_type, source, lat, lon)
                VALUES (?, 'place', 'kokudo_p33', ?, ?)
            """, (name, lat, lon))
            eid = cur.lastrowid

            db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'kokudo_p33', 0.8)",
                       (eid, theme))
            db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'social', 'kokudo_p33', 0.7)",
                       (eid,))

            geo = PREF_SPECIAL.get(pref_str) or PREF_TO_GEO.get(pref_str)
            if geo:
                db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', ?, 'kokudo_p33', 0.9)",
                           (eid, geo))

            existing_labels.add(name)
            pref_new += 1

        if pref_new > 0:
            print(f"    Pref {pref_str}: {pref_new}", flush=True)
        new_count += pref_new

        db.commit()
        time.sleep(1)  # Rate limit

    return new_count


def import_world_heritage(db, existing_labels, zip_data):
    """Import A34 world heritage sites (has GeoJSON)."""
    # Try GeoJSON first
    records = read_geojson_from_zip(zip_data, 'A34e')
    if not records:
        records = read_geojson_from_zip(zip_data, 'A34b')
    if not records:
        records = read_geojson_from_zip(zip_data)
    if not records:
        # Fall back to shapefile
        records = read_shapefile_from_zip(zip_data)

    print(f"  Parsed {len(records):,} records", flush=True)

    new_count = 0
    for record, lat, lon in records:
        # Try different field names
        name = (record.get('A34e_004') or record.get('A34b_003') or
                record.get('A34a_003') or record.get('構成資産名') or
                record.get('世界文化遺産名') or '').strip()

        heritage_name = (record.get('A34e_003') or record.get('A34b_003') or
                        record.get('世界文化遺産名') or '').strip()

        if not name or len(name) < 2:
            if heritage_name and len(heritage_name) >= 2:
                name = heritage_name
            else:
                continue

        if name in existing_labels:
            continue

        cur = db.execute("""
            INSERT INTO entities (label_ja, entity_type, source, lat, lon)
            VALUES (?, 'place', 'kokudo_a34', ?, ?)
        """, (name, lat, lon))
        eid = cur.lastrowid

        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', 'sacred_profane', 'kokudo_a34', 0.9)", (eid,))
        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', 'community_tradition', 'kokudo_a34', 0.8)", (eid,))
        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'aesthetic', 'kokudo_a34', 0.9)", (eid,))
        db.execute("INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'intellectual', 'kokudo_a34', 0.8)", (eid,))

        existing_labels.add(name)
        new_count += 1

    return new_count


def main():
    db = sqlite3.connect(DB_PATH)

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    total_new = 0

    # 1. Tourism Resources (P12)
    print("\n=== P12: Tourism Resources ===", flush=True)
    zip_data = download_zip(DATASETS['tourism']['url'])
    if zip_data:
        count = import_tourism(db, existing_labels, zip_data)
        db.commit()
        total_new += count
        print(f"  New entities: {count:,}", flush=True)

    # 2. Cultural Properties (P32)
    print("\n=== P32: Cultural Properties ===", flush=True)
    zip_data = download_zip(DATASETS['cultural_properties']['url'])
    if zip_data:
        count = import_cultural_properties(db, existing_labels, zip_data)
        db.commit()
        total_new += count
        print(f"  New entities: {count:,}", flush=True)

    # 3. World Heritage (A34)
    print("\n=== A34: World Heritage ===", flush=True)
    zip_data = download_zip(DATASETS['world_heritage']['url'])
    if zip_data:
        count = import_world_heritage(db, existing_labels, zip_data)
        db.commit()
        total_new += count
        print(f"  New entities: {count:,}", flush=True)

    # 4. Visitor Facilities (P33) - per prefecture
    print("\n=== P33: Visitor Facilities ===", flush=True)
    count = import_visitor_facilities(db, existing_labels)
    total_new += count
    print(f"  New entities: {count:,}", flush=True)

    # Romanize new entities
    print("\n=== Adding English labels ===", flush=True)
    try:
        import pykakasi
        kks = pykakasi.kakasi()
        missing = db.execute("""
            SELECT id, label_ja FROM entities
            WHERE source LIKE 'kokudo_%' AND label_en IS NULL AND label_ja IS NOT NULL
        """).fetchall()

        updated = 0
        for eid, label_ja in missing:
            try:
                result = kks.convert(label_ja)
                romaji = ' '.join(item['hepburn'] for item in result)
                if romaji and romaji != label_ja:
                    db.execute("UPDATE entities SET label_en = ? WHERE id = ?", (romaji, eid))
                    updated += 1
            except:
                pass

        db.commit()
        print(f"  Romanized: {updated:,}", flush=True)
    except ImportError:
        print("  pykakasi not available", flush=True)

    # Final stats
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n=== 国土数値情報 Import Complete ===", flush=True)
    print(f"New entities: {total_new:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
