"""
Phase 6A4: NDL SRU bulk fetch.
100 per page, 2s interval.
Record data is XML-escaped inside <recordData>, needs unescape + re-parse.
"""

NDL_SRU = "https://iss.ndl.go.jp/api/sru"

SEARCHES = [
    {"name": "ukiyoe", "query": 'anywhere="浮世絵"', "max_records": 5000},
    {"name": "nishikie", "query": 'anywhere="錦絵"', "max_records": 5000},
    {"name": "kotenseki", "query": 'anywhere="古典籍"', "max_records": 5000},
    {"name": "emaki", "query": 'anywhere="絵巻"', "max_records": 3000},
    {"name": "byobu", "query": 'anywhere="屏風"', "max_records": 3000},
    {"name": "yokai_art", "query": 'anywhere="妖怪"', "max_records": 2000},
    {"name": "hokusai", "query": 'anywhere="北斎"', "max_records": 3000},
    {"name": "hiroshige", "query": 'anywhere="広重"', "max_records": 2000},
    {"name": "sharaku", "query": 'anywhere="写楽"', "max_records": 500},
    {"name": "utamaro", "query": 'anywhere="歌麿"', "max_records": 1000},
]

import requests
import xml.etree.ElementTree as ET
import json
import time
import os
import re
import html

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[NDL] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[NDL] {msg}\n")

def fetch_sru(query, start=1, max_records=100):
    params = {
        'operation': 'searchRetrieve',
        'query': query,
        'maximumRecords': max_records,
        'startRecord': start,
        'recordSchema': 'dcndl',
    }

    for attempt in range(3):
        try:
            resp = requests.get(NDL_SRU, params=params, timeout=60)
            if resp.status_code == 200:
                return resp.text
            else:
                log(f"    HTTP {resp.status_code}, attempt {attempt+1}")
                time.sleep(10)
        except Exception as e:
            log_error(f"fetch_sru start={start}: {e}")
            time.sleep(10)
    return None

def parse_sru_response(xml_text):
    if not xml_text:
        return [], 0

    ns = {
        'srw': 'http://www.loc.gov/zing/srw/',
    }

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log_error(f"XML parse error: {e}")
        return [], 0

    total_el = root.find('.//srw:numberOfRecords', ns)
    total = int(total_el.text) if total_el is not None else 0

    records = []
    for rec in root.findall('.//srw:record', ns):
        record_data = rec.find('.//srw:recordData', ns)
        if record_data is None:
            continue

        # The record data content is XML-escaped text
        # Get the inner text (which is escaped XML)
        inner_xml = ''
        if record_data.text:
            inner_xml = record_data.text
        else:
            # Try to get it from serialization
            raw = ET.tostring(record_data, encoding='unicode')
            # Extract content between <recordData> and </recordData>
            m = re.search(r'>(.*)</', raw, re.DOTALL)
            if m:
                inner_xml = m.group(1)

        if not inner_xml.strip():
            continue

        # Unescape HTML entities
        inner_xml = html.unescape(inner_xml)

        item = {}

        try:
            # Parse the inner RDF XML
            rdf_root = ET.fromstring(inner_xml)
            for el in rdf_root.iter():
                tag = el.tag.split('}')[-1] if '}' in el.tag else el.tag

                if tag == 'title' and not item.get('title'):
                    # Check for nested rdf:value
                    for sub in el.iter():
                        subtag = sub.tag.split('}')[-1] if '}' in sub.tag else sub.tag
                        if subtag == 'value' and sub.text:
                            item['title'] = sub.text.strip()
                            break
                    if not item.get('title') and el.text:
                        item['title'] = el.text.strip()

                elif tag == 'name' and not item.get('creator'):
                    if el.text:
                        item['creator'] = el.text.strip()

                elif tag == 'date' and not item.get('date'):
                    if el.text and len(el.text.strip()) >= 4:
                        item['date'] = el.text.strip()

                elif tag == 'BibResource':
                    about = el.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about', '')
                    if about and 'ndlsearch' in about:
                        item['ndl_url'] = about
                        parts = about.rstrip('/').split('/')
                        if parts:
                            item['pid'] = parts[-1]

                elif tag == 'subject' and el.text:
                    if 'subjects' not in item:
                        item['subjects'] = []
                    item['subjects'].append(el.text.strip())

        except ET.ParseError:
            # Fallback: regex extraction from escaped XML
            title_m = re.search(r'<dcterms:title>([^<]+)</dcterms:title>', inner_xml)
            if title_m:
                item['title'] = title_m.group(1).strip()

        if item.get('title'):
            records.append(item)

    return records, total

def fetch_all_for_search(name, query, max_total):
    xml = fetch_sru(query, start=1, max_records=1)
    _, total = parse_sru_response(xml)
    actual_total = min(total, max_total)
    log(f"  Total available: {total:,}, fetching up to {actual_total:,}")

    all_records = []
    start = 1

    while start <= actual_total:
        if start % 500 == 1:
            log(f"  [{name}] start={start}/{actual_total}...")
        xml = fetch_sru(query, start=start, max_records=100)
        records, _ = parse_sru_response(xml)

        if not records:
            # May be temporary - try once more
            time.sleep(5)
            xml = fetch_sru(query, start=start, max_records=100)
            records, _ = parse_sru_response(xml)
            if not records:
                log(f"  [{name}] no records at start={start}, stopping")
                break

        all_records.extend(records)
        start += 100
        time.sleep(2)

    return all_records

def main():
    os.makedirs('data/ndl', exist_ok=True)

    total = 0
    for search in SEARCHES:
        log(f"\n=== NDL: {search['name']} ===")
        results = fetch_all_for_search(search['name'], search['query'], search['max_records'])

        output_path = f'data/ndl/{search["name"]}.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)
        log(f"  Saved: {output_path} ({len(results):,} records)")
        total += len(results)

    log(f"\n=== NDL Bulk Complete: {total:,} total records ===")

if __name__ == "__main__":
    main()
