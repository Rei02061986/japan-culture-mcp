"""Phase 2.5 Task 1+2: NDL画像・OCRパイプライン
画図百鬼夜行の画像取得 + OCR構造解析 + 品質テスト + 汎用化テスト
Python 3.8 compatible.
"""
from __future__ import annotations

import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

BASE_DIR = Path(__file__).parent.parent
RESP_DIR = BASE_DIR / "responses" / "phase2_5"
IMG_DIR = RESP_DIR / "images"
RESP_DIR.mkdir(parents=True, exist_ok=True)
IMG_DIR.mkdir(parents=True, exist_ok=True)

UA = "japan-culture-mcp/0.2 (research-project)"
TIMEOUT = 60


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save_json(name, data):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {name}")


def save_text(name, text):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  Saved: {name}")


# ================================================================
# Task 1A: NDL SRU → PID → IIIF Manifest → Image Download
# ================================================================

def search_ndl_sru(query_str, fname):
    """NDL SRUでCQLクエリを実行し、結果からPIDを抽出"""
    print(f"\n  SRU search: {query_str}")
    try:
        resp = requests.get(
            "https://iss.ndl.go.jp/api/sru",
            params={
                "operation": "searchRetrieve",
                "query": query_str,
                "maximumRecords": "10",
                "recordSchema": "dcndl",
            },
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        text = resp.text
        save_text(fname, text)

        # Parse total
        total_m = re.search(r"<numberOfRecords>(\d+)</numberOfRecords>", text)
        total = int(total_m.group(1)) if total_m else 0
        print(f"  Total records: {total}")

        # Extract record identifiers & titles
        records = []
        record_blocks = re.findall(r"<recordData>(.*?)</recordData>", text, re.DOTALL)
        for rec in record_blocks:
            title_m = re.search(r"<dc:title[^>]*>([^<]+)</dc:title>", rec)
            creator_m = re.search(r"<dc:creator[^>]*>([^<]+)</dc:creator>", rec)
            # Look for NDL digital collection identifiers
            ndl_ids = re.findall(r"https?://(?:www\.)?dl\.ndl\.go\.jp/(?:info:ndljp/pid/)?(\d+)", rec)
            # Also look for any identifier
            all_ids = re.findall(r"<dc:identifier[^>]*>([^<]+)</dc:identifier>", rec)
            records.append({
                "title": title_m.group(1) if title_m else "",
                "creator": creator_m.group(1) if creator_m else "",
                "ndl_pids": ndl_ids,
                "identifiers": all_ids,
            })
            if title_m:
                print(f"    {title_m.group(1)} | PIDs: {ndl_ids}")

        return {"total": total, "records": records}
    except Exception as e:
        print(f"  [FAIL] SRU: {e}")
        return {"total": 0, "records": [], "error": str(e)}


def get_iiif_manifest(pid, fname):
    """NDL IIIF Manifestを取得し、Canvas/Image情報を抽出"""
    url = f"https://www.dl.ndl.go.jp/api/iiif/{pid}/manifest.json"
    print(f"\n  IIIF manifest: {url}")
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        resp.raise_for_status()
        manifest = resp.json()
        save_json(fname, manifest)

        label = manifest.get("label", "N/A")
        attribution = manifest.get("attribution", "N/A")
        license_url = manifest.get("license", "N/A")
        metadata = manifest.get("metadata", [])

        print(f"  Label: {label}")
        print(f"  Attribution: {attribution}")
        print(f"  License: {license_url}")

        pages = []
        for seq in manifest.get("sequences", []):
            for canvas in seq.get("canvases", []):
                page_info = {
                    "label": canvas.get("label", ""),
                    "width": canvas.get("width"),
                    "height": canvas.get("height"),
                }
                for image in canvas.get("images", []):
                    resource = image.get("resource", {})
                    service = resource.get("service", {})
                    image_id = service.get("@id", "")
                    profile = service.get("profile", "")
                    page_info["image_base"] = image_id
                    page_info["profile"] = profile
                    if image_id:
                        page_info["thumb_url"] = f"{image_id}/full/200,/0/default.jpg"
                        page_info["mid_url"] = f"{image_id}/full/800,/0/default.jpg"
                        page_info["full_url"] = f"{image_id}/full/full/0/default.jpg"
                        page_info["info_url"] = f"{image_id}/info.json"
                pages.append(page_info)

        result = {
            "pid": pid,
            "manifest_url": url,
            "label": label,
            "attribution": attribution,
            "license": license_url,
            "metadata": metadata,
            "total_pages": len(pages),
            "pages": pages,
            "ts": now_iso(),
        }
        print(f"  Total pages: {len(pages)}")
        return result
    except Exception as e:
        print(f"  [FAIL] Manifest: {e}")
        return {"pid": pid, "error": str(e), "ts": now_iso()}


def download_image(url, filepath, label=""):
    """画像をダウンロード"""
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT, stream=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        size = len(resp.content)
        with open(filepath, "wb") as f:
            f.write(resp.content)
        print(f"    [{label}] {size:,} bytes, {ct} → {filepath.name}")
        return {"ok": True, "size": size, "content_type": ct}
    except Exception as e:
        print(f"    [{label}] FAIL: {e}")
        return {"ok": False, "error": str(e)}


def test_ndl_image_pipeline():
    print("\n" + "=" * 70)
    print("Task 1A: NDL画像パイプライン — 画図百鬼夜行")
    print("=" * 70)

    # Step 1: SRU search
    sru_result = search_ndl_sru(
        'anywhere="画図百鬼夜行" AND anywhere="鳥山石燕"',
        "ndl_hyakkiyako_search.xml",
    )

    # Step 2: Try to find PIDs
    all_pids = []
    for rec in sru_result.get("records", []):
        all_pids.extend(rec.get("ndl_pids", []))

    # Also try known PIDs for 画図百鬼夜行
    known_pids = ["1312139", "2551502", "1303387"]
    test_pids = list(set(all_pids + known_pids))
    print(f"\n  PIDs to test: {test_pids}")

    manifest_results = {}
    for pid in test_pids[:5]:
        result = get_iiif_manifest(pid, f"ndl_manifest_{pid}.json")
        manifest_results[pid] = result
        if result.get("total_pages", 0) > 0:
            print(f"  ✓ PID {pid}: {result['label']} ({result['total_pages']} pages)")
        time.sleep(0.5)

    # Step 3: Download images from first valid manifest
    image_results = []
    for pid, mresult in manifest_results.items():
        pages = mresult.get("pages", [])
        if not pages:
            continue
        print(f"\n  Downloading images from PID {pid} ({mresult.get('label', 'N/A')})...")

        for i, page in enumerate(pages[:3]):
            prefix = f"pid{pid}_p{i+1}"
            for size_label, url_key in [("thumb", "thumb_url"), ("mid", "mid_url")]:
                url = page.get(url_key, "")
                if url:
                    filepath = IMG_DIR / f"{prefix}_{size_label}.jpg"
                    dl_result = download_image(url, filepath, f"p{i+1}_{size_label}")
                    image_results.append({
                        "pid": pid,
                        "page": i + 1,
                        "size": size_label,
                        "url": url,
                        **dl_result,
                    })
            time.sleep(0.3)
        break  # Only download from first valid manifest

    # Save comprehensive result
    save_json("task1a_ndl_image_pipeline.json", {
        "sru_result": sru_result,
        "manifest_results": {k: {kk: vv for kk, vv in v.items() if kk != "pages"} for k, v in manifest_results.items()},
        "manifest_page_counts": {k: v.get("total_pages", 0) for k, v in manifest_results.items()},
        "image_downloads": image_results,
        "ts": now_iso(),
    })

    return manifest_results


# ================================================================
# Task 1B: 汎用化テスト（複数作品）
# ================================================================

def test_ndl_generalization():
    print("\n" + "=" * 70)
    print("Task 1B: NDL画像パイプライン汎用化テスト")
    print("=" * 70)

    test_works = [
        {
            "name": "富嶽三十六景",
            "query": 'anywhere="富嶽三十六景" AND anywhere="葛飾北斎"',
            "known_pids": ["1312413", "1310387"],
        },
        {
            "name": "名所江戸百景",
            "query": 'anywhere="名所江戸百景" AND anywhere="歌川広重"',
            "known_pids": ["1312694"],
        },
        {
            "name": "北斎漫画",
            "query": 'anywhere="北斎漫画"',
            "known_pids": ["1286328"],
        },
    ]

    generalization_results = []

    for work in test_works:
        print(f"\n--- {work['name']} ---")
        # SRU search
        sru = search_ndl_sru(work["query"], f"ndl_sru_{work['name']}.xml")
        time.sleep(1)

        # Try manifests
        all_pids = []
        for rec in sru.get("records", []):
            all_pids.extend(rec.get("ndl_pids", []))
        all_pids = list(set(all_pids + work.get("known_pids", [])))

        manifest_ok = False
        manifest_info = {}
        for pid in all_pids[:3]:
            mresult = get_iiif_manifest(pid, f"ndl_manifest_{pid}.json")
            if mresult.get("total_pages", 0) > 0:
                manifest_ok = True
                manifest_info = {
                    "pid": pid,
                    "label": mresult.get("label", ""),
                    "pages": mresult.get("total_pages", 0),
                    "license": mresult.get("license", ""),
                    "has_images": any(p.get("image_base") for p in mresult.get("pages", [])),
                    "sample_image_base": mresult.get("pages", [{}])[0].get("image_base", "") if mresult.get("pages") else "",
                }
                # Download one thumbnail
                if mresult.get("pages") and mresult["pages"][0].get("thumb_url"):
                    dl_path = IMG_DIR / f"{work['name']}_thumb.jpg"
                    download_image(mresult["pages"][0]["thumb_url"], dl_path, work["name"])
                break
            time.sleep(0.5)

        generalization_results.append({
            "name": work["name"],
            "sru_total": sru.get("total", 0),
            "pids_found": all_pids[:5],
            "manifest_ok": manifest_ok,
            "manifest_info": manifest_info,
        })
        time.sleep(1)

    save_json("task1b_generalization.json", {
        "results": generalization_results,
        "ts": now_iso(),
    })
    return generalization_results


# ================================================================
# Task 2A: OCR JSON構造解析
# ================================================================

def test_ocr_structure():
    print("\n" + "=" * 70)
    print("Task 2A: NDL OCR JSON構造解析")
    print("=" * 70)

    test_pids = ["897115", "1312139", "1286328"]
    ocr_results = []

    for pid in test_pids:
        url = f"https://lab.ndl.go.jp/dl/api/book/fulltext-json/{pid}"
        print(f"\n  OCR JSON: {url}")
        try:
            resp = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            save_json(f"ndl_ocr_{pid}.json", data)

            # Analyze structure
            structure = {
                "pid": pid,
                "ok": True,
                "top_type": type(data).__name__,
                "size_bytes": len(resp.content),
            }

            if isinstance(data, list):
                structure["page_count"] = len(data)
                structure["page_types"] = list(set(type(p).__name__ for p in data[:10]))
                if data:
                    first_page = data[0]
                    if isinstance(first_page, dict):
                        structure["page_keys"] = list(first_page.keys())
                        # Dive into contents
                        contents = first_page.get("contents", [])
                        if contents:
                            structure["contents_count_p1"] = len(contents)
                            first_content = contents[0]
                            if isinstance(first_content, dict):
                                structure["content_keys"] = list(first_content.keys())
                                # Extract sample text
                                sample_text = first_content.get("text", "")[:200]
                                structure["sample_text_p1"] = sample_text
                                # Check for coordinate info
                                has_coords = any(k in first_content for k in ["x", "y", "w", "h", "bbox", "coordinates", "region"])
                                structure["has_coordinates"] = has_coords
                            elif isinstance(first_content, str):
                                structure["content_type"] = "string"
                                structure["sample_text_p1"] = first_content[:200]
                        # Extract all text from first 3 pages
                        all_text_pages = []
                        for page in data[:3]:
                            if isinstance(page, dict):
                                page_text = ""
                                for block in page.get("contents", []):
                                    if isinstance(block, dict):
                                        page_text += block.get("text", "") + "\n"
                                    elif isinstance(block, str):
                                        page_text += block + "\n"
                                all_text_pages.append(page_text.strip())
                        structure["text_preview_3pages"] = all_text_pages
            elif isinstance(data, dict):
                structure["top_keys"] = list(data.keys())
                # Check common structures
                for key in ["pages", "results", "data", "text"]:
                    if key in data:
                        val = data[key]
                        structure[f"{key}_type"] = type(val).__name__
                        if isinstance(val, list):
                            structure[f"{key}_count"] = len(val)

            ocr_results.append(structure)
            print(f"  Structure: {structure.get('top_type')}, pages={structure.get('page_count', 'N/A')}")

        except Exception as e:
            ocr_results.append({"pid": pid, "ok": False, "error": str(e)})
            print(f"  [FAIL] {e}")
        time.sleep(1)

    save_json("task2a_ocr_structure.json", {"results": ocr_results, "ts": now_iso()})
    return ocr_results


# ================================================================
# Task 2B: OCR品質テスト
# ================================================================

def test_ocr_quality(ocr_results):
    print("\n" + "=" * 70)
    print("Task 2B: NDL OCR品質評価")
    print("=" * 70)

    quality_report = []
    for result in ocr_results:
        if not result.get("ok"):
            continue
        pid = result["pid"]
        texts = result.get("text_preview_3pages", [])
        if not texts:
            continue

        assessment = {
            "pid": pid,
            "pages_analyzed": len(texts),
        }

        for i, text in enumerate(texts):
            chars = len(text)
            # Basic metrics
            cjk_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff' or '\u3040' <= c <= '\u309f' or '\u30a0' <= c <= '\u30ff')
            ascii_chars = sum(1 for c in text if c.isascii() and c.isalpha())
            unknown_chars = sum(1 for c in text if ord(c) > 0xffff)  # Possible mojibake
            lines = text.count('\n') + 1

            assessment[f"page_{i+1}"] = {
                "total_chars": chars,
                "cjk_chars": cjk_chars,
                "ascii_chars": ascii_chars,
                "suspicious_chars": unknown_chars,
                "lines": lines,
                "cjk_ratio": round(cjk_chars / max(chars, 1), 3),
                "text_sample": text[:500],
            }

        quality_report.append(assessment)
        print(f"  PID {pid}: {len(texts)} pages analyzed")

    save_json("task2b_ocr_quality.json", {"results": quality_report, "ts": now_iso()})

    # Generate markdown report
    md_lines = ["# OCR品質レポート\n"]
    md_lines.append(f"生成日: {now_iso()}\n")
    for q in quality_report:
        md_lines.append(f"\n## PID: {q['pid']}")
        md_lines.append(f"分析ページ数: {q['pages_analyzed']}\n")
        for key, val in q.items():
            if key.startswith("page_") and isinstance(val, dict):
                md_lines.append(f"\n### {key}")
                md_lines.append(f"- 総文字数: {val['total_chars']}")
                md_lines.append(f"- CJK文字数: {val['cjk_chars']} ({val['cjk_ratio']*100:.1f}%)")
                md_lines.append(f"- 行数: {val['lines']}")
                md_lines.append(f"- 不審文字数: {val['suspicious_chars']}")
                md_lines.append(f"\n```\n{val['text_sample']}\n```\n")

    save_text("ocr_quality_report.md", "\n".join(md_lines))
    return quality_report


# ================================================================
# Task 2C: OCR ZIP (座標付き)
# ================================================================

def test_ocr_zip():
    print("\n" + "=" * 70)
    print("Task 2C: NDL OCR ZIP (座標付き)")
    print("=" * 70)

    pids = ["897115"]
    zip_results = []

    for pid in pids:
        url = f"https://lab.ndl.go.jp/dl/api/book/fulltext-zip/{pid}"
        print(f"\n  OCR ZIP: {url}")
        try:
            resp = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
            ct = resp.headers.get("Content-Type", "")
            size = len(resp.content)
            print(f"  Status: {resp.status_code}, Content-Type: {ct}, Size: {size:,} bytes")

            if resp.ok and ("zip" in ct or "octet" in ct or size > 100):
                zip_path = RESP_DIR / f"ndl_ocr_zip_{pid}.zip"
                with open(zip_path, "wb") as f:
                    f.write(resp.content)
                print(f"  Saved ZIP: {zip_path.name}")

                # Try to extract and analyze
                import zipfile
                extract_dir = RESP_DIR / f"ocr_zip_{pid}"
                extract_dir.mkdir(parents=True, exist_ok=True)
                try:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        file_list = zf.namelist()
                        print(f"  ZIP contents: {len(file_list)} files")
                        for fn in file_list[:10]:
                            info = zf.getinfo(fn)
                            print(f"    {fn}: {info.file_size:,} bytes")
                        zf.extractall(extract_dir)

                        # Read first JSON file
                        json_files = [f for f in file_list if f.endswith(".json")]
                        if json_files:
                            with zf.open(json_files[0]) as jf:
                                coord_data = json.loads(jf.read())
                                save_json(f"ocr_zip_{pid}_sample.json", coord_data)
                                if isinstance(coord_data, dict):
                                    print(f"  Coord JSON keys: {list(coord_data.keys())[:10]}")
                                elif isinstance(coord_data, list) and coord_data:
                                    first = coord_data[0]
                                    if isinstance(first, dict):
                                        print(f"  Coord JSON[0] keys: {list(first.keys())[:10]}")

                    zip_results.append({"pid": pid, "ok": True, "files": len(file_list), "file_list": file_list[:20]})
                except zipfile.BadZipFile:
                    print(f"  Not a valid ZIP (maybe HTML error page)")
                    # Save as text to inspect
                    save_text(f"ocr_zip_{pid}_response.txt", resp.text[:5000])
                    zip_results.append({"pid": pid, "ok": False, "error": "Not a valid ZIP", "response_preview": resp.text[:500]})
            else:
                print(f"  Not a ZIP response: {ct}")
                save_text(f"ocr_zip_{pid}_response.txt", resp.text[:5000] if resp.text else "(binary)")
                zip_results.append({"pid": pid, "ok": False, "status": resp.status_code, "content_type": ct})
        except Exception as e:
            print(f"  [FAIL] {e}")
            zip_results.append({"pid": pid, "ok": False, "error": str(e)})
        time.sleep(1)

    save_json("task2c_ocr_zip.json", {"results": zip_results, "ts": now_iso()})
    return zip_results


# ================================================================
# Main
# ================================================================

def main():
    print("=" * 70)
    print("Phase 2.5 Tasks 1+2: NDL画像・OCRパイプライン")
    print(f"Started: {now_iso()}")
    print(f"Output: {RESP_DIR}")
    print("=" * 70)

    # Task 1A: Image pipeline
    manifest_results = test_ndl_image_pipeline()
    time.sleep(2)

    # Task 1B: Generalization
    gen_results = test_ndl_generalization()
    time.sleep(2)

    # Task 2A: OCR structure
    ocr_results = test_ocr_structure()
    time.sleep(1)

    # Task 2B: OCR quality
    quality = test_ocr_quality(ocr_results)

    # Task 2C: OCR ZIP
    zip_results = test_ocr_zip()

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"  Manifests tested: {len(manifest_results)}")
    print(f"  OCR PIDs tested: {len(ocr_results)}")
    files = sorted(RESP_DIR.glob("*.json"))
    print(f"  Output files: {len(files)}")
    images = sorted(IMG_DIR.glob("*.jpg"))
    print(f"  Images downloaded: {len(images)}")
    for img in images:
        print(f"    {img.name}: {img.stat().st_size:,} bytes")
    print(f"\nCompleted: {now_iso()}")


if __name__ == "__main__":
    main()
