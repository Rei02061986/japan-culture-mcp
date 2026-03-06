"""Phase 2.5 Task 3: CODH/NIJL IIIF接続テスト
Python 3.8 compatible.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

BASE_DIR = Path(__file__).parent.parent
RESP_DIR = BASE_DIR / "responses" / "phase2_5"
IMG_DIR = RESP_DIR / "images"
RESP_DIR.mkdir(parents=True, exist_ok=True)
IMG_DIR.mkdir(parents=True, exist_ok=True)

UA = "japan-culture-mcp/0.2 (research-project)"
TIMEOUT = 30


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save_json(name, data):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {name}")


def test_iiif_manifest(label, url, download_thumb=True):
    """汎用IIIFマニフェストテスト"""
    print(f"\n  --- {label} ---")
    print(f"  URL: {url}")
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        size = len(resp.content)

        if "json" not in ct and "ld" not in ct:
            print(f"  Not JSON: {ct} ({size} bytes)")
            return {"label": label, "url": url, "ok": False, "content_type": ct, "size": size}

        data = resp.json()

        # IIIF v2 vs v3 detection
        context = data.get("@context", "")
        iiif_version = "v3" if "iiif.io/api/presentation/3" in str(context) else "v2"

        label_val = data.get("label", "N/A")
        if isinstance(label_val, dict):
            # IIIF v3 style
            label_val = label_val.get("ja", label_val.get("en", [str(label_val)]))[0] if isinstance(label_val.get("ja", label_val.get("en")), list) else str(label_val)

        result = {
            "label": label,
            "manifest_url": url,
            "ok": True,
            "iiif_version": iiif_version,
            "manifest_label": label_val,
            "attribution": data.get("attribution", data.get("requiredStatement", "N/A")),
            "license": data.get("license", data.get("rights", "N/A")),
        }

        # Parse pages
        pages = []
        if iiif_version == "v2":
            for seq in data.get("sequences", []):
                for canvas in seq.get("canvases", []):
                    page = {"label": canvas.get("label", ""), "width": canvas.get("width"), "height": canvas.get("height")}
                    for img in canvas.get("images", []):
                        res = img.get("resource", {})
                        svc = res.get("service", {})
                        base = svc.get("@id", "")
                        if base:
                            page["image_base"] = base
                            page["thumb_url"] = f"{base}/full/200,/0/default.jpg"
                    pages.append(page)
        else:  # v3
            for item in data.get("items", []):
                page = {"label": str(item.get("label", "")), "width": item.get("width"), "height": item.get("height")}
                for anno_page in item.get("items", []):
                    for anno in anno_page.get("items", []):
                        body = anno.get("body", {})
                        svc = body.get("service", [{}])
                        if isinstance(svc, list) and svc:
                            base = svc[0].get("id", svc[0].get("@id", ""))
                        elif isinstance(svc, dict):
                            base = svc.get("id", svc.get("@id", ""))
                        else:
                            base = ""
                        if base:
                            page["image_base"] = base
                            page["thumb_url"] = f"{base}/full/200,/0/default.jpg"
                pages.append(page)

        result["total_pages"] = len(pages)
        result["sample_pages"] = pages[:3]
        print(f"  [OK] {label_val} — {len(pages)} pages, IIIF {iiif_version}")

        # Download one thumbnail
        if download_thumb and pages and pages[0].get("thumb_url"):
            thumb_url = pages[0]["thumb_url"]
            safe_label = label.replace("/", "_").replace(" ", "_")[:30]
            thumb_path = IMG_DIR / f"codh_{safe_label}_thumb.jpg"
            try:
                tresp = requests.get(thumb_url, headers={"User-Agent": UA}, timeout=TIMEOUT)
                if tresp.ok and len(tresp.content) > 100:
                    with open(thumb_path, "wb") as f:
                        f.write(tresp.content)
                    result["thumb_downloaded"] = True
                    result["thumb_size"] = len(tresp.content)
                    print(f"  Thumbnail: {len(tresp.content):,} bytes → {thumb_path.name}")
                else:
                    result["thumb_downloaded"] = False
                    print(f"  Thumbnail failed: HTTP {tresp.status_code}")
            except Exception as e:
                result["thumb_downloaded"] = False
                print(f"  Thumbnail failed: {e}")

        return result
    except Exception as e:
        print(f"  [FAIL] {e}")
        return {"label": label, "url": url, "ok": False, "error": str(e)}


def main():
    print("=" * 60)
    print("Phase 2.5 Task 3: CODH/NIJL IIIF接続テスト")
    print(f"Started: {now_iso()}")
    print(f"Output: {RESP_DIR}")
    print("=" * 60)

    results = []

    # ── CODH datasets ──
    codh_manifests = [
        ("CODH 日本古典籍 (百鬼夜行絵巻)", "http://codh.rois.ac.jp/pmjt/book/200024363/manifest.json"),
        ("CODH 日本古典籍 (画図百鬼夜行)", "http://codh.rois.ac.jp/pmjt/book/200021660/manifest.json"),
        ("CODH 江戸料理レシピ", "http://codh.rois.ac.jp/edo-cooking/book/200014740/manifest.json"),
    ]

    for label, url in codh_manifests:
        r = test_iiif_manifest(label, url)
        results.append(r)
        time.sleep(1)

    # ── NIJL (国文学研究資料館) ──
    nijl_manifests = [
        ("NIJL 古典籍 百鬼夜行", "https://kotenseki.nijl.ac.jp/biblio/200024363/manifest"),
        ("NIJL 古典籍 sample", "https://kotenseki.nijl.ac.jp/biblio/200021660/manifest"),
    ]

    for label, url in nijl_manifests:
        r = test_iiif_manifest(label, url)
        results.append(r)
        time.sleep(1)

    # ── ColBase (国立博物館) ──
    colbase_manifests = [
        ("ColBase 風神雷神図屏風", "https://colbase.nich.go.jp/collection_items/tnm/A-10471/manifest.json"),
    ]

    for label, url in colbase_manifests:
        r = test_iiif_manifest(label, url)
        results.append(r)
        time.sleep(1)

    # ── NDL for comparison ──
    ndl_manifests = [
        ("NDL 北斎漫画 (比較用)", "https://www.dl.ndl.go.jp/api/iiif/1286328/manifest.json"),
    ]

    for label, url in ndl_manifests:
        r = test_iiif_manifest(label, url)
        results.append(r)
        time.sleep(1)

    # Summary
    save_json("task3_codh_iiif_results.json", {"results": results, "ts": now_iso()})

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    ok_count = sum(1 for r in results if r.get("ok"))
    print(f"  Tested: {len(results)}")
    print(f"  OK: {ok_count}")
    print(f"  FAIL: {len(results) - ok_count}")
    for r in results:
        status = "OK" if r.get("ok") else "FAIL"
        pages = r.get("total_pages", "?")
        ver = r.get("iiif_version", "?")
        print(f"  [{status}] {r['label']}: {pages} pages, IIIF {ver}")
    print(f"\nCompleted: {now_iso()}")


if __name__ == "__main__":
    main()
