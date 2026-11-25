#!/usr/bin/env python3
"""Extract text and images from report attachments (Task 4).

Input is ``attachment_manifest.csv`` (event_id, original_name, stored_name,
mime_type, path).  Output is a set of JSONL records plus an on-disk image tree:

    data/reports/report_documents.jsonl
    data/reports/report_images.jsonl
    data/reports/report_parse_manifest.csv
    data/reports/report_parse_validation.json
    data/reports/images/<doc_id>/<ord>.<ext>

Parsers
-------
HWP    pyhwp ``hwp5txt`` for text; ``olefile`` + raw-deflate for BinData images
HWPX   zip + OWPML XML (``<hp:t>`` text; ``BinData/*`` images)
PDF    PyMuPDF text per page; embedded images deduplicated by xref
DOCX   zip + ``word/document.xml`` (``<w:t>``); ``word/media/*`` images
PNG    image-only attachment; no text layer

HWP BinData streams are raw-deflate compressed when the document is
compressed (the common case).  We try raw deflate, then zlib, then treat the
bytes as-is; only streams whose magic matches a known image format are kept.

Run with the report venv (pyhwp + PyMuPDF):
    venv-report/bin/python scripts/extract_report_text.py \
        --manifest /tmp/attachment_manifest.csv --limit 5
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_VERSION = 1

IMAGE_MAGIC = (
    (b"\x89PNG\r\n\x1a\n", "png"),
    (b"\xff\xd8\xff", "jpg"),
    (b"BM", "bmp"),
    (b"GIF87a", "gif"),
    (b"GIF89a", "gif"),
    (b"II*\x00", "tif"),
    (b"MM\x00*", "tif"),
    (b"RIFF", "webp"),
)

TEXT_PARSERS = {"hwp", "hwpx", "pdf", "docx"}
SUPPORTED = TEXT_PARSERS | {"png"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def _image_ext(data: bytes) -> Optional[str]:
    for magic, ext in IMAGE_MAGIC:
        if data.startswith(magic):
            return ext
    return None


def _maybe_decompress(raw: bytes) -> bytes:
    """HWP compressed streams use headerless raw deflate."""
    for wbits in (-15, 15, 47):
        try:
            return zlib.decompress(raw, wbits)
        except zlib.error:
            continue
    return raw


def _clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _title(original_name: str) -> str:
    stem = original_name.rsplit(".", 1)[0]
    return " ".join(stem.replace("_", " ").split())


class _HtmlText(HTMLParser):
    """Collect visible text, keeping table cell/row boundaries."""

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script"):
            self._skip += 1
        if tag in ("p", "div", "tr", "br", "li", "h1", "h2", "h3", "table"):
            self.parts.append("\n")
        elif tag in ("td", "th"):
            self.parts.append(" | ")

    def handle_endtag(self, tag):
        if tag in ("style", "script") and self._skip:
            self._skip -= 1
        if tag in ("p", "tr", "table"):
            self.parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(data)


def _html_to_text(html: str) -> str:
    parser = _HtmlText()
    parser.feed(html)
    text = "".join(parser.parts)
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Format parsers.  Each returns (text, page_count, images, warnings), where
# images are tuples (source, ext, data, width, height).
# ---------------------------------------------------------------------------

def _parse_hwp_fallback(path: Path, min_image_bytes: int, warnings: list[str]):
    """Older path: hwp5txt (paragraphs only) + olefile BinData images."""
    exe = Path(sys.executable).with_name("hwp5txt")
    if not exe.exists():
        found = shutil.which("hwp5txt")
        if not found:
            raise RuntimeError("hwp5txt not found (install pyhwp)")
        exe = Path(found)
    proc = subprocess.run([str(exe), str(path)], capture_output=True)
    text = proc.stdout.decode("utf-8", "replace")
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"hwp5txt exit {proc.returncode}: {stderr[:300]}")

    images = []
    try:
        import olefile
    except ImportError:
        warnings.append("olefile missing; HWP images skipped")
        return text, None, images, warnings

    ole = olefile.OleFileIO(str(path))
    try:
        for entry in ole.listdir():
            name = "/".join(entry)
            if not name.startswith("BinData/"):
                continue
            raw = ole.openstream(entry).read()
            data = _maybe_decompress(raw)
            ext = _image_ext(data)
            if not ext:
                warnings.append(f"non-image BinData stream: {name}")
                continue
            if len(data) < min_image_bytes:
                continue
            images.append((name, ext, data, None, None))
    finally:
        ole.close()
    return text, None, images, warnings


def parse_hwp(path: Path, min_image_bytes: int):
    """HWP -> HTML via hwp5html, so table cells are included in the text.

    ``hwp5txt`` only emits paragraph text and replaces tables with ``<표>``,
    which reduced some table-only reports to a few dozen characters.  The HTML
    converter emits the full table structure, so we parse that instead and fall
    back to hwp5txt if conversion fails.
    """
    warnings: list[str] = []
    exe = Path(sys.executable).with_name("hwp5html")
    if not exe.exists():
        found = shutil.which("hwp5html")
        if found:
            exe = Path(found)
        else:
            warnings.append("hwp5html unavailable; using hwp5txt fallback")
            return _parse_hwp_fallback(path, min_image_bytes, warnings)

    tmp = Path(tempfile.mkdtemp(prefix="hwp5html_"))
    out = tmp / "out"
    try:
        proc = subprocess.run(
            [str(exe), f"--output={out}", str(path)], capture_output=True
        )
        index = out / "index.xhtml"
        if proc.returncode != 0 or not index.is_file():
            warnings.append(
                f"hwp5html failed (exit {proc.returncode}); using hwp5txt fallback"
            )
            return _parse_hwp_fallback(path, min_image_bytes, warnings)

        text = _html_to_text(index.read_text(encoding="utf-8", errors="replace"))
        images = []
        bindir = out / "bindata"
        if bindir.is_dir():
            for f in sorted(bindir.iterdir()):
                if not f.is_file():
                    continue
                data = f.read_bytes()
                ext = _image_ext(data)
                if not ext or len(data) < min_image_bytes:
                    continue
                images.append((f"bindata/{f.name}", ext, data, None, None))
        return text, None, images, warnings
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def parse_hwpx(path: Path, min_image_bytes: int):
    warnings: list[str] = []
    parts: list[str] = []
    images = []
    with zipfile.ZipFile(path) as z:
        for name in sorted(z.namelist()):
            if name.startswith("Contents/") and name.endswith(".xml"):
                xml = z.read(name).decode("utf-8", "replace")
                xml = xml.replace("</hp:p>", "\n")
                for match in re.findall(r"<hp:t[^>]*>(.*?)</hp:t>", xml, re.S):
                    parts.append(re.sub(r"<[^>]+>", "", match))
            elif name.startswith("BinData/") or name.startswith("Contents/"):
                ext = name.rsplit(".", 1)[-1].lower()
                if ext in {"png", "jpg", "jpeg", "bmp", "gif", "tif", "tiff"}:
                    data = z.read(name)
                    if len(data) >= min_image_bytes:
                        images.append((name, "jpg" if ext == "jpeg" else ext,
                                       data, None, None))
    return "\n".join(parts), None, images, warnings


def parse_docx(path: Path, min_image_bytes: int):
    warnings: list[str] = []
    with zipfile.ZipFile(path) as z:
        xml = z.read("word/document.xml").decode("utf-8", "replace")
        paragraphs = re.findall(r"<w:p[ >].*?</w:p>", xml, re.S)
        parts = []
        for para in paragraphs:
            runs = re.findall(r"<w:t[^>]*>(.*?)</w:t>", para, re.S)
            if runs:
                parts.append("".join(re.sub(r"<[^>]+>", "", r) for r in runs))
        images = []
        for name in sorted(z.namelist()):
            if not name.startswith("word/media/"):
                continue
            data = z.read(name)
            ext = _image_ext(data)
            if not ext:
                continue
            if len(data) < min_image_bytes:
                continue
            images.append((name, ext, data, None, None))
    return "\n".join(parts), None, images, warnings


def parse_pdf(path: Path, min_image_bytes: int):
    import pymupdf  # local import: only needed for PDFs

    warnings: list[str] = []
    doc = pymupdf.open(path)
    try:
        text = "\n".join(page.get_text("text") for page in doc)
        images = []
        seen: set[int] = set()
        for page in doc:
            for info in page.get_images(full=True):
                xref = info[0]
                if xref in seen:
                    continue
                seen.add(xref)
                try:
                    img = doc.extract_image(xref)
                except Exception:
                    continue
                data = img.get("image") or b""
                if len(data) < min_image_bytes:
                    continue
                ext = img.get("ext") or _image_ext(data) or "bin"
                if ext == "jpeg":
                    ext = "jpg"
                images.append((f"xref{xref}", ext, data,
                               img.get("width"), img.get("height")))
        return text, doc.page_count, images, warnings
    finally:
        doc.close()


def parse_png(path: Path, min_image_bytes: int):
    data = path.read_bytes()
    return "", 1, [("file", "png", data, None, None)], []


PARSERS = {
    "hwp": parse_hwp,
    "hwpx": parse_hwpx,
    "docx": parse_docx,
    "pdf": parse_pdf,
    "png": parse_png,
}


# ---------------------------------------------------------------------------
# One attachment
# ---------------------------------------------------------------------------

def extract_one(
    row: dict[str, str],
    images_dir: Path,
    min_image_bytes: int,
) -> dict[str, Any]:
    started = time.time()
    path = Path(row["path"])
    original_name = row.get("original_name") or path.name
    ext = path.suffix.lower().lstrip(".")
    warnings: list[str] = []
    error = None
    text = ""
    page_count = None
    images: list[tuple] = []

    if not path.is_file():
        error = "file not found"
    elif ext not in SUPPORTED:
        error = f"unsupported extension: {ext or '(none)'}"
    else:
        try:
            text, page_count, images, warnings = PARSERS[ext](path, min_image_bytes)
        except Exception as exc:  # noqa: BLE001 - record, do not abort the batch
            error = f"{type(exc).__name__}: {exc}"

    text = _clean_text(text or "")
    sha = _sha256_file(path) if path.is_file() else ""
    doc_id = sha or _sha256_bytes(str(path).encode())

    image_records = []
    if not error:
        dest_dir = images_dir / doc_id
        seen_hashes: set[str] = set()
        for ord_, (source, img_ext, data, width, height) in enumerate(images):
            digest = _sha256_bytes(data)
            if digest in seen_hashes:
                continue
            seen_hashes.add(digest)
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / f"{ord_:04d}.{img_ext}"
            dest.write_bytes(data)
            image_records.append({
                "doc_id": doc_id,
                "ord": ord_,
                "source": source,
                "format": img_ext,
                "width": width,
                "height": height,
                "bytes": len(data),
                "sha256": digest,
                "path": str(dest.relative_to(images_dir.parent)),
            })

    if error:
        status = "failed"
    elif text:
        status = "ok"
    elif image_records:
        status = "partial"  # images only (e.g. scanned PDF, PNG attachment)
    else:
        status = "empty"
        warnings.append("no text or images extracted")

    return {
        "schema_version": SCHEMA_VERSION,
        "doc_id": doc_id,
        "event_id": row.get("event_id", ""),
        "timestamp": row.get("timestamp", ""),
        "original_name": original_name,
        "stored_name": row.get("stored_name", path.name),
        "mime_type": row.get("mime_type", ""),
        "source_path": str(path),
        "ext": ext,
        "file_size": path.stat().st_size if path.is_file() else 0,
        "sha256": sha,
        "title": _title(original_name),
        "text": text,
        "text_chars": len(text),
        "page_count": page_count,
        "status": status,
        "error": error,
        "warnings": warnings,
        "image_count": len(image_records),
        "images": image_records,
        "parser": f"{ext}_v1",
        "extract_seconds": round(time.time() - started, 3),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--out-dir", default=str(ROOT / "data" / "reports"))
    ap.add_argument("--images-dir", default=None,
                    help="default: <out-dir>/images")
    ap.add_argument("--min-image-bytes", type=int, default=4096)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--events", default="",
                    help="comma-separated event IDs")
    ap.add_argument("--force", action="store_true",
                    help="re-extract even if output exists")
    args = ap.parse_args()

    manifest = Path(args.manifest)
    out_dir = Path(args.out_dir)
    images_dir = Path(args.images_dir) if args.images_dir else out_dir / "images"
    out_dir.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(parents=True, exist_ok=True)

    with manifest.open(newline="", encoding="utf-8-sig") as fh:
        rows = [r for r in csv.DictReader(fh) if r.get("path")]
    only = {x.strip() for x in args.events.split(",") if x.strip()}
    if only:
        rows = [r for r in rows if r.get("event_id") in only]
    if args.limit:
        rows = rows[: args.limit]
    if not rows:
        sys.stderr.write("no attachments selected\n")
        return 2

    started = time.time()
    results: list[dict[str, Any]] = []
    print(f"extracting {len(rows)} attachments with {args.workers} workers",
          flush=True)
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(extract_one, row, images_dir, args.min_image_bytes): row
            for row in rows
        }
        done = 0
        for future in as_completed(futures):
            done += 1
            try:
                results.append(future.result())
            except Exception as exc:  # noqa: BLE001
                row = futures[future]
                results.append({
                    "schema_version": SCHEMA_VERSION,
                    "doc_id": "",
                    "event_id": row.get("event_id", ""),
                    "original_name": row.get("original_name", ""),
                    "stored_name": row.get("stored_name", ""),
                    "source_path": row.get("path", ""),
                    "ext": Path(row.get("path", "")).suffix.lstrip("."),
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                    "warnings": [],
                    "text": "", "text_chars": 0,
                    "image_count": 0, "images": [],
                    "file_size": 0, "sha256": "",
                })
            if done % 10 == 0 or done == len(rows):
                print(f"  {done}/{len(rows)}", flush=True)

    results.sort(key=lambda r: (r["event_id"], r["original_name"]))

    docs_path = out_dir / "report_documents.jsonl"
    images_path = out_dir / "report_images.jsonl"
    manifest_path = out_dir / "report_parse_manifest.csv"
    validation_path = out_dir / "report_parse_validation.json"

    with docs_path.open("w", encoding="utf-8") as fh:
        for rec in results:
            fh.write(json.dumps(rec, ensure_ascii=False, allow_nan=False,
                                sort_keys=True))
            fh.write("\n")

    with images_path.open("w", encoding="utf-8") as fh:
        for rec in results:
            for img in rec["images"]:
                out = {"doc_id": rec["doc_id"], "event_id": rec["event_id"],
                       "original_name": rec["original_name"], **img}
                fh.write(json.dumps(out, ensure_ascii=False, allow_nan=False,
                                    sort_keys=True))
                fh.write("\n")

    fields = ["event_id", "original_name", "stored_name", "ext", "status",
              "text_chars", "image_count", "page_count", "file_size",
              "doc_id", "parser", "error", "warnings", "extract_seconds"]
    with manifest_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for rec in results:
            writer.writerow({
                "event_id": rec["event_id"],
                "original_name": rec["original_name"],
                "stored_name": rec.get("stored_name", ""),
                "ext": rec["ext"],
                "status": rec["status"],
                "text_chars": rec["text_chars"],
                "image_count": rec["image_count"],
                "page_count": rec.get("page_count"),
                "file_size": rec.get("file_size", 0),
                "doc_id": rec["doc_id"],
                "parser": rec.get("parser", ""),
                "error": rec.get("error") or "",
                "warnings": "; ".join(rec.get("warnings") or []),
                "extract_seconds": rec.get("extract_seconds"),
            })

    by_status: dict[str, int] = {}
    by_ext: dict[str, int] = {}
    for rec in results:
        by_status[rec["status"]] = by_status.get(rec["status"], 0) + 1
        by_ext[rec["ext"]] = by_ext.get(rec["ext"], 0) + 1
    total_images = sum(r["image_count"] for r in results)
    report = {
        "schema_version": SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "expected": len(rows),
        "ok": by_status.get("ok", 0),
        "partial": by_status.get("partial", 0),
        "empty": by_status.get("empty", 0),
        "failed": by_status.get("failed", 0),
        "by_status": by_status,
        "by_ext": by_ext,
        "documents": len(results),
        "chunks_text_chars": sum(r["text_chars"] for r in results),
        "images": total_images,
        "failed_files": [
            {"event_id": r["event_id"], "original_name": r["original_name"],
             "error": r.get("error")}
            for r in results if r["status"] == "failed"
        ],
        "empty_files": [
            {"event_id": r["event_id"], "original_name": r["original_name"],
             "warnings": r.get("warnings")}
            for r in results if r["status"] == "empty"
        ],
        "elapsed_seconds": round(time.time() - started, 2),
        "outputs": {
            "documents": str(docs_path),
            "images": str(images_path),
            "manifest": str(manifest_path),
            "image_dir": str(images_dir),
        },
    }
    validation_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(json.dumps({k: report[k] for k in
                      ("expected", "ok", "partial", "empty", "failed",
                       "images", "elapsed_seconds")}, ensure_ascii=False))
    if report["failed"]:
        print(f"failed files listed in {validation_path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
