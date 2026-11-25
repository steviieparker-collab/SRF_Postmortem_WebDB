"""Caption report images with Qwen2.5-VL-3B on the local GPU.

Reads ``report_images.jsonl`` (deduplicated by doc_id/ord), writes one JSONL
record per image with a Korean caption and the transcribed visible text.

    vlm-env/bin/python caption_report_images.py \
        --images-jsonl report_images.jsonl \
        --images-root srf_report_images \
        --out image_captions.jsonl
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import torch
from transformers import AutoProcessor, Qwen2_5_VLForConditionalGeneration
from qwen_vl_utils import process_vision_info

MODEL_NAME = "Qwen/Qwen2.5-VL-3B-Instruct"

PROMPT = """Analyze this image from an SRF (superconducting RF) accelerator maintenance report.
Report: {title}
Event: {event_id}

Write the answer as a JSON object with exactly two keys:
- "caption": Korean, 1-3 sentences. Describe the image type (alarm table, trend plot, control screen, photo), the main PV/subsystem, visible change or anomaly, and any alarm/fault. Use names visible in the image.
- "text": transcribe all visible text and numbers exactly, Korean and English, line by line. Include PV names, timestamps, alarm text, axis labels, and values. Use "" if nothing is readable.

Do not invent values. caption must be Korean. Output only the JSON object."""


def load_records(images_jsonl: Path, images_root: Path) -> list[dict]:
    seen = {}
    with images_jsonl.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            key = (rec["doc_id"], int(rec["ord"]))
            if key in seen:
                continue
            rel = rec.get("path", "")
            if rel.startswith("images/"):
                rel = rel[len("images/"):]
            rec["_file"] = str(images_root / rel)
            seen[key] = rec
    return list(seen.values())


def parse_sections(raw: str) -> dict:
    """Repair the model's JSON (literal newlines, loose quotes) into a dict."""
    from json_repair import repair_json

    cleaned = raw.strip().replace("```json", "").replace("```", "")
    try:
        data = json.loads(repair_json(cleaned))
    except Exception:
        data = {"caption": cleaned, "text": ""}
    if not isinstance(data, dict):
        data = {"caption": str(data), "text": ""}
    caption = data.get("caption", "")
    text = data.get("text", "")
    if isinstance(caption, list):
        caption = " ".join(str(x) for x in caption)
    if isinstance(text, list):
        text = "\n".join(str(x) for x in text)
    return {
        "caption": str(caption).strip(),
        "text": str(text).strip(),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--images-jsonl", default="report_images.jsonl")
    ap.add_argument("--images-root", default="srf_report_images")
    ap.add_argument("--out", default="image_captions.jsonl")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-pixels", type=int, default=1024 * 28 * 28)
    ap.add_argument("--max-new-tokens", type=int, default=512)
    args = ap.parse_args()

    records = load_records(Path(args.images_jsonl), Path(args.images_root))
    if args.limit:
        records = records[: args.limit]

    out_path = Path(args.out)
    done = set()
    if out_path.exists():
        with out_path.open(encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                    if not rec.get("error"):
                        done.add((rec["doc_id"], int(rec["ord"])))
                except Exception:
                    continue
    records = [r for r in records if (r["doc_id"], int(r["ord"])) not in done]
    print(f"images to caption: {len(records)} (resume skipped {len(done)})",
          flush=True)

    print("loading model...", flush=True)
    t0 = time.time()
    model = Qwen2_5_VLForConditionalGeneration.from_pretrained(
        MODEL_NAME, torch_dtype=torch.bfloat16, device_map="cuda"
    )
    processor = AutoProcessor.from_pretrained(MODEL_NAME)
    print("model loaded in", round(time.time() - t0, 1), "s", flush=True)

    started = time.time()
    with out_path.open("a", encoding="utf-8") as out:
        for i, rec in enumerate(records, 1):
            path = Path(rec["_file"])
            title = rec.get("original_name", "")
            prompt = PROMPT.format(title=title, event_id=rec.get("event_id", ""))
            t = time.time()
            try:
                messages = [{
                    "role": "user",
                    "content": [
                        {"type": "image", "image": str(path),
                         "min_pixels": 256 * 28 * 28,
                         "max_pixels": args.max_pixels},
                        {"type": "text", "text": prompt},
                    ],
                }]
                image_inputs, video_inputs = process_vision_info(messages)
                text = processor.apply_chat_template(
                    messages, tokenize=False, add_generation_prompt=True
                )
                inputs = processor(
                    text=[text], images=image_inputs, videos=video_inputs,
                    padding=True, return_tensors="pt",
                ).to("cuda")
                with torch.inference_mode():
                    generated = model.generate(
                        **inputs, max_new_tokens=args.max_new_tokens,
                        do_sample=False,
                    )
                raw = processor.batch_decode(
                    generated[:, inputs.input_ids.shape[1]:],
                    skip_special_tokens=True,
                )[0].strip()
                data = parse_sections(raw)
                result = {
                    "doc_id": rec["doc_id"],
                    "ord": int(rec["ord"]),
                    "event_id": rec.get("event_id", ""),
                    "original_name": title,
                    "format": rec.get("format", ""),
                    "bytes": rec.get("bytes"),
                    "path": rec.get("path", ""),
                    "caption": str(data.get("caption", "")).strip(),
                    "ocr_text": str(data.get("text", "")).strip(),
                    "model": MODEL_NAME,
                    "elapsed_s": round(time.time() - t, 2),
                    "error": None,
                }
            except Exception as exc:  # noqa: BLE001 - keep the batch going
                result = {
                    "doc_id": rec["doc_id"],
                    "ord": int(rec["ord"]),
                    "event_id": rec.get("event_id", ""),
                    "original_name": title,
                    "path": rec.get("path", ""),
                    "caption": "",
                    "ocr_text": "",
                    "model": MODEL_NAME,
                    "elapsed_s": round(time.time() - t, 2),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            out.write(json.dumps(result, ensure_ascii=False) + "\n")
            out.flush()
            if i % 5 == 0 or i == len(records):
                rate = (time.time() - started) / i
                eta = rate * (len(records) - i)
                print(f"  {i}/{len(records)}  {rate:.1f}s/img  "
                      f"eta {eta/60:.1f}min", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
