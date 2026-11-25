#!/usr/bin/env python3
"""Backfill per-event narrow waveform images for the Waveform Viewer.

Sources, in order of preference:

1. NAS scope images (``/mnt/<scope>/tek####_range2.jpg``), which were rendered
   from the raw 1 us CSV.  Only available from 2025-05-29 onward.
2. ``data/graph_only/{W,X,Y}/tek####_narrow.jpg`` for events handled by the
   current monitoring pipeline (before the grouper renames them by event id).
3. The merged parquet (``data/merged/event_<id>.parquet``), rendered into
   narrow ±1 ms images.  Used for events missing 1 or 2 scope images.

The NAS scope folders and the event table use different clocks: the NAS shows
UTC mtimes, while event ids/timestamps are KST.  Matching is therefore done on
absolute epoch seconds after converting the DB timestamp to KST.

Examples::

    venv/bin/python scripts/backfill_waveform_images.py --dry-run
    venv/bin/python scripts/backfill_waveform_images.py
    venv/bin/python scripts/backfill_waveform_images.py --force --since 2025-05-29
"""

from __future__ import annotations

import argparse
import bisect
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline.waveform_images import (  # noqa: E402
    SCOPES,
    copy_image_set,
    ensure_image_set,
    event_id_to_kst,
    existing_images,
    project_root,
    waveforms_dir,
)

NAS_DIRS = {
    "forward": Path("/mnt/forward"),
    "cavity": Path("/mnt/cavity"),
    "reflect": Path("/mnt/reflect"),
}
GRAPH_DIRS = {
    "forward": project_root() / "data" / "graph_only" / "W",
    "cavity": project_root() / "data" / "graph_only" / "X",
    "reflect": project_root() / "data" / "graph_only" / "Y",
}
MERGED_DIR = project_root() / "data" / "merged"


def _scan_images(folder: Path) -> list[tuple[float, Path]]:
    if not folder.is_dir():
        return []
    out = []
    for path in folder.iterdir():
        if path.is_file() and path.name.endswith("_range2.jpg"):
            try:
                out.append((path.stat().st_mtime, path))
            except OSError:
                continue
    out.sort(key=lambda item: item[0])
    return out


def _scan_csvs(folder: Path) -> list[tuple[float, Path]]:
    if not folder.is_dir():
        return []
    out = []
    for path in folder.iterdir():
        if path.is_file() and path.suffix.lower() == ".csv":
            try:
                out.append((path.stat().st_mtime, path))
            except OSError:
                continue
    out.sort(key=lambda item: item[0])
    return out


def _nearest(
    index: list[tuple[float, Path]],
    times: list[float],
    epoch: float,
    max_delta: float,
) -> tuple[float, Path] | None:
    if not index:
        return None
    pos = bisect.bisect_left(times, epoch)
    candidates = [i for i in (pos - 1, pos, pos + 1) if 0 <= i < len(index)]
    if not candidates:
        return None
    best = min(candidates, key=lambda i: abs(index[i][0] - epoch))
    delta = index[best][0] - epoch
    if abs(delta) > max_delta:
        return None
    return delta, index[best][1]


def _resolve_claims(
    candidates: dict[str, dict[str, tuple[float, Path]]],
) -> dict[str, dict[str, Path]]:
    """Give each source file to the closest event; drop the other claimants.

    Two events can be seconds apart (duplicate triggers), and a single
    ``_range2.jpg`` cannot belong to both.  The loser is rendered from the
    parquet later.
    """
    owners: dict[str, dict[Path, tuple[float, str]]] = {s: {} for s in SCOPES}
    for scope, per_event in candidates.items():
        for event_id, (delta, path) in per_event.items():
            current = owners[scope].get(path)
            if current is None or abs(delta) < abs(current[0]):
                owners[scope][path] = (delta, event_id)

    assigned: dict[str, dict[str, Path]] = defaultdict(dict)
    for scope, per_event in candidates.items():
        for event_id, (_, path) in per_event.items():
            owner = owners[scope].get(path)
            if owner and owner[1] == event_id:
                assigned[event_id][scope] = path
    return assigned


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="2025-05-29", help="first event date (YYYY-MM-DD)")
    parser.add_argument("--max-delta", type=float, default=180.0, help="max |mtime - event| seconds")
    parser.add_argument("--force", action="store_true", help="overwrite existing images")
    parser.add_argument("--only-missing", action="store_true", help="skip events that already have 3 images")
    parser.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    parser.add_argument("--no-generate", action="store_true", help="do not render from parquet")
    parser.add_argument("--limit", type=int, default=0, help="process at most N events (debug)")
    args = parser.parse_args()

    db_path = ROOT / "db" / "events.db"
    if not db_path.is_file():
        print(f"database not found: {db_path}")
        return 2
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = list(
            conn.execute(
                "SELECT id, timestamp FROM events WHERE timestamp >= ? ORDER BY timestamp DESC",
                (args.since,),
            )
        )
    finally:
        conn.close()

    if args.limit:
        rows = rows[: args.limit]

    nas_index = {scope: _scan_images(folder) for scope, folder in NAS_DIRS.items()}
    nas_times = {scope: [t for t, _ in index] for scope, index in nas_index.items()}
    csv_index = {scope: _scan_csvs(folder) for scope, folder in NAS_DIRS.items()}
    csv_times = {scope: [t for t, _ in index] for scope, index in csv_index.items()}

    print(f"events since {args.since}: {len(rows)}")
    for scope in SCOPES:
        print(f"  NAS {scope}: {len(nas_index[scope])} narrow jpgs, {len(csv_index[scope])} CSV")

    # ── Pass 1: collect candidates ────────────────────────────
    nas_candidates: dict[str, dict[str, tuple[float, Path]]] = {s: {} for s in SCOPES}
    graph_candidates: dict[str, dict[str, tuple[float, Path]]] = {s: {} for s in SCOPES}
    epochs: dict[str, float] = {}

    for row in rows:
        event_id = row["id"]
        kst = event_id_to_kst(row["timestamp"])
        if kst is None:
            print(f"  ! bad timestamp {event_id}: {row['timestamp']}")
            continue
        epoch = kst.timestamp()
        epochs[event_id] = epoch

        for scope in SCOPES:
            hit = _nearest(nas_index[scope], nas_times[scope], epoch, args.max_delta)
            if hit:
                nas_candidates[scope][event_id] = hit

            csv_hit = _nearest(csv_index[scope], csv_times[scope], epoch, args.max_delta)
            if csv_hit:
                stem = csv_hit[1].stem
                graph_path = GRAPH_DIRS[scope] / f"{stem}_narrow.jpg"
                if graph_path.is_file():
                    graph_candidates[scope][event_id] = (csv_hit[0], graph_path)

    nas_assigned = _resolve_claims(nas_candidates)
    graph_assigned = _resolve_claims(graph_candidates)

    # ── Pass 2: copy / render ─────────────────────────────────
    stats = defaultdict(int)
    manifest: dict[str, dict] = {}

    for row in rows:
        event_id = row["id"]
        if event_id not in epochs:
            continue
        have = existing_images(event_id)
        if args.only_missing and len(have) == len(SCOPES) and not args.force:
            stats["skipped"] += 1
            continue

        nas_sources = nas_assigned.get(event_id, {})
        graph_sources = graph_assigned.get(event_id, {})
        merged = MERGED_DIR / f"event_{event_id}.parquet"

        source_kind = ""
        sources: dict[str, Path] = {}
        if len(nas_sources) == len(SCOPES):
            source_kind = "nas"
            sources = dict(nas_sources)
        elif len(graph_sources) == len(SCOPES):
            source_kind = "graph_only"
            sources = dict(graph_sources)

        action = ""
        if sources:
            action = f"copy:{source_kind}"
            if not args.dry_run:
                copy_image_set(event_id, sources, force=args.force)
        elif merged.is_file() and not args.no_generate:
            action = "generate:parquet"
            if not args.dry_run:
                ensure_image_set(event_id, merged, force=True)
        elif nas_sources or graph_sources:
            partial = dict(nas_sources) or dict(graph_sources)
            action = f"partial:{len(partial)}/3"
            if not args.dry_run:
                copy_image_set(event_id, partial, force=args.force)
        else:
            action = "missing"
            stats["no_source"] += 1

        if action.startswith("copy"):
            stats["copied"] += 1
        elif action.startswith("generate"):
            stats["generated"] += 1
        elif action.startswith("partial"):
            stats["partial"] += 1

        if args.dry_run and action.startswith(("copy", "generate")):
            final = 3
        else:
            final = len(existing_images(event_id))
        manifest[event_id] = {
            "timestamp": row["timestamp"],
            "action": action,
            "images": final,
            "sources": {k: str(v) for k, v in sources.items()},
        }
        if args.dry_run or stats["copied"] + stats["generated"] + stats["partial"] <= 40:
            print(f"  {event_id}  {row['timestamp']}  {action}  ({final}/3)")

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "since": args.since,
        "max_delta_s": args.max_delta,
        "events": len(rows),
        "stats": dict(stats),
        "details": manifest,
    }
    out_dir = waveforms_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "backfill_manifest.json"
    if not args.dry_run:
        manifest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\nsummary:")
    print(json.dumps(dict(stats), ensure_ascii=False, indent=2))
    print(f"manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
