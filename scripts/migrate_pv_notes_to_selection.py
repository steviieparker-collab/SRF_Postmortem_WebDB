#!/usr/bin/env python3
"""Copy legacy PV Explorer selections from data/pv_notes into the DB.

The event page and PV Explorer now share `event_pv_selection` as the source of
truth for the PV list. Older PV Explorer selections were stored only in
data/pv_notes/{event_id}.json; copy those into the DB for events that do not
already have a DB selection.

Run once after deploying the shared-selection change:

    venv/bin/python scripts/migrate_pv_notes_to_selection.py

The script writes a marker file so a later run does nothing. Use --force only
when you intentionally want to repeat the migration.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "events.db"
NOTES_DIR = PROJECT_ROOT / "data" / "pv_notes"
MARKER = NOTES_DIR / ".migrated_to_event_pv_selection"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS event_pv_selection (
    event_id          TEXT NOT NULL,
    pv                TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    position          INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (event_id, pv)
);
"""


def migrate(db_path: Path, notes_dir: Path, marker: Path,
            force: bool = False) -> dict:
    if marker.exists() and not force:
        return {"skipped": "already migrated"}
    if not db_path.exists():
        return {"error": f"DB not found: {db_path}"}
    if not notes_dir.exists():
        return {"error": f"notes dir not found: {notes_dir}"}

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(CREATE_SQL)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    migrated_events = 0
    migrated_items = 0
    skipped_existing = 0
    try:
        for path in sorted(notes_dir.glob("*.json")):
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            items = doc.get("items") if isinstance(doc, dict) else None
            if not isinstance(items, list) or not items:
                continue

            event_id = path.stem
            has_selection = conn.execute(
                "SELECT 1 FROM event_pv_selection WHERE event_id = ? LIMIT 1",
                (event_id,),
            ).fetchone()
            if has_selection:
                skipped_existing += 1
                continue

            rows = []
            seen = set()
            for item in items:
                if not isinstance(item, dict):
                    continue
                pv = str(item.get("pv") or "").strip()
                if not pv or pv in seen:
                    continue
                seen.add(pv)
                rows.append((
                    event_id,
                    pv,
                    str(item.get("description") or "").strip(),
                    len(rows),
                    now,
                ))
            if not rows:
                continue

            conn.executemany(
                """
                INSERT OR IGNORE INTO event_pv_selection
                    (event_id, pv, description, position, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
            migrated_events += 1
            migrated_items += len(rows)
        conn.commit()
    finally:
        conn.close()

    notes_dir.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        f"migrated_events={migrated_events}\n"
        f"migrated_items={migrated_items}\n"
        f"at={now}\n",
        encoding="utf-8",
    )
    return {
        "migrated_events": migrated_events,
        "migrated_items": migrated_items,
        "skipped_existing": skipped_existing,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--notes-dir", type=Path, default=NOTES_DIR)
    parser.add_argument("--marker", type=Path, default=MARKER)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    result = migrate(args.db, args.notes_dir, args.marker, args.force)
    print(result)
    return 0 if "error" not in result else 1


if __name__ == "__main__":
    raise SystemExit(main())
