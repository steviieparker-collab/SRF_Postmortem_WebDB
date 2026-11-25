#!/usr/bin/env python3
"""Build or refresh the local RAG index from the SRF Postmortem DB.

The events DB is opened read-only; the index is a separate SQLite file
(default: data/rag/rag.sqlite3).

Examples:
    venv/bin/python scripts/rag_ingest.py --full
    venv/bin/python scripts/rag_ingest.py --incremental
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.rag.index import ingest, stats  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true", help="rebuild the whole index")
    mode.add_argument("--incremental", action="store_true",
                      help="only add/update changed documents (default)")
    ap.add_argument("--json", action="store_true", help="print JSON only")
    args = ap.parse_args()

    result = ingest(full=args.full)
    result["stats"] = stats()
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=1))
    else:
        print(f"documents: {result['documents']}")
        print(f"added={result['added']} updated={result['updated']} "
              f"skipped={result['skipped']} removed={result['removed']}")
        print(f"index: {result['index_path']} ({result['index_size']} bytes)")
        st = result["stats"]
        print(f"chunks: {st.get('chunks')} | by_type: {st.get('by_type')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
