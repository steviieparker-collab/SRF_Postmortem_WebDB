#!/usr/bin/env python3
"""
append-merge.py — Preprocessor + Grouper 통합 스크립트.

3개 scope 폴더의 CSV를 전처리(Parquet)한 뒤, 같은 시각에 생성된 파일끼리
묶어서 하나의 merged parquet 파일로 저장합니다.

Usage:
    python3 append-merge.py --input scope1 scope2 scope3 --output merged
    python3 append-merge.py --input scope1 scope2 scope3 --output merged --limit 5
    python3 append-merge.py -i scope1 scope2 scope3 -o merged --window 180
"""

import argparse
import shutil
import sys
import time
from pathlib import Path
from typing import List, Optional

# ── 프로젝트 루트를 sys.path에 추가 (import 전에 수행) ──────
# 이 파일: SRF_postmortem/data/append/append-merge.py
# 루트:   SRF_postmortem/  (= data/append/../..)
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
sys.path.insert(0, _PROJECT_ROOT)

from src.core.config import get_config
from src.core.logger import get_logger
from src.pipeline.preprocessor import Preprocessor
from src.pipeline.grouper import Grouper

logger = get_logger("append-merge")


def run_append_merge(
    input_dirs: List[Path],
    output_dir: Path,
    work_dir: Optional[Path] = None,
    limit: Optional[int] = None,
    window_s: float = 180.0,
    keep_parquet: bool = False,
) -> dict:
    """
    Preprocess CSVs → merge by timestamp → save to output.

    Args:
        input_dirs: 3 scope directories containing CSV files.
        output_dir: Where merged parquet files will be saved.
        work_dir:   Temporary working directory (default: output_dir/_work).
        limit:      Max files per scope to process (newest first).
        window_s:   Timestamp matching window in seconds.
        keep_parquet: If True, keep intermediate scope parquet files.

    Returns:
        Statistics dict.
    """
    start_time = time.time()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if work_dir is None:
        work_dir = output_dir / "_work"
    work_dir = Path(work_dir)

    config = get_config()

    # ─────────── Step 1: Preprocessing ───────────
    preprocessor = Preprocessor(config)
    scope_dirs: List[Path] = []

    for i, inp in enumerate(input_dirs[:3], 1):
        inp = Path(inp)
        if not inp.exists():
            logger.warning(f"Input directory not found, skipping: {inp}")
            continue

        scope_out = work_dir / f"scope{i}"
        logger.info(f"Preprocessing scope{i}: {inp} → {scope_out}")

        result = preprocessor.process_folder(
            input_dir=inp,
            output_dir=scope_out,
            pattern="*.csv",
            limit=limit,
        )

        stats = result.get("stats", {})
        logger.info(
            f"  scope{i} done: {stats.get('success', 0)} success, "
            f"{stats.get('skipped', 0)} skipped"
        )
        scope_dirs.append(scope_out)

    if len(scope_dirs) == 0:
        logger.error("No input directories with data found")
        return {"error": "no input data"}

    # ─────────── Step 2: Grouping / Merging ───────────
    logger.info(f"Grouping from {len(scope_dirs)} scope dirs → {output_dir}")

    grouper = Grouper(
        input_dirs=scope_dirs,
        output_dir=output_dir,
        window_s=window_s,
    )
    grouper_result = grouper.run(limit=None)  # 이미 제한된 파일만 있음

    # ─────────── Step 3: Cleanup ───────────
    if not keep_parquet and work_dir.exists():
        shutil.rmtree(work_dir)
        logger.info(f"Cleaned up working directory: {work_dir}")

    elapsed = time.time() - start_time
    logger.info(f"Done in {elapsed:.1f}s")

    return {
        "elapsed_s": round(elapsed, 1),
        "grouper": grouper_result,
        "output_dir": str(output_dir),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess CSV → merge by timestamp → save merged parquet",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-i", "--input",
        nargs="+",
        required=True,
        help="Scope directories containing CSV files (up to 3: scope1 scope2 scope3)",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output directory for merged parquet files",
    )
    parser.add_argument(
        "--work",
        default=None,
        help="Temporary working directory (default: output/_work)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max CSV files per scope to process (newest first; default: all)",
    )
    parser.add_argument(
        "--window",
        type=float,
        default=180.0,
        help="Timestamp matching window in seconds (default: 180)",
    )
    parser.add_argument(
        "--keep-parquet",
        action="store_true",
        help="Keep intermediate scope parquet files (for debugging)",
    )

    args = parser.parse_args()
    input_dirs = [Path(d) for d in args.input]

    if len(input_dirs) > 3:
        print(f"Warning: Only the first 3 directories will be used ({len(input_dirs)} given)")
        input_dirs = input_dirs[:3]

    result = run_append_merge(
        input_dirs=input_dirs,
        output_dir=Path(args.output),
        work_dir=Path(args.work) if args.work else None,
        limit=args.limit,
        window_s=args.window,
        keep_parquet=args.keep_parquet,
    )

    # Print summary
    g = result.get("grouper", {})
    print(f"\n=== Append-Merge Summary ===")
    print(f"  Total files scanned : {g.get('total_files', 0)}")
    print(f"  Matched events      : {g.get('matched_events', 0)}")
    print(f"  Errors              : {g.get('errors', 0)}")
    print(f"  Elapsed             : {result.get('elapsed_s', 0)}s")
    print(f"  Output              : {result.get('output_dir', '')}")

    sys.exit(0 if g.get('errors', 0) == 0 else 1)


if __name__ == "__main__":
    main()
