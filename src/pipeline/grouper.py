"""
Grouper module for SRF Event Monitoring System.

Merges parquet files from multiple scopes into single event files based on timestamp
matching within a configurable time window.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import polars as pl

from ..core.config import get_config as get_settings
from ..core.logger import get_logger, ContextLogger
from ..core.utils import split_baseline_std_cols, safe_read_parquet_polars, extract_event_timestamp, compute_file_hash, load_json, save_json
from ..core.channel_utils import classify_columns_polars


# Default baseline suffix
BASELINE_STD_SUFFIX = "_baseline_std"


class Grouper:
    """
    Merges parquet files from multiple input folders into event files.
    """

    def __init__(
        self,
        input_dirs: List[Path],
        output_dir: Path,
        window_s: float = 180.0,
        state_file: Optional[Path] = None,
    ):
        """
        Initialize grouper.

        Args:
            input_dirs: List of directories containing parquet files.
            output_dir: Directory to save merged event files.
            window_s: Time window for matching (seconds).
            state_file: Optional path to state file for resume capability.
        """
        self.input_dirs = input_dirs
        self.output_dir = output_dir
        self.window_s = window_s
        self.state_file = state_file

        self.log = ContextLogger("grouper")
        self.processed_hashes: Dict[str, Path] = {}  # hash -> output path
        self.stats = {
            "total_files": 0,
            "matched_events": 0,
            "unmatched_files": 0,
            "errors": 0,
        }

        # Load previous state if resuming
        if state_file and state_file.exists():
            self._load_state()

    def _load_state(self) -> None:
        """Load processed file hashes from state file."""
        try:
            data = load_json(self.state_file)
            self.processed_hashes = {h: Path(p) for h, p in data.get("processed_hashes", {}).items()}
            self.log.info("Loaded state", extra={"count": len(self.processed_hashes)})
        except Exception as e:
            self.log.warning("Failed to load state", extra={"error": str(e)})

    def _save_state(self) -> None:
        """Save processed file hashes to state file."""
        if not self.state_file:
            return
        data = {
            "processed_hashes": {h: str(p) for h, p in self.processed_hashes.items()},
            "stats": self.stats,
        }
        if save_json(data, self.state_file):
            self.log.debug("State saved", extra={"file": str(self.state_file)})
        else:
            self.log.warning("Failed to save state", extra={"file": str(self.state_file)})

    def scan_folder(self, folder: Path) -> List[Dict[str, Any]]:
        """
        Scan a folder for parquet files and extract event timestamps.

        Returns:
            List of dicts with keys: path, ts (POSIX timestamp), name, folder.
        """
        files = []
        for p in sorted(folder.glob("*.parquet")):
            df = safe_read_parquet_polars(p, columns=["event_timestamp"])
            if df is None:
                self.log.warning("Failed to read file", extra={"path": str(p)})
                self.stats["errors"] += 1
                continue

            ts = extract_event_timestamp(df)
            if ts is None:
                self.log.warning("Missing event_timestamp", extra={"path": str(p)})
                self.stats["errors"] += 1
                continue

            # Skip if already processed
            file_hash = compute_file_hash(p)
            if file_hash in self.processed_hashes:
                self.log.debug("Skipping already processed file", extra={"path": str(p)})
                continue

            files.append({
                "path": p,
                "ts": ts,
                "name": p.name,
                "folder": folder.name,
                "hash": file_hash,
            })
        return sorted(files, key=lambda x: x["ts"])

    def match_files(
        self,
        file_lists: List[List[Dict[str, Any]]],
        folder_names: List[str],
    ) -> List[List[Dict[str, Any]]]:
        """
        Match files across folders within time window.

        Args:
            file_lists: List of file lists per folder, each sorted by timestamp.
            folder_names: Names of folders for logging.

        Returns:
            List of groups, each group is a list of file dicts (one per folder).
        """
        groups = []
        # Use first folder as reference
        ref_list = file_lists[0]
        other_lists = file_lists[1:]

        for ref_file in ref_list:
            group = [ref_file]
            missing_from = []
            matched_indices = []  # indices in other_lists where match found

            for i, other_list in enumerate(other_lists):
                match = None
                for f in other_list:
                    if abs(f["ts"] - ref_file["ts"]) <= self.window_s:
                        match = f
                        break
                if match:
                    group.append(match)
                    matched_indices.append(i)
                else:
                    missing_from.append(folder_names[i + 1])

            if len(group) == len(file_lists):
                # Successful match across all folders
                groups.append(group)
                # Remove matched files from other lists to avoid reuse
                for idx, match_file in zip(matched_indices, group[1:]):
                    other_lists[idx] = [f for f in other_lists[idx] if f["path"] != match_file["path"]]
            else:
                self.log.debug(
                    "Unmatched file",
                    extra={
                        "file": ref_file["name"],
                        "missing_folders": missing_from,
                        "ts": ref_file["ts"],
                    },
                )
                self.stats["unmatched_files"] += 1

        return groups

    def merge_group(self, group: List[Dict[str, Any]]) -> Optional[pl.DataFrame]:
        """
        Merge a group of files into a single DataFrame.

        Args:
            group: List of file dicts.

        Returns:
            Merged DataFrame, or None on failure.
        """
        dfs = []
        for info in group:
            df = safe_read_parquet_polars(info["path"])
            if df is None:
                self.log.error("Failed to read file for merging", extra={"path": str(info["path"])})
                return None
            dfs.append(df)

        # Full join on t_rel_s
        merged = dfs[0]
        existing_cols = set(merged.columns)

        for next_df in dfs[1:]:
            cols_to_add = [
                c for c in next_df.columns
                if c not in existing_cols or c == "t_rel_s"
            ]
            merged = (
                merged
                .join(next_df.select(cols_to_add), on="t_rel_s", how="full", coalesce=True)
                .sort("t_rel_s")
            )
            existing_cols.update(cols_to_add)

        # Null handling
        merged = self._handle_null_values(merged)
        return merged

    def _handle_null_values(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply null handling strategies per column type.

        Args:
            df: DataFrame with possible nulls.

        Returns:
            DataFrame with nulls filled.
        """
        # Split columns
        bstd_cols, other_cols = split_baseline_std_cols(df.columns)
        analog_cols, digital_cols = classify_columns_polars(other_cols)

        # Analog: linear interpolation
        if analog_cols:
            df = df.with_columns([pl.col(c).interpolate() for c in analog_cols])

        # Digital: forward/backward fill, then fill with 0
        if digital_cols:
            for c in digital_cols:
                has_data = df[c].is_not_null().any()
                if has_data:
                    df = df.with_columns(pl.col(c).forward_fill().backward_fill())
                else:
                    df = df.with_columns(pl.col(c).fill_null(0))
            # Final safety fill
            df = df.with_columns([pl.col(c).fill_null(0).cast(pl.Float64) for c in digital_cols])

        # Baseline std: forward/backward fill (scalar values)
        if bstd_cols:
            df = df.with_columns(
                [pl.col(c).forward_fill().backward_fill() for c in bstd_cols]
            )
            # Fill any remaining nulls with 0.0
            df = df.with_columns(
                [pl.col(c).fill_null(0.0).cast(pl.Float32) for c in bstd_cols]
            )

        return df

    def save_group(self, group: List[Dict[str, Any]], merged: pl.DataFrame) -> Path:
        """
        Save merged DataFrame to output directory.

        Args:
            group: Original file group.
            merged: Merged DataFrame.

        Returns:
            Path to saved file.
        """
        # Use timestamp from first file
        base_ts = group[0]["ts"]
        dt_str = datetime.fromtimestamp(base_ts, tz=KST).strftime("%Y%m%d_%H%M%S")
        out_path = self.output_dir / f"event_{dt_str}.parquet"

        merged.write_parquet(out_path, compression="zstd")

        # Record hashes of processed files
        for info in group:
            self.processed_hashes[info["hash"]] = out_path

        sync_diff = max(f["ts"] for f in group) - min(f["ts"] for f in group)
        self.log.info(
            "Event saved",
            extra={
                "path": str(out_path),
                "analog": len([c for c in merged.columns if c.endswith("_v")]),
                "digital": len([c for c in merged.columns if c.endswith("_d")]),
                "baseline_std": len([c for c in merged.columns if c.endswith(BASELINE_STD_SUFFIX)]),
                "total_cols": len(merged.columns),
                "sync_diff": f"{sync_diff:.2f}s",
            },
        )
        return out_path

    def run(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute grouping process.

        Args:
            limit: If set, only use at most N newest files per input folder.

        Returns:
            Statistics dictionary.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.log.info(
            "Starting grouper",
            extra={
                "input_dirs": [str(d) for d in self.input_dirs],
                "output_dir": str(self.output_dir),
                "window_s": self.window_s,
            },
        )

        # Scan folders
        file_lists = []
        folder_names = []
        for d in self.input_dirs:
            self.log.debug("Scanning folder", extra={"path": str(d)})
            files = self.scan_folder(d)
            # Sort newest-first so --limit picks latest files
            files.sort(key=lambda f: f["ts"], reverse=True)
            if limit is not None and limit > 0:
                files = files[:limit]
            file_lists.append(files)
            folder_names.append(d.name)
            self.stats["total_files"] += len(files)

        # Check for empty folders
        if any(len(lst) == 0 for lst in file_lists):
            self.log.error("One or more folders contain no readable parquet files")
            return self.stats

        # Match files — first try full (all-folders) match
        groups = self.match_files(file_lists, folder_names)
        self.stats["matched_events"] = len(groups)

        # Collect remaining unmatched files from each folder (logged, not merged)
        matched_paths = set()
        for g in groups:
            for f in g:
                matched_paths.add(f["path"])
        for fl in file_lists:
            for f in fl:
                if f["path"] not in matched_paths:
                    self.log.warning(
                        "Unmatched file — not merging (requires all scopes)",
                        extra={"file": f["name"], "folder": f["folder"], "ts": f["ts"]},
                    )
                    self.stats["unmatched_files"] += 1

        # Process each group
        for i, group in enumerate(groups):
            print(f"Merging group [{i+1}/{len(groups)}]...", flush=True)
            self.log.debug("Processing group", extra={"index": i + 1, "total": len(groups)})
            merged = self.merge_group(group)
            if merged is None:
                self.stats["errors"] += 1
                continue
            self.save_group(group, merged)

            # Periodic state save (every 10 groups)
            if (i + 1) % 10 == 0:
                self._save_state()

        # Final state save
        self._save_state()

        self.log.info(
            "Grouper completed",
            extra={
                "total_files": self.stats["total_files"],
                "matched_events": self.stats["matched_events"],
                "unmatched_files": self.stats["unmatched_files"],
                "errors": self.stats["errors"],
            },
        )
        return self.stats


# ─────────────────────────────────────────────────────────────
# CLI and pipeline integration
# ─────────────────────────────────────────────────────────────
def run_grouper_from_config(
    config_path: Optional[Path] = None,
    state_file: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Run grouper using configuration from settings.

    Args:
        config_path: Optional path to config YAML (defaults to config/config.yaml).
        state_file: Optional path to state file for resuming.

    Returns:
        Statistics dictionary.
    """
    # Load settings (config_path is used by get_settings via environment)
    settings = get_settings()
    
    # Determine input directories: assume processed_dir contains subdirectories with same names as watch_folders
    processed_dir = settings.paths.processed_dir
    watch_folders = settings.paths.watch_folders
    
    input_dirs = []
    for folder in watch_folders:
        scope_name = folder.name  # e.g., "Test1"
        scope_dir = processed_dir / scope_name
        if scope_dir.exists() and scope_dir.is_dir():
            input_dirs.append(scope_dir)
        else:
            # fallback: assume processed_dir itself contains parquet files for all scopes
            # but we need separate folders, so warn and skip
            warnings.warn(f"Scope directory {scope_dir} not found. Skipping.")
    
    if not input_dirs:
        # No scope subdirectories found, fallback to processed_dir as single input directory
        # This assumes all parquet files are in one folder; grouping will still work
        # but matching across scopes may be impossible.
        input_dirs = [processed_dir]
    
    output_dir = settings.paths.merged_dir
    window_s = settings.grouper.window_s
    
    grouper = Grouper(input_dirs, output_dir, window_s, state_file)
    return grouper.run()


def run_grouper_cli(
    input_dirs: List[Path],
    output_dir: Path,
    window_s: float = 180.0,
    state_file: Optional[Path] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Run grouper with explicit parameters.

    Args:
        input_dirs: List of input directories.
        output_dir: Output directory.
        window_s: Matching window in seconds.
        state_file: Optional state file.
        limit: If set, process at most N newest files per folder.

    Returns:
        Statistics dictionary.
    """
    grouper = Grouper(input_dirs, output_dir, window_s, state_file)
    return grouper.run(limit=limit)


def main() -> None:
    """CLI entry point."""
    # Ensure project root is in sys.path for imports
    _script_dir = Path(__file__).resolve().parent.parent.parent
    if str(_script_dir) not in sys.path:
        sys.path.insert(0, str(_script_dir))
    parser = argparse.ArgumentParser(
        description="Merge parquet files from multiple scopes into event files.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="List of parquet folder paths (at least one).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for merged event files.",
    )
    parser.add_argument(
        "--window",
        type=float,
        default=180.0,
        help="Matching time window in seconds (default: 180).",
    )
    parser.add_argument(
        "--state",
        help="Optional state file for resuming.",
    )
    parser.add_argument(
        "--config",
        help="Optional config YAML path (overrides defaults).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N newest files per folder (default: all).",
    )
    args = parser.parse_args()

    # If config provided, load settings (but still require inputs/output?)
    if args.config:
        # TODO: integrate config loading
        pass

    input_dirs = [Path(d) for d in args.inputs]
    output_dir = Path(args.output)
    state_file = Path(args.state) if args.state else None

    stats = run_grouper_cli(input_dirs, output_dir, args.window, state_file, limit=args.limit)

    # Print summary
    print("\n=== Grouper Summary ===")
    print(f"Total files scanned: {stats['total_files']}")
    print(f"Matched events: {stats['matched_events']}")
    print(f"Unmatched files: {stats['unmatched_files']}")
    print(f"Errors: {stats['errors']}")

    sys.exit(0 if stats['errors'] == 0 else 1)


if __name__ == "__main__":
    main()