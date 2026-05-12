"""
Preprocessor module for SRF Event Monitoring System.

Processes CSV files from oscilloscopes, extracts relevant segments,
performs decimation, normalization, and saves as Parquet files.
"""

import argparse
import sys
import time
import traceback
import csv
from collections import Counter
from datetime import datetime, timezone, timedelta

# Asia/Seoul timezone (UTC+9)
KST = timezone(timedelta(hours=9))
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import numpy as np
import polars as pl
from scipy.signal import savgol_filter
import re

from ..core.config import get_config
from ..core.logger import get_logger


logger = get_logger(__name__)


class Preprocessor:
    """
    Preprocessor for CSV files.

    Attributes:
        config: Preprocessor configuration (from config)
        classification: Classification thresholds (from config)
        stats: Processing statistics
    """

    def __init__(self, config=None):
        if config is None:
            config = get_config()
        self.config = config.preprocessor
        self.classification = config.classification
        self.stats = {
            "total": 0,
            "success": 0,
            "skipped": 0,
            "errors": 0,
            "skip_reasons": Counter(),
            "processing_times": [],
        }
    def _load_scope_cache(self, scope_dir: Path) -> set:
        """
        Load success filenames from the scope's preprocess_report.csv.

        Reads the existing single-file report and returns filenames
        whose 상태 == 'success'. These files will be skipped on the
        next run.
        """
        cache = set()
        report_path = scope_dir / "preprocess_report.csv"
        if not report_path.exists():
            return cache
        try:
            with open(report_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    fname = row.get('파일명', '').strip()
                    status = row.get('상태', '').strip()
                    if fname and status == 'success':
                        cache.add(fname)
        except Exception as e:
            logger.warning(f"Could not read {report_path}: {e}")
        return cache

    # ----------------------------------------------------------------------
    # CSV parsing and header processing
    # ----------------------------------------------------------------------
    @staticmethod
    def _parse_csv_robust(csv_path: Path, skip_rows: int) -> np.ndarray:
        """
        Robust CSV parsing to numpy array.

        Args:
            csv_path: Path to CSV file
            skip_rows: Number of rows to skip

        Returns:
            Numpy array of shape (n_samples, n_columns)
        """
        try:
            df = pl.read_csv(
                csv_path,
                has_header=False,
                skip_rows=skip_rows,
                ignore_errors=True,
            )
            df = df.with_columns(pl.all().cast(pl.Float64, strict=False))
            return df.to_numpy()
        except Exception as e:
            logger.warning(f"Polars CSV parsing failed, falling back to numpy: {e}")
            import io
            with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
                content = "".join(f.readlines()[skip_rows:])
                return np.genfromtxt(
                    io.StringIO(content),
                    delimiter=",",
                    filling_values=np.nan,
                )

    @staticmethod
    def _parse_header(lines: List[str]) -> Tuple[Optional[int], Optional[int], List[str], List[str]]:
        """
        Parse CSV header to find label and data indices, channel names.

        Args:
            lines: List of lines from CSV file

        Returns:
            Tuple of (label_idx, data_idx, channel_names, digital_labels)
        """
        label_idx, data_idx = None, None
        for i, line in enumerate(lines):
            if line.startswith("Label,"):
                label_idx = i
            if line.startswith("TIME,CH1,CH2,CH3,CH4"):
                data_idx = i + 1
            if label_idx is not None and data_idx is not None:
                break

        if label_idx is None or data_idx is None:
            return None, None, [], []

        raw_labels = [p.strip() for p in lines[label_idx].split(",")]

        def _clean(name: str) -> Optional[str]:
            return name.strip().replace(" ", "_") if name and name.strip() != "" else None

        ch_names = [_clean(raw_labels[i]) or f"CH{i}" for i in range(1, 5)]
        d_labels = [_clean(raw_labels[i]) or f"D{i - 7}" for i in range(7, len(raw_labels))]
        return label_idx, data_idx, ch_names, d_labels

    # ----------------------------------------------------------------------
    # Core processing: t=0 centering and decimation
    # ----------------------------------------------------------------------
    @staticmethod
    def _decimate_with_alignment(
        t_seg: np.ndarray,
        analog_seg: np.ndarray,
        digital_seg: np.ndarray,
        factor: int,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Decimate with t=0 alignment.

        Args:
            t_seg: Time segment
            analog_seg: Analog signals (n_samples, 4)
            digital_seg: Digital signals (n_samples, n_digital)
            factor: Decimation factor

        Returns:
            Tuple of (t_dec, analog_dec, digital_dec)
        """
        # Apply Savitzky-Golay filter if enough points
        if len(analog_seg) >= 11:
            analog_seg = savgol_filter(analog_seg, window_length=31, polyorder=1, axis=0)

        zero_idx = np.abs(t_seg).argmin()

        left_indices = np.arange(zero_idx, -1, -factor)[::-1]
        right_indices = np.arange(zero_idx + factor, len(t_seg), factor)
        target_indices = np.concatenate([left_indices, right_indices])

        t_dec = t_seg[target_indices]
        analog_dec = analog_seg[target_indices]

        if digital_seg.shape[1] > 0:
            digital_dec_list = []
            half_f = factor // 2

            for idx in target_indices:
                start = max(0, idx - half_f)
                end = min(len(digital_seg), idx + half_f + 1)
                window = digital_seg[start:end]

                voted = np.zeros(window.shape[1], dtype=np.float32)
                for ch in range(window.shape[1]):
                    ch_data = window[:, ch]
                    valid_mask = ~np.isnan(ch_data)
                    if valid_mask.any():
                        valid_vals = ch_data[valid_mask]
                        voted[ch] = 1.0 if np.mean(valid_vals) > 0.5 else 0.0
                    else:
                        voted[ch] = 0.0
                digital_dec_list.append(voted)

            digital_dec = np.array(digital_dec_list)
        else:
            digital_dec = np.empty((len(t_dec), 0))

        return t_dec, analog_dec, digital_dec

    # ----------------------------------------------------------------------
    # Validation and processing steps
    # ----------------------------------------------------------------------
    def _validate_basic(
        self,
        t: np.ndarray,
        analog_raw: np.ndarray,
        digital_raw: Optional[np.ndarray] = None,
    ) -> Optional[str]:
        """
        Perform basic validation on data.

        Returns error message if validation fails, None otherwise.
        """
        # Check t=0 data exists
        t_zero_mask = np.abs(t) < 0.001
        if not np.any(t_zero_mask):
            return "t=0 기준 데이터 없음"

        # Check pre-zero segment (-50ms ~ 0ms)
        pre_zero_mask = (t >= -0.05) & (t < 0)
        if not np.any(pre_zero_mask):
            return "t=0 이전 (-50ms) 데이터 부족"

        # Check baseline segment
        bl_mask = (t >= self.classification.baseline_start_s) & (t <= self.classification.baseline_end_s)
        if not np.any(bl_mask):
            return "baseline 구간 데이터 없음"

        # Check digital data if present
        if digital_raw is not None and digital_raw.shape[1] > 0:
            digital_pre = digital_raw[t < 0]
            if digital_pre.shape[0] == 0:
                return "t=0 이전 디지털 데이터 없음"
            nan_ratio = np.isnan(digital_pre).sum() / digital_pre.size
            if nan_ratio > 0.5:
                return f"디지털 데이터 nan 비율 높음 ({nan_ratio:.2%})"

        return None

    def _validate_beam(
        self,
        t: np.ndarray,
        beam_channel: np.ndarray,
        baseline_mean: float,
    ) -> Optional[str]:
        """
        Validate beam current conditions.

        Returns error message if validation fails, None otherwise.
        """
        # Minimum beam voltage
        if baseline_mean <= self.classification.beam_min_v:
            return f"빔전류 낮음 ({baseline_mean:.2f}V)"

        # Beam dump detection
        beam_post = beam_channel[t >= 0]
        if len(beam_post) == 0:
            return "t >=0 데이터 없음"
        threshold = baseline_mean * self.classification.dump_ratio
        if not np.any(beam_post <= threshold):
            return "Beam Dump 없음"

        return None

    # ----------------------------------------------------------------------
    # Single file processing
    # ----------------------------------------------------------------------
    def process_one(
        self,
        csv_path: Path,
        parquet_path: Optional[Path] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Process a single CSV file.

        Args:
            csv_path: Input CSV file path
            parquet_path: Output Parquet file path. If None, auto-generates.
            max_retries: Number of retries for transient failures
            retry_delay: Delay between retries in seconds

        Returns:
            Tuple of (success, reason, metadata)
            metadata includes processing stats, channel names, etc.
        """
        if parquet_path is None:
            parquet_path = csv_path.with_suffix(".parquet")

        metadata = {
            "csv_path": str(csv_path),
            "parquet_path": str(parquet_path),
            "channel_names": [],
            "digital_labels": [],
            "baseline_means": [],
            "baseline_stds": [],
            "norm_baseline_stds": [],
            "processing_time": None,
        }

        for attempt in range(max_retries):
            try:
                start_time = time.time()
                result = self._process_one_attempt(csv_path, parquet_path, metadata)
                metadata["processing_time"] = time.time() - start_time
                return result
            except (IOError, OSError) as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed after {max_retries} retries: {e}")
                    return False, f"I/O error: {e}", None
                logger.warning(f"Retry {attempt + 1}/{max_retries} after I/O error: {e}")
                time.sleep(retry_delay)
            except Exception as e:
                logger.exception(f"Unexpected error processing {csv_path.name}")
                return False, f"Unexpected error: {e}", None

        return False, "Max retries exceeded", None

    def _process_one_attempt(
        self,
        csv_path: Path,
        parquet_path: Path,
        metadata: Dict[str, Any],
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Single attempt at processing a file."""
        # Read file
        with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        # Parse header
        label_idx, data_idx, ch_names, d_labels = self._parse_header(lines)
        if data_idx is None:
            return False, "데이터 헤더 없음", None

        metadata["channel_names"] = ch_names
        metadata["digital_labels"] = d_labels

        # Parse data
        data = self._parse_csv_robust(csv_path, skip_rows=data_idx)
        if data.shape[1] < 5:
            return False, f"컬럼 부족 ({data.shape[1]})", None

        # Remove NaN in time column and deduplicate
        data = data[np.isfinite(data[:, 0])]
        _, u_idx = np.unique(data[:, 0], return_index=True)
        data = data[u_idx]

        t, analog_raw = data[:, 0], data[:, 1:5]
        digital_raw = data[:, 7:] if data.shape[1] > 7 else np.empty((len(t), 0))

        # Basic validation
        if err := self._validate_basic(t, analog_raw, digital_raw):
            return False, err, None

        # Baseline calculation
        bl_mask = (t >= self.classification.baseline_start_s) & (t <= self.classification.baseline_end_s)
        baseline_means = np.nanmean(analog_raw[bl_mask], axis=0)  # shape (4,)
        baseline_stds = np.nanstd(analog_raw[bl_mask], axis=0)    # shape (4,)
        metadata["baseline_means"] = baseline_means.tolist()
        metadata["baseline_stds"] = baseline_stds.tolist()

        # Beam validation
        if err := self._validate_beam(t, analog_raw[:, 0], baseline_means[0]):
            return False, err, None

        # Normalization
        divisors = np.where(
            np.abs(baseline_means) < self.classification.baseline_clip_v,
            self.classification.baseline_clip_v,
            baseline_means,
        )
        analog_norm = analog_raw / divisors[np.newaxis, :]

        # Normalized baseline stds (same scale as normalized signals)
        norm_baseline_stds = baseline_stds / np.abs(divisors)  # shape (4,)
        metadata["norm_baseline_stds"] = norm_baseline_stds.tolist()

        # Segment extraction
        s_idx = np.searchsorted(t, -self.config.segment_pre_s)
        e_idx = np.searchsorted(t, self.config.segment_post_s)
        t_seg, analog_seg = t[s_idx:e_idx], analog_norm[s_idx:e_idx]
        digital_seg = digital_raw[s_idx:e_idx] if digital_raw.shape[1] > 0 else np.empty((len(t_seg), 0))

        # Decimation
        t_dec, analog_dec, digital_dec = self._decimate_with_alignment(
            t_seg, analog_seg, digital_seg, self.config.decimation_factor
        )

        n_rows = len(t_dec)

        # Build output dictionary
        out_dict = {
            "t_rel_s": t_dec,
            "event_timestamp": pl.Series(
                "event_timestamp",
                [datetime.fromtimestamp(csv_path.stat().st_mtime, tz=KST)] * n_rows,
                dtype=pl.Datetime("ms", "Asia/Seoul"),
            ),
        }

        # Analog channels
        for i, name in enumerate(ch_names):
            col = f"{name}_v"
            out_dict[col] = analog_dec[:, i].astype(np.float32)
            # baseline_std column
            std_col = f"{col}_baseline_std"
            out_dict[std_col] = np.full(n_rows, norm_baseline_stds[i], dtype=np.float32)

        # Save beam current raw baseline mean for import_job ×50 calculation
        beam_baseline_col = f"{ch_names[0]}_v_baseline_mean"
        out_dict[beam_baseline_col] = np.full(n_rows, divisors[0], dtype=np.float32)

        # Digital channels
        for i in range(digital_dec.shape[1]):
            lbl = d_labels[i] if i < len(d_labels) else f"D{i}"
            out_dict[f"{lbl}_d"] = digital_dec[:, i].astype(np.float32)

        # Write Parquet
        pl.DataFrame(out_dict).write_parquet(parquet_path, compression=self.config.parquet_compression)
        return True, "OK", metadata

    # ----------------------------------------------------------------------
    # Batch processing
    # ----------------------------------------------------------------------
    def process_folder(
        self,
        input_dir: Path,
        output_dir: Path,
        pattern: str = "*.csv",
        dry_run: bool = False,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Process CSV files in a folder (optionally limited count).

        Reads existing preprocess_report.csv (Sehwan's format) to skip
        already-successful files, processes remaining new files, then
        writes a combined report with all entries preserved.

        Args:
            input_dir: Input directory
            output_dir: Output directory (will be created)
            pattern: File pattern to match
            dry_run: If True, only simulate processing
            limit: If set, process at most N files (newest first).

        Returns:
            Dictionary with processing statistics.
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Reset stats for this folder run
        self.stats = {
            "total": 0,
            "success": 0,
            "skipped": 0,
            "errors": 0,
            "skip_reasons": Counter(),
            "processing_times": [],
        }

        # ── Load scope-specific cache (only 'success' entries) ──────
        processed_cache = self._load_scope_cache(output_dir)
        logger.info(f'Loaded {len(processed_cache)} already-successful files from cache')

        # ── Read existing report entries (preserve old timestamps) ──
        existing_entries: Dict[str, dict] = {}
        report_path = output_dir / "preprocess_report.csv"
        if report_path.exists():
            try:
                with open(report_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        fname = row.get('파일명', '').strip()
                        if fname:
                            existing_entries[fname] = {
                                '파일명': fname,
                                '상태': row.get('상태', '').strip(),
                                '원인 카테고리': row.get('원인 카테고리', '').strip(),
                                '상세 사유': row.get('상세 사유', '').strip(),
                                '날짜_시각': row.get('날짜_시각', '').strip(),
                            }
            except Exception as e:
                logger.warning(f"Could not read existing report {report_path}: {e}")

        # ── Gather new CSV files (skip already-successful ones) ─────
        all_csvs = list(input_dir.rglob(pattern))
        csv_files = [f for f in all_csvs if f.name not in processed_cache]
        # Sort newest-first so --limit picks the latest files
        csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        if limit is not None and limit > 0:
            csv_files = csv_files[:limit]

        logger.info(f'Found {len(csv_files)} new CSV files to process in {input_dir}')

        self.stats['total'] = len(all_csvs)
        self.stats['success'] = len([e for e in existing_entries.values() if e['상태'] == 'success'])

        # ── Process new files ──────────────────────────────────────
        curr_time = datetime.now(KST).strftime('%Y%m%d_%H%M%S')
        new_entries: Dict[str, dict] = {}

        for i, csv_path in enumerate(csv_files, 1):
            print(f'Processing [{i}/{len(csv_files)}] {csv_path.name}', flush=True)
            logger.info(f'Processing [{i}/{len(csv_files)}] {csv_path.name}')

            out_path = output_dir / f'{csv_path.stem}.parquet'
            success, reason, metadata = self.process_one(csv_path, out_path)

            if success:
                logger.info(f"  Saved to {out_path.name}")
                self.stats["success"] += 1
                if metadata and metadata.get("processing_time"):
                    self.stats["processing_times"].append(metadata["processing_time"])
                new_entries[csv_path.name] = {
                    '파일명': csv_path.name,
                    '상태': 'success',
                    '원인 카테고리': '',
                    '상세 사유': '',
                    '날짜_시각': curr_time,
                }
            else:
                logger.warning(f"  Skipped: {reason}")
                self.stats["skipped"] += 1
                self.stats["skip_reasons"][reason] += 1
                new_entries[csv_path.name] = {
                    '파일명': csv_path.name,
                    '상태': 'fail',
                    '원인 카테고리': Preprocessor._categorize_reason(reason),
                    '상세 사유': reason,
                    '날짜_시각': curr_time,
                }

        # ── Merge: old entries preserved unless overridden by new ───
        merged_entries = {**existing_entries, **new_entries}

        self._log_summary()
        if not dry_run:
            self._write_report(output_dir, merged_entries)

        return {
            "stats": dict(self.stats),
            "results": [(n, e['상태'] == 'success', e['상세 사유']) for n, e in merged_entries.items()],
        }

    def _log_summary(self) -> None:
        """Log processing summary."""
        total = self.stats["total"]
        success = self.stats["success"]
        skipped = self.stats["skipped"]
        errors = self.stats["errors"]

        logger.info("=" * 50)
        logger.info(f"  Total files     : {total}")
        logger.info(f"  Success         : {success}")
        logger.info(f"  Skipped         : {skipped}")
        logger.info(f"  Errors          : {errors}")

        if self.stats["skip_reasons"]:
            logger.info("  Skip reasons:")
            for reason, count in self.stats["skip_reasons"].most_common():
                logger.info(f"    {reason}: {count}")

        if self.stats["processing_times"]:
            times = self.stats["processing_times"]
            logger.info(f"  Processing times: avg={np.mean(times):.2f}s, min={np.min(times):.2f}s, max={np.max(times):.2f}s")

        logger.info("=" * 50)

    @staticmethod
    def _natural_sort_key(filename: str) -> tuple:
        """Natural sort key for filenames like tek0000.csv, tek0056.csv."""
        numbers = re.findall(r'\d+', filename)
        return tuple(int(n) for n in numbers) if numbers else (filename,)

    @staticmethod
    def _categorize_reason(reason: str) -> str:
        """Categorize skip reason."""
        if "빔전류" in reason:
            return "빔전류 낮음"
        elif "baseline" in reason:
            return "baseline 구간 데이터 없음"
        elif "Beam Dump" in reason:
            return "Beam Dump 없음"
        elif "t=0 이전" in reason and "디지털" in reason:
            return "디지털 데이터 없음"
        elif "t=0 이전" in reason:
            return "t=0 이전 데이터 부족"
        elif "t=0 기준" in reason:
            return "t=0 기준 데이터 없음"
        elif "nan 비율" in reason:
            return "디지털 nan 비율 과다"
        elif "헤더" in reason or "컬럼" in reason:
            return "파일 형식 오류"
        else:
            return "기타"

    def _write_report(
        self,
        output_dir: Path,
        merged_entries: Dict[str, dict],
    ) -> Path:
        """
        Write combined preprocess_report.csv in Sehwan's format.

        Format:
            파일명,상태,원인 카테고리,상세 사유,날짜_시각

        Args:
            output_dir: Scope output directory
            merged_entries: Dict of filename -> {파일명,상태,원인 카테고리,상세 사유,날짜_시각}
        """
        report_path = output_dir / "preprocess_report.csv"

        # Sort filenames naturally by embedded number
        sorted_fnames = sorted(merged_entries.keys(), key=self._natural_sort_key)

        with report_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(['파일명', '상태', '원인 카테고리', '상세 사유', '날짜_시각'])
            for name in sorted_fnames:
                row = merged_entries[name]
                writer.writerow([
                    row['파일명'],
                    row['상태'],
                    row['원인 카테고리'],
                    row['상세 사유'],
                    row['날짜_시각'],
                ])

        logger.info(f"Report saved to {report_path} ({len(merged_entries)} entries)")
        return report_path


# ----------------------------------------------------------------------
# CLI interface
# ----------------------------------------------------------------------
def main(args=None):
    """Command-line interface for preprocessor."""
    # Ensure project root is in sys.path for relative imports
    _script_dir = Path(__file__).resolve().parent.parent.parent  # src/pipeline/../.. → project root
    if str(_script_dir) not in sys.path:
        sys.path.insert(0, str(_script_dir))
    parser = argparse.ArgumentParser(
        description="CSV → Parquet preprocessor for SRF event monitoring"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory containing CSV files"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for Parquet files"
    )
    parser.add_argument(
        "--config",
        help="Path to config YAML file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate processing without writing files"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="File pattern to match (default: *.csv)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N files (newest first; default: all)"
    )

    parsed = parser.parse_args(args)

    # Configure logging
    from ..core.logger import setup_logging
    level = "DEBUG" if parsed.verbose else "INFO"
    setup_logging(level=level)

    # Load configuration
    config = None
    if parsed.config:
        from ..core.config import load_config
        config = load_config(Path(parsed.config))
    else:
        config = get_config()

    # Run preprocessor
    preprocessor = Preprocessor(config)
    result = preprocessor.process_folder(
        input_dir=Path(parsed.input),
        output_dir=Path(parsed.output),
        pattern=parsed.pattern,
        dry_run=parsed.dry_run,
        limit=parsed.limit,
    )

    # Exit code
    if result["stats"]["errors"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())