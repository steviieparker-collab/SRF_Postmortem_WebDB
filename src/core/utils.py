"""
Utility functions for SRF Event Monitoring System.
"""

from typing import List, Tuple, Optional, Dict, Any, Union, Callable, TypeVar
import pandas as pd
import numpy as np
import polars as pl
from pathlib import Path
import json
import hashlib
import os
import shutil
import tempfile
import time
from datetime import datetime
from functools import wraps

from .config import get_config
from .exceptions import FileProcessingError, ValidationError

T = TypeVar("T")


def classify_columns(
    columns: List[str], 
    df: Optional[pd.DataFrame] = None
) -> Tuple[List[str], List[str]]:
    """
    Classify columns into analog and digital channels.
    
    Args:
        columns: List of column names
        df: Optional DataFrame (unused, kept for compatibility)
        
    Returns:
        Tuple of (analog_columns, digital_columns)
    """
    analog = [c for c in columns if c.endswith("_v")]
    digital = [c for c in columns if c.endswith("_d")]
    return analog, digital


def ensure_directory(path: Path) -> Path:
    """
    Ensure directory exists and return Path.
    
    Args:
        path: Directory path
        
    Returns:
        Path with ensured existence
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_path(path: str, base_dir: Optional[Path] = None) -> Path:
    """
    Normalize a path string to Path, optionally relative to base_dir.
    
    Args:
        path: Path string
        base_dir: Base directory for relative paths
        
    Returns:
        Normalized Path object
    """
    p = Path(path)
    if not p.is_absolute() and base_dir is not None:
        p = base_dir / p
    return p.resolve()


def safe_read_parquet(path: Path, **kwargs) -> pd.DataFrame:
    """
    Safely read parquet file with error handling.
    Uses polars for timezone-aware columns (event_timestamp with Asia/Seoul),
    then converts to pandas.
    
    Args:
        path: Path to parquet file
        **kwargs: Additional arguments to pd.read_parquet
        
    Returns:
        DataFrame or empty DataFrame on error
    """
    import polars as pl
    try:
        df = pl.read_parquet(path)
        if "event_timestamp" in df.columns:
            # Convert timezone-aware to naive datetime (keep as string to avoid pyarrow crash)
            ts_vals = df["event_timestamp"].to_list()
            # Cast to string then parse to naive datetime
            # Convert Asia/Seoul → UTC → naive so pandas doesn't re-interpret the wall time
            df = df.with_columns(
                pl.col("event_timestamp")
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
                .alias("event_timestamp")
            )
        return df.to_pandas()
    except Exception as e:
        raise RuntimeError(f"Failed to read parquet file {path}: {e}")


def find_parquet_files(directory: Path, pattern: str = "*.parquet") -> List[Path]:
    """
    Find parquet files in directory.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern
        
    Returns:
        Sorted list of parquet file paths
    """
    if not directory.exists():
        return []
    files = list(directory.glob(pattern))
    return sorted(files)


def calculate_baseline(
    df: pd.DataFrame, 
    channel: str, 
    time_col: str = "t_rel_s",
    start_s: float = -0.5,
    end_s: float = -0.45,
) -> float:
    """
    Calculate baseline value for a channel.
    
    Args:
        df: DataFrame
        channel: Channel column name
        time_col: Time column name
        start_s: Start time for baseline window
        end_s: End time for baseline window
        
    Returns:
        Baseline value (median)
    """
    mask = (df[time_col] >= start_s) & (df[time_col] <= end_s)
    if not mask.any():
        return 1.0  # default
    values = df.loc[mask, channel].values
    return float(np.median(values))


def clip_small_value(value: float, threshold: float = 0.01) -> float:
    """
    Clip small absolute values to avoid division by zero.
    
    Args:
        value: Input value
        threshold: Minimum absolute value
        
    Returns:
        Clipped value
    """
    if abs(value) < threshold:
        return threshold if value >= 0 else -threshold
    return value


def split_baseline_std_cols(columns: List[str]) -> Tuple[List[str], List[str]]:
    """
    Separate baseline_std columns from other columns.
    
    Args:
        columns: List of column names.
        
    Returns:
        Tuple of (baseline_std_cols, other_cols).
    """
    baseline_suffixes = ("_baseline_std", "_baseline_mean")
    baseline_cols = [c for c in columns if c.endswith(baseline_suffixes)]
    other_cols = [c for c in columns if not c.endswith(baseline_suffixes)]
    return baseline_cols, other_cols


def safe_read_parquet_polars(path: Path, columns: Optional[List[str]] = None) -> Optional[pl.DataFrame]:
    """
    Read a parquet file safely with polars, returning None on failure.
    
    Args:
        path: Path to parquet file
        columns: Optional subset of columns to read
        
    Returns:
        DataFrame or None if reading fails
    """
    try:
        return pl.read_parquet(path, columns=columns)
    except Exception:
        return None


def extract_event_timestamp(df: pl.DataFrame) -> Optional[float]:
    """
    Extract event_timestamp from DataFrame and convert to POSIX timestamp.
    
    Args:
        df: DataFrame with 'event_timestamp' column.
        
    Returns:
        POSIX timestamp as float, or None if missing.
    """
    if "event_timestamp" not in df.columns:
        return None
    ts = df["event_timestamp"][0]
    if hasattr(ts, "timestamp"):
        return ts.timestamp()
    # Assume numeric timestamp in milliseconds
    try:
        return float(ts) / 1000.0
    except (TypeError, ValueError):
        return None


def compute_file_hash(path: Path, algorithm: str = "md5") -> str:
    """
    Compute hash of file content.
    
    Args:
        path: File path.
        algorithm: Hash algorithm (md5, sha1, sha256).
        
    Returns:
        Hex digest string.
    """
    hash_func = getattr(hashlib, algorithm)()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file, return empty dict on failure."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_json(data: Dict[str, Any], path: Path) -> bool:
    """Save JSON file, return success flag."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception:
        return False


def ensure_dir(path: Path) -> Path:
    """Ensure directory exists, return the path."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def rmdir_if_empty(path: Path) -> bool:
    """Remove directory if empty, return True if removed."""
    try:
        if path.exists() and path.is_dir() and not any(path.iterdir()):
            path.rmdir()
            return True
    except Exception:
        pass
    return False


def human_bytes(size: float) -> str:
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


# -----------------------------------------------------------------------------
# New utility functions required for core modules
# -----------------------------------------------------------------------------

def setup_directories() -> None:
    """
    Ensure all required directories exist.
    
    Creates directories defined in the configuration if they don't exist.
    """
    config = get_config()
    paths = config.paths
    
    directories = [
        paths.processed_dir,
        paths.merged_dir,
        paths.results_dir,
        paths.reports_dir,
        paths.graphs_dir,
        paths.log_dir,
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    
    # Also ensure watch folders exist
    for watch_folder in paths.watch_folders:
        watch_folder.mkdir(parents=True, exist_ok=True)


def validate_file(path: Union[str, Path], check_readable: bool = True, check_writable: bool = False) -> Path:
    """
    Validate file accessibility.
    
    Args:
        path: File path to validate.
        check_readable: Check if file is readable.
        check_writable: Check if file is writable.
        
    Returns:
        Path object if validation passes.
        
    Raises:
        FileNotFoundError: If file doesn't exist.
        PermissionError: If file is not accessible.
        ValidationError: If other validation fails.
    """
    path = Path(path)
    
    if not path.exists():
        raise FileNotFoundError(f"File does not exist: {path}")
    
    if not path.is_file():
        raise ValidationError(f"Path is not a file: {path}")
    
    if check_readable and not os.access(path, os.R_OK):
        raise PermissionError(f"File is not readable: {path}")
    
    if check_writable and not os.access(path, os.W_OK):
        raise PermissionError(f"File is not writable: {path}")
    
    return path


def get_timestamp(fmt: str = "%Y%m%d_%H%M%S") -> str:
    """
    Get current timestamp as formatted string.
    
    Args:
        fmt: Format string (default: YYYYMMDD_HHMMSS).
        
    Returns:
        Formatted timestamp string.
    """
    return datetime.now().strftime(fmt)


def calculate_md5(path: Union[str, Path], block_size: int = 65536) -> str:
    """
    Calculate MD5 checksum of a file.
    
    Args:
        path: Path to file.
        block_size: Block size for reading.
        
    Returns:
        MD5 hash as hexadecimal string.
        
    Raises:
        FileProcessingError: If file cannot be read.
    """
    path = Path(path)
    md5_hash = hashlib.md5()
    
    try:
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(block_size), b""):
                md5_hash.update(block)
    except (IOError, OSError) as e:
        raise FileProcessingError(f"Failed to read file for MD5 calculation: {path}") from e
    
    return md5_hash.hexdigest()


def safe_move(src: Union[str, Path], dst: Union[str, Path], overwrite: bool = False) -> Path:
    """
    Move file atomically using rename, with fallback to copy+delete.
    
    Args:
        src: Source file path.
        dst: Destination file path.
        overwrite: If True, overwrite existing destination file.
        
    Returns:
        Path to destination file.
        
    Raises:
        FileProcessingError: If move fails.
    """
    src = Path(src)
    dst = Path(dst)
    
    if not src.exists():
        raise FileNotFoundError(f"Source file does not exist: {src}")
    
    if dst.exists() and not overwrite:
        raise FileExistsError(f"Destination file already exists: {dst}")
    
    # Ensure destination directory exists
    dst.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Try atomic rename (works within same filesystem)
        src.replace(dst)
        return dst
    except OSError:
        # Fallback to copy + delete
        try:
            shutil.copy2(src, dst)
            src.unlink()
            return dst
        except (shutil.Error, OSError) as e:
            raise FileProcessingError(f"Failed to move {src} to {dst}") from e


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    logger: Optional[Any] = None,
):
    """
    Retry decorator for transient failures.
    
    Args:
        max_attempts: Maximum number of attempts (including first).
        delay: Initial delay between attempts in seconds.
        backoff: Multiplier for delay after each failure.
        exceptions: Tuple of exceptions to catch and retry.
        logger: Optional logger for retry attempts.
        
    Returns:
        Decorated function.
        
    Example:
        @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError,))
        def fetch_data():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts:
                        break
                    
                    if logger:
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed: {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            # All attempts failed
            raise last_exception  # type: ignore
        
        return wrapper
    return decorator


def size_to_human_readable(size_bytes: int) -> str:
    """
    Convert file size in bytes to human-readable string.
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        Human-readable size string (e.g., "1.5 MB").
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def ensure_extension(path: Union[str, Path], extension: str) -> Path:
    """
    Ensure a path has the specified extension.
    
    Args:
        path: Input path.
        extension: Desired extension (with or without dot).
        
    Returns:
        Path with guaranteed extension.
    """
    path = Path(path)
    if not extension.startswith("."):
        extension = f".{extension}"
    
    if path.suffix.lower() != extension.lower():
        return path.with_suffix(extension)
    return path


def create_temp_file(
    suffix: str = ".tmp",
    prefix: str = "srf_",
    directory: Optional[Union[str, Path]] = None,
    delete_on_exit: bool = True,
) -> Path:
    """
    Create a temporary file and return its path.
    
    Args:
        suffix: File suffix.
        prefix: File prefix.
        directory: Optional directory for temporary file.
        delete_on_exit: If True, schedule deletion on program exit.
        
    Returns:
        Path to temporary file.
    """
    if directory:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
    
    fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=directory)
    os.close(fd)
    
    path = Path(temp_path)
    
    if delete_on_exit:
        import atexit
        atexit.register(lambda: path.unlink(missing_ok=True))
    
    return path


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds.
        
    Returns:
        Formatted duration (e.g., "1h 30m 15.5s").
    """
    if seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds_remainder = divmod(remainder, 60)
    seconds_fraction = seconds - int(seconds)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds_remainder > 0 or seconds_fraction > 0:
        total_seconds = seconds_remainder + seconds_fraction
        parts.append(f"{total_seconds:.1f}s")
    
    return " ".join(parts) if parts else "0s"