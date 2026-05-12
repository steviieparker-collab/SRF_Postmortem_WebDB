"""
Channel utilities for column classification.
"""

from typing import List, Tuple, Optional
import pandas as pd
import polars as pl


def classify_columns(columns: List[str], df: Optional[pd.DataFrame] = None) -> Tuple[List[str], List[str]]:
    """
    Classify column names into analog (_v) and digital (_d) columns.

    This is a compatibility function for older code that expects a pandas DataFrame argument.
    The df argument is ignored.

    Args:
        columns: List of column names.
        df: Optional pandas DataFrame (ignored).

    Returns:
        Tuple of (analog_cols, digital_cols).
    """
    analog = [c for c in columns if c.endswith("_v")]
    digital = [c for c in columns if c.endswith("_d")]
    return analog, digital


def classify_columns_polars(columns: List[str]) -> Tuple[List[str], List[str]]:
    """
    Classify column names into analog (_v) and digital (_d) columns.

    Args:
        columns: List of column names.

    Returns:
        Tuple of (analog_cols, digital_cols).
    """
    return classify_columns(columns)