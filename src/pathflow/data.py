"""Data loading and validation helpers."""

from pathlib import Path

import pandas as pd


def load_table(path: str | Path) -> pd.DataFrame:
    """Load a CSV table and fail early when it is missing or empty."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"Data file is empty: {path}")
    return frame


def split_features_target(frame: pd.DataFrame, target: str) -> tuple[pd.DataFrame, pd.Series]:
    """Separate a target column from model features."""
    if target not in frame.columns:
        raise KeyError(f"Target column not found: {target}")
    return frame.drop(columns=[target]), frame[target]
