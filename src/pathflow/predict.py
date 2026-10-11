"""Prediction helpers."""

from pathlib import Path

import joblib
import pandas as pd

from .data import load_table


def predict(model_path: str | Path, data_path: str | Path) -> pd.Series:
    """Load a persisted model and return predictions for a CSV table."""
    model = joblib.load(model_path)
    return pd.Series(model.predict(load_table(data_path)), name="prediction")
