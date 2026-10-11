"""Model training entry points."""

from pathlib import Path

import joblib
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .data import load_table, split_features_target


def build_model(features):
    """Build a preprocessing and logistic-regression pipeline."""
    numeric = features.select_dtypes(include="number").columns.tolist()
    categorical = features.select_dtypes(exclude="number").columns.tolist()
    transformer = ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]),
                numeric,
            ),
            (
                "categorical",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        ("encode", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical,
            ),
        ],
        remainder="drop",
    )
    return Pipeline([("preprocess", transformer), ("classifier", LogisticRegression(max_iter=1000))])


def train(data_path: str | Path, target: str, model_path: str | Path) -> Pipeline:
    """Train and persist a classifier."""
    frame = load_table(data_path)
    features, labels = split_features_target(frame, target)
    model = build_model(features)
    model.fit(features, labels)
    model_path = Path(model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)
    return model
