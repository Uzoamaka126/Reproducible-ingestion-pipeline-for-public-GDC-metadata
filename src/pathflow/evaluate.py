"""Evaluation helpers."""

from sklearn.metrics import accuracy_score, f1_score


def evaluate(model, features, labels) -> dict[str, float]:
    """Return standard classification metrics."""
    predictions = model.predict(features)
    return {
        "accuracy": float(accuracy_score(labels, predictions)),
        "f1": float(f1_score(labels, predictions, average="weighted")),
    }
