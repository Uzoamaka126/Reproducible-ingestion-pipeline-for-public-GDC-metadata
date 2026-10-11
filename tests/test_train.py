import pandas as pd

from pathflow.train import build_model


def test_model_handles_numeric_and_categorical_features():
    features = pd.DataFrame({"age": [40, 50, 60, 70], "stage": ["I", "II", "I", "III"]})
    labels = [0, 1, 0, 1]
    model = build_model(features)
    model.fit(features, labels)
    assert len(model.predict(features)) == 4
