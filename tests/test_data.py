import pandas as pd
import pytest

from pathflow.data import split_features_target


def test_split_features_target():
    features, target = split_features_target(pd.DataFrame({"age": [40, 50], "target": [0, 1]}), "target")
    assert list(features.columns) == ["age"]
    assert target.tolist() == [0, 1]


def test_split_features_target_requires_existing_target():
    with pytest.raises(KeyError):
        split_features_target(pd.DataFrame({"age": [40]}), "target")
