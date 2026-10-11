# PathFlow

A lightweight, reproducible machine-learning starter project for computational oncology.

## Layout

- `src/pathflow/`: data loading, training, evaluation, and prediction utilities
- `configs/`: experiment configuration files
- `notebooks/`: exploratory analysis notebooks
- `tests/`: automated tests

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
ruff check .
```

The training pipeline expects a CSV with a named target column:

```python
from pathflow.train import train

train("data/processed/records.csv", target="target", model_path="artifacts/model.joblib")
```
