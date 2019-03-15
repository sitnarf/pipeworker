from typing import Callable, Iterable
from sklearn.metrics import mean_absolute_error
import numpy as np
from pipeworker.types import Dataset


def apply_to_dataset(
    metric: Callable[[Iterable[float], Iterable[float]], float],
    payload_key: str,
    dataset: Dataset,
    column: str
) -> Dataset:
    y_predict = dataset.predict[column]
    y_test = dataset.data.loc[dataset.predict.index][column]
    return dataset.update(
        payload={
            **dataset.payload,
            "measurements": {
                **dataset.payload["measurements"],
                payload_key: metric(y_test, y_predict),
            },
        }
    )


def compute_mape(y_test_input: Iterable[float], y_predict_input: Iterable[float]) -> float:
    y_test, y_predict = np.array(y_test_input), np.array(y_predict_input)
    return float(np.mean(np.abs((y_test - y_predict) / y_test)))


def mae(dataset: Dataset, column: str) -> Dataset:
    return apply_to_dataset(mean_absolute_error, "mae", dataset, column)


def mape(dataset: Dataset, column: str) -> Dataset:
    return apply_to_dataset(compute_mape, "mape", dataset, column)
