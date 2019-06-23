from functools import reduce
from typing import Callable, Iterable

from pipeworker.base import Block
from pipeworker.types import Dataset


class Measure(Block):
    def __init__(
            self,
            measurements: Iterable[Callable[[Dataset], Dataset]],
            column: str = "y",
    ):
        self.column = column
        self.measurements = measurements

    def execute(self, dataset: Dataset) -> Dataset:
        evaluated = reduce(
            lambda dataset, metric: metric(dataset, self.column),
            self.measurements,
            input_dataset
        )

        return evaluated
