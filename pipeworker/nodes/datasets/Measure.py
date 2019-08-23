from functools import reduce
from typing import Callable, Iterable

from pipeworker.base import Node, NodeExecutionResponse
from pipeworker.types import Dataset
from pipeworker.utils import log


class Measure(Node):
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
            dataset
        )

        return evaluated

    def _log_execution_policy(self, execution_policy: NodeExecutionResponse) -> None:
        log(self.full_name)
