from typing import Optional

from pipeworker.base import Node, InvocationResult, NodeExecutionResponse
from pipeworker.cache_engine import CodeState
from pipeworker.functions.utils import title, table
from pipeworker.types import Dataset


class PrintMeasurements(Node):
    def fit(self, dataset: Dataset) -> Dataset:
        print()
        if dataset.label:
            print(title("Measurements for %s" % dataset.label))
        try:
            measurements = dataset.payload["measurements"]
        except KeyError:
            print("No measurements\n")
            return dataset

        print(table(measurements.items()))

        return dataset

    def _should_execute_fit(
            self,
            executed: Optional[bool],
            current_state: CodeState,
            cached_input: Optional[InvocationResult]
    ) -> NodeExecutionResponse:
        return NodeExecutionResponse(should_execute=True)

    def _log_execution_policy(self, execution_policy: NodeExecutionResponse) -> None:
        pass
