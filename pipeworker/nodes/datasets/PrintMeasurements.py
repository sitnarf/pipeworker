from typing import Optional

from colored import fg, attr

from pipeworker.base import Node, InvocationResult, NodeExecutionResponse
from pipeworker.cache_engine import CodeState
from pipeworker.functions.utils import title, table
from pipeworker.types import Dataset
from pipeworker.utils import log


class PrintMeasurements(Node):
    def execute(self, dataset: Dataset) -> Dataset:
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

    def _should_execute(
            self,
            executed: Optional[bool],
            current_state: CodeState,
            cached_input: Optional[InvocationResult]
    ) -> NodeExecutionResponse:
        return NodeExecutionResponse(should_execute=True)

    def _log_execution_policy(self, execution_policy: NodeExecutionResponse) -> None:
        print()
        log(fg(245) + self.full_name + attr('reset'))
