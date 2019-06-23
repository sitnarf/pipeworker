from pipeworker.base import Block
from pipeworker.functions.utils import title, table
from pipeworker.types import Dataset


class PrintMeasurements(Block):
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
