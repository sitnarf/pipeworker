from typing import Dict, List, Union
from termcolor import colored, cprint
from numpy import mean
from pydash import sort
from tabulate import tabulate
from pipeworker.base.Block import Block
from pipeworker.functions.utils import title, sign_color, table
from pipeworker.types.Dataset import Dataset


class CompareMeasurementAndPrint(Block):
    which: str

    def __init__(self, which: str):
        self.which = which

    def datasets_difference(self, dataset1: Dataset, dataset2: Dataset) -> float:
        return (
            dataset1.payload["measurements"][self.which] -
            dataset2.payload["measurements"][self.which]
        )

    def compute(self, datasets: Dict[str, Dataset]) -> List[List[Union[str, float]]]:
        table = []
        averaged = []
        for key1, dataset1 in datasets.items():
            measurement_sum = 0
            for key2, dataset2 in datasets.items():
                if(key1 != key2):
                    measurement_sum += self.datasets_difference(dataset1, dataset2)

            averaged.append({"key": key1, "avg": measurement_sum / (len(datasets)-1)})

        sort(averaged, lambda item1, item2: mean(item1["avg"]) - mean(item2["avg"]))
        sorted_keys = list(map(lambda item: item["key"], averaged))
        sorted_averages = list(map(lambda item: item["avg"], averaged))

        for i, key1 in enumerate(sorted_keys):
            dataset1 = datasets[key1]
            line = [
                dataset1.label if dataset1.label else key1,
            ]
            for key2 in sorted_keys:
                dataset2 = datasets[key2]
                line.append(self.datasets_difference(dataset1, dataset2))
            line.append(sorted_averages[i])
            table.append(line)
        return table

    def execute(self, datasets: Dict[str, Dataset]) -> Dict[str, Dataset]:
        computed = self.compute(datasets)
        output = []

        output.append((
            "",
            *map(lambda item: item[0], computed),
            "AVG"
        ))

        for line_number, line in enumerate(computed):
            output_line = []
            for cell_number, cell in enumerate(line):
                if cell_number == 0:
                    output_line.append(cell)
                elif cell_number-1 == line_number:
                    output_line.append("")
                else:
                    output_line.append(sign_color("%.2f" % cell, cell))
            output.append(output_line)

        print(title("\nAbsolute difference in %s" % self.which))
        print(table(output))
        return datasets
