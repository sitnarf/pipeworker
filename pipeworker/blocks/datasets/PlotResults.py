from typing import Iterable
from matplotlib import pyplot
from pipeworker.base import Block
from pipeworker.types import Dataset


class PlotForecast(Block):

    def __init__(self, columns: Iterable[str], label: str = None, file: str = None):
        self.label = label
        self.columns = columns
        self.file = file

    def execute(self, dataset: Dataset) -> Dataset:
        if self.label:
            pyplot.title(self.label)
        for column in self.columns:
            if column in dataset.data:
                pyplot.plot(dataset.data[column])
            if column in dataset.predict:
                pyplot.plot(dataset.predict[column])
        if self.file:
            pyplot.savefig('books_read.png')
        else:
            pyplot.show()
        return dataset
