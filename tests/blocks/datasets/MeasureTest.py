import unittest
from pandas import DataFrame
from pipeworker.blocks.datasets.Measure import Measure
from pipeworker.types.Dataset import Dataset


class MeasureTest(unittest.TestCase):

    def test_execute(self):
        input_dataset = Dataset(DataFrame({"a": [1]}))

        def metric1(dataset, column):
            return dataset.update(
                payload={
                    "a": 2,
                    "b": column
                }
            )

        def metric2(dataset, _):
            return dataset.update(
                payload={
                    **dataset.payload,
                    "a": dataset.payload["a"]+1,
                }
            )

        evaluate = Measure([metric1, metric2], "a")

        self.assertEqual(
            evaluate.invoke(input_dataset).payload,
            {'a': 3, 'b': 'a'},
        )


if __name__ == '__main__':
    unittest.main()
