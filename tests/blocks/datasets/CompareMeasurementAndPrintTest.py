import unittest
from pandas import DataFrame
from pipeworker.blocks.datasets.CompareMeasurementAndPrint import CompareMeasurementAndPrint
from pipeworker.types.Dataset import Dataset


class CompareAndPrintTest(unittest.TestCase):

    def test_compute(self):
        block = CompareMeasurementAndPrint("a")
        result = block.compute({
            "0": Dataset(DataFrame({}), label="dataset1", payload={
                "measurements": {
                    "a": 4,
                }
            }),
            "1": Dataset(DataFrame({}), label="dataset2",  payload={
                "measurements": {
                    "a": 6,
                }
            }),
            "2": Dataset(DataFrame({}), label="dataset3",  payload={
                "measurements": {
                    "a": 2,
                }
            })
        })

        self.assertEqual(
            [
                ['dataset3', 0, -2, -4, -3.0],
                ['dataset1', 2, 0, -2, 0.0],
                ['dataset2', 4, 2, 0, 3.0]
            ],
            result,
        )


if __name__ == '__main__':
    unittest.main()
