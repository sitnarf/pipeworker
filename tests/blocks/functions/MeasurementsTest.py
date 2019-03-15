import unittest
from pandas import DataFrame
from pipeworker.functions.measurements import mae
from pipeworker.types.Dataset import Dataset


class MeasurementsTest(unittest.TestCase):
    def test_compute_mae(self):
        data = DataFrame({"a": [1, 1, 2, 2, 5, 4]}, index=[0, 1, 2, 3, 4, 5])
        predict = DataFrame({"a": [6, 2]}, index=[4, 5])
        dataset = Dataset(
            data=data,
            predict=predict
        )
        result = mae(dataset, "a")
        self.assertEqual(
            result.payload["measurements"],
            {
                "mae": 1.5,
            }
        )


if __name__ == '__main__':
    unittest.main()
