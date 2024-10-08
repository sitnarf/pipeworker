import unittest
from pandas import DataFrame
from pipeworker.base import Pipeline
from pipeworker.nodes.datasets.TrainTestSplit import TrainTestSplit
from pipeworker.type.Dataset import Dataset


class TrainTestSplitTest(unittest.TestCase):

    def test_execute(self):
        result = Pipeline(
            TrainTestSplit(train_size=2, shuffle=False)
        ).execute(Dataset(DataFrame({
            "a": [1, 2, 3, 4, 5],
        })))

        self.assertEqual(
             list(result.predict.a.values),
             [3, 4, 5]
        )


if __name__ == '__main__':
    unittest.main()
