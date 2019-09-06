import unittest
from pipeworker.base import Node, Pipeline
from pipeworker.nodes.transformers import Map


class MultiplyBy(Node):

    def __init__(self, by):
        self.by = by

    def fit(self, input_data=None):
        return input_data * self.by


class MapnodeTest(unittest.TestCase):

    def test_merge(self):

        result = Pipeline(
             Map(MultiplyBy(5))
        ).execute([1, 2, 3])

        self.assertEqual(
             list(result),
             [5, 10, 15]
        )


if __name__ == '__main__':
    unittest.main()
