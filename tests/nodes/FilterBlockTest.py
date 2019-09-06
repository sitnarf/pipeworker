import unittest
from pipeworker.base import Node, Pipeline
from pipeworker.nodes.Filter import Filter


class MultiplyBy(Node):

    def __init__(self, by):
        self.by = by

    def fit(self, input_data=None):
        return input_data < 3


class FilternodeTest(unittest.TestCase):

    def test_merge(self):

        result = Pipeline(
             Filter(MultiplyBy(5))
        ).execute([1, 2, 3])

        self.assertEqual(
             list(result),
             [1, 2]
        )


if __name__ == '__main__':
    unittest.main()
