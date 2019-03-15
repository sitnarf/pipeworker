import unittest
from pipeworker.base.Block import Block
from pipeworker.base.Pipeline import Pipeline
from pipeworker.blocks.Filter import Filter


class MultiplyBy(Block):

    def __init__(self, by):
        self.by = by

    def execute(self, input_data=None):
        return input_data < 3


class FilterBlockTest(unittest.TestCase):

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
