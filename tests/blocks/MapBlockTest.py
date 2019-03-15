import unittest
from pipeworker.base.Block import Block
from pipeworker.base.Pipeline import Pipeline
from pipeworker.blocks.Map import Map


class MultiplyBy(Block):

    def __init__(self, by):
        self.by = by

    def execute(self, input_data=None):
        return input_data * self.by


class MapBlockTest(unittest.TestCase):

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
