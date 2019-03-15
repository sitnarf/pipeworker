import unittest

from pipeworker.base.Block import Block
from pipeworker.base.Pipeline import Pipeline
from pipeworker.blocks.Data import Data
from pipeworker.blocks.Merge import Merge


class MultiplyBy(Block):
    def __init__(self, what, by):
        self.by = by
        self.what = what

    def execute(self, input_data=None):
        return {
            self.what: input_data[self.what] * self.by
        }


class MergeBlockTest(unittest.TestCase):
    def test_merge(self):
        result = Pipeline(
            ((
                     Data({
                         "number": 5,
                     }) |
                     MultiplyBy("number", 2) |
                     MultiplyBy("number", 2)
             ) & (
                     Data({
                         "number2": 1,
                     }) |
                     MultiplyBy("number2", 2) |
                     MultiplyBy("number2", 2)
             )) |
            Merge()
        ).execute()

        self.assertEqual(
            result,
            {'number': 20, 'number2': 4}
        )

    def test_merge_primitive_values(self):
        result = Pipeline(
            ((
                 Data({
                     "number": 5,
                 })
             ) & (
                 Data({
                     "number": 1,
                 })
             )) |
            Merge()
        ).execute()

        self.assertEqual(
            result,
            {'number': 5}
        )

    def test_merge_iterables(self):
        result = Pipeline(
            ((
                 Data({
                     "list": [1, 2],
                 })
             ) & (
                 Data({
                     "list": [3, 4],
                 })
             )) |
            Merge()
        ).execute()

        self.assertEqual(
            {'list': [1, 2, 3, 4]},
            result,
        )


if __name__ == '__main__':
    unittest.main()
