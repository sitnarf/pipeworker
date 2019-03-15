import unittest
from pipeworker.base.Block import Block
from pipeworker.base.Pipeline import Pipeline
from pipeworker.blocks.Data import Data


class MultiplyBy(Block):

    def __init__(self, what, by):
        self.by = by
        self.what = what

    def execute(self, input_data):
        return {
            self.what: input_data[self.what] * self.by
        }


class DefinedCompositionTest(unittest.TestCase):

    def test_basic_execution(self):

        result = Pipeline(
            Data({
                "number": 5,
            }) |
            MultiplyBy("number", 2)
        ).execute()

        self.assertEqual(
            result,
            {"number": 10}
        )

    def test_composed_execution(self):

        result = Pipeline(
            (
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
            )
        ).execute()

        self.assertEqual(
            {0: {'number': 20}, 1: {'number2': 4}},
            result,
        )


if __name__ == '__main__':
    unittest.main()
