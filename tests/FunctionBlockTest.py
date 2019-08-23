import unittest
from pipeworker.base import Pipeline
from pipeworker.nodes.Function import Function


class FunctionnodeTest(unittest.TestCase):

    def test_functions(self):

        pipeline = Pipeline(
            Function(lambda x: x * 2) |
            Function(lambda x: x + 5)
        )

        self.assertEqual(
            pipeline.execute(10),
            25
        )


if __name__ == '__main__':
    unittest.main()
