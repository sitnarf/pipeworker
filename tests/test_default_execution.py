import unittest

from pipeworker.base import Block, Pipeline


class BasicCompositionTest(unittest.TestCase):
    def test_sequences(self):
        block1 = Block()
        block2 = Block()
        block3 = Block()

        sequence = (
                block1 |
                block2 |
                block3
        )

        pipeline = Pipeline(sequence)

        self.assertEqual(
            pipeline.execute(10),
            10
        )

    def test_parallel(self):
        block1 = Block()
        block2 = Block()
        block3 = Block()

        parallel = (
                block1 &
                block2 &
                block3
        )

        pipeline = Pipeline(parallel)

        self.assertEqual(
            pipeline.execute(10),
            {0: 10, 1: 10, 2: 10}
        )


if __name__ == '__main__':
    unittest.main()
