import unittest
from pipeworker.base.Block import Block


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

        self.assertEqual(
            list(sequence),
            [block1, block2, block3]
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

        self.assertEqual(
            list(parallel),
            [block1, block2, block3]
        )


if __name__ == '__main__':
    unittest.main()
