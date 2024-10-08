import unittest

from pipeworker.base import Node, Pipeline


class BasicCompositionTest(unittest.TestCase):
    def test_sequences(self):
        node1 = Node()
        node2 = Node()
        node3 = Node()

        sequence = (
                node1 |
                node2 |
                node3
        ).not_cache()

        pipeline = Pipeline(sequence)

        self.assertEqual(
            pipeline.execute(10),
            10
        )

    def test_parallel(self):
        node1 = Node()
        node2 = Node()
        node3 = Node()

        parallel = (
                node1 &
                node2 &
                node3
        )

        pipeline = Pipeline(parallel)

        self.assertEqual(
            pipeline.execute(10),
            {0: 10, 1: 10, 2: 10}
        )


if __name__ == '__main__':
    unittest.main()
