import unittest

from pipeworker.base import Node


class BasicCompositionTest(unittest.TestCase):
    def test_sequences(self):
        node1 = Node()
        node2 = Node()
        node3 = Node()

        sequence = (
                node1 |
                node2 |
                node3
        )

        self.assertEqual(
            len(list(sequence)),
            3,
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

        self.assertEqual(
            len(list(parallel)),
            3,
        )


if __name__ == '__main__':
    unittest.main()
