import unittest

from pipeworker.functional import provide_previous, PreviousCurrent


class FunctionalTest(unittest.TestCase):
    def test_provide_previous(self):
        self.assertEqual(
            list(provide_previous([1, 2, 3])),
            [
                PreviousCurrent(None, 1),
                PreviousCurrent(1, 2),
                PreviousCurrent(2, 3),
            ]
        )


if __name__ == '__main__':
    unittest.main()
