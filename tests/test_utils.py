import unittest

from pipeworker.utils import hash_str


class UtilsTest(unittest.TestCase):
    def test_hash_str(self):
        self.assertIsInstance(hash_str("123"), str)


if __name__ == '__main__':
    unittest.main()
