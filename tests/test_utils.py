import unittest

from pipeworker.utils import hash_str, copy_first_level


class UtilsTest(unittest.TestCase):
    def test_hash_str(self):
        self.assertIsInstance(hash_str("123"), str)

    def test_copy_first_level(self):
        class A:
            def __init__(self):
                self.list = [1, 2, [3, 4]]
                self.property = "test"

        a = A()
        a.property = "test2"
        b = copy_first_level(a)
        self.assertEqual(b.property, "test2")
        b.list.append(4)
        self.assertEqual([1, 2, [3, 4]], a.list)
        b.list[2].append(5)
        self.assertEqual([1, 2, [3, 4, 5]], a.list)


if __name__ == '__main__':
    unittest.main()
