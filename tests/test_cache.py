import os
import unittest

from pipeworker.cache import Cache, CodeStateGenerator
from tests.files.some_block import SomeBlock


class CacheTest(unittest.TestCase):
    def test_make_key(self):
        key = Cache._make_key("abc")
        key2 = Cache._make_key("abc2")
        key3 = Cache._make_key(("abc", "a"))
        key4 = Cache._make_key("abc")

        self.assertNotEqual(key, key2)
        self.assertNotEqual(key2, key3)
        self.assertEqual(key, key4)
        self.assertIsInstance(key, str)
        self.assertIsInstance(key2, str)
        self.assertIsInstance(key3, str)
        self.assertIsInstance(key4, str)

    def test_get_code_state(self):
        some_block = SomeBlock()
        code_state = CodeStateGenerator(some_block.__module__).get()
        self.assertEqual(
            code_state,
            CodeStateGenerator(some_block.__module__).get(),
        )
        self.assertIsInstance(code_state, dict)
        os.utime("files/some_block.py")
        self.assertNotEqual(
            code_state,
            CodeStateGenerator(some_block.__module__).get(),
        )


if __name__ == '__main__':
    unittest.main()
