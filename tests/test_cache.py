import os
import unittest

from pipeworker.cache_engine import CacheEngine, CodeStateGenerator
from tests.files.some_node import SomeNode


class CacheTest(unittest.TestCase):
    def test_make_key(self):
        key = CacheEngine._make_key("abc")
        key2 = CacheEngine._make_key("abc2")
        key3 = CacheEngine._make_key(("abc", "a"))
        key4 = CacheEngine._make_key("abc")

        self.assertNotEqual(key, key2)
        self.assertNotEqual(key2, key3)
        self.assertEqual(key, key4)
        self.assertIsInstance(key, str)
        self.assertIsInstance(key2, str)
        self.assertIsInstance(key3, str)
        self.assertIsInstance(key4, str)

    def test_get_code_state(self):
        some_node = SomeNode()
        code_state = CodeStateGenerator(some_node.__module__).get()
        self.assertEqual(
            code_state,
            CodeStateGenerator(some_node.__module__).get(),
        )
        self.assertIsInstance(code_state, dict)
        os.utime("files/some_node.py")
        self.assertNotEqual(
            code_state,
            CodeStateGenerator(some_node.__module__).get(),
        )


if __name__ == '__main__':
    unittest.main()
