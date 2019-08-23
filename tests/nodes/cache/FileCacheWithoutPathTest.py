import unittest
from pipeworker.nodes.cache import FileCacheEngine
from pipeworker.cache_engine import CacheMissException


class FileCacheTest(unittest.TestCase):
    def test_cache(self):
        cache = FileCacheEngine()
        cache.set("key", 123)
        self.assertEqual(cache.get("key"), 123)
        self.assertRaises(CacheMissException, lambda: cache.get("key2"))

    def test_cache_clear(self):
        cache = FileCacheEngine()
        cache.set("key", 123)
        cache.clear()
        self.assertRaises(CacheMissException, lambda: cache.get("key"))

    def setUp(self):
        cache = FileCacheEngine()
        cache.clear()

    def tearDown(self):
        cache = FileCacheEngine()
        cache.clear()


if __name__ == '__main__':
    unittest.main()
