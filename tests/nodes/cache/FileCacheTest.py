import unittest

from pipeworker.cache_engine import FileCacheEngine, CacheMissException


class FileCacheTest(unittest.TestCase):
    CACHE_PATH = "./tests/temp/cache.pickle"

    def test_cache(self):
        cache = FileCacheEngine(self.CACHE_PATH)
        cache.set("key", 123)
        self.assertEqual(cache.get("key"), 123)
        self.assertRaises(CacheMissException, lambda: cache.get("key2"))

    def test_cache_clear(self):
        cache = FileCacheEngine(self.CACHE_PATH)
        cache.set("key", 123)
        cache.clear()
        self.assertRaises(CacheMissException, lambda: cache.get("key"))

    def setUp(self):
        cache = FileCacheEngine(self.CACHE_PATH)
        cache.clear()

    def tearDown(self):
        cache = FileCacheEngine(self.CACHE_PATH)
        cache.clear()


if __name__ == '__main__':
    unittest.main()
