import unittest

from pipeworker.blocks.cache.CacheMissException import CacheMissException
from pipeworker.blocks.cache.FileCache import FileCache


class FileCacheTest(unittest.TestCase):

    CACHE_PATH = "./tests/temp/cache.pickle"

    def test_cache(self):
        cache = FileCache(self.CACHE_PATH)
        cache.set("key", 123)
        self.assertEqual(cache.get("key"), 123)
        self.assertRaises(CacheMissException, lambda: cache.get("key2"))

    def test_cache_clear(self):
        cache = FileCache(self.CACHE_PATH)
        cache.set("key", 123)
        cache.clear()
        self.assertRaises(CacheMissException, lambda: cache.get("key"))

    def setUp(self):
        cache = FileCache(self.CACHE_PATH)
        cache.clear()

    def tearDown(self):
        cache = FileCache(self.CACHE_PATH)
        cache.clear()


if __name__ == '__main__':
    unittest.main()
