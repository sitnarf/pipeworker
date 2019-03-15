import os
import pickle
import tempfile
from pipeworker.blocks.cache.CacheMissException import CacheMissException


class FileCache:
    def __init__(self, cache_file=None):
        self.cache_file = cache_file

    def _get_file(self):
        if self.cache_file is None:
            self.cache_file = "%s/cache.pickle" % tempfile.gettempdir()
        return self.cache_file

    def _get_resource(self, mode):
        cache_file = self._get_file()
        try:
            return open(cache_file, mode)
        except FileNotFoundError:
            with open(cache_file, "wb+") as f:
                pickle.dump({}, f)
            return open(cache_file, mode)

    def _get_cache(self):
        with self._get_resource("rb") as file:
            return pickle.load(file)

    def _set_cache(self, cache):
        with self._get_resource("wb+") as file:
            pickle.dump(cache, file)

    def set(self, key, value):
        cache = self._get_cache()
        cache[key] = value
        self._set_cache(cache)

    def get(self, key):
        cache = self._get_cache()
        try:
            return cache[key]
        except KeyError:
            raise CacheMissException

    def clear(self):
        try:
            os.remove(self._get_file())
        except FileNotFoundError:
            pass
