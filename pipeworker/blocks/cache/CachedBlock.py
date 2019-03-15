from abc import ABC
from datetime import datetime

from pipeworker.blocks.cache.CacheMissException import CacheMissException
from pipeworker.blocks.cache.FileCache import FileCache
from pipeworker.base.Block import Block
from pipeworker.utils import log


class CachedBlock(Block, ABC):
    cache_engine = None

    def invoke(self, data=None):
        if not not hasattr(self, 'cache_engine'):
            self.cache_engine = FileCache()

        try:
            last_serial = self.cache_engine.get(
                "%s_last_key" % self.name
            )
        except CacheMissException:
            last_serial = None

        current_serial = self.get_cache_serial()

        if last_serial != current_serial:
            new_data = self.save_to_cache_and_execute(current_serial, data)
            return new_data
        else:
            try:
                log(("Loading \"%s\" from cache" % self.name), 3)
                return self.cache_engine.get("%s_cache" % self.name)
            except KeyError:
                new_data = self.save_to_cache_and_execute(current_serial, data)
                return new_data

    def save_to_cache_and_execute(self, current_serial, data):
        log(("Cache miss of \"%s\", executing and saving to cache" % self.name), 3)
        new_data = self.execute(data)
        self.cache_engine.set(("%s_cache" % self.name), new_data)
        self.cache_engine.set(("%s_last_key" % self.name), current_serial)
        return new_data

    def set_cache_engine(self, cache_engine):
        self.cache_engine = cache_engine
        return self

    def get_cache_serial(self):
        log("Warning: Method get_cache_serial was not overridden, using current hour as fallback", 2)
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
