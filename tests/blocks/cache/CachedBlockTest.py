import unittest
from unittest.mock import MagicMock, call
from pipeworker.blocks.cache.CachedBlock import CachedBlock
from pipeworker.blocks.cache.FileCache import FileCache


class MassiveComputationBlock(CachedBlock):

    serial = 56

    def execute(self, data):
        return data

    def get_cache_serial(self):
        return self.serial


class CachedBlockTest(unittest.TestCase):
    def test_cache(self):
        cache = FileCache("./tests/temp/cache.pickle")
        cache.clear()
        block1 = MassiveComputationBlock().set_cache_engine(cache).set_name("my_block")
        block1.execute = MagicMock(return_value=42)

        result1 = block1.invoke()
        result2 = block1.invoke()

        block1.execute.assert_called_once()
        self.assertEqual(result1, 42)
        self.assertEqual(result2, 42)
        cache.clear()

    def test_cache_serial(self):
        cache = FileCache("./tests/temp/cache.pickle")
        cache.clear()
        block1 = MassiveComputationBlock().set_cache_engine(cache)
        block1.invoke = MagicMock(return_value=42)

        result1 = block1.invoke()
        result2 = block1.invoke()
        result3 = block1.invoke()
        block1.serial = 57
        result4 = block1.invoke()

        block1.invoke.assert_has_calls([call(), call()])
        self.assertEqual(result1, 42)
        self.assertEqual(result2, 42)
        self.assertEqual(result3, 42)
        self.assertEqual(result4, 42)

        cache.clear()


if __name__ == '__main__':
    unittest.main()
