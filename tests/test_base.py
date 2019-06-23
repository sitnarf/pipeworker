import unittest
from unittest.mock import patch

from pipeworker.base import Block, CacheBehaviour, InvocationData
from pipeworker.cache import CachedOutput, Cache
from tests.files.some_block import SomeBlock


class TestBlock(Block):
    pass


class TestBlock2(Block):
    pass


class BaseTest(unittest.TestCase):
    def test_cache(self):
        block = Block()
        new_block = block.cache()
        self.assertEqual(CacheBehaviour.ALWAYS, new_block.cache_behaviour)

    def test_no_cache(self):
        block = Block()
        new_block = block.not_cache()
        self.assertEqual(CacheBehaviour.NEVER, new_block.cache_behaviour)

    def test_identifier(self):
        block1 = TestBlock()
        block2 = TestBlock()
        block3 = TestBlock2()
        self.assertIsInstance(block1.identifier, str)
        self.assertEqual(block1.identifier, block2.identifier)
        self.assertNotEqual(block3.identifier, block1.identifier)

    def test_update_identifier(self):
        block1 = TestBlock()
        block2 = TestBlock()
        block3 = TestBlock2()

        self.assertEqual(
            block2.update_identifier("123").identifier,
            block1.update_identifier("123").identifier
        )

        self.assertNotEqual(
            block1.update_identifier("123").identifier,
            block3.update_identifier("123").identifier
        )

    def test_cache_behaviour(self):
        class CacheBehaviourTestBlock(Block):
            def _load_cached_data(self):
                return CachedOutput(
                    code_hash="",
                    output="123"
                )

            def execute(self, dataset):
                return "123"

        with patch.object(CacheBehaviourTestBlock, "execute", return_value="out") as execute:
            block = CacheBehaviourTestBlock().not_cache()
            data = block.invoke(InvocationData("abc"))
            self.assertEqual("out", data.output)
            execute.assert_called_once()

        with patch.object(CacheBehaviourTestBlock, "execute", return_value="out") as execute:
            block = CacheBehaviourTestBlock().cache()
            data = block.invoke(InvocationData("abc"))
            self.assertEqual("123", data.output)
            execute.assert_not_called()

    def test_load_save_cache(self):
        class DummyCacheEngine(Cache):
            data = {}

            def set(self, name, value):
                self.data[self._make_key(name)] = value

            def get(self, name):
                return self.data[self._make_key(name)]

            def clear(self):
                pass

        block = Block()
        block.set_cache_engine(DummyCacheEngine())
        block._save_cached_data("abc")
        self.assertEqual("abc", block._load_cached_data())
        block._save_cached_code_state("111")
        self.assertEqual("111", block._load_cached_code_state())

    def test_auto(self):
        class CodeDidChangeTestBlock(Block):
            def _did_code_changed(self, _did_code_changed):
                return True

            def _load_cached_data(self):
                return CachedOutput(
                    code_hash="",
                    output="123"
                )

            def execute(self, dataset):
                return "123"

        with patch.object(CodeDidChangeTestBlock, "execute", return_value="out") as execute:
            block = CodeDidChangeTestBlock()
            data = block.invoke(InvocationData("abc"))
            self.assertEqual("out", data.output)
            execute.assert_called_once()

        class CodeDidNotChangeTestBlock(Block):
            def _did_code_changed(self, _did_code_changed):
                return False

            def _load_cached_data(self):
                return CachedOutput(
                    code_hash="",
                    output="123"
                )

            def execute(self, dataset):
                return "123"

        with patch.object(CodeDidNotChangeTestBlock, "execute", return_value="out") as execute:
            block = CodeDidNotChangeTestBlock()
            data = block.invoke(InvocationData("abc"))
            self.assertEqual("123", data.output)
            execute.assert_not_called()

    def test_code_change(self):
        block = SomeBlock()
        block.execute(None)


if __name__ == '__main__':
    unittest.main()
