from typing import Optional
import unittest

from pipeworker.base import Node, CacheBehaviour, InvocationResult, LogLevel
from pipeworker.cache_engine import CachedOutput, CacheEngine
from tests.files.some_node import SomeNode


class TestNode(Node):
    pass


class TestNode2(Node):
    pass


class BaseTest(unittest.TestCase):
    def test_cache(self):
        node = Node()
        new_node = node.cache()
        self.assertEqual(CacheBehaviour.ALWAYS, new_node.cache_behaviour)

    def test_no_cache(self):
        node = Node()
        new_node = node.not_cache()
        self.assertEqual(CacheBehaviour.NEVER, new_node.cache_behaviour)

    def test_identifier(self):
        node1 = TestNode()
        node2 = TestNode()
        node3 = TestNode2()
        self.assertIsInstance(node1.identifier, str)
        self.assertEqual(node1.identifier, node2.identifier)
        self.assertNotEqual(node3.identifier, node1.identifier)

    def test_update_identifier(self):
        node1 = TestNode()
        node2 = TestNode()
        node3 = TestNode2()

        self.assertEqual(
            node2.update_identifier("123").identifier,
            node1.update_identifier("123").identifier
        )

        self.assertNotEqual(
            node1.update_identifier("123").identifier,
            node3.update_identifier("123").identifier
        )

    def test_cache_behaviour(self):
        class CacheBehaviourTestNode(Node):
            def _load_cached_input(self):
                return CachedOutput(
                    code_hash="",
                    output="abc"
                )

            def execute(self, dataset):
                return "123"

        node = CacheBehaviourTestNode().not_cache()
        data = node.invoke(InvocationResult("abc", executed=False))
        self.assertEqual("123", data.output)

        node = CacheBehaviourTestNode().cache()
        data = node.invoke(InvocationResult("abc", executed=False))
        self.assertEqual(None, data.output)
        self.assertEqual(False, data.executed)

    def test_load_save_cache(self):
        class DummyCacheEngine(CacheEngine):
            data = {}

            def set(self, name, value):
                self.data[self._make_key(name)] = value

            def get(self, name):
                return self.data[self._make_key(name)]

            def clear(self):
                pass

        node = Node()
        node_with_cache = node.set_cache_engine(DummyCacheEngine())
        node_with_cache._save_cached_input("abc")
        self.assertEqual("abc", node_with_cache._load_cached_input())
        node_with_cache._save_cached_code_state("111")
        self.assertEqual("111", node_with_cache._load_cached_code_state())

    def test_auto(self):
        class CodeDidChangeTestNode(Node):
            def _did_code_changed(self, _did_code_changed):
                return True

            def _load_cached_input(self):
                return CachedOutput(
                    code_hash="",
                    output="abc"
                )

            def execute(self, dataset):
                return "123"

        node = CodeDidChangeTestNode()
        data = node.invoke(InvocationResult("x"))
        self.assertEqual("123", data.output)

        class CodeDidNotChangeTestNode(Node):
            def _did_code_changed(self, _did_code_changed):
                return False

            def _load_cached_input(self):
                return CachedOutput(
                    code_hash="",
                    output="123_cached"
                )

            def execute(self, dataset):
                return "123_executed"

        node = CodeDidNotChangeTestNode()
        data = node.invoke(InvocationResult("abc", executed=False))
        self.assertEqual(data.output, None)

    def test_code_change(self):
        node = SomeNode()
        node.execute(None)

    def test_name(self):
        node = SomeNode()
        self.assertEqual(node.set_name("some_name").name, "some_name")
        self.assertEqual(node.name, "SomeNode")

    def test_full_name(self):
        class SomeOtherNode(Node):
            def __init__(self, log_name: Optional[str]):
                self._name_to_log = log_name

            @property
            def log_name(self) -> Optional[str]:
                return self._name_to_log

        node = SomeNode()
        node.ancestors = [
            SomeOtherNode(None),
            SomeOtherNode("log_name"),
            SomeOtherNode(None),
            SomeOtherNode(None),
            SomeOtherNode("some_other_log_name")
        ]
        self.assertEqual("log_name → some_other_log_name → SomeNode", node.full_name)

    def test_sequence_getitem(self):
        node1 = SomeNode()
        node3 = Node()
        sequence = node1 | SomeNode() | node3
        self.assertIsInstance(sequence[0], SomeNode)
        self.assertIsInstance(sequence[2], Node)

    def test_parallel_getitem(self):
        node1 = SomeNode()
        node3 = Node()
        parallel = node1 & SomeNode() & node3
        self.assertIsInstance(parallel[0], SomeNode)
        self.assertIsInstance(parallel[2], Node)

    def test_provide_ancestors(self):
        sequence = SomeNode() | Node() | SomeNode()
        self.assertIsInstance(sequence[2].ancestors[0], SomeNode)
        self.assertIsInstance(sequence[2].ancestors[1], Node)

    def test_propagate_context(self):
        sequence = SomeNode() | Node() | SomeNode().set_log_level(LogLevel.DISABLED)
        with_log = sequence.set_log_level(LogLevel.ENABLED)
        self.assertEqual(LogLevel.ENABLED, with_log[2].log_level)


if __name__ == '__main__':
    unittest.main()
