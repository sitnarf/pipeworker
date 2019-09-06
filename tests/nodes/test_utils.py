import unittest

from mock import patch, call

from pipeworker.nodes.utils import NonModifyingNode


class NonModifyingNodeTest(NonModifyingNode):
    def fit(self, dataset):
        pass


class NodeUtilsTest(unittest.TestCase):
    def test_non_modifying(self):
        with patch.object(NonModifyingNodeTest, 'fit', return_value=None) as mock_method:
            node_test = NonModifyingNodeTest()
            node_test.fit()
            node_test.fit()
            mock_method.assert_has_calls([call(None), call(None)])
