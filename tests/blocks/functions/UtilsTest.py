import unittest
from pipeworker.functions.utils import dict_deep_merge


class UtilsTest(unittest.TestCase):
    def test_dict_deep_merge(self):
        result = dict_deep_merge(
            {"a": 1, "b": {"c": 3, "d": 4, "e": [1]}},
            {"a2": 1, "b": {"d": 5, "e": [2]}}
        )
        self.assertEqual(
            result,
            {'a': 1, 'a2': 1, 'b': {'c': 3, 'd': 5, 'e': [1, 2]}},
        )


if __name__ == '__main__':
    unittest.main()
