import unittest

from pipeworker.immutable import set_immutable, immutable_action


class SomeClass:
    property = 12

    def get_property(self):
        return self.property


class SomeOtherClass:
    property = 12

    def set_property(self, value):
        self.property = value


class Utils(unittest.TestCase):
    def test_set_attribute(self):
        some_instance = SomeClass()
        some_other_instance = set_immutable(some_instance, "property", 15)
        self.assertEqual(some_instance.property, 12)
        self.assertEqual(some_other_instance.property, 15)

    def test_immutable_action(self):
        some_instance = SomeOtherClass()
        some_other_instance = immutable_action(some_instance, lambda o: o.set_property(13))
        self.assertEqual(some_instance.property, 12)
        self.assertEqual(some_other_instance.property, 13)


if __name__ == '__main__':
    unittest.main()
