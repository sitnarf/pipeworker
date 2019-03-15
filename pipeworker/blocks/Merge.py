import collections
from itertools import chain
from collections.abc import Mapping
from functools import reduce
from typing import Iterable, TypeVar, List, Dict, Union
from pipeworker.base.Block import Block
from pipeworker.utils import is_primitive


T = TypeVar('T')


def merge_iterable(value1: Iterable, value2: Iterable) -> List:
    return list(chain(value1, value2))


class Merge(Block):

    _highest_number = 0

    def invoke(self, data: Union[Iterable[T], Dict] = None) -> T:
        return reduce(
            lambda current, next_item:
            self.merge(
                current,
                next_item
            ),
            data.values() if isinstance(data, collections.Mapping) else data,
            {}
        )

    def merge(self, value1: T, value2: T) -> T:
        if is_primitive(value1) or is_primitive(value2):
            return value1
        else:
            if type(value1) != type(value2):
                raise TypeError("Cannot merge %s and %s" % (type(value1), type(value2)))

            if isinstance(value1, Mapping):
                return self.merge_mapping(value1, value2)

            if isinstance(value1, Iterable):
                return merge_iterable(value1, value2)

    def merge_mapping(self, value1: Mapping, value2: Mapping) -> Mapping:
        result = {}
        for key in set().union(value1, value2):
            if key in value1 and key in value2:
                result[key] = self.merge(value1[key], value2[key])
            elif key in value1:
                result[key] = value1[key]
            elif key in value2:
                result[key] = value2[key]
        return result
