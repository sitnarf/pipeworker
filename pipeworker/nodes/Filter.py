from collections import Mapping
from toolz import valfilter
from pipeworker.base import Node
from pipeworker.types import Invokable


class Filter(Node):

    def __init__(self, node: Invokable):
        self.node = node

    def fit(self, data):
        return (
            (valfilter if isinstance(data, Mapping) else filter)(
                lambda value: self.node.invoke(value),
                data,
            )
        )
