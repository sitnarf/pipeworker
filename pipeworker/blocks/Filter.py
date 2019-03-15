from collections import Mapping
from toolz import valfilter
from pipeworker.base.Block import Block
from pipeworker.types import Invokable


class Filter(Block):

    def __init__(self, block: Invokable):
        self.block = block

    def execute(self, data):
        return (
            (valfilter if isinstance(data, Mapping) else filter)(
                lambda value: self.block.invoke(value),
                data,
            )
        )
