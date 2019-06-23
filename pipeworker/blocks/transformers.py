from typing import Mapping

from toolz import pipe, valmap
import toolz.curried as c

from pipeworker.base import Group, Component, InvocationData


class Map(Group):
    def __init__(self, block: Component):
        self.block = block

    def invoke(self, data):
        result = (
            (valmap if isinstance(data, Mapping) else map)(
                lambda value: self.invoke_block(self.block, InvocationData(value, data.did_change)),
                data.output,
            )
        )

        some_did_changed = pipe(
            result.values(),
            c.map(lambda r: r.did_change),
            any,
        )
        return InvocationData(
            did_change=some_did_changed,
            output=valmap(lambda r: r.output, result)
        )
