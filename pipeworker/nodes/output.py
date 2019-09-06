from toolz import pipe, valmap
import toolz.curried as c

from pipeworker.base import Component, InvocationResult


class Each(Component):
    def __init__(self, node: Component):
        self.node = node

    def fit(self, invocation: InvocationResult = None):
        result = (
            valmap(
                lambda value: (
                    self._invoke_fit(self.node, InvocationResult(value, invocation.executed))
                ),
                invocation.output,
            )
        )
        some_did_changed = pipe(
            result.values(),
            c.map(lambda r: r.executed),
            any,
        )
        return InvocationResult(
            executed=some_did_changed,
            output=valmap(lambda r: r.output, result)
        )
