from toolz import pipe, valmap
import toolz.curried as c

from pipeworker.base import Component, InvocationResult


class Map(Component):
    def __init__(self, node: Component):
        self.node = node

    @property
    def log_name(self):
        return "%s(%s)" % (self.__class__.__name__, self.node.log_name)

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

    def set_ancestors(self, ancestors):
        super().set_ancestors(ancestors)
        self.node.set_ancestors([*ancestors, self])
