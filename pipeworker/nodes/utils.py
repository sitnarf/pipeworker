from pipeworker.base import Node, InvocationResult


class NonModifyingNode(Node):
    def fit(self, invocation: InvocationResult = None) -> InvocationResult:
        invocation = invocation or InvocationResult(
            output=None,
            executed=None,
        )
        self.fit(invocation.output)
        return InvocationResult(
            invocation.output,
            executed=False
        )


class BreakPoint(NonModifyingNode):
    def fit(self, dataset):
        breakpoint()
        return dataset
