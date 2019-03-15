from functools import reduce

from pipeworker.base.BlockGroup import BlockGroup


class Sequence(BlockGroup):
    def __init__(self, initialize=None):
        self.sequence = initialize if initialize else []

    def invoke(self, data=None):
        result = reduce(
            lambda current_data, block:
            self.invoke_block(block, current_data),
            self.sequence,
            data
        )
        return result

    def append(self, block):
        self.sequence.append(block)
        return self

    def get_sequence(self):
        return self.sequence

    def __or__(self, next_block):
        self.append(next_block)
        return self

    def __and__(self, next_block):
        from pipeworker.base.Parallel import Parallel
        return Parallel([self, next_block])

    def __iter__(self):
        return iter(self.sequence)
